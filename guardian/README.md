# Guardian Layer (Layer 3)
## Zero-Trust Intelligent Security Perimeter for Municipal IoT Infrastructure

The **Guardian Layer** is an active, zero-trust edge security and anomaly detection gatekeeper in a 3-layer urban monitoring architecture:
- **Layer 1: Digital Twin** — Physical & virtual sensor transducers (stormwater drainage, power distribution, arterial traffic).
- **Layer 3: Guardian Layer** — Untrusted stream ingestion gate, digital signal processing (DSP) wavelet decomposition, dual-horizon XGBoost AI inference, relational persistence, and automated FastMCP containment.
- **Layer 2: City Brain** — Municipal supervisory control and autonomous actuation. Operates in a zero-trust holding state (`WAITING_FOR_GUARDIAN_TRUST`), executing city actuations only upon receiving an Ed25519-signed trust verdict from the Guardian.

---

## 1. System Prerequisites

Before running the project, verify that your host system meets the following requirements:

### Operating System:
- **Linux** (Ubuntu 22.04/24.04, Debian 12, Fedora, or equivalent recommended).
- **macOS** (13+ Ventura or Sonoma with Docker Desktop).
- **Windows** (via WSL2 with Ubuntu).

### Required Software:
1. **Python 3.12+** (tested on Python 3.12.3).
2. **`uv` (0.12.20+)** — Ultra-fast Rust-based Python package and workspace manager:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
3. **Docker Engine (24.0+) & Docker Compose (v2.20+)** — For running Kafka KRaft brokers, PostgreSQL 16, Elasticsearch, Fluent Bit, and Grafana.
4. **OpenSSL** — For generating secure hexadecimal passwords.
5. **Git** — For repository cloning and version control.

---

## 2. Installation & Environment Configuration

### Step 1: Clone the Repository & Install Dependencies
Clone the repository and synchronize the isolated virtual environment using `uv`:

```bash
git clone https://github.com/your-org/<repo_name>.git
cd <repo_name>

# Install all runtime and developer dependencies using uv
uv sync
```

### Step 2: Configure Environment Variables
Create your local `.env` file from the provided template:

```bash
cp .env.example .env
```

Key environment variables:

| Variable | Purpose | Default / Example |
| :--- | :--- | :--- |
| `POSTGRES_USER` | PostgreSQL superuser username | `guardian` |
| `POSTGRES_DB` | PostgreSQL runtime database name | `guardian` |
| `GUARDIAN_DATABASE_URL` | Application database connection string | `postgresql://guardian:...@localhost:5432/guardian` |
| `ELASTIC_PASSWORD` | Password for Elasticsearch `elastic` admin account | *Generated hex secret* |
| `ELASTICSEARCH_PASSWORD` | Password for Python asynchronous logging client | *Same as ELASTIC_PASSWORD* |
| `GUARDIAN_ELASTICSEARCH_URL`| Endpoint for Elasticsearch audit logging | `http://localhost:9200` |
| `GF_SECURITY_ADMIN_PASSWORD` | Admin password for Grafana web dashboard | *Generated admin secret* |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka cluster broker bootstrap addresses | `localhost:29094,localhost:29095,localhost:29096` |

### Step 3: Generate Required Secret Files
Security policies mandate that passwords for PostgreSQL, Kafka, and Grafana are mounted strictly through Docker secrets under the `secrets/` directory (never committed to Git).

Run the following script to generate all required secret files with 32-byte hexadecimal passwords:

```bash
mkdir -p secrets

for secret in \
  postgres_password.txt \
  postgres_grafana_password.txt \
  elastic_grafana_password.txt \
  kafka_broker_password.txt \
  kafka_admin_password.txt \
  kafka_client_password.txt \
  kafka_twin_password.txt \
  kafka_brain_password.txt \
  kafka_broadcast_password.txt \
  kafka_action_taker_password.txt; do
  if [ ! -f "secrets/$secret" ]; then
    openssl rand -hex 32 > "secrets/$secret"
    chmod 600 "secrets/$secret"
  fi
done
```

> [!IMPORTANT]
> The `secrets/` directory is excluded by `.gitignore`. Never commit plaintext credentials to Git. If a PostgreSQL data volume already exists, updating `secrets/postgres_password.txt` will not retroactively change the database credentials unless the volume is deleted or altered manually in PostgreSQL.

---

## 3. Starting Infrastructure Services (Docker Compose)

The backend infrastructure runs as a distributed cluster defined across modular compose specifications (`docker-compose.broker.yml`, `docker-compose.processing.yml`, `docker-compose.monitoring.yml`):

### Start the Full Stack:
```bash
docker compose up -d
```

### Or Start Subsystems Individually:
- **Relational Storage & Stream Processing (PostgreSQL 16, Apache Flink):**
  ```bash
  docker compose -f docker-compose.processing.yml up -d
  ```
- **3-Broker Kafka Cluster (KRaft Mode):**
  ```bash
  docker compose -f docker-compose.broker.yml up -d
  ```
- **Observability Stack (Elasticsearch, Fluent Bit, Grafana, Kafka UI):**
  ```bash
  docker compose -f docker-compose.monitoring.yml up -d
  ```

### Verify Service Status:
```bash
docker compose ps
```

### Inspect Kafka Topic Provisioning:
```bash
docker compose logs kafka-init
```

The `kafka-init` container automatically provisions the broker topics and configures role-based access control (RBAC):

| Topic | Partitions | Replication | Primary Purpose & Access |
| :--- | :---: | :---: | :--- |
| `telemetry-ingest` | 6 | 3 | Raw incoming stream from Twin (`User:twin` write, `User:guardian` read) |
| `telemetry-validated` | 6 | 3 | Clean stream consumed by Guardian and City Brain |
| `telemetry-confirmation`| 6 | 3 | Ed25519-signed trust verdicts (`User:guardian` write, `User:brain`/`User:twin` read) |
| `telemetry-anomaly` | 3 | 3 | Anomaly events consumed by ActionTaker (`User:action_taker` read) |
| `telemetry-dlq` | 3 | 3 | Dead-letter queue for rejected/corrupt payloads |
| `devices-metadata` | 3 (compacted) | 3 | Topology and physical range bounds |

---

## 4. Running the Guardian Orchestrator

The **Guardian Orchestrator** connects message brokers, sliding ring buffers, DSP feature pipelines, and machine learning models:

```bash
uv run python -m guardian_layer.orchestrator
```

### Orchestrator Pipeline Flow:
1. **Stream Ingestion & Buffering:** Consumes validated readings into sliding memory buffers ($2.75\,\mu\text{s}$ ingestion latency).
2. **Deterministic Static Filtering:** Catches out-of-range readings, dropout silence ($>60\text{ s}$), and spatial $k$-NN cluster spoofing.
3. **Discrete Wavelet Transforms (DWT):** Computes multi-level Haar wavelet energy subbands ($d_1, d_2, d_3$), robust $Z$-scores, and difference kurtosis.
4. **Machine Learning Inference:** Evaluates short-window ($20\text{ s}$) XGBoost models (`AnomalyDetector`) and long-window ($24\text{ h}$) historical models (`LongAnomalyDetector`).
5. **Relational Storage:** Persists raw readings, transformed feature windows (JSONB), and model outputs with model version attribution (`model_version`).
6. **Zero-Trust Dispatch:** Publishes Ed25519-signed cryptographic verdicts to `telemetry-confirmation` and dispatches detected anomalies to `telemetry-anomaly`.
7. **Rate-Limited Logging:** Batches operational logging metrics every 30 seconds to prevent console and I/O saturation.

---

## 5. Running the Cross-Layer Integration Scenarios

The integration harness (`mocks/run_scenarios.py`) connects **Layer 1 (Digital Twin)**, **Layer 3 (Guardian Layer)**, and **Layer 2 (City Brain)** in a closed-loop zero-trust verification suite:

```bash
uv run python mocks/run_scenarios.py
```

### Verified Scenarios:
- **Scenario 1 [BASELINE]:** Nominal Stream Operation $\rightarrow$ Guardian signs `TRUSTED` $\rightarrow$ Brain executes municipal optimizations (`set_valve(optimal_flow)`).
- **Scenario 2 [STATIC_CHECK]:** Physical Range Breach ($15.5\text{m} > 12.0\text{m}$) $\rightarrow$ Device quarantined $\rightarrow$ Brain executes flood valve relief.
- **Scenario 3 [STATIC_CHECK]:** Sensor Silence ($>60\text{ s}$) $\rightarrow$ Device quarantined $\rightarrow$ Brain dispatches maintenance crew.
- **Scenario 4 [SPATIAL_CHECK]:** Spatial Cluster Spoofing $\rightarrow$ Device isolated $\rightarrow$ Brain confirms security containment.
- **Scenario 5 [AI_INFERENCE]:** Hardware Jitter Burst $\rightarrow$ XGBoost classifies `SENSOR_MALFUNCTION` ($0.991$) $\rightarrow$ Technician dispatched.
- **Scenario 6 [AI_INFERENCE]:** Synthetic Bitstream Inversion $\rightarrow$ XGBoost classifies `NOT_SENSOR_DATA` ($1.000$) $\rightarrow$ Device isolated.
- **Scenario 7 [AI_LONG_WINDOW]:** 24-hour Database Lookback $\rightarrow$ Historical Haar grid evaluated $\rightarrow$ ModelOutput logged with `model_version="long_window_24h"`.
- **Scenario 8 [SECURITY_GATE]:** Anti-Corruption ACL Gate $\rightarrow$ Telemetry from quarantined/isolated devices dropped at boundary.

---

## 6. Running Performance & Latency Benchmarks

The benchmark suite measures microsecond ingestion speed and complete end-to-end pipeline latency under production-equivalent database and ML load:

```bash
uv run python scripts/benchmark_orchestrator.py
```

### Measured Key Performance Indicators (KPIs):
- **Ingestion Throughput:** **40,941 readings/sec** ($24.43\,\mu\text{s}$ per reading).
- **Sliding Ring Buffer Flush Latency:** **$15.55\,\mu\text{s}$**.
- **100-Reading Batch Latency:**
  - **Mean:** **$52.87\text{ ms}$** ($\pm 4.12\text{ ms}$)
  - **P50 (Median):** **$51.20\text{ ms}$**
  - **P95:** **$64.50\text{ ms}$**
  - **P99:** **$68.99\text{ ms}$**
- **Pipeline Processing Rate:** **~1,891 readings/second** with live dual-table PostgreSQL 16 persistence ($528\,\mu\text{s}$ effective per-reading cost).
- **Latency Headroom:** Achieves $>54\%$ CPU margin under sustained peak load relative to the $150\text{ ms}$ real-time budget.

---

## 7. Automated Testing, Linting & Code Quality

### Run Complete Test Suite:
```bash
uv run pytest
```

### Run Infrastructure Verification Tests:
```bash
uv run pytest tests/infra/test_compose.py -v
```

### Run Fast Code Linting (`ruff`):
```bash
uv run ruff check .
```

### Format Codebase (`ruff`):
```bash
uv run ruff format .
```

---

## 8. Web Dashboards & Observability

| Service | Local URL | Default Credentials / Secret Reference | Description |
| :--- | :--- | :--- | :--- |
| **Grafana Monitoring** | `http://localhost:3000` | User: `admin` / Password: `GF_SECURITY_ADMIN_PASSWORD` | Live monitoring of sensor health, model scores, and logs |
| **Apache Flink Dashboard** | `http://localhost:8081` | None (Local bind) | Stream processing topology, JobManager overview, TaskManager slot allocation, and checkpoints |
| **Kafka UI** | `http://localhost:8080` | User: `admin` / Password: `secrets/kafka_admin_password.txt` | Broker cluster health, topic partition inspector, ACL viewer |
| **PostgreSQL** | `localhost:5432` | User: `guardian` / Password: `secrets/postgres_password.txt` | Relational database (`raw_readings`, `model_outputs`) |
| **Elasticsearch** | `http://localhost:9200` | User: `elastic` / Password: `ELASTIC_PASSWORD` | Distributed JSON log analytics index (`guardian-logs`) |

---

## 9. Standards Compliance & Security Guidelines

- **ISO/IEC 42001:2023 (Artificial Intelligence Management System):** Strict adherence to data provenance, scale-free wavelet normalization, model lineage tracking (`model_version`), and an ultra-low false-alarm budget ($<0.04$ FA/sensor-day, achieved $0.008$).
- **NIST SP 800-207 (Zero Trust Architecture):** The City Brain holds incoming telemetry in `WAITING_FOR_GUARDIAN_TRUST`, preventing unverified readings from triggering false city actuations.
- **NIST SP 800-57 & Ed25519 Cryptography:** Asymmetric signatures provide cryptographic non-repudiation on ingest streams and outgoing trust confirmation messages.
- **Least Privilege:** Microservices use dedicated SASL credentials with topic-scoped permissions. Internal database and broker ports are bound strictly to `127.0.0.1`.


----

## 10. Incase Of error
if uv complain about missing JAVA_HOME simply run as follow

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 uv sync
```