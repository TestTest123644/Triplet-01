# Guardian monitoring dashboard (Grafana)

Issue **#108**: one Grafana dashboard over Elasticsearch (container / app logs) and PostgreSQL (predictions + sensor readings). Grafana only **reads** data; it does not ingest or generate it.

## Architecture

```
Compose services (stdout)
        │
        ▼  Docker fluentd logging driver → 127.0.0.1:24224
   fluent-bit
        │
        ▼  index guardian-logs
  elasticsearch  ──read──►  grafana (Server logs)
  postgres       ──read──►  grafana (verdicts / sensors)
```

Kafka, Postgres, Grafana, and other non-Python services do **not** write to Elasticsearch themselves. **Fluent Bit** is the log shipper: it collects container stdout and indexes documents shaped for the dashboard (`@timestamp`, `level`, `logger`, `message`).

Flink (`jobmanager` / `taskmanager` / `job-submitter`) is **out of scope** here until `Dockerfile.flink` exists (owned separately). Skip those services for monitoring demos.

## What you need

| Piece | Role |
|---|---|
| `docker-compose.yml` → `grafana` | UI on `127.0.0.1:3000` |
| `docker-compose.yml` → `fluent-bit` | Ships Compose service stdout into Elasticsearch |
| `fluent-bit/fluent-bit.conf` + `rewrite.lua` | Forward input → `guardian-logs` |
| `grafana/provisioning/datasources/datasources.yml` | ES + Postgres plugins |
| `grafana/dashboards/guardian-monitoring.json` | Provisioned dashboard |
| `.env` | `GF_SECURITY_ADMIN_PASSWORD`, `ELASTIC_PASSWORD`, `ELASTICSEARCH_PASSWORD` |
| `secrets/postgres_password.txt` | Postgres password (Compose secret) |
| `secrets/postgres_grafana_password.txt` | Dedicated Grafana reader password |
| `secrets/elastic_grafana_password.txt` | Dedicated Elasticsearch reader password |
| `secrets/kafka_*.txt` | Non-empty **hex** passwords (CR/LF is stripped by the Kafka entrypoint) |

Start **`fluent-bit` before** other services that use the `fluentd` logging driver (`x-logging` in Compose). Fluent Bit does not ship its own logs (avoids a feedback loop).

## Start

Create secret files first (Postgres / Grafana reader / Elasticsearch reader / Kafka hex passwords).

```bash
docker network create shared-backend 2>/dev/null || true

docker compose up -d --wait elasticsearch
docker compose up -d fluent-bit
docker compose up -d --wait postgres
docker compose up -d grafana
```

Kafka + app services (after all `secrets/kafka_*_password.txt` are valid hex):

```bash
docker compose up -d --force-recreate kafka-1 kafka-2 kafka-3
# wait until brokers are Up (not Restarting)
docker compose up kafka-init   # must exit 0
docker compose up -d --build action-taker twin-mcp device-metadata-producer
```

Do **not** start Flink until `Dockerfile.flink` is available.

PostgreSQL panels read the ORM stage tables (`processed_readings`, `model_outputs`) created by the storage schema (`create_schema`). Ensure those tables exist and contain rows before expecting non-empty sensor/verdict panels.

1. Open http://localhost:3000 (`admin` / value of `GF_SECURITY_ADMIN_PASSWORD`).
2. Open **Guardian monitoring** (or `/d/guardian-monitoring`).
3. The dashboard time picker defaults to **2004-02-01 → now** so Intel Lab sample rows and live Server logs appear together. Auto-refresh is **30s**.
4. **Connections → Data sources** → Test **Elasticsearch** and **PostgreSQL**.

## Datasources

| Plugin | URL (Compose network) | Data |
|---|---|---|
| Elasticsearch | `http://elasticsearch:9200` | Index `guardian-logs` (`@timestamp`, `level`, `logger`, `message`) via dedicated reader `grafana_reader` |
| PostgreSQL | `postgres:5432` | `processed_readings`, `model_outputs` (dedicated read-only role) |

`logger` is the Docker container name (e.g. `grafana`, `action-taker`, `guardian_layer-kafka-1-1`). `level` is inferred from the log line (`INFO` / `WARNING` / `ERROR` / `CRITICAL`).

## Dashboard sections

| Section | Panels |
|---|---|
| **1 · Server logs** | Total / Warnings / Errors / Critical · Logs by level (donut) · Top loggers · Log explorer |
| **2 · Model verdicts** | Stored verdicts / Stored anomalies / Anomaly rate / Mean score · Verdict scores · Anomalies by sensor · Stored verdict details |
| **3 · Sensors** | Readings / Sensors / Clusters · Inventory · Avg value by cluster · Recent readings |

## Filters

| Variable | Use |
|---|---|
| **Time range** | Filters logs on `@timestamp`, sensors on `processed_readings.timestamp`, verdicts on `model_outputs.window_end` (default 2004-02-01 → now, refresh 30s) |
| **Log level** | Elasticsearch panels only |
| **Sensor** / **Cluster** | Sensor + verdict panels |
| **Anomaly** | Verdict panels (`model_outputs.is_anomaly`) |

Sensor panels use `processed_readings`. Verdict panels use `model_outputs` (`score`, `is_anomaly`, `model_version`, `window_end`). Other Postgres tables are intentionally not queried.

## Expected empty states

- **Server logs** stay empty until Fluent Bit is running and at least one Compose service with the logging driver is emitting stdout.
- Kafka brokers are noisy; quieter apps (e.g. `action-taker`) may show few lines until they handle traffic.
- **Model verdicts / Sensors** stay empty until rows exist in `model_outputs` / `processed_readings` (current sample data is dated 2004).
- Flink loggers will not appear until Flink images/services are runnable.
- Narrowing the picker to “Last 15 minutes” hides 2004 Postgres rows; keep the wide default (or equivalent) to see both.
