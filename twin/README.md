# Twin layer (Layer 1)

The Twin layer simulates 42 devices across 6 zones in Greater Tunis. It streams Ed25519-signed telemetry to Kafka at 420 messages per second, exposes 16 command tools over Model Context Protocol (MCP) on port 8000, and runs an adversary engine backed by an isolated SQLite ground-truth ledger.

---

## Quick start

All commands run from the repository root.

If a container named "kafka" already exists, remove or rename it first (`docker rm -f kafka` or `docker rename kafka kafka-old`).

```bash
# 1. Start the Twin layer, Kafka broker, and console (SIM_SPEED=20 for scenario pace)
# For demo runs with MCP auth enabled (routes test commands to Test harness lane):
# MCP_AUTH_TOKENS="brain:tok-b,guardian:tok-g,scenario:tok-s" SIM_SPEED=20 docker compose up -d --build
# To align with partner networks, set TRIPLET_NETWORK (e.g. twin_network for Brain, shared-backend for Guardian):
# TRIPLET_NETWORK=twin_network SIM_SPEED=20 docker compose up -d --build
SIM_SPEED=20 docker compose up -d --build

# 2. Export the red-team token generated inside the container
export RED_TEAM_TOKEN=$(docker exec twin_layer cat /app/data/red_team.token)

# 3. Run an incident scenario with mock partners
python3 scenarios/t5_grid_overload.py --with-mocks

# 4. Open the operations console
open http://localhost:8501
```

---

## Local development run

To run locally without containers:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run automated test suite
python3 -m pytest -q

# 3. Start the Twin engine (requires running Kafka or WAIT_FOR_KAFKA=false)
PYTHONPATH=src python3 -m twin.main

# 4. In a separate terminal, launch the console
PYTHONPATH=src streamlit run dashboard.py
```

---

## Endpoints and services

- **Kafka telemetry:** `localhost:19092` (internal `kafka:9092`, topic `twin.sensors`). Accepts comma-separated brokers (`host1:port,host2:port,host3:port`) via `KAFKA_BOOTSTRAP_SERVERS`. The topic `twin.sensors` must already exist on the broker (the Twin never creates topics or modifies broker settings). Messages are keyed by `device_id` to ensure strict per-device ordering within partitions. Producer uses durable delivery (`acks=all`, `enable.idempotence=True`), overridable via `KAFKA_ACKS` (`all`, `1`, `0`). Set `WAIT_FOR_KAFKA=false` if broker ACLs disallow cluster description. Optional TLS CA file via `KAFKA_SSL_CA_LOCATION`. Optional `KAFKA_BROKER_ADDRESS_FAMILY` (`any`, `v4`, `v6`, default `any`; set `v4` to suppress IPv6 "Connection refused" log noise when brokers bind to 127.0.0.1).
- **MCP command server:** `http://localhost:8000/sse` and `/mcp` (JSON-RPC). Host port configurable via `TWIN_MCP_HOST_PORT` (default 8000).
- **Red-team adversary API:** `http://localhost:8100/api/status` (header `X-Red-Team-Token`). Includes `POST /api/secrets/reload` for zero-downtime secret rotation without restarting. Host port configurable via `TWIN_RED_TEAM_HOST_PORT` (default 8100).
- **Operations console:** `http://localhost:8501` (Streamlit). Host port configurable via `TWIN_CONSOLE_HOST_PORT` (default 8501).
- **Docker network:** defaults to `city_network`, customizable via `TRIPLET_NETWORK` (e.g. `twin_network` for Brain, `shared-backend` for Guardian). Set `TRIPLET_NETWORK_EXTERNAL=true` to join a pre-existing Docker network without editing compose files.
- **Secrets volume:** Host secrets directory (default `./secrets` in root, `../secrets` in `twin/`, or via `TWIN_SECRETS_DIR`) is mounted read-only to `/app/secrets:ro`.

### Connecting to the Guardian's Kafka cluster

The Guardian's SASL password file contains 48 hex characters generated with `openssl rand -hex 24` (strip any trailing newline).

#### (a) Host process (`python -m twin.main`)

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:29094,localhost:29095,localhost:29096
export KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
export KAFKA_SASL_MECHANISM=PLAIN
export KAFKA_USERNAME=twin_producer
export KAFKA_PASSWORD_FILE=./secrets/kafka_password
export KAFKA_TOPIC=telemetry-ingest
export KAFKA_BROKER_ADDRESS_FAMILY=v4
python -m twin.main
```

#### (b) Container on shared-backend network

```bash
docker network create shared-backend  # once
TRIPLET_NETWORK=shared-backend TRIPLET_NETWORK_EXTERNAL=true \
KAFKA_BOOTSTRAP_SERVERS=guardian-kafka:9092 \
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT \
KAFKA_USERNAME=twin_producer \
KAFKA_PASSWORD_FILE=/app/secrets/kafka_password \
docker compose up -d
```

If the Brain runs on the same host, set `TWIN_RED_TEAM_HOST_PORT` (default 8100) and `TWIN_MCP_HOST_PORT` (default 8000) to prevent port collisions.

---

## Secrets and dynamic rotation

Secrets (`KAFKA_PASSWORD`, `MCP_AUTH_TOKENS`, `RED_TEAM_TOKEN`) resolve via pluggable hook (`TWIN_SECRET_HOOK`), files (`<NAME>_FILE`), or environment variables. Reload secrets at runtime without restarting via:

```bash
curl -X POST http://localhost:8100/api/secrets/reload -H "X-Red-Team-Token: $RED_TEAM_TOKEN"
```

Or send `kill -HUP <twin_pid>`, or configure `TWIN_SECRET_RELOAD_SECONDS`. Device signing keys never rotate at runtime.

### HashiCorp Vault Key Synchronization

The Twin can optionally synchronize inter-layer communication keys from HashiCorp Vault KV v2 using AppRole authentication:

```bash
export VAULT_ADDR="https://<cluster>.hashicorp.cloud:8200"
export VAULT_NAMESPACE="admin"
export VAULT_ROLE_ID_FILE="./secrets/vault_twin_role_id"     # in container: /app/secrets/vault_twin_role_id
export VAULT_SECRET_ID_FILE="./secrets/vault_twin_secret_id" # in container: /app/secrets/vault_twin_secret_id
export VAULT_REFRESH_SECONDS=300
```

Paths synchronized: `comm/twin/guardian/private`, `comm/twin/brain/private`, `comm/guardian/twin/public`, `comm/brain/twin/public`. Supports both PEM and kid formats with automatic key algorithm detection (X25519, Ed25519, RSA). Keys are fetched and kept fresh in memory but not yet used for any traffic encryption. Check metadata status via `GET /api/status` under the `vault` key.

---

## Documentation index

- [Interface Card](INTERFACE.md): One-page contract specification, real wire messages, and authentication
- [Interface Reference](docs/INTERFACE_REFERENCE.md): Full reference for all 16 MCP tool signatures and parameter rules
- [Developer Guide](docs/DEVELOPER_GUIDE.md): Architecture, module layout, and mock boundary specifications
- [User Guide](docs/USER_GUIDE.md): Operations console manual, timeline navigation, and manual attack injection
- [Physics and Adversary Spec](docs/PHYSICS_AND_ADVERSARY_SPEC.md): Differential equations, diurnal curves, and adversary capabilities
