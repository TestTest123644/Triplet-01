# Twin layer (Layer 1)

The Twin layer simulates 42 devices across 6 zones in Greater Tunis. It streams Ed25519-signed telemetry to Kafka at 420 messages per second, exposes 16 command tools over Model Context Protocol (MCP) on port 8000, and runs an adversary engine backed by an isolated SQLite ground-truth ledger.

---

## Quick start

All commands run from the repository root.

If a container named "kafka" already exists, remove or rename it first (`docker rm -f kafka` or `docker rename kafka kafka-old`).

```bash
# 1. Start the Twin layer, Kafka broker, and console (SIM_SPEED=20 for scenario pace)
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

- **Kafka telemetry:** `localhost:19092` (internal `kafka:9092`, topic `twin.sensors`). Accepts comma-separated brokers (`host1:port,host2:port,host3:port`) via `KAFKA_BOOTSTRAP_SERVERS`. The topic `twin.sensors` must already exist on the broker (the Twin never creates topics or modifies broker settings). Messages are keyed by `device_id` to ensure strict per-device ordering within partitions. Producer uses durable delivery (`acks=all`, `enable.idempotence=True`), overridable via `KAFKA_ACKS` (`all`, `1`, `0`). Set `WAIT_FOR_KAFKA=false` if broker ACLs disallow cluster description. Optional TLS CA file via `KAFKA_SSL_CA_LOCATION`.
- **MCP command server:** `http://localhost:8000/sse` and `/mcp` (JSON-RPC)
- **Red-team adversary API:** `http://localhost:8100/api/status` (header `X-Red-Team-Token`)
- **Operations console:** `http://localhost:8501` (Streamlit)

---

## Documentation index

- [Interface Card](INTERFACE.md): One-page contract specification, real wire messages, and authentication
- [Interface Reference](docs/INTERFACE_REFERENCE.md): Full reference for all 16 MCP tool signatures and parameter rules
- [Developer Guide](docs/DEVELOPER_GUIDE.md): Architecture, module layout, and mock boundary specifications
- [User Guide](docs/USER_GUIDE.md): Operations console manual, timeline navigation, and manual attack injection
- [Physics and Adversary Spec](docs/PHYSICS_AND_ADVERSARY_SPEC.md): Differential equations, diurnal curves, and adversary capabilities
