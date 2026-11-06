# Twin layer (Layer 1)

The Twin layer simulates 42 devices across 6 zones in Greater Tunis. It streams Ed25519-signed telemetry to Kafka at 420 messages per second, exposes 16 command tools over Model Context Protocol (MCP) on port 8000, and runs an adversary engine backed by an isolated SQLite ground-truth ledger.

---

## Quick start in 3 commands

From the `twin/` directory:

```bash
# 1. Start the Twin layer, Kafka broker, and console
docker compose up -d

# 2. Run an incident scenario with mock partners
python3 ../scenarios/t5_grid_overload.py --with-mocks

# 3. Open the operations console
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

- **Kafka telemetry:** `localhost:19092` (internal `kafka:9092`, topic `twin.sensors`)
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
