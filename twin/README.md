# Layer 1: Digital Twin & Physical World Simulation

The **Twin Layer** simulates the physical smart city environment, produces high-frequency telemetry, signs outgoing data cryptographically, and executes actuation and containment commands.

---

## 1. Responsibilities
- **Physical World Simulation (10 Hz):** Models 42 devices across 6 geographic zones in Greater Tunis spanning 5 sensor types (`traffic_camera`, `water_level`, `power_load`, `air_quality`, `emergency_call`).
- **Telemetry Streaming (Kafka):** Broadcasts 420 msgs/s to Apache Kafka topic `twin.sensors`. Every payload conforms to the sub-300-byte schema and includes a unique Ed25519 digital signature.
- **Synchronous Commands (MCP):** Exposes an official Model Context Protocol (MCP) server on port `8000` (`http://localhost:8000/sse`) for device discovery (`list_devices`), cryptographic key retrieval (`get_device_key`), actuation (`dispatch`, `set_valve`, `switch_grid`, `set_signal_plan`), and containment (`quarantine_device`, `isolate_device`, `rollback_reading`).
- **Red Team Adversary Engine:** Injects 6 cyber-physical attack capabilities (`link`, `rogue`, `device_key`, `device_physical`, `actuator`, `sensor_fault`).
- **Ground Truth Ledger:** Logs uncompromised ground truth and attack events into an isolated local SQLite database (`ground_truth.db`).

---

## 2. Directory Layout
```text
twin/
├── .env.example          # Environment variable reference and defaults
├── Dockerfile            # Container build definition for Twin Layer runtime
├── INTERFACE.md          # Complete API contract and Kafka schema specification
├── README.md             # This document
├── pyproject.toml        # Pytest configuration (pythonpath = ["src", "."])
├── requirements.txt      # Python dependencies
├── docker-compose.yml    # Standalone compose: Kafka + Twin Layer + Dashboard
├── dashboard.py          # Operations and adversary management console
├── src/
│   └── twin/             # Core twin package
│       ├── __init__.py   # Package exports
│       ├── simulation.py # 10 Hz physical world simulation and diurnal kinetics
│       ├── mcp_server.py # MCP JSON-RPC Server (Port 8000)
│       ├── kafka_streamer.py # High-throughput Kafka producer with Ed25519 signing
│       ├── crypto_utils.py # Ed25519 signing, canonical JSON formatting, verification
│       ├── db_ledger.py  # SQLite ground-truth storage (isolated)
│       └── main.py       # Twin runtime entrypoint
├── mocks/                # Mock implementations for Layer 2 and Layer 3 partners
│   ├── mock_brain.py     # Mock City Brain actuator
│   └── mock_guardian.py  # Mock Guardian verifier and containment caller
├── tests/                # Automated pytest suite and contract locks
├── scripts/              # Documentation build and capture scripts
└── docs/                 # Guides, specifications, and architecture assets
```

---

## 3. Running the Twin Layer

### Using Docker Compose (Standalone Twin Ecosystem)
From the `twin/` directory:
```bash
docker compose up -d
```
Starts:
- Apache Kafka broker on `localhost:19092`
- Twin Layer engine on `http://localhost:8000/sse`
- Streamlit operations console on `http://localhost:8501`

### Running Locally with Python
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the test suite
python -m pytest -q

# 3. Launch the Twin engine (requires Kafka broker or set WAIT_FOR_KAFKA=false)
PYTHONPATH=src python -m twin.main

# 4. In a separate terminal, launch the Operations Console
PYTHONPATH=src streamlit run dashboard.py
```

### Endpoints and Ports
- **Kafka Telemetry:** `localhost:19092` (Topic: `twin.sensors`, 420 msgs/s)
- **MCP Server & REST:** `http://localhost:8000/sse` (MCP JSON-RPC) and `http://localhost:8000/api/status`
- **Operations Console:** `http://localhost:8501`
