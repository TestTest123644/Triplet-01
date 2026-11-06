# City Brain Convergence — Triplet-07

A 3-layer cyber-physical architecture for smart city simulation, autonomic decision-making, and cryptographic security monitoring in Greater Tunis.

---

## 1. Triplet Architecture

### Layer 1: Digital Twin & Physical World
Simulates the physical city environment and generates live telemetry across 42 IoT sensors in 6 zones at 10 Hz (420 messages per second). Each message is signed with a per-device Ed25519 key and broadcast to Apache Kafka on topic `twin.sensors`. The Twin also runs an MCP server on port 8000 to execute physical actuation and containment commands while keeping true physical states isolated in a local SQLite ledger.

### Layer 2: City Brain
Consumes live Kafka telemetry to assess municipal operational state across traffic, drainage, electrical grid, and public safety subsystems. When anomalies or capacity spikes occur, the Brain sends actuation commands through MCP to deploy emergency units, adjust traffic signal plans, open drainage valves, or reroute power feeders.

### Layer 3: Guardian
Monitors Kafka telemetry in parallel with the City Brain to protect municipal infrastructure from cyber-physical attacks. The Guardian verifies Ed25519 payload signatures using public keys queried from the Twin via MCP. When spoofing, replay attacks, or data corruption are detected, it issues containment commands to isolate or quarantine compromised devices and roll back bad telemetry.

---

## 2. Team Members

| Layer | Team | Members | Primary Contact |
| :--- | :--- | :--- | :--- |
| **Layer 1: Digital Twin** | Triplet-07 Twin Team | *[Placeholder - Twin Team]* | *[Contact]* |
| **Layer 2: City Brain** | Triplet-07 Brain Team | *[Placeholder - Brain Team]* | *[Contact]* |
| **Layer 3: Guardian** | Triplet-07 Guardian Team | *[Placeholder - Guardian Team]* | *[Contact]* |

---

## 3. Quickstart

All commands run from the repository root.

If a container named "kafka" already exists, remove or rename it first (`docker rm -f kafka` or `docker rename kafka kafka-old`).

To launch the triplet infrastructure (Kafka broker, Twin Layer simulation, and Operations Console):

```bash
# 1. Start infrastructure (pass SIM_SPEED=20 for scenario pace):
SIM_SPEED=20 docker compose up -d --build

# 2. Export the red-team token generated inside the container:
export RED_TEAM_TOKEN=$(docker exec twin_layer cat /app/data/red_team.token)

# 3. Run an incident scenario with mock partners:
python3 scenarios/t5_grid_overload.py --with-mocks
```

### Shared Service Endpoints
- **Apache Kafka:** `localhost:19092` (Topic: `twin.sensors`, pre-existing, supports comma-separated brokers)
- **Twin MCP Server:** `http://localhost:8000/sse` and `/mcp`
- **Red-Team Adversary API:** `http://localhost:8100/api/status`
- **Twin Operations Console:** `http://localhost:8501`

---

## 4. Documentation & Repository Links

- **Layer 1 Implementation & Local Setup:** [twin/README.md](twin/README.md)
- **Layer 2 Workspace:** [brain/](brain/)
- **Layer 3 Workspace:** [guardian/](guardian/)
- **Cross-Layer Integration Guide:** [INTEGRATION.md](INTEGRATION.md)
- **Shared Test Scenarios & Verification:** [TESTBOOK.md](TESTBOOK.md)
- **Twin Interface Specification:** [twin/INTERFACE.md](twin/INTERFACE.md)
