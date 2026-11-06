# Triplet Integration Guide (Layer 1: Twin, Layer 2: Brain, Layer 3: Guardian)

This document describes the cross-layer communication architecture connecting the **Twin Layer (Layer 1)**, **City Brain (Layer 2)**, and **Guardian (Layer 3)**.

---

## 1. Architecture Overview

```
       ┌────────────────────────────────────────────────────────┐
       │                   Twin Layer (Layer 1)                 │
       │  ┌───────────────────────┐  ┌───────────────────────┐  │
       │  │ Physics Engine (10Hz) │  │  Ground Truth SQLite  │  │
       │  └──────────┬────────────┘  │     (Isolated)        │  │
       │             │               └───────────────────────┘  │
       └─────────────┼───────────────────────────▲──────────────┘
                     │ Outbound Telemetry        │ Synchronous RPC
                     │ (Ed25519 Signed)          │ (JSON-RPC)
                     ▼                           │
              ┌──────────────┐            ┌──────────────┐
              │ Kafka Broker │            │  MCP Server  │
              │(Apache Kafka)│            │  (Port 8000) │
              └──────┬───────┘            └──────▲───────┘
                     │                           │
          ┌──────────┴──────────┐     ┌──────────┴──────────┐
          ▼                     ▼     │                     │
    ┌───────────┐         ┌───────────┴─┐             ┌─────┴───────┐
    │City Brain │         │  Guardian   │             │ City Brain  │
    │ (Layer 2) │         │  (Layer 3)  │             │  (Layer 2)  │
    └───────────┘         └─────────────┘             └─────────────┘
```

---

## 2. Shared Endpoints & Configuration

| Service | Internal (Docker Network) | External (Host Machine) | Description |
| :--- | :--- | :--- | :--- |
| **Apache Kafka** | `kafka:9092` | `localhost:19092` | Telemetry topic: `twin.sensors` (420 msgs/s) |
| **Twin MCP Server** | `http://twin_layer:8000/sse` | `http://localhost:8000/sse` | Synchronous actuation, queries, and containment (SSE and `/mcp`) |

---

## 3. Layer Integration Checklists

### Layer 2: City Brain
1. **Consume Telemetry:** Connect a Kafka consumer to `localhost:19092` (or `kafka:9092` inside Docker) and subscribe to `twin.sensors`.
2. **Execute Actuation Commands:** Connect to the MCP Server (`http://localhost:8000/sse`) and call:
   - `dispatch(incident_type, zone_id)`: Deploy medical, fire, or police units.
   - `set_signal_plan(device_id, target_state)`: Switch traffic light programs (`green_wave`, `all_red`, `adaptive`).
   - `set_valve(device_id, target_state)`: Adjust flood drainage valves (`open`, `half`, `closed`).
   - `switch_grid(device_id, target_state)`: Switch substation power routing (`grid_a`, `grid_b`, `island`).
3. **Reference Implementation:** See `twin/mocks/mock_brain.py`.

### Layer 3: Guardian
1. **Consume Telemetry:** Subscribe to `twin.sensors` on `localhost:19092`.
2. **Verify Cryptographic Signatures:** Query MCP `get_device_key(device_id)` to retrieve the Ed25519 public key. Verify the `sig` field against the canonical JSON payload.
3. **Trigger Containment:** When an anomaly or poisoned reading is detected:
   - `quarantine_device(device_id)`: Blacklist and revoke device network authority.
   - `isolate_device(device_id)`: Isolate physical zone segment.
   - `rollback_reading(reading_id)`: Mark reading as rolled back in the system.
4. **Reference Implementation:** See `twin/mocks/mock_guardian.py`.

---

## 4. Contract Specifications
- **Interface Card:** See [twin/INTERFACE.md](twin/INTERFACE.md).
- **Full API Specification:** See [twin/docs/INTERFACE_REFERENCE.md](twin/docs/INTERFACE_REFERENCE.md).
- **Developer Guide:** See [twin/docs/DEVELOPER_GUIDE.md](twin/docs/DEVELOPER_GUIDE.md).
- **User Guide:** See [twin/docs/USER_GUIDE.md](twin/docs/USER_GUIDE.md).
