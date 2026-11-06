# Layer 1 Digital Twin Scenarios

All scenario scripts run as external clients communicating with Layer 1 strictly over HTTP (`/api` endpoints), Model Context Protocol (MCP over SSE on port 8000), and Apache Kafka telemetry streaming (`twin.sensors`).

> **Architectural Boundary:** Zero internal `twin.*` package imports in `scenarios/` or `mocks/`.

---

## Authentication & Caller Attribution Environment

To ensure full auditability across all three layers, the Twin runtime enforces bearer token authentication. Each principal authenticates with a distinct token:

```bash
# In twin/.env or execution environment:
MCP_AUTH_TOKENS="brain:tok-b,guardian:tok-g,scenario:tok-s"
RED_TEAM_TOKEN="red-team-secret"
```

When running scenarios with mocks (`--with-mocks`):
- **Mock Brain** connects with `Bearer tok-b` -> commands logged with caller `brain`.
- **Mock Guardian** connects with `Bearer tok-g` -> containments logged with caller `guardian`.
- **Scenario Runner** connects with `Bearer tok-s` -> queries/restorations logged with caller `scenario`.

---

## Single "Run All" Command

To execute all 11 scenarios sequentially against a live Twin runtime with live mock subprocesses:

```bash
python3 scenarios/run_all.py --with-mocks
```

To run against live partner implementations (City Brain and Guardian):

```bash
python3 scenarios/run_all.py --partners
```

---

## Command Flags

- `--with-mocks`: Starts `mocks.mock_guardian` and `mocks.mock_brain` as live subprocesses, authenticated via `--brain-token` and `--guardian-token`.
- `--no-brain`: Disables the Brain mock so incidents run unmitigated to verify natural physical consequences (e.g., flood breaching 8.0 m in `t3`, or grid overload escalating to blackout in `t5`).
- `--partners`: Observes external partner implementations. The scenario sends **zero actuation or containment commands**, reports which devices remain contained, and clears only its own injected attacks and events.
- `--twin-host`: Twin Red Team REST API endpoint (default: `http://localhost:8100`).
- `--mcp-url`: Twin MCP server endpoint (default: `http://localhost:8000`).
- `--kafka-bootstrap`: Kafka broker address (default: `localhost:19092`).

---

## Scenario Catalog & Conformance Matrix

| Scenario | Command | What It Proves | Sim Duration | Wall Time (`SIM_SPEED=20`) |
| :--- | :--- | :--- | :--- | :--- |
| **t1_baseline_stream** | `python3 scenarios/t1_baseline_stream.py --with-mocks` | 42 devices emit continuous telemetry, every payload <= 300 B, every Ed25519 signature verifies, nominal rate 400-440 msg/s. | 100 sim-s | 5.0 s |
| **t2_traffic_accident** | `python3 scenarios/t2_traffic_accident.py --with-mocks` | Accident on `z1-tc1` drops vehicle flow and spikes emergency calls; Brain issues `dispatch(medical_emergency)` and incident reaches `resolved_by`. | 60 sim-s | 3.0 s |
| **t3_port_flood** | `python3 scenarios/t3_port_flood.py --with-mocks` | 3x rain storm in `zone-2`; Brain commands `set_valve(open)` keeping water level < 8.0 m. With `--no-brain`, storm rises > 8.0 m after 15-20 sim-min. | 300-1800 sim-s | 15-90 s |
| **t4_fire** | `python3 scenarios/t4_fire.py --with-mocks` | Fire in `zone-5` drives AQI > 150; Brain commands `dispatch(fire)` and incident is resolved after 180 sim-s. | 180 sim-s | 9.0 s |
| **t5_grid_overload** | `python3 scenarios/t5_grid_overload.py --with-mocks` | High power load in `zone-3`; Brain triggers `switch_grid(shed)` preventing blackout cascade. With `--no-brain`, 60 sim-s overload escalates to blackout. | 60 sim-s | 3.0 s |
| **t6_fake_port_flood** | `python3 scenarios/t6_fake_port_flood.py --with-mocks` | Cyber spoofing on `z2-wl1` (9.5 m) with valid signature; Guardian detects paired-water divergence and isolates `z2-wl1`; Brain holds valve closed. | 60 sim-s | 3.0 s |
| **t7_stuck_sensor** | `python3 scenarios/t7_stuck_sensor.py --with-mocks` | Sensor fault stuck on `z4-aq1`; Guardian identifies 50 consecutive constant readings, isolates sensor, and rolls back reading. | 100 sim-s | 5.0 s |
| **t8_rogue_device** | `python3 scenarios/t8_rogue_device.py --with-mocks` | Rogue device `rg-p1` queries `get_device_key` (returns null); Guardian triggers `quarantine_device`; Twin runtime blocks subsequent frames. | 40 sim-s | 2.0 s |
| **t9_stolen_key** | `python3 scenarios/t9_stolen_key.py --with-mocks` | Stolen key on `z1-pl1` broadcasts out-of-bounds 180 MW power reading; Brain ignores implausible reading; Guardian triggers `quarantine_device` and `rollback_reading`. | 60 sim-s | 3.0 s |
| **t10_link_tamper** | `python3 scenarios/t10_link_tamper.py --mode all --with-mocks` | Link adversary on `z3-tc1` across all 5 modes (`corrupt_bytes`, `corrupt_sig`, `replay`, `delay`, `drop`); wire checks and corresponding Guardian containment. | 120 sim-s | 6.0 s |
| **t11_actuator_refusal** | `python3 scenarios/t11_actuator_refusal.py --with-mocks` | Cyber-physical actuator tampering on `z2-wl1`; `set_valve` returns ok but journal records `executed=false`; reported state open while water rises >= 0.3 m over 5 sim-min. | 300 sim-s | 15.0 s |

---

## Log Output Format

Every scenario run creates a JSON lines log in `logs/tN-<UTC stamp>.log`.
- Every entry has `wall_ts` and `sim_ts` (filled from `/api/status` at step time).
- Scenario lines and mock lines are interleaved in strict `wall_ts` chronological order.
- The final line of every log file contains the machine-verifiable outcome:

```json
{"wall_ts": "...", "sim_ts": "...", "layer": "scenario", "event": "result", "pass": true, "checks": [{"name": "...", "pass": true, "detail": "..."}]}
```
