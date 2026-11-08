# Layer 1 Digital Twin Scenarios

All scenario scripts run as external clients communicating with Layer 1 strictly over HTTP (`/api` endpoints), Model Context Protocol (MCP over SSE on port 8000 or HTTP `/mcp`), and Apache Kafka telemetry streaming (`twin.sensors`).

> **Architectural Boundary:** Zero internal `twin.*` package imports in `scenarios/` or `mocks/`.

---

## 1. Authentication & Caller Attribution Environment

To ensure full auditability across all three layers, the Twin runtime enforces bearer token authentication. Each principal authenticates with a distinct token:

```bash
# In twin/.env or execution environment:
export MCP_AUTH_TOKENS="brain:tok-b,guardian:tok-g,scenario:tok-s"
export RED_TEAM_TOKEN="$(docker exec twin_layer cat /app/data/red_team.token)"
```

When running scenarios with mocks (`--with-mocks`):
- **Mock Brain** connects with `Bearer tok-b` -> commands logged with caller `brain`.
- **Mock Guardian** connects with `Bearer tok-g` -> containments logged with caller `guardian`.
- **Scenario Runner** connects with `Bearer tok-s` -> queries/restorations logged with caller `scenario`.

Environment variables accepted by the scenario harness:
- `RED_TEAM_TOKEN` or `--red-team-token`: Secret token for Red Team REST API.
- `MCP_AUTH_TOKEN` or `MCP_SCENARIO_TOKEN`: Bearer token used by the scenario runner (default: `tok-s`).
- `MCP_BRAIN_TOKEN`: Bearer token assigned to the mock Brain subprocess (default: `tok-b`).
- `MCP_GUARDIAN_TOKEN`: Bearer token assigned to the mock Guardian subprocess (default: `tok-g`).
- `KAFKA_BOOTSTRAP_SERVERS` or `--kafka-bootstrap`: Kafka broker address (default: `localhost:19092`).

---

## 2. Deterministic Scenario Settle Step

Every scenario starts with a deterministic settle step (`ctx.settle()`), bringing the simulated city to a known quiet baseline before any scenario assertions execute. In addition, `t10_link_tamper.py` executes `ctx.settle()` upon completion to guarantee that delayed frame buffers are flushed before subsequent scenarios begin:

1. **Clear active threats:** Calls `/api/attack/clear` and `/api/event/clear` to flush all active adversary attacks and physical events.
2. **Drain delay backlog:** Waits until `delayed_frames_pending == 0`. When a delay attack is cleared, all held frames are emitted immediately in FIFO order to the wire.
3. **Reset actuators:** Commands traffic cameras back to `normal` and power loads back to `grid_a`.
4. **Reinstate devices:** Reinstates any isolated or quarantined devices as caller `scenario`.
5. **Drain flood water:** Monitors water level sensors until readings drop below 5.0 m (opening flood valves while waiting, then restoring them to `half`).
6. **Verify quiet baseline:** Confirms that no new containment or command events appear in the journal for at least 10 simulation seconds.
7. **Attribution boundary:** Scenario false-positive guards filter out journal entries prior to settle completion (`sim_ts <= settle_end_sim_time`), preventing cross-scenario false alarms.

---

## 3. Running Scenarios

### Single "Run All" Command (Automated Conformance)
To execute all 11 scenarios sequentially against a running Twin runtime with live mock subprocesses:

```bash
python3 scenarios/run_all.py --with-mocks
```

To run against live partner implementations (City Brain and Guardian):

```bash
python3 scenarios/run_all.py --partners
```

### Running Against Guardian SASL Cluster from Host

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:29094,localhost:29095,localhost:29096
export KAFKA_TOPIC=telemetry-ingest
export SCENARIO_KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
export SCENARIO_KAFKA_USERNAME=<a user with Read on the topic and on group prefix scenario-runner>
export SCENARIO_KAFKA_PASSWORD_FILE=secrets/<file>
python3 scenarios/run_all.py --partners
```

The Twin's user `twin` is write-only on that cluster, so the cluster owner must grant a reader (topic Read + Describe, group prefix Read).

### Command Flags

- `--with-mocks`: Starts `mocks.mock_guardian` and `mocks.mock_brain` as live subprocesses, authenticated via `--brain-token` and `--guardian-token`. Both mocks monitor parent process liveness (`--exit-with-parent`) and terminate cleanly on exit.
- `--no-brain`: Disables the Brain mock so incidents run unmitigated to verify natural physical consequences (e.g., flood breaching 8.0 m in `t3`, or grid overload escalating to blackout in `t5`).
- `--partners`: Observes external partner implementations. The scenario sends **zero actuation or containment commands**, reports which devices remain contained, and clears only its own injected attacks and events.
- `--twin-host`: Twin Red Team REST API endpoint (default: `http://localhost:8100`).
- `--mcp-url`: Twin MCP server endpoint (default: `http://localhost:8000`).
- `--kafka-bootstrap`: Kafka broker address (default: `localhost:19092`).

---

## 4. Scenario Catalog & Conformance Matrix

| Scenario | Command | What It Proves & Checks | Sim Duration | Wall Time (`SIM_SPEED=20`) |
| :--- | :--- | :--- | :--- | :--- |
| **t1_baseline_stream** | `python3 scenarios/t1_baseline_stream.py --with-mocks` | All 42 devices emit continuous telemetry; every payload <= 300 B; all Ed25519 signatures verify; message rate nominal (~400-440 msg/s); zero false-positive containments. | 100 sim-s | 5.0 s |
| **t2_traffic_accident** | `python3 scenarios/t2_traffic_accident.py --with-mocks` | Accident on `z1-tc1` reduces vehicle throughput and triggers emergency calls; Brain issues `dispatch(medical_emergency)` to `zone-1`; incident reaches `resolved_by: dispatch`. | 60 sim-s | 3.0 s |
| **t3_port_flood** | `python3 scenarios/t3_port_flood.py --with-mocks` | 3x rain storm in `zone-2`; Brain commands `set_valve(open)` on `z2-wl1`, mitigating flood peak safely below 8.0 m. With `--no-brain`, water breaches 8.0 m. | 300-1800 sim-s | 15-90 s |
| **t4_fire** | `python3 scenarios/t4_fire.py --with-mocks` | Fire in `zone-5` drives AQI > 150; Brain commands `dispatch(fire)` to `zone-5`; incident is resolved after response time. | 180 sim-s | 9.0 s |
| **t5_grid_overload** | `python3 scenarios/t5_grid_overload.py --with-mocks` | High power load in `zone-3`; Brain triggers `switch_grid(shed)` on `z3-pl1`, preventing blackout cascade. With `--no-brain`, continuous load > 114 MW triggers blackout. | 60 sim-s | 3.0 s |
| **t6_fake_port_flood** | `python3 scenarios/t6_fake_port_flood.py --with-mocks` | Physical spoofing on `z2-wl1` (9.5 m) with valid signature; Guardian detects paired-water divergence against nominal counterpart `z2-wl2` (< 5.0 m) and isolates `z2-wl1`; Brain refrains from actuation. | 60 sim-s | 3.0 s |
| **t7_stuck_sensor** | `python3 scenarios/t7_stuck_sensor.py --with-mocks` | Sensor fault stuck on `z4-aq1`; Guardian identifies frozen constant readings, isolates sensor, and rolls back reading. | 100 sim-s | 5.0 s |
| **t8_rogue_device** | `python3 scenarios/t8_rogue_device.py --with-mocks` | Unauthorized hardware `rg-p1` appears on wire; `get_device_key` returns null; Guardian quarantines device; Twin blocks subsequent frames on wire. | 40 sim-s | 2.0 s |
| **t9_stolen_key** | `python3 scenarios/t9_stolen_key.py --with-mocks` | Stolen key on `z1-pl1` broadcasts implausible 180 MW reading; Brain ignores outlier; Guardian quarantines sensor and rolls back poisoned reading. | 60 sim-s | 3.0 s |
| **t10_link_tamper** | `python3 scenarios/t10_link_tamper.py --mode all --with-mocks` | Tests all 5 link adversary modes (`corrupt_bytes`, `corrupt_sig`, `replay`, `delay`, `drop`) on `z3-tc1`; verifies wire corruption, timestamp lag, and Guardian containment; flushes delay buffer via settle on exit. | 120 sim-s | 6.0 s |
| **t11_actuator_refusal** | `python3 scenarios/t11_actuator_refusal.py --with-mocks` | Actuator tampering on `z2-wl1`; `set_valve` returns status ok to caller but journal records `executed=false, refused_code=ACTUATOR_ATTACK`; physical water level rises >= 0.3 m over 5 sim-min despite reported open position. | 300 sim-s | 15.0 s |

---

## 5. Log Output Format

Every scenario run creates a JSON lines log in `logs/tN-<UTC stamp>.log`.
- Every entry includes `wall_ts` and `sim_ts` (fetched from `/api/status`).
- Scenario runner events and mock logs are interleaved in strict `wall_ts` chronological order.
- The final line of every log file contains the machine-verifiable outcome:

```json
{"wall_ts": "...", "sim_ts": "...", "layer": "scenario", "event": "result", "pass": true, "checks": [{"name": "...", "pass": true, "detail": "..."}]}
```
