# City Brain Convergence - Triplet Testbook

This testbook documents end-to-end integration and demonstration scenarios for the smart city platform.
Each scenario verifies cyber-physical realism, adversarial resilience, and multi-layer coordination across Layer 1 (Digital Twin), Layer 2 (City Brain), and Layer 3 (Guardian).

---

## Authentication & Verification Setup

All scenarios and mocks execute as authenticated clients against the Twin runtime.
Audit attribution in the ground-truth journal is strictly configured via:

```bash
export MCP_AUTH_TOKENS="brain:tok-b,guardian:tok-g,scenario:tok-s"
export RED_TEAM_TOKEN="red-team-secret"
```

Principals and credentials:
- **City Brain (Layer 2):** `Bearer tok-b` -> caller attribute `brain`
- **Guardian (Layer 3):** `Bearer tok-g` -> caller attribute `guardian`
- **Scenario Runner:** `Bearer tok-s` -> caller attribute `scenario`

---

## Twin Test Suite (Layer 1)

### T1: Baseline Telemetry Stream Conformance
- **ID:** T1
- **Name:** Baseline Telemetry Stream Conformance
- **Layers involved:** Layer 1 (Twin), Layer 3 (Guardian)
- **Description:** Verifies high-throughput nominal telemetry streaming across all 42 registered municipal devices without active adversary interference.
- **Trigger:** Continuous physics simulation tick loop with nominal baseline environment.
- **Expected Twin output:**
  - 42 registered devices actively publishing to Kafka topic `twin.sensors`.
  - Every telemetry payload strictly within 300 bytes.
  - Every payload cryptographically signed with valid Ed25519 signature verifiable against MCP `get_device_key`.
  - Steady message throughput between 400 and 440 msg/s at 10 Hz wall pacing.
  - Zero device containments and zero actuation commands in ground-truth ledger.
- **Expected Guardian output:** Telemetry accepted and validated; zero containments or flags (to confirm with partner team).
- **Expected Brain output:** Observes normal city flow; zero emergency actuations (to confirm with partner team).
- **How to verify:** Run `python3 scenarios/t1_baseline_stream.py --with-mocks`; verify `logs/t1-*.log` ends with `{"event":"result","pass":true}`.

---

### T2: Traffic Accident Incident Response
- **ID:** T2
- **Name:** Traffic Accident Incident Response
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain)
- **Description:** A severe multi-vehicle collision occurs on arterial road camera `z1-tc1`, dropping traffic flow while emergency calls spike.
- **Trigger:** Red Team API `POST /api/event/inject` with `{"type": "traffic_accident", "zone_id": "zone-1", "device_id": "z1-tc1", "intensity": 2.0, "duration_s": 600.0}`.
- **Expected Twin output:**
  - `z1-tc1` traffic vehicle count drops significantly.
  - `z1-ec1` emergency call count surges (>= 6 calls).
  - Ground-truth journal records incident active and logs actuation dispatch command.
  - Incident reaches `resolved` status with `resolved_by: dispatch:...`.
- **Expected Guardian output:** Passive observation; telemetry authentic (to confirm with partner team).
- **Expected Brain output:** Corroborates camera collapse and emergency call surge; invokes MCP `dispatch(incident_type="medical_emergency", zone_id="zone-1")` (to confirm with partner team).
- **How to verify:** Run `python3 scenarios/t2_traffic_accident.py --with-mocks`; verify `logs/t2-*.log` ends with `{"event":"result","pass":true}`.

---

### T3: Port Flood Storm and Valve Mitigation
- **ID:** T3
- **Name:** Port Flood Storm and Valve Mitigation
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain)
- **Description:** Severe 3x rain storm hits coastal zone-2. Without Brain intervention, harbor water levels breach the 8.0 m danger threshold. With Brain intervention, drainage sluice valves open to mitigate flood surge.
- **Trigger:** Red Team API `POST /api/event/inject` with `{"type": "rain_storm", "zone_id": "zone-2", "intensity": 3.0, "duration_s": 1800.0}`.
- **Expected Twin output:**
  - Both `z2-wl1` and `z2-wl2` water sensors record rapid water level increase.
  - Under unmitigated condition (`--no-brain`), peak level exceeds 8.0 m.
  - Under mitigated condition (`--with-mocks`/`--partners`), journal records MCP command `set_valve(open)` on `z2-wl1`/`z2-wl2` and water level stabilizes below 8.0 m.
- **Expected Guardian output:** Both paired sensors track concordantly; no false positive divergence isolation (to confirm with partner team).
- **Expected Brain output:** Corroborates rising water levels > 7.0 m across paired sensors; commands MCP `set_valve(device_id="z2-wl1", target_state="open")` (to confirm with partner team).
- **How to verify:** Run `python3 scenarios/t3_port_flood.py --with-mocks`; verify `logs/t3-*.log` ends with `{"event":"result","pass":true}`.

---

### T4: Zone Fire Incident and Emergency Dispatch
- **ID:** T4
- **Name:** Zone Fire Incident and Emergency Dispatch
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain)
- **Description:** Urban fire breaks out in industrial zone-5, generating heavy particulate smoke and driving Air Quality Index (AQI) above dangerous thresholds.
- **Trigger:** Red Team API `POST /api/event/inject` with `{"type": "fire", "zone_id": "zone-5", "intensity": 2.0, "duration_s": 1200.0}`.
- **Expected Twin output:**
  - `z5-aq1` particulate levels and AQI spike above 150.
  - Ground-truth journal records MCP tool execution `dispatch(incident_type="fire", zone_id="zone-5")`.
  - After 180 sim-seconds of emergency firefighting, event transitions to `resolved` with `resolved_by: dispatch:...`.
- **Expected Guardian output:** Telemetry authentic; no containment actions (to confirm with partner team).
- **Expected Brain output:** Detects sustained hazardous AQI > 150; commands MCP `dispatch(incident_type="fire", zone_id="zone-5")` (to confirm with partner team).
- **How to verify:** Run `python3 scenarios/t4_fire.py --with-mocks`; verify `logs/t4-*.log` ends with `{"event":"result","pass":true}`.

---

### T5: Grid Overload Incident and Automated Load Shedding
- **ID:** T5
- **Name:** Grid Overload Incident and Automated Load Shedding
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain)
- **Description:** Sudden industrial surge causes power substation in zone-3 to overload beyond 108 MW. Brain must shed non-critical load via MCP `switch_grid` before a blackout occurs.
- **Trigger:** Red Team API `POST /api/event/inject` with `{"type": "grid_overload", "zone_id": "zone-3", "intensity": 1.5, "duration_s": 600.0}`.
- **Expected Twin output:**
  - `z3-pl1` power load rises above 108 MW.
  - With Brain: Journal logs MCP command `switch_grid(device_id="z3-pl1", target_state="shed")`; power drops to nominal and incident resolves without blackout.
  - Without Brain (`--no-brain`): Overload exceeds 60 sim-seconds tolerance; Twin engine escalates incident to blackout and journals `event_escalated:<event_id>` with `action: escalated, to: blackout`.
- **Expected Guardian output:** Telemetry signatures valid; no containment (to confirm with partner team).
- **Expected Brain output:** Identifies critical electrical overload > 108 MW; issues MCP `switch_grid(target_state="shed")` (to confirm with partner team).
- **How to verify:** Run `python3 scenarios/t5_grid_overload.py --with-mocks`; verify `logs/t5-*.log` ends with `{"event":"result","pass":true}`.

---

### T6: Fake Port Flood Sensor Spoofing Defense
- **ID:** T6
- **Name:** Fake Port Flood Sensor Spoofing Defense
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain), Layer 3 (Guardian)
- **Description:** Adversary gains physical access to coastal water sensor `z2-wl1` and tampers its reading to report a catastrophic 9.5 m flood while counterpart `z2-wl2` reports nominal tide.
- **Trigger:** Red Team API `POST /api/event/inject` with `{"capability": "device_physical", "target_device_id": "z2-wl1", "params": {"value": 9.5}}`.
- **Expected Twin output:**
  - `z2-wl1` emits 9.5 m on wire signed with authentic device key.
  - Counterpart `z2-wl2` emits nominal level (~2.0 m).
  - Ground-truth journal records Guardian MCP call `isolate_device(device_id="z2-wl1")` and `rollback_reading`.
  - Brain does NOT open the drainage valve (`set_valve`) and does NOT dispatch emergency flood services.
- **Expected Guardian output:** Cross-corroborates paired water sensors; identifies > 2.0 m divergence for 20 readings; invokes MCP `isolate_device(device_id="z2-wl1")` and rolls back spoofed readings (to confirm with partner team).
- **Expected Brain output:** Respects isolated status of `z2-wl1`; refrains from actuation (to confirm with partner team).
- **How to verify:** Run `python3 scenarios/t6_fake_port_flood.py --with-mocks`; verify `logs/t6-*.log` ends with `{"event":"result","pass":true}`.

---

### T7: Stuck Sensor Fault Detection and Isolation
- **ID:** T7
- **Name:** Stuck Sensor Fault Detection and Isolation
- **Layers involved:** Layer 1 (Twin), Layer 3 (Guardian)
- **Description:** Hardware failure causes air quality sensor `z4-aq1` to freeze and emit an identical value for 50 consecutive ticks.
- **Trigger:** Red Team API `POST /api/event/inject` with `{"capability": "sensor_fault", "target_device_id": "z4-aq1", "params": {"fault_mode": "stuck", "stuck_value": 42.0}}`.
- **Expected Twin output:**
  - `z4-aq1` outputs exactly 42.00 on wire repeatedly.
  - Ground-truth journal records Guardian MCP call `isolate_device(device_id="z4-aq1")` and `rollback_reading`.
  - Device status transitions to `isolated` in Twin state.
- **Expected Guardian output:** Flags stuck sensor after 50 identical readings; invokes MCP `isolate_device` and `rollback_reading` (to confirm with partner team).
- **Expected Brain output:** Ignores isolated sensor in air quality aggregation (to confirm with partner team).
- **How to verify:** Run `python3 scenarios/t7_stuck_sensor.py --with-mocks`; verify `logs/t7-*.log` ends with `{"event":"result","pass":true}`.

---

### T8: Rogue Unregistered Device Detection and Quarantine
- **ID:** T8
- **Name:** Rogue Unregistered Device Detection and Quarantine
- **Layers involved:** Layer 1 (Twin), Layer 3 (Guardian)
- **Description:** An unauthorized rogue device `rg-p1` injects rogue telemetry into Kafka topic `twin.sensors`.
- **Trigger:** Red Team API `POST /api/event/inject` with `{"capability": "rogue", "params": {"rogue_device_id": "rg-p1", "zone_id": "zone-2", "device_type": "power_load", "value": 88.88}}`.
- **Expected Twin output:**
  - Rogue telemetry envelopes appear on wire signed with untrusted throwaway key.
  - MCP query `get_device_key("rg-p1")` returns `{"public_key": null}`.
  - Journal records Guardian MCP call `quarantine_device(device_id="rg-p1")`.
  - Twin simulation drops subsequent rogue frames (`blocked_frames.rg-p1 > 0`).
- **Expected Guardian output:** Queries MCP `get_device_key("rg-p1")`; receives null; invokes MCP `quarantine_device(device_id="rg-p1")` (to confirm with partner team).
- **Expected Brain output:** Disregards unauthenticated rogue telemetry (to confirm with partner team).
- **How to verify:** Run `python3 scenarios/t8_rogue_device.py --with-mocks`; verify `logs/t8-*.log` ends with `{"event":"result","pass":true}`.

---

### T9: Compromised Device Key Attack and Cryptographic Quarantine
- **ID:** T9
- **Name:** Compromised Device Key Attack and Cryptographic Quarantine
- **Layers involved:** Layer 1 (Twin), Layer 3 (Guardian)
- **Description:** Attacker extracts private key of power sensor `z1-pl1` and broadcasts malicious out-of-bounds readings that cryptographically verify against the legitimate public key.
- **Trigger:** Red Team API `POST /api/event/inject` with `{"capability": "device_key", "target_device_id": "z1-pl1", "params": {"spoofed_value": 180.0}}`.
- **Expected Twin output:**
  - `z1-pl1` emits 180 MW on wire with valid cryptographic Ed25519 signature.
  - Ground-truth journal records Guardian MCP containment `quarantine_device(device_id="z1-pl1")` and `rollback_reading`.
- **Expected Guardian output:** Validates cryptographic signature but identifies physical violation (> 140 MW); invokes MCP `quarantine_device` and `rollback_reading` (to confirm with partner team).
- **Expected Brain output:** Disregards quarantined device; does not trigger false grid shedding (to confirm with partner team).
- **How to verify:** Run `python3 scenarios/t9_stolen_key.py --with-mocks`; verify `logs/t9-*.log` ends with `{"event":"result","pass":true}`.

---

### T10: Link-Layer Adversary Tampering Modes
- **ID:** T10
- **Name:** Link-Layer Adversary Tampering Modes
- **Layers involved:** Layer 1 (Twin), Layer 3 (Guardian)
- **Description:** Evaluates link-level network tampering against traffic camera `z3-tc1` across five adversarial modes: payload corruption (`corrupt_bytes`), signature corruption (`corrupt_sig`), duplicate reading IDs (`replay`), latency distortion (`delay`), and packet annihilation (`drop`).
- **Trigger:** Red Team API `POST /api/event/inject` with `{"capability": "link", "target_device_id": "z3-tc1", "params": {"link_mode": "<mode>"}}`.
- **Expected Twin output:**
  - `corrupt_bytes`: Wire payload contains corrupted bytes; Guardian quarantine recorded.
  - `corrupt_sig`: Signature mathematical verification fails; Guardian quarantine recorded.
  - `replay`: Repeating reading IDs seen twice on wire; Guardian isolation recorded.
  - `delay`: Frame timestamp lags stream head; Guardian isolation recorded.
  - `drop`: Zero wire frames received for `z3-tc1`.
- **Expected Guardian output:** Quarantines or isolates `z3-tc1` according to violated protocol invariant (to confirm with partner team).
- **Expected Brain output:** Ignores contained or missing camera without issuing false emergency dispatches (to confirm with partner team).
- **How to verify:** Run `python3 scenarios/t10_link_tamper.py --mode all --with-mocks`; verify `logs/t10-*.log` ends with `{"event":"result","pass":true}`.

---

### T11: Cyber-Physical Actuator Tampering and Deceit Detection
- **ID:** T11
- **Name:** Cyber-Physical Actuator Tampering and Deceit Detection
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain), Layer 3 (Guardian)
- **Description:** Adversary tampers with the physical actuator of flood valve `z2-wl1` during a storm. An actuation command returns success to the caller and updates reported state to "open", but the ground-truth physical valve remains half-open, causing water levels to continue rising.
- **Trigger:** Red Team API `POST /api/event/inject` storm on `zone-2` and `POST /api/event/inject` with `{"capability": "actuator", "target_device_id": "z2-wl1"}`.
- **Expected Twin output:**
  - MCP tool call `set_valve(device_id="z2-wl1", target_state="open")` returns `{"status": "ok", "valve_position": "open"}`.
  - Ground-truth journal records command execution `executed: false` and `refused_code: "ACTUATOR_ATTACK"`.
  - MCP query `get_actuator_state("z2-wl1")` reports `valve_position: "open"` (actuator deceit).
  - True physical water level continues rising by >= 0.3 m over 5 sim-minutes because the valve remains half-open.
- **Expected Guardian output:** Cross-correlates reported actuator state with physical water level response to flag deceit (to confirm with partner team).
- **Expected Brain output:** Re-evaluates water level telemetry and diagnoses actuator discrepancy (to confirm with partner team).
- **How to verify:** Run `python3 scenarios/t11_actuator_refusal.py --with-mocks`; verify `logs/t11-*.log` ends with `{"event":"result","pass":true}`.

---

## Guardian Layer & Cross-Layer Zero-Trust Test Suite (Layer 3)

The Guardian layer verifies telemetry integrity through deterministic static bounds, spatial peer correlation, short-window wavelet ML (XGBoost), and 24-hour long-window temporal drift analysis.

### Zero-Trust Verification Architecture
1. **Telemetry Egress:** Digital Twin (Layer 1) broadcasts signed sensor telemetry to the message broker (`telemetry-validated` / `twin.sensors`).
2. **Brain Gating:** City Brain (Layer 2) ingests telemetry but suspends execution in `WAITING_FOR_GUARDIAN_TRUST` state pending cryptographic trust validation.
3. **Guardian Anomaly Scoring:** Guardian (Layer 3) processes flushed batches through:
   - Static range and signal silence checks.
   - Spatial cluster peer divergence checks.
   - AI Short-Window Haar Wavelet energy feature extraction & XGBoost classifier.
   - AI Long-Window 24-hour hourly trend evaluation.
4. **Trust Verdict Publication:** Guardian signs verdicts with Ed25519 identity keys and publishes to `telemetry-confirmation`:
   - `state='TRUSTED', trusted=True` -> Twin logs `'trusted message'`.
   - `state='UNTRUSTED', trusted=False` -> Twin logs `'not trusted'`.
5. **Brain Execution:**
   - On `TRUSTED`: Brain releases gate and executes autonomous municipal operations (`OPERATIONAL_DECISION_EXECUTED`).
   - On `UNTRUSTED`: Brain rejects corrupt data and triggers automated municipal defensive relief (`FLOOD_RELIEF_ACTUATED`, `MAINTENANCE_DISPATCHED`, `REPAIR_DISPATCHED`, `SECURITY_CONTAINMENT_VERIFIED`).
6. **Physical Containment:** Guardian ActionTaker executes autonomous containment commands (`quarantine_device` / `isolate_device`) against the Twin Layer via MCP JSON-RPC.

---

### G1: Nominal Stream Operation
- **ID:** G1 (`scenario_1_normal`)
- **Name:** Nominal Stream Operation [BASELINE]
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain), Layer 3 (Guardian)
- **Target Device:** `z1-wl1` (Water Level Sensor)
- **Category:** `BASELINE`
- **Description:** Clean, nominal telemetry stream within expected physical thresholds. Verifies baseline ML inference and dual PostgreSQL/SQLite persistence.
- **Trigger:** Normal telemetry batch generation (100 readings, nominal value range).
- **Expected Twin output:**
  - Emits 100 nominal readings to broker.
  - Consumes `telemetry-confirmation` with `trusted=True` -> logs `'trusted message'`.
  - Device status remains `ok`.
- **Expected Guardian output:**
  - Detected anomalies: `0`.
  - ML short-window anomaly score ~ `0.000` (`is_anomaly=False`).
  - Persists 100 raw readings and `ModelOutput` record in database.
  - Publishes signed confirmation: `state='TRUSTED', trusted=True`.
- **Expected Brain output:**
  - Releases `WAITING_FOR_GUARDIAN_TRUST` gate.
  - Executes municipal optimization actuation: `OPERATIONAL_DECISION_EXECUTED`.
- **How to verify:** Run `python3 scenarios/run_scenarios.py`; verify Scenario 1 logs `PASS [✓]`.

---

### G2: Physical Range Breach (Static Check)
- **ID:** G2 (`scenario_2_out_of_range`)
- **Name:** Physical Range Breach [STATIC_CHECK]
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain), Layer 3 (Guardian)
- **Target Device:** `z1-wl1` (Water Level Sensor)
- **Category:** `STATIC_CHECK`
- **Description:** Sensor reading breaches physical upper envelope (15.5 m > 12.0 m maximum range). Evaluates instantaneous boundary policing.
- **Trigger:** Telemetry batch with value 15.5 m exceeding upper range.
- **Expected Twin output:**
  - Emits 100 out-of-range readings to broker.
  - Consumes `telemetry-confirmation` with `trusted=False` -> logs `'not trusted'`.
  - Receives MCP command `quarantine_device(device_id="z1-wl1")`; transitions status to `quarantined`.
- **Expected Guardian output:**
  - Static boundary check fires: `Anomalies.OUT_OF_RANGE`.
  - Stores ModelOutput record (`score=1.000`, `is_anomaly=True`).
  - Publishes signed confirmation: `state='UNTRUSTED', trusted=False`.
  - ActionTaker issues MCP call `quarantine_device(target="z1-wl1")`.
- **Expected Brain output:**
  - Gate releases on untrusted verdict.
  - Triggers emergency flood safety mitigation: `FLOOD_RELIEF_ACTUATED`.
- **How to verify:** Run `python3 scenarios/run_scenarios.py`; verify Scenario 2 logs `PASS [✓]`.

---

### G3: Sensor Silence / Loss of Signal (Static Check)
- **ID:** G3 (`scenario_3_sensor_is_down`)
- **Name:** Sensor Silence / Loss of Signal [STATIC_CHECK]
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain), Layer 3 (Guardian)
- **Target Device:** `z2-wl2` (Water Level Sensor)
- **Category:** `STATIC_CHECK`
- **Description:** Sensor telemetric transmission halts or packets lag wall clock by >60 seconds. Tests watchdog heartbeat and liveness detection.
- **Trigger:** Telemetry batch with stale timestamps (>60s delta) followed by silence.
- **Expected Twin output:**
  - Sensor stream stops publishing.
  - Receives MCP command `quarantine_device(device_id="z2-wl2")`; transitions status to `quarantined`.
- **Expected Guardian output:**
  - Watchdog flags `Anomalies.SENSOR_IS_DOWN`.
  - Publishes signed confirmation: `state='UNTRUSTED', trusted=False`.
  - ActionTaker issues MCP call `quarantine_device(target="z2-wl2")`.
- **Expected Brain output:**
  - Detects sensor failure / untrusted signal.
  - Dispatches field maintenance crew: `MAINTENANCE_DISPATCHED`.
- **How to verify:** Run `python3 scenarios/run_scenarios.py`; verify Scenario 3 logs `PASS [✓]`.

---

### G4: Spatial Cluster Spoofing (Spatial Check)
- **ID:** G4 (`scenario_4_spoofed_data`)
- **Name:** Spatial Cluster Spoofing [SPATIAL_CHECK]
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain), Layer 3 (Guardian)
- **Target Device:** `z1-wl1` (Water Level Sensor)
- **Category:** `SPATIAL_CHECK`
- **Description:** Adversary tampers with a single sensor while nearby cluster peers report nominal environmental conditions. Evaluates cluster covariance and peer residual checks.
- **Trigger:** Telemetry batch where `z1-wl1` reading diverges significantly from its spatial peer cluster.
- **Expected Twin output:**
  - Emits diverged reading on wire.
  - Receives MCP command `isolate_device(device_id="z1-wl1")`; transitions status to `isolated`.
- **Expected Guardian output:**
  - Spatial correlation pipeline flags `Anomalies.SPOOFED_DATA`.
  - Publishes signed confirmation: `state='UNTRUSTED', trusted=False`.
  - ActionTaker issues MCP call `isolate_device(target="z1-wl1")`.
- **Expected Brain output:**
  - Ignores diverged spoofed reading; validates isolation: `SECURITY_CONTAINMENT_VERIFIED`.
- **How to verify:** Run `python3 scenarios/run_scenarios.py`; verify Scenario 4 logs `PASS [✓]`.

---

### G5: Hardware Malfunction & Noise Burst (AI Short-Window)
- **ID:** G5 (`scenario_5_sensor_malfunction`)
- **Name:** Hardware Malfunction & Noise Burst [AI_INFERENCE]
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain), Layer 3 (Guardian)
- **Target Device:** `z3-tc1` (Traffic Camera Sensor)
- **Category:** `AI_INFERENCE`
- **Description:** Hardware degradation generates high-frequency jitter and electrical noise bursts. Evaluates 100-sample Haar wavelet multi-resolution energy subbands scored by XGBoost classifier.
- **Trigger:** Telemetry batch exhibiting high-frequency Gaussian noise burst and jitter.
- **Expected Twin output:**
  - Emits erratic noisy packets on wire.
  - Receives MCP command `quarantine_device(device_id="z3-tc1")`; transitions status to `quarantined`.
- **Expected Guardian output:**
  - Haar wavelet feature extraction detects abnormal high-frequency detail band energy.
  - XGBoost ShortAnomalyDetector classifies `Anomalies.SENSOR_MALFUNCTION`.
  - Stores ModelOutput record with `model_version='short_window'`.
  - Publishes signed confirmation: `state='UNTRUSTED', trusted=False`.
  - ActionTaker issues MCP call `quarantine_device(target="z3-tc1")`.
- **Expected Brain output:**
  - Dispatches automated diagnostic and repair task: `REPAIR_DISPATCHED`.
- **How to verify:** Run `python3 scenarios/run_scenarios.py`; verify Scenario 5 logs `PASS [✓]`.

---

### G6: Non-Physical Bitstream Inversion (AI Short-Window)
- **ID:** G6 (`scenario_6_not_sensor_data`)
- **Name:** Non-Physical Bitstream Inversion [AI_INFERENCE]
- **Layers involved:** Layer 1 (Twin), Layer 2 (Brain), Layer 3 (Guardian)
- **Target Device:** `z4-tc1` (Traffic Camera Sensor)
- **Category:** `AI_INFERENCE`
- **Description:** Adversary injects synthetic, square-wave bitstream inversions lacking natural physical entropy. Evaluated by XGBoost as non-sensor payload.
- **Trigger:** Telemetry batch containing synthetic rectangular waveform patterns.
- **Expected Twin output:**
  - Emits inverted synthetic payloads.
  - Receives MCP command `isolate_device(device_id="z4-tc1")`; transitions status to `isolated`.
- **Expected Guardian output:**
  - Wavelet energy and kurtosis features deviate completely from physical sensors.
  - XGBoost classifier scores `Anomalies.NOT_SENSOR_DATA` (`score=1.000`).
  - Publishes signed confirmation: `state='UNTRUSTED', trusted=False`.
  - ActionTaker issues MCP call `isolate_device(target="z4-tc1")`.
- **Expected Brain output:**
  - Confirms security threat isolation: `SECURITY_CONTAINMENT_VERIFIED`.
- **How to verify:** Run `python3 scenarios/run_scenarios.py`; verify Scenario 6 logs `PASS [✓]`.

---

### G7: Hourly Database Long-Window Prediction (AI Long-Window)
- **ID:** G7 (`scenario_7_hourly`)
- **Name:** Hourly Database Long-Window Prediction [AI_LONG_WINDOW]
- **Layers involved:** Layer 3 (Guardian Orchestrator, PostgreSQL Storage)
- **Target Device:** `z1-wl1-hourly`
- **Category:** `AI_LONG_WINDOW`
- **Description:** Periodic hourly cron evaluator queries 24-hour historical raw readings from PostgreSQL database, extracts multi-scale temporal trend features, and scores with LongAnomalyDetector.
- **Trigger:** Invocation of `orchestrator.evaluate_hourly_database_window(lookback_seconds=3600.0)`.
- **Expected Guardian output:**
  - Evaluates historical reading windows from PostgreSQL storage.
  - Extracts 24-hour Haar wavelet macro features.
  - Produces `ModelOutput` database record with `model_version='long_window_24h'`.
  - Persists verdicts to database audit tables.
- **How to verify:** Run `python3 scenarios/run_scenarios.py`; verify Scenario 7 logs `PASS [✓]`.

---

### G8: Anti-Corruption ACL Gateway (Dropping Quarantined Readings)
- **ID:** G8 (`scenario_8_quarantine_filter`)
- **Name:** Anti-Corruption ACL Quarantine Gate [SECURITY_GATE]
- **Layers involved:** Layer 1 (Twin), Layer 3 (Guardian Boundary Adapter)
- **Target Device:** `z1-wl1`
- **Category:** `SECURITY_GATE`
- **Description:** Verifies that telemetry emitted by an isolated or quarantined device is immediately intercepted, sanitized, and dropped at the `TwinToGuardian` boundary adapter before entering downstream AI pipelines.
- **Trigger:** Telemetry payloads passed with `status='ok'` vs `status='quarantined'`.
- **Expected Gateway output:**
  - Active device payload (`status='ok'`): Validated and transformed into `Sensor` domain object.
  - Contained device payload (`status='quarantined'`): Flagged, isolated, and prevented from reaching internal processing queues.
- **How to verify:** Run `python3 scenarios/run_scenarios.py`; verify Scenario 8 logs `PASS [✓]`.

---

### Cross-Layer Guardian Scorecard Summary

| Scenario ID | Test Name | Category | Target Device | Expected Anomaly | Expected Twin Status | Expected Brain Action |
|:---|:---|:---|:---|:---|:---|:---|
| **G1** | Nominal Stream Operation | `BASELINE` | `z1-wl1` | `None` | `ok` | `OPERATIONAL_DECISION_EXECUTED` |
| **G2** | Physical Range Breach | `STATIC_CHECK` | `z1-wl1` | `OUT_OF_RANGE` | `quarantined` | `FLOOD_RELIEF_ACTUATED` |
| **G3** | Sensor Silence / Loss of Signal | `STATIC_CHECK` | `z2-wl2` | `SENSOR_IS_DOWN` | `quarantined` | `MAINTENANCE_DISPATCHED` |
| **G4** | Spatial Cluster Spoofing | `SPATIAL_CHECK` | `z1-wl1` | `SPOOFED_DATA` | `isolated` | `SECURITY_CONTAINMENT_VERIFIED` |
| **G5** | Hardware Malfunction & Jitter | `AI_INFERENCE` | `z3-tc1` | `SENSOR_MALFUNCTION` | `quarantined` | `REPAIR_DISPATCHED` |
| **G6** | Non-Physical Bitstream Inversion | `AI_INFERENCE` | `z4-tc1` | `NOT_SENSOR_DATA` | `isolated` | `SECURITY_CONTAINMENT_VERIFIED` |
| **G7** | Hourly Database AI Evaluation | `AI_LONG_WINDOW` | `z1-wl1-hourly` | Multi-hour drift | `persisted` | `LONG_WINDOW_EVALUATED` |
| **G8** | Anti-Corruption Quarantine Gate | `SECURITY_GATE` | `z1-wl1` | Contained device | `dropped` | `TRAFFIC_BLOCKED` |


