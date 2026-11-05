# City Brain Convergence - Triplet Testbook

This testbook documents end-to-end integration and demonstration scenarios for the smart city platform.
Each scenario verifies cyber-physical realism, adversarial resilience, and multi-layer coordination across Layer 1 (Digital Twin), Layer 2 (City Brain), and Layer 3 (Guardian).

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

