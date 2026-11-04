# Twin Layer: Physical Simulation & Adversary Specifications

This reference documents the physical formulas, diurnal kinetic curves, and state machines powering the simulation engine in `twin/simulation.py`.

---

## 1. Simulation Engine Principles

1. **Fixed Timestep (10 Hz):** The simulation clock advances by exactly 100 milliseconds (`Delta-t = 0.1 s`) every tick. All differential equations integrate with respect to this fixed timestep.
2. **Deterministic Seeded PRNG:** All sensor noise, baseline fluctuations, and initial device keypairs derive from a deterministic random seed (`SIM_SEED = 42`). Executing the simulation with the same seed produces the identical sequence of readings.
3. **Diurnal Time Mapping:** Simulation time maps continuously to a 24-hour daily cycle (`t_day` from `0.0` to `24.0` hours). Sensor kinetics vary according to circadian human activity patterns.

---

## 2. Sensor Kinetics & Physical Models

The system models 42 IoT devices across 5 distinct municipal sensor categories:

### A. Traffic Camera (`traffic_camera`)
- **Metric:** Vehicle passage rate in vehicles per minute (`V`).
- **Operating Bounds:** `0.0 <= V <= 200.0 veh/min`.
- **Physical Model:** Bimodal diurnal curve with morning rush hour (08:30) and evening rush hour (17:30):

> **V(t) = V_base + A1 * exp[ -(t - 8.5)^2 / (2 * sigma1^2) ] + A2 * exp[ -(t - 17.5)^2 / (2 * sigma2^2) ] + GaussianNoise(0, sigma_noise)**

- **Parameters:**
  - `V_base`: Overnight baseline flow (~25.0 veh/min).
  - `A1`: Morning peak amplitude (~95.0 veh/min, spread `sigma1 = 1.2` hrs).
  - `A2`: Evening peak amplitude (~110.0 veh/min, spread `sigma2 = 1.5` hrs).
  - `sigma_noise`: High-frequency camera jitter (standard deviation = 2.5 veh/min).
- **Actuation Impact (`set_signal_plan`):**
  - `green_wave`: Relieves queue bottlenecks by reducing effective density (-18% queue volume over 30 simulation ticks).
  - `all_red`: Stops traffic inflow for emergency clearance.
  - `adaptive`: Dynamically balances cross-street vehicle throughput.

---

### B. Water Level Float (`water_level`)
- **Metric:** Drainage channel water depth in meters (`H`).
- **Operating Bounds:** `0.0 <= H <= 12.0 meters`.
- **Physical Model:** Torricelli gravity drainage differential model with upstream tidal inflow:

> **dH/dt = Q_inflow(t) - k_valve * sqrt[ 2 * g * max(0, H - H0) ]**

- **Parameters:**
  - `g`: Gravitational acceleration (9.81 m/s^2).
  - `H0`: Baseline retention channel depth (1.2 meters).
  - `Q_inflow`: Environmental precipitation and tidal runoff rate (m/s).
- **Actuation Impact (`set_valve`):**
  - `open`: `k = 1.0` (rapid gravity discharge; drains water at ~0.08 m/s).
  - `half`: `k = 0.5` (restricted flow rate).
  - `closed`: `k = 0.0` (zero discharge; water accumulates with upstream runoff).

---

### C. Power Load Meter (`power_load`)
- **Metric:** Substation electrical demand in megawatts (`P`).
- **Operating Bounds:** `5.0 <= P <= 120.0 MW`.
- **Physical Model:** Base baseline idle demand plus commercial and industrial daytime load:

> **P(t) = P_base + P_peak * [ 0.6 * sin( pi * (t - 6.0) / 14.0 ) + 0.4 ] + GaussianNoise(0, 1.2)**

- **Operational Behavior:**
  - During daytime hours (`06:00 <= t <= 20:00`), industrial refrigeration, air conditioning, and manufacturing draw power toward peak load (~85 to 110 MW).
  - During night hours (`t < 06:00` or `t > 22:00`), demand stabilizes at base idle (~25 to 35 MW).
- **Actuation Impact (`switch_grid`):**
  - `grid_a`: Primary high-voltage transmission line.
  - `grid_b`: Redundant secondary substation (sheds non-critical industrial load by 15%).
  - `island`: Microgrid isolation mode for localized electrical fault containment.

---

### D. Air Quality Station (`air_quality`)
- **Metric:** Particulate Matter Pollution Index (`AQI`).
- **Operating Bounds:** `10 <= AQI <= 350`.
- **Physical Model:** Strongly correlated with local vehicular traffic `V(t)` and ambient atmospheric dispersion:

> **AQI(t) = AQI_background + alpha * V(t) + beta * IndustrialFactor + BrownianJitter**

- **Operational Behavior:**
  - Heavy traffic congestion directly elevates particulate counts in Downtown Financial (`zone-1`) and Industrial Charguia (`zone-3`).
  - Residential Hills (`zone-4`) benefits from atmospheric dispersion and remains near clean baseline (AQI 25 to 45).

---

### E. Emergency Call Dispatch (`emergency_call`)
- **Metric:** Incoming emergency call rate in calls per minute (`C`).
- **Operating Bounds:** `0 <= C <= 30 calls/min`.
- **Physical Model:** Poisson arrival process with dynamic rate parameter `lambda(t)`:

> **lambda(t) = lambda_base + gamma_traffic * Indicator(V > 120) + gamma_flood * Indicator(H > 8.0)**

- **Operational Behavior:**
  - Baseline call rate is low (`lambda_base = 2.0 calls/min`).
  - Severe traffic gridlock (`V > 120 veh/min`) triggers accident reports (+4.5 calls/min).
  - Dangerous flood channel levels (`H > 8.0 meters`) activate flood rescue calls (+8.0 calls/min).
- **Actuation Impact (`dispatch`):**
  - City Brain invokes `dispatch(incident_type, zone_id)` to acknowledge and route municipal emergency units (police, fire, EMS), decrementing active incident counts.

---

## 3. Adversary Capability Engine

The adversary model implements 6 distinct attack vectors, altering telemetry broadcast on the Kafka stream while the internal SQLite ledger preserves ground truth:

```text
+-----------------------+         +-----------------------+
|     Physics Engine    |         | SQLite "readings_truth"|
| (True Physical State) |-------->|  (Preserved Truth)    |
+----------+------------+         +-----------------------+
           |
           v
+-----------------------+
|   Adversary Module    | <------- Active Attacks Table
| (Injected Corruption) |
+----------+------------+
           |
           v
+-----------------------+
|  Kafka "twin.sensors" | --------> External World (Brain & Guardian)
| (Poisoned Wire Stream)|
+-----------------------+
```

| Attack Capability | Wire Manipulation Method | Detection & Mitigation |
| :--- | :--- | :--- |
| **`device_physical`** | Overwrites telemetry metric with attacker value; maintains legitimate Ed25519 signature. | Guardian detects impossible physical gradients or discrepancies with neighboring sensors; rolls back reading via MCP. |
| **`device_key`** | Signs payload with an unauthorized, rogue private key. | Guardian verifies against public key retrieved via MCP `get_device_key`; signature check fails immediately. |
| **`link`** | Flips bits in transit or drops packets entirely. | Guardian receives malformed JSON or cryptographic signature validation failure (`Ed25519 verification failed`). |
| **`rogue`** | Broadcasts telemetry under an unregistered identifier (e.g. `z1-rogue-1`). | Guardian cross-checks with MCP `list_devices`; flags sensor as an unauthorized hardware intrusion. |
| **`actuator`** | Acknowledges MCP commands as `"ok"` but refuses to alter physical simulation state. | City Brain detects telemetry failing to converge toward expected target; alerts human operators. |
| **`sensor_fault`** | Injects subtle continuous linear drift (`dt * drift_rate`) or frozen repeated values. | Statistical outlier filtering across zone-level sensor clusters flags drifting readings. |
