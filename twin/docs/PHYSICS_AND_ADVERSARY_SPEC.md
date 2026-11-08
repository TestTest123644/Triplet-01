# Twin Layer: Physical Simulation & Adversary Specifications

This reference documents the physical formulas, diurnal kinetic curves, incident engine dynamics, and adversary state machines powering the simulation engine in `twin/simulation.py`.

---

## 1. Simulation Engine Principles & Dual-Clock System

1. **Dual-Clock Coordination:**
   - **Simulation Time:** Strictly deterministic RFC 3339 UTC timestamps (`YYYY-MM-DDTHH:MM:SSZ`).
   - **Wall-Clock Pacing:** Cycle pacing runs strictly at 10 Hz (100 ms per wall cycle) via `SimulationClock.pace_wall_clock()`. Wall-clock time is never leaked into Kafka payloads, SQLite truth readings, or MCP responses.
2. **Simulation Speed Acceleration (`SIM_SPEED`):**
   - Configurable via environment variable `SIM_SPEED` (default `1.0`, allowable range `0.1` to `60.0`).
   - Each simulation tick advances simulation time by $\Delta t = 0.1 \times \text{SIM\_SPEED}$ simulation seconds.
   - All physical differential equations, noise terms, and exponential lags are written per simulation second, ensuring physical trajectory invariance regardless of `SIM_SPEED`.
3. **Euler-Maruyama Stochastic Integration & Lags:**
   - Noise terms scale with $\sqrt{\Delta t}$ in accordance with the Euler-Maruyama scheme: $\sigma \sqrt{\Delta t} \cdot \mathcal{N}(0, 1)$.
   - Dynamic inertia follows first-order exponential lag: $\alpha = 1 - \exp(-\Delta t / \tau)$, where $\tau$ is the characteristic time constant in simulation seconds.
4. **Dedicated Physics PRNG (`physics_rng`):**
   - Physics updates, camera variety factors, and stochastic Poisson arrival processes derive exclusively from a dedicated instance of `random.Random(SIM_SEED)`.
   - Network interactions, mock clients, and API requests do not advance the physics RNG, guaranteeing bit-for-bit trajectory determinism across identical runs.
5. **Municipal Zones & Geographic Coordinates:**
   - The city comprises 42 IoT devices distributed evenly (7 devices per zone) across 6 municipal zones.
   - Zone-2 ("Port & Coastal Harbor") is positioned at the residential center of **La Goulette** with centroid coordinates `(36.8185, 10.3050)`.
   - The 7 devices in each zone follow the deterministic geographic offset formula:
     $$\text{lat} = \text{round}(z_{\text{lat}} + (\text{idx} \times 0.0012) - 0.0036, 6)$$
     $$\text{lon} = \text{round}(z_{\text{lon}} + (\text{idx} \times 0.0015) - 0.0045, 6)$$

---

## 2. Sensor Kinetics & Physical Formulations

The digital twin models 42 IoT devices across 5 municipal categories:

### A. Traffic Camera (`traffic_camera`)
- **Metric:** Vehicle passage throughput in vehicles per minute ($V$).
- **Operating Bounds:** $0.0 \le V \le 200.0\text{ veh/min}$.
- **Diurnal Target Demand ($V^*(t)$):**
  For simulation hour $t \in [0.0, 24.0)$:
  $$V^*(t) = 25.0 + 95.0 \cdot \exp\left( -\frac{(t - 8.5)^2}{2 \cdot 1.2^2} \right) + 110.0 \cdot \exp\left( -\frac{(t - 17.5)^2}{2 \cdot 1.5^2} \right)$$
  - Morning rush hour peaks around 08:30 (~120 veh/min).
  - Evening rush hour peaks around 17:30 (~135 veh/min).
  - Overnight baseline remains below 35 veh/min (~25 veh/min).
- **Per-Camera Variety:** Each traffic camera is assigned a fixed variety multiplier drawn from $[0.85, 1.15]$ using `physics_rng`.
- **Actuation Impact (`set_signal_plan`):**
  - `normal`: Multiplier $1.0$.
  - `green_wave`: Multiplier $1.15$ (clears congestion corridors).
  - `red_restriction`: Multiplier $0.55$ (dampens incoming vehicle density).
  - `all_red`: Multiplier $0.05$ (emergency cordon stop).
  - `adaptive`: Targets the zone-wide mean target across both cameras in the zone.
- **Incident Impact (`traffic_accident`):**
  - Reduces target throughput to $\times 0.3$ ramped over 60 simulation seconds.
- **Dynamics:** Time constant $\tau = 60\text{ s}$, noise $\sigma = 0.6\sqrt{\Delta t}\text{ veh/min}$. Steady-state spread $\sigma\sqrt{\tau/2} \approx 3.29\text{ veh/min}$.

---

### B. Water Level Float (`water_level`)
- **Metric:** Drainage channel flood level in meters ($H$).
- **Operating Bounds:** $0.0 \le H \le 12.0\text{ meters}$.
- **Differential Equation:**
  $$\frac{dH}{dt} = Q_{\text{in}}(t) - k \cdot c \cdot \sqrt{2g \max(0, H - H_0)}$$
- **Constants & Parameters:**
  - Gravitational acceleration: $g = 9.81\text{ m/s}^2$.
  - Retention threshold: $H_0 = 1.2\text{ meters}$.
  - Calibrated discharge coefficient: $c = 0.00089$.
  - Inflow rate: $Q_{\text{base}} = 0.003\text{ m/s}$ modulated by a $\pm 10\%$ tidal term with a 12.4-hour period ($44,640\text{ s}$):
    $$Q_{\text{base}}(t) = 0.003 \cdot \left(1.0 + 0.10 \cdot \sin\left(\frac{2\pi \cdot t_{\text{sim\_sec}}}{44640}\right)\right)$$
  - Each device has a fixed catchment multiplier $C_i \sim \mathcal{U}(0.85, 1.15)$ drawn from the physics RNG applied to inflow ($Q_{\text{in}, i}(t) = Q_{\text{base}}(t) \cdot C_i$).
  - Each device has a starting level drawn from the physics RNG ($H_{0, i} \sim \mathcal{U}(3.0, 3.8)\text{ meters}$).
  - No sensor noise on water (spec preserved), determinism for a given seed, storms scale per zone ($Q_{\text{in}, i}(t) = Q_{\text{base}}(t) \cdot C_i + Q_{\text{base}}(t) \cdot (\text{intensity} - 1.0)$), and the two sensors in one zone stay strongly correlated.
- **Actuation Impact (`set_valve`):**
  - Valve ratio $k$: `open` ($1.0$), `half` ($0.5$), `closed` ($0.0$), `"NN%"` ($\text{NN}/100$).
  - Default initial valve position: `"half"` ($k = 0.5$).
- **Calibrated Behaviors:**
  - *Nominal Half Valve:* Holds steady between $2.5\text{ m}$ and $4.5\text{ m}$ for 2+ simulation hours.
  - *Closed Valve:* Starting from $3.5\text{ m}$, crosses the $8.0\text{ m}$ critical threshold after $15$ to $40$ simulation minutes (~25 min).
  - *Open Valve:* Rapid gravity drainage settles near $1.78\text{ m} \approx 1.8\text{ m}$.
  - *Rain Storm ($\times 3$ Inflow) with Half Valve:* Crosses $8.0\text{ m}$ within $8$ to $25$ simulation minutes (~16.6 min).
  - *Rain Storm ($\times 3$ Inflow) with Open Valve:* Equilibrium settles at $H \approx 6.41\text{ m}$, staying safely below $8.0\text{ m}$.

---

### C. Power Load Meter (`power_load`)
- **Metric:** Substation electrical load in megawatts ($P$).
- **Operating Bounds:** $5.0 \le P \le 120.0\text{ MW}$.
- **Diurnal Target Demand ($P^*(t)$):**
  - **Evening Transition ($20:00 \le t < 21:00$):**
    $$P^*(t) = 30.0 + 14.0 \cdot (1.0 + \cos(\pi(t - 20.0)))$$
    Smooth cosine descent from $58.0\text{ MW}$ down to $30.0\text{ MW}$.
  - **Deep Night ($21:00 \le t < 05:00$):**
    With $t_{\text{night}} = (t - 21.0) \pmod{24}$:
    $$P^*(t) = 30.0 + 3.0 \cdot \sin\left(\frac{2\pi \cdot t_{\text{night}}}{8.0}\right)$$
    Night load oscillates gently between $27.0\text{ MW}$ and $33.0\text{ MW}$ (strictly within $25 - 35\text{ MW}$).
  - **Morning Transition ($05:00 \le t < 06:00$):**
    $$P^*(t) = 30.0 + 14.0 \cdot (1.0 - \cos(\pi(t - 5.0)))$$
    Smooth cosine ascent from $30.0\text{ MW}$ up to $58.0\text{ MW}$.
  - **Daytime ($06:00 \le t \le 20:00$):**
    $$P^*(t) = 30.0 + 70.0 \cdot \left(0.6 \cdot \sin\left(\frac{\pi(t - 6.0)}{14.0}\right) + 0.4\right)$$
    Daytime peak reaches $100.0\text{ MW}$ at 13:00 (within $85 - 110\text{ MW}$).
  - Seamless continuity at boundary hours $05:00$, $06:00$, $20:00$, and $21:00$ prevents jumps $> 3\text{ MW}$ between consecutive simulation minutes (max step $< 0.75\text{ MW/min}$).
- **Actuation Impact (`switch_grid`):**
  - Grid state factors: `grid_a` ($1.0$), `grid_b` ($0.85$), `island` ($0.60$), `shed` ($0.40$), `standby` ($0.20$).
- **Incident Impact (`grid_overload`):**
  - Target demand is set to $\max(P^*(t) \cdot 1.35, 118.0\text{ MW})$, ensuring the overload is visible and exceeds the $114.0\text{ MW}$ threshold at any hour of day or night.
  - If true power exceeds $114.0\text{ MW}$ continuously for 60 simulation seconds, **blackout** triggers: power target drops to $10\%$ of normal target and emergency calls surge ($+4.0$).
- **Dynamics:** Time constant $\tau = 120\text{ s}$, noise $\sigma = 0.3\sqrt{\Delta t}\text{ MW}$. Steady-state spread $\sigma\sqrt{\tau/2} \approx 2.32\text{ MW}$.

---

### D. Air Quality Station (`air_quality`)
- **Metric:** Ambient Air Quality Index ($AQI$).
- **Operating Bounds:** $10.0 \le AQI \le 350.0$.
- **Target Formulation:**
  $$\text{Target} = \text{Background}(\text{zone}) + 0.45 \cdot \text{mean}(\text{zone true traffic}) + \text{IndustrialTerm} + \text{IncidentTerms}$$
- **Parameters:**
  - Zone Backgrounds: `zone-1` ($18.0$), `zone-2` ($15.0$), `zone-3` ($20.0$), `zone-4` ($8.0$), `zone-5` ($14.0$), `zone-6` ($12.0$).
  - Clean Baseline: In `zone-4` (Green Hills), background of $8.0$ yields measured AQI of about $19$ at night and about $60$ at rush hour.
  - Industrial Contribution: `zone-3` receives $+25.0\text{ AQI}$.
  - Incidents:
    - `fire`: $+150.0\text{ AQI}$ ramping linearly over 120 simulation seconds.
    - `air_hazard`: $+100.0\text{ AQI}$, mitigated by signal plans (`red_restriction` reduces bonus by 50%, `all_red` reduces bonus by 80%).
- **Dynamics:** Time constant $\tau = 300\text{ s}$, noise $\sigma = 0.15\sqrt{\Delta t}\text{ AQI}$. Steady-state spread $\sigma\sqrt{\tau/2} \approx 1.84\text{ AQI}$.

---

### E. Emergency Call Dispatch (`emergency_call`)
- **Metric:** Municipal emergency 911/112 call volume in calls per minute ($C$).
- **Operating Bounds:** $0.0 \le C \le 30.0\text{ calls/min}$.
- **Arrival Process:**
  - In each tick, arrivals $n \sim \text{Poisson}(\lambda \cdot \Delta t / 60)$ are drawn from `physics_rng`.
  - Rate parameter $\lambda$ (calls/min) is dynamic per zone:
    $$\lambda = 2.0 + 4.5 \cdot \mathbb{I}(\text{any zone cam} > 120) + 8.0 \cdot \mathbb{I}(\text{any zone water} > 8.0) + \text{IncidentSurges} - 1.0 \cdot N_{\text{generic\_dispatches}}$$
    Floored at $\lambda_{\text{min}} = 0.2\text{ calls/min}$.
  - Incident surges: `traffic_accident` ($+6.0$), `fire` ($+8.0$), `grid_overload` with blackout ($+4.0$).
- **Telemetry Reporting:**
  - Reported reading is the exact count of arrivals during the past 60 simulation seconds, maintained in a FIFO deque per zone and capped at 30 calls/min.
  - Active generic dispatches relieve emergency pressure for `DISPATCH_DURATION_S` (default 900 s).

---

## 3. Physical Incident Engine

Physical incidents represent real municipal emergencies, distinct from adversary cyber attacks. They alter physical reality directly and can be resolved through timely city actuation.

| Incident Type | Scope | Physical Effect | Resolution & Duration Defaults |
| :--- | :--- | :--- | :--- |
| **`rain_storm`** | Zone | Upstream inflow $Q_{\text{in}}$ multiplied by intensity ($1.0 - 5.0$, default $3.0$). | Defaults to `duration_s = 1800` (30 min) when omitted; naturally expires. |
| **`traffic_accident`** | Device (Camera) | Camera target multiplied by $\times 0.3$ ramped over 60 s; zone $\lambda + 6.0$. | Lasts until resolved by `dispatch` matching `traffic_accident`, `medical_emergency`, or `medical_surge` after 300 s response time (or cleared). |
| **`fire`** | Zone | AQI target $+150$ ramped over 120 s; zone $\lambda + 8.0$. | Lasts until resolved by `dispatch` matching `fire` after 180 s response time (or cleared). |
| **`grid_overload`** | Zone (Power) | Power target $\max(P^* \cdot 1.35, 118\text{ MW})$. Continuous load $> 114\text{ MW}$ for 60 s causes blackout (target $\times 0.10$, $\lambda + 4.0$). | Pre-blackout: resolved immediately by `switch_grid` to `grid_b`, `shed`, or `standby`. Post-blackout: resolved by `dispatch("blackout")` after 240 s. Lasts until resolved or cleared. |
| **`air_hazard`** | Zone | AQI target $+100$, mitigated by `red_restriction` or `all_red` signal plans. | Defaults to `duration_s = 1800` (30 min) when omitted; naturally expires. |

### Zero-Trust Isolation of Physical Incidents
- **MCP Invisibility:** Physical events are never exposed to City Brain or Guardian through MCP queries.
- **Contained / Attacked Commands:** Any command refused due to device containment (`DEVICE_CONTAINED`) or suppressed by an actuator attack does **not** resolve physical incidents.
- **Red Team API (Port 8100):** Protected by `X-Red-Team-Token`:
  - `POST /api/event/inject`: Injects incident (`type`, `zone_id`, `device_id`, `intensity`, `duration_s`).
  - `POST /api/event/clear`: Clears specific incident or all incidents.
  - `GET /api/event/active`: Lists active physical events.
- **Ground Truth Ledger:** Initiations, durations, and resolutions are recorded in SQLite table `physical_events`.

---

## 4. Adversary Capability Engine

The adversary model corrupts wire telemetry while the internal SQLite ledger preserves ground truth:

| Attack Capability | Wire Manipulation Method | Detection & Guardian Mitigation |
| :--- | :--- | :--- |
| **`device_physical`** | Overwrites telemetry value with attacker-defined spoofing (`spoofed_value` or `value` parameter); generates valid Ed25519 signature with legitimate device key. | Guardian flags anomalous physical gradients or cross-sensor contradictions; invokes `rollback_reading` or `isolate_device`. |
| **`device_key`** | Attacker extracts valid private key and broadcasts malicious readings (`spoofed_value` or `value` parameter) that cryptographically verify on wire. | Guardian cross-checks with historical sensor baselines and neighbor correlations; invokes `quarantine_device`. |
| **`link`** | Flips payload bits (`corrupt_bytes`), drops readings (`drop`), replays past packets (`replay`), or buffers frames in a FIFO delay queue (`delay`). When cleared, held frames are immediately released in FIFO order. | Guardian detects invalid Ed25519 signatures, sequence gaps, or timestamp regression; alerts operators. |
| **`rogue`** | Broadcasts telemetry under an unregistered identifier (e.g., `rg-01`). | Guardian compares against `list_devices`; quaranines rogue hardware via MCP `quarantine_device`. |
| **`actuator`** | Acknowledges MCP commands as `"ok"` but leaves true physical state unchanged. | City Brain observes telemetry failing to converge toward expected physical setpoints. |
| **`sensor_fault`** | Injects subtle linear drift ($\Delta t \cdot \text{drift\_rate}$), stuck frozen values, or zero dropouts. | Statistical moving-average and deviation filters identify sensor drift. |

### 4.1 Link Adversary and Delay Backlog Lifecycle
When the `link` capability is configured in `delay` mode (`delay_ticks`), frames for the targeted device are held in a FIFO delay queue up to a maximum depth of 600 ticks. The count of unreleased frames across all device buffers is exposed in the Red Team API (`/api/status`) under `delayed_frames_pending`. When a link attack in delay mode is cleared (via `/api/attack/clear`, individual attack clear, or simulator settle), all held frames are immediately released to the wire in strict chronological order instead of dribbling out over subsequent ticks, and `delayed_frames_pending` drops to 0.

### 4.2 Parameter Aliases for Injection Payloads
For the `device_physical` and `device_key` adversary capabilities, the target sensor value may be specified using either `spoofed_value` or `value` in the injection parameter dictionary (`params: {"spoofed_value": 185.0}` or `params: {"value": 185.0}`). If both or either are provided, the simulator resolves and normalizes them identically to enforce consistent spoofing on wire telemetry.
