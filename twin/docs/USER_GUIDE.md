# Twin Layer: Operations & User Guide

This guide is an illustrated walkthrough of the Twin Layer Adversarial Console. It explains the visual interface, details how each sector and sensor functions, and provides a 5-minute hands-on tutorial for testing all 6 adversarial capabilities.

---

## 1. Interface Tour & Visual System

The Adversarial Console runs in dark tactical mode (`#060913` background with navy glassmorphic panels). The interface uses three status colors:
- **Cyan (`#00e5ff`):** Nominal telemetry and healthy sensor pillars.
- **Amber (`#ffb300`):** Minor variance, unverified key, or degraded connectivity.
- **Crimson (`#ff1744`):** Active adversarial attack, signature failure, or physical state divergence.

### A. 3D Tactical Map (Greater Tunis)

![Greater Tunis 3D Tactical Map](images/3d_tactical_map_overview.png)

The map displays all 6 municipal sectors across Greater Tunis, with fixed centroids on dry land:
1. **Downtown Financial (`zone-1`):** Commercial center along Avenue Habib Bourguiba.
2. **Port & Coastal Harbor (`zone-2`):** Radès and La Goulette harbor docks and shipping canal.
3. **North Industrial Park (`zone-3`):** Charguia and Ariana Sud industrial hub.
4. **West Residential Hills (`zone-4`):** Residential terrain across Bardo and Manouba hills (positioned north of Sebkhet Sijoumi lake).
5. **Central University & Med (`zone-5`):** El Manar campus and Rabta medical district.
6. **South Logistics Hub (`zone-6`):** Ben Arous freight rail terminal and supply warehouses.

Each sensor is rendered as a 3D hexagonal column whose height scales with its current metric. When a sensor is nominal, its pillar and sector halo glow cyan. When an attack is injected, the target pillar elevates and turns crimson, surrounded by a red warning beacon.

---

### B. Neighborhood Operational Cards & Red Team Sidebar

![Metropolitan Neighborhood Sectors and Telemetry Summary](images/adversarial_console_and_neighborhoods.png)

![Red Team Threat Matrix Controls](images/red_team_sidebar_controls.png)

- **Neighborhood Cards (Main View):** Each card summarizes the 7 sensors operating within that zone:
  - **Traffic Cameras (`tc1`, `tc2`):** Vehicle passage rates (0 - 200 veh/min).
  - **Water Level Floats (`wl1`, `wl2`):** Drainage channel depth (0 - 12 meters).
  - **Power Load Meters (`pl1`):** Electrical grid draw (5 - 120 MW).
  - **Air Quality Stations (`aq1`):** Particulate index (10 - 350 AQI).
  - **Emergency Dispatch (`ec1`):** Active incoming emergency calls (0 - 30 call/min).
- **Red Team Controls (Sidebar):** Contains attack configuration controls, one-click demo presets, simulation clock pulse controls, and the active threat manifest showing all currently running exploits.

---

### C. Clean Vector Cartography

![Clean Vector Cartography](images/vector_cartography_zoom.png)

The basemap uses clean dark vector tiles (`CARTO Dark Matter`) without cluttered text labels, allowing clean zooming into street-level intersections without broken fonts or overlapping water bodies.

---

## 2. Hands-On Tutorial: Testing the System in 5 Minutes

Follow these steps to test every capability in real time.

### Step 1: Open the Console & Verify Nominal Baseline
1. Open your browser and navigate to `http://localhost:8501`.
2. Inspect the top Scoreboard:
   - **Total Readings:** Actively incrementing.
   - **Active Incidents / Tampered:** 0.
   - **Active Red Team Attacks:** 0 (`Zero Threats`).
   - **System Status:** `[SYSTEM STATUS: AUTONOMOUS] // LOCAL SIMULATION RUNTIME ACTIVE`.
3. Verify that all 6 neighborhood cards show green `[NOMINAL INTEGRITY // 7/7 ONLINE]` badges.

---

### Step 2: Inject a Physical Sensor Spoof (`device_physical`)
In this attack, the physical sensor reading is falsified on the wire, but the ground-truth database records the true physical reality.

1. In the sidebar under **"MANUAL THREAT MATRIX"**:
2. Set **Target Sensor Identifier** to `z1-tc1` (Downtown Traffic Camera 1).
3. Set **Adversary Capability Model** to `4. Physical Layer (Sensor Spoofing)`.
4. Enter **Spoofed Telemetry Value**: `185.0` (simulating extreme artificial traffic gridlock).
5. Click **"EXECUTE ATTACK"**.

**What happens immediately:**
- The Downtown Financial card flips to red: `[CRITICAL THREAT ACTIVE]`.
- On the 3D map, the `z1-tc1` pillar elevates and pulses crimson.
- The sidebar **Active Threat Manifest** lists `[THREAT ACTIVE] device_physical (z1-tc1)`.

---

### Step 3: Inspect Ground Truth vs Broadcast Divergence
1. Click **Tab 2: "Live Telemetry & Divergence Curves"**.
2. Select Device `z1-tc1` from the dropdown.
3. Observe the Altair time-series chart:
   - **Solid Emerald Line:** Real physical reality (e.g., ~45 veh/min).
   - **Dotted Crimson Line:** Spoofed broadcast value (185 veh/min).
   - **Divergence Metric:** Shows an absolute discrepancy of ~140 veh/min.
4. This divergence proves the physical isolation rule: the external world receives the poisoned data, but the Twin's internal SQLite ledger preserves true ground reality.

---

### Step 4: Test the Other 5 Attack Capabilities

You can test each capability individually using the sidebar controls:

| Capability | Target Device | Test Configuration | Observable Result |
| :--- | :--- | :--- | :--- |
| **`device_key`** (Compromised Key) | `z3-pl1` (Power Load) | Replaces device key with an attacker key | Outbound signature remains valid, but Guardian detects revoked/unauthorized public key on MCP. |
| **`link`** (Network Tampering) | `z2-wl1` (Water Float) | Corrupts raw JSON bytes or drops frames | Guardian signature validation immediately fails (`Ed25519 verification failed`); payload corrupted. |
| **`rogue`** (Unregistered Transmitter) | `zone-4` (Residential) | Transmits unauthorized packet `z4-rogue-1` | Packet appears on wire; Guardian MCP tool `list_devices` flags sensor as unregistered intruder. |
| **`sensor_fault`** (Hardware Drift) | `z5-aq1` (Air Quality) | Injects additive drift (`+45.0 AQI`) | Values drift upward without triggering hard boundaries; caught by multi-sensor neighborhood correlation. |
| **`actuator`** (Command Disobedience)| `z1-wl2` (Flood Valve) | Sets actuation state to `stuck_closed` | Device returns `"status": "ok"` to City Brain MCP call, but physically refuses to open drainage valve. |

---

### Step 5: Use One-Click Hackathon Pitch Presets
During live presentations, you do not need to manually configure parameters. The sidebar includes **6 Pitch Presets** pre-calibrated for demonstration:

1. Click **"DEMO PRESET 1: PHYSICAL SPOOF (DOWNTOWN)"**.
   - Spikes traffic camera `z1-tc1` to 195 veh/min.
   - Demonstrates immediate detection on 3D map.
2. Click **"DEMO PRESET 2: FORGED KEY (INDUSTRIAL)"**.
   - Emulates key theft on power substation `z3-pl1`.
   - Demonstrates zero-trust key revocation.
3. Click **"DEMO PRESET 3: LINK CORRUPTION (HARBOR)"**.
   - Injects bitflips into harbor water level `z2-wl1`.
   - Demonstrates signature verification failure.

---

### Step 6: Disarm Threats & Confirm Baseline Recovery
1. In the sidebar, click the button **"DISARM ALL THREATS"**.
2. **Verification:**
   - Active attacks count immediately resets to `0` (`Zero Threats`).
   - All 6 neighborhood cards turn back to `[NOMINAL INTEGRITY // 7/7 ONLINE]`.
   - All 3D pillars return to uniform cyan baseline.
   - The Active Threat Manifest clears completely.

---

### Step 7: Forensic Audit Ledger
1. Click **Tab 3: "Raw Ledger & Forensic Audit"**.
2. The table shows the immutable record of all injected attacks stored in `attacks_injected`:
   - Exact simulation timestamp (`start_sim_time` and `end_sim_time`).
   - Attack type, target device, and attack parameters.
   - Active status (`1` when running, `0` once disarmed).
3. This audit trail gives judges and security evaluators proof of attack lifecycles and response timestamps.
