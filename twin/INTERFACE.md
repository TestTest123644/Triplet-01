# PARTNER INTEGRATION INTERFACE (Twin Layer - Layer 1)
**Project:** TSYP 14 City Brain Convergence  
**Target Audience:** Layer 2 (City Brain) & Layer 3 (Guardian) Engineering Teams  
**Protocol Version:** Phase 0 Contract Compliant (R1)

---

## 1. Architecture & Zero-Trust Integration Rules

The Twin Layer (Layer 1) simulates the smart city physical environment and provides live telemetry and command interfaces to the **City Brain (Layer 2)** and **Guardian (Layer 3)**.

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

### Critical Operational Directives:
1. **Unidirectional Streaming (Kafka):** Telemetry is pushed from the Twin to the Kafka topic `twin.sensors`. Partners must NEVER produce sensor data to this topic.
2. **Synchronous Command Interface (MCP):** Commands and queries are executed exclusively via the Model Context Protocol (MCP) JSON-RPC server (HTTP/SSE on port `8000`). MCP commands are NEVER routed over Kafka.
3. **Dual Clocks:**
   - **Simulation Time (RFC 3339 UTC, `ts`):** All telemetry timestamps use simulation time with second precision ending in `'Z'` (e.g. `2026-10-17T12:00:00Z`).
   - **Wall-Clock Time:** Strictly internal to the Twin for cycle pacing (10 Hz).
4. **Sub-300-Byte Payload Budget:** Every JSON payload is guaranteed to stay strictly <= 300 bytes.
5. **Cryptographic Signatures:** Every message is signed using an Ed25519 keypair unique to each device. Guardian must fetch the device public key via MCP `get_device_key` and verify the `sig` field.

---

## 2. Kafka Outbound Data Stream (`twin.sensors`)

### Connection Parameters
- **Internal (Docker Network):** `kafka:9092`
- **External (Host Machine):** `localhost:19092`
- **Topic Name:** `twin.sensors`
- **Throughput Rate:** 420 messages/second (42 devices at 10 Hz fixed rate)
- **Message Key:** UTF-8 bytes of `device_id` (e.g., `"z1-tc1"`)
- **Message Value:** Compact JSON envelope adhering to `schema.json`

### JSON Schema Specification (`schema.json`)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "TwinSensorReading",
  "type": "object",
  "properties": {
    "reading_id": {
      "type": "string",
      "description": "Unique identifier for the reading (e.g. 'r-z1-tc1-1')"
    },
    "device_id": {
      "type": "string",
      "description": "Unique device identifier (e.g. 'z1-tc1')"
    },
    "device_type": {
      "type": "string",
      "enum": [
        "traffic_camera",
        "water_level",
        "power_load",
        "air_quality",
        "emergency_call"
      ]
    },
    "ts": {
      "type": "string",
      "format": "date-time",
      "description": "Simulation time in RFC 3339 UTC format ending with 'Z'"
    },
    "seq": {
      "type": "integer",
      "minimum": 0,
      "description": "Strictly monotonic sequence counter per device"
    },
    "value": {
      "type": "number",
      "description": "Physical sensor reading measurement"
    },
    "unit": {
      "type": "string",
      "description": "Measurement unit (veh/min, meters, MW, AQI, call/min)"
    },
    "location": {
      "oneOf": [
        {
          "type": "string",
          "description": "Zone identifier string (e.g. 'zone-1')"
        },
        {
          "type": "object",
          "properties": {
            "zone_id": { "type": "string" },
            "lat": { "type": "number" },
            "lon": { "type": "number" }
          },
          "required": ["zone_id"],
          "additionalProperties": false
        }
      ]
    },
    "status": {
      "type": "string",
      "enum": [
        "ok",
        "isolated",
        "quarantined",
        "rolled_back",
        "modelled"
      ]
    },
    "sig": {
      "type": "string",
      "description": "Base64-encoded Ed25519 digital signature"
    }
  },
  "required": [
    "reading_id",
    "device_id",
    "device_type",
    "ts",
    "seq",
    "value",
    "unit",
    "location",
    "status",
    "sig"
  ],
  "additionalProperties": false
}
```

### Example Kafka Telemetry Payload (<290 bytes)
```json
{
  "reading_id": "r-z1-tc1-105",
  "device_id": "z1-tc1",
  "device_type": "traffic_camera",
  "ts": "2026-10-17T12:00:10Z",
  "seq": 105,
  "value": 48.75,
  "unit": "veh/min",
  "location": "zone-1",
  "status": "ok",
  "sig": "iM801oV+sPcr3R9eR+aA55qZq6d5e/c7aVlVw89z+bY1..."
}
```

### Signature Verification Algorithm (Guardian Layer 3)
To verify a message:
1. Strip the `"sig"` key from the dictionary.
2. Canonicalize the remaining dictionary using alphabetical key sorting and compact separators: `json.dumps(clean_dict, sort_keys=True, separators=(',', ':')).encode('utf-8')`.
3. Fetch the device public key via MCP `get_device_key(device_id)`.
4. Verify using standard Ed25519 cryptography.

Python verification snippet:
```python
import base64, json
from cryptography.hazmat.primitives.asymmetric import ed25519

def verify_twin_reading(payload: dict, pub_key_b64: str) -> bool:
    sig_b64 = payload["sig"]
    clean = {k: v for k, v in payload.items() if k != "sig"}
    canonical_bytes = json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")
    
    pub_bytes = base64.b64decode(pub_key_b64)
    public_key = ed25519.Ed25519PublicKey.from_public_bytes(pub_bytes)
    
    try:
        public_key.verify(base64.b64decode(sig_b64), canonical_bytes)
        return True
    except Exception:
        return False
```

---

## 3. Synchronous Command Server (MCP - Model Context Protocol)

### Connection Parameters
- **Protocol:** Official MCP Python SDK JSON-RPC
- **Transport:** SSE (Server-Sent Events) over HTTP
- **Endpoint:** `http://localhost:8000/sse` (or inside docker network `http://twin_layer:8000/sse`)
- **Messages Post Path:** `http://localhost:8000/messages/`

---

### A. Queries (Available to City Brain & Guardian)

#### 1. `list_devices`
Returns inventory of all registered physical devices, measurement units, bounds, and zone assignments.
- **Inputs:**
  - `zone_id` (`string`, optional): Filter devices by zone (e.g., `"zone-1"` to `"zone-6"`).
- **Output:** `Array<Object>`
  ```json
  [
    {
      "device_id": "z1-tc1",
      "zone_id": "zone-1",
      "device_type": "traffic_camera",
      "unit": "veh/min",
      "lat": 36.7964,
      "lon": 10.1755,
      "min_val": 0.0,
      "max_val": 200.0,
      "status": "ok",
      "actuator_state": { "signal_plan": "normal" }
    }
  ]
  ```

#### 2. `get_device_key`
Returns the Base64-encoded Ed25519 public signing key for a device.
- **Inputs:**
  - `device_id` (`string`, required): Identifier of target device (e.g., `"z1-tc1"`).
- **Output:** `string` (Base64-encoded raw 32-byte Ed25519 public key) or `null` if not found.
  ```text
  "m3Y8K8sQ1qJ3h...b=="
  ```

#### 3. `get_device_state`
Fetches current live telemetry, sequence number, and operational status of a single device.
- **Inputs:**
  - `device_id` (`string`, required): Device identifier.
- **Output:** `Object`
  ```json
  {
    "device_id": "z1-tc1",
    "zone_id": "zone-1",
    "device_type": "traffic_camera",
    "current_value": 47.8,
    "unit": "veh/min",
    "seq": 105,
    "status": "ok",
    "actuator_state": { "signal_plan": "normal" },
    "ts": "2026-10-17T12:00:10Z"
  }
  ```

#### 4. `get_city_state`
Returns city-wide summary across all 6 zones, total device counts, and active emergency dispatches.
- **Inputs:** None.
- **Output:** `Object`
  ```json
  {
    "sim_time": "2026-10-17T12:00:10Z",
    "total_devices": 42,
    "zones": {
      "zone-1": {
        "name": "Downtown Financial",
        "active_dispatches": 0,
        "devices": [ ... ]
      }
    }
  }
  ```

#### 5. `get_reading`
Retrieves a past published reading from the live broadcast cache by its unique `reading_id`.
- **Inputs:**
  - `reading_id` (`string`, required): Reading identifier (e.g., `"r-z1-tc1-105"`).
- **Output:** `Object` (The cached reading payload) or `null`.

#### 6. `get_readings`
Fetches recent published readings from the broadcast cache.
- **Inputs:**
  - `device_id` (`string`, optional): Filter by device ID.
  - `since_ts` (`string`, optional): RFC 3339 UTC timestamp filter (e.g. `"2026-10-17T12:00:00Z"`).
  - `limit` (`integer`, optional, default 50): Maximum number of readings to fetch.
- **Output:** `Array<Object>`

---

### B. Actuation Endpoints (Called by Layer 2 - City Brain)

#### 7. `dispatch`
Dispatches emergency response units to an incident in a designated zone.
- **Inputs:**
  - `incident_type` (`string`, required): E.g., `"medical_surge"`, `"flood"`, `"traffic_accident"`.
  - `zone_id` (`string`, required): E.g., `"zone-1"`.
  - `idempotency_key` (`string`, optional): Client-generated key for idempotent execution.
- **Output:** `Object`
  ```json
  {
    "status": "ok",
    "dispatch_id": "disp-9a1b2c3d",
    "zone_id": "zone-1"
  }
  ```

#### 8. `set_signal_plan`
Updates traffic signal plan, dynamically altering vehicle flow rates in next physics tick.
- **Inputs:**
  - `device_id` (`string`, required): Target `traffic_camera` device (e.g., `"z1-tc1"`).
  - `target_state` (`string`, required): Plan name: `"green_wave"`, `"normal"`, `"red_restriction"`.
  - `idempotency_key` (`string`, optional): Idempotent token.
- **Output:** `Object`
  ```json
  {
    "status": "ok",
    "device_id": "z1-tc1",
    "signal_plan": "green_wave"
  }
  ```

#### 9. `set_valve`
Actuates water drainage valve, altering reservoir outflow differential equations.
- **Inputs:**
  - `device_id` (`string`, required): Target `water_level` sensor/actuator (e.g., `"z1-wl1"`).
  - `target_state` (`string`, required): Valve state: `"open"`, `"closed"`, or ratio string like `"50%"`.
  - `idempotency_key` (`string`, optional): Idempotent token.
- **Output:** `Object`
  ```json
  {
    "status": "ok",
    "device_id": "z1-wl1",
    "valve_position": "open"
  }
  ```

#### 10. `switch_grid`
Switches electrical substation feeder routing.
- **Inputs:**
  - `device_id` (`string`, required): Target `power_load` substation (e.g., `"z1-pl1"`).
  - `target_state` (`string`, required): Grid state: `"grid_a"`, `"grid_b"`, `"shed"`, `"standby"`.
  - `idempotency_key` (`string`, optional): Idempotent token.
- **Output:** `Object`
  ```json
  {
    "status": "ok",
    "device_id": "z1-pl1",
    "grid_state": "grid_b"
  }
  ```

#### 11. `get_command_result`
Queries the cached execution result of an actuation command.
- **Inputs:**
  - `idempotency_key` (`string`, required): The key passed in actuation call.
- **Output:** `Object` or `null`.

---

### C. Containment Endpoints (Called by Layer 3 - Guardian)

#### 12. `isolate_device`
Marks a device status as `"isolated"`, dropping its physical actuation authority.
- **Inputs:** `device_id` (`string`, required)
- **Output:** `{"status": "ok", "device_id": "...", "new_status": "isolated"}`

#### 13. `quarantine_device`
Quarantines an anomalous, spoofed, or rogue device, causing subsequent Kafka telemetry to publish with `"status": "quarantined"`.
- **Inputs:** `device_id` (`string`, required)
- **Output:** `{"status": "ok", "device_id": "...", "new_status": "quarantined"}`

#### 14. `reinstate_device`
Restores an isolated or quarantined device back to operational status `"ok"`.
- **Inputs:** `device_id` (`string`, required)
- **Output:** `{"status": "ok", "device_id": "...", "new_status": "ok"}`

#### 15. `release_device`
Releases a device from containment back to operational status `"ok"`.
- **Inputs:** `device_id` (`string`, required)
- **Output:** `{"status": "ok", "device_id": "...", "new_status": "ok"}`

#### 16. `rollback_reading`
Marks a specific reading in the historical buffer as `"rolled_back"` following detection of falsification.
- **Inputs:** `reading_id` (`string`, required)
- **Output:** `{"status": "ok", "reading_id": "...", "reading_status": "rolled_back"}`

---

## 4. Device Inventory & Physical Specifications

| Zone | Device ID | Type | Bounds | Unit | Actuation Controls |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Zone 1** (Downtown Financial) | `z1-tc1`, `z1-tc2` | `traffic_camera` | 0 - 200 | veh/min | `set_signal_plan` |
| | `z1-wl1`, `z1-wl2` | `water_level` | 0 - 12 | meters | `set_valve` |
| | `z1-pl1` | `power_load` | 5 - 120 | MW | `switch_grid` |
| | `z1-aq1` | `air_quality` | 10 - 350 | AQI | - |
| | `z1-ec1` | `emergency_call` | 0 - 30 | call/min | `dispatch` |
| **Zone 2** (Port & Coastal Harbor) | `z2-tc1`, `z2-tc2` | `traffic_camera` | 0 - 200 | veh/min | `set_signal_plan` |
| | `z2-wl1`, `z2-wl2` | `water_level` | 0 - 12 | meters | `set_valve` |
| | `z2-pl1` | `power_load` | 5 - 120 | MW | `switch_grid` |
| | `z2-aq1` | `air_quality` | 10 - 350 | AQI | - |
| | `z2-ec1` | `emergency_call` | 0 - 30 | call/min | `dispatch` |
| **Zone 3** (North Industrial Park) | `z3-tc1`..`z3-ec1` | (same 5 types) | - | - | - |
| **Zone 4** (West Residential Hills)| `z4-tc1`..`z4-ec1` | (same 5 types) | - | - | - |
| **Zone 5** (University & Hospital) | `z5-tc1`..`z5-ec1` | (same 5 types) | - | - | - |
| **Zone 6** (South Logistics Hub)   | `z6-tc1`..`z6-ec1` | (same 5 types) | - | - | - |

*(Total: 42 devices across 6 zones, generating 420 msgs/sec at 10 Hz).*

---

## 5. Reference: Allowed Values, Error Codes & Auth

### Allowed Values & Parameter Constraints
- **Identifier Length Limit:** All string parameters (`device_id`, `zone_id`, `reading_id`, `idempotency_key`, `target_state`, `incident_type`) must not exceed 64 characters. Longer values return `INVALID_ARGUMENT`.
- **Zones:** `["zone-1", "zone-2", "zone-3", "zone-4", "zone-5", "zone-6"]`.
- **Incident Types (`dispatch`):** Must match `^[a-z_]{1,32}$`. Known types: `medical_surge`, `medical_emergency`, `flood`, `traffic_accident`, `fire`, `blackout`, `air_hazard`. Any other matching string is dispatched as generic and returns `"note": "unrecognized incident_type, dispatched as generic"`.
- **Traffic Signal Plans (`set_signal_plan`):** Allowed values are `["normal", "green_wave", "red_restriction", "all_red", "adaptive"]`.
- **Water Drainage Valves (`set_valve`):** Allowed values are `["open", "half", "closed", "0%-100%"]`. Ratios: `open` = 1.0, `half` = 0.5, `closed` = 0.0, `NN%` = NN/100.0.
- **Power Grid States (`switch_grid`):** Allowed values are `["grid_a", "grid_b", "island", "shed", "standby"]`.
- **Reading Queries (`get_readings`):** `since_ts` must parse as RFC 3339 UTC ending with `Z` (e.g. `2026-10-17T12:00:00Z`); `limit` must be an integer between 1 and 500.

### Uniform Error Response Format
All tools return errors adhering to this exact JSON structure:
```json
{
  "status": "error",
  "code": "<UPPER_SNAKE_CODE>",
  "message": "<Human-readable explanatory sentence>",
  "allowed": ["optional", "list", "of", "valid", "options"]
}
```

| Error Code | Description | Trigger Example |
| :--- | :--- | :--- |
| `DEVICE_NOT_FOUND` | Device ID does not exist in registered inventory. | `get_device_state("unknown-1")` |
| `ZONE_NOT_FOUND` | Zone ID does not exist. (Includes `allowed` field). | `dispatch("flood", "zone-99")` |
| `WRONG_DEVICE_TYPE` | Command invoked on incompatible device type. | `set_valve("z1-tc1", "open")` |
| `INVALID_TARGET_STATE` | Target state not recognized for device type. (Includes `allowed`). | `set_signal_plan("z1-tc1", "turbo")` |
| `INVALID_INCIDENT_TYPE` | Incident type syntax does not match `^[a-z_]{1,32}$`. | `dispatch("MEDICAL SURGE!", "zone-1")` |
| `INVALID_ARGUMENT` | Parameter exceeds 64 characters or numeric bound is invalid. | String length > 64 chars, or `limit=0` |
| `INVALID_TIMESTAMP` | Timestamp is not valid RFC 3339 UTC ending in `'Z'`. | `get_readings(since_ts="2026-10-17")` |
| `DEVICE_CONTAINED` | Target device is isolated or quarantined and refuses actuation. | `set_signal_plan` on quarantined camera |
| `COMMAND_NOT_FOUND` | Idempotency key not found in results cache. | `get_command_result("missing-key")` |
| `IDEMPOTENCY_CONFLICT` | Idempotency key was previously used with different tool or arguments. | Reusing key with altered payload |
| `READING_NOT_FOUND` | Reading ID not found in broadcast cache. | `get_reading("r-nonexistent-1")` |

> [!NOTE]
> **Exception for `get_device_key`:** To maintain compatibility with Guardian rogue detection logic (`if not pub_key`), `get_device_key` returns `null` (never an error object) for unknown or invalid device IDs.

### Authentication & Transports
- **Authentication:** Controlled by environment variable `MCP_AUTH_TOKENS="brain:<token>,guardian:<token>"`.
  - When unset: open access mode is enabled; Twin logs `[ALERT]` on startup; tool caller identity recorded as `"anonymous"`. It is strongly recommended to set `MCP_AUTH_TOKENS` whenever the Twin is reachable on a LAN.
  - When set: every HTTP request (`/sse`, `/messages/`, `/mcp`) requires header `Authorization: Bearer <token>`. Unauthorized requests return HTTP 401 with code `UNAUTHORIZED`.
  - Caller identity (`"brain"` or `"guardian"`) is verified in constant time and logged in truth journals.
- **Transports:**
  - SSE transport served at `/sse` (messages at `/messages/`).
  - Streamable HTTP transport served at `/mcp`.
  - Host/DNS rebinding protection: `MCP_ALLOWED_HOSTS` is unset (default) or `"*"`: DNS-rebinding protection is OFF (open LAN access). When set to a comma-separated list of hosts, protection is ON with that list.

---

## 6. Behaviour Changes in Rework

The following partner-visible behaviour changes apply to the Twin Layer:
1. `get_device_state` and `get_city_state` return the last broadcast value, not physical truth. `list_devices` never returned a value and is unchanged.
2. Actuation with an unknown `target_state`, or on an isolated or quarantined device, returns `{"status":"error",...}`.
3. Repeating an idempotency key returns the cached result without running the command again.
4. Public keys are stable across restarts, and `seq` continues across restarts.
5. Rogue messages now actually reach `twin.sensors`.
6. `/api/attack/*` moves off port 8000.
7. Zone coordinates are corrected onto land.
8. MCP gains optional bearer-token auth and an extra `/mcp` streamable-HTTP endpoint; `/sse` stays. Auth is off unless `MCP_AUTH_TOKENS` is set.
9. The root `docker-compose.yml` network is now named `city_network` (it was `twin_network`). This matters only for partners who attach their own containers to it.
10. `get_reading` for an unknown or expired id returns `{"status":"error","code":"READING_NOT_FOUND",...}` instead of `null`.
11. `get_device_state` and `get_command_result` return structured errors (`DEVICE_NOT_FOUND`, `COMMAND_NOT_FOUND`) instead of `null`. `get_device_key` still returns `null` for unknown ids.
12. Quarantining or isolating a rogue id blocks it at the network (repeat calls return ok); release_device or reinstate_device unblocks it.
13. `reinstate_device` and `release_device` both return isolated or quarantined devices to `ok`.
