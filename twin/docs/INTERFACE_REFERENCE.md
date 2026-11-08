# Twin Layer Interface Reference

This document provides the full reference specification for all 16 Model Context Protocol (MCP) tools, device inventory, parameter constraints, and error codes exposed by the Twin Layer (Layer 1).

---

## 1. Connection Parameters

- Protocol: Official Model Context Protocol (MCP) Python SDK JSON-RPC
- Transports:
  - SSE (Server-Sent Events) over HTTP: `http://localhost:8000/sse` (messages posted to `/messages/`)
  - Streamable HTTP: `http://localhost:8000/mcp`
- Docker internal address: `http://twin_layer:8000/sse`
- Authentication: When `MCP_AUTH_TOKENS` or `MCP_AUTH_TOKENS_FILE` is configured, calls require `Authorization: Bearer <token>`. Unauthorized MCP calls return HTTP 401 UNAUTHORIZED. When unset, access is open ("anonymous").
- Kafka topics: The Twin never creates topics (`KAFKA_TOPIC` must already exist on the broker).

---

## 2. MCP Tools Reference

### A. Queries

#### 1. `list_devices`
Returns inventory of all registered physical devices, measurement units, bounds, and zone assignments.
- Inputs:
  - `zone_id` (`string`, optional): Filter devices by zone (e.g., `"zone-1"` to `"zone-6"`).
- Output: `Array<Object>`
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
- Inputs:
  - `device_id` (`string`, required): Identifier of target device (e.g., `"z1-tc1"`).
- Output: `string` (Base64-encoded raw 32-byte Ed25519 public key) or `null` if not found.
  ```text
  "m3Y8K8sQ1qJ3h...b=="
  ```

#### 3. `get_device_state`
Fetches current live telemetry, sequence number, and operational status of a single device.
- Inputs:
  - `device_id` (`string`, required): Device identifier.
- Output: `Object`
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
- Inputs: None.
- Output: `Object`
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
- Inputs:
  - `reading_id` (`string`, required): Reading identifier (e.g., `"r-z1-tc1-105"`).
- Output: `Object` (The cached reading payload) or error object `READING_NOT_FOUND`.

#### 6. `get_readings`
Fetches recent published readings from the broadcast cache.
- Inputs:
  - `device_id` (`string`, optional): Filter by device ID.
  - `since_ts` (`string`, optional): RFC 3339 UTC timestamp filter (e.g. `"2026-10-17T12:00:00Z"`).
  - `limit` (`integer`, optional, default 50): Maximum number of readings to fetch.
- Output: `Array<Object>`

---

### B. Actuation Endpoints

#### 7. `dispatch`
Dispatches emergency response units to an incident in a designated zone.
- Inputs:
  - `incident_type` (`string`, required): E.g., `"medical_surge"`, `"flood"`, `"traffic_accident"`.
  - `zone_id` (`string`, required): E.g., `"zone-1"`.
  - `idempotency_key` (`string`, optional): Client-generated key for idempotent execution.
- Output: `Object`
  ```json
  {
    "status": "ok",
    "dispatch_id": "disp-9a1b2c3d",
    "zone_id": "zone-1"
  }
  ```

#### 8. `set_signal_plan`
Updates traffic signal plan, dynamically altering vehicle flow rates in next physics tick.
- Inputs:
  - `device_id` (`string`, required): Target `traffic_camera` device (e.g., `"z1-tc1"`).
  - `target_state` (`string`, required): Plan name: `"green_wave"`, `"normal"`, `"red_restriction"`.
  - `idempotency_key` (`string`, optional): Idempotent token.
- Output: `Object`
  ```json
  {
    "status": "ok",
    "device_id": "z1-tc1",
    "signal_plan": "green_wave"
  }
  ```

#### 9. `set_valve`
Actuates water drainage valve, altering reservoir outflow differential equations.
- Inputs:
  - `device_id` (`string`, required): Target `water_level` sensor/actuator (e.g., `"z1-wl1"`).
  - `target_state` (`string`, required): Valve state: `"open"`, `"half"`, `"closed"`, or ratio string like `"50%"`.
  - `idempotency_key` (`string`, optional): Idempotent token.
- Output: `Object`
  ```json
  {
    "status": "ok",
    "device_id": "z1-wl1",
    "valve_position": "open"
  }
  ```

#### 10. `switch_grid`
Switches electrical substation feeder routing.
- Inputs:
  - `device_id` (`string`, required): Target `power_load` substation (e.g., `"z1-pl1"`).
  - `target_state` (`string`, required): Grid state: `"grid_a"`, `"grid_b"`, `"shed"`, `"standby"`.
  - `idempotency_key` (`string`, optional): Idempotent token.
- Output: `Object`
  ```json
  {
    "status": "ok",
    "device_id": "z1-pl1",
    "grid_state": "grid_b"
  }
  ```

#### 11. `get_command_result`
Queries the cached execution result of an actuation command.
- Inputs:
  - `idempotency_key` (`string`, required): The key passed in actuation call.
- Output: `Object` or error object `COMMAND_NOT_FOUND`.

---

### C. Containment Endpoints

#### 12. `isolate_device`
Marks a device status as `"isolated"`, dropping its physical actuation authority.
- Inputs: `device_id` (`string`, required)
- Output: `{"status": "ok", "device_id": "...", "new_status": "isolated"}`

#### 13. `quarantine_device`
Quarantines an anomalous, spoofed, or rogue device, causing subsequent Kafka telemetry to publish with `"status": "quarantined"`.
- Inputs: `device_id` (`string`, required)
- Output: `{"status": "ok", "device_id": "...", "new_status": "quarantined"}`

#### 14. `reinstate_device`
Restores an isolated or quarantined device back to operational status `"ok"`.
- Inputs: `device_id` (`string`, required)
- Output: `{"status": "ok", "device_id": "...", "new_status": "ok"}`

#### 15. `release_device`
Releases a device from containment back to operational status `"ok"`.
- Inputs: `device_id` (`string`, required)
- Output: `{"status": "ok", "device_id": "...", "new_status": "ok"}`

#### 16. `rollback_reading`
Marks a specific reading in the historical buffer as `"rolled_back"` following detection of falsification.
- Inputs: `reading_id` (`string`, required)
- Output: `{"status": "ok", "reading_id": "...", "reading_status": "rolled_back"}`

---

## 3. Device Inventory and Physical Specifications

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

---

## 4. Parameter Constraints and Error Codes

- Identifier limit: Strings (`device_id`, `zone_id`, `reading_id`, `idempotency_key`, `target_state`, `incident_type`) must not exceed 64 characters.
- Zones: `zone-1` through `zone-6`.
- Incident types (`dispatch`): Pattern `^[a-z_]{1,32}$`. Known: `medical_surge`, `medical_emergency`, `flood`, `traffic_accident`, `fire`, `blackout`, `air_hazard`.
- Signal plans (`set_signal_plan`): `["normal", "green_wave", "red_restriction", "all_red", "adaptive"]`.
- Valve states (`set_valve`): `["open", "half", "closed", "0%-100%"]`.
- Grid states (`switch_grid`): `["grid_a", "grid_b", "island", "shed", "standby"]`.

Error response schema:
```json
{
  "status": "error",
  "code": "<UPPER_SNAKE_CODE>",
  "message": "<Human-readable explanatory sentence>",
  "allowed": ["optional", "list", "of", "valid", "options"]
}
```

| Error Code | Trigger Example |
| :--- | :--- |
| `DEVICE_NOT_FOUND` | `get_device_state("unknown-1")` |
| `ZONE_NOT_FOUND` | `dispatch("flood", "zone-99")` |
| `WRONG_DEVICE_TYPE` | `set_valve("z1-tc1", "open")` |
| `INVALID_TARGET_STATE` | `set_signal_plan("z1-tc1", "turbo")` |
| `INVALID_INCIDENT_TYPE` | `dispatch("EMERGENCY!", "zone-1")` |
| `INVALID_ARGUMENT` | Parameter length > 64 chars or `limit=0` |
| `INVALID_TIMESTAMP` | `get_readings(since_ts="bad-ts")` |
| `DEVICE_CONTAINED` | Actuation on isolated or quarantined device |
| `COMMAND_NOT_FOUND` | `get_command_result("missing-key")` |
| `IDEMPOTENCY_CONFLICT` | Key reused with different tool or arguments |
| `READING_NOT_FOUND` | `get_reading("r-nonexistent-1")` |

---

## 5. Secrets and Runtime Rotation

The Twin manages three operational secrets: `KAFKA_PASSWORD`, `MCP_AUTH_TOKENS`, and `RED_TEAM_TOKEN`. Ed25519 device signing keys (`KEYSTORE_PATH`) are static identity keys; they never rotate at runtime because partner services verify telemetry against keys returned by `get_device_key`.

### Secret resolution order

When resolving any secret name `<NAME>`, the Twin checks sources in order and uses the first non-empty value:

1. **Python hook (`TWIN_SECRET_HOOK="package.module:function"`):**
   The function is invoked with the secret name and returns the secret string or `None`. Uncaught exceptions and missing modules are logged as warnings and fall through without leaking secret data.
2. **File (`<NAME>_FILE`):**
   Reads file path from environment variable (`KAFKA_PASSWORD_FILE`, `MCP_AUTH_TOKENS_FILE`, `RED_TEAM_TOKEN_FILE`). Strips one trailing newline. Works with HashiCorp Vault Agent, Kubernetes secrets, or Docker secrets.
3. **Environment variable (`<NAME>`):**
   Reads `<NAME>` directly from the process environment (`KAFKA_PASSWORD`, `MCP_AUTH_TOKENS`, `RED_TEAM_TOKEN`).

### Example HashiCorp Vault KV v2 hook

A partner team using HashiCorp Vault can supply secrets via a hook without installing third-party libraries:

```python
import os, json, urllib.request

def vault_hook(name: str) -> str | None:
    addr = os.getenv("VAULT_ADDR", "http://127.0.0.1:8200")
    token = os.getenv("VAULT_TOKEN", "")
    path = os.getenv("VAULT_SECRET_PATH", "secret/data/twin")
    req = urllib.request.Request(f"{addr}/v1/{path}", headers={"X-Vault-Token": token})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("data", {}).get("data", {}).get(name)
    except Exception:
        return None
```

Set `TWIN_SECRET_HOOK="my_module:vault_hook"` to enable it.

### Reloading secrets at runtime

Secrets can be reloaded without process restarts or losing simulator state:

- **REST endpoint:** `POST http://localhost:8100/api/secrets/reload`
  - Authentication: Header `X-Red-Team-Token` (checked against the existing active token before rotation).
  - Response payload:
    ```json
    {
      "status": "ok",
      "results": {
        "KAFKA_PASSWORD": "unchanged",
        "MCP_AUTH_TOKENS": "changed",
        "RED_TEAM_TOKEN": "unchanged"
      },
      "reloaded_at_sim": "2026-10-17T16:15:30Z",
      "reloaded_at_wall": "2026-11-06T19:00:00Z"
    }
    ```
- **POSIX signal:** Send `SIGHUP` to the Twin process (`kill -HUP <pid>`).
- **Periodic polling:** Set `TWIN_SECRET_RELOAD_SECONDS=<seconds>` to trigger automated periodic checks in the background.

During reload:
- MCP tokens swap atomically; in-flight calls complete with the prior token map.
- Red-team API token swaps immediately after validating the reload request.
- Kafka producer validates new credentials against the broker with a metadata probe before swapping; if authentication fails, the active producer is preserved and `"error: new credentials rejected by broker"` is reported. Message sequence continuity is maintained across successful swaps (`seq` numbers increment continuously).
- Rotation events are written to the SQLite ledger `secret_rotations` table and visible via `GET /api/journal`.

---

## 6. Environment and Compose Configuration

All runtime networking, authentication, and port bindings are configurable via environment variables without editing compose files.

### Configuration reference

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` (`localhost:19092` host) | Comma-separated broker list (`h1:p,h2:p,h3:p`) |
| `KAFKA_TOPIC` | `twin.sensors` | Telemetry topic name (must already exist; Twin does not create topics) |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Broker protocol: `PLAINTEXT`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | `PLAIN` | SASL auth mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `KAFKA_USERNAME` | *(unset)* | SASL username (required when protocol is `SASL_*`) |
| `KAFKA_PASSWORD_FILE` | *(unset)* | Path to file containing SASL password (48 hex chars from `openssl rand -hex 24`) |
| `KAFKA_PASSWORD` | *(unset)* | SASL plaintext password (used if `KAFKA_PASSWORD_FILE` unset) |
| `KAFKA_SSL_CA_LOCATION` | *(unset)* | Path to CA certificate file (optional for `SASL_SSL`) |
| `KAFKA_ACKS` | `all` | Producer durability: `all` (with idempotence), `1`, or `0` |
| `KAFKA_BROKER_ADDRESS_FAMILY` | `any` | Address family: `any`, `v4`, `v6` (use `v4` to suppress IPv6 connection refused logs) |
| `WAIT_FOR_KAFKA` | `true` | Skip broker describe check on startup if `false` (useful under restrictive broker ACLs) |
| `MCP_HOST` / `MCP_PORT` | `0.0.0.0` / `8000` | MCP server binding address |
| `TWIN_MCP_HOST_PORT` | `8000` | Host port mapped to container port 8000 (MCP command server) |
| `MCP_AUTH_TOKENS` | *(unset)* | Bearer tokens `principal:token,...` (off by default; callers logged as "anonymous") |
| `MCP_AUTH_TOKENS_FILE` | *(unset)* | Path to file containing bearer tokens (takes priority over `MCP_AUTH_TOKENS`) |
| `RED_TEAM_PORT` | `8100` | Dedicated red-team attack injection REST port |
| `TWIN_RED_TEAM_HOST_PORT` | `8100` | Host port mapped to container port 8100 (Red Team REST API) |
| `TWIN_CONSOLE_HOST_PORT` | `8501` | Host port mapped to container port 8501 (Streamlit operations console) |
| `RED_TEAM_TOKEN` | *(unset)* | Red-team API token literal |
| `RED_TEAM_TOKEN_FILE` | *(unset)* | Path to red-team token file (compose writes to `/app/data/red_team.token`) |
| `TWIN_SECRET_HOOK` | *(unset)* | Pluggable secret resolver `module:func` called as `func(name) -> str | None` |
| `TWIN_SECRET_RELOAD_SECONDS` | *(unset)* | Periodic background reload interval in seconds (optional) |
| `TRIPLET_NETWORK` | `city_network` | Docker bridge network name (e.g. `shared-backend` for Guardian) |
| `TRIPLET_NETWORK_EXTERNAL` | `false` | Set `true` to connect to an existing external Docker network |
| `TWIN_SECRETS_DIR` | `./secrets` | Host secrets directory mounted read-only to `/app/secrets:ro` (`../secrets` in `twin/`) |
| `VAULT_ADDR` | *(unset)* | Vault cluster URL (e.g. `https://<cluster>.hashicorp.cloud:8200`; unset = disabled) |
| `VAULT_NAMESPACE` | `admin` | Vault namespace header (default `admin`; empty string = no header) |
| `VAULT_ROLE_ID_FILE` | *(unset)* | Path to AppRole Role ID file (e.g. `./secrets/vault_twin_role_id`; container `/app/secrets/vault_twin_role_id`) |
| `VAULT_SECRET_ID_FILE` | *(unset)* | Path to AppRole Secret ID file (e.g. `./secrets/vault_twin_secret_id`; container `/app/secrets/vault_twin_secret_id`) |
| `VAULT_CACERT` | *(unset)* | Path to custom CA bundle (unset = certifi or system default; verification never disabled) |
| `VAULT_REFRESH_SECONDS` | `300` | Background key refresh interval in seconds (0 = disabled) |
| `SIM_SPEED` | `1.0` | Simulation speed multiplier (pass `20.0` for scenarios) |

### Path resolution: host vs container

When configuring `KAFKA_PASSWORD_FILE`, `MCP_AUTH_TOKENS_FILE`, `VAULT_ROLE_ID_FILE`, or `VAULT_SECRET_ID_FILE`:
- **Running on host (`python -m twin.main`):** Use the host relative or absolute path (e.g. `KAFKA_PASSWORD_FILE=./secrets/kafka_password`, `VAULT_ROLE_ID_FILE=./secrets/vault_twin_role_id`, `VAULT_SECRET_ID_FILE=./secrets/vault_twin_secret_id`).
- **Running in Docker Compose:** The host directory `${TWIN_SECRETS_DIR:-./secrets}` is mounted to `/app/secrets:ro`. Configure the container-internal path (e.g. `KAFKA_PASSWORD_FILE=/app/secrets/kafka_password`, `VAULT_ROLE_ID_FILE=/app/secrets/vault_twin_role_id`, `VAULT_SECRET_ID_FILE=/app/secrets/vault_twin_secret_id`).

### HashiCorp Vault Key Synchronization

The Twin optionally fetches and maintains dynamic communication keys from HashiCorp Vault KV v2:
- **Paths synchronized:** `comm/twin/guardian/private`, `comm/twin/brain/private`, `comm/guardian/twin/public`, `comm/brain/twin/public`.
- **Formats supported:** PEM (`{"key": "<PEM>"}`) and kid (`{"kid", "private_key"|"public_key", "fingerprint", "active_after"}`).
- **Key algorithms:** Automatically detects X25519, Ed25519, and RSA keys.
- **Rotation:** Private keys with future `active_after` pick version-1; public keys preserve latest and previous versions.
- **Status inspection:** Query `GET /api/status` for the `vault` metadata block.
- **Traffic usage note:** Communication keys are fetched and kept fresh in memory but not yet used for any traffic encryption.

---

## 7. Behaviour Changes in Rework

1. `get_device_state` and `get_city_state` return the last broadcast value, not physical truth.
2. Actuation with unknown `target_state`, or on isolated/quarantined devices, returns structured error.
3. Repeating an idempotency key returns cached output without re-executing.
4. Public keys and sequence counters persist across restarts.
5. Rogue messages reach `twin.sensors` wire topic.
6. Red-team REST API moved to isolated port 8100.
7. Zone 2 coordinates placed on land in La Goulette.
8. MCP supports optional bearer auth and adds `/mcp` streamable HTTP transport alongside `/sse`.
9. Root compose network named `city_network`.
10. `get_reading` for missing ID returns `READING_NOT_FOUND` error object instead of null.
11. `get_device_state` and `get_command_result` return structured errors instead of null. `get_device_key` returns null for unknown devices.
12. Containment on rogue IDs blocks subsequent wire frames.
13. `reinstate_device` and `release_device` both restore devices to status `ok`.
14. Diurnal physical kinetics: power load peaks at mid-day, traffic peaks at rush hours, water valves affect water levels.
15. Water level is continuous without artificial sensor jitter.
16. Containment state persists across restarts.




