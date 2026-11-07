# Twin layer interface card

The Twin layer (Layer 1) simulates 42 devices across Greater Tunis, streaming signed telemetry to Kafka and exposing command endpoints over Model Context Protocol (MCP). Full reference in [docs/INTERFACE_REFERENCE.md](docs/INTERFACE_REFERENCE.md).

---

## What the Twin sends (Kafka `twin.sensors`)

Broadcasts 420 messages/second (10 Hz per device) to Kafka topic `twin.sensors`. Messages are keyed by `device_id` so each device stays in order within its partition. The topic `twin.sensors` must already exist on the broker (the Twin never creates topics or alters broker settings). Every payload is strictly <= 300 bytes and digitally signed with Ed25519.

Real telemetry payload:
```json
{
  "reading_id": "r-z1-tc1-7559",
  "device_id": "z1-tc1",
  "device_type": "traffic_camera",
  "ts": "2026-10-17T16:11:56Z",
  "seq": 7559,
  "value": 87.54,
  "unit": "veh/min",
  "location": "zone-1",
  "status": "ok",
  "sig": "gnSioLUV68nf/w5k7PGRnmHOe9vApO92dB93PeP5kkLb7JRAMsK6pj0s2kifn6IsAEZLglUdDNus+FTN2HBrBw=="
}
```

---

## What the Twin accepts (MCP on port 8000)

Exposes 16 MCP tools over SSE (`http://localhost:8000/sse`) and Streamable HTTP (`/mcp`):

| Tool | Purpose |
| :--- | :--- |
| `list_devices` | List registered devices, measurement units, bounds, and zone mappings |
| `get_device_key` | Return Base64-encoded Ed25519 public key (returns null if unknown) |
| `get_device_state` | Fetch last broadcast telemetry reading and operational status |
| `get_city_state` | Return city-wide overview across all 6 zones and active dispatches |
| `get_reading` | Fetch past reading from broadcast cache by reading ID |
| `get_readings` | Query historical broadcast buffer by device, timestamp, and limit |
| `dispatch` | Deploy emergency units (medical, fire, flood, traffic, blackout) |
| `set_signal_plan` | Update traffic plan (normal, green_wave, red_restriction, adaptive) |
| `set_valve` | Adjust reservoir drainage valve position (open, half, closed, 0%-100%) |
| `switch_grid` | Route power grid substation (grid_a, grid_b, island, shed, standby) |
| `get_command_result`| Retrieve cached result of previous actuation by idempotency key |
| `isolate_device` | Isolate compromised device and drop physical actuation authority |
| `quarantine_device` | Blacklist device at network level and tag future telemetry as quarantined |
| `reinstate_device` | Return isolated or quarantined device back to operational status ok |
| `release_device` | Release device from containment back to operational status ok |
| `rollback_reading` | Mark poisoned reading as rolled_back in broadcast cache |

Real call and reply (`set_valve`):
```json
// Call: {"device_id": "z1-wl1", "target_state": "open"}
{
  "status": "ok",
  "device_id": "z1-wl1",
  "valve_position": "open"
}
```

Error response shape:
```json
{
  "status": "error",
  "code": "DEVICE_NOT_FOUND",
  "message": "Device 'unknown-dev' not found."
}
```

---

## Configuration and partner addresses

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Comma-separated broker list (`h1:p,h2:p,h3:p` or container `kafka:9092`) |
| `KAFKA_TOPIC` | `twin.sensors` | Telemetry topic name (must already exist; Twin never creates topics) |
| `KAFKA_ACKS` | `all` | Producer durability: `all` (with idempotence), `1`, or `0` |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Broker protocol: `PLAINTEXT`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | `PLAIN` | SASL auth mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `KAFKA_USERNAME` | *(unset)* | SASL username |
| `KAFKA_PASSWORD` | *(unset)* | SASL password literal |
| `KAFKA_PASSWORD_FILE` | *(unset)* | Path to file containing SASL password (takes priority) |
| `KAFKA_SSL_CA_LOCATION` | *(unset)* | Path to CA certificate file (optional, only used for `SASL_SSL`) |
| `KAFKA_BROKER_ADDRESS_FAMILY` | `any` | Address family: `any`, `v4`, `v6` (use `v4` to suppress IPv6 connection refused logs) |
| `WAIT_FOR_KAFKA` | `true` | Set `false` to skip describe cluster check if broker ACLs disallow it |
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
| `TRIPLET_NETWORK_EXTERNAL` | `false` | Set `true` to connect to an existing external Docker network without compose edits |
| `TWIN_SECRETS_DIR` | `./secrets` | Host secrets directory mounted read-only to `/app/secrets:ro` (`../secrets` in `twin/`) |
| `SIM_SPEED` | `1.0` | Simulation speed multiplier (pass `20.0` for scenarios) |

---

## Authentication

1. **Telemetry signatures (Ed25519):** Partners fetch the public key via `get_device_key(device_id)`. Clean payload without `sig` key, sort keys alphabetically with compact JSON separators `(",", ":")`, and verify signature bytes against canonical UTF-8 bytes.
2. **MCP command auth:** Set `MCP_AUTH_TOKENS="brain:tok-b,guardian:tok-g,scenario:tok-s"`. When set, callers must provide HTTP header `Authorization: Bearer <token>`. Unauthorized calls return HTTP 401 `UNAUTHORIZED`. When unset (default in compose), auth is disabled and callers are logged as "anonymous".
3. **Red-team API auth:** Header `X-Red-Team-Token`. If unset, the Twin generates a random token; in compose it is saved to `/app/data/red_team.token`.

---

## Secrets and rotation

Secrets (`KAFKA_PASSWORD`, `MCP_AUTH_TOKENS`, `RED_TEAM_TOKEN`) resolve via: (1) `TWIN_SECRET_HOOK="pkg.mod:func"` (Vault or custom secret store), (2) file `<NAME>_FILE` (Vault Agent, Docker secret, k8s), (3) env `<NAME>`. Device signing keys (`KEYSTORE_PATH`) never rotate at runtime.
Reload secrets without restarting via `POST http://localhost:8100/api/secrets/reload` (header `X-Red-Team-Token` checked against old token), sending `kill -HUP <twin_pid>`, or setting `TWIN_SECRET_RELOAD_SECONDS`.

---

## Behaviour changes in rework

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
