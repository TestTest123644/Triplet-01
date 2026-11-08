# Twin layer interface card

The Twin layer (Layer 1) simulates 42 devices across Greater Tunis, streaming signed telemetry to Kafka and exposing command endpoints over Model Context Protocol (MCP).

## What the Twin sends

Broadcasts 420 msgs/s (10 Hz/device) to Kafka topic `twin.sensors`, keyed by `device_id` (<= 300 bytes/frame).
```json
{"reading_id":"r-z1-tc1-7559","device_id":"z1-tc1","device_type":"traffic_camera","ts":"2026-10-17T16:11:56Z","seq":7559,"value":87.54,"unit":"veh/min","location":"zone-1","status":"ok","sig":"gnSioLUV68nf/w5k7PGRnmHOe9vApO92dB93PeP5kkLb7JRAMsK6pj0s2kifn6IsAEZLglUdDNus+FTN2HBrBw=="}
```
Signing: Ed25519 signature over canonical UTF-8 JSON bytes (keys sorted alphabetically, compact separators, without `sig`).
Verification: Obtain the Base64-encoded public key for any device via `get_device_key(device_id)`.

## What the Twin accepts

Exposes 16 MCP tools over SSE (`http://localhost:8000/sse`) and Streamable HTTP (`/mcp`):
- Queries: `list_devices`, `get_device_key`, `get_device_state`, `get_city_state`, `get_reading`, `get_readings`
- Actuation: `dispatch`, `set_signal_plan`, `set_valve`, `switch_grid`, `get_command_result`
- Containment: `isolate_device`, `quarantine_device`, `reinstate_device`, `release_device`, `rollback_reading`

Example call and reply (`set_valve`):
```json
// Call: {"device_id": "z1-wl1", "target_state": "open"}
{"status": "ok", "device_id": "z1-wl1", "valve_position": "open"}
```

## How to repoint it

Configure partner broker and auth via environment variables:

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Comma-separated broker list |
| `KAFKA_TOPIC` | `twin.sensors` | Telemetry topic name (must already exist) |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Broker protocol: `PLAINTEXT`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `KAFKA_USERNAME` | *(unset)* | SASL username |
| `KAFKA_PASSWORD_FILE` | *(unset)* | Path to file containing SASL password |
| `MCP_AUTH_TOKENS_FILE` | *(unset)* | Path to file containing bearer tokens |

## How it authenticates

- MCP commands: Bearer tokens per principal (`principal:token,...` via `MCP_AUTH_TOKENS_FILE` or `MCP_AUTH_TOKENS`). Header: `Authorization: Bearer <token>`. Unset = open ("anonymous").
- Kafka streaming: SASL authentication (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`) when `KAFKA_SECURITY_PROTOCOL` is `SASL_*`.
- Red-team API: Header `X-Red-Team-Token` on dedicated port 8100. If unset, random token generated and saved to `/app/data/red_team.token`.

Full reference: docs/INTERFACE_REFERENCE.md
