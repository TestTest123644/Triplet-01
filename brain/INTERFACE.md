# City Brain — interface card

One page: what the Brain reads, what it writes, how to reach it, and how to configure it.
Everything here is enforced by tests; the contracts live in `contracts/` and the message
schemas in `schemas/`.

## What it is

The Brain is the decision layer of triplet 07. It reads the Twin's sensor readings and the
Guardian's trust verdicts, decides what the city needs, and asks the Twin to do it. It never
trusts a reading the Guardian has not vouched for, it refuses more than it acts, and every
record it writes says what it rested on and what would have changed its mind.

## Inputs

| Channel | Shape | Schema | Notes |
|---|---|---|---|
| `twin.sensors` | one signed reading per device per 100 ms | `schemas/partners/twin-triplet07.reading.schema.json` | Ed25519 per device key from `get_device_key` |
| `guardian.verdicts` | one verdict per reading | `schemas/guardian-verdict.v1.schema.json` | bound to the reading by `reading_id` + `sig_hash` |
| `guardian.events` | heartbeat, containment, verdict revocation | `schemas/guardian-event.v1.schema.json` | optional; a partner may publish none |

Example reading:

```json
{"reading_id":"r-z1-wl1-1914","device_id":"z1-wl1","device_type":"water_level",
 "ts":"2026-10-17T12:03:11Z","seq":1914,"value":6.02,"unit":"meters",
 "location":"zone-1","status":"ok","sig":"<base64 ed25519>"}
```

Example verdict:

```json
{"verdict_id":"v-000412","reading_id":"r-z1-wl1-1914","sig_hash":"0f3a1c9b2d4e5f60",
 "device_id":"z1-wl1","state":"TRUSTED","trust":1.0,"reasons":[],
 "issued_at":"2026-10-17T12:03:11Z","expires_at":"2026-10-17T12:03:41Z","sig":"<base64>"}
```

Example guardian event:

```json
{"kind":"containment","device_id":"z2-wl1","action":"quarantined",
 "effective_at":"2026-10-17T12:00:05Z","reasons":["CROSS_SENSOR_CONFLICT"],
 "issued_at":"2026-10-17T12:00:05Z","sig":"<base64>"}
```

A message that does not verify is rejected at the edge with a reason code
(`MALFORMED_JSON`, `SCHEMA_VIOLATION`, `BAD_SIGNATURE`, `UNKNOWN_DEVICE`, `TYPE_MISMATCH`,
`OVERSIZED`, `UNKNOWN_VERDICT_STATE`) and counted; it never reaches the decision core.

## Outputs

`brain.events` — one CloudEvents 1.0 record per decision, command, verification, alert,
incident and status change (`schemas/brain-event.v1.schema.json`). Each record carries
`prev_hash` and `hash` (SHA-256 chain) and, when the Brain holds a signing key, `sig`
(Ed25519 over everything but `hash`). Verify a run log with:

```bash
python -m city_brain.adapters.files.jsonl_log --verify <log.jsonl> --pubkey <base64>
```

Commands reach the Twin as MCP tool calls, named by the partner profile: `set_valve`,
`set_signal_plan`, `switch_grid`, `dispatch`. Every call carries an `idempotency_key`
(`brain-<epoch>-<action_id>`), and the Brain never resends: a lost reply becomes `UNKNOWN`,
the target freezes, and the operator is told.

## Ports

| Port | What | Auth |
|---|---|---|
| `8100` | REST query and operator API (`/v1/status`, `/v1/zones/{zone}`, `/v1/incidents`, `/v1/actions/{id}`, `/v1/explain/{id}`, `/metrics`) | read-only endpoints are open |
| `8100` | `POST /v1/approvals/{action_id}`, `POST /v1/control/{pause,resume}`, `POST /v1/ingest/{stream}` | `Authorization: Bearer <operator token>` |
| `8110` | read-only MCP server (`get_zone_state`, `list_incidents`, `explain_decision`) | none; it cannot change anything |
| `8120` | Timeline UI and run browser | the operator token is entered in the page |

`contracts/openapi.json` is the REST contract; `contracts/asyncapi.yaml` the channels.

## Configuration

Every key can be set in a YAML config (`config/standalone.yaml`, `config/triplet.yaml`) or
overridden by an environment variable of the form `BRAIN__SECTION__KEY`.

| Key | Env | Default | Meaning |
|---|---|---|---|
| `mode` | `BRAIN__MODE` | — | `standalone` or `triplet` |
| `kafka.bootstrap` | `BRAIN__KAFKA__BOOTSTRAP` | `kafka:9092` | the broker |
| `kafka.group` | `BRAIN__KAFKA__GROUP` | `city-brain` | consumer group |
| `kafka.topics.readings` | `BRAIN__KAFKA__TOPICS__READINGS` | `twin.sensors` | input topic |
| `kafka.topics.verdicts` | `BRAIN__KAFKA__TOPICS__VERDICTS` | `guardian.verdicts` | input topic |
| `kafka.topics.events` | `BRAIN__KAFKA__TOPICS__EVENTS` | `guardian.events` | input topic |
| `twin.profile` | `BRAIN__TWIN__PROFILE` | `twin-triplet07` | partner profile in `profiles/` |
| `twin.mcp_url` | `BRAIN__TWIN__MCP_URL` | `http://twin_layer:8000/sse` | the Twin's MCP SSE endpoint |
| `twin.call_timeout_s` | `BRAIN__TWIN__CALL_TIMEOUT_S` | `2.0` | a slower reply is `UNKNOWN` |
| `guardian.profile` | `BRAIN__GUARDIAN__PROFILE` | `guardian-triplet07` | verdict vocabulary |
| `guardian.public_key_file` | `BRAIN__GUARDIAN__PUBLIC_KEY_FILE` | — | the key verdicts are checked against |
| `policy_file` | — | `policy/default.yaml` | thresholds, bands, catalog, assessor |
| `params_file` | — | `params/twin-triplet07-v1.yaml` | filter and estimator tuning |
| `logs_dir` | `BRAIN__LOGS_DIR` | `logs` | run logs and summaries |
| `signing_key` | `BRAIN__SIGNING_KEY` | — | base64 Ed25519 key for `brain.events` |
| — | `BRAIN_OPERATOR_TOKEN` | — | the bearer token the operator endpoints require |

Authentication is Ed25519 throughout: the Twin signs readings per device, the Guardian signs
verdicts and events with one key, and the Brain signs its own records. No shared secrets, no
symmetric keys, and no endpoint that changes the Brain's behaviour without the operator token.

## Rule R1

The Brain acts only on evidence it can name. Every command cites the readings behind it,
every reading carries the Guardian's verdict, and every record says what would have changed
the decision. If the evidence is missing, stale, contained or doubted, the Brain holds and
says so — and if holding is itself dangerous, it asks the operator rather than deciding
alone.
