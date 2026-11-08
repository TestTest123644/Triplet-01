# City Brain — interface card

One page: what the Brain reads, what it writes, how to reach it, and how to configure it.
Everything here is enforced by tests; the contracts live in `contracts/` and the message
schemas in `schemas/`.

## What it is

The Brain is the decision layer of triplet 07. It reads the Twin's sensor readings and the
Guardian's trust verdicts, decides what the city needs, and asks the Twin to do it. It never
trusts a reading the Guardian has not vouched for, it refuses more than it acts, and every
record it writes says what it rested on and what would have changed its mind.

One exception, and it is a configured one: with `guardian.optional: true`
(`config/triplet.yaml`, so the triplet can run before the Guardian is up) a reading from a
device reporting `status: ok` is trusted at full weight with no verdict behind it, and the
Brain never enters `GUARDIAN_DEGRADED`. Every record still names what it rested on, so an
implicitly trusted reading is marked `implicit_ok` and stays visible in the log. Set
`guardian.optional: false` to get the stricter posture back.

## Inputs

| Channel | Shape | Schema | Notes |
|---|---|---|---|
| `twin.sensors` | one signed reading per device per 100 ms | `schemas/partners/twin-triplet07.reading.schema.json` | Ed25519 per device key from `get_device_key` |
| `guardian.verdicts` | one verdict per reading | `schemas/guardian-verdict.v1.schema.json` | bound to the reading by `reading_id` + `sig_hash` |
| `guardian.events` | heartbeat, containment, verdict revocation | `schemas/guardian-event.v1.schema.json` | optional; a partner may publish none |

The names above are the Brain's stream names and the standalone defaults. On the triplet
network the topics are mapped in `config/triplet.yaml`: `readings` is **`telemetry-ingest`**,
consumed as group **`brain-processor`** over `SASL_PLAINTEXT` as user `brain`, while
`verdicts` and `events` keep their names. Remap any of them with
`BRAIN__KAFKA__TOPICS__READINGS` and friends: the stream name on the left is what the Brain's
code and logs use, so only the topic on the right changes.

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

Example `brain.events` record, a verification taken from a real `T02` run log:

```json
{"specversion":"1.0","type":"tn.tsyp.brain.verification.v1","id":"0-41",
 "source":"urn:tsyp14:triplet-07:brain","subject":"z3-wl1","epoch":0,"schemaversion":"1",
 "time":"2026-11-08T10:21:42.080449+00:00","simtime":"2026-10-17T12:03:07.000000Z",
 "actionid":"act-1563f88e62468f79","correlationid":"flood-risk:z3-wl1:1",
 "datacontenttype":"application/json",
 "data":{"expected":{"channel":"z3-wl1","metric":"rate","comparator":"<=",
                     "threshold":-0.01,"window_s":3.0},
         "observed":{"slope":-0.03108,"since_tick":1826,"excluded_ticks":[]},
         "verdict":"verified"},
 "prev_hash":"2deff836841c49197a27fd44c1d4bee96f653ab01ae7b5afe8caf2500e620e46",
 "hash":"4b7f8fdfb736f72a7f82e514a03ab0ba0b71864d8d89ee5122ec1d427733fd7a",
 "sig":"<base64 ed25519>"}
```

A `command` record carries the same envelope with `data.shield` (every check and whether it
passed), `data.risk_class`, `data.status` and `data.twin_reply`, so a refusal reads as
clearly as an action. The decision, incident, alert and status records share the envelope and
differ only in `data`; `schemas/brain-event.v1.schema.json` is the authority.

Example command to the Twin, named as the partner profile names it:

```json
{"tool":"set_valve",
 "args":{"device_id":"z3-wl1","target_state":"open",
         "idempotency_key":"brain-0-act-1563f88e62468f79"}}
```

Commands reach the Twin as MCP tool calls, named by the partner profile: `set_valve`,
`set_signal_plan`, `switch_grid`, `dispatch`. Every call carries an `idempotency_key`
(`brain-<epoch>-<action_id>`), and the Brain never resends: a lost reply becomes `UNKNOWN`,
the target freezes, and the operator is told.

## Ports

| Port | What | Auth |
|---|---|---|
| `8200` | REST query and operator API (`/v1/status`, `/v1/zones/{zone}`, `/v1/incidents`, `/v1/actions/{id}`, `/v1/explain/{id}`, `/metrics`) | read-only endpoints are open |
| `8200` | `POST /v1/approvals/{action_id}`, `POST /v1/control/{pause,resume}`, `POST /v1/ingest/{stream}` | `Authorization: Bearer <operator token>` |
| `8110` | read-only MCP server (`get_zone_state`, `list_incidents`, `explain_decision`) | none; it cannot change anything |
| `8120` | Timeline UI and run browser | the operator token is entered in the page |

The REST port is `BRAIN_REST_PORT` (default `8200`) and the MCP port `BRAIN_MCP_PORT`
(default `8110`). The REST API used to be on `8100`; it moved because the Twin publishes its
red-team adversary API on host `8100`, so anything still pointed at `8100` reaches the Twin
rather than the Brain.

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
| `twin.auth_token` | `MCP_AUTH_TOKEN` | — | bearer token the Twin's MCP server requires |
| `twin.auth_token_file` | `MCP_AUTH_TOKENS_FILE` | — | the same token from a file, Docker-secret style |
| `guardian.profile` | `BRAIN__GUARDIAN__PROFILE` | `guardian-triplet07` | verdict vocabulary |
| `guardian.public_key_file` | `BRAIN__GUARDIAN__PUBLIC_KEY_FILE` | — | the key verdicts are checked against |
| `policy_file` | — | `policy/default.yaml` | thresholds, bands, catalog, assessor |
| `params_file` | — | `params/twin-triplet07-v1.yaml` | filter and estimator tuning |
| `logs_dir` | `BRAIN__LOGS_DIR` | `logs` | run logs and summaries |
| `signing_key` | `BRAIN__SIGNING_KEY` | — | base64 Ed25519 key for `brain.events` |
| — | `BRAIN_OPERATOR_TOKEN` | — | the bearer token the operator endpoints require |
| — | `BRAIN_REST_PORT` | `8200` | REST query and operator API |
| — | `BRAIN_MCP_PORT` | `8110` | read-only MCP server |

Besides `BRAIN__…`, the Brain reads the triplet's shared variable names, so one `.env` can
configure all three layers. These override the YAML file:

| Env | Config key | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.bootstrap` | the brokers |
| `KAFKA_AUTO_OFFSET_RESET` | `kafka.offset_reset` | `earliest` on the triplet, so nothing is missed |
| `KAFKA_SECURITY_PROTOCOL` | `kafka.security_protocol` | `PLAINTEXT`, `SASL_PLAINTEXT` or `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | `kafka.sasl_mechanism` | `PLAIN` |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | `kafka.sasl_username`, `…_password` | the broker principal |
| `KAFKA_PASSWORD_FILE` | `kafka.sasl_password_file` | the password from a file |
| `KAFKA_SSL_CA_LOCATION` | `kafka.ssl_ca_location` | for `SASL_SSL` |
| `KAFKA_ACKS` | `kafka.acks` | producer acks |
| `MCP_AUTH_TOKEN` | `twin.auth_token` | the Twin's bearer token |
| `MCP_AUTH_TOKENS` | `twin.auth_token` | the shared `brain:<tok>,guardian:<tok>` form; the Brain takes its own entry |
| `MCP_AUTH_TOKENS_FILE` | `twin.auth_token` | that same list from a file |

An empty variable is not a value. Compose defaults such as
`KAFKA_PASSWORD: ${KAFKA_PASSWORD:-}` arrive as empty strings, and the Brain ignores them
rather than letting them overwrite the YAML file.

Where a secret can come from two places, the file wins, matching the triplet's
`.env.example`: `KAFKA_PASSWORD_FILE` takes precedence over `KAFKA_PASSWORD`, and
`MCP_AUTH_TOKENS_FILE` over `MCP_AUTH_TOKENS`. `MCP_AUTH_TOKEN` is the Brain's own override
and beats both. A token file may hold the shared `brain:<tok>,guardian:<tok>` list or
nothing but the token itself. If SASL is configured with a username and no password can be
resolved, the Brain says so at `WARNING` instead of letting the broker reject it silently.

The output topic is the one thing not configurable: the Brain always publishes to
`brain.events`.

Authentication: Ed25519 for everything on the wire — the Twin signs readings per device, the
Guardian signs verdicts and events with one key, and the Brain signs its own records, so no
message is believed for coming from the right place. Two bearer tokens sit outside that, and
they authenticate a deployment rather than a message: `MCP_AUTH_TOKEN`, which the Twin's MCP
server requires of the Brain, and `BRAIN_OPERATOR_TOKEN`, without which no endpoint changes
the Brain's behaviour. Supply both by environment variable or file, and commit neither.

## Rule R1

The Brain acts only on evidence it can name. Every command cites the readings behind it,
every reading carries the Guardian's verdict, and every record says what would have changed
the decision. If the evidence is missing, stale, contained or doubted, the Brain holds and
says so — and if holding is itself dangerous, it asks the operator rather than deciding
alone.
