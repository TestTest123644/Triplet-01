# action_taker

Consumes `telemetry-dlq` (records Flink rejected) and `telemetry-anomaly` (Guardian's
anomaly detections) and contains devices through the twin's MCP server
(`http://<host>:8000/sse`). Issue #116.

## What it does

One message at a time: parse → decide → call MCP → commit the offset.

| Signal | Action |
|---|---|
| DLQ `E200_*`, `E201_*` (forged or unknown source) | `isolate_device`, plus `rollback_reading` if the record has a `reading_id` |
| DLQ `E3xx_*` / `E4xx_*` (schema / validation) `repeat_threshold` times for one device within `repeat_window_seconds` | `quarantine_device` |
| Anomaly `spoofed_data`, `not_sensor_data` | `rollback_reading` if there is a `reading_id`; also `isolate_device` once repeated `repeat_threshold` times within the window |
| Anomaly `out_of_range`, `sensor_malfunction`, `sensor_is_down` | `quarantine_device` |
| Anomaly with `"label": null` (back to normal), device isolated or quarantined | `reinstate_device` |
| Anything else (`E100_*`, `E500_*`, unknown) | nothing; logged and committed |

Anomaly message format:

```json
{"device_id": "z1-tc1", "reading_id": "r-z1-tc1-105", "label": "spoofed_data", "detected_at": "2026-11-04T10:00:00+00:00"}
```

`label` is one of `out_of_range`, `spoofed_data`, `not_sensor_data`,
`sensor_malfunction`, `sensor_is_down`, or `null`. `reading_id` may be `null`.

Delivery is at-least-once:

- The offset is committed only after every action of the message got an answer from
  MCP, or the message was skipped on purpose (unparseable, no rule, or the twin
  answered with an error such as "Device not found", which a retry would not change).
- While MCP is unreachable (after `mcp.retry_attempts` tries), the message is rewound,
  consumption pauses for `kafka.pause_seconds`, and the same message is tried again.
- Repeating an action is harmless: the twin's containment tools only set a status, and
  `reinstate_device` is called only after `get_device_state` shows the device contained.
- SIGTERM: the current message is finished and committed, then the consumer closes.
  During an MCP outage, shutdown stops the retries and leaves the message uncommitted;
  it is redelivered on the next start.

## Configuration

`utils/settings.yaml`, or the file named by `ACTION_TAKER_CONFIG`. A missing file is an
error. Unknown keys are rejected.

| Key | Default | Meaning |
|---|---|---|
| `kafka.bootstrap_servers` | `kafka-1:19094,kafka-2:19094,kafka-3:19094` | brokers |
| `kafka.username` | `action_taker` | SASL/PLAIN user |
| `kafka.password_file` | `/run/secrets/kafka_action_taker_password` | file holding the password (never put the password in the YAML) |
| `kafka.group_id` | `action-taker-processor` | consumer group |
| `kafka.dlq_topic` / `kafka.anomaly_topic` | `telemetry-dlq` / `telemetry-anomaly` | input topics |
| `kafka.auto_offset_reset` | `earliest` | where a new group starts |
| `kafka.pause_seconds` | `5.0` | pause while MCP is down |
| `mcp.url` | `http://host.docker.internal:8000/sse` | MCP SSE endpoint |
| `mcp.timeout` | `3.0` | seconds per MCP call |
| `mcp.retry_attempts` | `5` | tries per call before pausing |
| `mcp.retry_backoff_seconds` | `0.5` | exponential backoff multiplier |
| `rules.repeat_threshold` | `3` | hits that count as "repeated" |
| `rules.repeat_window_seconds` | `60.0` | window for those hits |
| `dry_run` | `true` | log the intended action, make no MCP call |

Environment: `GUARDIAN_LAYER_LOG=INFO` (the shared logger defaults to WARNING, which
hides the action log lines).

## Secrets

```bash
openssl rand -hex 24 | tr -d '\r\n' > secrets/kafka_action_taker_password.txt
chmod 644 secrets/kafka_action_taker_password.txt
```

The password must be hexadecimal or the brokers refuse to start. Strip `\r` too: on
Windows `openssl` ends the line with `\r\n`.

After adding the user, recreate the brokers, then apply the ACLs:

```bash
docker compose up -d --force-recreate kafka-1 kafka-2 kafka-3
docker compose up kafka-init
```

## Run

```bash
docker compose build action-taker
docker compose up -d action-taker
docker compose logs -f action-taker
```

The log shows `joined group action-taker-processor, assigned telemetry-dlq[0], ...`.
The twin's MCP server must be running on the host
(`python -m twin.mcp_server` in the triplet-07 repository, port 8000).

### Dry run and replay

Messages handled in dry run are committed, so switching `dry_run` off does not replay
them. To replay, stop the service and reset the group:

```bash
docker compose stop action-taker
docker compose exec kafka-1 sh /tmp/admin.sh kafka-consumer-groups.sh --reset-offsets --to-earliest --group action-taker-processor --all-topics --execute
```

(`/tmp/admin.sh` is created in the next section.)

### Requirements

`requirements.txt` is exported from `uv.lock`; regenerate it after dependency changes:

```bash
uv export --no-dev --no-hashes --no-emit-project -o src/guardian_layer/consumer_producer/action_taker/requirements.txt
```

`--no-emit-project` matters: without it the file starts with `-e .`, which cannot be
installed inside the image.

## Testing with kafka-console-producer

Inside `kafka-1`, create a helper that runs a Kafka CLI tool as a given user (it reads
the password from the broker's mounted secrets; delete the files afterwards):

```bash
docker compose exec kafka-1 sh -c 'for user in admin:kafka_admin_password guardian:kafka_client_password action_taker:kafka_action_taker_password; do
  name=${user%%:*}; secret=${user#*:}
  printf "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";\n" "$name" "$(cat /run/secrets/$secret)" > /tmp/$name.properties
done
printf "%s\n" "tool=\$1; shift; exec /opt/kafka/bin/\$tool --bootstrap-server kafka-1:19094 --command-config /tmp/admin.properties \"\$@\"" > /tmp/admin.sh'
```

1. ACLs, and a denied write:

   ```bash
   docker compose exec kafka-1 sh /tmp/admin.sh kafka-acls.sh --list --principal User:action_taker
   echo '{}' | docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:19094 --producer.config /tmp/action_taker.properties --producer-property enable.idempotence=false --topic telemetry-dlq
   ```

   Expected: Read/Describe on both topics and Read on the group only; the write fails
   with `TopicAuthorizationException`.

   `--producer-property enable.idempotence=false` is needed for every console producer
   here: the idempotent default asks for a cluster-level permission no client user
   has, and fails with `ClusterAuthorizationException` before the topic ACL is checked.

   In Git Bash, run these with `MSYS_NO_PATHCONV=1` set, or it rewrites `/tmp/...`
   into a Windows path.

2. An anomaly published as `guardian`:

   ```bash
   echo '{"device_id": "z1-tc1", "reading_id": null, "label": "out_of_range", "detected_at": "2026-11-04T10:00:00+00:00"}' | docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:19094 --producer.config /tmp/guardian.properties --producer-property enable.idempotence=false --topic telemetry-anomaly
   ```

   Expected log: `action tool=quarantine_device target=z1-tc1 ... result=dry_run` in dry
   run, `result=done` with `dry_run: false`; MCP `get_device_state("z1-tc1")` then shows
   `"status": "quarantined"`.

3. Back to normal: the same command with `"label": null` → `reinstate_device ... done`.

4. A malformed message (`echo '{not json'` to either topic) → `skipping invalid
   message topic=... partition=... offset=...`; the next valid message is still handled.

5. MCP outage: stop the MCP server, send the step 2 message → retries, then
   `MCP unavailable at ..., pausing`; start MCP → the action runs.
   `docker compose exec kafka-1 sh /tmp/admin.sh kafka-consumer-groups.sh --describe --group action-taker-processor`
   shows the offset moving only after that.

6. Send the step 2 message again → `done` again, no error.

Clean up: `docker compose exec kafka-1 rm -f /tmp/admin.properties /tmp/guardian.properties /tmp/action_taker.properties /tmp/admin.sh`.

## Tests

```bash
uv run pytest tests/action_taker
```
