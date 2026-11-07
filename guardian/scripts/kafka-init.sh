#!/bin/sh
set -eu
umask 077

ADMIN_PASSWORD="$(cat /run/secrets/kafka_admin_password)"

case "$ADMIN_PASSWORD" in
  ''|*[!0123456789abcdefABCDEF]*)
    echo "ERROR: Admin password must be non-empty hexadecimal." >&2
    exit 1
    ;;
esac

ADMIN_CONFIG=/tmp/kafka-admin.properties
trap 'rm -f "$ADMIN_CONFIG"' EXIT
trap 'exit 1' HUP INT TERM

cat > "$ADMIN_CONFIG" <<PROPERTIES
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="${ADMIN_PASSWORD}";
PROPERTIES

unset ADMIN_PASSWORD

BOOTSTRAP="kafka-1:19094"
MAX_ATTEMPTS=60

echo "Waiting for all three Kafka brokers..."
attempt=0

while :; do
  ready=true

  for broker in kafka-1:19094 kafka-2:19094 kafka-3:19094; do
    if ! /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server "$broker" \
      --command-config "$ADMIN_CONFIG" \
      >/dev/null 2>&1; then
      ready=false
      break
    fi
  done

  if [ "$ready" = true ]; then
    break
  fi

  attempt=$((attempt + 1))
  if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    echo "ERROR: All three Kafka brokers did not become ready within 5 minutes." >&2
    exit 1
  fi

  sleep 5
done

echo "All three Kafka broker endpoints respond."

create_topic() {
  topic="$1"
  partitions="$2"

  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$ADMIN_CONFIG" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 3
}

set_topic_config() {
  topic="$1"
  config="$2"

  /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$ADMIN_CONFIG" \
    --alter \
    --entity-type topics \
    --entity-name "$topic" \
    --add-config "$config"
}

echo "Creating topics if needed..."
create_topic telemetry-ingest 6
create_topic video-frames-ingest 4
create_topic telemetry-validated 6
create_topic telemetry-dlq 3
create_topic devices.metadata 3
create_topic telemetry-anomaly 3
create_topic brain.events 3
create_topic twin.sensors 6

echo "Applying topic configurations..."
set_topic_config telemetry-ingest \
  "retention.ms=604800000,min.insync.replicas=2"

set_topic_config video-frames-ingest \
  "max.message.bytes=5242880,retention.ms=3600000,min.insync.replicas=2"

set_topic_config telemetry-validated \
  "retention.ms=604800000,min.insync.replicas=2"

set_topic_config telemetry-dlq \
  "retention.ms=2592000000,min.insync.replicas=2"

set_topic_config devices.metadata \
  "cleanup.policy=compact,min.insync.replicas=2"

set_topic_config telemetry-anomaly \
  "min.insync.replicas=2"

echo "Granting Guardian input-topic permissions..."
for topic in telemetry-ingest video-frames-ingest devices.metadata; do
  /opt/kafka/bin/kafka-acls.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal User:guardian \
    --operation Read \
    --operation Describe \
    --topic "$topic"
done

echo "Granting Guardian output-topic permissions..."
for topic in telemetry-validated telemetry-dlq telemetry-anomaly; do
  /opt/kafka/bin/kafka-acls.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal User:guardian \
    --operation Write \
    --operation Describe \
    --topic "$topic"
done

# action_taker only reads: it must never get Write on any topic
echo "Granting action_taker read permissions..."
for topic in telemetry-dlq telemetry-anomaly; do
  /opt/kafka/bin/kafka-acls.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal User:action_taker \
    --operation Read \
    --operation Describe \
    --topic "$topic"
done

/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" \
  --add \
  --allow-principal User:action_taker \
  --operation Read \
  --group action-taker-processor

echo "Granting Guardian consumer-group permissions..."
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" \
  --add \
  --allow-principal User:guardian \
  --operation Read \
  --group guardian-processor

echo "Verifying topic partition replication..."
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" \
  --describe

echo "Granting twin write permissions..."
for topic in telemetry-ingest video-frames-ingest; do
  /opt/kafka/bin/kafka-acls.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal User:twin \
    --operation Write \
    --operation Describe \
    --topic "$topic"
done

echo "Granting brain read permissions..."
for topic in telemetry-ingest video-frames-ingest twin.sensors telemetry-validated; do
  /opt/kafka/bin/kafka-acls.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal User:brain \
    --operation Read \
    --operation Describe \
    --topic "$topic"
done

echo "Granting brain write permissions..."
for topic in brain.events; do
  /opt/kafka/bin/kafka-acls.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal User:brain \
    --operation Write \
    --operation Describe \
    --topic "$topic"
done

/opt/kafka/bin/kafka-acls.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal User:broadcast \
    --operation Write \
    --operation Describe \
    --topic devices.metadata

/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" \
  --add \
  --allow-principal User:brain \
  --operation Read \
  --group brain-processor


echo "Kafka topic configurations and Guardian ACLs applied."

echo "Current ACLs:"
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN_CONFIG" \
  --list