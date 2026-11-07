#!/bin/sh
set -eu
umask 077

read_secret() {
  secret_file="$1"
  secret_value="$(cat "$secret_file")"

  # Hexadecimal passwords avoid JAAS quoting and backslash problems.
  case "$secret_value" in
    ''|*[!0123456789abcdefABCDEF]*)
      echo "ERROR: $secret_file must contain a non-empty hexadecimal password." >&2
      exit 1
      ;;
  esac

  printf '%s' "$secret_value"
}

KAFKA_BROKER_PASSWORD="$(read_secret /run/secrets/kafka_broker_password)"
KAFKA_ADMIN_PASSWORD="$(read_secret /run/secrets/kafka_admin_password)"
KAFKA_CLIENT_PASSWORD="$(read_secret /run/secrets/kafka_client_password)"
KAFKA_TWIN_PASSWORD="$(read_secret /run/secrets/kafka_twin_password)"
KAFKA_BRAIN_PASSWORD="$(read_secret /run/secrets/kafka_brain_password)"
KAFKA_BROADCAST_PASSWORD="$(read_secret /run/secrets/kafka_broadcast_password)"
KAFKA_ACTION_TAKER_PASSWORD="$(read_secret /run/secrets/kafka_action_taker_password)"
JAAS_FILE=/tmp/kafka_server_jaas.conf

cat > "$JAAS_FILE" <<JAAS
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="broker"
  password="${KAFKA_BROKER_PASSWORD}"
  user_broker="${KAFKA_BROKER_PASSWORD}"
  user_admin="${KAFKA_ADMIN_PASSWORD}"
  user_guardian="${KAFKA_CLIENT_PASSWORD}"
  user_twin="${KAFKA_TWIN_PASSWORD}"
  user_brain="${KAFKA_BRAIN_PASSWORD}"
  user_broadcast="${KAFKA_BROADCAST_PASSWORD}"
  user_brain="${KAFKA_BRAIN_PASSWORD}"
  user_action_taker="${KAFKA_ACTION_TAKER_PASSWORD}";
};
controller.KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="broker"
  password="${KAFKA_BROKER_PASSWORD}"
  user_broker="${KAFKA_BROKER_PASSWORD}"
  user_admin="${KAFKA_ADMIN_PASSWORD}"
  user_guardian="${KAFKA_CLIENT_PASSWORD}";
};
KafkaClient {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="broker"
  password="${KAFKA_BROKER_PASSWORD}";
};
JAAS

unset KAFKA_BROKER_PASSWORD KAFKA_ADMIN_PASSWORD KAFKA_CLIENT_PASSWORD KAFKA_TWIN_PASSWORD KAFKA_BRAIN_PASSWORD KAFKA_BROADCAST_PASSWORD KAFKA_ACTION_TAKER_PASSWORD
unset KAFKA_BROKER_PASSWORD KAFKA_ADMIN_PASSWORD KAFKA_CLIENT_PASSWORD KAFKA_TWIN_PASSWORD KAFKA_BRAIN_PASSWORD

export KAFKA_OPTS="${KAFKA_OPTS:-} -Djava.security.auth.login.config=$JAAS_FILE"

# Preserve the official Apache image's configuration and storage initialization.
exec /etc/kafka/docker/run
