#!/bin/sh
set -eu

read_secret() {
  SECRET_FILE="/run/secrets/$1"
  if [ ! -f "$SECRET_FILE" ]; then
    echo "ERROR: missing Docker secret $SECRET_FILE" >&2
    exit 1
  fi

  SECRET_VALUE="$(tr -d '\r\n' < "$SECRET_FILE")"
  if [ -z "$SECRET_VALUE" ]; then
    echo "ERROR: $SECRET_FILE must contain a non-empty password." >&2
    exit 1
  fi
  printf '%s' "$SECRET_VALUE"
}

POSTGRES_GRAFANA_PASSWORD="$(read_secret postgres_grafana_password)"
ELASTIC_GRAFANA_PASSWORD="$(read_secret elastic_grafana_password)"
export POSTGRES_GRAFANA_PASSWORD ELASTIC_GRAFANA_PASSWORD

exec /run.sh "$@"
