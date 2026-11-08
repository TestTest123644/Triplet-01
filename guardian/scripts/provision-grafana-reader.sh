#!/bin/sh
set -eu

read_secret() {
  secret_file="$1"
  if [ ! -f "$secret_file" ]; then
    echo "ERROR: missing Docker secret $secret_file" >&2
    exit 1
  fi

  secret_value="$(tr -d '\r\n' < "$secret_file")"
  if [ -z "$secret_value" ]; then
    echo "ERROR: $secret_file must contain a non-empty password." >&2
    exit 1
  fi
  printf '%s' "$secret_value"
}

POSTGRES_PASSWORD="$(read_secret /run/secrets/postgres_password)"
POSTGRES_GRAFANA_PASSWORD="$(read_secret /run/secrets/postgres_grafana_password)"
if [ "$POSTGRES_USER" = "$POSTGRES_GRAFANA_USER" ]; then
  echo "ERROR: POSTGRES_GRAFANA_USER must differ from POSTGRES_USER." >&2
  exit 1
fi
export PGPASSWORD="$POSTGRES_PASSWORD"
export POSTGRES_GRAFANA_PASSWORD

psql --host postgres --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=reader_user="$POSTGRES_GRAFANA_USER" \
  --set=database="$POSTGRES_DB" \
  --set=owner="$POSTGRES_USER" <<'SQL'
\getenv reader_password POSTGRES_GRAFANA_PASSWORD
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'reader_user', :'reader_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'reader_user')
\gexec

ALTER ROLE :"reader_user" WITH LOGIN PASSWORD :'reader_password'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
ALTER ROLE :"reader_user" SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE :"database" TO :"reader_user";
GRANT USAGE ON SCHEMA public TO :"reader_user";
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM :"reader_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA public
  REVOKE SELECT ON TABLES FROM :"reader_user";

CREATE TABLE IF NOT EXISTS public.raw_readings (
  id BIGSERIAL PRIMARY KEY,
  sensor_id VARCHAR(255) NOT NULL,
  cluster_id VARCHAR(255),
  timestamp TIMESTAMPTZ NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  unit VARCHAR(64) NOT NULL,
  metadata JSONB,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_raw_readings_sensor_timestamp ON public.raw_readings (sensor_id, timestamp);

CREATE TABLE IF NOT EXISTS public.processed_readings (
  id BIGSERIAL PRIMARY KEY,
  sensor_id VARCHAR(255) NOT NULL,
  cluster_id VARCHAR(255),
  timestamp TIMESTAMPTZ NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  unit VARCHAR(64) NOT NULL,
  metadata JSONB,
  quality_flags JSONB,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_processed_readings_sensor_timestamp ON public.processed_readings (sensor_id, timestamp);

CREATE TABLE IF NOT EXISTS public.transformed_windows (
  id BIGSERIAL PRIMARY KEY,
  sensor_id VARCHAR(255) NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  features JSONB NOT NULL,
  transformed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_transformed_windows_sensor_end ON public.transformed_windows (sensor_id, window_end);

CREATE TABLE IF NOT EXISTS public.model_outputs (
  id BIGSERIAL PRIMARY KEY,
  sensor_id VARCHAR(255) NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  score DOUBLE PRECISION NOT NULL,
  is_anomaly BOOLEAN NOT NULL,
  model_version VARCHAR(64) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_model_outputs_sensor_end ON public.model_outputs (sensor_id, window_end);

GRANT SELECT ON TABLE public.raw_readings, public.processed_readings, public.transformed_windows, public.model_outputs TO :"reader_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"reader_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA public
  GRANT SELECT ON TABLES TO :"reader_user";
SQL
