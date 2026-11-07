# Guardian Layer

## 1. Prerequisites

Guardian Layer uses Docker Compose to run PostgreSQL, a three-broker Apache Kafka cluster, and Elasticsearch for application logs.

Requirements:

- Docker Engine or Docker Desktop running.
- Docker Compose.
- Python and `uv` to run the project and its tests.
- OpenSSL to generate secure Kafka passwords.

## 2. Environment Configuration

### Create the environment file

Create a local `.env` file from the example:

```bash
cp .env.example .env
```

The `.env.example` file documents the environment variables used by the project. Configure the Elasticsearch password values when needed.

The following variables are documented:

| Variable | Purpose |
|---|---|
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_DB` | PostgreSQL database name |
| `ELASTIC_PASSWORD` | Password for Elasticsearch's built-in `elastic` user in Compose |
| `ELASTICSEARCH_PASSWORD` | Password used by the Python Elasticsearch logger |
| `GUARDIAN_ELASTICSEARCH_URL` | Elasticsearch endpoint for application logs; an empty value disables Elasticsearch logging |

Set `ELASTICSEARCH_PASSWORD` to the same value as `ELASTIC_PASSWORD` when the Python logger connects to the Elasticsearch service.

**Important:** The Compose configuration requires `ELASTIC_PASSWORD`. Because Compose validates the configuration as a whole, commands that start other services may also require this variable to be set.

### Required secret files

Passwords for PostgreSQL and Kafka are stored in local files under `secrets/`, not directly in `.env`.

The following secret files are required:

| File | Purpose |
|---|---|
| `secrets/postgres_password.txt` | PostgreSQL password |
| `secrets/kafka_broker_password.txt` | Kafka broker-to-broker authentication |
| `secrets/kafka_admin_password.txt` | Kafka administration and initialization |
| `secrets/kafka_client_password.txt` | Guardian application client authentication |

Create these files locally if they do not already exist. Generate a hexadecimal password with:

```bash
openssl rand -hex 32
```

Use a different password for each Kafka account. Kafka passwords must contain only hexadecimal characters and must not be empty.

Never commit the `secrets/` directory or share its contents. The directory must be excluded by `.gitignore`.

**Important:** If PostgreSQL has already initialized its data volume, changing `secrets/postgres_password.txt` does not automatically change the password stored in PostgreSQL. Keep the existing password unless you intentionally update the database credentials too.

## 3. PostgreSQL

The database uses the `postgres:16-alpine` image.

### Start PostgreSQL

```bash
docker compose up -d postgres
```

PostgreSQL listens on `127.0.0.1:5432` on the host and stores its data in the persistent `postgres_data` volume.

### Password configuration

PostgreSQL uses the password stored in `secrets/postgres_password.txt`. The Compose configuration reads it through the Docker secret `postgres_password`.

Do not set `POSTGRES_PASSWORD` in `.env` for this configuration. The PostgreSQL password is supplied through the secret file.

The application reads its database connection from the `GUARDIAN_DATABASE_URL` variable.

- **From the host:** connect to `127.0.0.1:5432`.
- **From another Compose service:** connect to `postgres:5432`.

Use the configured PostgreSQL username and database name, along with the password stored locally in `secrets/postgres_password.txt`, when constructing the connection URL. Do not commit real credentials to Git.

If the password contains special URL characters, percent-encode them when constructing the URL.

PostgreSQL initializes its user and database when the data volume is empty. Changing the initialization settings later does not automatically update an existing database password.

**Warning:** `docker compose down -v` deletes persistent volumes and can permanently remove database data. Use it only when you intentionally want to delete that data.

### Making a service wait for PostgreSQL

PostgreSQL has a healthcheck based on `pg_isready`.

A Compose service that depends on PostgreSQL can wait for the database to become healthy:

```yaml
services:
  my-service:
    depends_on:
      postgres:
        condition: service_healthy
```

If a service also depends on Kafka, configure its Kafka dependencies separately. The `depends_on` directive alone does not guarantee that Kafka is ready to accept authenticated requests. A Kafka readiness check is still required.

## 4. Kafka Cluster

The Kafka infrastructure uses `apache/kafka:4.3.1` in KRaft mode. KRaft allows Kafka to manage cluster metadata without a separate ZooKeeper service.

### Cluster topology

The cluster contains three Kafka brokers:

| Service | Node ID | Host port |
|---|---:|---:|
| `kafka-1` | 1 | `29094` |
| `kafka-2` | 2 | `29095` |
| `kafka-3` | 3 | `29096` |

The host ports are bound to localhost. Within the Docker Compose network, brokers communicate using their service names and port `19094`.

Each broker has its own persistent volume:

- `kafka1_data`
- `kafka2_data`
- `kafka3_data`

### Start the cluster

First, create the required secret files described in Section 2 and configure the required environment variables.

Start the services:

```bash
docker compose up -d
```

Check the service status:

```bash
docker compose ps
```

Inspect the initialization logs:

```bash
docker compose logs kafka-init
```

The initialization service waits for Kafka to become available, creates the topics if they do not exist, and configures application permissions. It exits after successful initialization.

### Topics

The initialization service creates the following topics:

| Topic | Partitions | Replication factor |
|---|---:|---:|
| `telemetry-ingest` | 6 | 3 |
| `video-frames-ingest` | 4 | 3 |

A replication factor of 3 means each partition has replicas distributed across the three brokers.

The configuration sets `min.insync.replicas` to 2. For writes using `acks=all`, Kafka requires at least two in-sync replicas to acknowledge the write.

This configuration is designed to tolerate one broker failure when the remaining replicas are healthy and in sync. Actual failover behavior must be verified in a running Docker environment.

### Authentication and authorization

Kafka clients use SASL/PLAIN authentication.

- `broker` is used for broker-to-broker authentication.
- `admin` is used by the initialization service.
- `guardian` is the application producer account.

The initialization script configures the following permissions for the `guardian` application account:

- `Read` and `Describe` on `telemetry-ingest`.
- `Read` and `Describe` on `video-frames-ingest`.
- `Read` on the `guardian-processor` consumer group.

The `broker` and `admin` accounts are configured as Kafka superusers.

**Note:** If the cluster was initialized previously with broader permissions, inspect existing Kafka ACLs and remove obsolete permissions carefully. Updating the initialization script does not automatically remove ACLs already stored in Kafka.

### Connect from the host

Use these bootstrap servers for applications running on the host:

```text
localhost:29094,localhost:29095,localhost:29096
```

A producer client configuration should include:

```properties
bootstrap.servers=localhost:29094,localhost:29095,localhost:29096
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="guardian" password="<KAFKA_CLIENT_PASSWORD>";
```

Replace `<KAFKA_CLIENT_PASSWORD>` with the value stored locally in `secrets/kafka_client_password.txt`. Do not commit real passwords or credentials to Git.

**Security note:** `SASL_PLAINTEXT` provides authentication but does not encrypt network traffic. For production or untrusted networks, configure TLS and use `SASL_SSL`.

## 5. Log Storage (Elasticsearch)

The stack includes an Elasticsearch service for application logs. The logging integration is designed to run in the background so application logging does not block normal application work during ordinary operation.

### Start Elasticsearch

```bash
docker compose up -d elasticsearch
```

Set `ELASTIC_PASSWORD` in `.env` to a strong password. Pass the same value to the Python process as `ELASTICSEARCH_PASSWORD`; the logger authenticates as the `elastic` user by default.

Elasticsearch security is enabled, and its HTTP port is published on localhost only. HTTP TLS is disabled for this local Compose setup, so do not expose the service beyond a trusted development machine.

### Logger configuration

The logger sends records only when `GUARDIAN_ELASTICSEARCH_URL` is set to a non-empty endpoint.

Use:

- `http://localhost:9200` when Python runs on the host.
- `http://elasticsearch:9200` from a container on the Compose network.

Endpoint, batching, retry, and shutdown settings live in the `elasticsearch` section of `config/settings.yaml`. The endpoint can be overridden by environment variables, while credentials remain in environment variables.

Python does not load `.env` files automatically. Export the required environment variables when running the application directly from the host.

Set `GUARDIAN_LAYER_LOG=INFO` to include informational events; warnings and errors are logged by default.

### Logging behavior

Records are serialized before entering a bounded in-memory queue and sent by a lazily started background worker in short batches.

- Transient connection errors, HTTP 429, and HTTP 5xx responses are retried using exponential backoff.
- During an outage, errors written to stderr are rate-limited.
- Dropped records are counted by the handler.
- `flush()` and `logging.shutdown()` wait for queued events up to the configured shutdown timeout.
- Each event receives a stable ID so a retried batch does not create duplicate documents.
- Context is stored as JSON text to avoid Elasticsearch mapping conflicts when callers use the same context key with different value types.

The documents are indexed in the `guardian-logs` index.

Example:

```python
from guardian_layer.utils import get_logger

logger = get_logger(__name__)
logger.info("service started", component="guardian")
```

## 6. Tests and Validation

### Run the infrastructure tests

```bash
uv run pytest tests/infra/test_compose.py -q
```

### Run the complete test suite

```bash
uv run pytest tests -v
```

### Incoming sensor verification

`BufferManager.add` and `BufferManager.create` verify readings before they enter a
buffer. Rejected readings return all validation failures and are logged with the
sensor ID, type, and reason; repeated log messages for the same sensor are rate-limited.
Readings outside their physical or declared sensor range are buffered with an
`out_of_range` quality flag for downstream anomaly handling.

General checks require finite values and supplied ranges, increasing bounds when both
are present, non-blank sensor/cluster/unit fields, a non-future timestamp, a
non-negative packet order, and valid paired coordinates. A single declared range
bound is allowed. Incoming types are `water_level`, `power_load`, and `air_quality`;
unknown types are rejected. `water_level` uses unit `cm` and physical bounds 0–300.
Type-specific rules for `power_load` and `air_quality` are not configured yet.

To add or update a rule, add the type to `SensorType` in
`src/guardian_layer/utils/schema.py` and add its unit and physical bounds to
`SENSOR_RULES` in `src/guardian_layer/utils/sensor_verification.py`.

### Validate the Docker Compose configuration

```bash
docker compose config
```

Compose configuration validation requires the variables referenced by the Compose file to be set. For a local syntax check, if `ELASTIC_PASSWORD` is otherwise unset, you can use a temporary value:

```bash
ELASTIC_PASSWORD=temporary-validation-value docker compose config -q
```

This command validates the Compose configuration; it does not start containers or verify that the supplied password is suitable for a real deployment.

Python tests validate configuration structure and expected settings. They do not prove that Kafka starts successfully, authentication works at runtime, Elasticsearch accepts logs, or broker failover succeeds. These checks require a working Docker environment.

## 7. Data and Security Notes

- Never commit `.env` or files containing real credentials.
- Keep the existing PostgreSQL password consistent with the password stored in its initialized database.
- Keep separate credentials for Kafka broker, administrator, and application accounts.
- Remember that `SASL_PLAINTEXT` does not encrypt Kafka traffic.
- Do not expose local Elasticsearch or PostgreSQL ports to untrusted networks.
- Do not use `docker compose down -v` unless you intentionally want to delete persistent data.
- Verify authentication, topic permissions, log delivery, and broker failover in a running environment before treating the stack as production-ready.
