"""The Postgres service in docker-compose.yml: no secrets committed, data persisted,
healthcheck present, and the anchor future services use to wait for it."""

from pathlib import Path
from typing import Any

import pytest
import yaml

from guardian_layer.utils.helpers import find_project_root

DATA_DIR = "/var/lib/postgresql/data"

POSTGRES_ENV_VARS = ("POSTGRES_USER", "POSTGRES_DB")
SECRET_FILES = (
    "postgres_password",
    "kafka_broker_password",
    "kafka_admin_password",
    "kafka_client_password",
)

ALL_ENV_VARS = (
    "POSTGRES_USER",
    "POSTGRES_DB",
    "GUARDIAN_ELASTICSEARCH_URL",
    "ELASTIC_PASSWORD",
    "ELASTICSEARCH_PASSWORD",
    "GUARDIAN_ROTATION_URL",
    "TWIN_ROTATION_URL",
    "BRAIN_ROTATION_URL",
)


@pytest.fixture(scope="module")
def root() -> Path:
    found = find_project_root()
    assert found is not None
    return found


@pytest.fixture(scope="module")
def compose(root: Path) -> dict[str, Any]:
    return yaml.safe_load((root / "docker-compose.yml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def postgres(compose: dict[str, Any]) -> dict[str, Any]:
    return compose["services"]["postgres"]


def test_credentials_come_from_env_vars_not_literals(postgres: dict[str, Any]):
    for name in POSTGRES_ENV_VARS:
        assert postgres["environment"][name].startswith("${"), name


def test_postgres_password_comes_from_docker_secret(postgres: dict[str, Any]):
    environment = postgres["environment"]

    assert environment["POSTGRES_PASSWORD_FILE"] == "/run/secrets/postgres_password"
    assert "POSTGRES_PASSWORD" not in environment
    assert "postgres_password" in postgres["secrets"]


def test_docker_secrets_are_declared(compose: dict[str, Any]):
    secrets = compose["secrets"]

    for name in SECRET_FILES:
        assert name in secrets
        assert Path(secrets[name]["file"]).name == f"{name}.txt"


def test_data_is_persisted_in_a_declared_named_volume(
    postgres: dict[str, Any], compose: dict[str, Any]
):
    assert f"postgres_data:{DATA_DIR}" in postgres["volumes"]
    assert "postgres_data" in compose["volumes"]


def test_healthcheck_uses_pg_isready(postgres: dict[str, Any]):
    command = " ".join(postgres["healthcheck"]["test"])
    assert "pg_isready" in command
    # $$ so the container shell expands them, not Compose on the host
    assert "$${POSTGRES_USER}" in command and "$${POSTGRES_DB}" in command


def test_port_is_published_on_localhost_only(postgres: dict[str, Any]):
    assert postgres["ports"] == ["127.0.0.1:5432:5432"]


def test_elasticsearch_requires_authentication_and_is_local_only(
    compose: dict[str, Any],
):
    elasticsearch = compose["services"]["elasticsearch"]
    environment = elasticsearch["environment"]
    assert environment["xpack.security.enabled"] == "true"
    assert environment["ELASTIC_PASSWORD"].startswith("${ELASTIC_PASSWORD:?")
    assert elasticsearch["ports"] == ["127.0.0.1:9200:9200"]
    healthcheck = " ".join(elasticsearch["healthcheck"]["test"])
    assert "curl -fsS -u elastic:$${ELASTIC_PASSWORD}" in healthcheck


def test_anchor_makes_a_service_wait_for_a_healthy_postgres(compose: dict[str, Any]):
    wait = compose["x-wait-for-postgres"]
    assert wait["depends_on"]["postgres"]["condition"] == "service_healthy"


def test_existing_services_are_untouched(compose: dict[str, Any]):
    assert {
        "kafka-1",
        "kafka-2",
        "kafka-3",
        "kafka-init",
        "postgres",
    } <= compose["services"].keys()


def test_env_file_is_gitignored(root: Path):
    ignored = (root / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".env" in ignored


def test_env_example_documents_settings_and_secret_files(root: Path):
    lines = (root / ".env.example").read_text(encoding="utf-8").splitlines()
    values = dict(
        line.split("=", 1)
        for line in lines
        if line.strip() and not line.lstrip().startswith("#")
    )

    assert values.keys() == set(ALL_ENV_VARS)
    assert values["ELASTIC_PASSWORD"] == ""
    assert values["ELASTICSEARCH_PASSWORD"] == ""

    content = "\n".join(lines)
    for name in SECRET_FILES:
        assert f"secrets/{name}.txt" in content


def test_readme_documents_postgres_settings_and_secrets(root: Path):
    readme = (root / "README.md").read_text(encoding="utf-8")
    readme_lower = readme.lower()

    for name in SECRET_FILES:
        assert f"{name}.txt" in readme

    assert "postgres" in readme_lower
    assert "secret" in readme_lower


def test_three_kafka_nodes_share_quorum(compose):
    services = compose["services"]
    nodes = [services[f"kafka-{i}"] for i in range(1, 4)]

    assert all(
        node["environment"]["CLUSTER_ID"] == nodes[0]["environment"]["CLUSTER_ID"]
        for node in nodes
    )
    assert [node["environment"]["KAFKA_NODE_ID"] for node in nodes] == [1, 2, 3]

    expected_voters = "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
    assert all(
        node["environment"]["KAFKA_CONTROLLER_QUORUM_VOTERS"] == expected_voters
        for node in nodes
    )


def test_kafka_replication_and_authorization(compose):
    kafka = compose["services"]["kafka-1"]["environment"]

    assert kafka["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] == 3
    assert kafka["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"] == 3
    assert kafka["KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"] == 2
    assert kafka["KAFKA_MIN_INSYNC_REPLICAS"] == 2
    assert kafka["KAFKA_SASL_ENABLED_MECHANISMS"] == "PLAIN"
    assert (
        kafka["KAFKA_AUTHORIZER_CLASS_NAME"]
        == "org.apache.kafka.metadata.authorizer.StandardAuthorizer"
    )
    assert kafka["KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND"] == "false"


def test_each_kafka_node_has_its_own_volume(compose):
    for i in range(1, 4):
        service = compose["services"][f"kafka-{i}"]
        assert f"kafka{i}_data:/var/lib/kafka/data" in service["volumes"]
        assert f"kafka{i}_data" in compose["volumes"]


def test_kafka_init_service_exists(compose):
    assert "kafka-init" in compose["services"]
