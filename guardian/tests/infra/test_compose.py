"""The Postgres service in docker-compose.yml: no secrets committed, data persisted,
healthcheck present, and the anchor future services use to wait for it."""

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from guardian_layer.utils.helpers import find_project_root

DATA_DIR = "/var/lib/postgresql/data"

POSTGRES_ENV_VARS = ("POSTGRES_USER", "POSTGRES_DB")
SECRET_FILES = (
    "postgres_password",
    "postgres_grafana_password",
    "elastic_grafana_password",
    "kafka_broker_password",
    "kafka_admin_password",
    "kafka_client_password",
)

ALL_ENV_VARS = (
    "POSTGRES_USER",
    "POSTGRES_DB",
    "ELASTIC_PASSWORD",
    "ELASTICSEARCH_PASSWORD",
    "GUARDIAN_ELASTICSEARCH_URL",
    "GUARDIAN_LAYER_LOG",
    "GF_SECURITY_ADMIN_PASSWORD",
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
    raw = yaml.safe_load((root / "docker-compose.yml").read_text(encoding="utf-8"))
    includes = raw.get("include", [])
    if not includes:
        return raw

    merged: dict[str, Any] = {}
    for inc in includes:
        inc_path = root / inc
        if inc_path.exists():
            data = yaml.safe_load(inc_path.read_text(encoding="utf-8")) or {}
            for key, val in data.items():
                if isinstance(val, dict):
                    merged.setdefault(key, {}).update(val)
                elif key not in merged:
                    merged[key] = val
    return merged


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


def test_fluent_bit_ships_container_logs_to_elasticsearch(
    root: Path, compose: dict[str, Any]
):
    fluent_bit = compose["services"]["fluent-bit"]
    assert fluent_bit["image"].startswith("cr.fluentbit.io/fluent/fluent-bit:")
    assert fluent_bit["environment"]["ELASTICSEARCH_PASSWORD"].startswith(
        "${ELASTICSEARCH_PASSWORD:?"
    )
    assert fluent_bit["depends_on"]["elasticsearch"]["condition"] == "service_healthy"
    assert any(
        str(volume).startswith("./fluent-bit/fluent-bit.conf:")
        for volume in fluent_bit["volumes"]
    )
    assert (root / "fluent-bit/fluent-bit.conf").is_file()
    assert (root / "fluent-bit/rewrite.lua").is_file()
    assert compose["x-logging"]["driver"] == "fluentd"
    assert compose["services"]["action-taker"]["logging"]["driver"] == "fluentd"
    assert (
        "GUARDIAN_ELASTICSEARCH_URL"
        not in compose["services"]["action-taker"]["environment"]
    )


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
    assert values["GUARDIAN_ELASTICSEARCH_URL"] == ""
    assert values["GF_SECURITY_ADMIN_PASSWORD"] == ""

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


def test_grafana_is_local_only_and_waits_for_data_backends(compose: dict[str, Any]):
    grafana = compose["services"]["grafana"]
    assert grafana["ports"] == ["127.0.0.1:3000:3000"]
    assert grafana["environment"]["GF_SECURITY_ADMIN_PASSWORD"].startswith(
        "${GF_SECURITY_ADMIN_PASSWORD:?"
    )
    assert "postgres_grafana_password" in grafana["secrets"]
    assert "postgres_password" not in grafana["secrets"]
    depends_on = grafana["depends_on"]
    assert depends_on["postgres"]["condition"] == "service_healthy"
    assert depends_on["grafana-postgres-init"]["condition"] == (
        "service_completed_successfully"
    )
    assert depends_on["grafana-elasticsearch-init"]["condition"] == (
        "service_completed_successfully"
    )
    assert depends_on["elasticsearch"]["condition"] == "service_healthy"
    assert "grafana_data" in compose["volumes"]


def test_grafana_postgres_role_is_provisioned_with_separate_credentials(
    root: Path, compose: dict[str, Any]
):
    service = compose["services"]["grafana-postgres-init"]
    assert service["environment"]["POSTGRES_GRAFANA_USER"] == "grafana_reader"
    assert set(service["secrets"]) == {
        "postgres_password",
        "postgres_grafana_password",
    }

    datasource = yaml.safe_load(
        (root / "grafana/provisioning/datasources/datasources.yml").read_text(
            encoding="utf-8"
        )
    )["datasources"][1]
    assert datasource["user"] == "$POSTGRES_GRAFANA_USER"
    assert datasource["secureJsonData"]["password"] == "$POSTGRES_GRAFANA_PASSWORD"

    provisioner = (root / "scripts/provision-grafana-reader.sh").read_text(
        encoding="utf-8"
    )
    assert "NOSUPERUSER NOCREATEDB NOCREATEROLE" in provisioner
    assert 'if [ "$POSTGRES_USER" = "$POSTGRES_GRAFANA_USER" ]' in provisioner
    assert (
        "GRANT SELECT ON TABLE public.raw_readings, public.processed_readings, "
        "public.transformed_windows, public.model_outputs TO" in provisioner
    )
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA public TO" in provisioner
    assert "ALTER DEFAULT PRIVILEGES FOR ROLE" in provisioner
    assert "REVOKE SELECT ON TABLES FROM" in provisioner
    assert "--set=ON_ERROR_STOP=1" in provisioner

    runtime_schema = (root / "scripts/create-runtime-schema.sql").read_text(
        encoding="utf-8"
    )
    for table in ("sensors", "sensor_summaries", "verdicts"):
        assert f"public.{table}" in runtime_schema
    assert "data JSONB NOT NULL" in runtime_schema


def test_grafana_elasticsearch_role_is_index_scoped(
    root: Path, compose: dict[str, Any]
):
    service = compose["services"]["grafana-elasticsearch-init"]
    assert "elastic_grafana_password" in service["secrets"]
    assert "ELASTIC_PASSWORD" not in compose["services"]["grafana"]["environment"]

    datasources = yaml.safe_load(
        (root / "grafana/provisioning/datasources/datasources.yml").read_text(
            encoding="utf-8"
        )
    )["datasources"]
    assert datasources[0]["basicAuthUser"] == "$ELASTIC_GRAFANA_USER"
    assert (
        datasources[0]["secureJsonData"]["basicAuthPassword"]
        == "$ELASTIC_GRAFANA_PASSWORD"
    )
    provisioner = (
        root / "scripts/provision-grafana-elasticsearch-reader.py"
    ).read_text(encoding="utf-8")
    assert '"names": ["guardian-logs"]' in provisioner
    assert '"privileges": ["read", "view_index_metadata"]' in provisioner


def test_dashboard_review_fixes(root: Path):
    dashboard = json.loads(
        (root / "grafana/dashboards/guardian-monitoring.json").read_text(
            encoding="utf-8"
        )
    )
    panels = {panel.get("title"): panel for panel in dashboard["panels"]}
    logs_by_level = panels["Logs by level"]
    assert logs_by_level["type"] == "piechart"
    level_target = logs_by_level["targets"][0]
    assert level_target["bucketAggs"][0]["field"] == "level"
    assert level_target["alias"] == "{{term level}}"
    assert all(agg["type"] != "date_histogram" for agg in level_target["bucketAggs"])
    top_loggers = panels["Top loggers"]["targets"][0]
    assert top_loggers["bucketAggs"][0]["field"] == "logger"
    assert top_loggers["alias"] == "{{term logger}}"
    assert panels["Top loggers"]["options"]["reduceOptions"]["values"]

    assert dashboard["time"] == {
        "from": "2004-02-01T00:00:00.000Z",
        "to": "now",
    }
    assert dashboard["refresh"] == "30s"
    for panel in dashboard["panels"]:
        assert "timeFrom" not in panel
        assert "timeTo" not in panel

    scores = panels["Verdict scores"]["targets"][0]["rawSql"]
    assert "sensor_id" in scores
    assert "score" in scores
    assert "public.model_outputs" in scores
    assert "window_end BETWEEN $__timeFrom()" in scores
    assert panels["Verdict scores"]["type"] == "table"
    assert panels["Anomalies by sensor"]["options"]["reduceOptions"]["values"]
    assert panels["Stored verdict details"]["targets"][0]["rawSql"].startswith(
        "SELECT\n  id,"
    )
    assert "Needs attention" not in panels
    assert "Attention detail" not in panels
    assert "Log explorer" in panels
    assert "Inventory" in panels

    cluster = next(
        variable
        for variable in dashboard["templating"]["list"]
        if variable["name"] == "cluster_id"
    )
    assert cluster["allValue"] == "'__all_clusters__'"
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            query = target.get("rawSql", "")
            assert "sensor_id IN (${sensor_id:sqlstring})" not in query
            assert "public.sensors" not in query
            assert "public.verdicts" not in query
            assert "quality_flags" not in query
            if query:
                assert (
                    "public.processed_readings" in query
                    or "public.model_outputs" in query
                )
            if "public.model_outputs" in query:
                assert "window_end BETWEEN $__timeFrom()" in query

    dashboard_docs = (root / "docs/dashboard.md").read_text(encoding="utf-8")
    assert "processed_readings" in dashboard_docs
    assert "model_outputs" in dashboard_docs
    assert "Needs attention" not in dashboard_docs
    assert "Attention detail" not in dashboard_docs
    variables = {variable["name"] for variable in dashboard["templating"]["list"]}
    assert "time_basis" not in variables
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            query = target.get("rawSql", "")
            if "cluster_id IN (${cluster_id:sqlstring})" in query:
                assert "'__all_clusters__' IN (${cluster_id:sqlstring})" in query
