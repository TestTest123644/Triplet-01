"""Tests for docker-compose configuration rendering and environment override conformance."""

import json
import os
import shutil
import subprocess
import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None,
    reason="docker binary not found in PATH"
)


def _find_repo_root() -> str:
    """Find absolute path to the repository root via .git directory."""
    current = os.path.dirname(os.path.abspath(__file__))
    while current and current != os.path.dirname(current):
        if os.path.exists(os.path.join(current, ".git")):
            return current
        current = os.path.dirname(current)
    raise RuntimeError("Could not find repository root containing .git")


def _run_compose_config(compose_file: str, env_overrides: dict = None) -> dict:
    """Run `docker compose -f <compose_file> config --format json` with env overrides."""
    repo_root = _find_repo_root()
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)

    cmd = ["docker", "compose", "-f", compose_file, "config", "--format", "json"]
    proc = subprocess.run(
        cmd,
        cwd=repo_root,
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    return json.loads(proc.stdout)


def test_root_compose_default_config():
    """Root docker-compose.yml defaults: city_network (not external), kafka:9092, ports 8000/8100."""
    repo_root = _find_repo_root()
    compose_path = os.path.join(repo_root, "docker-compose.yml")
    config = _run_compose_config(compose_path)

    net = config["networks"]["city_network"]
    assert net["name"] == "city_network"
    assert net.get("external") in (None, False)

    twin_service = config["services"]["twin_layer"]
    env = twin_service["environment"]
    assert env["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:9092"
    assert env["KAFKA_TOPIC"] == "twin.sensors"
    assert env["KAFKA_SECURITY_PROTOCOL"] == "PLAINTEXT"
    assert env["KAFKA_SASL_MECHANISM"] == "PLAIN"
    assert env["KAFKA_ACKS"] == "all"
    assert env["KAFKA_BROKER_ADDRESS_FAMILY"] == "any"
    assert env["WAIT_FOR_KAFKA"] == "true"

    published_ports = {
        str(p.get("published")): p.get("target") for p in twin_service["ports"]
    }
    assert published_ports.get("8000") == 8000
    assert published_ports.get("8100") == 8100

    console_service = config["services"]["dashboard"]
    console_ports = {
        str(p.get("published")): p.get("target") for p in console_service["ports"]
    }
    assert console_ports.get("8501") == 8501

    secret_volumes = [
        v for v in twin_service["volumes"] if v.get("target") == "/app/secrets"
    ]
    assert len(secret_volumes) == 1
    assert secret_volumes[0].get("read_only") is True


def test_root_compose_custom_environment():
    """Root docker-compose.yml custom env: external network, custom bootstrap, custom host ports."""
    repo_root = _find_repo_root()
    compose_path = os.path.join(repo_root, "docker-compose.yml")
    overrides = {
        "TRIPLET_NETWORK": "shared-backend",
        "TRIPLET_NETWORK_EXTERNAL": "true",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka-1:19094",
        "TWIN_MCP_HOST_PORT": "8010",
        "TWIN_RED_TEAM_HOST_PORT": "8110",
        "TWIN_CONSOLE_HOST_PORT": "8510",
    }
    config = _run_compose_config(compose_path, env_overrides=overrides)

    net = config["networks"]["city_network"]
    assert net["name"] == "shared-backend"
    assert net.get("external") is True

    twin_service = config["services"]["twin_layer"]
    env = twin_service["environment"]
    assert env["KAFKA_BOOTSTRAP_SERVERS"] == "kafka-1:19094"

    published_ports = {
        str(p.get("published")): p.get("target") for p in twin_service["ports"]
    }
    assert published_ports.get("8010") == 8000
    assert published_ports.get("8110") == 8100

    console_service = config["services"]["dashboard"]
    console_ports = {
        str(p.get("published")): p.get("target") for p in console_service["ports"]
    }
    assert console_ports.get("8510") == 8501


def test_twin_compose_default_config():
    """twin/docker-compose.yml defaults: twin_network named city_network (not external), kafka:9092."""
    repo_root = _find_repo_root()
    compose_path = os.path.join(repo_root, "twin", "docker-compose.yml")
    config = _run_compose_config(compose_path)

    net = config["networks"]["twin_network"]
    assert net["name"] == "city_network"
    assert net.get("external") in (None, False)

    twin_service = config["services"]["twin_layer"]
    env = twin_service["environment"]
    assert env["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:9092"
    assert env["KAFKA_TOPIC"] == "twin.sensors"
    assert env["KAFKA_SECURITY_PROTOCOL"] == "PLAINTEXT"
    assert env["KAFKA_SASL_MECHANISM"] == "PLAIN"
    assert env["KAFKA_ACKS"] == "all"
    assert env["KAFKA_BROKER_ADDRESS_FAMILY"] == "any"
    assert env["WAIT_FOR_KAFKA"] == "true"

    published_ports = {
        str(p.get("published")): p.get("target") for p in twin_service["ports"]
    }
    assert published_ports.get("8000") == 8000
    assert published_ports.get("8100") == 8100

    console_service = config["services"]["dashboard"]
    console_ports = {
        str(p.get("published")): p.get("target") for p in console_service["ports"]
    }
    assert console_ports.get("8501") == 8501

    secret_volumes = [
        v for v in twin_service["volumes"] if v.get("target") == "/app/secrets"
    ]
    assert len(secret_volumes) == 1
    assert secret_volumes[0].get("read_only") is True


def test_twin_compose_custom_environment():
    """twin/docker-compose.yml custom env: external network, custom bootstrap, custom host ports."""
    repo_root = _find_repo_root()
    compose_path = os.path.join(repo_root, "twin", "docker-compose.yml")
    overrides = {
        "TRIPLET_NETWORK": "shared-backend",
        "TRIPLET_NETWORK_EXTERNAL": "true",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka-1:19094",
        "TWIN_MCP_HOST_PORT": "8010",
        "TWIN_RED_TEAM_HOST_PORT": "8110",
        "TWIN_CONSOLE_HOST_PORT": "8510",
    }
    config = _run_compose_config(compose_path, env_overrides=overrides)

    net = config["networks"]["twin_network"]
    assert net["name"] == "shared-backend"
    assert net.get("external") is True

    twin_service = config["services"]["twin_layer"]
    env = twin_service["environment"]
    assert env["KAFKA_BOOTSTRAP_SERVERS"] == "kafka-1:19094"

    published_ports = {
        str(p.get("published")): p.get("target") for p in twin_service["ports"]
    }
    assert published_ports.get("8010") == 8000
    assert published_ports.get("8110") == 8100

    console_service = config["services"]["dashboard"]
    console_ports = {
        str(p.get("published")): p.get("target") for p in console_service["ports"]
    }
    assert console_ports.get("8510") == 8501
