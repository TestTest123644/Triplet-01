"""Tests for environment example files (.env.example and twin/.env.example)."""

import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import pytest

EXPECTED_ENV_VARS = {
    "GROUND_TRUTH_DB_PATH",
    "KAFKA_ACKS",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_BROKER_ADDRESS_FAMILY",
    "KAFKA_MESSAGE_TIMEOUT_MS",
    "KAFKA_PASSWORD",
    "KAFKA_PASSWORD_FILE",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SSL_CA_LOCATION",
    "KAFKA_TOPIC",
    "KAFKA_USERNAME",
    "KAFKA_WAIT_RETRIES",
    "KEYSTORE_PATH",
    "MCP_ALLOWED_HOSTS",
    "MCP_AUTH_TOKENS",
    "MCP_AUTH_TOKENS_FILE",
    "MCP_HOST",
    "MCP_PORT",
    "MCP_URL",
    "READING_CACHE_MAX",
    "RED_TEAM_HOST",
    "RED_TEAM_PORT",
    "RED_TEAM_TOKEN",
    "RED_TEAM_TOKEN_FILE",
    "RESET_LEDGER",
    "SIM_SEED",
    "SIM_SPEED",
    "SIM_START_TIME",
    "SINK_MAX",
    "STREAMLIT_TESTING",
    "TIMELINE_LOG_DIR",
    "TRIPLET_NETWORK",
    "TRIPLET_NETWORK_EXTERNAL",
    "TWIN_API_URL",
    "TWIN_CONSOLE_HOST_PORT",
    "TWIN_MCP_HOST_PORT",
    "TWIN_RED_TEAM_HOST_PORT",
    "TWIN_SECRETS_DIR",
    "TWIN_SECRET_HOOK",
    "TWIN_SECRET_RELOAD_SECONDS",
    "VAULT_ADDR",
    "VAULT_CACERT",
    "VAULT_KV_MOUNT",
    "VAULT_NAMESPACE",
    "VAULT_REFRESH_SECONDS",
    "VAULT_ROLE_ID",
    "VAULT_ROLE_ID_FILE",
    "VAULT_SECRET_ID",
    "VAULT_SECRET_ID_FILE",
    "VAULT_TWIN_PATHS",
    "WAIT_FOR_KAFKA",
}


def _find_repo_root() -> Path:
    """Find repository root relative to test file path."""
    repo_root = Path(__file__).resolve().parents[2]
    root_env = repo_root / ".env.example"
    twin_env = repo_root / "twin" / ".env.example"
    if not (root_env.is_file() and twin_env.is_file()):
        pytest.skip(f"Env example files not found at expected repo root: {repo_root}")
    return repo_root


@pytest.mark.parametrize("rel_path", [".env.example", "twin/.env.example"])
def test_env_example_pure_python_syntax_and_vars(rel_path: str):
    """Verify that every non-blank, non-comment line matches ^[A-Z][A-Z0-9_]*=[^#]*$ with no trailing whitespace."""
    repo_root = _find_repo_root()
    env_file = repo_root / rel_path
    assert env_file.is_file(), f"Missing env file: {env_file}"

    content = env_file.read_text(encoding="utf-8")
    parsed_vars = set()

    for lineno, raw_line in enumerate(content.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        # No trailing whitespace before end of line
        assert raw_line == raw_line.rstrip(), (
            f"{rel_path}:{lineno} contains trailing whitespace: {raw_line!r}"
        )

        # Non-comment line must match ^[A-Z][A-Z0-9_]*=[^#]*$
        assert re.match(r"^[A-Z][A-Z0-9_]*=[^#]*$", raw_line), (
            f"{rel_path}:{lineno} does not match ^[A-Z][A-Z0-9_]*=[^#]*$: {raw_line!r}"
        )

        var_name = raw_line.split("=", 1)[0]
        parsed_vars.add(var_name)

    assert parsed_vars == EXPECTED_ENV_VARS, (
        f"Variable set mismatch in {rel_path}. "
        f"Extra: {parsed_vars - EXPECTED_ENV_VARS}, Missing: {EXPECTED_ENV_VARS - parsed_vars}"
    )


@pytest.mark.parametrize("rel_path", [".env.example", "twin/.env.example"])
def test_env_example_docker_compose_render(rel_path: str):
    """If docker compose is available, render with env_file and assert no rendered value contains '#'."""
    if shutil.which("docker") is None:
        pytest.skip("docker binary not found in PATH")

    try:
        check_proc = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if check_proc.returncode != 0:
            pytest.skip("docker compose not available or failed to report version")
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        pytest.skip(f"docker compose not available: {exc}")

    repo_root = _find_repo_root()
    env_file = (repo_root / rel_path).resolve()
    assert env_file.is_file(), f"Missing env file: {env_file}"

    with tempfile.TemporaryDirectory() as td:
        compose_file = Path(td) / "compose.yml"
        compose_file.write_text(
            f"services:\n  test:\n    image: alpine:3.20\n    env_file:\n      - {env_file}\n",
            encoding="utf-8",
        )

        res = subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "config", "--format", "json"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert res.returncode == 0, f"docker compose config failed: {res.stderr}"

        data = json.loads(res.stdout)
        env = data.get("services", {}).get("test", {}).get("environment", {})
        assert len(env) == len(EXPECTED_ENV_VARS), (
            f"Expected {len(EXPECTED_ENV_VARS)} rendered variables, got {len(env)}"
        )

        for var_name, var_value in env.items():
            if var_value is not None:
                assert "#" not in str(var_value), (
                    f"Rendered variable {var_name} contains '#': {var_value!r}"
                )

