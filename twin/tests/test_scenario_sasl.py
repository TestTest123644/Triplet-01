"""Tests for scenario Kafka reader configuration, SASL credentials, and wire preflight."""

import json
import os
from pathlib import Path
import sys
import pytest

REPO_ROOT = str(Path(__file__).resolve().parents[2])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scenarios._twin_common import (
    ScenarioContext,
    WireConsumer,
    get_scenario_kafka_consumer_config,
    resolve_tokens,
)
import scenarios.run_all as run_all


@pytest.fixture(autouse=True)
def clean_kafka_env(monkeypatch):
    """Ensure clean environment for each test."""
    kafka_vars = [
        "SCENARIO_KAFKA_SECURITY_PROTOCOL",
        "SCENARIO_KAFKA_SASL_MECHANISM",
        "SCENARIO_KAFKA_USERNAME",
        "SCENARIO_KAFKA_PASSWORD",
        "SCENARIO_KAFKA_PASSWORD_FILE",
        "SCENARIO_KAFKA_SSL_CA_LOCATION",
        "SCENARIO_KAFKA_BROKER_ADDRESS_FAMILY",
        "SCENARIO_KAFKA_GROUP_PREFIX",
        "SCENARIO_WIRE_PREFLIGHT_TIMEOUT",
        "KAFKA_SECURITY_PROTOCOL",
        "KAFKA_SASL_MECHANISM",
        "KAFKA_USERNAME",
        "KAFKA_PASSWORD",
        "KAFKA_PASSWORD_FILE",
        "KAFKA_SSL_CA_LOCATION",
        "KAFKA_BROKER_ADDRESS_FAMILY",
        "MCP_AUTH_TOKENS",
        "MCP_AUTH_TOKENS_FILE",
        "MCP_AUTH_TOKEN",
        "MCP_BRAIN_TOKEN",
        "MCP_GUARDIAN_TOKEN",
        "MCP_SCENARIO_TOKEN",
    ]
    for v in kafka_vars:
        monkeypatch.delenv(v, raising=False)


def test_helper_defaults_equal_baseline_config():
    """With clean environment, helper returns baseline consumer config."""
    conf = get_scenario_kafka_consumer_config("localhost:19092")
    assert conf["bootstrap.servers"] == "localhost:19092"
    assert conf["group.id"].startswith("scenario-runner-")
    assert conf["auto.offset.reset"] == "latest"
    assert conf["enable.auto.commit"] is False
    assert "security.protocol" not in conf
    assert "broker.address.family" not in conf
    assert "sasl.mechanism" not in conf
    assert "sasl.username" not in conf
    assert "sasl.password" not in conf


def test_helper_scenario_wins_over_kafka(monkeypatch):
    """SCENARIO_KAFKA_* settings override KAFKA_* fallback settings."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
    monkeypatch.setenv("KAFKA_USERNAME", "kafka_user")
    monkeypatch.setenv("KAFKA_PASSWORD", "kafka_pass")

    monkeypatch.setenv("SCENARIO_KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("SCENARIO_KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("SCENARIO_KAFKA_USERNAME", "scenario_user")
    monkeypatch.setenv("SCENARIO_KAFKA_PASSWORD", "scenario_pass")

    conf = get_scenario_kafka_consumer_config()
    assert conf["security.protocol"] == "SASL_SSL"
    assert conf["sasl.mechanism"] == "PLAIN"
    assert conf["sasl.username"] == "scenario_user"
    assert conf["sasl.password"] == "scenario_pass"


def test_helper_fallback_to_kafka(monkeypatch):
    """Helper falls back to KAFKA_* settings when SCENARIO_KAFKA_* is unset or empty."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("KAFKA_USERNAME", "fallback_user")
    monkeypatch.setenv("KAFKA_PASSWORD", "fallback_pass")
    monkeypatch.setenv("SCENARIO_KAFKA_USERNAME", "")

    conf = get_scenario_kafka_consumer_config()
    assert conf["security.protocol"] == "SASL_PLAINTEXT"
    assert conf["sasl.mechanism"] == "SCRAM-SHA-512"
    assert conf["sasl.username"] == "fallback_user"
    assert conf["sasl.password"] == "fallback_pass"


def test_helper_password_file_wins_over_literal(tmp_path, monkeypatch):
    """Password file wins over literal password and strips trailing newlines."""
    pw_file = tmp_path / "secret.txt"
    pw_file.write_text("my_file_secret_987\r\n", encoding="utf-8")

    monkeypatch.setenv("SCENARIO_KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("SCENARIO_KAFKA_USERNAME", "reader")
    monkeypatch.setenv("SCENARIO_KAFKA_PASSWORD", "literal_pass_ignored")
    monkeypatch.setenv("SCENARIO_KAFKA_PASSWORD_FILE", str(pw_file))

    conf = get_scenario_kafka_consumer_config()
    assert conf["sasl.password"] == "my_file_secret_987"


def test_helper_invalid_values_and_named_errors(tmp_path, monkeypatch):
    """Each invalid value, missing username, or empty/missing password file gives named error."""
    # 1. Invalid protocol
    monkeypatch.setenv("SCENARIO_KAFKA_SECURITY_PROTOCOL", "INVALID_PROTO")
    with pytest.raises(ValueError) as exc:
        get_scenario_kafka_consumer_config()
    assert "Invalid SCENARIO_KAFKA_SECURITY_PROTOCOL: 'INVALID_PROTO'" in str(exc.value)

    # 2. Invalid mechanism
    monkeypatch.setenv("SCENARIO_KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("SCENARIO_KAFKA_SASL_MECHANISM", "INVALID_MECH")
    with pytest.raises(ValueError) as exc:
        get_scenario_kafka_consumer_config()
    assert "Invalid SCENARIO_KAFKA_SASL_MECHANISM: 'INVALID_MECH'" in str(exc.value)

    # 3. Missing username
    monkeypatch.setenv("SCENARIO_KAFKA_SASL_MECHANISM", "PLAIN")
    with pytest.raises(ValueError) as exc:
        get_scenario_kafka_consumer_config()
    assert "Missing required environment variable for SASL authentication: SCENARIO_KAFKA_USERNAME" in str(exc.value)

    # 4. Missing password and password file
    monkeypatch.setenv("SCENARIO_KAFKA_USERNAME", "u")
    with pytest.raises(ValueError) as exc:
        get_scenario_kafka_consumer_config()
    assert "Missing required environment variable for SASL authentication: SCENARIO_KAFKA_PASSWORD or SCENARIO_KAFKA_PASSWORD_FILE" in str(exc.value)

    # 5. Non-existent password file
    monkeypatch.setenv("SCENARIO_KAFKA_PASSWORD_FILE", "/nonexistent/secret_file.txt")
    with pytest.raises(ValueError) as exc:
        get_scenario_kafka_consumer_config()
    assert "Password file specified by SCENARIO_KAFKA_PASSWORD_FILE does not exist" in str(exc.value)

    # 6. Empty password file
    empty_file = tmp_path / "empty.txt"
    empty_file.write_text("   \n\r\n", encoding="utf-8")
    monkeypatch.setenv("SCENARIO_KAFKA_PASSWORD_FILE", str(empty_file))
    with pytest.raises(ValueError) as exc:
        get_scenario_kafka_consumer_config()
    assert "Password file specified by SCENARIO_KAFKA_PASSWORD_FILE is empty" in str(exc.value)

    # 7. Invalid broker address family
    monkeypatch.delenv("SCENARIO_KAFKA_PASSWORD_FILE")
    monkeypatch.setenv("SCENARIO_KAFKA_PASSWORD", "valid_pw")
    monkeypatch.setenv("SCENARIO_KAFKA_BROKER_ADDRESS_FAMILY", "invalid_fam")
    with pytest.raises(ValueError) as exc:
        get_scenario_kafka_consumer_config()
    assert "Invalid SCENARIO_KAFKA_BROKER_ADDRESS_FAMILY: 'invalid_fam'" in str(exc.value)


def test_password_never_in_error_text_or_log(tmp_path, monkeypatch):
    """The password value never appears in any exception text or logged event."""
    secret = "SUPER_SECRET_12345_XYZ"
    pw_file = tmp_path / "unreadable.txt"
    pw_file.write_text(secret + "\n", encoding="utf-8")

    monkeypatch.setenv("SCENARIO_KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("SCENARIO_KAFKA_USERNAME", "user1")
    monkeypatch.setenv("SCENARIO_KAFKA_PASSWORD", secret)
    monkeypatch.setenv("SCENARIO_KAFKA_BROKER_ADDRESS_FAMILY", "bad_fam")

    with pytest.raises(ValueError) as exc:
        get_scenario_kafka_consumer_config()
    assert secret not in str(exc.value)

    # Also check WireConsumer preflight logging
    ctx = ScenarioContext("t_mock", "test_mock", log_dir=str(tmp_path), queue_source=None)
    ctx.wire.config_error = "Test config error"
    with pytest.raises(SystemExit):
        ctx._check_wire_preflight()

    with open(ctx.log_file, "r", encoding="utf-8") as f:
        log_content = f.read()
    assert secret not in log_content


def test_helper_group_prefix_applied(monkeypatch):
    """SCENARIO_KAFKA_GROUP_PREFIX modifies the generated group.id."""
    monkeypatch.setenv("SCENARIO_KAFKA_GROUP_PREFIX", "custom-suite-prefix")
    conf = get_scenario_kafka_consumer_config()
    assert conf["group.id"].startswith("custom-suite-prefix-")


def test_wire_preflight_timeout_exits_2(tmp_path, monkeypatch, capsys):
    """Wire preflight waits for message; on timeout, prints clear message and exits 2."""
    ctx = ScenarioContext(
        "t_timeout",
        "test_timeout",
        log_dir=str(tmp_path),
        wire_preflight_timeout=0.05,
    )

    class FakeEmptyConsumer:
        def poll(self, timeout):
            return None
        def close(self):
            pass

    ctx.wire.consumer = FakeEmptyConsumer()

    # Mock rest get_status so reachability check passes
    monkeypatch.setattr(ctx.rest, "get_status", lambda: {"status": "ok", "sim_speed": 1.0, "sim_time": "2026-10-17T12:00:00Z"})

    with pytest.raises(SystemExit) as excinfo:
        with ctx:
            pass

    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    assert "Stream is not readable at" in captured.err
    assert "Likely causes: wrong topic, invalid SASL credentials, missing Read ACL on the topic, or missing Read ACL on group prefix." in captured.err

    # Check logged event
    with open(ctx.log_file, "r", encoding="utf-8") as f:
        events = [json.loads(line.strip()) for line in f if line.strip()]
    wf_event = next(e for e in events if e.get("event") == "wire_preflight")
    assert wf_event["details"]["result"] == "timeout_no_messages"


def test_wire_preflight_skipped_with_queue_source(tmp_path, monkeypatch):
    """QueueSource path skips wire preflight check."""
    class FakeQueueSource:
        def get_reading(self, timeout=1.0):
            return None

    ctx = ScenarioContext(
        "t_queue",
        "test_queue",
        log_dir=str(tmp_path),
        queue_source=FakeQueueSource(),
    )
    monkeypatch.setattr(ctx.rest, "get_status", lambda: {"status": "ok", "sim_speed": 1.0, "sim_time": "2026-10-17T12:00:00Z"})
    monkeypatch.setattr(ctx, "settle", lambda: 0.0)

    # Should enter context without error
    with ctx:
        assert ctx.queue_source is not None


def test_run_all_stops_on_exit_2(tmp_path, monkeypatch, capsys):
    """run_all.py stops immediately at the first scenario that fails preflight with exit code 2."""
    monkeypatch.chdir(tmp_path)
    scenarios_executed = []

    class MockModule1:
        @staticmethod
        def run(ctx):
            scenarios_executed.append("mod1")
            sys.exit(2)

    class MockModule2:
        @staticmethod
        def run(ctx):
            scenarios_executed.append("mod2")
            return True

    monkeypatch.setattr(run_all.TwinRestClient, "get_status", lambda self: {"status": "ok", "sim_speed": 1.0})
    monkeypatch.setattr(run_all, "SCENARIOS", [("t1", "mock_mod1"), ("t2", "mock_mod2")])

    def mock_import(name):
        if name == "mock_mod1":
            return MockModule1
        if name == "mock_mod2":
            return MockModule2
        raise ImportError(name)

    monkeypatch.setattr("importlib.import_module", mock_import)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_all.py", "--red-team-token", "dummy", "--log-dir", str(tmp_path)],
    )

    # Mock ScenarioContext preflights and settle on run_all.ScenarioContext so mock_mod1 runs without real services
    monkeypatch.setattr(run_all.ScenarioContext, "_check_wire_preflight", lambda self: None)
    monkeypatch.setattr(run_all.ScenarioContext, "settle", lambda self: 0.0)
    monkeypatch.setattr(run_all.ScenarioContext, "finish", lambda self: True)

    code = run_all.main()
    assert code == 2
    assert scenarios_executed == ["mod1"]
    captured = capsys.readouterr()
    assert "Scenario preflight failed (exit code 2). Stopping scenario suite." in captured.err


def test_resolve_tokens_file_fallback_when_env_unset(tmp_path, monkeypatch):
    """When MCP_AUTH_TOKENS is unset, resolve_tokens reads MCP_AUTH_TOKENS_FILE."""
    token_file = tmp_path / "mcp_tokens.txt"
    token_file.write_text("brain:tok-b-file\nguardian:tok-g-file,scenario:tok-s-file\n", encoding="utf-8")

    monkeypatch.delenv("MCP_AUTH_TOKENS", raising=False)
    monkeypatch.setenv("MCP_AUTH_TOKENS_FILE", str(token_file))

    b, g, s = resolve_tokens()
    assert b == "tok-b-file"
    assert g == "tok-g-file"
    assert s == "tok-s-file"


def test_resolve_tokens_env_wins_over_file(tmp_path, monkeypatch):
    """MCP_AUTH_TOKENS in environment takes precedence over MCP_AUTH_TOKENS_FILE."""
    token_file = tmp_path / "mcp_tokens.txt"
    token_file.write_text("brain:file-b,guardian:file-g,scenario:file-s\n", encoding="utf-8")

    monkeypatch.setenv("MCP_AUTH_TOKENS", "brain:env-b,guardian:env-g,scenario:env-s")
    monkeypatch.setenv("MCP_AUTH_TOKENS_FILE", str(token_file))

    b, g, s = resolve_tokens()
    assert b == "env-b"
    assert g == "env-g"
    assert s == "env-s"


def test_resolve_tokens_missing_file_error_names_variable_and_path(capsys, monkeypatch):
    """Missing token file prints an error naming MCP_AUTH_TOKENS_FILE and the file path."""
    missing_path = "/tmp/does_not_exist_mcp_tokens_12345.txt"
    monkeypatch.delenv("MCP_AUTH_TOKENS", raising=False)
    monkeypatch.setenv("MCP_AUTH_TOKENS_FILE", missing_path)

    b, g, s = resolve_tokens()
    assert b is None
    assert g is None
    assert s is None

    captured = capsys.readouterr()
    assert "MCP_AUTH_TOKENS_FILE" in captured.err
    assert missing_path in captured.err


def test_resolve_tokens_token_values_never_in_errors_or_logs(tmp_path, capsys, monkeypatch):
    """Secret token values are never printed or logged during unreadable file error."""
    secret_val = "SECRET_SUPER_BEARER_TOKEN_99"
    unreadable_file = tmp_path / "unreadable_tokens.txt"
    unreadable_file.write_text(f"brain:{secret_val}\n", encoding="utf-8")
    os.chmod(str(unreadable_file), 0o000)

    try:
        monkeypatch.delenv("MCP_AUTH_TOKENS", raising=False)
        monkeypatch.setenv("MCP_AUTH_TOKENS_FILE", str(unreadable_file))

        resolve_tokens()
        captured = capsys.readouterr()
        assert secret_val not in captured.err
        assert secret_val not in captured.out
    finally:
        os.chmod(str(unreadable_file), 0o644)

