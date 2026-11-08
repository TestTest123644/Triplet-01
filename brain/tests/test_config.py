import pytest

from city_brain.config import load_config, load_config_from_dict


def test_loads_standalone_with_policy_and_params():
    cfg = load_config("config/standalone.yaml")
    assert cfg.mode == "standalone"
    assert cfg.policy["water"]["high_band"] == 6.0
    assert cfg.params["filter"]["b_prior"] == 0.045


def test_env_override_nested():
    cfg = load_config("config/triplet.yaml", env={"BRAIN__KAFKA__BOOTSTRAP": "localhost:19092"})
    assert cfg.kafka.bootstrap == "localhost:19092"


def test_unknown_key_is_a_clear_error():
    with pytest.raises(ValueError, match="kafka.bootstrapp"):
        load_config_from_dict({"mode": "triplet", "kafka": {"bootstrapp": "x"}})


def test_triplet_yaml_parses_with_spec_values():
    cfg = load_config("config/triplet.yaml")
    assert cfg.mode == "triplet"
    assert cfg.kafka.group == "brain-processor"
    assert cfg.kafka.topics["readings"] == "telemetry-ingest"
    assert cfg.twin.profile == "twin-triplet07" and cfg.guardian.profile == "guardian-triplet07"
    assert cfg.twin.mcp_url == "http://twin_layer:8000/sse"
    assert cfg.twin.call_timeout_s == 2
    assert cfg.twin.auth_token is None
    assert cfg.policy["trust"]["pending_weight"] == 0.5
    assert cfg.guardian.optional is True


def test_mcp_auth_token_resolution():
    env = {
        "MCP_AUTH_TOKENS": "brain:secret-brain-tok,guardian:secret-guardian-tok",
    }
    cfg = load_config("config/triplet.yaml", env=env)
    assert cfg.twin.auth_token == "secret-brain-tok"
    assert cfg.twin.resolve_auth_token() == "secret-brain-tok"


def test_kafka_sasl_config_from_standard_env(tmp_path):
    pwd_file = tmp_path / "kafka_secret.txt"
    pwd_file.write_text("hex16passwordval\n")
    env = {
        "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "KAFKA_SASL_MECHANISM": "PLAIN",
        "KAFKA_USERNAME": "broadcast",
        "KAFKA_PASSWORD_FILE": str(pwd_file),
        "KAFKA_ACKS": "1",
    }
    cfg = load_config("config/triplet.yaml", env=env)
    assert cfg.kafka.security_protocol == "SASL_PLAINTEXT"
    assert cfg.kafka.sasl_mechanism == "PLAIN"
    assert cfg.kafka.sasl_username == "broadcast"
    assert cfg.kafka.acks == "1"

    client_sec = cfg.kafka.client_security_config()
    assert client_sec["security.protocol"] == "SASL_PLAINTEXT"
    assert client_sec["sasl.mechanism"] == "PLAIN"
    assert client_sec["sasl.username"] == "broadcast"
    assert client_sec["sasl.password"] == "hex16passwordval"


# ----- the shared .env contract: a file beats a variable, and an empty variable is not a value -----


def test_an_empty_mcp_auth_tokens_does_not_shadow_the_tokens_file(tmp_path):
    """`.env.example` ships `MCP_AUTH_TOKENS=` empty; it must not hide the file."""
    tokens = tmp_path / "mcp_tokens"
    tokens.write_text("brain:tok-from-file,guardian:other", encoding="utf-8")
    cfg = load_config("config/triplet.yaml", env={
        "MCP_AUTH_TOKENS": "",
        "MCP_AUTH_TOKENS_FILE": str(tokens),
    })
    assert cfg.twin.auth_token == "tok-from-file"
    assert cfg.twin.resolve_auth_token() == "tok-from-file"


def test_the_mcp_tokens_file_takes_priority_over_the_variable(tmp_path):
    """The root `.env.example` says the file "takes priority over MCP_AUTH_TOKENS"."""
    tokens = tmp_path / "mcp_tokens"
    tokens.write_text("brain:tok-from-file", encoding="utf-8")
    cfg = load_config("config/triplet.yaml", env={
        "MCP_AUTH_TOKENS": "brain:stale-tok-from-env",
        "MCP_AUTH_TOKENS_FILE": str(tokens),
    })
    assert cfg.twin.auth_token == "tok-from-file"


def test_an_explicit_mcp_auth_token_still_wins(tmp_path):
    """The Brain's own override stays the most specific thing anyone can set."""
    tokens = tmp_path / "mcp_tokens"
    tokens.write_text("brain:tok-from-file", encoding="utf-8")
    cfg = load_config("config/triplet.yaml", env={
        "MCP_AUTH_TOKEN": "tok-explicit",
        "MCP_AUTH_TOKENS": "brain:tok-from-env",
        "MCP_AUTH_TOKENS_FILE": str(tokens),
    })
    assert cfg.twin.auth_token == "tok-explicit"


def test_a_tokens_file_holding_only_the_token_is_accepted(tmp_path):
    """A Docker secret usually holds the bare value, with no `brain:` in front of it."""
    tokens = tmp_path / "mcp_token"
    tokens.write_text("bare-tok\n", encoding="utf-8")
    cfg = load_config("config/triplet.yaml", env={"MCP_AUTH_TOKENS_FILE": str(tokens)})
    assert cfg.twin.auth_token == "bare-tok"


def test_an_empty_mcp_auth_token_falls_through(tmp_path):
    tokens = tmp_path / "mcp_tokens"
    tokens.write_text("brain:tok-from-file", encoding="utf-8")
    cfg = load_config("config/triplet.yaml", env={
        "MCP_AUTH_TOKEN": "",
        "MCP_AUTH_TOKENS_FILE": str(tokens),
    })
    assert cfg.twin.auth_token == "tok-from-file"


def test_the_kafka_password_file_takes_precedence_over_the_literal(tmp_path):
    """`.env.example`: the file "takes precedence over KAFKA_PASSWORD"."""
    pwd_file = tmp_path / "kafka_secret.txt"
    pwd_file.write_text("pw-from-file\n", encoding="utf-8")
    cfg = load_config("config/triplet.yaml", env={
        "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "KAFKA_USERNAME": "brain",
        "KAFKA_PASSWORD": "stale-pw-from-env",
        "KAFKA_PASSWORD_FILE": str(pwd_file),
    })
    assert cfg.kafka.client_security_config()["sasl.password"] == "pw-from-file"


def test_an_empty_kafka_variable_does_not_wipe_the_configured_value():
    """`KAFKA_USERNAME=` in a compose default must leave `config/triplet.yaml` alone."""
    cfg = load_config("config/triplet.yaml", env={
        "KAFKA_USERNAME": "",
        "KAFKA_PASSWORD": "",
        "KAFKA_PASSWORD_FILE": "",
    })
    assert cfg.kafka.sasl_username == "brain"


def test_sasl_without_a_resolvable_password_says_so(caplog):
    """The silent failure mode: SASL, a username, and no password at all."""
    import logging

    cfg = load_config("config/triplet.yaml", env={
        "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "KAFKA_USERNAME": "brain",
    })
    with caplog.at_level(logging.WARNING, logger="city_brain.config"):
        client_sec = cfg.kafka.client_security_config()
    assert "sasl.password" not in client_sec
    assert "KAFKA_PASSWORD" in caplog.text


def test_sasl_with_a_password_logs_nothing(tmp_path, caplog):
    import logging

    pwd_file = tmp_path / "pw"
    pwd_file.write_text("pw-from-file", encoding="utf-8")
    cfg = load_config("config/triplet.yaml", env={
        "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "KAFKA_USERNAME": "brain",
        "KAFKA_PASSWORD_FILE": str(pwd_file),
    })
    with caplog.at_level(logging.WARNING, logger="city_brain.config"):
        cfg.kafka.client_security_config()
    assert caplog.text == ""
