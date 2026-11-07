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
