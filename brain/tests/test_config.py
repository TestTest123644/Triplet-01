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
    assert cfg.kafka.group == "city-brain"
    assert cfg.kafka.topics == {"readings": "twin.sensors", "verdicts": "guardian.verdicts"}
    assert cfg.twin.mcp_url == "http://twin_layer:8000/sse"
    assert cfg.twin.call_timeout_s == 2
    assert cfg.policy["trust"]["pending_weight"] == 0.5
