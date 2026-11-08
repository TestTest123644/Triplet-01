from pathlib import Path

import pytest
from pydantic import ValidationError

from guardian_layer.consumer_producer.action_taker.utils.settings import (
    KafkaSettings,
    get_settings,
)

ENV = "ACTION_TAKER_CONFIG"


def write_yaml(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_settings_load_from_named_yaml(tmp_path, monkeypatch):
    config = write_yaml(
        tmp_path / "settings.yaml", "rules:\n  repeat_threshold: 7\ndry_run: false\n"
    )
    monkeypatch.setenv(ENV, str(config))
    settings = get_settings()
    assert settings.rules.repeat_threshold == 7
    assert settings.dry_run is False
    assert settings.kafka.group_id == "action-taker-processor"  # default kept


def test_named_yaml_missing_raises(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV, str(tmp_path / "nope.yaml"))
    with pytest.raises(FileNotFoundError):
        get_settings()


def test_unknown_key_is_rejected(tmp_path, monkeypatch):
    # extra="forbid": a password in the YAML is refused, it comes only from the file
    config = write_yaml(tmp_path / "settings.yaml", "kafka:\n  password: abc\n")
    monkeypatch.setenv(ENV, str(config))
    with pytest.raises(ValidationError):
        get_settings()


def test_repo_settings_yaml_loads(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    settings = get_settings()
    assert settings.dry_run is True
    assert settings.kafka.dlq_topic == "telemetry-dlq"
    assert settings.kafka.anomaly_topic == "telemetry-anomaly"
    assert settings.mcp.timeout == 3.0


def test_client_config_reads_password_file(tmp_path):
    secret = tmp_path / "password.txt"
    secret.write_text("ab12\n", encoding="utf-8")
    config = KafkaSettings(password_file=secret).client_config()
    assert config["sasl.password"] == "ab12"
    assert config["sasl.username"] == "action_taker"
    assert config["group.id"] == "action-taker-processor"
    assert config["security.protocol"] == "SASL_PLAINTEXT"
    assert config["sasl.mechanism"] == "PLAIN"
    assert config["enable.auto.commit"] is False
    assert config["auto.offset.reset"] == "earliest"


def test_client_config_missing_password_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        KafkaSettings(password_file=tmp_path / "missing.txt").client_config()


def test_password_not_in_settings_repr(tmp_path):
    secret = tmp_path / "password.txt"
    secret.write_text("ab12", encoding="utf-8")
    kafka = KafkaSettings(password_file=secret)
    kafka.client_config()
    assert "ab12" not in repr(kafka)
    assert "ab12" not in kafka.model_dump_json()
