"""action_taker settings, read from one YAML file (pattern of kafka_producer)."""

import os
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    YamlConfigSettingsSource,
)

from guardian_layer.utils.helpers import find_project_root

CONFIG_ENV = "ACTION_TAKER_CONFIG"
REPO_CONFIG = "src/guardian_layer/consumer_producer/action_taker/utils/settings.yaml"


class KafkaSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bootstrap_servers: str = "kafka-1:19094,kafka-2:19094,kafka-3:19094"
    username: str = "action_taker"
    # the password itself is never in the YAML: only the path of the mounted secret
    password_file: Path = Path("/run/secrets/kafka_action_taker_password")
    group_id: str = "action-taker-processor"
    dlq_topic: str = "telemetry-dlq"
    anomaly_topic: str = "telemetry-anomaly"
    auto_offset_reset: Literal["earliest", "latest"] = "earliest"
    pause_seconds: float = Field(default=5.0, gt=0)

    def client_config(self) -> dict[str, Any]:
        """confluent-kafka Consumer config; never log it, it holds the password."""
        password = SecretStr(self.password_file.read_text(encoding="utf-8").strip())
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": self.username,
            "sasl.password": password.get_secret_value(),
            # offsets are committed by hand, after the MCP call (at-least-once)
            "enable.auto.commit": False,
            "auto.offset.reset": self.auto_offset_reset,
        }


class McpSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = "http://host.docker.internal:8000/sse"
    timeout: float = Field(default=3.0, gt=0)
    retry_attempts: int = Field(default=5, ge=1)
    retry_backoff_seconds: float = Field(default=0.5, ge=0)


class RuleSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    repeat_threshold: int = Field(default=3, ge=1)
    repeat_window_seconds: float = Field(default=60.0, gt=0)


class Settings(BaseSettings):
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    mcp: McpSettings = Field(default_factory=McpSettings)
    rules: RuleSettings = Field(default_factory=RuleSettings)
    dry_run: bool = True  # log the intended action without calling MCP

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        *args: PydanticBaseSettingsSource,
        **kwargs: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # YAML only: init arguments, env vars and dotenv are left out on purpose
        return (
            YamlConfigSettingsSource(
                settings_cls, yaml_file=config_path(), yaml_file_encoding="utf-8"
            ),
        )


def config_path() -> Path:
    """The YAML to read: $ACTION_TAKER_CONFIG, else the one in the repository.

    A missing file is an error: the YAML source would silently fall back to the
    defaults, and dry_run would then be the only thing standing between a typo in
    a path and a service acting on devices with settings nobody chose.
    """
    override = os.environ.get(CONFIG_ENV)
    if override:
        path = Path(override)
    else:
        root = find_project_root()
        if root is None:
            raise FileNotFoundError(f"{CONFIG_ENV} is not set and no project root")
        path = root / REPO_CONFIG
    if not path.is_file():
        raise FileNotFoundError(f"action_taker settings file not found: {path}")
    return path


def get_settings() -> Settings:
    return Settings()
