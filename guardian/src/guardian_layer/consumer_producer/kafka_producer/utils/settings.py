import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    YamlConfigSettingsSource,
)


class MCPSettings(BaseModel):
    url: str = Field(default="", description="MCP URL for SSE connection")
    timeout: float = Field(
        default=3.0, description="Timeout for MCP requests in seconds"
    )

    def model_post_init(self, context: Any, /) -> None:
        self.url = os.environ.get("MCP_URL", self.url)
        if not self.url:
            raise ValueError(
                "MCP URL must be provided via environment variable "
                + "MCP_URL or settings.yaml"
            )


class KafkaSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str = Field(default="sensors.raw", description="Kafka topic for sensor data")
    bootstrap_servers: str = Field(
        default="broker:9092", description="Kafka bootstrap servers"
    )

    connector_jar: str = Field(
        default="flink-sql-connector-kafka-2.3.0.jar",
        description="Path to the Kafka connector JAR file",
    )
    username: str = Field(
        default="broadcast", description="Username for Kafka SASL authentication"
    )
    password: str = Field(
        default="", description="Password for Kafka SASL authentication"
    )

    def model_post_init(self, context: Any, /) -> None:
        password_path = Path(os.environ.get("KAFKA_PASSWORD", self.password))
        self.password = (
            password_path.read_text().strip()
            if password_path.is_file() and password_path.exists()
            else self.password
        )

        if not self.password:
            raise ValueError("Kafka password must be provided via settings.yaml")


class Settings(BaseSettings):
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    mcp: MCPSettings = Field(default_factory=MCPSettings)
    data_path: str = Field(
        default="./data/device_metadata.json",
        description="Path to the device metadata cache",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        *args: PydanticBaseSettingsSource,
        **kwargs: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # YAML only: init arguments, env vars and dotenv are left out on purpose
        override = os.environ.get("Sync_confgig")
        if override:
            path = Path(override)
            # the YAML source skips a missing file; an explicitly named one is a mistake
            if not path.is_file():
                raise FileNotFoundError(f"Sync_confgig file not found: {path}")
        else:
            root: Path = Path(__file__).parent
            path = root / "settings.yaml"
        # explicit: Windows would read the file in its own code page ("°" -> "Â°")
        return (
            YamlConfigSettingsSource(
                settings_cls, yaml_file=path, yaml_file_encoding="utf-8"
            ),
        )


def get_settings() -> Settings:
    return Settings()
