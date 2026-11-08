from dataclasses import dataclass
from datetime import datetime

try:
    # Works natively in Python 3.11+
    from enum import StrEnum  # pyright: ignore[reportAssignmentType]
except ImportError:
    # Fallback for Python 3.10 and below
    from enum import Enum

    class StrEnum(str, Enum):  # noqa: UP042
        def __str__(self) -> str:
            return str(self.value)


from typing import Any

from pydantic import BaseModel, Field

from guardian_layer.consumer_producer.kafka_consumer.utils.sensor_validator import (
    SensorValidator,
)


class Metadata(BaseModel):
    lat: float | None = None
    lon: float | None = None
    packet_order: int
    type: str
    upper_range: float | None = None
    lower_range: float | None = None


class Anomalies(StrEnum):
    OUT_OF_RANGE = "out_of_range"
    SPOOFED_DATA = "spoofed_data"
    NOT_SENSOR_DATA = "not_sensor_data"
    SENSOR_MALFUNCTION = "sensor_malfunction"
    SENSOR_IS_DOWN = "sensor_is_down"


class Sensor(BaseModel):
    sensor_id: str
    cluster_id: str
    value: float
    unit: str
    timestamp: datetime
    metadata: Metadata | None = None
    quality_flags: list[Anomalies] = Field(default_factory=list)

    def model_post_init(self, context: Any) -> None:

        result = SensorValidator(self).execute()
        if not result.valid:
            raise ValueError("; ".join(result.errors))
        self.quality_flags = list(
            dict.fromkeys((*self.quality_flags, *result.anomalies))
        )


class SensorType(StrEnum):
    WATER_LEVEL = "water_level"
    POWER_LOAD = "power_load"
    AIR_QUALITY = "air_quality"


@dataclass(frozen=True, slots=True)
class SensorRule:
    unit: str
    lower: float
    upper: float


SENSOR_RULES: dict[SensorType, SensorRule | None] = {
    SensorType.WATER_LEVEL: SensorRule("m", 0.0, 3.000),
    SensorType.POWER_LOAD: None,
    SensorType.AIR_QUALITY: None,
}


@dataclass(frozen=True, slots=True)
class VerificationResult:
    errors: tuple[str, ...] = ()
    anomalies: tuple[Anomalies, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.errors

    @property
    def flagged(self) -> bool:
        return bool(self.anomalies)
