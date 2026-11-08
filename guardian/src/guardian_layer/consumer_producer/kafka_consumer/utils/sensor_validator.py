"""Quality checks for sensor readings before they enter a buffer."""

from __future__ import annotations

import math
from datetime import datetime

from guardian_layer.consumer_producer.kafka_consumer.utils.schema import (
    SENSOR_RULES,
    Anomalies,
    Sensor,
    SensorRule,
    SensorType,
    VerificationResult,
)


class SensorValidator:
    """Validate a Sensor and report errors and quality anomalies."""

    def __init__(self, sensor: Sensor) -> None:
        self.sensor = sensor

    def execute(self) -> VerificationResult:
        errors: list[str] = []
        anomalies: list[Anomalies] = []
        self._validate_required_fields(errors)
        finite_value = self._validate_value(errors)
        self._validate_timestamp(errors)
        sensor_rule = self._validate_metadata(errors)
        self._validate_unit(errors, sensor_rule)

        if finite_value:
            self._flag_out_of_range(anomalies, sensor_rule)

        return VerificationResult(
            errors=tuple(errors),
            anomalies=tuple(dict.fromkeys(anomalies)),
        )

    def _validate_required_fields(self, errors: list[str]) -> None:
        for field_name in ("sensor_id", "cluster_id", "unit"):
            field_value = getattr(self.sensor, field_name)
            if not isinstance(field_value, str) or not field_value.strip():
                errors.append(f"{field_name} must not be empty or whitespace")

    def _validate_value(self, errors: list[str]) -> bool:
        finite_value = _is_finite(self.sensor.value)
        if not finite_value:
            errors.append("value must be finite")
        return finite_value

    def _validate_timestamp(self, errors: list[str]) -> None:
        timestamp = self.sensor.timestamp
        if not isinstance(timestamp, datetime):
            errors.append("timestamp must be a valid datetime")
        elif timestamp > datetime.now(timestamp.tzinfo):
            errors.append("timestamp must not be in the future")

    def _validate_metadata(self, errors: list[str]) -> SensorRule | None:
        metadata = self.sensor.metadata
        if metadata is None:
            return None

        try:
            sensor_type = SensorType(metadata.type)
            sensor_rule = SENSOR_RULES[sensor_type]
        except ValueError:
            sensor_rule = None

        if (
            not isinstance(metadata.packet_order, int)
            or isinstance(metadata.packet_order, bool)
            or metadata.packet_order < 0
        ):
            errors.append("packet_order must be a non-negative integer")

        for name, coordinate, minimum, maximum in (
            ("lat", metadata.lat, -90.0, 90.0),
            ("lon", metadata.lon, -180.0, 180.0),
        ):
            if coordinate is not None and (
                not _is_finite(coordinate) or not minimum <= coordinate <= maximum
            ):
                errors.append(
                    f"{name} must be finite and between {minimum:g} and {maximum:g}"
                )

        if (metadata.lat is None) != (metadata.lon is None):
            errors.append("lat and lon must be provided together")

        self._validate_range_bounds(errors, metadata.lower_range, metadata.upper_range)
        return sensor_rule

    def _validate_range_bounds(
        self,
        errors: list[str],
        lower_range: float | None,
        upper_range: float | None,
    ) -> None:
        for name, bound in (
            ("lower_range", lower_range),
            ("upper_range", upper_range),
        ):
            if bound is not None and not _is_finite(bound):
                errors.append(f"{name} must be finite")

        if (
            lower_range is not None
            and upper_range is not None
            and _is_finite(lower_range)
            and _is_finite(upper_range)
            and upper_range <= lower_range
        ):
            errors.append("upper_range must be greater than lower_range")

    def _validate_unit(self, errors: list[str], sensor_rule: SensorRule | None) -> None:
        metadata = self.sensor.metadata
        if (
            metadata is not None
            and sensor_rule is not None
            and self.sensor.unit != sensor_rule.unit
        ):
            errors.append(f"{metadata.type} unit must be {sensor_rule.unit!r}")

    def _flag_out_of_range(
        self, anomalies: list[Anomalies], sensor_rule: SensorRule | None
    ) -> None:
        sensor = self.sensor
        metadata = sensor.metadata
        if (
            sensor_rule is not None
            and not sensor_rule.lower <= sensor.value <= sensor_rule.upper
        ):
            anomalies.append(Anomalies.OUT_OF_RANGE)
        if metadata is not None:
            if (
                metadata.lower_range is not None
                and _is_finite(metadata.lower_range)
                and sensor.value < metadata.lower_range
            ):
                anomalies.append(Anomalies.OUT_OF_RANGE)
            if (
                metadata.upper_range is not None
                and _is_finite(metadata.upper_range)
                and sensor.value > metadata.upper_range
            ):
                anomalies.append(Anomalies.OUT_OF_RANGE)


def _is_finite(value: float) -> bool:
    # bool is an int subclass, so exclude it; math.isfinite rejects NaN and infinity.
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )
