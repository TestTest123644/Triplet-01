from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from guardian_layer.data_transformer.factory_transformer import TransformerFactory
from guardian_layer.data_transformer.interface.factory_transformer_interface import (
    TransformerInterface,
)
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import Metadata, Sensor, TwinSensorReading

logger = get_logger(__name__)


class TransformDropped(Exception):
    """Reading skipped on purpose (e.g. device isolated). Not an error."""


class TransformRejected(Exception):
    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code, self.detail = code, detail


@TransformerFactory.register("twin_v1")
class TwinToGuardian(TransformerInterface):
    def transform(self, payload: Mapping[str, Any]) -> Sensor:
        if isinstance(payload, Sensor):
            return payload

        # Pass through if already a valid Sensor representation
        if (
            "sensor_id" in payload
            and "cluster_id" in payload
            and "timestamp" in payload
            and "device_id" not in payload
        ):
            try:
                return Sensor.model_validate(payload)
            except ValidationError as error:
                raise TransformRejected(
                    "E300_SCHEMA", error.json(include_url=False)
                ) from error

        raw_status = payload.get("status")
        status_str: str | None = None
        if raw_status is not None:
            status_str = (
                raw_status.value if hasattr(raw_status, "value") else str(raw_status)
            )

        try:
            reading = TwinSensorReading.model_validate(payload)
        except ValidationError as error:
            logger.error(
                "twin reading rejected: device=%s %d invalid field(s): %s",
                payload.get("device_id"),
                error.error_count(),
                error.errors(include_url=False),
            )
            raise TransformRejected(
                "E300_SCHEMA", error.json(include_url=False)
            ) from error

        device_meta = payload.get("device_metadata") or {}
        if not isinstance(device_meta, dict):
            device_meta = {}

        location = reading.location if isinstance(reading.location, dict) else {}
        loc_zone = reading.location if isinstance(reading.location, str) else None

        lat = (
            device_meta.get("lat")
            if device_meta.get("lat") is not None
            else location.get("lat")
        )
        lon = (
            device_meta.get("lon")
            if device_meta.get("lon") is not None
            else location.get("lon")
        )

        lower_range = (
            device_meta.get("min_val")
            if device_meta.get("min_val") is not None
            else payload.get("lower_range")
        )
        upper_range = (
            device_meta.get("max_val")
            if device_meta.get("max_val") is not None
            else payload.get("upper_range")
        )

        zone_id = device_meta.get("zone_id") or loc_zone or payload.get("zone_id")

        sensor_type = device_meta.get("device_type") or reading.device_type

        unit = reading.unit
        if unit in ("meters", "meter"):
            unit = "m"

        try:
            return Sensor(
                sensor_id=reading.device_id,
                cluster_id=reading.device_type,
                value=reading.value,
                unit=unit,
                timestamp=reading.ts,
                reading_id=payload.get("reading_id"),
                status=status_str,
                metadata=Metadata(
                    lat=lat,
                    lon=lon,
                    packet_order=reading.seq,
                    type=sensor_type,
                    lower_range=lower_range,
                    upper_range=upper_range,
                    zone_id=zone_id,
                ),
            )
        except ValidationError as error:
            logger.error(
                "twin reading rejected: device=%s %d invalid field(s): %s",
                reading.device_id,
                error.error_count(),
                error.errors(include_url=False),
            )
            raise TransformRejected(
                "E300_SCHEMA", error.json(include_url=False)
            ) from error
        except ValueError as error:
            raise TransformRejected("E400_SENSOR_VALIDATION", str(error)) from error
