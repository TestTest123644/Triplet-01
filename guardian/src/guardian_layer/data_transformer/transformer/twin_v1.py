from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from guardian_layer.data_transformer.factory_transformer import TransformerFactory
from guardian_layer.data_transformer.interface.factory_transformer_interface import (
    TransformerInterface,
)
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import Metadata, Sensor, TwinSensorReading, TwinStatus

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
        try:
            status = TwinStatus(payload.get("status"))
        except ValueError as exc:
            logger.warning(
                "twin reading dropped: device=%s unknown status=%r",
                payload.get("device_id"),
                payload.get("status"),
            )
            raise TransformRejected(
                "E320_UNKNOWN_STATUS", repr(payload.get("status"))
            ) from exc
        if status is not TwinStatus.OK:
            logger.info(
                "twin reading dropped: device=%s status=%s",
                payload.get("device_id"),
                status,
            )
            raise TransformDropped(str(status))

        try:
            reading = TwinSensorReading.model_validate(payload)
            location = reading.location if isinstance(reading.location, dict) else {}

            return Sensor(  # SensorValidator ValueError still propagates
                sensor_id=reading.device_id,
                cluster_id=reading.device_type,
                value=reading.value,
                unit=reading.unit,
                timestamp=reading.ts,
                metadata=Metadata(
                    lat=location.get("lat", None),
                    lon=location.get("lon", None),
                    packet_order=reading.seq,
                    type=reading.device_type,
                ),
            )
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
