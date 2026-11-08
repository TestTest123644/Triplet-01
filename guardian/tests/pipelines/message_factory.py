"""Build telemetry readings in the agreed schema for preprocessing tests."""

from typing import Any

from guardian_layer.utils.schema import Sensor


class _UncheckedSensor(Sensor):
    def model_post_init(self, context: Any) -> None:
        pass


def make_message_dict(
    sensor_id: str = "water_level-01",
    value: float = 10.0,
    second: int = 0,
    *,
    packet_order: int | None = None,
    cluster_id: str = "water_level",
    lat: float | None = 36.79,
    lon: float | None = 10.18,
    lower_range: float | None = 0.0,
    upper_range: float | None = 100.0,
    with_metadata: bool = True,
) -> dict[str, Any]:
    """One raw message at 00:00:<second>Z; packet_order defaults to second + 1."""
    message: dict[str, Any] = {
        "sensor_id": sensor_id,
        "cluster_id": cluster_id,
        "value": value,
        "unit": "m",
        "timestamp": f"2026-10-02T00:00:{second:02d}Z",
        "metadata": None,
    }
    if with_metadata:
        message["metadata"] = {
            "lat": lat,
            "lon": lon,
            "packet_order": second + 1 if packet_order is None else packet_order,
            "type": cluster_id,
            "upper_range": upper_range,
            "lower_range": lower_range,
        }
    return message


def make_reading(*args: Any, **kwargs: Any) -> Sensor:
    """``make_message_dict`` validated as a Sensor, as Buffer.readings holds it."""
    return Sensor.model_validate(make_message_dict(*args, **kwargs))


def make_unvalidated_reading(*args: Any, **kwargs: Any) -> Sensor:
    """Build a malformed reading to test downstream defenses against bypasses."""
    return _UncheckedSensor.model_validate(make_message_dict(*args, **kwargs))
