from datetime import datetime

import pytest
from pydantic import ValidationError

from guardian_layer.utils.schema import Metadata, Sensor


def test_metadata_creation() -> None:
    metadata = Metadata(
        lat=36.8065,
        lon=10.1815,
        packet_order=1,
        type="temperature",
        upper_range=50.0,
        lower_range=-10.0,
    )

    assert metadata.lat == 36.8065
    assert metadata.lon == 10.1815
    assert metadata.packet_order == 1
    assert metadata.type == "temperature"
    assert metadata.upper_range == 50.0
    assert metadata.lower_range == -10.0


def test_sensor_creation_with_metadata() -> None:
    timestamp = datetime(2026, 10, 3, 12, 0, 0)
    metadata = Metadata(packet_order=1, type="temperature")

    sensor = Sensor(
        sensor_id="sensor-1",
        cluster_id="temperature",
        value=23.5,
        unit="C",
        timestamp=timestamp,
        metadata=metadata,
    )

    assert sensor.sensor_id == "sensor-1"
    assert sensor.cluster_id == "temperature"
    assert sensor.value == 23.5
    assert sensor.unit == "C"
    assert sensor.timestamp == timestamp
    assert sensor.metadata == metadata


def test_sensor_metadata_is_optional() -> None:
    sensor = Sensor(
        sensor_id="sensor-1",
        cluster_id="temperature",
        value=23.5,
        unit="C",
        timestamp=datetime(2026, 10, 3, 12, 0, 0),
    )

    assert sensor.metadata is None


def test_metadata_requires_packet_order_and_type() -> None:
    with pytest.raises(ValidationError):
        Metadata()  # type: ignore[call-arg]
