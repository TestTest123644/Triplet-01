from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from guardian_layer.managers.buffer_manger import BufferManager
from guardian_layer.utils.channels import FlushChannel
from guardian_layer.utils.schema import Anomalies, Metadata, Sensor


def make_sensor(
    *,
    value: float = 1.0,
    sensor_type: str = "water_level",
    unit: str = "m",
    sensor_id: str = "sensor-1",
    cluster_id: str = "water_level",
    timestamp: datetime | None = None,
    lat: float | None = 36.8,
    lon: float | None = 10.18,
    lower_range: float | None = 0.0,
    upper_range: float | None = 3.0,
    packet_order: int = 1,
) -> Sensor:
    return Sensor(
        sensor_id=sensor_id,
        cluster_id=cluster_id,
        value=value,
        unit=unit,
        timestamp=timestamp or datetime.now(UTC),
        metadata=Metadata(
            lat=lat,
            lon=lon,
            packet_order=packet_order,
            type=sensor_type,
            lower_range=lower_range,
            upper_range=upper_range,
        ),
    )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_values_are_rejected_during_sensor_creation(value: float) -> None:
    with pytest.raises(ValidationError, match="value must be finite"):
        make_sensor(value=value)


@pytest.mark.parametrize(
    ("lower", "upper"),
    [(0.0, 0.0), (2.0, 1.0), (float("-inf"), 10.0), (0.0, float("inf"))],
)
def test_invalid_ranges_are_rejected_during_sensor_creation(
    lower: float, upper: float
) -> None:
    with pytest.raises(ValidationError, match="range"):
        make_sensor(lower_range=lower, upper_range=upper)


def test_valid_and_single_range_bounds_are_accepted() -> None:
    assert make_sensor(lower_range=0.0, upper_range=3.0)
    assert make_sensor(lower_range=1.0, upper_range=None)
    assert make_sensor(lower_range=None, upper_range=1.0)


@pytest.mark.parametrize(("lat", "lon"), [(-90.0, -180.0), (90.0, 180.0)])
def test_coordinate_bounds_are_inclusive(lat: float, lon: float) -> None:
    assert make_sensor(lat=lat, lon=lon)


@pytest.mark.parametrize(
    ("lat", "lon", "error"),
    [
        (90.01, 0.0, "lat"),
        (-90.01, 0.0, "lat"),
        (0.0, 180.01, "lon"),
        (0.0, -180.01, "lon"),
        (36.8, None, "provided together"),
        (None, 10.18, "provided together"),
    ],
)
def test_invalid_coordinates_are_rejected(
    lat: float | None, lon: float | None, error: str
) -> None:
    with pytest.raises(ValidationError, match=error):
        make_sensor(lat=lat, lon=lon)


@pytest.mark.parametrize(
    ("make_invalid_sensor", "field"),
    [
        (lambda: make_sensor(sensor_id="  "), "sensor_id"),
        (lambda: make_sensor(cluster_id=""), "cluster_id"),
        (lambda: make_sensor(unit="\t"), "unit"),
    ],
)
def test_blank_required_strings_are_rejected(
    make_invalid_sensor: Callable[[], Sensor], field: str
) -> None:
    with pytest.raises(ValidationError, match=field):
        make_invalid_sensor()


def test_future_timestamp_is_rejected() -> None:
    future = datetime.now(UTC) + timedelta(seconds=1)
    with pytest.raises(ValidationError, match="timestamp must not be in the future"):
        make_sensor(timestamp=future)


def test_negative_packet_order_is_rejected() -> None:
    with pytest.raises(ValidationError, match="packet_order"):
        make_sensor(packet_order=-1)


@pytest.mark.parametrize("value", [0.0, 3.0, 3.38, 12.0])
def test_water_level_physical_limits_are_inclusive(value: float) -> None:
    sensor = make_sensor(value=value, lower_range=None, upper_range=None)
    assert sensor.quality_flags == []


def test_water_level_unit_must_match() -> None:
    with pytest.raises(ValidationError, match="unit"):
        make_sensor(unit="cm")


@pytest.mark.parametrize("sensor_type", ["power_load", "air_quality"])
def test_unbounded_sensor_types_get_general_checks_only(sensor_type: str) -> None:
    sensor = make_sensor(
        sensor_type=sensor_type,
        unit="unspecified",
        lower_range=None,
        upper_range=None,
    )
    assert sensor.quality_flags == []


def test_custom_sensor_types_are_supported() -> None:
    sensor = make_sensor(
        sensor_type="temperature",
        unit="C",
        lower_range=None,
        upper_range=None,
    )
    assert sensor.quality_flags == []


def test_out_of_range_is_flagged_during_sensor_creation() -> None:
    sensor = make_sensor(value=3.01)
    assert sensor.quality_flags == [Anomalies.OUT_OF_RANGE]


def test_all_failed_checks_are_reported_during_sensor_creation() -> None:
    with pytest.raises(ValidationError) as exc_info:
        make_sensor(
            value=float("nan"),
            sensor_id=" ",
            cluster_id="",
            unit=" ",
            timestamp=datetime.now(UTC) + timedelta(days=1),
            lat=91.0,
            lon=None,
            lower_range=3.0,
            upper_range=2.0,
            packet_order=-1,
        )

    error_message = str(exc_info.value)
    for field in (
        "value",
        "sensor_id",
        "cluster_id",
        "unit",
        "timestamp",
        "lat",
        "provided together",
        "packet_order",
        "upper_range",
    ):
        assert field in error_message


def test_invalid_sensor_is_rejected_before_reaching_buffer_manager() -> None:
    manager = BufferManager(FlushChannel())

    with pytest.raises(ValidationError, match="value must be finite"):
        make_sensor(sensor_id="invalid-reading", value=float("nan"))

    assert manager.buffers == {}


def test_valid_reading_is_buffered_unchanged() -> None:
    manager = BufferManager(FlushChannel())
    sensor = make_sensor(value=1.5)
    manager.create(sensor.sensor_id)
    assert manager.add(sensor).ok
    assert manager.buffers["sensor-1"].readings[0] is sensor
    assert sensor.quality_flags == []
