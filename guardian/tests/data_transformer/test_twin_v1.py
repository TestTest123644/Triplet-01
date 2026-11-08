import logging
from datetime import UTC, datetime, timedelta

import pytest

from guardian_layer.data_transformer.transformer.twin_v1 import (
    TransformDropped,
    TransformRejected,
    TwinToGuardian,
)
from guardian_layer.utils.schema import Metadata, Sensor

LOGGER = "guardian_layer.data_transformer.transformer.twin_v1"

PAYLOAD = {
    "device_id": "pump-07",
    "device_type": "pressure",
    "value": 4.2,
    "unit": "bar",
    "ts": "2026-10-20T10:15:30Z",
    "seq": 42,
    "status": "ok",
    "location": {"lat": 36.8, "lon": 10.18},
}


@pytest.fixture
def twin_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger(LOGGER)
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    yield caplog
    module_logger.removeHandler(caplog.handler)


def transform(**changes):
    return TwinToGuardian().transform({**PAYLOAD, **changes})


def test_ok_reading_maps_every_field():
    assert transform() == Sensor(
        sensor_id="pump-07",
        cluster_id="pressure",
        value=4.2,
        unit="bar",
        timestamp=datetime(2026, 10, 20, 10, 15, 30, tzinfo=UTC),
        status="ok",
        metadata=Metadata(lat=36.8, lon=10.18, packet_order=42, type="pressure"),
    )


def test_ts_offset_is_kept():
    sensor = transform(ts="2026-10-20T11:15:30+01:00")
    assert sensor is not None
    assert sensor.timestamp.utcoffset() == timedelta(hours=1)
    assert sensor.timestamp == datetime(2026, 10, 20, 10, 15, 30, tzinfo=UTC)


@pytest.mark.parametrize("location", ["Tunis, site B", None])
def test_location_not_an_object_leaves_no_coordinates(location):
    sensor = transform(location=location)
    assert sensor is not None and sensor.metadata is not None
    assert (sensor.metadata.lat, sensor.metadata.lon) == (None, None)


def test_missing_location_leaves_no_coordinates():
    payload = {k: v for k, v in PAYLOAD.items() if k != "location"}
    sensor = TwinToGuardian().transform(payload)
    assert sensor is not None and sensor.metadata is not None
    assert sensor.metadata.lat is None


@pytest.mark.parametrize(
    "status",
    ["ok", "isolated", "quarantined", "rolled_back", "modelled", "custom", None],
)
def test_all_statuses_are_accepted(status):
    sensor = transform(status=status)
    assert sensor.status == status


@pytest.mark.parametrize(
    ("changes", "field"),
    [
        ({"value": "high"}, "value"),
        ({"seq": "first"}, "seq"),
        ({"ts": "2026-10-20T10:15:30"}, "ts"),  # RFC 3339 needs an offset
        ({"ts": "yesterday"}, "ts"),
        ({"device_id": None}, "device_id"),
        ({"location": {"lat": "north"}}, "lat"),
    ],
)
def test_invalid_ok_reading_is_rejected_and_logged(twin_log, changes, field):
    with pytest.raises((TransformRejected, TransformDropped)):
        transform(**changes)
    record = twin_log.records[-1]
    assert record.levelno == logging.ERROR
    assert "twin reading rejected" in record.getMessage()
    assert field in record.getMessage()
