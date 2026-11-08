import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.melbourne import Melbourne
from guardian_layer.utils.dataset_config import DatasetSettings

BOM = "﻿"
MICRO = (
    BOM + "device_id;received_at;sensorlocation;latlong;minimumwinddirection;"
    "averagewinddirection;maximumwinddirection;minimumwindspeed;averagewindspeed;"
    "gustwindspeed;airtemperature;relativehumidity;atmosphericpressure;pm25;pm10;noise\n"
    "ICTMicroclimate-03;2023-05-18T06:09:52+00:00;CH1 rooftop;-37.8140348, 144.96728;"
    "179.0;200.0;211.0;1.3;1.5;2.8;12.4;68.2;1016.7;1.0;3.0;87.7\n"
    "ICTMicroclimate-03;2023-05-18T05:39:49+00:00;CH1 rooftop;-37.8140348, 144.96728;"
    "124.0;172.0;179.0;0.9;1.4;3.9;12.6;66.5;1016.6;;3.0;76.7\n"
)


@pytest.fixture
def melbourne_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.melbourne")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.melbourne")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "melbourne"
    folder.mkdir()
    (folder / "microclimate.csv").write_bytes(MICRO.encode())
    return list(Melbourne(DatasetSettings(root=str(tmp_path))).load())


def by_type(readings, kind):
    return [r for r in readings if r.sensor.metadata and r.sensor.metadata.type == kind]


def test_bom_header_is_read(tmp_path):
    air = by_type(load(tmp_path), "air_temperature")
    assert air[0].sensor.sensor_id == "melbourne:ICTMicroclimate-03:air_temperature"
    assert [r.sensor.value for r in air] == [12.6, 12.4]  # sorted: 05:39 first
    assert air[0].sensor.timestamp == datetime(2023, 5, 18, 5, 39, 49, tzinfo=UTC)


def test_latlong_is_split(tmp_path):
    metadata = by_type(load(tmp_path), "air_temperature")[0].sensor.metadata
    assert metadata is not None
    assert (metadata.lat, metadata.lon) == (-37.8140348, 144.96728)


def test_noise_is_not_loaded(tmp_path):
    types = {r.sensor.metadata.type for r in load(tmp_path) if r.sensor.metadata}
    assert "noise" not in types
    assert "air_temperature" in types


def test_empty_value_is_skipped(tmp_path, melbourne_log):
    assert len(by_type(load(tmp_path), "pm25")) == 1
    warnings = [r for r in melbourne_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {1}
