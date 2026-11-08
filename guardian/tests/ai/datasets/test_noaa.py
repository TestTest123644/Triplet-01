import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.noaa import NoaaTides
from guardian_layer.utils.dataset_config import DatasetSettings
from guardian_layer.utils.schema import Anomalies

FILES = {
    "8518750_air_temperature.csv": (
        "Date Time, Air Temperature, X, N, R \n"
        "2026-09-12 00:00,24.9,0,0,0\n"
        "2026-09-12 00:06,-40.0,0,1,0\n"
        "2026-09-12 00:12,,0,0,0\n"
    ),
    "8518750_water_level.csv": (
        "Date Time, Water Level, Sigma, O or I (for verified), F, R, L, Quality \n"
        "2026-09-12 00:00,1.647,0.051,3,0,0,0,v\n"
        "2026-09-12 00:06,1.647,0.000,0,1,0,0,p\n"
    ),
    "8518750_wind.csv": (
        "Date Time, Speed, Direction, Direction, Gust, X, R \n"
        "2026-09-12 00:00,3.1,200.00,SSW,4.2,0,0\n"
    ),
    "9414290_wind.csv": (
        "Date Time, Speed, Direction, Direction, Gust, X, R \n"
        "Error: No data was found. This product may not be offered at this station.\n"
    ),
}


@pytest.fixture
def noaa_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.noaa")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.noaa")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "noaa_tides"
    folder.mkdir()
    for name, text in FILES.items():
        (folder / name).write_bytes(text.encode())
    return list(NoaaTides(DatasetSettings(root=str(tmp_path))).load())


def by_type(readings, kind):
    return [r for r in readings if r.sensor.metadata and r.sensor.metadata.type == kind]


def test_n_flag_is_out_of_range(tmp_path):
    air = by_type(load(tmp_path), "air_temperature")
    assert [r.label for r in air] == [None, Anomalies.OUT_OF_RANGE]
    assert air[0].sensor.sensor_id == "noaa_tides:8518750:air_temperature"
    assert air[0].sensor.unit == "°C"
    assert air[0].sensor.timestamp == datetime(2026, 9, 12, tzinfo=UTC)


def test_f_flag_is_malfunction(tmp_path):
    level = by_type(load(tmp_path), "water_level")
    # O is a count of samples, not a flag: 3 there leaves the reading unlabelled
    assert [r.label for r in level] == [None, Anomalies.SENSOR_MALFUNCTION]
    assert [r.sensor.value for r in level] == pytest.approx([1.647, 1.647])
    assert all(r.sensor.unit == "m" for r in level)


def test_wind_reads_the_numeric_direction(tmp_path):
    readings = load(tmp_path)
    assert [r.sensor.value for r in by_type(readings, "wind_direction")] == [200.0]
    assert [r.sensor.value for r in by_type(readings, "wind_gust")] == [4.2]


def test_no_data_file_yields_nothing(tmp_path, noaa_log):
    readings = load(tmp_path)
    assert not [r for r in readings if "9414290" in r.sensor.sensor_id]
    warnings = [r for r in noaa_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {1}  # the empty air temperature
