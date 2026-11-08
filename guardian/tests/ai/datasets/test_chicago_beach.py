import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.chicago_beach import ChicagoBeach
from guardian_layer.utils.dataset_config import DatasetSettings

WATER = (
    "Beach Name,Measurement Timestamp,Water Temperature,Turbidity,Transducer Depth,"
    "Wave Height,Wave Period,Battery Life,Measurement Timestamp Label,Measurement ID\n"
    "Calumet Beach,06/14/2019 07:00:00 PM,16.0,2.18,,0.1,3.0,11.8,x,a\n"
    "Calumet Beach,06/14/2019 06:00:00 PM,15.5,2.10,,0.2,3.0,11.8,x,b\n"
    "Calumet Beach,06/14/2019 08:00:00 PM,-100000.00,2.2,,0.1,3.0,11.8,x,d\n"
)
WEATHER = (
    "Station Name,Measurement Timestamp,Air Temperature,Wet Bulb Temperature,Humidity,"
    "Rain Intensity,Interval Rain,Total Rain,Precipitation Type,Wind Direction,"
    "Wind Speed,Maximum Wind Speed,Barometric Pressure,Solar Radiation,Heading,"
    "Battery Life,Measurement Timestamp Label,Measurement ID\n"
    "63rd Street Weather Station,09/27/2018 01:00:00 PM,16.40,12.2,61,0.0,0.00,260.3,0,"
    "231,2.5,4.7,996.3,484,356,11.9,x,c\n"
    "63rd Street Weather Station,09/27/2018 11:00:00 AM,15.50,12.2,61,0.0,0.00,260.3,0,"
    "231,999.9,999.9,999.9,484,356,11.9,x,e\n"
)


@pytest.fixture
def beach_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.chicago_beach")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.chicago_beach")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "chicago_beach"
    folder.mkdir()
    (folder / "water_sensors.csv").write_bytes(WATER.encode())
    (folder / "weather_stations.csv").write_bytes(WEATHER.encode())
    return list(ChicagoBeach(DatasetSettings(root=str(tmp_path))).load())


def by_type(readings, kind):
    return [r for r in readings if r.sensor.metadata and r.sensor.metadata.type == kind]


def test_pm_time_is_parsed(tmp_path):
    air = by_type(load(tmp_path), "air_temperature")
    assert air[-1].sensor.timestamp == datetime(2018, 9, 27, 13, tzinfo=UTC)
    assert air[0].sensor.sensor_id == (
        "chicago_beach:63rd Street Weather Station:air_temperature"
    )
    assert air[0].sensor.unit == "°C"


def test_unsorted_rows_come_out_in_time_order(tmp_path):
    air = by_type(load(tmp_path), "air_temperature")
    assert [r.sensor.value for r in air] == [15.5, 16.4]


def test_skipped_columns_are_absent(tmp_path):
    kinds = {r.sensor.metadata.type for r in load(tmp_path) if r.sensor.metadata}
    assert "air_temperature" in kinds
    assert not kinds & {"total_rain", "battery_life", "heading", "precipitation_type"}
    # the lake file and solar radiation are not flood readings
    assert not kinds & {"water_temperature", "turbidity", "wave_height"}
    assert "solar_radiation" not in kinds


def test_empty_cell_is_skipped(tmp_path, beach_log):
    load(tmp_path)
    warnings = [r for r in beach_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {2}  # the two 999.9 wind speeds


def test_sentinels_are_skipped(tmp_path):
    readings = load(tmp_path)
    assert [r.sensor.value for r in by_type(readings, "wind_speed")] == [2.5]
    # 999.9 hPa is a real pressure: only the wind columns use it as a sentinel
    pressures = [r.sensor.value for r in by_type(readings, "barometric_pressure")]
    assert pressures == [999.9, 996.3]
