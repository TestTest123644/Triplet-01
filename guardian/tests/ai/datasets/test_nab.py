import json
import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.nab import Nab
from guardian_layer.utils.dataset_config import DatasetSettings, NabSettings

SPEED = "realTraffic/speed_6005.csv"
OCCUPANCY = "realTraffic/occupancy_6005.csv"
SPEED_ROWS = """\
timestamp,value
2015-09-16 18:30:00,80
2015-09-16 18:40:00,70
2015-09-16 18:45:00,40
2015-09-16 18:50:00,60
2015-09-16 19:00:00,82
2015-09-16 19:10:00,
"""
OCCUPANCY_ROWS = """\
timestamp,value
2013-07-04 00:00:00,69.88
2013-07-04 01:00:00,71.22
"""
WINDOWS = {
    SPEED: [["2015-09-16 18:40:00.000000", "2015-09-16 18:50:00.000000"]],
    OCCUPANCY: [],
}


@pytest.fixture
def nab_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.nab")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.nab")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def nab_settings(tmp_path):
    folder = tmp_path / "nab"
    folder.mkdir()
    (folder / "data__realTraffic__speed_6005.csv").write_bytes(SPEED_ROWS.encode())
    (folder / "data__realTraffic__occupancy_6005.csv").write_bytes(
        OCCUPANCY_ROWS.encode()
    )
    (folder / "labels__combined_windows.json").write_text(json.dumps(WINDOWS))
    series = {name: NabSettings().series[name] for name in (SPEED, OCCUPANCY)}
    return DatasetSettings(root=str(tmp_path), nab=NabSettings(series=series))


@pytest.fixture
def readings(tmp_path):
    return list(Nab(nab_settings(tmp_path)).load())


def speed(readings):
    return [
        r for r in readings if r.sensor.metadata and r.sensor.metadata.type == "speed"
    ]


def test_ids_and_units(readings):
    first = speed(readings)[0]
    assert first.sensor.sensor_id == "nab:6005:speed"
    assert first.sensor.cluster_id == "nab:speed"
    assert first.sensor.unit == "unknown"  # NAB states no units
    assert first.sensor.value == 80.0
    assert first.sensor.timestamp == datetime(2015, 9, 16, 18, 30, tzinfo=UTC)
    assert first.sensor.metadata is not None
    assert first.sensor.metadata.lat is None
    occupancy = [r for r in readings if r.sensor.sensor_id == "nab:6005:occupancy"]
    assert len(occupancy) == 2


def test_window_readings_are_normal_events(readings):
    assert [r.event for r in speed(readings)] == [False, True, True, True, False]
    assert all(r.label is None for r in readings)


def test_packet_order_counts_per_sensor(readings):
    orders = [r.sensor.metadata.packet_order for r in readings if r.sensor.metadata]
    assert orders == [1, 2, 3, 4, 5, 1, 2]


def test_empty_value_is_skipped_and_counted(tmp_path, nab_log):
    # load here, not in a fixture: caplog.records holds only the test's own phase
    readings = list(Nab(nab_settings(tmp_path)).load())
    assert len(speed(readings)) == 5
    warnings = [r for r in nab_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {1}


def test_skipped_files_are_not_loaded():
    names = " ".join(NabSettings().series)
    assert "machine_temperature" not in names
    assert "nyc_taxi" not in names
    assert "ambient_temperature" not in names  # indoor office, not in the scenario
    assert len(NabSettings().series) == 7


def test_malformed_timestamp_is_skipped_not_fatal(tmp_path):
    folder = tmp_path / "nab"
    folder.mkdir()
    rows = "timestamp,value\nnot a time,50\n2015-09-16 18:30:00,80\n"
    (folder / "data__realTraffic__speed_6005.csv").write_bytes(rows.encode())
    (folder / "labels__combined_windows.json").write_text(json.dumps({SPEED: []}))
    series = {SPEED: NabSettings().series[SPEED]}
    settings = DatasetSettings(root=str(tmp_path), nab=NabSettings(series=series))
    assert [r.sensor.value for r in Nab(settings).load()] == [80.0]
