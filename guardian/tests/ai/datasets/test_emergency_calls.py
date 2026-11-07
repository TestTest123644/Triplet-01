import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.emergency_calls import EmergencyCalls
from guardian_layer.utils.dataset_config import DatasetSettings

# the flood windows are whole days: 1 Sept 2021 (NYC) and 29 Aug 2021 (New Orleans)
NYC = """\
incident_datetime,borough
2021-08-31T23:59:10.000,BROOKLYN
2021-08-31T23:59:50.000,BROOKLYN
2021-09-01T00:00:05.000,BROOKLYN
2021-09-01T00:03:30.000,BROOKLYN
2021-09-01T00:00:20.000,QUEENS
2021-09-01T00:01:00.000,
not a time,QUEENS
"""
NOLA = """\
timecreate,policedistrict,selfinitiated
2021-08-29T10:00:01.250,3,N
2021-08-29T10:00:40.000,3,N
2021-08-29T10:00:50.000,3,Y
2021-08-29T10:02:00.000,3,N
2021-08-30T00:00:00.000,3,N
"""


@pytest.fixture
def calls_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.emergency_calls")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.emergency_calls")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "emergency_calls"
    folder.mkdir()
    (folder / "nyc_ems.csv").write_bytes(NYC.encode())
    (folder / "nola_calls.csv").write_bytes(NOLA.encode())
    return list(EmergencyCalls(DatasetSettings(root=str(tmp_path))).load())


def sensor(readings, sensor_id):
    return [r for r in readings if r.sensor.sensor_id == sensor_id]


def test_calls_are_counted_per_zone_and_minute(tmp_path):
    brooklyn = sensor(load(tmp_path), "nyc_ems:BROOKLYN:call_count")
    first = brooklyn[0].sensor
    assert first.timestamp == datetime(2021, 8, 31, 23, 59, tzinfo=UTC)
    assert first.value == 2.0  # two calls in the same minute
    assert first.unit == "calls/min"
    assert first.metadata is not None
    assert first.metadata.type == "call_count"


def test_empty_minutes_between_calls_are_zero(tmp_path):
    brooklyn = sensor(load(tmp_path), "nyc_ems:BROOKLYN:call_count")
    # 23:59 to 00:03 is five minutes, one reading each, none left out
    assert [r.sensor.value for r in brooklyn] == [2.0, 1.0, 0.0, 0.0, 1.0]
    orders = [r.sensor.metadata.packet_order for r in brooklyn if r.sensor.metadata]
    assert orders == [1, 2, 3, 4, 5]


def test_each_city_is_its_own_cluster(tmp_path):
    clusters = {r.sensor.cluster_id for r in load(tmp_path)}
    assert clusters == {"nyc_ems:call_count", "nola_calls:call_count"}


def test_flood_day_readings_are_normal_events(tmp_path):
    readings = load(tmp_path)
    brooklyn = sensor(readings, "nyc_ems:BROOKLYN:call_count")
    # the day before is not an event; midnight starts the flood day
    assert [r.event for r in brooklyn] == [False, True, True, True, True]
    assert all(r.label is None for r in readings)
    nola = sensor(readings, "nola_calls:3:call_count")
    assert [r.event for r in nola][:3] == [True, True, True]
    assert nola[-1].event is False  # 30 Aug 00:00 is after the window (end is open)


def test_officer_initiated_calls_are_not_counted(tmp_path):
    nola = sensor(load(tmp_path), "nola_calls:3:call_count")
    assert [r.sensor.value for r in nola[:3]] == [2.0, 0.0, 1.0]  # the Y row is out


def test_bad_rows_are_skipped_and_counted(tmp_path, calls_log):
    load(tmp_path)
    warnings = [r for r in calls_log.records if r.levelno == logging.WARNING]
    # NYC: the call without a borough and the one without a time
    assert {r.skipped for r in warnings} == {2}
