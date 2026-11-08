import gzip
import logging
from datetime import UTC, datetime
from math import cos, radians

import pytest

from guardian_layer.ai.datasets.intel_lab import IntelLab
from guardian_layer.utils.dataset_config import DatasetSettings
from guardian_layer.utils.schema import Anomalies

# date time epoch mote temperature humidity light voltage, as in data.txt
ROWS = """\
2004-02-28 00:59:16.02785 3 1 19.9884 37.0933 45.08 2.69964
2004-02-28 01:03:16 4 1 19.3 38.4 45.08 2.39
2004-02-28 01:05:00 5 1 122.153 -3.91901 11.04 2.6
2004-02-28 01:06:00 6 1 19.0 38.0 45.0
2004-02-28 01:07:00 65535 1 19.0 38.0 45.0 2.6
2004-02-28 01:08:00 7 55 19.0 38.0 45.0 2.6
2004-02-28 01:09:00 8 2 19.0 nan 45.0 2.6
2004-02-28 01:10:00 9 2 19.0 38.0 45.0 2.6
"""


@pytest.fixture
def readings(tmp_path):
    folder = tmp_path / "intel_lab"
    folder.mkdir()
    (folder / "data.txt.gz").write_bytes(gzip.compress(ROWS.encode()))
    (folder / "mote_locs.txt").write_bytes(b"1 21.5 23\n")
    return list(IntelLab(DatasetSettings(root=str(tmp_path))).load())


@pytest.fixture
def intel_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.intel_lab")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.intel_lab")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def test_a_row_gives_one_reading_per_type(readings):
    first = readings[:3]
    assert [r.sensor.sensor_id for r in first] == [
        "intel_lab:1:temperature",
        "intel_lab:1:humidity",
        "intel_lab:1:light",
    ]
    assert [r.sensor.cluster_id for r in first] == [
        "intel_lab:temperature",
        "intel_lab:humidity",
        "intel_lab:light",
    ]
    assert [r.sensor.unit for r in first] == ["C", "%", "lux"]
    temperature = first[0]
    assert temperature.sensor.value == 19.9884
    assert temperature.sensor.timestamp == datetime(2004, 2, 28, 0, 59, 16, 27850, UTC)
    assert temperature.sensor.metadata is not None
    assert temperature.sensor.metadata.packet_order == 3
    assert temperature.sensor.metadata.type == "temperature"
    assert temperature.sensor.metadata.lower_range == 0.0
    assert temperature.sensor.metadata.upper_range == 50.0
    assert all(r.label is None and r.event is False for r in first)


def test_location_is_offset_from_the_origin(readings):
    mote_1 = readings[0].sensor.metadata
    assert mote_1 is not None
    assert mote_1.lat == pytest.approx(36.79 + 23 / 111320)
    assert mote_1.lon == pytest.approx(10.18 + 21.5 / (111320 * cos(radians(36.79))))
    mote_2 = readings[-1].sensor.metadata  # no line in mote_locs.txt
    assert mote_2 is not None
    assert mote_2.lat is None
    assert mote_2.lon is None


def test_low_voltage_labels_the_whole_row(readings):
    row = readings[3:6]
    assert {r.sensor.metadata.packet_order for r in row if r.sensor.metadata} == {4}
    assert all(r.label is Anomalies.SENSOR_MALFUNCTION for r in row)


def test_impossible_value_labels_the_whole_row(readings):
    row = readings[6:9]
    assert {r.sensor.metadata.packet_order for r in row if r.sensor.metadata} == {5}
    assert all(r.label is Anomalies.SENSOR_MALFUNCTION for r in row)


def test_bad_rows_are_skipped_and_counted(tmp_path, intel_log):
    folder = tmp_path / "intel_lab"
    folder.mkdir()
    (folder / "data.txt.gz").write_bytes(gzip.compress(ROWS.encode()))
    (folder / "mote_locs.txt").write_bytes(b"1 21.5 23\n")
    readings = list(IntelLab(DatasetSettings(root=str(tmp_path))).load())
    assert len(readings) == 12
    orders = [r.sensor.metadata.packet_order for r in readings if r.sensor.metadata]
    assert list(dict.fromkeys(orders)) == [3, 4, 5, 9]
    assert all(
        r.sensor.metadata.type != "voltage" for r in readings if r.sensor.metadata
    )
    # caplog may see the one warning through two handlers
    warnings = [r for r in intel_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {4}  # short row, bad epoch, mote 55, nan


def test_readings_come_in_time_order_not_epoch_order(tmp_path):
    # data.txt is sorted by epoch, and a mote's epoch restarts when it reboots
    rows = """\
2004-03-31 03:38:15 2 1 19.0 38.0 45.0 2.6
2004-02-28 00:59:16 3 1 20.0 38.0 45.0 2.6
"""
    folder = tmp_path / "intel_lab"
    folder.mkdir()
    (folder / "data.txt.gz").write_bytes(gzip.compress(rows.encode()))
    (folder / "mote_locs.txt").write_bytes(b"1 21.5 23\n")
    readings = list(IntelLab(DatasetSettings(root=str(tmp_path))).load())
    temperature = [
        r for r in readings if r.sensor.sensor_id == "intel_lab:1:temperature"
    ]
    assert [r.sensor.value for r in temperature] == [20.0, 19.0]
    assert [
        r.sensor.metadata.packet_order for r in temperature if r.sensor.metadata
    ] == [
        3,
        2,
    ]  # packet_order stays the epoch, as in the notebook


def test_malformed_epoch_or_date_is_skipped_not_fatal(tmp_path):
    rows = """\
2004-02-28 00:59:16 3.5 1 19.0 38.0 45.0 2.6
2004-02-30 00:59:16 4 1 19.0 38.0 45.0 2.6
2004-02-28 01:00:00 5 1 21.0 38.0 45.0 2.6
"""
    folder = tmp_path / "intel_lab"
    folder.mkdir()
    (folder / "data.txt.gz").write_bytes(gzip.compress(rows.encode()))
    (folder / "mote_locs.txt").write_bytes(b"1 21.5 23\n")
    readings = list(IntelLab(DatasetSettings(root=str(tmp_path))).load())
    assert {r.sensor.metadata.packet_order for r in readings if r.sensor.metadata} == {
        5
    }
