import io
import zipfile
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.bellinge import Bellinge
from guardian_layer.utils.dataset_config import BellingeSettings, DatasetSettings
from guardian_layer.utils.schema import Anomalies

FLAGS = "man_remove,ffill,stamp,outbound,frozen,outlier"
HEADER = f",time,raw_value,value_no_errors,{FLAGS},depth_s,level\n"
F = "False"


def level_row(index, time, value, flags=()):
    marks = ",".join("True" if f in flags else F for f in FLAGS.split(","))
    return f"{index},{time},{value},{value},{marks},{value},17.1\n"


def write_zip(path, members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, text in members.items():
            archive.writestr(name, text.encode())
    path.write_bytes(buffer.getvalue())


CLEANED = {
    # listed before p1 on purpose: periods are read in order, not zip order
    "2_cleaned_data/G71F04R_Level1_System2000p2_proc_v6.csv": HEADER
    + level_row(0, "2020-01-06 08:41:00", "0.20")
    + level_row(1, "2020-01-06 08:42:00", "0.21", {"frozen"})
    + level_row(2, "2020-01-06 08:43:00", "0.00", {"stamp", "outbound"})
    + level_row(3, "2020-01-06 08:44:00", "3.10", {"outbound"})
    + level_row(4, "2020-01-06 08:45:00", "0.22", {"ffill"}),
    "2_cleaned_data/G71F04R_Level1_System2000p1_proc_v6.csv": HEADER
    + level_row(0, "2010-08-01 00:00:00", "0.10"),
    "2_cleaned_data/G71F05R_position_iFixp1_proc_v6.csv": HEADER
    + level_row(0, "2020-10-13 00:00:00", "0.6"),
    "2_cleaned_data/G71F68Yp1_iFixp1_proc_v6.csv": HEADER
    + level_row(0, "2020-10-13 00:00:00", "0.07")
    + level_row(1, "2020-10-13 00:01:00", "0.07", {"frozen"})
    + level_row(2, "2020-10-13 00:02:00", "0.00"),
    "2_cleaned_data/G71F68Y_LevelPS_iFixp1_proc_v6.csv": HEADER
    + level_row(0, "2020-10-13 00:00:00", "0.50"),
    "2_cleaned_data/G72F040_Danovap1_proc_v6.csv": HEADER
    + level_row(0, "2020-01-08 05:16:00", "0.09"),
    "2_cleaned_data/readme.txt": "Cleaned according to the scripts\n",
    # two period-1 files of one sensor from the two SCADA systems: the older first,
    # whatever the names sort to ("System2000" sorts before "iFix")
    "2_cleaned_data/G80F13P_LevelPS_System2000p1_proc_v6.csv": HEADER
    + level_row(0, "2020-03-01 00:00:00", "0.70"),
    "2_cleaned_data/G80F13P_LevelPS_iFixp1_proc_v6.csv": HEADER
    + level_row(0, "2019-03-01 00:00:00", "0.60"),
}
GAUGES = {
    "#3a_Raingauges/5425_ts.txt": "Rainfall Intensity[mu-m/sec]:Mean_Step_Accumulated\n"
    "Time;5425_data\n"
    "2009-05-19 23:59:00;3.333\n"
    "2009-05-20 00:30:00;1.0\n"
    "2009-06-05 00:00:00;0.667\n",
    "#3a_Raingauges/5425_missing_data.txt": "------\n  STATIONSNEDBRUD\n------\n"
    "  Start UTC        Stop UTC      Varighed\n"
    "20090520 00:00  20090604 14:00    15 14:00\n",
    "#3a_Raingauges/Aabakken_bellinge_vandvaerk_v2_ts.txt": "Rainfall Intensity\n"
    "Time;Aabakken_bellinge_vandvaerk_v2\n"
    "17-08-2017 14:06:00;0\n",
}


@pytest.fixture
def readings(tmp_path):
    return list(Bellinge(fixture(tmp_path)).load())


def fixture(tmp_path):
    folder = tmp_path / "bellinge"
    folder.mkdir()
    write_zip(folder / "2_cleaned_data.zip", CLEANED)
    write_zip(folder / "3a_Raingauges.zip", GAUGES)
    # the fixture has two of the three real gauges
    gauges = {
        name: station
        for name, station in BellingeSettings().gauges.items()
        if name in GAUGES
    }
    return DatasetSettings(root=str(tmp_path), bellinge=BellingeSettings(gauges=gauges))


def sensor(readings, sensor_id):
    return [r for r in readings if r.sensor.sensor_id == sensor_id]


def test_flags_map_to_labels_and_skips(readings):
    level = sensor(readings, "bellinge:G71F04R_Level1:water_depth")
    later = [r for r in level if r.sensor.timestamp.year == 2020]
    # stamp and ffill skip the reading; outbound beats nothing else here
    assert [(r.sensor.value, r.label) for r in later] == [
        (0.20, None),
        (0.21, Anomalies.SENSOR_MALFUNCTION),
        (3.10, Anomalies.OUT_OF_RANGE),
    ]
    assert level[0].sensor.unit == "m"
    assert level[0].sensor.cluster_id == "bellinge:water_depth"


def test_periods_are_read_in_order(readings):
    level = sensor(readings, "bellinge:G71F04R_Level1:water_depth")
    assert level[0].sensor.timestamp == datetime(2010, 8, 1, tzinfo=UTC)
    orders = [r.sensor.metadata.packet_order for r in level if r.sensor.metadata]
    assert orders == [1, 2, 3, 4]


def test_danova_file_is_a_level_series(readings):
    assert [
        r.sensor.value for r in sensor(readings, "bellinge:G72F040_Level:water_depth")
    ] == [0.09]


def test_position_file_is_skipped(readings):
    ids = {r.sensor.sensor_id for r in readings}
    assert not [i for i in ids if "G71F05R" in i]


def test_pump_is_loaded_without_labels(readings):
    pump = sensor(readings, "bellinge:G71F68Y_p1:pump_flow")
    assert [r.sensor.value for r in pump] == [0.07, 0.07, 0.0]
    assert pump[0].sensor.cluster_id == "bellinge:pump_flow"
    assert pump[0].sensor.unit == "m3/s"
    # a constant-speed pump is flagged frozen: not a fault
    assert all(r.label is None for r in pump)
    # the sump level of the same pumping station is a separate sensor
    assert sensor(readings, "bellinge:G71F68Y_LevelPS:water_depth")


def test_gauge_reading_in_a_missing_period_is_skipped(readings):
    gauge = sensor(readings, "bellinge:5425:rain_intensity")
    assert [r.sensor.value for r in gauge] == [3.333, 0.667]
    assert gauge[0].sensor.unit == "µm/s"


def test_gauge_time_formats_differ(readings):
    aabakken = sensor(readings, "bellinge:aabakken:rain_intensity")
    assert aabakken[0].sensor.timestamp == datetime(2017, 8, 17, 14, 6, tzinfo=UTC)


def test_skips_are_counted(tmp_path, caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    import logging

    module_logger = logging.getLogger("guardian_layer.ai.datasets.bellinge")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.bellinge")
    try:
        list(Bellinge(fixture(tmp_path)).load())
    finally:
        module_logger.removeHandler(caplog.handler)
    counts = {
        r.path: r.skipped
        for r in caplog.records
        if r.levelno == logging.WARNING and hasattr(r, "skipped")  # per-file counts
    }
    assert sorted(counts.values()) == [1, 2]  # stamp + ffill; the missing period


def test_files_of_one_sensor_are_read_oldest_first(readings):
    level = sensor(readings, "bellinge:G80F13P_LevelPS:water_depth")
    assert [r.sensor.value for r in level] == [0.60, 0.70]
