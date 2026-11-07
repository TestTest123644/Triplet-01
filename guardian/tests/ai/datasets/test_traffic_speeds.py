import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.traffic_speeds import TrafficSpeeds
from guardian_layer.utils.dataset_config import DatasetSettings

# both files come newest-first, as the real downloads do
CHICAGO = """\
"time","region_id","speed","bus_count","num_reads"
"2026-04-30T01:30:00.000","29","24","4","21"
"2026-04-30T01:20:00.000","29","22","4","20"
"2026-04-30T01:10:00.000","29","20","2","39"
"2026-04-30T01:00:00.000","29","0","0","0"
"""
NYC = """\
"id","link_id","speed","travel_time","status","data_as_of"
"311","4362342","31.6","120","0","2026-10-11T19:27:06.000"
"312","4362342","0","0","-101","2026-10-11T19:25:06.000"
"313","4362342","28.0","130","-101","2026-10-11T19:22:06.000"
"""


@pytest.fixture
def speeds_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.traffic_speeds")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.traffic_speeds")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "traffic_speeds"
    folder.mkdir()
    (folder / "chicago_bus_regions.csv").write_bytes(CHICAGO.encode())
    (folder / "nyc_links.csv").write_bytes(NYC.encode())
    return list(TrafficSpeeds(DatasetSettings(root=str(tmp_path))).load())


def test_ids_and_types(tmp_path):
    readings = load(tmp_path)
    ids = {r.sensor.sensor_id for r in readings}
    assert ids == {
        "traffic_speeds:chicago_29:bus_region_speed",
        "traffic_speeds:nyc_4362342:link_speed",
    }
    clusters = {r.sensor.cluster_id for r in readings}
    assert clusters == {"traffic_speeds:bus_region_speed", "traffic_speeds:link_speed"}
    assert {r.sensor.unit for r in readings} == {"unknown"}


def test_newest_first_rows_come_out_oldest_first(tmp_path):
    chicago = [r for r in load(tmp_path) if "chicago" in r.sensor.sensor_id]
    assert [r.sensor.value for r in chicago] == [20.0, 22.0, 24.0]
    assert chicago[0].sensor.timestamp == datetime(2026, 4, 30, 1, 10, tzinfo=UTC)
    orders = [r.sensor.metadata.packet_order for r in chicago if r.sensor.metadata]
    assert orders == [1, 2, 3]


def test_nyc_zero_travel_time_is_skipped(tmp_path, speeds_log):
    nyc = [r for r in load(tmp_path) if "nyc" in r.sensor.sensor_id]
    assert [r.sensor.value for r in nyc] == [28.0, 31.6]
    warnings = [r for r in speeds_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {1}


def test_chicago_row_without_buses_is_skipped(tmp_path):
    # no bus seen in the region: speed 0 there is no measurement
    chicago = [r for r in load(tmp_path) if "chicago" in r.sensor.sensor_id]
    assert 0.0 not in {r.sensor.value for r in chicago}
