import gzip
import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.array_of_things import ArrayOfThings
from guardian_layer.utils.dataset_config import (
    ArrayOfThingsSettings,
    DatasetSettings,
    InjectionSettings,
)

ROWS = """\
timestamp,node_id,subsystem,sensor,parameter,value_raw,value_hrf
2018/01/01 00:00:06,001e0610e532,chemsense,co,concentration,10,0.5
2018/01/01 00:00:06,001e0610e532,chemsense,o3,concentration,20,0.7
2018/01/01 00:00:06,001e0610e532,chemsense,chemsense,id,5,000017e2bb
2018/01/01 00:00:06,001e0610e532,metsense,tmp112,temperature,-1106,-11.06
2018/01/01 00:00:06,001e0610e532,metsense,spv1840lr5h_b,intensity,512,0.3
2018/01/01 00:00:30,001e0610e532,chemsense,co,concentration,11,NA
2018/01/01 00:00:31,001e0610e532,chemsense,co,concentration,12,0.6
2018/01/01 00:00:31,001e0610e540,chemsense,co,concentration,12,0.4
"""


@pytest.fixture
def aot_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.array_of_things")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.array_of_things")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path, rows=ROWS, injection=None):
    folder = tmp_path / "array_of_things"
    folder.mkdir()
    (folder / ArrayOfThingsSettings().data_file).write_bytes(
        gzip.compress(rows.encode())
    )
    # no sampling by default: these tests are about parsing
    injection = injection or InjectionSettings(sample_every_hours={})
    settings = DatasetSettings(root=str(tmp_path), injection=injection)
    return list(ArrayOfThings(settings).load())


def test_type_is_sensor_and_parameter(tmp_path):
    readings = load(tmp_path)
    first = readings[0].sensor
    assert first.sensor_id == "array_of_things:001e0610e532:chemsense_co_concentration"
    assert first.cluster_id == "array_of_things:chemsense_co_concentration"
    assert first.metadata is not None
    assert first.metadata.type == "chemsense_co_concentration"
    assert readings[1].sensor.cluster_id == "array_of_things:chemsense_o3_concentration"
    types = {r.sensor.metadata.type for r in readings if r.sensor.metadata}
    assert "metsense_tmp112_temperature" in types


def test_time_and_value(tmp_path):
    first = load(tmp_path)[0]
    assert first.sensor.timestamp == datetime(2018, 1, 1, 0, 0, 6, tzinfo=UTC)
    assert first.sensor.value == 0.5
    assert first.sensor.unit == "unknown"
    assert first.sensor.metadata is not None
    assert first.sensor.metadata.lat is None
    assert first.label is None


def test_id_and_na_rows_are_skipped(tmp_path, aot_log):
    readings = load(tmp_path)
    assert len(readings) == 5
    types = {r.sensor.metadata.type for r in readings if r.sensor.metadata}
    assert not any(kind.endswith("_id") for kind in types)
    assert not any("spv1840lr5h_b" in kind for kind in types)  # the microphone
    warnings = [r for r in aot_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {3}


def test_packet_order_counts_per_sensor(tmp_path):
    orders: dict[str, list[int]] = {}
    for reading in load(tmp_path):
        assert reading.sensor.metadata is not None
        orders.setdefault(reading.sensor.sensor_id, []).append(
            reading.sensor.metadata.packet_order
        )
    assert orders["array_of_things:001e0610e532:chemsense_co_concentration"] == [1, 2]
    assert orders["array_of_things:001e0610e540:chemsense_co_concentration"] == [1]


def test_one_chip_on_two_boards_is_two_sensors(tmp_path):
    # the real file has tsl250rd/intensity on both the lightsense and metsense boards
    rows = """timestamp,node_id,subsystem,sensor,parameter,value_raw,value_hrf
2018/01/01 00:00:06,001e0610e532,lightsense,tsl250rd,intensity,123,0.21
2018/01/01 00:00:06,001e0610e532,metsense,tsl250rd,intensity,10,0.504
"""
    light, met = load(tmp_path, rows)
    assert light.sensor.sensor_id == (
        "array_of_things:001e0610e532:lightsense_tsl250rd_intensity"
    )
    assert met.sensor.sensor_id == (
        "array_of_things:001e0610e532:metsense_tsl250rd_intensity"
    )
    assert light.sensor.cluster_id != met.sensor.cluster_id


def test_malformed_rows_are_skipped_not_fatal(tmp_path):
    # one bad line hours into the 33 GB file must not end the pass
    rows = """\
timestamp,node_id,subsystem,sensor,parameter,value_raw,value_hrf
2018/01/01 00:00:06,001e0610e532,chemsense,co
not a time,001e0610e532,chemsense,co,concentration,10,0.5
2018/01/01 00:00:31,001e0610e532,chemsense,co,concentration,12,0.6
"""
    readings = load(tmp_path, rows)
    assert [r.sensor.value for r in readings] == [0.6]


def test_rows_outside_sampled_hours_are_not_read(tmp_path):
    rows = """timestamp,node_id,subsystem,sensor,parameter,value_raw,value_hrf
2018/01/01 00:00:06,001e0610e532,chemsense,co,concentration,10,0.5
2018/01/01 01:00:06,001e0610e532,chemsense,co,concentration,10,0.6
2018/01/01 02:00:06,001e0610e532,chemsense,co,concentration,10,0.7
"""
    injection = InjectionSettings(
        block_hours=1, sample_every_hours={"array_of_things": 3}
    )
    readings = load(tmp_path, rows, injection)
    assert [r.sensor.value for r in readings] == [0.5]
