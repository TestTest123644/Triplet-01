import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.ett import Ett
from guardian_layer.utils.dataset_config import DatasetSettings

HEADER = "date,HUFL,HULL,MUFL,MULL,LUFL,LULL,OT\n"


@pytest.fixture
def ett_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.ett")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.ett")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "ett_transformer"
    folder.mkdir()
    one = HEADER + "2016-07-01 00:00:00,5.8,2.0,1.6,0.46,4.2,1.34,30.5\n"
    two = HEADER + "2016-07-01 00:00:00,41.1,12.5,36.5,9.3,4.4,1.3,38.6\n"
    (folder / "ETTm1.csv").write_bytes(one.encode())
    (folder / "ETTm2.csv").write_bytes(two.encode())
    return list(Ett(DatasetSettings(root=str(tmp_path))).load())


def test_columns_become_types(tmp_path):
    readings = load(tmp_path)
    kinds = [r.sensor.metadata.type for r in readings[:6] if r.sensor.metadata]
    assert kinds == ["hufl", "hull", "mufl", "mull", "lufl", "lull"]
    assert not [r for r in readings if r.sensor.sensor_id.endswith(":ot")]  # OT dropped
    assert readings[0].sensor.unit == "unknown"
    assert readings[0].sensor.timestamp == datetime(2016, 7, 1, tzinfo=UTC)


def test_station_per_file(tmp_path):
    ids = {
        r.sensor.sensor_id
        for r in load(tmp_path)
        if r.sensor.sensor_id.endswith(":hufl")
    }
    assert ids == {"ett:transformer1:hufl", "ett:transformer2:hufl"}


def test_bad_value_is_skipped(tmp_path, ett_log):
    folder = tmp_path / "ett_transformer"
    folder.mkdir()
    (folder / "ETTm1.csv").write_bytes(
        (HEADER + "2016-07-01 00:00:00,x,1,1,1,1,1,1\n").encode()
    )
    (folder / "ETTm2.csv").write_bytes(HEADER.encode())
    readings = list(Ett(DatasetSettings(root=str(tmp_path))).load())
    assert len(readings) == 5
    warnings = [r for r in ett_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {1}
