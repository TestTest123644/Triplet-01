import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.usgs import Usgs
from guardian_layer.utils.dataset_config import DatasetSettings

RDB = (
    "# ---- WARNING ----\n"
    "#    252060      00065     Gage height, feet\n"
    "#    69928       00060     Discharge, cubic feet per second\n"
    "#    252061      00010     Temperature, water, degrees Celsius\n"
    "agency_cd\tsite_no\tdatetime\ttz_cd\t252060_00065\t252060_00065_cd\t69928_00060\t69928_00060_cd\t252061_00010\t252061_00010_cd\n"
    "5s\t15s\t20d\t6s\t14n\t10s\t14n\t10s\t14n\t10s\n"
    "USGS\t01646500\t2026-09-12 20:25\tEDT\t2.8\tP\t1980\tP\t26.8\tP\n"
    "USGS\t01646500\t2026-09-12 20:30\tEDT\t\t\t2030\tP\t26.8\tP\n"
    "USGS\t01646500\t2026-09-12 20:35\tEDT\t2.9\tP\t2040\tP\t26.9\tP\n"
)


@pytest.fixture
def usgs_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.usgs")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.usgs")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "usgs_water"
    folder.mkdir()
    (folder / "01646500.rdb").write_bytes(RDB.encode())
    return list(Usgs(DatasetSettings(root=str(tmp_path))).load())


def test_usgs_time_zone_is_applied(tmp_path):
    first = load(tmp_path)[0].sensor
    assert first.timestamp == datetime(2026, 9, 13, 0, 25, tzinfo=UTC)  # EDT is -4 h
    assert first.sensor_id == "usgs:01646500:gage_height"
    assert first.cluster_id == "usgs:gage_height"
    assert first.unit == "ft"
    assert first.value == 2.8


def test_unlisted_codes_are_skipped(tmp_path):
    kinds = {r.sensor.metadata.type for r in load(tmp_path) if r.sensor.metadata}
    # discharge 00060 is derived and water quality is not a flood reading
    assert kinds == {"gage_height"}


def test_empty_cell_is_skipped(tmp_path, usgs_log):
    readings = load(tmp_path)
    assert [r.sensor.value for r in readings] == [2.8, 2.9]
    warnings = [r for r in usgs_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {1}
