import io
import logging
import zipfile
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.uci_load import UciLoad
from guardian_layer.utils.dataset_config import DatasetSettings

TXT = (
    '"";"MT_001";"MT_002"\n'
    '"2011-01-01 00:15:00";0;2,5\n'
    '"2011-01-01 00:30:00";17,766;0\n'
    '"2011-01-01 00:45:00";0;x\n'
)


@pytest.fixture
def load_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.uci_load")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.uci_load")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "uci_load_370_meters"
    folder.mkdir()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("LD2011_2014.txt", TXT)
        archive.writestr("__MACOSX/._LD2011_2014.txt", "junk")
    (folder / "data.zip").write_bytes(buffer.getvalue())
    return list(UciLoad(DatasetSettings(root=str(tmp_path))).load())


def meter(readings, name):
    return [r for r in readings if r.sensor.sensor_id == f"uci_load:{name}:load"]


def test_leading_zeros_are_skipped_then_kept(tmp_path):
    readings = load(tmp_path)
    one = meter(readings, "MT_001")
    # the meter was not installed before its first reading; zeros after that count
    assert [r.sensor.value for r in one] == [17.766, 0.0]
    assert one[0].sensor.timestamp == datetime(2011, 1, 1, 0, 30, tzinfo=UTC)
    assert one[0].sensor.unit == "kW"
    assert [r.sensor.value for r in meter(readings, "MT_002")] == [2.5, 0.0]


def test_decimal_comma_parses(tmp_path):
    assert meter(load(tmp_path), "MT_002")[0].sensor.value == 2.5


def test_bad_value_is_counted(tmp_path, load_log):
    load(tmp_path)
    warnings = [r for r in load_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {1}  # the "x"; leading zeros are expected
