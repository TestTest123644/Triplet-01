import io
import logging
import zipfile
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.uci_air_quality import UciAirQuality
from guardian_layer.utils.dataset_config import DatasetSettings

CSV = (
    "Date;Time;CO(GT);PT08.S1(CO);NMHC(GT);C6H6(GT);PT08.S2(NMHC);NOx(GT);PT08.S3(NOx);"
    "NO2(GT);PT08.S4(NO2);PT08.S5(O3);T;RH;AH;;\n"
    "10/03/2004;18.00.00;2,6;1360;150;11,9;1046;166;1056;113;1692;1268;13,6;48,9;0,7578;;\n"
    "10/03/2004;19.00.00;-200;1292;112;-200,0;955;103;1174;92;1559;972;13,3;47,7;0,7255;;\n"
    ";;;;;;;;;;;;;;;;\n"
)


@pytest.fixture
def aq_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.uci_air_quality")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.uci_air_quality")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "uci_air_quality"
    folder.mkdir()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("AirQualityUCI.csv", CSV)
    (folder / "data.zip").write_bytes(buffer.getvalue())
    return list(UciAirQuality(DatasetSettings(root=str(tmp_path))).load())


def by_type(readings, kind):
    return [r for r in readings if r.sensor.metadata and r.sensor.metadata.type == kind]


def test_decimal_comma_parses(tmp_path):
    readings = load(tmp_path)
    co = by_type(readings, "co_gt")
    assert co[0].sensor.value == 2.6
    assert co[0].sensor.unit == "mg/m³"
    assert co[0].sensor.sensor_id == "uci_air_quality:multisensor:co_gt"
    assert co[0].sensor.timestamp == datetime(2004, 3, 10, 18, tzinfo=UTC)
    assert [r.sensor.value for r in by_type(readings, "temperature")] == [13.6, 13.3]


def test_minus_200_is_skipped(tmp_path):
    readings = load(tmp_path)
    assert len(by_type(readings, "co_gt")) == 1
    assert len(by_type(readings, "c6h6_gt")) == 1  # written -200,0 in the file


def test_absolute_humidity_is_skipped(tmp_path):
    assert not by_type(load(tmp_path), "ah")


def test_empty_row_is_skipped(tmp_path, aq_log):
    readings = load(tmp_path)
    assert len(readings) == 2 * 12 - 2  # 12 kept columns, two -200
    warnings = [r for r in aq_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {14}  # two -200; 12 values of the empty row
