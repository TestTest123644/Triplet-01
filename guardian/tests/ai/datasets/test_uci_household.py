import io
import logging
import zipfile
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.uci_household import UciHousehold
from guardian_layer.utils.dataset_config import DatasetSettings

TXT = (
    "Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;"
    "Sub_metering_1;Sub_metering_2;Sub_metering_3\n"
    "16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000\n"
    "21/12/2006;11:23:00;?;?;?;?;?;?;\n"
)


@pytest.fixture
def household_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.uci_household")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.uci_household")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "uci_household_power"
    folder.mkdir()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("household_power_consumption.txt", TXT)
    (folder / "data.zip").write_bytes(buffer.getvalue())
    return list(UciHousehold(DatasetSettings(root=str(tmp_path))).load())


def test_date_and_time_join(tmp_path):
    readings = load(tmp_path)
    assert readings[0].sensor.timestamp == datetime(2006, 12, 16, 17, 24, tzinfo=UTC)
    assert readings[0].sensor.sensor_id == "uci_household:household:global_active_power"
    assert readings[0].sensor.unit == "kW"
    kinds = [r.sensor.metadata.type for r in readings if r.sensor.metadata]
    assert kinds == [
        "global_active_power",
        "global_reactive_power",
        "voltage",
        "sub_metering_1",
        "sub_metering_2",
        "sub_metering_3",
    ]  # Global_intensity is derived: skipped


def test_question_mark_is_skipped(tmp_path, household_log):
    assert len(load(tmp_path)) == 6
    warnings = [r for r in household_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {6}
