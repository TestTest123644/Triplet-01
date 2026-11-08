import json
import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.uk_traffic import UkTraffic
from guardian_layer.utils.dataset_config import DatasetSettings


def row(date, end, mph, volume):
    return {
        "Site Name": "A1M/2259B",
        "Report Date": date,
        "Time Period Ending": end,
        "Time Interval": "0",
        "0 - 10 mph": "",
        "Avg mph": mph,
        "Total Volume": volume,
    }


@pytest.fixture
def uk_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.uk_traffic")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.uk_traffic")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "uk_traffic"
    folder.mkdir()
    rows = [
        row("2024-06-01T00:00:00", "23:59:59", "61", "40"),
        row("2024-06-02T00:00:00", "00:14:59", "62", "47"),
    ]
    empty = [row("2024-06-01T00:00:00", "00:14:59", "", "")]
    for name, data in (("site_2.json", rows), ("site_13.json", empty)):
        (folder / name).write_bytes(json.dumps({"Header": {}, "Rows": data}).encode())
    return list(UkTraffic(DatasetSettings(root=str(tmp_path))).load())


def test_time_is_date_plus_period_end(tmp_path):
    readings = load(tmp_path)
    speed = [
        r
        for r in readings
        if r.sensor.metadata and r.sensor.metadata.type == "average_speed"
    ]
    assert [r.sensor.timestamp for r in speed] == [
        datetime(2024, 6, 1, 23, 59, 59, tzinfo=UTC),
        datetime(2024, 6, 2, 0, 14, 59, tzinfo=UTC),
    ]
    assert speed[0].sensor.sensor_id == "uk_traffic:site_2:average_speed"
    assert speed[0].sensor.unit == "mph"
    volume = [
        r for r in readings if r.sensor.metadata and r.sensor.metadata.type == "volume"
    ]
    assert [r.sensor.value for r in volume] == [40.0, 47.0]
    assert volume[0].sensor.unit == "vehicles"


def test_empty_values_are_skipped(tmp_path, uk_log):
    readings = load(tmp_path)
    assert not [r for r in readings if "site_13" in r.sensor.sensor_id]
    warnings = [r for r in uk_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {2}  # both empty values of site_13
