import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.uk_flood import UkFlood
from guardian_layer.utils.dataset_config import DatasetSettings

URL = "http://environment.data.gov.uk/flood-monitoring/id"
HEADER = (
    "dateTime,date,measure,station,label,stationReference,parameter,qualifier,"
    "datumType,period,unitName,valueType,value\n"
)


def line(time, measure, parameter, qualifier, unit, value):
    return (
        f"{time},{time[:10]},{URL}/measures/{measure},{URL}/stations/4387TH,Label,"
        f"4387TH,{parameter},{qualifier},,900,{unit},instantaneous,{value}\n"
    )


@pytest.fixture
def flood_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.uk_flood")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.uk_flood")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "uk_flood"
    folder.mkdir()
    asd = "4387TH-level-stage-i-15_min-mASD"
    aod = "4387TH-level-stage-i-15_min-mAOD"
    day1 = (
        HEADER
        + line("2026-10-09T00:15:00Z", asd, "level", "Stage", "mASD", "1.2")
        + line("2026-10-09T00:00:00Z", asd, "level", "Stage", "mASD", "1.1")
        + line("2026-10-09T00:00:00Z", aod, "level", "Stage", "mAOD", "20.1")
        + line("2026-10-09T00:30:00Z", asd, "level", "Stage", "mASD", "0.5|0.6")
        + line(
            "2026-10-09T00:00:00Z",
            "4387TH-flow--i-15_min-m3_s",
            "flow",
            "",
            "m3/s",
            "9",
        )
    )
    day2 = HEADER + line("2026-10-10T00:00:00Z", asd, "level", "Stage", "mASD", "1.3")
    (folder / "readings-full-2026-10-09.csv").write_bytes(day1.encode())
    (folder / "readings-full-2026-10-10.csv").write_bytes(day2.encode())
    return list(UkFlood(DatasetSettings(root=str(tmp_path))).load())


def test_two_measures_of_one_station_stay_apart(tmp_path):
    readings = load(tmp_path)
    ids = {r.sensor.sensor_id for r in readings}
    assert ids == {
        "uk_flood:4387TH-level-stage-i-15_min-mASD:level_stage_masd",
        "uk_flood:4387TH-level-stage-i-15_min-mAOD:level_stage_maod",
    }
    assert {r.sensor.unit for r in readings} == {"mASD", "mAOD"}
    # a datum is part of the type: heights above sea level and local depths differ
    clusters = {r.sensor.cluster_id for r in readings}
    assert clusters == {"uk_flood:level_stage_masd", "uk_flood:level_stage_maod"}


def test_flow_is_not_loaded(tmp_path):
    kinds = {r.sensor.metadata.type for r in load(tmp_path) if r.sensor.metadata}
    assert not [kind for kind in kinds if kind.startswith("flow")]


def test_rows_are_sorted_per_measure(tmp_path):
    asd = [r for r in load(tmp_path) if r.sensor.unit == "mASD"]
    assert [r.sensor.value for r in asd] == [1.1, 1.2, 1.3]
    assert asd[0].sensor.timestamp == datetime(2026, 10, 9, tzinfo=UTC)
    orders = [r.sensor.metadata.packet_order for r in asd if r.sensor.metadata]
    assert orders == [1, 2, 3]


def test_pipe_value_is_skipped(tmp_path, flood_log):
    readings = load(tmp_path)
    assert 0.5 not in {r.sensor.value for r in readings}
    warnings = [r for r in flood_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {1}
