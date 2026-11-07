import io
import logging
import zipfile
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.grid_frequency import GridFrequency, GridFrequency100ms
from guardian_layer.utils.dataset_config import DatasetSettings, InjectionSettings

EE = (
    "Time;f50_EE;QI_EE;f50_DE_KA;QI_DE_KA\n"
    "2019-03-25 11:09:11;-19.0355;0;1.59044;0\n"
    "2019-03-25 11:09:12;-19.5;2;0.97;0\n"
    "2019-03-25 11:09:13;0;1;0.5;0\n"
    "2019-03-25 11:09:14;NaN;NaN;0.4;0\n"
    "2019-03-25 11:09:15;-18.0;0;0.3;0\n"
)
ZA = ";f50;QI\n2025-05-01 00:00:00;158.77;0\n"  # no time header in the real file
SYNC = "Time;f50_DE_KA;QI_DE_KA\n2019-07-09 00:00:00;-12.2;0\n"
ECLIPSE = "freq,QI\n50.002,0\n"


@pytest.fixture
def grid_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.grid_frequency")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.grid_frequency")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path, extra=None, injection=None, loader=GridFrequency):
    folder = tmp_path / "grid_frequency"
    folder.mkdir()
    files = {
        "FrequencyData__EE01__EE01.csv": EE,
        "FrequencyData__EE01__Numbers.txt": "Begins: 2019-03-25 11:09:11\n",
        "FrequencyData__ZA02__ZA02.csv": ZA,
        "FrequencyData__sync01__SYNC01.csv": SYNC,
        "FrequencyData__ES_PM_solar_eclipse_2026__"
        "ES_PM_solar_eclipse_2026.csv": ECLIPSE,
    }
    files.update(extra or {})
    for name, text in files.items():
        (folder / name).write_bytes(text.encode())
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "DE_KA01_100msForOSF.csv",
            "Time;f50_DE_KA;QI_DE_KA\n2019-07-08 23:59:59.1;-12.785;0\n",
        )
    (folder / "FrequencyData__DE_KA01__DE_KA01_100ms.zip").write_bytes(
        buffer.getvalue()
    )
    # no sampling by default: these tests are about parsing
    injection = injection or InjectionSettings(sample_every_hours={})
    settings = DatasetSettings(root=str(tmp_path), injection=injection)
    return list(loader(settings).load())


def station(readings, name, source="grid_frequency"):
    return [
        r
        for r in readings
        if r.sensor.sensor_id == f"{source}:{name}:frequency_deviation"
    ]


def test_reference_columns_are_ignored(tmp_path):
    ee = station(load(tmp_path), "EE01")
    assert [r.sensor.value for r in ee] == [-19.0355, -18.0]
    assert ee[0].sensor.unit == "mHz"
    assert ee[0].sensor.timestamp == datetime(2019, 3, 25, 11, 9, 11, tzinfo=UTC)


def test_qi_1_and_2_are_skipped(tmp_path, grid_log):
    load(tmp_path)
    warnings = [r for r in grid_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {3}  # QI 2, QI 1, NaN


def test_100ms_recordings_are_their_own_dataset(tmp_path):
    readings = load(tmp_path, loader=GridFrequency100ms)
    fast = station(readings, "DE_KA01", "grid_frequency_100ms")
    assert len(fast) == len(readings)  # only the 100 ms zip, none of the 1 s files
    assert fast[0].sensor.timestamp == datetime(
        2019, 7, 8, 23, 59, 59, 100000, tzinfo=UTC
    )


def test_headerless_time_column_and_skipped_files(tmp_path):
    readings = load(tmp_path)
    assert [r.sensor.value for r in station(readings, "ZA02")] == [158.77]
    stations = {r.sensor.sensor_id.split(":")[1] for r in readings}
    # no sync01, no eclipse, no 100 ms recording (grid_frequency_100ms reads those)
    assert stations == {"EE01", "ZA02"}


def test_rows_outside_sampled_hours_are_not_read(tmp_path):
    long = (
        "Time;f50_LONG;QI_LONG\n"
        "2019-03-25 00:00:01;1.0;0\n"
        "2019-03-25 01:00:01;2.0;0\n"
        "2019-03-25 02:00:01;3.0;0\n"
    )
    injection = InjectionSettings(
        block_hours=1, sample_every_hours={"grid_frequency": 3}
    )
    readings = load(tmp_path, {"FrequencyData__LONG01__LONG01.csv": long}, injection)
    assert [r.sensor.value for r in station(readings, "LONG01")] == [1.0]


def test_100ms_recordings_are_sampled_by_their_own_key(tmp_path):
    # the fixture's one 100 ms reading is at 23:59: an every_hours that keeps that hour,
    # then one that skips it; grid_frequency's own key keeps everything
    def kept(every_hours):
        injection = InjectionSettings(
            block_hours=1,
            sample_every_hours={
                "grid_frequency": 1,
                "grid_frequency_100ms": every_hours,
            },
        )
        folder = tmp_path / str(every_hours)
        folder.mkdir()
        return len(load(folder, injection=injection, loader=GridFrequency100ms))

    hours = int(datetime(2019, 7, 8, 23, tzinfo=UTC).timestamp() // 3600)
    keep = next(e for e in range(2, 50) if hours % e < 1)
    drop = next(e for e in range(2, 50) if hours % e >= 1)
    assert (kept(keep), kept(drop)) == (1, 0)
