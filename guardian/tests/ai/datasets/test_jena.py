import io
import logging
import zipfile
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.jena import Jena
from guardian_layer.utils.dataset_config import DatasetSettings

HEADER = (
    '"Date Time","p (mbar)","T (degC)","Tpot (K)","rh (%)","wv (m/s)","wd (deg)",'
    '"SWDR (W/m²)","PAR (µmol/m²/s)","CO2 (ppm)"\n'
)


def zipped(stem, rows):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(f"{stem}.csv", (HEADER + rows).encode("latin-1"))
    return buffer.getvalue()


@pytest.fixture
def jena_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.jena")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.jena")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "jena_weather"
    folder.mkdir()
    first = "01.01.2023 00:10:00,  988.64,   16.64,  290.74, 52.40, 6.90, 207.10, 0.00"
    first += ", 1.5, 422\n"
    second = "01.01.2024 00:10:00,  979.65,    7.63,  282.43, 71.53, -9999.00, 209.00"
    second += ", 0.00, 2.5, 430\n"
    (folder / "mpi_roof_2024.zip").write_bytes(zipped("mpi_roof_2024", second))
    (folder / "mpi_roof_2023a.zip").write_bytes(zipped("mpi_roof_2023a", first))
    return list(Jena(DatasetSettings(root=str(tmp_path))).load())


def by_type(readings, kind):
    return [r for r in readings if r.sensor.metadata and r.sensor.metadata.type == kind]


def test_latin1_header_matches(tmp_path):
    readings = load(tmp_path)
    temperature = by_type(readings, "temperature")
    assert [r.sensor.value for r in temperature] == [16.64, 7.63]  # 2023a, 2024
    first = temperature[0].sensor
    assert first.sensor_id == "jena:mpi_roof:temperature"
    assert first.timestamp == datetime(2023, 1, 1, 0, 10, tzinfo=UTC)


def test_solar_par_and_co2_are_dropped(tmp_path):
    kinds = {r.sensor.metadata.type for r in load(tmp_path) if r.sensor.metadata}
    assert not kinds & {"shortwave_radiation", "par", "max_par", "co2"}


def test_derived_columns_are_skipped(tmp_path):
    kinds = {r.sensor.metadata.type for r in load(tmp_path) if r.sensor.metadata}
    assert not kinds & {"tpot", "potential_temperature"}


def test_minus_9999_is_skipped(tmp_path, jena_log):
    assert [r.sensor.value for r in by_type(load(tmp_path), "wind_speed")] == [6.9]
    warnings = [r for r in jena_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {1}
