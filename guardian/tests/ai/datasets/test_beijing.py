import io
import logging
import zipfile
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.beijing import Beijing
from guardian_layer.utils.dataset_config import DatasetSettings

HEADER = (
    '"No","year","month","day","hour","PM2.5","PM10","SO2","NO2","CO","O3","TEMP",'
    '"PRES","DEWP","RAIN","wd","WSPM","station"\n'
)
INNER = "PRSA_Data_20130301-20170228"


def station_csv(name, pm25):
    return (
        HEADER
        + f'1,2013,3,1,0,{pm25},4,4,7,300,77,-0.7,1023,-18.8,0,"NNW",4.4,"{name}"\n'
    )


def zipped(members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


@pytest.fixture
def beijing_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.beijing")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.ai.datasets.beijing")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "beijing_air_quality"
    folder.mkdir()
    inner = zipped(
        {
            f"{INNER}/PRSA_Data_Aotizhongxin_20130301-20170228.csv": station_csv(
                "Aotizhongxin", "4"
            ),
            f"{INNER}/PRSA_Data_Changping_20130301-20170228.csv": station_csv(
                "Changping", "NA"
            ),
        }
    )
    outer = zipped(
        {
            "PRSA2017_Data_20130301-20170228.zip": inner,
            "data.csv": "Date,Open,Close\n2020-01-01,1,2\n",
            "test.csv": "Date,Open\n",
        }
    )
    (folder / "data.zip").write_bytes(outer)
    return list(Beijing(DatasetSettings(root=str(tmp_path))).load())


def test_nested_zip_stations_load(tmp_path):
    readings = load(tmp_path)
    stations = {r.sensor.sensor_id.split(":")[1] for r in readings}
    assert stations == {"Aotizhongxin", "Changping"}
    pm25 = [r for r in readings if r.sensor.sensor_id == "beijing:Aotizhongxin:pm25"]
    assert pm25[0].sensor.value == 4.0
    assert pm25[0].sensor.unit == "µg/m³"
    assert pm25[0].sensor.timestamp == datetime(2013, 3, 1, tzinfo=UTC)


def test_na_is_skipped(tmp_path, beijing_log):
    readings = load(tmp_path)
    assert not [r for r in readings if r.sensor.sensor_id == "beijing:Changping:pm25"]
    warnings = [r for r in beijing_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {1}


def test_stock_csv_and_wind_direction_are_not_read(tmp_path):
    kinds = {r.sensor.metadata.type for r in load(tmp_path) if r.sensor.metadata}
    assert len(kinds) == 11
    assert "wd" not in kinds
