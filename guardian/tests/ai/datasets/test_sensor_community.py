import logging
from datetime import UTC, datetime

import pytest

from guardian_layer.ai.datasets.sensor_community import SensorCommunity
from guardian_layer.utils.dataset_config import DatasetSettings

FILES = {
    "2026-10-09_bme280_sensor_100017.csv": (
        "sensor_id;sensor_type;location;lat;lon;timestamp;pressure;altitude;"
        "pressure_sealevel;temperature;humidity\n"
        "100017;BME280;93323;53.58;9.86;2026-10-09T00:06:25;100912.0;;;6.90;99.0\n"
        "100017;BME280;93323;53.58;9.86;2026-10-09T00:01:25;100911.9;;;6.84;\n"
    ),
    "2026-10-09_dht22_sensor_100027.csv": (
        "sensor_id;sensor_type;location;lat;lon;timestamp;temperature;humidity\n"
        "100027;DHT22;93332;-37.756;145.006;2026-10-09T00:00:13;23.30;41.76\n"
    ),
    "2026-10-09_radiation_si22g_sensor_100418.csv": (
        "sensor_id;sensor_type;location;lat;lon;timestamp;counts_per_minute;"
        "hv_pulses;tube;counts;sample_time_ms\n"
        "100418;Radiation_Si22G;93726;-33.39;-70.52;2026-10-09T00:01:51;74;95;;190;152485\n"
    ),
}


@pytest.fixture
def community_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.ai.datasets.sensor_community")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(
        logging.DEBUG, logger="guardian_layer.ai.datasets.sensor_community"
    )
    yield caplog
    module_logger.removeHandler(caplog.handler)


def load(tmp_path):
    folder = tmp_path / "sensor_community"
    folder.mkdir()
    for name, text in FILES.items():
        (folder / name).write_bytes(text.encode())
    return list(SensorCommunity(DatasetSettings(root=str(tmp_path))).load())


def test_type_carries_the_model(tmp_path):
    readings = load(tmp_path)
    clusters = {
        r.sensor.cluster_id for r in readings if "temperature" in r.sensor.cluster_id
    }
    assert clusters == {
        "sensor_community:bme280_temperature",
        "sensor_community:dht22_temperature",
    }
    # the radiation counter is not a flood reading
    assert not [
        r for r in readings if r.sensor.sensor_id.startswith("sensor_community:100418")
    ]


def test_unlisted_columns_are_skipped(tmp_path):
    kinds = {r.sensor.metadata.type for r in load(tmp_path) if r.sensor.metadata}
    assert not [
        k for k in kinds if k.endswith(("altitude", "hv_pulses", "counts", "tube"))
    ]


def test_lat_lon_from_row_and_time_order(tmp_path):
    bme = [
        r
        for r in load(tmp_path)
        if r.sensor.sensor_id == "sensor_community:100017:bme280_temperature"
    ]
    assert [r.sensor.value for r in bme] == [6.84, 6.90]  # sorted by time
    assert bme[0].sensor.timestamp == datetime(2026, 10, 9, 0, 1, 25, tzinfo=UTC)
    assert bme[0].sensor.metadata is not None
    assert (bme[0].sensor.metadata.lat, bme[0].sensor.metadata.lon) == (53.58, 9.86)


def test_empty_value_is_skipped(tmp_path, community_log):
    load(tmp_path)
    warnings = [r for r in community_log.records if r.levelno == logging.WARNING]
    assert {r.skipped for r in warnings} == {1}  # the empty BME280 humidity
