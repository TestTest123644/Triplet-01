from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from guardian_layer.managers.buffer_manger import BufferManager
from guardian_layer.managers.cluster_manger import ClusterManager
from guardian_layer.utils.channels import FlushChannel
from guardian_layer.utils.schema import Ack, Buffer, Cluster, Component, Status


@pytest.fixture
def sensor_factory():
    """Create an independent Sensor factory for this test module."""
    from datetime import UTC, datetime

    from guardian_layer.utils.schema import Metadata, Sensor

    def make(value=1, sensor_id="sensor-1", cluster_id="temperature"):
        return Sensor(
            sensor_id=sensor_id,
            cluster_id=cluster_id,
            value=float(value),
            unit="C",
            timestamp=datetime.now(UTC),
            metadata=Metadata(packet_order=value, type=cluster_id),
        )

    return make


@pytest.fixture
def channel():
    return FlushChannel(maxsize=50)


@pytest.fixture
def manager(channel):
    return ClusterManager(channel)


def test_empty_and_sink(manager, channel):
    assert len(manager) == 0
    assert manager.clusters == {}
    assert manager._sink is channel


def test_validate_missing(manager):
    ack = manager.validate("unknown")
    assert not ack.ok
    assert ack.status is Status.FAILED
    assert ack.sender is Component.CLUSTER
    assert ack.receiver is Component.CONSUMER


def test_create_and_duplicate(manager, sensor_factory):
    sensor = sensor_factory()
    cluster = manager.create(sensor)
    assert isinstance(cluster, Cluster)
    assert cluster.id == "temperature"
    assert cluster.sensor_type == "temperature"
    assert isinstance(cluster.buffers["sensor-1"], BufferManager)
    assert manager.create(sensor) is None
    assert len(manager) == 1
    assert manager.validate("temperature").ok


def test_create_without_metadata(manager, sensor_factory):
    sensor = sensor_factory().model_copy(update={"metadata": None})
    cluster = manager.create(sensor)
    assert cluster.sensor_type == sensor.cluster_id


def test_shared_sink(manager, channel, sensor_factory):
    cluster = manager.create(sensor_factory())
    assert cluster.buffers["sensor-1"]._sink is channel


def test_add_to_missing_cluster(manager, sensor_factory):
    ack = manager.add(sensor_factory())
    assert not ack.ok
    assert ack.sender is Component.CLUSTER
    assert ack.receiver is Component.CONSUMER


def test_add_first_reading(manager, sensor_factory):
    sensor = sensor_factory(1)
    manager.create(sensor)
    ack = manager.add(sensor)
    assert ack.ok
    assert ack.sender is Component.CLUSTER
    assert ack.receiver is Component.CONSUMER
    cluster = manager.get("temperature")
    buffer = cluster.buffers["sensor-1"].get("sensor-1")
    assert isinstance(buffer, Buffer)
    assert buffer.readings == [sensor]
    assert cluster.last_update == buffer.last_update


def test_add_second_sensor_creates_buffer_manager(manager, channel, sensor_factory):
    manager.create(sensor_factory())
    sensor2 = sensor_factory(2, sensor_id="sensor-2")
    assert manager.add(sensor2).ok
    cluster = manager.get("temperature")
    assert len(cluster.buffers) == 2
    assert cluster.buffers["sensor-2"]._sink is channel
    buffer = cluster.buffers["sensor-2"].get("sensor-2")
    assert isinstance(buffer, Buffer)
    assert buffer.readings == [sensor2]


def test_add_multiple_existing_readings(manager, sensor_factory):
    manager.create(sensor_factory())
    for i in (1, 2, 3):
        assert manager.add(sensor_factory(i)).ok
    cluster = manager.get("temperature")
    buffer = cluster.buffers["sensor-1"].get("sensor-1")
    assert isinstance(buffer, Buffer)
    assert [sensor.value for sensor in buffer.readings] == [1.0, 2.0, 3.0]


def test_buffer_failure_status_is_propagated(manager, sensor_factory):
    sensor = sensor_factory()
    manager.create(sensor)
    with patch.object(BufferManager, "add", autospec=True) as mocked:
        mocked.return_value = Ack(
            "x", "failure", Status.FAILED, Component.BUFFER, Component.CLUSTER
        )
        ack = manager.add(sensor)
    assert not ack.ok
    assert ack.detail == "failure"
    assert ack.sender is Component.CLUSTER
    assert ack.receiver is Component.CONSUMER


def test_get_delete_len(manager, sensor_factory):
    assert isinstance(manager.get("missing"), Ack)
    assert not manager.delete("missing").ok
    manager.create(sensor_factory())
    manager.create(sensor_factory(cluster_id="humidity", sensor_id="sensor-2"))
    assert len(manager) == 2
    assert manager.delete("temperature").ok
    assert len(manager) == 1
    assert isinstance(manager.get("humidity"), Cluster)


def test_concurrent_create_single_cluster(manager, sensor_factory):
    sensor = sensor_factory()
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(manager.create, [sensor] * 48))
    assert sum(result is not None for result in results) == 1
    assert len(manager) == 1


def test_concurrent_add_existing_sensor_no_loss(manager, sensor_factory):
    manager.create(sensor_factory())

    def add(value):
        ack = manager.add(sensor_factory(value))
        assert ack.ok

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(add, range(100)))
    cluster = manager.get("temperature")
    buffer = cluster.buffers["sensor-1"].get("sensor-1")
    assert isinstance(buffer, Buffer)
    assert sorted(sensor.value for sensor in buffer.readings) == list(
        map(float, range(100))
    )


def test_concurrent_add_new_sensor_no_loss(manager, sensor_factory):
    manager.create(sensor_factory())

    def add(value):
        ack = manager.add(sensor_factory(value, sensor_id="new-sensor"))
        assert ack.ok

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(add, range(100)))
    cluster = manager.get("temperature")
    buffer = cluster.buffers["new-sensor"].get("new-sensor")
    assert isinstance(buffer, Buffer)
    assert sorted(sensor.value for sensor in buffer.readings) == list(
        map(float, range(100))
    )
