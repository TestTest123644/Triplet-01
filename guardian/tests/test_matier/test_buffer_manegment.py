import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from guardian_layer.managers.buffer_manger import BufferManager
from guardian_layer.utils.channels import FlushChannel
from guardian_layer.utils.schema import Ack, Buffer, Component, FlushedBatch, Status


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
    return FlushChannel(maxsize=20)


@pytest.fixture
def manager(channel):
    return BufferManager(channel)


def test_initial_state_and_sink(manager, channel):
    assert manager.buffers == {}
    assert manager._sink is channel


def test_validate_missing_and_existing(manager):
    missing = manager.validate("s1")
    assert not missing.ok
    assert missing.status is Status.FAILED
    assert missing.sender is Component.BUFFER
    assert missing.receiver is Component.CLUSTER
    assert manager.create("s1").ok
    assert manager.validate("s1").ok


def test_create_empty_and_duplicate(manager):
    assert manager.create("s1").ok
    buffer = manager.get("s1")
    assert isinstance(buffer, Buffer)
    assert buffer.readings == []
    assert not manager.create("s1").ok
    assert len(manager.buffers) == 1


def test_add_requires_create(manager, sensor_factory):
    sensor = sensor_factory()
    assert not manager.add(sensor).ok
    assert sensor.sensor_id not in manager.buffers


def test_add_without_flush(manager, channel, sensor_factory):
    sensor = sensor_factory()
    manager.create(sensor.sensor_id)
    assert manager.add(sensor).ok
    buffer = manager.get(sensor.sensor_id)
    assert isinstance(buffer, Buffer)
    assert buffer.readings == [sensor]
    assert channel.qsize() == 0


def test_publish_real_batch(manager, channel, sensor_factory):
    manager.create("sensor-1")
    buffer = manager.get("sensor-1")
    assert isinstance(buffer, Buffer)
    buffer.high_watermark = 2
    buffer.flush_size = 1
    for i in (1, 2, 3):
        assert manager.add(sensor_factory(i)).ok
    assert channel.qsize() == 1
    batch = channel.get(timeout=0.1)
    assert isinstance(batch, FlushedBatch)
    assert [r.value for r in batch.readings] == [1.0]
    assert [r.value for r in buffer.readings] == [2.0, 3.0]
    channel.task_done()


def test_channel_full_restores_data(sensor_factory):
    channel = FlushChannel(maxsize=1)
    manager = BufferManager(channel)
    manager.create("sensor-1")
    buffer = manager.get("sensor-1")
    assert isinstance(buffer, Buffer)
    buffer.high_watermark = 1
    buffer.flush_size = 1
    manager.add(sensor_factory(1))
    manager.add(sensor_factory(2))
    assert channel.qsize() == 1
    assert manager.add(sensor_factory(3)).ok
    assert [r.value for r in buffer.readings] == [2.0, 3.0]
    first = channel.get(timeout=0.1)
    assert [r.value for r in first.readings] == [1.0]
    channel.task_done()


def test_exception_from_buffer_is_logged(manager, sensor_factory):
    manager.create("sensor-1")
    with (
        patch.object(
            Buffer, "add_reading", autospec=True, side_effect=RuntimeError("failure")
        ),
        patch("guardian_layer.managers.buffer_manger.logger") as logger,
    ):
        assert manager.add(sensor_factory()).ok
        logger.exception.assert_called_once()


def test_flush_logs_once_per_publication(manager, sensor_factory):
    manager.create("sensor-1")
    buffer = manager.get("sensor-1")
    assert isinstance(buffer, Buffer)
    buffer.high_watermark = 2
    buffer.flush_size = 1
    with patch("guardian_layer.managers.buffer_manger.logger") as logger:
        for i in range(5):
            manager.add(sensor_factory(i))
    calls = [c for c in logger.info.call_args_list if c.args == ("Buffer flushed",)]
    assert len(calls) == 3
    for call in calls:
        assert call.kwargs["buffer_id"] == "sensor-1"
        assert call.kwargs["sensor_id"] == "sensor-1"
        assert call.kwargs["batch_id"]
        assert call.kwargs["flushed"] == 1
        assert call.kwargs["duration_us"] >= 0
        assert "+00:00" in call.kwargs["flushed_at"]


def test_channel_full_logs_error(sensor_factory):
    channel = FlushChannel(maxsize=1)
    manager = BufferManager(channel)
    manager.create("sensor-1")
    buffer = manager.get("sensor-1")
    assert isinstance(buffer, Buffer)
    buffer.high_watermark = 1
    buffer.flush_size = 1
    manager.add(sensor_factory(1))
    manager.add(sensor_factory(2))
    with patch("guardian_layer.managers.buffer_manger.logger") as logger:
        manager.add(sensor_factory(3))
    logger.error.assert_called_once()
    call = logger.error.call_args
    assert call.args == ("Flush channel full, readings restored",)
    assert call.kwargs["restored"] == 1


def test_get_delete_and_ack(manager: BufferManager, sensor_factory):
    assert isinstance(manager.get("missing"), Ack)
    assert not manager.delete("missing").ok
    manager.create("sensor-1")
    assert isinstance(manager.get("sensor-1"), Buffer)
    ack = manager._added(sensor=sensor_factory(), detail="test")
    assert ack.ok
    assert ack.sender is Component.BUFFER
    assert ack.receiver is Component.CLUSTER
    assert manager.delete("sensor-1").ok
    assert "sensor-1" not in manager.buffers


def test_concurrent_create_only_one_success(manager):
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(manager.create, ["sensor-1"] * 48))
    assert sum(ack.ok for ack in results) == 1
    assert len(manager.buffers) == 1


def test_worker_can_consume_without_source_lock(manager, channel, sensor_factory):
    manager.create("sensor-1")
    buffer = manager.get("sensor-1")
    assert isinstance(buffer, Buffer)
    buffer.high_watermark = 1
    buffer.flush_size = 1
    manager.add(sensor_factory(1))
    manager.add(sensor_factory(2))
    batch = channel.get(timeout=0.1)
    started = threading.Event()
    finish = threading.Event()
    errors = []

    def consume():
        try:
            started.set()
            assert finish.wait(timeout=5)
            assert len(batch.readings) == 1
        except BaseException as error:
            errors.append(error)
        finally:
            channel.task_done()

    worker = threading.Thread(target=consume)
    worker.start()
    try:
        assert started.wait(timeout=5)
        acquired = buffer._lock.acquire(timeout=1)
        assert acquired
        if acquired:
            buffer._lock.release()
    finally:
        finish.set()
        worker.join(timeout=5)
    assert not errors
    assert not worker.is_alive()
