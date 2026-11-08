import queue
from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from guardian_layer.managers.buffer_manger import BufferManager
from guardian_layer.managers.cluster_manger import ClusterManager
from guardian_layer.utils.schema import Metadata, Sensor


class FullSink:
    def publish(self, batch):
        raise queue.Full


class BrokenSink:
    def publish(self, batch):
        raise RuntimeError("boom")


class FakeSink:
    def __init__(self):
        self.batches = []

    def publish(self, batch):
        self.batches.append(batch)


@pytest.fixture
def fake_sink():
    return FakeSink()


@pytest.fixture
def full_sink():
    return FullSink()


@pytest.fixture
def broken_sink():
    return BrokenSink()


@pytest.fixture
def make_sensor() -> Callable[..., Sensor]:
    def _make(sensor_id="s1", value=1.0, cluster_id="c1", metadata=True):
        return Sensor(
            sensor_id=sensor_id,
            cluster_id=cluster_id,
            value=value,
            unit="m",
            timestamp=datetime.now(UTC),
            metadata=Metadata(packet_order=1, type="water_level") if metadata else None,
        )

    return _make


@pytest.fixture
def cluster_manager(fake_sink) -> ClusterManager:
    return ClusterManager(fake_sink)


@pytest.fixture
def manager(fake_sink) -> BufferManager:
    return BufferManager(fake_sink)
