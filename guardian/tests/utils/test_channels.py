import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest

from guardian_layer.utils.channels import FlushChannel
from guardian_layer.utils.schema import (
    FLUSH_QUEUE_MAXSIZE,
    FlushedBatch,
    Metadata,
    Sensor,
)


def make_batch(number: int) -> FlushedBatch:
    reading = Sensor(
        sensor_id="sensor-1",
        cluster_id="temperature",
        value=float(number),
        unit="C",
        timestamp=datetime.now(UTC),
        metadata=Metadata(packet_order=number, type="temperature"),
    )
    return FlushedBatch(
        id=f"batch-{number}",
        sensor_id="sensor-1",
        readings=(reading,),
        flushed_at=datetime.now(UTC),
    )


def test_default_settings():
    channel = FlushChannel()
    assert channel._queue.maxsize == FLUSH_QUEUE_MAXSIZE
    assert channel._publish_timeout == 0.0
    assert channel.qsize() == 0


@pytest.mark.parametrize("maxsize", [0, -1, -100])
def test_rejects_unbounded_or_negative_capacity(maxsize):
    with pytest.raises(ValueError, match="maxsize must be > 0"):
        FlushChannel(maxsize=maxsize)


def test_channel_supports_sink_source_protocols():
    # Runtime structural checks via Protocol members (no runtime_checkable needed).
    channel = FlushChannel()
    assert callable(channel.publish)
    assert callable(channel.get)
    assert callable(channel.task_done)


def test_publish_get_fifo_and_identity():
    channel = FlushChannel(maxsize=3)
    batches = [make_batch(i) for i in range(3)]
    for batch in batches:
        channel.publish(batch)
    assert channel.qsize() == 3
    for batch in batches:
        assert channel.get(timeout=0.1) is batch
        channel.task_done()
    assert channel.qsize() == 0
    channel.join()


def test_full_channel_raises_immediately_with_nonblocking_default():
    channel = FlushChannel(maxsize=1)
    channel.publish(make_batch(1))
    with pytest.raises(queue.Full):
        channel.publish(make_batch(2))
    assert channel.qsize() == 1
    assert channel.get(timeout=0.1).id == "batch-1"
    channel.task_done()


def test_positive_publish_timeout_raises_when_full():
    channel = FlushChannel(maxsize=1, publish_timeout=0.01)
    channel.publish(make_batch(1))
    with pytest.raises(queue.Full):
        channel.publish(make_batch(2))
    assert channel.qsize() == 1
    channel.get(timeout=0.1)
    channel.task_done()


def test_positive_publish_timeout_allows_wait_for_capacity():
    channel = FlushChannel(maxsize=1, publish_timeout=1.0)
    first, second = make_batch(1), make_batch(2)
    channel.publish(first)
    started = threading.Event()
    published = threading.Event()
    errors = []

    def producer():
        started.set()
        try:
            channel.publish(second)
            published.set()
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=producer)
    thread.start()
    try:
        assert started.wait(timeout=2)
        assert channel.get(timeout=0.5) is first
        channel.task_done()
        assert published.wait(timeout=2)
        assert channel.get(timeout=0.5) is second
        channel.task_done()
    finally:
        thread.join(timeout=2)
    assert not thread.is_alive()
    assert not errors
    channel.join()


def test_get_raises_empty_when_no_batches_available():
    channel = FlushChannel(maxsize=1)
    with pytest.raises(queue.Empty):
        channel.get(timeout=0.01)


def test_task_done_and_join_wait_for_processing():
    channel = FlushChannel(maxsize=2)
    channel.publish(make_batch(1))
    batch = channel.get(timeout=0.1)
    assert batch.id == "batch-1"
    joined = threading.Event()

    thread = threading.Thread(target=lambda: (channel.join(), joined.set()))
    thread.start()
    try:
        # join remains blocked while one task is unfinished.
        assert not joined.wait(timeout=0.02)
        channel.task_done()
        assert joined.wait(timeout=2)
    finally:
        if not joined.is_set():
            channel.task_done()
        thread.join(timeout=2)
    assert not thread.is_alive()


def test_task_done_without_get_or_publish_raises_valueerror():
    channel = FlushChannel(maxsize=1)
    with pytest.raises(ValueError, match="task_done"):
        channel.task_done()


def test_multiple_producers_and_consumer_no_loss():
    channel = FlushChannel(maxsize=500)
    total = 200

    def publish(number):
        channel.publish(make_batch(number))

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(publish, range(total)))

    result = []
    for _ in range(total):
        batch = channel.get(timeout=0.1)
        result.append(batch.id)
        channel.task_done()

    channel.join()
    assert len(result) == total
    assert len(set(result)) == total
    assert set(result) == {f"batch-{i}" for i in range(total)}
    assert channel.qsize() == 0
