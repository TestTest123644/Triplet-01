import threading
import time
from _thread import LockType
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import patch

import pytest

from guardian_layer.utils.schema import Buffer, FlushedBatch


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


def make_buffer(sensor_factory, numbers=(), high_watermark=5, flush_size=2):
    return Buffer(
        id="buffer-1",
        sensor_id="sensor-1",
        readings=[sensor_factory(i) for i in numbers],
        last_update=datetime.now(UTC),
        high_watermark=high_watermark,
        flush_size=flush_size,
    )


def values(readings):
    return [int(r.value) for r in readings]


def test_defaults(sensor_factory):
    buffer = make_buffer(sensor_factory)
    assert (buffer.high_watermark, buffer.flush_size) == (5, 2)
    assert not buffer.is_full
    assert buffer.readings == []


def test_default_window_and_flush_size(sensor_factory):
    buffer = Buffer("b", "sensor-1", [], datetime.now(UTC))
    assert buffer.high_watermark == 200
    assert buffer.flush_size == 100


def test_add_without_flush(sensor_factory):
    buffer = make_buffer(sensor_factory, [1, 2])
    assert buffer.add_reading(sensor_factory(3)) is None
    assert values(buffer.readings) == [1, 2, 3]


def test_flush_trigger_oldest_order_and_new_reading(sensor_factory):
    buffer = make_buffer(sensor_factory, [1, 2, 3, 4, 5])
    batch = buffer.add_reading(sensor_factory(6))
    assert isinstance(batch, FlushedBatch)
    assert values(batch.readings) == [1, 2]
    assert isinstance(batch.readings, tuple)
    assert values(buffer.readings) == [3, 4, 5, 6]
    assert batch.sensor_id == buffer.sensor_id
    assert batch.id != buffer.id


def test_flush_at_default_threshold(sensor_factory):
    buffer = Buffer(
        "b", "sensor-1", [sensor_factory(i) for i in range(200)], datetime.now(UTC)
    )
    batch = buffer.add_reading(sensor_factory(200))
    assert batch is not None
    assert values(batch.readings) == list(range(100))
    assert values(buffer.readings) == list(range(100, 201))


def test_multiple_flushes_preserve_data(sensor_factory):
    buffer = make_buffer(sensor_factory)
    emitted = []
    for i in range(31):
        batch = buffer.add_reading(sensor_factory(i))
        if batch:
            emitted.extend(batch.readings)
        assert len(buffer.readings) <= buffer.high_watermark
    assert values([*emitted, *buffer.readings]) == list(range(31))


def test_timestamps_on_add_and_flush(sensor_factory):
    buffer = make_buffer(sensor_factory, [1, 2, 3, 4, 5])
    old = datetime(2020, 1, 1, tzinfo=UTC)
    buffer.last_flush = old
    buffer.last_update = old
    before = datetime.now(UTC)
    batch = buffer.add_reading(sensor_factory(6))
    after = datetime.now(UTC)
    assert batch is not None
    assert before <= buffer.last_flush <= after
    assert before <= buffer.last_update <= after
    assert batch.flushed_at == buffer.last_flush
    assert buffer.last_flush.utcoffset() == timedelta(0)


def test_last_flush_unchanged_without_flush(sensor_factory):
    buffer = make_buffer(sensor_factory, [1])
    old = datetime(2020, 1, 1, tzinfo=UTC)
    buffer.last_flush = old
    before = datetime.now(UTC)
    assert buffer.add_reading(sensor_factory(2)) is None
    assert buffer.last_flush == old
    assert before <= buffer.last_update <= datetime.now(UTC)


def test_restore_preserves_order(sensor_factory):
    buffer = make_buffer(sensor_factory, [3, 4])
    buffer.restore(tuple(sensor_factory(i) for i in (1, 2)))
    assert values(buffer.readings) == [1, 2, 3, 4]


def test_batch_build_failure_propagates(sensor_factory):
    buffer = make_buffer(sensor_factory, [1, 2, 3, 4, 5])
    with patch(
        "guardian_layer.utils.schema.FlushedBatch",
        side_effect=ValueError("batch failure"),
    ):
        with pytest.raises(
            RuntimeError, match="Flush failed for sensor sensor-1"
        ) as exc_info:
            buffer.add_reading(sensor_factory(6))
    assert isinstance(exc_info.value.__cause__, ValueError)
    assert values(buffer.readings) == [3, 4, 5, 6]
    assert not buffer._lock.locked()


def test_batch_constructed_outside_lock(sensor_factory):
    buffer = make_buffer(sensor_factory, [1, 2, 3, 4, 5])
    original = FlushedBatch
    states = []

    def build(*args, **kwargs):
        acquired = buffer._lock.acquire(blocking=False)
        states.append(acquired)
        if acquired:
            buffer._lock.release()
        return original(*args, **kwargs)

    with patch("guardian_layer.utils.schema.FlushedBatch", side_effect=build):
        batch = buffer.add_reading(sensor_factory(6))
    assert batch is not None
    assert states == [True]


def test_stress_multiple_writers_no_loss_or_duplicate(sensor_factory):
    buffer = make_buffer(sensor_factory, high_watermark=20, flush_size=10)
    workers, per_worker = 8, 200
    barrier = threading.Barrier(workers)
    batches = []
    lengths = []
    result_lock = threading.Lock()

    def write(tid):
        local_batches, local_lengths = [], []
        barrier.wait(timeout=10)
        for index in range(per_worker):
            value = tid * per_worker + index
            batch = buffer.add_reading(sensor_factory(value))
            if batch is not None:
                local_batches.append(batch)
            with buffer._lock:
                local_lengths.append(len(buffer.readings))
        with result_lock:
            batches.extend(local_batches)
            lengths.extend(local_lengths)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(write, i) for i in range(workers)]
        for future in futures:
            future.result(timeout=30)

    received = [r for batch in batches for r in batch.readings] + list(buffer.readings)
    actual = values(received)
    expected = list(range(workers * per_worker))
    assert sorted(actual) == expected
    assert len(set(actual)) == len(expected)
    assert max(lengths) <= buffer.high_watermark


def test_lock_hold_time_measured(sensor_factory, capsys):
    buffer = make_buffer(sensor_factory, high_watermark=20, flush_size=10)

    class MeasuredLock:
        def __init__(self):
            self.lock = threading.Lock()
            self.durations_ns = []

        def __enter__(self):
            self.lock.acquire()
            self.start = time.perf_counter_ns()
            return self

        def __exit__(self, *_):
            self.durations_ns.append(time.perf_counter_ns() - self.start)
            self.lock.release()

        def locked(self):
            return self.lock.locked()

    instrumented = MeasuredLock()
    buffer._lock = cast(LockType, instrumented)
    for index in range(1000):
        buffer.add_reading(sensor_factory(index))
    assert len(instrumented.durations_ns) == 1000
    average_us = sum(instrumented.durations_ns) / len(instrumented.durations_ns) / 1000
    maximum_us = max(instrumented.durations_ns) / 1000
    print(f"Lock hold: avg={average_us:.3f} µs, max={maximum_us:.3f} µs")
    assert 0 <= average_us <= maximum_us
    assert not instrumented.locked()
