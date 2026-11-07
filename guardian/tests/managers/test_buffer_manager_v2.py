import threading

import pytest

from guardian_layer.managers.buffer_manger import BufferManager
from guardian_layer.utils.schema import Ack, Buffer, Status

FILL = 200  # Buffer.high_watermark: the next add triggers a flush


def prefill(manager: BufferManager, make_sensor, sensor_id="s1", n=FILL):
    manager.create(sensor_id)
    manager.buffers[sensor_id].readings.extend(make_sensor(sensor_id) for _ in range(n))


# ---------- validate / create / get / delete ----------


def test_validate_unknown(manager: BufferManager):
    assert manager.validate("nope").status is Status.FAILED


def test_validate_existing(manager: BufferManager):
    manager.create("s1")
    assert manager.validate("s1").ok


def test_create_new(manager: BufferManager):
    assert manager.create("s1").ok
    assert "s1" in manager.buffers


def test_create_duplicate(manager: BufferManager):
    manager.create("s1")
    assert manager.create("s1").status is Status.FAILED


def test_get_unknown_id(manager: BufferManager):
    result = manager.get("does-not-exist")
    if isinstance(result, Ack):
        assert result.status is Status.FAILED
        assert "not found" in result.detail
    else:
        raise AssertionError("Expected a Buffer with FAILED status, got something else")


def test_get_existing(manager: BufferManager):
    manager.create("s1")
    assert isinstance(manager.get("s1"), Buffer)


def test_delete_existing(manager: BufferManager):
    manager.create("s1")
    assert manager.delete("s1").ok
    buffer = manager.get("s1")
    if isinstance(buffer, Ack):
        assert buffer.status is Status.FAILED
    else:
        raise AssertionError("Buffer was not deleted properly")


def test_delete_unknown(manager: BufferManager):
    assert manager.delete("nope").status is Status.FAILED


# ---------- add: the five paths ----------


def test_add_unknown_sensor(manager: BufferManager, make_sensor):
    assert manager.add(make_sensor("ghost")).status is Status.FAILED


def test_add_no_flush(manager: BufferManager, make_sensor, fake_sink):
    manager.create("s1")
    assert manager.add(make_sensor()).ok
    assert len(manager.buffers["s1"]) == 1
    assert fake_sink.batches == []


def test_add_triggers_flush(manager: BufferManager, make_sensor, fake_sink):
    prefill(manager, make_sensor)
    assert manager.add(make_sensor()).ok
    assert len(fake_sink.batches) == 1
    assert len(fake_sink.batches[0].readings) == 100
    assert len(manager.buffers["s1"]) == 101


def test_add_sink_full_restores_readings(make_sensor, full_sink):
    mgr = BufferManager(full_sink)
    prefill(mgr, make_sensor)
    sensor = make_sensor()
    ack = mgr.add(sensor)
    print(ack)
    assert ack.ok  # what does the caller learn here?
    assert len(mgr.buffers["s1"]) == 201  # nothing lost


def test_add_reading_raises(manager: BufferManager, make_sensor, monkeypatch):
    manager.create("s1")

    def boom(self, reading):
        raise RuntimeError("flush failed")

    monkeypatch.setattr(Buffer, "add_reading", boom)
    result = manager.add(make_sensor())
    print(result)
    assert len(manager.buffers["s1"]) == 0


# ---------- risky condition ----------


def test_add_unexpected_sink_error(make_sensor, broken_sink):
    mgr = BufferManager(broken_sink)
    prefill(mgr, make_sensor)
    mgr.add(make_sensor())
    print(
        f"Length of buffer: {len(mgr.buffers['s1'])}"
    )  # 100 readings were taken out; where did they go?
    assert len(mgr.buffers["s1"]) == 201  # nothing lost, restored


# ---------- multithreading ----------


@pytest.mark.timeout(10)
def test_create_concurrent_same_id(manager):
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(manager.create("s1")))
        for _ in range(20)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(r.ok for r in results) == 1


@pytest.mark.timeout(10)
def test_add_concurrent_same_buffer(manager: BufferManager, make_sensor):
    manager.create("s1")
    threads = [
        threading.Thread(target=lambda: [manager.add(make_sensor()) for _ in range(25)])
        for _ in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(manager.buffers["s1"]) == 200  # 8 x 25, no flush yet


# ---------- benchmarks (one per function) ----------


def test_bench_validate(benchmark, manager):
    manager.create("s1")
    benchmark(manager.validate, "s1")


def test_bench_get(benchmark, manager):
    manager.create("s1")
    benchmark(manager.get, "s1")


def test_bench_create(benchmark, fake_sink):
    benchmark.pedantic(
        lambda m: m.create("s1"),
        setup=lambda: ((BufferManager(fake_sink),), {}),
        rounds=1000,
    )


def test_bench_delete(benchmark, fake_sink):
    def setup():
        m = BufferManager(fake_sink)
        m.create("s1")
        return (m,), {}

    benchmark.pedantic(lambda m: m.delete("s1"), setup=setup, rounds=1000)


def test_bench_add_no_flush(benchmark, fake_sink, make_sensor):
    sensor = make_sensor()

    def setup():
        m = BufferManager(fake_sink)
        m.create("s1")
        return (m,), {}

    benchmark.pedantic(lambda m: m.add(sensor), setup=setup, rounds=1000)


def test_bench_add_with_flush(benchmark, fake_sink, make_sensor):
    sensor = make_sensor()

    def setup():
        m = BufferManager(fake_sink)
        prefill(m, make_sensor)
        return (m,), {}

    benchmark.pedantic(lambda m: m.add(sensor), setup=setup, rounds=300)


@pytest.mark.timeout(20)
def test_add_concurrent_across_flush(manager: BufferManager, make_sensor, fake_sink):
    manager.create("s1")
    sensors = [make_sensor() for _ in range(400)]  # 8 threads x 50
    chunks = [sensors[i::8] for i in range(8)]

    def worker(chunk):
        for s in chunk:
            manager.add(s)

    threads = [threading.Thread(target=worker, args=(c,)) for c in chunks]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    in_buffer = [id(r) for r in manager.buffers["s1"].readings]
    flushed = [id(r) for b in fake_sink.batches for r in b.readings]
    all_ids = in_buffer + flushed

    assert len(all_ids) == 400  # nothing lost
    assert len(set(all_ids)) == 400  # nothing duplicated
    assert set(all_ids) == {id(s) for s in sensors}


@pytest.mark.xfail(reason="QUESTION-01: create('') accepted, no id validation")
def test_create_empty_id_rejected(manager):
    assert manager.create("").status is Status.FAILED


def test_get_empty_id_when_absent(manager: BufferManager):
    buffer = manager.get("")
    if isinstance(buffer, Ack):
        assert buffer.status is Status.FAILED
    else:
        pytest.fail("Expected a Ack got buffer")


def test_empty_id_roundtrip(manager: BufferManager):
    result = manager.create("")
    assert result.status is Status.FAILED
