import threading

import pytest

from guardian_layer.managers.cluster_manger import ClusterManager
from guardian_layer.utils.schema import Cluster, Status


def run_threads(targets):
    threads = [threading.Thread(target=t) for t in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


# ---------- validate / create / get / delete / len ----------


def test_validate_unknown(cluster_manager: ClusterManager):
    assert cluster_manager.validate("nope").status is Status.FAILED


def test_validate_existing(cluster_manager: ClusterManager, make_sensor):
    cluster_manager.create(make_sensor())
    assert cluster_manager.validate("c1").ok


def test_create_new_cluster(cluster_manager: ClusterManager, make_sensor):
    cluster = cluster_manager.create(make_sensor())
    assert isinstance(cluster, Cluster)
    assert cluster.id == "c1"
    assert cluster.sensor_type == "water_level"
    assert "s1" in cluster.buffers
    assert len(cluster_manager) == 1


def test_create_without_metadata_uses_cluster_id(
    cluster_manager: ClusterManager, make_sensor
):
    cluster = cluster_manager.create(make_sensor(metadata=False))
    if cluster is not None:
        assert cluster.sensor_type == "c1"
    else:
        raise AssertionError("Cluster creation failed when it should have succeeded")


def test_create_existing_cluster_returns_none(
    cluster_manager: ClusterManager, make_sensor
):
    cluster_manager.create(make_sensor())
    assert cluster_manager.create(make_sensor("s2")) is None
    assert len(cluster_manager) == 1


def test_get_unknown(cluster_manager):
    assert cluster_manager.get("nope").status is Status.FAILED


def test_get_existing(cluster_manager: ClusterManager, make_sensor):
    cluster_manager.create(make_sensor())
    assert isinstance(cluster_manager.get("c1"), Cluster)


def test_delete_existing(cluster_manager: ClusterManager, make_sensor):
    cluster_manager.create(make_sensor())
    assert cluster_manager.delete("c1").ok
    assert len(cluster_manager) == 0


def test_delete_unknown(cluster_manager: ClusterManager):
    assert cluster_manager.delete("nope").status is Status.FAILED


def test_len_empty(cluster_manager: ClusterManager):
    assert len(cluster_manager) == 0


# ---------- add ----------


def test_add_unknown_cluster(cluster_manager: ClusterManager, make_sensor):
    assert cluster_manager.add(make_sensor()).status is Status.FAILED


def test_add_first_reading(cluster_manager: ClusterManager, make_sensor):
    cluster = cluster_manager.create(make_sensor())
    assert cluster_manager.add(make_sensor()).ok
    if cluster is None:
        raise AssertionError("Cluster creation failed when it should have succeeded")
    assert len(cluster.buffers["s1"].buffers["s1"]) == 1
    assert cluster.last_update is not None


def test_add_new_sensor_to_existing_cluster(
    cluster_manager: ClusterManager, make_sensor
):
    cluster = cluster_manager.create(make_sensor("s1"))
    if cluster is None:
        raise AssertionError("Cluster creation failed when it should have succeeded")
    assert cluster_manager.add(make_sensor("s2")).ok
    assert set(cluster.buffers) == {"s1", "s2"}


def test_add_failure_does_not_touch_last_update(
    cluster_manager, make_sensor, full_sink
):
    # hint: which Ack status does the cluster pass through when the buffer fails?
    cluster = cluster_manager.create(make_sensor())
    assert cluster.last_update is None


def test_delete_cluster_with_readings(
    cluster_manager: ClusterManager, make_sensor, fake_sink
):
    sensor = make_sensor()
    cluster_manager.create(sensor)
    cluster_manager.add(sensor)
    assert cluster_manager.delete("c1").ok
    print(len(fake_sink.batches))  # the buffered reading: flushed, or silently gone?


# ---------- multithreading ----------


@pytest.mark.timeout(10)
def test_create_concurrent_same_cluster(cluster_manager: ClusterManager, make_sensor):
    sensor = make_sensor()
    results = []
    run_threads([lambda: results.append(cluster_manager.create(sensor))] * 20)
    assert sum(r is not None for r in results) == 1
    assert len(cluster_manager) == 1


@pytest.mark.timeout(10)
def test_add_concurrent_first_readings_same_sensor(
    cluster_manager: ClusterManager, make_sensor
):
    cluster = cluster_manager.create(make_sensor("s0"))
    sensors = [make_sensor("new") for _ in range(40)]
    if cluster is None:
        raise AssertionError("Cluster creation failed when it should have succeeded")

    def worker(chunk):
        return lambda: [cluster_manager.add(s) for s in chunk]

    run_threads([worker(sensors[i::8]) for i in range(8)])
    assert set(cluster.buffers) == {"s0", "new"}  # one manager, not several
    assert len(cluster.buffers["new"].buffers["new"]) == 40  # nothing lost


@pytest.mark.timeout(10)
def test_delete_concurrent_same_cluster(cluster_manager: ClusterManager, make_sensor):
    # hint: compare which of create / add / delete takes the lock. May be intermittent.
    cluster_manager.create(make_sensor())
    errors = []

    def worker():
        try:
            cluster_manager.delete("c1")
        except Exception as exc:
            errors.append(exc)

    run_threads([worker] * 20)
    assert errors == []


# ---------- benchmarks (one per function) ----------


def test_bench_validate(benchmark, cluster_manager, make_sensor):
    cluster_manager.create(make_sensor())
    benchmark(cluster_manager.validate, "c1")


def test_bench_get(benchmark, cluster_manager, make_sensor):
    cluster_manager.create(make_sensor())
    benchmark(cluster_manager.get, "c1")


def test_bench_len(benchmark, cluster_manager, make_sensor):
    cluster_manager.create(make_sensor())
    benchmark(len, cluster_manager)


def test_bench_create(benchmark, fake_sink, make_sensor):
    sensor = make_sensor()
    benchmark.pedantic(
        lambda m: m.create(sensor),
        setup=lambda: ((ClusterManager(fake_sink),), {}),
        rounds=1000,
    )


def test_bench_delete(benchmark, fake_sink, make_sensor):
    sensor = make_sensor()

    def setup():
        m = ClusterManager(fake_sink)
        m.create(sensor)
        return (m,), {}

    benchmark.pedantic(lambda m: m.delete("c1"), setup=setup, rounds=1000)


def test_bench_add_existing_sensor(benchmark, fake_sink, make_sensor):
    sensor = make_sensor()

    def setup():
        m = ClusterManager(fake_sink)
        m.create(sensor)
        m.add(sensor)  # buffer already exists
        return (m,), {}

    benchmark.pedantic(lambda m: m.add(sensor), setup=setup, rounds=1000)


def test_bench_add_new_sensor(benchmark, fake_sink, make_sensor):
    first, second = make_sensor("s1"), make_sensor("s2")

    def setup():
        m = ClusterManager(fake_sink)
        m.create(first)
        return (m,), {}

    benchmark.pedantic(lambda m: m.add(second), setup=setup, rounds=1000)
