import cProfile
import pstats
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pytest
from message_factory import make_reading, make_unvalidated_reading

from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.utils.config import get_settings
from guardian_layer.utils.schema import SensorSummary

CONFIG = get_settings().preprocess


def median_time(fn, *args, repeats=80):
    timings = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn(*args)
        timings.append(time.perf_counter() - start)
    return statistics.median(timings)


def orders(*packet_orders):
    return [
        make_reading(second=i, packet_order=order)
        for i, order in enumerate(packet_orders)
    ]


def build_window(size: int):
    return [
        make_reading(
            sensor_id=f"s{i % 5}",
            cluster_id="water",
            second=i % 60,
            value=float(i % 10),
            lower_range=0.0,
            upper_range=100.0,
        )
        for i in range(size)
    ]


def test_duplicate_is_dropped_and_counted():
    first = make_reading(second=0)
    unique, counters = PreProcess().deduplicate(
        [first, make_reading(second=1), first.model_copy()]
    )
    assert len(unique) == 2
    assert unique[0] is first
    assert counters["water_level-01"]["duplicate_count"] == 1


def test_messages_without_metadata_dedup_on_exact_content():
    repeat = make_reading(second=0, with_metadata=False)
    changed = make_reading(second=0, value=99.0, with_metadata=False)
    unique, counters = PreProcess().deduplicate([repeat, repeat.model_copy(), changed])
    assert unique == [repeat, changed]
    assert counters["water_level-01"] == {
        "duplicate_count": 1.0,
        "out_of_order_count": 0.0,
        "replay_count": 0.0,
        "packet_gap_max": 0.0,
    }


def test_small_step_back_is_out_of_order():
    _, counters = PreProcess().deduplicate(orders(1, 2, 4, 3))
    assert counters["water_level-01"]["out_of_order_count"] == 1
    assert counters["water_level-01"]["replay_count"] == 0
    assert counters["water_level-01"]["packet_gap_max"] == 0


def test_far_step_back_is_a_replay_and_does_not_fake_a_gap():
    window = orders(10, 11, 12, 3)
    unique, counters = PreProcess().deduplicate(window)
    assert unique == window[:3]
    assert counters["water_level-01"]["replay_count"] == 1
    assert counters["water_level-01"]["out_of_order_count"] == 0
    assert counters["water_level-01"]["packet_gap_max"] == 0


def test_largest_missing_run_is_the_packet_gap():
    _, counters = PreProcess().deduplicate(orders(1, 2, 6, 7, 9))
    assert counters["water_level-01"]["packet_gap_max"] == 3


def test_every_sensor_gets_every_counter():
    window = [make_reading("a"), make_reading("b", with_metadata=False)]
    _, counters = PreProcess().deduplicate(window)
    assert set(counters) == {"a", "b"}
    for counts in counters.values():
        assert tuple(counts) == CONFIG.counter_keys


def test_series_is_sorted_by_time():
    late, early = make_reading(second=5, value=2.0), make_reading(second=1, value=1.0)
    times, values = PreProcess().sensor_series([late, early])
    assert times[1] - times[0] == 4
    assert values.tolist() == [1.0, 2.0]


def test_own_range_wins():
    own = make_reading(lower_range=0.0, upper_range=10.0)
    peer = make_reading("water_level-02", lower_range=0.0, upper_range=500.0)
    assert PreProcess().find_range([own], [own, peer]) == (0.0, 10.0)


def test_range_falls_back_to_the_cluster_then_to_none():
    no_range = make_reading(lower_range=None, upper_range=None)
    peer = make_reading("water_level-02", lower_range=0.0, upper_range=500.0)
    assert PreProcess().find_range([no_range], [no_range, peer]) == (0.0, 500.0)
    assert PreProcess().find_range([no_range], [no_range]) is None


def test_range_scaling_maps_the_range_to_zero_one():
    values = np.array([0.0, 50.0, 100.0, 150.0])
    assert PreProcess().normalize(values, (0.0, 100.0)).tolist() == [0.0, 0.5, 1.0, 1.5]


def test_without_range_values_are_z_scored():
    scaled = PreProcess().normalize(np.array([1.0, 2.0, 3.0]), None)
    assert scaled.mean() == pytest.approx(0.0)
    assert scaled.std() == pytest.approx(1.0)
    assert PreProcess().normalize(np.array([4.0, 4.0]), None).tolist() == [0.0, 0.0]


def test_resample_fills_gaps_on_a_one_second_grid():
    pre = PreProcess()
    assert np.allclose(
        pre.resample(np.array([0.0, 3.0]), np.array([0.0, 3.0])),
        np.array([0.0, 1.0, 2.0, 3.0]),
    )


def test_resample_grid_is_bounded_by_an_old_timestamp():
    resampled = PreProcess().resample(
        np.array([0.0, 31_536_000.0]), np.array([0.0, 1.0])
    )
    assert len(resampled) <= 21


def test_stuck_series_is_flat():
    times = np.arange(6, dtype=float)
    features = PreProcess().value_features(times, np.full(6, 0.4), has_range=True)
    assert features["flatline_ratio"] == 1.0
    assert features["rate_of_change_max_abs"] == 0.0
    assert features["has_range"] == 1.0


def test_jump_gives_a_high_rate_of_change():
    times = np.arange(6, dtype=float)
    values = np.array([0.4, 0.4, 0.4, 0.9, 0.4, 0.4])
    features = PreProcess().value_features(times, values, has_range=True)
    assert features["rate_of_change_max_abs"] == pytest.approx(0.5)
    assert features["rolling_variance_max"] > 0


def test_features_always_have_the_same_keys():
    empty = PreProcess().value_features(np.array([]), np.array([]), has_range=False)
    single = PreProcess().value_features(
        np.array([0.0]), np.array([0.5]), has_range=False
    )
    assert tuple(empty) == tuple(single) == CONFIG.value_keys
    assert single["sample_count"] == 1.0
    assert single["rate_of_change_mean_abs"] == 0.0


def test_constant_values_without_range_normalize_to_zero_despite_rounding():
    for value in (0.1, 0.7, 3.3, 12.34):
        for count in (3, 6):
            constant = np.full(count, value)
            assert PreProcess().normalize(constant, None).tolist() == [0.0] * count


def test_value_features_empty_window_branch():
    features = PreProcess().value_features(
        np.array([], dtype=float), np.array([], dtype=float), has_range=False
    )
    assert features["sample_count"] == 0.0
    assert all(value == 0.0 for key, value in features.items() if key != "sample_count")


def test_preprocess_value_features_non_moving_branch_is_zeroed():
    times = np.array([0.0, 1.0, 2.0, 3.0], dtype=float)
    values = np.array([0.5, 0.5, 0.5, 0.5], dtype=float)
    features = PreProcess().value_features(times, values, has_range=True)
    assert features["flatline_ratio"] == 1.0
    assert features["rate_of_change_max_abs"] == 0.0


def test_naive_timestamp_is_read_as_utc():
    expected = (
        np.datetime64("2026-10-02T00:00:00Z", "ns")
        .astype("datetime64[us]")
        .astype("int64")
        / 1e6
    )
    reading = make_reading()
    naive = reading.model_copy(
        update={"timestamp": reading.timestamp.replace(tzinfo=None)}
    )
    assert PreProcess().timestamp_seconds(naive) == pytest.approx(expected)


def test_cluster_is_the_cluster_id():
    reading = make_reading(cluster_id="water_level", with_metadata=False)
    assert PreProcess().cluster_key(reading) == "water_level"


def test_location_skips_messages_without_coordinates():
    messages = [make_reading(lat=None, lon=None), make_reading(lat=1.0, lon=2.0)]
    assert PreProcess().location(messages) == (1.0, 2.0)
    assert PreProcess().location([make_reading(with_metadata=False)]) == (None, None)


def test_location_skips_invalid_coordinates_until_valid_pair():
    bad = make_unvalidated_reading(sensor_id="bad", lat=95.0, lon=0.0)
    good = make_reading(sensor_id="good", lat=1.0, lon=2.0)
    assert PreProcess().location([bad, good]) == (1.0, 2.0)


def test_neighbors_are_the_five_nearest_in_the_cluster():
    pre = PreProcess()
    center = SensorSummary("center", "water", 36.79, 10.18, 0.5, 0.5)
    peers = [
        SensorSummary(f"p{i}", "water", 36.79 + i * 0.001, 10.18, 0.5, 0.5)
        for i in range(1, 8)
    ]
    assert [sensor.sensor_id for sensor in pre.neighbors(center, [center, *peers])] == [
        "p1",
        "p2",
        "p3",
        "p4",
        "p5",
    ]


def test_without_coordinates_the_whole_cluster_is_used():
    pre = PreProcess()
    lost = SensorSummary("lost", "water", None, None, 0.5, 0.5)
    peers = [
        SensorSummary(f"p{i}", "water", 36.79 + i, 10.18, 0.5, 0.5) for i in range(1, 8)
    ]
    assert len(pre.neighbors(lost, [lost, *peers])) == len(peers)


def test_spoofed_sensor_stands_out_from_its_neighbours():
    pre = PreProcess()
    normal = [
        SensorSummary(f"p{i}", "water", 36.79 + i * 0.001, 10.18, 0.5, 0.5)
        for i in range(3)
    ]
    spoofed = SensorSummary("spoofed", "water", 36.79, 10.18, 0.8, 0.9)
    features = pre.spatial_features([spoofed, *normal])
    assert features["spoofed"]["spatial_delta_mean"] == pytest.approx(0.3)
    assert features["spoofed"]["spatial_delta_last"] == pytest.approx(0.4)
    assert features["spoofed"]["neighbor_count"] == 3


def test_lonely_sensor_gets_zeros():
    features = PreProcess().spatial_features(
        [SensorSummary("alone", "water", None, None, 0.5, 0.5)]
    )
    assert features["alone"] == dict.fromkeys(CONFIG.spatial_keys, 0.0)


def test_preprocess_find_range_rejects_reversed_bounds():
    readings = [make_unvalidated_reading(lower_range=20.0, upper_range=10.0)]
    assert PreProcess().find_range(readings, []) is None


@pytest.mark.parametrize("size", [16, 64, 256])
def test_bench_preprocess_methods(size):
    pre = PreProcess()
    window = build_window(size)
    times = np.arange(size, dtype=float)
    values = np.linspace(0.0, 1.0, size)
    cluster = [
        make_reading(
            sensor_id=f"p{i}",
            cluster_id="water",
            second=i % 60,
            value=float(i % 7),
            lower_range=0.0,
            upper_range=100.0,
        )
        for i in range(size)
    ]

    timings = {
        "deduplicate": median_time(pre.deduplicate, window),
        "find_range": median_time(pre.find_range, window[:5], cluster),
        "normalize": median_time(pre.normalize, values, (0.0, 1.0)),
        "resample": median_time(pre.resample, times, values),
        "value_features": median_time(pre.value_features, times, values, True),
        "location": median_time(pre.location, window),
        "haversine_km": median_time(pre.haversine_km, 36.79, 10.18, 36.80, 10.19),
    }

    assert set(timings) >= {
        "deduplicate",
        "find_range",
        "normalize",
        "resample",
        "value_features",
        "location",
        "haversine_km",
    }
    assert all(value >= 0.0 for value in timings.values())


def test_shared_preprocess_instance_is_safe_under_parallel_calls():
    pre = PreProcess()
    window = [
        make_reading(
            sensor_id="s1", second=i, value=float(i), lower_range=0.0, upper_range=100.0
        )
        for i in range(12)
    ]

    def worker(_):
        unique, counters = pre.deduplicate(window)
        return len(unique), counters["s1"]["duplicate_count"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(worker, range(8)))

    assert results
    assert all(item[0] == len(window) for item in results)
    assert all(item[1] == 0.0 for item in results)


def test_cluster_neighbors_profile_runs_and_reports_hot_path():
    pre = PreProcess()
    members = [
        SensorSummary(f"s{i}", "water", 36.79 + i * 0.001, 10.18, 0.5, 0.5)
        for i in range(80)
    ]
    profiler = cProfile.Profile()
    profiler.enable()
    pre.cluster_neighbors(members)
    profiler.disable()
    stats = pstats.Stats(profiler)
    func_names = {key[2] for key in stats.stats}
    assert "cluster_neighbors" in func_names
    assert stats.total_tt >= 0.0
