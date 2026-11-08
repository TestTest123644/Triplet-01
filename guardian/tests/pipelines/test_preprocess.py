import logging
import math
import sys
import tracemalloc
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pytest
from message_factory import make_message_dict, make_reading, make_unvalidated_reading

from guardian_layer.ai.datasets.windows import process_window
from guardian_layer.pipelines.decompose import Decompose
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.utils.config import get_settings
from guardian_layer.utils.schema import Sensor, SensorSummary

# mocks/ isn't an installed package; put the repo root on sys.path to import it
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from mocks.telemetry_generator import generate  # noqa: E402

CONFIG = get_settings().preprocess


def orders(*packet_orders: int) -> list[Sensor]:
    """One sensor's readings arriving with these packet_orders, in this order."""
    return [
        make_reading(second=i, packet_order=order)
        for i, order in enumerate(packet_orders)
    ]


def summary(sensor_id, mean=0.5, last=0.5, lat=36.79, lon=10.18, cluster="water"):
    return SensorSummary(sensor_id, cluster, lat, lon, mean, last)


# --- deduplication ------------------------------------------------------------


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
    assert unique == window[:3]  # the replay's stale value stays out of the series
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


# --- value features -----------------------------------------------------------


def test_series_is_sorted_by_time():
    late, early = make_reading(second=5, value=2.0), make_reading(second=1, value=1.0)
    times, values = PreProcess().sensor_series([late, early])
    assert times[1] - times[0] == 4
    assert values.tolist() == [1.0, 2.0]


def test_readings_sharing_a_timestamp_keep_their_packet_order():
    readings = [
        make_reading(value=value, second=3, packet_order=order)
        for value, order in ((5.0, 2), (1.0, 3), (9.0, 1))
    ]
    times, values = PreProcess().sensor_series(readings)
    assert values.tolist() == [9.0, 5.0, 1.0]
    assert len(set(times.tolist())) == 1  # timestamps untouched: nothing is spread


def test_tied_readings_without_metadata_keep_their_arrival_order():
    readings = [
        make_reading(value=value, second=3, with_metadata=False)
        for value in (5.0, 1.0, 9.0)
    ]
    assert PreProcess().sensor_series(readings)[1].tolist() == [5.0, 1.0, 9.0]


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
    resampled = PreProcess().resample(np.array([0.0, 3.0]), np.array([0.0, 3.0]))
    assert resampled.tolist() == [0.0, 1.0, 2.0, 3.0]


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


# --- readings -----------------------------------------------------------------


def test_naive_timestamp_is_read_as_utc():
    expected = datetime(2026, 10, 2, tzinfo=UTC).timestamp()
    zulu = make_reading()
    naive = zulu.model_copy(update={"timestamp": datetime(2026, 10, 2)})
    assert naive.timestamp.tzinfo is None
    assert PreProcess().timestamp_seconds(naive) == expected
    assert PreProcess().timestamp_seconds(zulu) == expected


def test_cluster_is_the_cluster_id():
    reading = make_reading(cluster_id="water_level", with_metadata=False)
    assert PreProcess().cluster_key(reading) == "water_level"


# --- spatial ------------------------------------------------------------------


def test_location_skips_messages_without_coordinates():
    messages = [make_reading(lat=None, lon=None), make_reading(lat=1.0, lon=2.0)]
    assert PreProcess().location(messages) == (1.0, 2.0)
    assert PreProcess().location([make_reading(with_metadata=False)]) == (None, None)


def test_neighbors_are_the_five_nearest_in_the_cluster():
    center = summary("center")
    peers = [summary(f"p{i}", lat=36.79 + i * 0.001) for i in range(1, 8)]
    other_cluster = summary("far", lat=36.79, cluster="rain")
    chosen = PreProcess().neighbors(center, [center, *peers, other_cluster])
    assert [peer.sensor_id for peer in chosen] == ["p1", "p2", "p3", "p4", "p5"]


def test_without_coordinates_the_whole_cluster_is_used():
    lost = summary("lost", lat=None, lon=None)
    peers = [summary(f"p{i}", lat=36.79 + i) for i in range(1, 8)]
    assert len(PreProcess().neighbors(lost, [lost, *peers])) == 7


def test_spoofed_sensor_stands_out_from_its_neighbours():
    normal = [summary(f"p{i}", mean=0.5, last=0.5) for i in range(3)]
    spoofed = summary("spoofed", mean=0.8, last=0.9)
    features = PreProcess().spatial_features([spoofed, *normal])
    assert features["spoofed"]["spatial_delta_mean"] == pytest.approx(0.3)
    assert features["spoofed"]["spatial_delta_last"] == pytest.approx(0.4)
    assert features["spoofed"]["neighbor_count"] == 3


def test_lonely_sensor_gets_zeros():
    features = PreProcess().spatial_features([summary("alone")])
    assert features["alone"] == dict.fromkeys(CONFIG.spatial_keys, 0.0)


# --- the stages chained per window --------------------------------------------
# PreProcess.process is gone (the orchestrator will chain the stages), so these tests
# chain the stages themselves and pin what each one guarantees to the next.


def window_features(
    window: list[Sensor], method: str | None = None
) -> tuple[dict[str, dict[str, float]], list[SensorSummary]]:
    """Value, counter and decoupling features per sensor, plus the spatial inputs."""
    preprocessor = PreProcess(method)
    unique, counters = preprocessor.deduplicate(preprocessor.finite(window))
    features, summaries = {}, []
    for sensor_id in dict.fromkeys(reading.sensor_id for reading in unique):
        readings = [r for r in unique if r.sensor_id == sensor_id]
        cluster = readings[0].cluster_id
        peers = [r for r in unique if r.cluster_id == cluster]
        value_range = preprocessor.find_range(readings, peers)
        times, raw = preprocessor.sensor_series(readings)
        values = preprocessor.normalize(raw, value_range)
        features[sensor_id] = {
            **preprocessor.value_features(times, values, value_range is not None),
            **counters[sensor_id],
            **Decompose().caller(
                preprocessor.method, preprocessor.resample(times, values)
            ),
        }
        lat, lon = preprocessor.location(readings)
        summaries.append(
            SensorSummary(
                sensor_id, cluster, lat, lon, float(values.mean()), float(values[-1])
            )
        )
    return features, summaries


def test_every_sensor_of_a_generated_window_gets_every_feature():
    telemetry, _ = generate(seed=1, sensors=8, duration=20)
    readings = [Sensor.model_validate(message) for message in telemetry]
    features, summaries = window_features(readings)
    spatial = PreProcess().spatial_features(summaries)
    assert set(features) == set(spatial) == {reading.sensor_id for reading in readings}
    for sensor_id, sensor_features in features.items():
        row = {**sensor_features, **spatial[sensor_id]}
        assert tuple(row) == PreProcess().feature_keys(CONFIG.default_method)
        assert all(math.isfinite(value) for value in row.values())


@pytest.mark.parametrize("method", ["none", "fft", "haar"])
def test_method_selects_the_decoupling_keys(method):
    window = [make_reading(second=s, value=10.0 + s) for s in range(8)]
    features, _ = window_features(window, method)
    keys = (*features["water_level-01"], *CONFIG.spatial_keys)
    assert keys == PreProcess().feature_keys(method)


def test_unknown_method_is_rejected():
    with pytest.raises(ValueError):
        PreProcess("magic")


def test_empty_method_is_rejected_not_replaced_by_the_default():
    with pytest.raises(ValueError):
        PreProcess("")
    with pytest.raises(KeyError):
        PreProcess().feature_keys("")


def test_duplicates_are_counted_not_double_used():
    first = make_reading(second=0, value=10.0)
    window = [first, make_reading(second=1, value=20.0), first.model_copy()]
    features = window_features(window)[0]["water_level-01"]
    assert features["duplicate_count"] == 1
    assert features["sample_count"] == 2


def test_message_without_metadata_still_gets_features():
    window = [
        make_reading("bare", value=v, second=s, with_metadata=False)
        for s, v in enumerate([1.0, 2.0, 3.0, 4.0])
    ]
    features, summaries = window_features(window)
    assert features["bare"]["has_range"] == 0.0  # z-scored
    assert features["bare"]["replay_count"] == 0.0
    spatial = PreProcess().spatial_features(summaries)
    assert spatial["bare"]["neighbor_count"] == 0.0


def test_spoofed_sensor_has_the_largest_spatial_delta():
    window = []
    for i in range(4):
        value = 80.0 if i == 0 else 50.0
        for second in range(5):
            window.append(
                make_reading(
                    f"water_level-0{i}",
                    value=value,
                    second=second,
                    lat=36.79 + i / 1000,
                )
            )
    _, summaries = window_features(window)
    spatial = PreProcess().spatial_features(summaries)
    deltas = {sid: abs(f["spatial_delta_mean"]) for sid, f in spatial.items()}
    assert max(deltas, key=deltas.__getitem__) == "water_level-00"


def test_year_old_message_without_metadata_gives_finite_features_in_bounded_memory():
    window = [make_reading(second=s, value=10.0 + s) for s in range(8)]
    old = Sensor.model_validate(
        {
            **make_message_dict(value=99.0, with_metadata=False),
            "timestamp": "2025-10-02T00:00:00Z",
        }
    )
    window.insert(4, old)
    tracemalloc.start()
    features, summaries = window_features(window)
    spatial = PreProcess().spatial_features(summaries)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    # memory, not wall time, so the check is deterministic: an unbounded 1 s grid
    # over a year is 31.5 M points (~250 MB per array)
    assert peak < 10_000_000
    row = {**features["water_level-01"], **spatial["water_level-01"]}
    assert all(math.isfinite(value) for value in row.values())


def test_nan_reading_leaves_every_feature_of_every_sensor_finite():
    window = [
        make_reading(sensor_id, value=10.0 + s, second=s)
        for sensor_id in ("water_level-01", "water_level-02")
        for s in range(6)
    ]
    window[2] = window[2].model_copy(update={"value": float("nan")})
    features, summaries = window_features(window)
    spatial = PreProcess().spatial_features(summaries)
    assert set(features) == set(spatial) == {"water_level-01", "water_level-02"}
    for sensor_id, sensor_features in features.items():
        row = {**sensor_features, **spatial[sensor_id]}
        assert all(math.isfinite(value) for value in row.values())


def test_empty_window_gives_no_features():
    assert PreProcess().deduplicate(PreProcess().finite([])) == ([], {})
    assert PreProcess().spatial_features([]) == {}


def test_nan_reading_is_dropped_before_the_means():
    readings = [make_reading(second=s, value=10.0 + s) for s in range(3)]
    bad = [
        make_unvalidated_reading(second=3, value=float("nan")),
        make_reading(second=4),
    ]
    bad[1].value = float("inf")
    assert PreProcess().finite([*readings, *bad]) == readings
    features, _ = window_features([*readings, *bad])
    assert list(features) == ["water_level-01"]
    assert features["water_level-01"]["sample_count"] == 3
    assert features["water_level-01"]["mean"] == pytest.approx(0.11)  # 10-12 of 100
    assert all(math.isfinite(value) for value in features["water_level-01"].values())


def test_reading_without_metadata_gets_every_feature_key():
    window = [
        make_reading("s1", value=1.0 + s, second=s, with_metadata=False)
        for s in range(6)
    ]
    features, summaries = window_features(window)
    row = {**features["s1"], **PreProcess().spatial_features(summaries)["s1"]}
    assert tuple(row) == PreProcess().feature_keys("none")
    assert len(row) == 15


# --- #13 remediation ------------------------------------------------------------


def test_feature_keys_default_to_the_instance_method():
    assert PreProcess("ssa").feature_keys() == PreProcess().feature_keys("ssa")


@pytest.mark.parametrize(
    ("point", "expected"),
    [((-36.79, -169.82), math.pi * CONFIG.earth_radius_km), ((36.79, 10.18), 0.0)],
)
def test_haversine_survives_rounding_at_the_extremes(point, expected):
    # the antipode and the same point: h can round to just past 1 or below 0
    assert PreProcess().haversine_km(36.79, 10.18, *point) == pytest.approx(expected)


def test_haversine_clamps_h_rounded_past_one(monkeypatch):
    # rounding on some inputs/platforms pushes h just past 1; simulate it through cos
    monkeypatch.setattr(math, "cos", lambda _: 1.0 + 1e-12)
    distance = PreProcess().haversine_km(0.0, 0.0, 0.0, 180.0)
    assert distance == pytest.approx(math.pi * CONFIG.earth_radius_km)


@pytest.fixture
def preprocess_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.pipelines.preprocess")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.pipelines.preprocess")
    yield caplog
    module_logger.removeHandler(caplog.handler)


@pytest.mark.parametrize(
    ("low", "high"),
    [(0.0, math.inf), (-math.inf, 100.0), (math.nan, 100.0), (0.0, math.nan)],
)
def test_non_finite_own_range_falls_back_to_the_cluster(preprocess_log, low, high):
    own = [make_unvalidated_reading(lower_range=low, upper_range=high)]
    peer = [make_reading("water_level-02", lower_range=0.0, upper_range=50.0)]
    assert PreProcess().find_range(own, peer) == (0.0, 50.0)
    assert "non-finite range" in preprocess_log.text


def test_only_non_finite_ranges_give_none():
    bad = [make_unvalidated_reading(lower_range=0.0, upper_range=math.inf)]
    assert PreProcess().find_range(bad, bad) is None


def test_dropped_non_finite_readings_are_logged(preprocess_log):
    PreProcess().finite(
        [make_unvalidated_reading(value=math.nan), make_reading(second=1)]
    )
    assert "non-finite" in preprocess_log.text


def test_whole_cluster_fallback_is_logged(preprocess_log):
    lonely = summary("a", lat=None, lon=None)
    PreProcess().neighbors(lonely, [lonely, summary("b")])
    assert "whole cluster" in preprocess_log.text


def test_spatial_features_log_the_whole_cluster_fallback(preprocess_log):
    lonely = summary("a", lat=None, lon=None)
    PreProcess().spatial_features([lonely, summary("b")])
    assert "whole cluster" in preprocess_log.text


def dead_reading() -> Sensor:
    # pydantic rejects value=None, so build it the way a bypassing caller would
    alive = make_reading()
    object.__setattr__(alive, "value", None)
    return alive


@pytest.mark.parametrize("step", ["finite", "deduplicate"])
def test_a_reading_without_value_raises(step):
    with pytest.raises(ValueError, match="water_level-01 sent no value"):
        getattr(PreProcess(), step)([make_reading(second=1), dead_reading()])


@pytest.mark.parametrize(
    ("packet_orders", "kept", "rejected"),
    [
        ((10, 11, 99999, 12), (10, 11, 12), 1),
        ((99999, 10, 11, 12), (10, 11, 12), 1),
        ((10, 11, 99999, 3), (10, 11), 2),  # 99999 corrupt, 3 a replay (tolerance 2)
        ((10, 99999), (10,), 1),
    ],
)
def test_absurd_packet_order_is_rejected_not_the_reference(
    packet_orders, kept, rejected
):
    unique, counters = PreProcess().deduplicate(orders(*packet_orders))
    assert tuple(r.metadata.packet_order for r in unique if r.metadata) == kept
    assert counters["water_level-01"]["replay_count"] == rejected
    assert counters["water_level-01"]["packet_gap_max"] == 0


def test_single_reading_is_never_corrupt():
    unique, _ = PreProcess().deduplicate(orders(99999))
    assert len(unique) == 1


def test_corrupt_check_ignores_readings_without_metadata():
    window = [*orders(10, 11), make_reading(second=5, with_metadata=False)]
    unique, _ = PreProcess().deduplicate(window)
    assert len(unique) == 3


def test_rejected_packets_are_logged(preprocess_log):
    PreProcess().deduplicate(orders(10, 11, 99999, 12))
    assert "bad packet_order" in preprocess_log.text


def test_a_repeated_corrupt_packet_cannot_outvote_the_good_ones():
    # duplicates must not weigh in the median, or a replayed bogus packet wins
    unique, counters = PreProcess().deduplicate(orders(10, 11, 12, *[99999] * 4))
    assert tuple(r.metadata.packet_order for r in unique if r.metadata) == (10, 11, 12)
    assert counters["water_level-01"]["replay_count"] == 1


@pytest.mark.parametrize(
    ("lat", "lon"),
    [(math.nan, 10.0), (36.0, math.inf), (90.5, 10.0), (-91.0, 10.0), (36.0, 180.5)],
)
def test_invalid_coordinates_are_skipped_and_logged(preprocess_log, lat, lon):
    bad = make_unvalidated_reading(lat=lat, lon=lon)
    good = make_reading(second=1, lat=1.0, lon=2.0)
    assert PreProcess().location([bad, good]) == (1.0, 2.0)
    assert PreProcess().location([bad]) == (None, None)
    assert "invalid coordinates" in preprocess_log.text


def test_dropped_non_finite_readings_name_their_sensors(preprocess_log):
    window = [
        make_unvalidated_reading("a", value=math.nan),
        make_unvalidated_reading("b", value=math.inf),
    ]
    PreProcess().finite([*window, make_reading("c")])
    records = {id(r): r for r in preprocess_log.records}.values()
    (record,) = [r for r in records if r.getMessage() == "Dropped non-finite readings"]
    assert record.sensor_ids == ["a", "b"]


# --- short-window features (#96) -------------------------------------------------


def test_robust_z_uses_median_and_mad():
    z = PreProcess().robust_z(np.array([1.0, 2.0, 3.0, 4.0, 100.0]))
    assert z[2] == 0.0
    assert z[3] == pytest.approx(1 / 1.4826)


def test_robust_z_falls_back_to_z_score_then_zeros():
    preprocessor = PreProcess()
    assert preprocessor.robust_z(np.array([5.0, 5.0, 5.0, 9.0])).std() == (
        pytest.approx(1.0)
    )
    assert not preprocessor.robust_z(np.full(4, 7.0)).any()


def test_a_spike_gives_a_large_step_and_level():
    values = np.random.default_rng(0).normal(0.0, 0.1, 20)
    values[10] = 5.0
    features = PreProcess().shape_features(values)
    assert features["max_abs_step"] > 10
    assert features["max_abs_level"] > 10


def test_native_grid_keeps_a_100ms_spacing():
    times = np.arange(200) * 0.1
    assert len(PreProcess().native_grid(times, np.sin(times))) == 200


def test_native_grid_is_bounded_by_an_old_timestamp():
    times = np.array([0.0, 1e9, 1e9 + 1, 1e9 + 2])
    assert len(PreProcess().native_grid(times, np.arange(4.0))) <= 21


def test_native_haar_energies_are_shares():
    times = np.arange(64) * 0.1
    features = PreProcess().native_decoupling(
        Decompose(), "haar", times, np.sin(times * 7)
    )
    shares = [features[f"n_haar_d{level}_energy"] for level in (1, 2, 3)]
    assert sum(shares) == pytest.approx(1.0)


def test_short_features_of_a_single_reading_are_zeros():
    features = PreProcess().short_features(
        Decompose(), np.array([0.0]), np.array([3.0])
    )
    assert tuple(features) == PreProcess().short_keys()
    assert all(value == 0.0 for value in features.values())


def test_short_features_of_a_flat_window_only_show_the_flatline():
    times = np.arange(20, dtype=np.float64)
    features = PreProcess().short_features(Decompose(), times, np.full(20, 3.0))
    assert tuple(features) == PreProcess().short_keys()
    assert features.pop("rz_flatline_ratio") == 1.0
    assert all(value == 0.0 for value in features.values())


NO_CLEANING = CONFIG.model_copy(update={"clean_method": "none"})
KALMAN = CONFIG.model_copy(update={"clean_method": "kalman"})
NOISY_TIMES = np.arange(40, dtype=np.float64)
NOISY = 20.0 + np.sin(NOISY_TIMES / 4) + 0.3 * np.random.default_rng(5).normal(size=40)


def test_short_features_read_the_cleaned_series():
    cleaning = PreProcess(settings=KALMAN)
    expected = PreProcess(settings=NO_CLEANING).short_features(
        Decompose(), NOISY_TIMES, cleaning.clean(NOISY)
    )
    assert cleaning.short_features(Decompose(), NOISY_TIMES, NOISY) == expected
    raw = PreProcess(settings=NO_CLEANING).short_features(
        Decompose(), NOISY_TIMES, NOISY
    )
    assert expected != raw  # the cleaning changes them


def test_without_cleaning_clean_returns_an_equal_copy():
    out = PreProcess(settings=NO_CLEANING).clean(NOISY)
    assert np.array_equal(out, NOISY) and out is not NOISY


def test_section_4_features_stay_raw_under_cleaning():
    window = [
        make_reading(value=float(value), second=second)
        for second, value in enumerate(NOISY[:20])
    ]
    raw, cleaned = (
        process_window(PreProcess(settings=s), Decompose(), "none", window)
        for s in (NO_CLEANING, KALMAN)
    )
    sensor = next(iter(raw))
    for key in PreProcess().feature_keys("none"):
        assert cleaned[sensor][key] == raw[sensor][key], key
    assert any(
        cleaned[sensor][key] != raw[sensor][key] for key in PreProcess().short_keys()
    )


def test_known_keys_cover_every_method_and_the_short_keys():
    preprocessor = PreProcess()
    assert (
        set(preprocessor.feature_keys("ssa")) | set(preprocessor.short_keys())
        <= preprocessor.known_keys()
    )


def test_native_grid_is_bounded_by_a_burst_of_close_timestamps():
    # a device that flushes a batch microseconds apart must not give a huge grid
    times = 1.7e9 + np.array([0.0, 1e-6, 2e-6, 3e-6, 4e-6, 20.0])
    grid = PreProcess().native_grid(times, np.arange(6.0))
    assert len(grid) <= CONFIG.native_grid_per_reading * 6


def test_known_keys_include_the_long_keys():
    assert set(get_settings().preprocess.long_keys) <= PreProcess().known_keys()
