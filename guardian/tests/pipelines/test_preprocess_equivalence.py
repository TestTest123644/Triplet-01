"""PreProcess's spatial step and range lookup must give the same features at any speed.

The references below are the plain O(n^2) versions. A faster PreProcess must match them
exactly (same neighbours, same tie order, same range), so the model needs no retrain.
"""

import math
import sys
from pathlib import Path

import pytest
from message_factory import make_reading, make_unvalidated_reading

from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.utils.config import get_settings
from guardian_layer.utils.schema import Sensor, SensorSummary

# mocks/ isn't an installed package; put the repo root on sys.path to import it
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from mocks.telemetry_generator import generate  # noqa: E402

CONFIG = get_settings().preprocess


def reference_neighbors(
    sensor: SensorSummary, sensors: list[SensorSummary]
) -> list[SensorSummary]:
    """k nearest located same-cluster peers (ties: input order), else the cluster."""
    peers = [
        p
        for p in sensors
        if p.cluster == sensor.cluster and p.sensor_id != sensor.sensor_id
    ]
    if sensor.lat is None or sensor.lon is None:
        return peers
    haversine = PreProcess().haversine_km
    located = sorted(
        (haversine(sensor.lat, sensor.lon, p.lat, p.lon), index, p)
        for index, p in enumerate(peers)
        if p.lat is not None and p.lon is not None
    )
    if not located:
        return peers
    return [p for _, _, p in located[: CONFIG.neighbors]]


def reference_spatial(sensors: list[SensorSummary]) -> dict[str, dict[str, float]]:
    result = {}
    for sensor in sensors:
        peers = reference_neighbors(sensor, sensors)
        if not peers:
            result[sensor.sensor_id] = dict.fromkeys(CONFIG.spatial_keys, 0.0)
            continue
        result[sensor.sensor_id] = {
            "spatial_delta_mean": sensor.mean - sum(p.mean for p in peers) / len(peers),
            "spatial_delta_last": sensor.last - sum(p.last for p in peers) / len(peers),
            "neighbor_count": float(len(peers)),
        }
    return result


def reference_range(
    readings: list[Sensor], cluster_readings: list[Sensor]
) -> tuple[float, float] | None:
    """The sensor's own range, else its cluster's first valid one (arrival order)."""
    for reading in (*readings, *cluster_readings):
        if reading.metadata is None:
            continue
        low, high = reading.metadata.lower_range, reading.metadata.upper_range
        if low is None or high is None:
            continue
        # as find_range since #75: a range with an inf or NaN bound is skipped
        if math.isfinite(low) and math.isfinite(high) and high > low:
            return low, high
    return None


def summaries_of(window: list[Sensor]) -> list[SensorSummary]:
    """One summary per sensor, from its first reading (values do not matter here)."""
    preprocessor, first = PreProcess(), {}
    for reading in window:
        first.setdefault(reading.sensor_id, reading)
    return [
        SensorSummary(
            sensor_id,
            reading.cluster_id,
            *preprocessor.location([reading]),
            reading.value / 100,
            reading.value / 50,
        )
        for sensor_id, reading in first.items()
    ]


def generated_window(seed: int, sensors: int) -> list[Sensor]:
    return [
        Sensor.model_validate(m)
        for m in generate(seed=seed, sensors=sensors, duration=20)[0]
    ]


def tied_summary(sensor_id: str, lat: float | None, lon: float | None, mean: float):
    return SensorSummary(sensor_id, "water", lat, lon, mean, mean)


TIES = [  # equal distances: the earlier peer in the input wins the last slot
    tied_summary("center", 36.0, 10.0, 0.5),
    *(tied_summary(f"ring-{i}", 36.0, 10.01, i / 10) for i in range(4)),
    *(tied_summary(f"far-{i}", 36.0, 10.02, 0.9) for i in range(3)),
    tied_summary("no-location", None, None, 0.1),
]


@pytest.mark.parametrize(("seed", "sensors"), [(0, 60), (1, 60), (2, 200)])
def test_spatial_features_match_the_reference_on_generated_windows(seed, sensors):
    summaries = summaries_of(generated_window(seed, sensors))
    expected = reference_spatial(summaries)
    actual = PreProcess().spatial_features(summaries)
    assert actual.keys() == expected.keys()
    for sensor_id, features in expected.items():
        assert actual[sensor_id] == pytest.approx(features, rel=1e-12, abs=1e-12)


def test_spatial_features_keep_the_tie_order():
    expected = reference_spatial(TIES)
    actual = PreProcess().spatial_features(TIES)
    for sensor_id, features in expected.items():
        assert actual[sensor_id] == pytest.approx(features, rel=1e-12, abs=1e-12)


@pytest.mark.parametrize(("seed", "sensors"), [(0, 60), (2, 200)])
def test_find_range_matches_the_reference_on_generated_windows(seed, sensors):
    window = generated_window(seed, sensors)
    by_sensor: dict[str, list[Sensor]] = {}
    by_cluster: dict[str, list[Sensor]] = {}
    for reading in window:
        by_sensor.setdefault(reading.sensor_id, []).append(reading)
        by_cluster.setdefault(reading.cluster_id, []).append(reading)
    preprocessor = PreProcess()
    for readings in by_sensor.values():
        cluster = by_cluster[readings[0].cluster_id]
        assert preprocessor.find_range(readings, cluster) == reference_range(
            readings, cluster
        )


def test_find_range_prefers_own_then_first_cluster_range():
    own = [make_reading("a", lower_range=None, upper_range=None)]
    cluster = [
        *own,
        make_unvalidated_reading("b", lower_range=5.0, upper_range=1.0),
        make_reading("c", lower_range=0.0, upper_range=50.0),
        make_reading("d", lower_range=0.0, upper_range=80.0),
    ]
    assert PreProcess().find_range(own, cluster) == reference_range(own, cluster)
    assert PreProcess().find_range(own, cluster) == (0.0, 50.0)


def test_own_range_wins_over_an_earlier_cluster_range():
    own = [make_reading("d", lower_range=0.0, upper_range=80.0)]
    cluster = [make_reading("c", lower_range=0.0, upper_range=50.0), *own]
    assert PreProcess().find_range(own, cluster) == reference_range(own, cluster)
    assert PreProcess().find_range(own, cluster) == (0.0, 80.0)


def test_a_non_finite_cluster_range_is_skipped_like_the_reference():
    own = [make_reading("a", lower_range=None, upper_range=None)]
    cluster = [
        *own,
        make_unvalidated_reading("b", lower_range=0.0, upper_range=math.inf),
        make_reading("c", lower_range=0.0, upper_range=50.0),
    ]
    assert PreProcess().find_range(own, cluster) == reference_range(own, cluster)
    assert PreProcess().find_range(own, cluster) == (0.0, 50.0)


def test_find_range_stops_at_the_first_valid_range():
    class Exploding(list):
        def __iter__(self):
            yield self[0]
            raise AssertionError("read the whole cluster")

    own = [make_reading("a", lower_range=None, upper_range=None)]
    cluster = Exploding([make_reading("b", lower_range=0.0, upper_range=50.0), *own])
    assert PreProcess().find_range(own, cluster) == (0.0, 50.0)


def assert_matches_reference(sensors):
    expected = reference_spatial(sensors)
    actual = PreProcess().spatial_features(sensors)
    assert actual == expected  # exact: the model was trained on these floats


def test_single_sensor_cluster_gets_zeros():
    lone = [tied_summary("only", 36.0, 10.0, 0.5)]
    assert PreProcess().spatial_features(lone)["only"] == dict.fromkeys(
        CONFIG.spatial_keys, 0.0
    )


def test_only_self_located_falls_back_to_cluster():
    assert_matches_reference(
        [tied_summary("a", 36.0, 10.0, 0.1), tied_summary("b", None, None, 0.9)]
    )


def test_colocated_sensors_are_neighbours():
    assert_matches_reference(
        [tied_summary(f"s{i}", 36.0, 10.0, i / 10) for i in range(7)]
    )


def test_cluster_neighbors_gives_indices_in_tie_order():
    picked = PreProcess().cluster_neighbors(TIES)
    assert picked[0].tolist() == [1, 2, 3, 4, 5]  # 4 ring, then the first far
    assert picked[8].tolist() == list(range(8))  # no location: whole cluster


@pytest.mark.parametrize(("seed", "sensors"), [(0, 60), (1, 200)])
def test_spatial_features_equal_the_reference_bit_for_bit(seed, sensors):
    assert_matches_reference(summaries_of(generated_window(seed, sensors)))


def test_chunking_does_not_change_neighbours():
    summaries = summaries_of(generated_window(0, 60))
    small = PreProcess(settings=CONFIG.model_copy(update={"spatial_chunk_rows": 3}))
    assert small.spatial_features(summaries) == PreProcess().spatial_features(summaries)
