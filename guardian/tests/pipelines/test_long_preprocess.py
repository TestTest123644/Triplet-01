from datetime import UTC, datetime, timedelta

import numpy as np
import pytest
from message_factory import make_message_dict

from guardian_layer.pipelines.decompose import Decompose
from guardian_layer.pipelines.denoise import kalman
from guardian_layer.pipelines.long_preprocess import (
    LongPreProcess,
    process_long_window,
)
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.utils.config import (
    KalmanSettings,
    LongWindowSettings,
    get_settings,
)
from guardian_layer.utils.schema import CleanContext, Sensor

LONG_KEYS = get_settings().preprocess.long_keys
NEIGHBOUR_KEYS = {"lw_neighbour_slope_delta", "lw_neighbor_count"}
SIX_HOURS = np.arange(0.0, 6 * 3600, 60.0)  # one reading a minute


@pytest.fixture
def long_preprocess() -> LongPreProcess:
    return LongPreProcess(PreProcess(), Decompose(), LongWindowSettings())


def noise(size: int, seed: int = 0) -> np.ndarray:
    return np.random.default_rng(seed).normal(0.0, 1.0, size)


def test_grid_averages_each_bucket(long_preprocess):
    # the first bucket is centred on the first reading: [-150, 150), [150, 450)
    times = np.array([0.0, 60.0, 120.0, 300.0, 360.0, 420.0])
    _, grid, filled = long_preprocess.time_grid(times, np.arange(6.0))
    assert filled == 2
    assert grid.tolist() == [1.0, 4.0]


def test_a_gap_is_interpolated_but_not_counted(long_preprocess):
    times = np.array([0.0, 60.0, 960.0, 1020.0])  # buckets 0 and 3 filled
    _, grid, filled = long_preprocess.time_grid(times, np.array([0.0, 0.0, 3.0, 3.0]))
    assert filled == 2
    assert grid.tolist() == [0.0, 1.0, 2.0, 3.0]


def test_hourly_sensor_is_not_interpolated(long_preprocess):
    times = np.arange(6) * 3600.0
    _, grid, filled = long_preprocess.time_grid(times, np.arange(6.0))
    assert filled == 6
    assert len(grid) == 6


def test_a_jittered_hourly_sensor_keeps_one_bucket_per_reading(long_preprocess):
    # live clocks jitter: the first reading late, another early, by under half a step
    times = np.arange(6) * 3600.0 + np.array([10.0, -30.0, 0.0, 45.0, -5.0, 20.0])
    _, grid, filled = long_preprocess.time_grid(times, np.arange(6.0))
    assert filled == 6
    np.testing.assert_allclose(grid, np.arange(6.0))


def test_an_old_timestamp_does_not_stretch_the_grid(long_preprocess):
    ten_years = 10 * 365 * 86400.0
    times = np.concatenate(([-ten_years], np.arange(0.0, 86400.0, 300.0)))
    _, grid, _ = long_preprocess.time_grid(times, np.ones(len(times)))
    assert len(grid) <= 86400 / 300 + 1


def test_a_future_timestamp_does_not_erase_the_window(long_preprocess):
    ten_years = 10 * 365 * 86400.0
    day = np.arange(0.0, 86400.0, 300.0)
    times = np.concatenate((day, [ten_years]))
    _, grid, filled = long_preprocess.time_grid(
        times, np.arange(len(times), dtype=float)
    )
    assert filled == len(day)
    assert len(grid) <= 86400 / 300 + 1


def test_ramp_has_a_high_trend_scatter_ratio(long_preprocess):
    ramp = np.linspace(0.0, 10.0, len(SIX_HOURS)) + 0.1 * noise(len(SIX_HOURS))
    ramp_row, _ = long_preprocess.long_features(SIX_HOURS, ramp)
    noise_row, _ = long_preprocess.long_features(SIX_HOURS, noise(len(SIX_HOURS)))
    assert ramp_row["lw_trend_scatter_ratio"] > 5 * noise_row["lw_trend_scatter_ratio"]


def test_frozen_tail_gives_a_long_flat_run(long_preprocess):
    half = len(SIX_HOURS) // 2
    # 0.1 is not exact in binary: bucket means of 0.1 differ in the last bit
    values = np.concatenate((noise(half), np.full(len(SIX_HOURS) - half, 0.1)))
    row, _ = long_preprocess.long_features(SIX_HOURS, values)
    # half the readings frozen; the bucket holding the change mixes both halves
    assert row["lw_flat_run_share"] >= 36 / 73


def test_flat_series_has_no_nan(long_preprocess):
    row, slope = long_preprocess.long_features(SIX_HOURS, np.full(len(SIX_HOURS), 3.0))
    assert slope == 0.0
    assert row["lw_trend_scatter_ratio"] == 0.0
    assert row["lw_flat_run_share"] == 1.0
    assert all(np.isfinite(list(row.values())))


def test_a_day_stuck_at_an_inexact_value_is_flat(long_preprocess):
    # epoch timestamps with a fraction, uneven bucket counts, and 0.1 (not exact in
    # binary): a stuck sensor's grid must still be exactly constant
    times = 1.7e9 + 0.37 + np.arange(1440) * 60.0
    times[::7] += 17.0  # still sorted: 17 s < the 60 s step
    row, slope = long_preprocess.long_features(times, np.full(len(times), 0.1))
    assert slope == 0.0
    assert row["lw_trend_scatter_ratio"] == 0.0
    assert row["lw_flat_run_share"] == 1.0
    assert row["lw_rz_variance"] == 0.0
    assert row["lw_max_abs_level"] == 0.0


def test_a_perfect_ramp_is_capped(long_preprocess):
    ramp = np.linspace(0.0, 10.0, len(SIX_HOURS))
    row, slope = long_preprocess.long_features(SIX_HOURS, ramp)
    assert slope > 0
    assert 0 < row["lw_trend_scatter_ratio"] <= PreProcess().settings.ratio_cap


def test_one_reading_gives_zeros(long_preprocess):
    row, slope = long_preprocess.long_features(np.array([0.0]), np.array([5.0]))
    assert row["lw_grid_points"] == 1.0
    assert slope == 0.0
    assert all(value == 0.0 for key, value in row.items() if key != "lw_grid_points")


def test_readings_at_one_timestamp_give_zeros(long_preprocess):
    row, _ = long_preprocess.long_features(np.zeros(5), np.arange(5.0))
    assert row["lw_grid_points"] == 1.0
    assert all(np.isfinite(list(row.values())))


def test_a_change_at_the_window_start_changes_the_features(long_preprocess):
    # the 20 s path keeps only the end of a window; the long features see all of it
    values = noise(len(SIX_HOURS))
    shifted = values.copy()
    shifted[:60] += 5.0  # the first hour only
    first, _ = long_preprocess.long_features(SIX_HOURS, values)
    second, _ = long_preprocess.long_features(SIX_HOURS, shifted)
    assert first != second


def test_long_features_writes_the_long_keys_minus_neighbours(long_preprocess):
    row, _ = long_preprocess.long_features(SIX_HOURS, noise(len(SIX_HOURS)))
    assert tuple(row) == tuple(key for key in LONG_KEYS if key not in NEIGHBOUR_KEYS)


# --- several sensors: neighbour slopes and process_long_window ------------------

START = datetime(2026, 10, 2, tzinfo=UTC)


def reading_at(
    sensor_id: str, value: float, seconds: float, order: int, cluster: str = "temp"
) -> Sensor:
    """One reading ``seconds`` after 2026-10-02T00:00Z, sent ``order``-th."""
    message = make_message_dict(
        sensor_id, value, packet_order=order, cluster_id=cluster
    )
    message["timestamp"] = (START + timedelta(seconds=seconds)).isoformat()
    return Sensor.model_validate(message)


def series(sensor_id: str, values: np.ndarray, cluster: str = "temp") -> list[Sensor]:
    """One reading a minute, as SIX_HOURS."""
    return [
        reading_at(sensor_id, float(value), float(seconds), order + 1, cluster)
        for order, (seconds, value) in enumerate(zip(SIX_HOURS, values, strict=True))
    ]


def ramp(seed: int) -> np.ndarray:
    return np.linspace(0.0, 10.0, len(SIX_HOURS)) + 0.1 * noise(len(SIX_HOURS), seed)


def rows_of(long_preprocess: LongPreProcess, window: list[Sensor]):
    preprocessor = long_preprocess.preprocessor
    return process_long_window(
        long_preprocess, preprocessor, long_preprocess.decompose, "none", window
    )


def test_a_shared_trend_gives_a_small_delta(long_preprocess):
    window = [r for seed in range(4) for r in series(f"s{seed}", ramp(seed))]
    rows = rows_of(long_preprocess, window)
    assert all(abs(row["lw_neighbour_slope_delta"]) < 0.1 for row in rows.values())
    assert all(row["lw_neighbor_count"] == 3.0 for row in rows.values())


def test_one_drifting_sensor_stands_out(long_preprocess):
    flat = ["f0", "f1", "f2"]
    window = [r for seed, s in enumerate(flat) for r in series(s, noise(360, seed))]
    rows = rows_of(long_preprocess, window + series("drift", ramp(9)))
    others = max(abs(rows[s]["lw_neighbour_slope_delta"]) for s in flat)
    # noise alone moves a 6 h slope by about 0.07 z/h; the ramp gives about 0.5
    assert abs(rows["drift"]["lw_neighbour_slope_delta"]) > 5 * others


def test_a_lone_sensor_has_no_neighbours(long_preprocess):
    window = series("a", ramp(0)) + series("alone", ramp(1), cluster="water")
    rows = rows_of(long_preprocess, window + series("b", ramp(2)))
    assert rows["alone"]["lw_neighbor_count"] == 0.0
    assert rows["alone"]["lw_neighbour_slope_delta"] == 0.0
    assert rows["a"]["lw_neighbor_count"] == 1.0


def test_rows_hold_the_long_keys_then_the_reading_count(long_preprocess):
    readings = series("drift", ramp(0))
    rows = rows_of(long_preprocess, readings + readings[:10])  # 10 repeats dropped
    assert tuple(rows["drift"]) == (*LONG_KEYS, "sample_count")
    assert rows["drift"]["sample_count"] == float(len(readings))


def test_an_empty_window_gives_no_rows(long_preprocess):
    assert rows_of(long_preprocess, []) == {}


def test_a_long_window_keeps_all_its_readings(long_preprocess):
    # 2,800 readings 31 s apart: their packet orders spread far beyond the 20 s
    # path's max_jump (1000), and none of them is a repeat
    window = [reading_at("s", float(n % 7), n * 31.0, n + 1) for n in range(2800)]
    rows = rows_of(long_preprocess, window)
    assert rows["s"]["sample_count"] == 2800.0


def test_a_restarted_packet_order_keeps_its_readings(long_preprocess):
    # the device restarts after 300 readings: its packet orders start again at 1
    window = [reading_at("s", float(n % 7), n * 60.0, n % 300 + 1) for n in range(600)]
    rows = rows_of(long_preprocess, window)
    assert rows["s"]["sample_count"] == 600.0


ONE_DAY = np.arange(0.0, 86400.0, 60.0)  # one reading a minute
LONG_KALMAN = LongWindowSettings(
    clean_method="kalman", kalman=KalmanSettings(mode="smoother", jump_sigmas=3.0)
)


# the raw grid, now that the shipped long setup cleans it
NO_CLEANING = LongWindowSettings(clean_method="none")


def long_cleaning(settings: LongWindowSettings) -> LongPreProcess:
    return LongPreProcess(PreProcess(), Decompose(), settings)


def test_clean_reads_the_long_window_settings():
    grid = 0.5 + 0.01 * noise(200)
    cfg = get_settings().preprocess
    short_kalman = PreProcess(
        settings=cfg.model_copy(update={"clean_method": "kalman"})
    )
    # the short model's cleaning does not reach the long grid
    raw = LongPreProcess(short_kalman, Decompose(), NO_CLEANING).clean(grid)
    assert np.array_equal(raw, grid) and raw is not grid
    shipped = LongWindowSettings()
    cleaned = long_cleaning(shipped).clean(grid)
    expected = kalman(CleanContext(grid, shipped.kalman, cfg.min_samples))
    assert np.array_equal(cleaned, expected)


def test_cleaning_changes_a_noisy_grid():
    values = 20.0 + noise(len(ONE_DAY))
    raw, _ = long_cleaning(NO_CLEANING).long_features(ONE_DAY, values)
    cleaned, _ = long_cleaning(LONG_KALMAN).long_features(ONE_DAY, values)
    assert any(raw[key] != cleaned[key] for key in raw)
    assert raw["lw_grid_points"] == cleaned["lw_grid_points"]


def test_cleaning_keeps_a_stuck_day_flat():
    # the inexact-value case of test_a_day_stuck_at_an_inexact_value_is_flat, after a
    # noisy morning: the stuck afternoon must stay one flat run under the cleaning
    times = 1.7e9 + 0.37 + ONE_DAY
    times[::7] += 17.0
    values = np.full(len(times), 0.1)
    values[:720] += 0.01 * noise(720)
    raw, _ = long_cleaning(NO_CLEANING).long_features(times, values)
    cleaned, _ = long_cleaning(LONG_KALMAN).long_features(times, values)
    assert raw["lw_flat_run_share"] >= 0.49
    assert cleaned["lw_flat_run_share"] == raw["lw_flat_run_share"]
