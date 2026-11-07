"""First-order lag filter for the non-water channels (spec 8.2)."""

import math

import pytest

from city_brain.config import load_config
from city_brain.core.channel_filter import LagFilter, lag_params
from city_brain.domain.types import SensorKind

PARAMS = load_config("config/standalone.yaml").params
TRAFFIC = lag_params(PARAMS, SensorKind.TRAFFIC_FLOW)
POWER = lag_params(PARAMS, SensorKind.POWER_LOAD)
PERIOD_S = TRAFFIC["sample_period_s"]


def traffic_filter(min_val=0.0, max_val=200.0, **overrides):
    return LagFilter({**TRAFFIC, **overrides}, min_val=min_val, max_val=max_val)


def feed(f, values, start_tick=1, weight=1.0):
    for n, value in enumerate(values):
        f.update(value, start_tick + n, weight)
    return f


def steady(f, value, ticks, start_tick=1, weight=1.0):
    return feed(f, [value] + [value] * (ticks - 1), start_tick, weight)


# ----- tracking -----


def test_a_new_filter_is_not_initialised():
    f = traffic_filter()
    assert not f.initialised and math.isnan(f.level) and math.isnan(f.value)


def test_an_unweighted_reading_does_not_initialise():
    f = traffic_filter()
    f.update(45.0, 1, weight=0.0)
    assert not f.initialised


def test_the_first_weighted_reading_initialises_at_its_value():
    f = traffic_filter()
    f.update(45.0, 1, weight=1.0)
    assert f.initialised and f.level == pytest.approx(45.0) and f.value == pytest.approx(45.0)
    assert f.drift == pytest.approx(0.0)


def test_a_flat_channel_keeps_its_level_and_no_drift():
    f = steady(traffic_filter(), 45.0, 100)
    assert f.level == pytest.approx(45.0, abs=0.5)
    assert f.drift == pytest.approx(0.0, abs=0.05)


def test_a_ramp_is_tracked_with_a_drift():
    f = traffic_filter()
    for n in range(1, 301):  # +0.1 veh/min per tick = 1.0 per second
        f.update(45.0 + 0.1 * n, n, weight=1.0)
    assert f.level == pytest.approx(75.0, abs=1.5)
    assert f.drift == pytest.approx(1.0, abs=0.2)


def test_an_unweighted_reading_leaves_the_measured_value_alone():
    f = steady(traffic_filter(), 45.0, 30)
    f.update(180.0, 31, weight=0.0)
    assert f.value == pytest.approx(45.0)


def test_the_same_readings_give_the_same_state():
    values = [45.0 + 0.3 * n for n in range(60)]
    a, b = feed(traffic_filter(), values), feed(traffic_filter(), values)
    assert (a.level, a.drift, a.flags) == (b.level, b.drift, b.flags)


# ----- fault flags -----


def test_dropout_when_the_channel_falls_to_its_floor():
    f = steady(traffic_filter(), 45.0, 60)
    f.update(0.0, 61, weight=1.0)
    assert "DROPOUT" in f.flags


def test_no_dropout_when_the_channel_was_already_near_the_floor():
    f = steady(traffic_filter(), 0.2, 60)
    f.update(0.0, 61, weight=1.0)
    assert "DROPOUT" not in f.flags


def test_dropout_clears_when_the_channel_returns():
    f = steady(traffic_filter(), 45.0, 60)
    f.update(0.0, 61, weight=1.0)
    f = steady(f, 45.0, 10, start_tick=62)
    assert "DROPOUT" not in f.flags


def test_an_implausible_jump_is_gated():
    f = LagFilter(POWER, min_val=5.0, max_val=120.0)
    f = steady(f, 68.0, 100)
    f.update(118.0, 101, weight=1.0)
    assert "IMPLAUSIBLE_JUMP" in f.flags
    assert f.level == pytest.approx(68.0, abs=1.0), "the spike must not move the estimate"


def test_a_gated_jump_is_kept_out_of_the_baseline():
    f = LagFilter(POWER, min_val=5.0, max_val=120.0)
    f = steady(f, 68.0, 100)
    before = f.baseline_mean
    f.update(118.0, 101, weight=1.0)
    assert f.baseline_mean == pytest.approx(before, abs=0.2)


def test_a_sustained_shift_reinitialises_the_filter():
    f = LagFilter(POWER, min_val=5.0, max_val=120.0)
    f = steady(f, 68.0, 100)
    f = steady(f, 118.0, 10, start_tick=101)
    assert f.level == pytest.approx(118.0, abs=2.0)
    assert "MODEL_MISMATCH" in f.flags


def test_a_slew_beyond_the_limit_is_gated_even_when_the_variance_is_wide():
    f = traffic_filter(slew_limit=5.0, jump_sigma=1000.0)
    f = steady(f, 45.0, 30)
    f.update(80.0, 31, weight=1.0)
    assert "IMPLAUSIBLE_JUMP" in f.flags


def test_stuck_after_enough_identical_readings():
    f = steady(traffic_filter(), 42.0, 40)
    assert "STUCK" in f.flags


def test_not_stuck_before_enough_identical_readings():
    f = steady(traffic_filter(), 42.0, 10)
    assert "STUCK" not in f.flags


def test_stuck_clears_when_the_value_moves():
    f = steady(traffic_filter(), 42.0, 40)
    f.update(44.0, 41, weight=1.0)
    assert "STUCK" not in f.flags


def test_a_sensor_at_its_ceiling_is_not_stuck():
    f = traffic_filter()
    for n in range(1, 41):
        f.update(200.0, n, weight=1.0, at_ceiling=True)
    assert "STUCK" not in f.flags and "CEILING" in f.flags


def test_stale_after_the_timeout_and_clear_on_the_next_reading():
    f = steady(traffic_filter(), 45.0, 30)
    assert not f.is_stale(30) and "STALE" not in f.flags
    gap = round(TRAFFIC["stale_s"] / PERIOD_S)
    assert f.is_stale(30 + gap)
    f.update(45.0, 30 + gap, weight=1.0)
    assert "STALE" in f.flags, "the gap before this reading was too long"
    f.update(45.0, 31 + gap, weight=1.0)
    assert "STALE" not in f.flags


def test_an_uninitialised_filter_is_stale():
    assert traffic_filter().is_stale(0)


def test_unweighted_readings_do_not_keep_a_channel_fresh():
    f = steady(traffic_filter(), 45.0, 30)
    gap = round(TRAFFIC["stale_s"] / PERIOD_S)
    for n in range(31, 31 + gap + 5):
        f.update(45.0, n, weight=0.0)
    assert f.is_stale(31 + gap + 5)


# ----- slope and baseline -----


def test_observed_slope_needs_enough_points():
    f = feed(traffic_filter(), [45.0, 45.1, 45.2])
    assert f.observed_slope(since_tick=0) is None


def test_observed_slope_measures_the_ramp():
    f = feed(traffic_filter(), [45.0 + 0.1 * n for n in range(60)])
    assert f.observed_slope(since_tick=0) == pytest.approx(1.0, abs=0.01)


def test_observed_slope_can_stop_before_now():
    f = traffic_filter()
    for n in range(1, 61):
        f.update(45.0, n, weight=1.0)
    for n in range(61, 121):
        f.update(45.0 + 0.1 * (n - 60), n, weight=1.0)
    assert f.observed_slope(since_tick=0, until_tick=60) == pytest.approx(0.0, abs=0.01)
    assert f.observed_slope(since_tick=61) == pytest.approx(1.0, abs=0.01)


def test_observed_slope_excludes_ticks():
    f = feed(traffic_filter(), [45.0] * 30)
    f.update(200.0, 31, weight=1.0)
    f = feed(f, [45.0] * 30, start_tick=32)
    assert f.observed_slope(since_tick=0, exclude_ticks=frozenset({31})) == pytest.approx(0.0, abs=0.01)


def test_the_baseline_tracks_mean_and_spread():
    # A spread the filter accepts: a 10 veh/min alternation in a 1.5-variance channel is a jump.
    f = feed(traffic_filter(), [44.0, 46.0] * 30)
    assert f.baseline_mean == pytest.approx(45.0, abs=0.01)
    assert f.baseline_std == pytest.approx(1.0, abs=0.01)
    assert f.baseline_count == 60


def test_the_baseline_forgets_readings_older_than_its_window():
    f = traffic_filter(baseline_window_s=1.0)  # 10 ticks
    f = feed(f, [45.0 + 0.1 * n for n in range(30)])
    assert f.baseline_count == 10
    assert f.baseline_mean == pytest.approx(45.0 + 0.1 * 24.5, abs=0.3)


def test_unweighted_readings_stay_out_of_the_baseline():
    f = feed(traffic_filter(), [45.0] * 30)
    f.update(200.0, 31, weight=0.0)
    assert f.baseline_mean == pytest.approx(45.0)


def test_the_z_score_uses_the_baseline():
    f = feed(traffic_filter(), [44.0, 46.0] * 30)
    assert f.zscore(48.0) == pytest.approx(3.0, abs=0.01)
    assert f.zscore() == pytest.approx(1.0, abs=0.01)


def test_the_z_score_is_unknown_without_a_spread():
    f = feed(traffic_filter(), [45.0] * 30)
    assert f.zscore(60.0) is None


def test_the_z_score_is_unknown_without_enough_points():
    f = feed(traffic_filter(), [44.0, 46.0])
    assert f.zscore(60.0) is None


# ----- parameters -----


def test_lag_params_overlays_the_kind_on_the_defaults():
    assert POWER["r0"] == 0.3 and POWER["slew_limit"] == 12.0
    assert POWER["sample_period_s"] == PARAMS["lag_filter"]["defaults"]["sample_period_s"]


def test_lag_params_falls_back_to_the_defaults_for_an_unlisted_kind():
    assert lag_params(PARAMS, SensorKind.WATER_LEVEL) == PARAMS["lag_filter"]["defaults"]
