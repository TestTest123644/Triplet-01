import random

import pytest

from city_brain.config import load_config
from city_brain.core.estimator import WaterFilter
from city_brain.core.events import ReadingReceived, RegistryLoaded
from city_brain.core.state import BrainState
from city_brain.core.step import step
from city_brain.domain.types import DeviceStatus
from mocks.twin import MockTwin
from tests.factories import T0, make_observation

CFG = load_config("config/standalone.yaml")
FP = CFG.params["filter"]
DT = FP["sample_period_s"]
B = FP["b_prior"]


def run_series(seconds, ratio, then_ratio=None, switch_at_s=None, commanded_u_from_s=None,
               commanded_u=0.0, stuck_from_s=None, inflow=0.015):
    """Feed a WaterFilter a series from the Twin's water equations.

    The level follows the deterministic equation; the 0.002 m per-tick noise is
    measurement noise (integrating it, as the mock does, makes the true slope a random walk).
    """
    rng = random.Random(1)
    f = WaterFilter(FP)
    v = 3.2
    for tick in range(1, int(seconds / DT) + 1):
        t = tick * DT
        r = then_ratio if then_ratio is not None and t > switch_at_s else ratio
        v += (inflow - B * r) * DT
        u = commanded_u if commanded_u_from_s is not None and t > commanded_u_from_s else r
        if stuck_from_s is not None and t > stuck_from_s:
            reading = stuck_value
        else:
            reading = round(v + rng.gauss(0.0, 0.002), 2)
            stuck_value = reading
        f.update(value=reading, tick=tick, weight=1.0, u=u)
    return f


def filter_from_mock(device_id, seconds):
    twin = MockTwin()
    f = WaterFilter(FP)
    for _ in range(int(seconds / DT)):
        for wire in twin.tick():
            if wire["device_id"] == device_id:
                f.update(value=wire["value"], tick=wire["seq"], weight=1.0, u=0.0)
    return f


def test_closed_valve_rate_converges_to_inflow():
    f = run_series(seconds=20, ratio=0.0)
    assert f.rate == pytest.approx(0.015, abs=0.003)


def test_open_valve_rate_is_negative_with_known_u():
    f = run_series(seconds=20, ratio=0.0, then_ratio=1.0, switch_at_s=10)
    assert f.rate == pytest.approx(-0.030, abs=0.004)


def test_zero_weight_does_not_update_and_variance_grows():
    f = run_series(seconds=5, ratio=0.0)
    v0 = f.var_level
    level0 = f.level
    f.update(value=11.0, tick=f.last_tick + 1, weight=0.0, u=0.0)
    assert f.var_level > v0 and f.level == pytest.approx(level0 + 0.015 * 0.1, abs=0.002)


def test_jump_flagged_and_ignored():
    f = run_series(seconds=10, ratio=0.0)
    lvl = f.level
    f.update(value=lvl + 1.0, tick=f.last_tick + 1, weight=1.0, u=0.0)
    assert "IMPLAUSIBLE_JUMP" in f.flags and f.level == pytest.approx(lvl, abs=0.01)


def test_jump_flag_clears_when_a_later_update_passes_the_gate():
    f = run_series(seconds=10, ratio=0.0)
    f.update(value=f.level + 1.0, tick=f.last_tick + 1, weight=1.0, u=0.0)
    f.update(value=f.level + 0.0015, tick=f.last_tick + 1, weight=1.0, u=0.0)
    assert "IMPLAUSIBLE_JUMP" not in f.flags


def test_model_mismatch_resets_to_measurements():
    # filter told u=1.0 (valve open) but the true valve stays closed (actuator attack)
    f = run_series(seconds=10, ratio=0.0, commanded_u_from_s=5, commanded_u=1.0)
    assert "MODEL_MISMATCH" in f.flags
    assert f.observed_slope(since_tick=f.last_tick - 30) == pytest.approx(0.015, abs=0.005)


def test_model_mismatch_flag_clears_after_ungated_updates():
    f = run_series(seconds=10, ratio=0.0, commanded_u_from_s=5, commanded_u=1.0)
    for _ in range(60):
        f.update(value=round(f.level + (0.015 - B) * DT, 2), tick=f.last_tick + 1, weight=1.0, u=1.0)
    assert "MODEL_MISMATCH" not in f.flags


def test_mock_stream_rate_matches_twin():
    f = filter_from_mock("z1-wl1", seconds=60)
    assert f.rate == pytest.approx(0.015, abs=0.004)


def test_observed_slope_ignores_model_and_needs_ten_points():
    f = WaterFilter(FP)
    for tick in range(1, 10):
        f.update(value=3.0 + 0.0015 * tick, tick=tick, weight=1.0, u=0.0)
    assert f.observed_slope(since_tick=0) is None
    f.update(value=3.0 + 0.0015 * 10, tick=10, weight=1.0, u=0.0)
    assert f.observed_slope(since_tick=0) == pytest.approx(0.015, abs=1e-6)
    assert f.observed_slope(since_tick=6) is None


def test_observed_slope_skips_zero_weight_readings():
    f = WaterFilter(FP)
    for tick in range(1, 21):
        f.update(value=3.0 + 0.0015 * tick, tick=tick, weight=1.0, u=0.0)
    for tick in range(21, 31):
        f.update(value=99.0, tick=tick, weight=0.0, u=0.0)
    assert f.observed_slope(since_tick=0) == pytest.approx(0.015, abs=1e-6)


def test_plateaus_from_rounding_do_not_flag_stuck():
    f = run_series(seconds=60, ratio=0.0)
    assert "STUCK" not in f.flags


def test_stuck_sensor_flags_then_clears_when_value_changes():
    f = run_series(seconds=30, ratio=0.0, stuck_from_s=10)
    assert "STUCK" in f.flags
    f.update(value=f.level + 0.5, tick=f.last_tick + 1, weight=1.0, u=0.0)
    assert "STUCK" not in f.flags


def test_ceiling_mirrors_observation_flag():
    f = run_series(seconds=5, ratio=0.0)
    f.update(value=f.level, tick=f.last_tick + 1, weight=1.0, u=0.0, at_ceiling=True)
    assert "CEILING" in f.flags
    f.update(value=f.level, tick=f.last_tick + 1, weight=1.0, u=0.0)
    assert "CEILING" not in f.flags


def test_is_stale_after_stale_s_without_weighted_update():
    f = run_series(seconds=5, ratio=0.0)
    assert not f.is_stale(f.last_tick + 19)
    assert f.is_stale(f.last_tick + 20)
    f.update(value=11.0, tick=f.last_tick + 25, weight=0.0, u=0.0)
    assert f.is_stale(f.last_tick)


def test_never_updated_filter_is_stale():
    assert WaterFilter(FP).is_stale(0)


# ----- step wiring -----


def fresh_state():
    return BrainState.new(CFG.policy, CFG.params)


def feed(state, **kw):
    tag = f"{kw.get('device_id', 'z1-wl1')}-{kw['seq']}"
    state, _ = step(state, ReadingReceived(make_observation(reading_id=f"r-{tag}", sig_hash=tag, **kw)))
    return state


def test_step_creates_one_filter_per_water_device_and_updates_it():
    s = fresh_state()
    for seq in range(1, 4):
        s = feed(s, seq=seq, value=3.0 + 0.01 * seq)
    s = feed(s, seq=1, device_id="z1-wl2", value=2.8)
    assert sorted(s.filters) == ["z1-wl1", "z1-wl2"]
    assert s.filters["z1-wl1"].last_tick == 3


def test_step_ignores_non_water_kinds():
    from city_brain.domain.types import CanonicalUnit, SensorKind

    s = feed(fresh_state(), seq=1, device_id="z1-tr1", kind=SensorKind.TRAFFIC_FLOW,
             unit=CanonicalUnit.VEH_PER_MIN, value=40.0)
    assert s.filters == {}


def test_step_gives_zero_weight_to_non_ok_status():
    s = fresh_state()
    s = feed(s, seq=1, value=3.0)
    s = feed(s, seq=2, value=50.0, device_status=DeviceStatus.QUARANTINED)
    f = s.filters["z1-wl1"]
    assert f.level == pytest.approx(3.0, abs=0.01) and "IMPLAUSIBLE_JUMP" not in f.flags


def test_step_passes_ceiling_flag():
    s = fresh_state()
    s = feed(s, seq=1, value=3.0)
    s = feed(s, seq=2, value=3.0, flags=frozenset({"AT_CEILING"}))
    assert "CEILING" in s.filters["z1-wl1"].flags


def test_new_epoch_clears_filters():
    s = fresh_state()
    s = feed(s, seq=5000, value=3.0, sim_time=T0)
    assert s.filters
    s, _ = step(s, RegistryLoaded((), keys_changed=True))
    s, _ = step(s, ReadingReceived(make_observation(seq=3, reading_id="n", sig_hash="n", sim_time=T0)))
    assert s.epoch == 1 and list(s.filters) == ["z1-wl1"] and s.filters["z1-wl1"].last_tick == 3


def test_lone_spike_does_not_reach_observed_slope():
    f = run_series(seconds=10, ratio=0.0)
    start = f.last_tick - 29
    f.update(value=f.level + 1.0, tick=f.last_tick + 1, weight=1.0, u=0.0)
    for _ in range(10):
        f.update(value=round(f.level + 0.0015, 2), tick=f.last_tick + 1, weight=1.0, u=0.0)
    assert "IMPLAUSIBLE_JUMP" not in f.flags
    assert f.observed_slope(since_tick=start) == pytest.approx(0.015, abs=0.005)


def test_initialised_false_until_first_weighted_reading():
    f = WaterFilter(FP)
    assert not f.initialised
    f.update(value=3.0, tick=1, weight=0.0, u=0.0)
    assert not f.initialised
    f.update(value=3.0, tick=2, weight=1.0, u=0.0)
    assert f.initialised


def test_clamped_rise_to_the_ceiling_is_not_stuck_and_the_valve_can_open():
    f = WaterFilter(CFG.params["filter"])
    tick, level = 0, 11.0
    while level < 12.0:
        tick += 1
        level = min(12.0, level + 0.002)
        f.update(level, tick, 1.0, u=0.0, at_ceiling=level >= 12.0)
    for _ in range(100):
        tick += 1
        f.update(12.0, tick, 1.0, u=0.0, at_ceiling=True)
        assert "STUCK" not in f.flags
    assert "CEILING" in f.flags
    for _ in range(50):
        tick += 1
        level -= 0.0025
        f.update(level, tick, 1.0, u=1.0)
    assert "STUCK" not in f.flags and "CEILING" not in f.flags
    assert f.observed_slope(tick - 30) == pytest.approx(-0.025, abs=1e-6)


def test_a_flat_sensor_below_the_ceiling_is_still_stuck():
    f = WaterFilter(CFG.params["filter"])
    for tick in range(1, 40):
        f.update(5.0 + 0.002 * tick, tick, 1.0, u=0.0)
    for tick in range(40, 80):
        f.update(5.08, tick, 1.0, u=0.0)
    assert "STUCK" in f.flags
