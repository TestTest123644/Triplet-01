"""Enumerative water MPC and the bounded RLS valve gain (spec 8.4, 8.5, 10.3)."""

import random
from datetime import timedelta

import pytest

from city_brain.config import load_config
from city_brain.core.estimator import WaterFilter
from city_brain.core.trust import evidence_ref
from city_brain.core.water import WaterContext, commanded_ratio, plan_water_t0
from city_brain.core.water_mpc import plan_water_mpc, planner_tier
from city_brain.domain.types import CapabilityTier, RiskClass
from mocks.twin import physics
from tests.factories import T0, make_observation, make_verdict

CFG = load_config("config/standalone.yaml")
POLICY, PARAMS = CFG.policy, CFG.params
MPC = POLICY["water"]["mpc"]
PERIOD_S = PARAMS["filter"]["sample_period_s"]
B_PRIOR = PARAMS["filter"]["b_prior"]
INFLOW = 0.015  # m/s, the Twin's mean inflow
DEVICE = "tank"


def cited(n=3, weight=1.0):
    """The readings a planner has to be able to point at before it may plan at all."""
    return tuple(
        evidence_ref(make_observation(device_id=DEVICE, reading_id=f"r-{i}", sig_hash=f"s-{i}"),
                     make_verdict(reading_id=f"r-{i}", sig_hash=f"s-{i}"), weight)
        for i in range(n)
    )


def context(level=3.2, valve="closed", inflow=INFLOW, tick=None, trusted_for_s=30.0, **kw):
    """A settled channel at `level` with a known inflow, as the filter would see it."""
    water_filter = WaterFilter(PARAMS["filter"])
    ratio = commanded_ratio(valve) or 0.0
    rate = inflow - water_filter.b_hat * ratio
    # Long enough for the smoothed inflow to have converged: the planner needs a few
    # multiples of inflow_tau_s of data before its model is worth planning against.
    ticks = 2000
    start = level - rate * PERIOD_S * ticks  # so the filter ends up at `level`, moving at `rate`
    for n in range(1, ticks + 1):
        water_filter.update(start + rate * PERIOD_S * n, n, 1.0, u=ratio)
    now = T0 + timedelta(seconds=ticks * PERIOD_S)
    fields = dict(
        device_id=DEVICE, zone_id="zone-1", filter=water_filter, valve_state=valve,
        recent=cited(), below_low_since=None, now=now, plan_version=1,
        global_tick=ticks if tick is None else tick, target=DEVICE,
        min_val=0.0, max_val=12.0,
        trusted_since=None if trusted_for_s is None else now - timedelta(seconds=trusted_for_s),
    )
    fields.update(kw)
    return WaterContext(**fields)


def plan(**kw):
    return plan_water_mpc(context(**kw), POLICY)


def pct_of(intent):
    assert intent is not None
    return round((commanded_ratio(intent.state) or 0.0) * 100)


# ----- the gain estimator -----


def test_the_gain_starts_at_the_prior():
    assert WaterFilter(PARAMS["filter"]).b_hat == pytest.approx(B_PRIOR)


def test_a_consistent_measurement_pulls_the_gain_towards_the_truth():
    f = WaterFilter(PARAMS["filter"])
    truth = 0.030
    for _ in range(200):
        f.identify_gain(u=1.0, slope=f.inflow - truth * 1.0)
    assert f.b_hat == pytest.approx(truth, abs=0.002)


def test_the_gain_converges_from_below_too():
    f = WaterFilter(PARAMS["filter"])
    truth = 0.070
    for _ in range(200):
        f.identify_gain(u=0.5, slope=f.inflow - truth * 0.5)
    assert f.b_hat == pytest.approx(truth, abs=0.003)


@pytest.mark.parametrize("truth", [0.0, 0.5])
def test_the_gain_stays_inside_its_bounds(truth):
    f = WaterFilter(PARAMS["filter"])
    for _ in range(500):
        f.identify_gain(u=1.0, slope=f.inflow - truth)
    assert PARAMS["filter"]["b_min"] <= f.b_hat <= PARAMS["filter"]["b_max"]


def test_a_closed_valve_says_nothing_about_the_gain():
    f = WaterFilter(PARAMS["filter"])
    f.identify_gain(u=0.0, slope=0.5)
    assert f.b_hat == pytest.approx(B_PRIOR)


def test_one_wild_measurement_cannot_move_the_gain_far():
    f = WaterFilter(PARAMS["filter"])
    f.identify_gain(u=1.0, slope=-100.0)
    assert PARAMS["filter"]["b_min"] <= f.b_hat <= PARAMS["filter"]["b_max"]


def test_the_rate_uses_the_learned_gain():
    f = WaterFilter(PARAMS["filter"])
    for _ in range(300):
        f.identify_gain(u=1.0, slope=f.inflow - 0.030)
    f.update(3.0, 1, 1.0, u=1.0)
    assert f.rate == pytest.approx(f.inflow - f.b_hat, abs=1e-9)


# ----- the planner's choice -----


def test_a_tank_at_the_target_is_held_there_by_a_part_open_valve():
    intent = plan(level=4.0)
    assert pct_of(intent) == 33, "the inflow is a third of the full drain"
    assert intent.state == "ratio:33" and intent.capability == "valve"


def test_a_tank_low_in_the_band_is_held_rather_than_pushed_about():
    # Filling to the target means letting the level rise past it later, which the high band
    # punishes: the balancing position is the one that keeps the tank where it is safe.
    assert pct_of(plan(level=3.2, valve="closed")) == 33


def test_a_tank_at_the_high_band_is_opened_further():
    assert pct_of(plan(level=6.2)) > 33


def test_a_tank_at_the_target_with_the_right_valve_is_left_alone():
    assert plan(level=4.0, valve="ratio:33") is None


def test_a_closed_valve_at_the_high_band_is_always_opened():
    intent = plan(level=6.1, valve="closed")
    assert intent is not None and pct_of(intent) > 0


def test_a_tank_below_the_low_band_stops_being_drained():
    intent = plan(level=2.5, valve="open")
    pct = pct_of(intent)
    assert pct < 33, "the valve must let the tank refill"
    ctx = context(level=2.5, valve="open")
    assert ctx.filter.inflow - ctx.filter.b_hat * pct / 100 > 0, "the level has to turn around"


def test_the_valve_is_not_nudged_for_a_tiny_gain():
    # A third open at the target: moving to 20% or 50% cannot pay for lambda_change.
    assert plan(level=4.05, valve="ratio:33") is None


def test_every_choice_comes_from_the_candidate_set():
    for level in (0.5, 2.0, 3.2, 4.0, 5.0, 6.5, 9.5, 11.5):
        for valve in ("closed", "ratio:33", "open"):
            intent = plan(level=level, valve=valve)
            if intent is not None:
                assert pct_of(intent) in MPC["candidates"]


def test_the_plan_is_only_remade_once_a_second():
    replan_ticks = round(MPC["replan_s"] / PERIOD_S)
    assert plan(level=6.2, tick=replan_ticks * 3) is not None
    assert plan(level=6.2, tick=replan_ticks * 3 + 1) is None


# ----- risk, effects and evidence -----


def test_opening_further_is_r1_and_expects_the_level_to_stop_rising():
    intent = plan(level=6.2, valve="ratio:33")
    assert intent.risk_class is RiskClass.R1
    effect = intent.expected_effect
    assert effect.comparator == "<=" and effect.window_s == MPC["open_window_s"]
    assert effect.channel == DEVICE and effect.metric == "rate"


def test_closing_is_r2_and_expects_the_level_to_stop_falling():
    intent = plan(level=2.5, valve="open")
    assert intent.risk_class is RiskClass.R2
    effect = intent.expected_effect
    assert effect.comparator == ">=" and effect.window_s == MPC["close_window_s"]


def test_a_closing_move_needs_sustained_trusted_data():
    assert plan(level=2.5, valve="open", trusted_for_s=1.0) is None
    assert plan(level=2.5, valve="open", trusted_for_s=None) is None


def test_an_opening_move_does_not_wait_for_sustained_data():
    assert plan(level=6.2, valve="closed", trusted_for_s=0.0) is not None


def test_the_expected_rate_follows_the_learned_gain():
    from city_brain.core.water_mpc import inflow_estimate

    ctx = context(level=6.2, valve="ratio:33")
    intent = plan_water_mpc(ctx, POLICY)
    pct = pct_of(intent)
    predicted = inflow_estimate(ctx) - ctx.filter.b_hat * pct / 100
    assert intent.expected_effect.threshold == pytest.approx(predicted + MPC["tolerance"])


def test_the_intent_cites_the_channels_evidence():
    ref = evidence_ref(make_observation(device_id=DEVICE), None, 0.5)
    refs = (*cited(2), ref)
    intent = plan_water_mpc(context(level=6.2, recent=refs), POLICY)
    assert intent.evidence == refs


def test_a_held_channel_is_not_planned_on():
    assert plan_water_mpc(context(level=6.2, untrusted=True), POLICY) is None


def test_a_contained_channel_heading_for_critical_asks_for_an_override():
    intent = plan_water_mpc(context(level=9.2, valve="closed", contained=True), POLICY)
    assert intent is not None
    assert intent.risk_class is RiskClass.R3 and intent.containment_override is True


def test_a_contained_channel_that_is_not_critical_is_held():
    assert plan_water_mpc(context(level=4.0, valve="closed", contained=True), POLICY) is None


# ----- the tier switch -----


def test_the_tier_follows_the_configured_planner():
    assert planner_tier(POLICY) is CapabilityTier.T2
    assert planner_tier({"water": {"planner": "t0"}}) is CapabilityTier.T0


# ----- against the Twin's own physics -----


def simulate(planner, seconds, start=3.2, seed=7, policy=POLICY, after_s=0.0):
    """Drive the Twin's own water physics with a planner in the loop.

    Returns (valve changes after `after_s`, every level). The low-band clock is kept the way
    `step` keeps it: it starts when the level first drops into the band and is cleared above.
    """
    rng = random.Random(seed)
    water_filter = WaterFilter(PARAMS["filter"])
    low = POLICY["water"]["low_band"]
    level, valve, changes, levels = start, "closed", 0, []
    below_low_since = None
    for tick in range(1, round(seconds / PERIOD_S) + 1):
        ratio = commanded_ratio(valve) or 0.0
        t_norm = (tick * PERIOD_S % 86400) / 86400.0
        level = min(12.0, max(0.0, physics.water(level, ratio, t_norm, rng)))
        water_filter.update(level, tick, 1.0, u=ratio)
        now = T0 + timedelta(seconds=tick * PERIOD_S)
        if level > low:
            below_low_since = None
        elif below_low_since is None and ratio > 0.0:
            below_low_since = now
        ctx = WaterContext(
            device_id=DEVICE, zone_id="zone-1", filter=water_filter, valve_state=valve,
            recent=cited(), below_low_since=below_low_since, now=now, plan_version=tick,
            global_tick=tick, target=DEVICE, min_val=0.0, max_val=12.0, trusted_since=T0,
        )
        intent = planner(ctx, policy)
        if intent is not None and intent.state != valve:
            valve = intent.state
            if tick * PERIOD_S >= after_s:
                changes += 1
        levels.append(level)
    return changes, levels


def test_the_mpc_holds_the_level_in_the_band_around_the_target():
    changes, levels = simulate(plan_water_mpc, 900.0)
    settled = levels[600:]  # the first minute is the climb from 3.2 m
    assert min(settled) >= 3.0 and max(settled) <= 6.0
    assert abs(sum(settled) / len(settled) - MPC["target"]) < 1.0


def test_the_mpc_settles_on_a_valve_position():
    late, _ = simulate(plan_water_mpc, 900.0, after_s=120.0)
    assert late <= 2, f"{late} valve changes after settling"


def test_the_same_run_gives_the_same_plan():
    assert simulate(plan_water_mpc, 300.0) == simulate(plan_water_mpc, 300.0)


def test_the_mpc_moves_the_valve_far_less_than_bang_bang():
    mpc_changes, _ = simulate(plan_water_mpc, 900.0)
    t0_changes, _ = simulate(plan_water_t0, 900.0)
    assert mpc_changes <= t0_changes / 2, f"mpc {mpc_changes} vs t0 {t0_changes}"


# ----- through `step` -----


def city():
    """A whole zone with the configured (model-based) planner in charge."""
    from tests.helpers_city import city_state

    return city_state()


def water_commands(state):
    from tests.helpers_city import commands

    return commands(state, "valve")


def drive(state, levels):
    from tests.helpers_city import device_id, tick

    for level in levels:
        state = tick(state, {device_id("wl1"): level})
    return state


def test_the_mpc_runs_the_water_channel_through_step():
    state = drive(city(), [3.2 + 0.002 * n for n in range(400)])
    sent = [c for c in water_commands(state) if c["status"] == "DISPATCHED"]
    assert sent, "the model-based planner must actuate the valve"
    assert all(c["capability"] == "valve" for c in sent)
    assert any(c["state"].startswith("ratio:") for c in sent), "a ratio is what an MPC asks for"


def test_the_t0_planner_can_be_configured_instead():
    from city_brain.core.state import BrainState
    from city_brain.core.step import step
    from tests.helpers_city import ALLOWLIST, SPEC, profile
    from city_brain.core.events import RegistryLoaded

    policy = {**POLICY, "water": {**POLICY["water"], "planner": "t0"}}
    state = BrainState.new(policy, PARAMS)
    devices = tuple(profile(suffix) for suffix in SPEC)
    state, effects = step(state, RegistryLoaded(devices, ALLOWLIST))
    state.last_effects, state.levels = list(effects), {}
    state.mode = state.mode.__class__.NORMAL
    state.guardian_heard = True
    state = drive(state, [5.6 + 0.004 * n for n in range(200)])
    sent = [c for c in water_commands(state) if c["status"] == "DISPATCHED"]
    assert sent and all(c["state"] in ("open", "closed") for c in sent)


def test_a_verified_valve_move_teaches_the_channel_its_gain():
    from tests.helpers_city import device_id, published

    state = city()
    state = drive(state, [3.2 + 0.002 * n for n in range(400)])
    level = state.levels[device_id("wl1")]
    state = drive(state, [level - 0.003 * n for n in range(1, 120)])
    channel = state.filters[device_id("wl1")]
    verdicts = [e["data"]["verdict"] for e in published(state, "verification")]
    assert verdicts, "the valve move must be checked"
    assert channel.b_hat != B_PRIOR, "a settled move with a measured slope teaches the gain"
    assert PARAMS["filter"]["b_min"] <= channel.b_hat <= PARAMS["filter"]["b_max"]
