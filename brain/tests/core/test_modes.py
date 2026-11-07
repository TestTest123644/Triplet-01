"""Degradation modes and the Shield's MODE_FORBIDS check (spec 8.9, 8.6 check 9)."""

from datetime import timedelta

import pytest

from city_brain.core import modes
from city_brain.core.events import (
    GuardianEventReceived,
    OperatorCommand,
    Publish,
    SendCommand,
    StartTimer,
    TimerFired,
)
from city_brain.core.shield import ShieldContext, TokenBucket, evaluate
from city_brain.core.state import BrainState
from city_brain.core.step import step
from city_brain.domain.types import (
    ActionIntent,
    ActionStatus,
    ActuatorCapability,
    BrainMode,
    CanonicalUnit,
    DeviceProfile,
    DeviceStatus,
    ExpectedEffect,
    GuardianHeartbeat,
    RiskClass,
    SensorKind,
)
from tests.factories import T0
from tests.helpers_coordinator import (
    CFG,
    PERIOD_S,
    RISE_RATE,
    apply,
    feed_tick,
    of_type,
    state_with_registry,
)

# ----- the Shield's check 9 -----

ALLOW = {"valve": frozenset({"open", "closed", "ratio"})}
POLICY = {
    "shield": {"cooldown_s": {"valve": 5.0}, "rate_limit": {"burst": 8, "per_s": 2.0}},
    "modes": {"safe_baseline_r1": False},
}


def profile(device_id, current="closed"):
    return DeviceProfile(
        device_id=device_id, zone_id="zone-1", kind=SensorKind.WATER_LEVEL, unit=CanonicalUnit.M,
        min_val=0.0, max_val=10.0, status=DeviceStatus.OK,
        actuator=ActuatorCapability(capability="valve", target=device_id,
                                    states=frozenset({"open", "closed"}), current_state=current),
    )


def intent(state="open", risk=RiskClass.R1, target="z1-wl1"):
    return ActionIntent(
        action_id="a-1", incident_id="i-1", plan_version=1, capability="valve",
        target=target, state=state, risk_class=risk,
        expected_effect=ExpectedEffect(channel="c", metric="rate", comparator="<=",
                                       threshold=-0.01, window_s=3.0),
        evidence=(), rationale="r", alternatives=(),
    )


def shield(intent_obj, mode, policy=POLICY, current="closed"):
    context = ShieldContext(
        registry={"z1-wl1": profile("z1-wl1", current)}, in_flight=set(), cycle_intents=(),
        last_command_at={}, bucket=TokenBucket(burst=8, per_s=2.0), suspect=set(), now=T0, mode=mode,
    )
    return evaluate(intent_obj, context, ALLOW, policy)


def test_the_check_is_part_of_every_result():
    assert "MODE_FORBIDS" in [c.name for c in shield(intent(), BrainMode.NORMAL).checks]


@pytest.mark.parametrize("risk", list(RiskClass))
def test_normal_mode_forbids_nothing(risk):
    assert "MODE_FORBIDS" not in shield(intent(risk=risk), BrainMode.NORMAL).refusals


@pytest.mark.parametrize(
    "mode", [BrainMode.WARMUP, BrainMode.TWIN_DEGRADED, BrainMode.PAUSED, BrainMode.GUARDIAN_DEGRADED]
)
@pytest.mark.parametrize("risk", [RiskClass.R1, RiskClass.R2, RiskClass.R3])
def test_degraded_modes_allow_r0_only(mode, risk):
    assert "MODE_FORBIDS" in shield(intent(risk=risk), mode).refusals
    assert "MODE_FORBIDS" not in shield(intent(risk=RiskClass.R0), mode).refusals


def test_safe_baseline_allows_a_drain_direction_r1_while_the_guardian_is_down():
    policy = {**POLICY, "modes": {"safe_baseline_r1": True}}
    allowed = shield(intent(state="open"), BrainMode.GUARDIAN_DEGRADED, policy)
    assert "MODE_FORBIDS" not in allowed.refusals


def test_safe_baseline_still_refuses_closing_the_valve():
    policy = {**POLICY, "modes": {"safe_baseline_r1": True}}
    refused = shield(intent(state="closed"), BrainMode.GUARDIAN_DEGRADED, policy, current="open")
    assert "MODE_FORBIDS" in refused.refusals


def test_safe_baseline_does_not_apply_to_r2():
    policy = {**POLICY, "modes": {"safe_baseline_r1": True}}
    assert "MODE_FORBIDS" in shield(intent(risk=RiskClass.R2), BrainMode.GUARDIAN_DEGRADED, policy).refusals


def test_safe_baseline_does_not_apply_in_other_modes():
    policy = {**POLICY, "modes": {"safe_baseline_r1": True}}
    assert "MODE_FORBIDS" in shield(intent(state="open"), BrainMode.PAUSED, policy).refusals


# ----- the state machine -----


def heartbeat(state, through_s=0.0, issued_s=None):
    issued_s = through_s if issued_s is None else issued_s
    return apply(state, GuardianEventReceived(GuardianHeartbeat(
        evaluated_through=T0 + timedelta(seconds=through_s),
        issued_at=T0 + timedelta(seconds=issued_s),
    )))


def modes_announced(state):
    return [
        e.event["data"]["mode"] for e in state.last_effects
        if isinstance(e, Publish) and e.event["data"].get("reason") == "mode_changed"
    ]


def warming(*device_ids):
    """A registry with the Brain still in WARMUP, as a real run starts."""
    return state_with_registry(*device_ids, mode=BrainMode.WARMUP)


def fresh():
    state = BrainState.new(CFG.policy, CFG.params)
    state.last_effects, state.levels = [], {}
    return state


def test_a_new_brain_is_in_warmup():
    assert fresh().mode is BrainMode.WARMUP


def test_the_registry_alone_does_not_leave_warmup():
    assert warming("z1-wl1").mode is BrainMode.WARMUP


def test_a_heartbeat_without_a_registry_does_not_leave_warmup():
    assert heartbeat(fresh()).mode is BrainMode.WARMUP


def test_registry_and_a_heartbeat_reach_normal():
    state = heartbeat(warming("z1-wl1"))
    assert state.mode is BrainMode.NORMAL and state.guardian_heard is True
    assert modes_announced(state) == [BrainMode.NORMAL]


def test_a_verdict_also_reaches_normal():
    state = feed_tick(warming("z1-wl1"), {"z1-wl1": 4.0})
    assert state.mode is BrainMode.NORMAL


def test_a_late_registry_still_reaches_normal():
    state = heartbeat(fresh())
    state = apply(state, _registry_event())
    assert state.mode is BrainMode.NORMAL


def _registry_event():
    from city_brain.core.events import RegistryLoaded
    from tests.helpers_coordinator import ALLOWLIST, water_device
    return RegistryLoaded((water_device("z1-wl1"),), ALLOWLIST)


def test_reaching_normal_arms_the_heartbeat_watchdog():
    state = heartbeat(warming("z1-wl1"))
    timers = [t for t in of_type(StartTimer, state.last_effects) if t.kind == "guardian_heartbeat"]
    assert timers and timers[-1].wall_seconds == CFG.policy["modes"]["guardian_heartbeat_timeout_s"]


def test_a_silent_guardian_degrades_and_opens_an_outage_window():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 4.0})
    state = apply(state, TimerFired("guardian_hb", "guardian_heartbeat"))
    assert state.mode is BrainMode.GUARDIAN_DEGRADED
    assert state.outage_windows and state.outage_windows[-1][1] is None


def test_a_resumed_guardian_recovers_and_closes_the_window():
    state = feed_tick(warming("z1-wl1"), {"z1-wl1": 4.0})
    state = apply(state, TimerFired("guardian_hb", "guardian_heartbeat"))
    state = heartbeat(state, through_s=state.sim_now.timestamp() - T0.timestamp())
    assert state.mode is BrainMode.NORMAL
    assert state.outage_windows[-1][1] is not None
    assert modes_announced(state) == [BrainMode.NORMAL, BrainMode.GUARDIAN_DEGRADED, BrainMode.NORMAL]


def test_a_stale_heartbeat_does_not_recover():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 4.0})
    for n in range(2, 60):  # advance sim time well past the recovery lag
        state = feed_tick(state, {"z1-wl1": 4.0}, seq=n)
    state = apply(state, TimerFired("guardian_hb", "guardian_heartbeat"))
    state = heartbeat(state, through_s=0.0)
    assert state.mode is BrainMode.GUARDIAN_DEGRADED and state.outage_windows[-1][1] is None


def test_a_lagging_watermark_degrades_without_a_timer():
    state = state_with_registry("z1-wl1")
    state = heartbeat(state, through_s=0.0)
    assert state.mode is BrainMode.NORMAL
    lag = CFG.policy["modes"]["verdict_lag_degraded_s"]
    for n in range(1, round((lag + 1.0) / PERIOD_S) + 1):
        state = feed_tick(state, {"z1-wl1": 4.0}, verdict=None, seq=n)
    assert state.mode is BrainMode.GUARDIAN_DEGRADED


def test_a_silent_twin_degrades_and_the_next_reading_recovers():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 4.0})
    state = apply(state, TimerFired("twin_stale", "twin_stale"))
    assert state.mode is BrainMode.TWIN_DEGRADED
    state = feed_tick(state, {"z1-wl1": 4.0})
    assert state.mode is BrainMode.NORMAL


def test_a_reading_arms_the_twin_watchdog():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 4.0})
    timers = [t for t in of_type(StartTimer, state.last_effects) if t.kind == "twin_stale"]
    assert timers and timers[-1].wall_seconds == CFG.policy["modes"]["twin_stale_timeout_s"]


def test_a_twin_recovery_during_a_guardian_outage_returns_to_guardian_degraded():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 4.0})
    state = apply(state, TimerFired("guardian_hb", "guardian_heartbeat"))
    state = apply(state, TimerFired("twin_stale", "twin_stale"))
    assert state.mode is BrainMode.TWIN_DEGRADED
    state = feed_tick(state, {"z1-wl1": 4.0}, verdict=None)
    assert state.mode is BrainMode.GUARDIAN_DEGRADED


def test_pause_and_resume():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 4.0})
    state = apply(state, OperatorCommand(action="pause"))
    assert state.mode is BrainMode.PAUSED
    state = apply(state, OperatorCommand(action="resume"))
    assert state.mode is BrainMode.NORMAL


def test_resume_while_the_guardian_is_down_returns_to_guardian_degraded():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 4.0})
    state = apply(state, TimerFired("guardian_hb", "guardian_heartbeat"))
    state = apply(state, OperatorCommand(action="pause"))
    state = apply(state, OperatorCommand(action="resume"))
    assert state.mode is BrainMode.GUARDIAN_DEGRADED


def test_resume_without_a_pause_changes_nothing():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 4.0})
    before = len(state.last_effects)
    state = apply(state, OperatorCommand(action="resume"))
    assert state.mode is BrainMode.NORMAL and len(state.last_effects) == before


def test_pausing_twice_announces_one_change():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 4.0})
    state = apply(state, OperatorCommand(action="pause"))
    state = apply(state, OperatorCommand(action="pause"))
    assert modes_announced(state).count(BrainMode.PAUSED) == 1


def test_an_approval_command_does_not_move_the_mode():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 4.0})
    state = apply(state, OperatorCommand(action="approve", action_id="a-1"))
    assert state.mode is BrainMode.NORMAL


# ----- the kill switch end to end -----


def rise(state, device_id="z1-wl1", start=5.5, ticks=400, **kw):
    level = start
    for _ in range(ticks):
        state = feed_tick(state, {device_id: level}, **kw)
        level = round(level + RISE_RATE * PERIOD_S, 6)
        if any(r.status is ActionStatus.DISPATCHED for r in state.actions.values()):
            break
    return state


def test_paused_refuses_the_water_valve_and_sends_nothing():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 5.0})
    state = apply(state, OperatorCommand(action="pause"))
    state = rise(state)
    assert state.actions, "no action was ever proposed"
    statuses = {r.status for r in state.actions.values()}
    assert statuses == {ActionStatus.REFUSED}
    refused = next(iter(state.actions.values()))
    assert "MODE_FORBIDS" in refused.shield.refusals
    assert of_type(SendCommand, state.last_effects) == []


def test_resuming_lets_the_valve_open_again():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 5.0})
    state = apply(state, OperatorCommand(action="pause"))
    state = rise(state)
    state = apply(state, OperatorCommand(action="resume"))
    state = rise(state, start=state.levels["z1-wl1"])
    assert of_type(SendCommand, state.last_effects)


def test_a_guardian_outage_blocks_the_water_valve():
    state = feed_tick(state_with_registry("z1-wl1"), {"z1-wl1": 5.0})
    state = apply(state, TimerFired("guardian_hb", "guardian_heartbeat"))
    state = rise(state, verdict=None)
    assert of_type(SendCommand, state.last_effects) == []
    assert all(r.status is ActionStatus.REFUSED for r in state.actions.values())


# ----- transition_mode on its own -----


def test_transition_mode_to_the_same_mode_does_nothing():
    state = fresh()
    assert modes.transition_mode(state, "noop", BrainMode.WARMUP) == []


def test_transition_mode_names_the_previous_mode_and_the_cause():
    state = heartbeat(state_with_registry("z1-wl1"))
    effects = modes.transition_mode(state, "drill", BrainMode.PAUSED)
    data = of_type(Publish, effects)[0].event["data"]
    assert data["mode"] == BrainMode.PAUSED and data["from"] == BrainMode.NORMAL
    assert data["cause"] == "drill" and data["reason"] == "mode_changed"


def test_step_returns_the_state_it_was_given():
    state = fresh()
    returned, _ = step(state, OperatorCommand(action="pause"))
    assert returned is state
