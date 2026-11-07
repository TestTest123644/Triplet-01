"""The containment dilemma: a quarantined drain valve and rising water (spec 8.7, T09)."""

from datetime import timedelta

import pytest

from city_brain.core.events import (
    CancelTimer,
    GuardianEventReceived,
    OperatorCommand,
    Publish,
    SendCommand,
    StartTimer,
    TimerFired,
)
from city_brain.domain.types import (
    ActionStatus,
    DeviceStatus,
    GuardianContainment,
    RiskClass,
    TrustState,
)
from tests.helpers_coordinator import (
    CFG,
    PERIOD_S,
    RISE_RATE,
    apply,
    feed_tick,
    of_type,
    state_with_registry,
)

APPROVAL_WAIT_S = CFG.policy["approvals"]["wait_s"]
DEVICE = "z1-wl1"


def quarantine(state, device_id=DEVICE, action="quarantined"):
    return apply(state, GuardianEventReceived(GuardianContainment(
        device_id=device_id, action=action, effective_at=state.sim_now,
        reasons=frozenset({"CROSS_SENSOR_CONFLICT"}), issued_at=state.sim_now,
    )))


def rise(state, start=5.5, ticks=600, rate=RISE_RATE, **kw):
    level = start
    for _ in range(ticks):
        state = feed_tick(state, {DEVICE: level}, **kw)
        level = round(level + rate * PERIOD_S, 6)
        if state.actions:
            break
    return state


def only_action(state):
    assert len(state.actions) == 1, [(a.intent.action_id, a.status) for a in state.actions.values()]
    return next(iter(state.actions.values()))


def alerts(state, code=None):
    out = [
        e.event["data"] for e in state.last_effects
        if isinstance(e, Publish) and e.event["type"] == "tn.tsyp.brain.alert.v1"
    ]
    return [a for a in out if code is None or a["code"] == code]


def holds(state):
    return [
        e.event["data"]["incident"]["hold_reason"]
        for e in state.last_effects
        if isinstance(e, Publish) and e.event["type"] == "tn.tsyp.brain.decision.v1"
        and e.event["data"]["outcome"] == "hold"
    ]


def rising_quarantined_state(start=5.5, rate=RISE_RATE, **kw):
    """A quarantined tank whose level is climbing towards the critical band."""
    state = feed_tick(state_with_registry(DEVICE, valve="closed"), {DEVICE: start - 0.1})
    state = quarantine(state)
    return rise(state, start=start, rate=rate, **kw)


# ----- the dilemma -----


def test_a_quarantined_tank_below_the_critical_forecast_is_held():
    state = feed_tick(state_with_registry(DEVICE, valve="closed"), {DEVICE: 6.2})
    state = quarantine(state)
    state = feed_tick(state, {DEVICE: 6.2})
    state = feed_tick(state, {DEVICE: 6.2})
    assert state.actions == {}
    assert holds(state)[-1] == "TARGET_CONTAINED"


def test_rising_water_on_a_quarantined_tank_asks_the_operator():
    state = rising_quarantined_state()
    record = only_action(state)
    assert record.intent.risk_class is RiskClass.R3
    assert record.intent.containment_override is True
    assert record.status is ActionStatus.AWAITING_APPROVAL
    assert record.intent.state == "open", "only the drain direction is worth asking about"


def test_nothing_is_sent_while_the_operator_has_not_answered():
    state = rising_quarantined_state()
    assert of_type(SendCommand, state.last_effects) == []


def test_the_operator_is_alerted_and_the_wait_is_timed():
    state = rising_quarantined_state()
    record = only_action(state)
    alert = alerts(state, "APPROVAL_REQUIRED")
    assert alert and alert[-1]["audience"] == "operator" and alert[-1]["severity"] == "critical"
    assert alert[-1]["refs"]["action_id"] == record.intent.action_id
    timers = [t for t in of_type(StartTimer, state.last_effects) if t.kind == "approval_wait"]
    assert timers[-1] == StartTimer(
        f"approval:{record.intent.action_id}", "approval_wait", APPROVAL_WAIT_S)


def test_the_pending_request_is_recorded_for_the_operator_api():
    state = rising_quarantined_state()
    record = only_action(state)
    assert set(state.pending_approvals) == {record.intent.action_id}
    assert state.pending_approvals[record.intent.action_id] == state.sim_now


def test_the_request_cites_the_last_trusted_readings():
    state = rising_quarantined_state()
    record = only_action(state)
    assert record.intent.evidence, "an approval request must say what it is based on"
    assert all(ref.trust_state is TrustState.TRUSTED for ref in record.intent.evidence)


def test_approval_dispatches_at_once():
    state = rising_quarantined_state()
    record = only_action(state)
    state = apply(state, OperatorCommand(action="approve", action_id=record.intent.action_id))
    assert record.status is ActionStatus.DISPATCHED
    sent = of_type(SendCommand, state.last_effects)
    assert [(c.capability, c.target, c.state) for c in sent] == [("valve", DEVICE, "open")]
    assert CancelTimer(f"approval:{record.intent.action_id}") in state.last_effects
    assert state.pending_approvals == {}


def test_denial_blocks_without_sending_anything():
    state = rising_quarantined_state()
    record = only_action(state)
    state = apply(state, OperatorCommand(action="deny", action_id=record.intent.action_id))
    assert record.status is ActionStatus.BLOCKED and "OPERATOR_DENIED" in record.reasons
    assert of_type(SendCommand, state.last_effects) == []
    assert state.pending_approvals == {}


def test_the_timeout_blocks_without_sending_anything():
    state = rising_quarantined_state()
    record = only_action(state)
    state = apply(state, TimerFired(f"approval:{record.intent.action_id}", "approval_wait"))
    assert record.status is ActionStatus.BLOCKED and "APPROVAL_TIMEOUT" in record.reasons
    assert of_type(SendCommand, state.last_effects) == []
    assert state.pending_approvals == {}


def test_an_approval_for_an_unknown_action_does_nothing():
    state = rising_quarantined_state()
    record = only_action(state)
    state = apply(state, OperatorCommand(action="approve", action_id="act-nobody"))
    assert record.status is ActionStatus.AWAITING_APPROVAL
    assert of_type(SendCommand, state.last_effects) == []


def test_an_approval_without_an_action_id_does_nothing():
    state = rising_quarantined_state()
    record = only_action(state)
    state = apply(state, OperatorCommand(action="approve"))
    assert record.status is ActionStatus.AWAITING_APPROVAL


def test_approval_cannot_revive_a_denied_action():
    state = rising_quarantined_state()
    record = only_action(state)
    state = apply(state, OperatorCommand(action="deny", action_id=record.intent.action_id))
    state = apply(state, OperatorCommand(action="approve", action_id=record.intent.action_id))
    assert record.status is ActionStatus.BLOCKED
    assert of_type(SendCommand, state.last_effects) == []


def test_a_pending_approval_keeps_the_target_from_being_replanned():
    state = rising_quarantined_state()
    before = only_action(state).intent.action_id
    state = rise(state, start=8.0, ticks=100)
    assert set(state.actions) == {before}


def test_a_release_returns_the_valve_to_ordinary_control():
    state = feed_tick(state_with_registry(DEVICE, valve="closed"), {DEVICE: 5.4})
    state = quarantine(state)
    state = quarantine(state, action="released")
    assert state.registry[DEVICE].status is DeviceStatus.OK
    state = rise(state, start=5.5)
    record = only_action(state)
    assert record.intent.risk_class is RiskClass.R1
    assert record.intent.containment_override is False


def test_an_isolated_tank_is_the_same_dilemma():
    state = feed_tick(state_with_registry(DEVICE, valve="closed"), {DEVICE: 5.4})
    state = quarantine(state, action="isolated")
    state = rise(state, start=5.5)
    assert only_action(state).status is ActionStatus.AWAITING_APPROVAL


# ----- the Shield's side of it -----


def test_the_shield_refuses_a_contained_target_without_the_override():
    from city_brain.core.shield import ShieldContext, TokenBucket, evaluate
    from tests.factories import T0

    state = feed_tick(state_with_registry(DEVICE, valve="closed"), {DEVICE: 5.4})
    state = quarantine(state)
    plain = rising_quarantined_state()
    intent = only_action(plain).intent
    context = ShieldContext(
        registry=state.registry, in_flight=set(), cycle_intents=(), last_command_at={},
        bucket=TokenBucket(8, 2.0), suspect=set(), now=T0, zones=state.zones,
    )
    without = intent.model_copy(update={"risk_class": RiskClass.R1, "containment_override": False})
    assert "TARGET_CONTAINED" in evaluate(without, context, state.allowlist, state.policy).refusals
    with_override = intent
    assert "TARGET_CONTAINED" not in evaluate(
        with_override, context, state.allowlist, state.policy).refusals


def test_the_override_is_refused_when_the_risk_class_is_not_r3():
    from city_brain.core.shield import ShieldContext, TokenBucket, evaluate
    from tests.factories import T0

    state = rising_quarantined_state()
    intent = only_action(state).intent.model_copy(update={"risk_class": RiskClass.R2})
    context = ShieldContext(
        registry=state.registry, in_flight=set(), cycle_intents=(), last_command_at={},
        bucket=TokenBucket(8, 2.0), suspect=set(), now=state.sim_now, zones=state.zones,
    )
    assert "TARGET_CONTAINED" in evaluate(intent, context, state.allowlist, state.policy).refusals


# WARMUP and TWIN_DEGRADED cannot persist while readings and verdicts keep arriving: the
# mode machine recovers out of both, which is what its own tests check.
@pytest.mark.parametrize("mode_name", ["PAUSED", "GUARDIAN_DEGRADED"])
def test_no_mode_but_normal_may_ask_for_an_override(mode_name):
    from city_brain.domain.types import BrainMode

    state = feed_tick(state_with_registry(DEVICE, valve="closed"), {DEVICE: 5.4})
    state = quarantine(state)
    state.mode = BrainMode[mode_name]
    state = rise(state, start=5.5)
    record = only_action(state)
    assert record.status is ActionStatus.REFUSED and "MODE_FORBIDS" in record.shield.refusals
    assert state.pending_approvals == {}
