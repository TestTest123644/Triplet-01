from datetime import timedelta

import pytest

from city_brain.core.shield import ShieldContext, TokenBucket, evaluate
from city_brain.domain.types import (
    ActionIntent, ActuatorCapability, CanonicalUnit, DeviceProfile, DeviceStatus,
    ExpectedEffect, RiskClass, SensorKind,
)
from tests.factories import T0

ALLOW = {"valve": frozenset({"open", "closed", "ratio"})}
POLICY = {"shield": {"cooldown_s": {"valve": 5.0}, "rate_limit": {"burst": 8, "per_s": 2.0}}}


def profile(device_id, current="closed", status=DeviceStatus.OK):
    return DeviceProfile(
        device_id=device_id, zone_id="zone-1", kind=SensorKind.WATER_LEVEL, unit=CanonicalUnit.M,
        min_val=0.0, max_val=10.0, status=status,
        actuator=ActuatorCapability(capability="valve", target=device_id,
                                    states=frozenset({"open", "closed"}), current_state=current),
    )


def intent(target="z1-wl1", state="open", capability="valve", action_id="a-1", capability_state=None):
    return ActionIntent(
        action_id=action_id, incident_id="i-1", plan_version=1, capability=capability,
        target=target, state=state, risk_class=RiskClass.R1,
        expected_effect=ExpectedEffect(channel="c", metric="rate", comparator="<=",
                                       threshold=-0.01, window_s=3.0),
        evidence=(), rationale="r", alternatives=(),
    )


def open_intent(target="z1-wl1"):
    return intent(target=target)


def ctx(status=None, in_flight=(), cycle=(), last=None, bucket=None, suspect=(), current="closed"):
    status = status or {}
    registry = {t: profile(t, current, status.get(t, DeviceStatus.OK))
                for t in ("z1-wl1", "z2-wl1")}
    return ShieldContext(
        registry=registry, in_flight=set(in_flight), cycle_intents=tuple(cycle),
        last_command_at=last or {}, bucket=bucket or TokenBucket(burst=8, per_s=2.0),
        suspect=set(suspect), now=T0,
    )


def run(i=None, **kw):
    return evaluate(i or intent(), ctx(**kw), ALLOW, POLICY)


def test_clean_intent_passes():
    r = run()
    assert r.passed and r.refusals == () and len(r.checks) == 9
    assert all(c.passed and c.code is None for c in r.checks)


@pytest.mark.parametrize("state", ["half", "foo", "ratio:101", "ratio:-1", "ratio:07", "ratio:", "ratio:5.0", "ratio"])
def test_state_not_allowed(state):
    r = run(intent(state=state))
    assert not r.passed and "STATE_NOT_ALLOWED" in r.refusals


@pytest.mark.parametrize("state", ["ratio:0", "ratio:50", "ratio:100"])
def test_ratio_in_range_allowed(state):
    assert "STATE_NOT_ALLOWED" not in run(intent(state=state)).refusals


def test_unknown_capability_not_allowed():
    r = run(intent(capability="signal"))
    assert "STATE_NOT_ALLOWED" in r.refusals


def test_unknown_target():
    r = run(intent(target="nope"))
    assert not r.passed and "UNKNOWN_TARGET" in r.refusals
    assert len(r.checks) == 9


def test_suspect_target():
    r = run(suspect={"z1-wl1"})
    assert not r.passed and "ACTUATOR_SUSPECT" in r.refusals


def test_contained_target_refused():
    r = evaluate(open_intent("z2-wl1"), ctx(status={"z2-wl1": DeviceStatus.QUARANTINED}), ALLOW, POLICY)
    assert not r.passed and "TARGET_CONTAINED" in r.refusals


def test_no_op():
    r = run(current="open")
    assert not r.passed and "NO_OP" in r.refusals


def test_in_flight():
    r = run(in_flight={"z1-wl1"})
    assert not r.passed and "IN_FLIGHT" in r.refusals


def test_conflict_other_intent_different_state_same_target():
    other = intent(state="closed", action_id="a-2")
    r = run(cycle=[other])
    assert not r.passed and "CONFLICT" in r.refusals


def test_no_conflict_for_self_same_state_or_other_target():
    me = intent()
    others = [me, intent(action_id="a-2"), intent(target="z2-wl1", state="closed", action_id="a-3")]
    assert "CONFLICT" not in run(cycle=others).refusals


def test_cooldown_active_and_elapsed():
    assert "COOLDOWN" in run(last={"z1-wl1": T0 - timedelta(seconds=4.9)}).refusals
    assert "COOLDOWN" not in run(last={"z1-wl1": T0 - timedelta(seconds=5.0)}).refusals


def test_cooldown_unconfigured_capability_fails_closed():
    allow = {"signal": frozenset({"boost"})}
    r = evaluate(intent(capability="signal", state="boost"), ctx(), allow, POLICY)
    assert not r.passed and "COOLDOWN" in r.refusals


def test_rate_limit_peeks_without_taking():
    b = TokenBucket(burst=1, per_s=1.0)
    r = run(bucket=b)
    assert r.passed
    assert b.peek(T0)
    b.take(T0)
    r = run(bucket=b)
    assert not r.passed and "RATE_LIMIT" in r.refusals


def test_all_checks_reported_even_after_failure():
    r = evaluate(intent(state="half"), ctx(in_flight={"z1-wl1"}), ALLOW, POLICY)
    assert {"STATE_NOT_ALLOWED", "IN_FLIGHT"} <= set(r.refusals) and len(r.checks) == 9


def test_refusals_follow_check_order():
    r = run(intent(state="half"), in_flight={"z1-wl1"}, suspect={"z1-wl1"})
    assert r.refusals == ("STATE_NOT_ALLOWED", "ACTUATOR_SUSPECT", "IN_FLIGHT")


def test_burst_of_eight_then_rate_limited():
    b = TokenBucket(burst=8, per_s=2.0)
    assert [b.take(T0) for _ in range(9)] == [True] * 8 + [False]
    assert b.take(T0 + timedelta(seconds=0.5))


def test_bucket_caps_at_burst_and_ignores_time_going_back():
    b = TokenBucket(burst=2, per_s=2.0)
    later = T0 + timedelta(seconds=100)
    assert [b.take(later) for _ in range(3)] == [True, True, False]
    assert not b.take(T0)
