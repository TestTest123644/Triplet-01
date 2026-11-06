"""Whatever order events arrive in, a DISPATCHED event proves its own trust requirement."""

from datetime import timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from city_brain.core.events import CommandResult, Publish, SendCommand, TimerFired, VerdictReceived
from city_brain.domain.types import DeviceStatus, RiskClass, TrustState
from tests.helpers_coordinator import (
    apply,
    feed_tick,
    of_type,
    state_with_registry,
    verdict_for,
)

DEVICE = "z1-wl1"
COMMANDABLE = ("TRUSTED", "DOUBTFUL")
HIGH = 6.5  # valve closed: plans an open (R1)
LOW = 2.5  # valve open: plans a close (R2)

readings = st.tuples(st.just("reading"), st.sampled_from(list(DeviceStatus)))
verdicts = st.tuples(st.just("verdict"), st.sampled_from(list(TrustState)), st.integers(0, 6), st.booleans())
timers = st.just(("timer",))
results = st.tuples(st.just("result"), st.sampled_from(["ACCEPTED", "REJECTED", "UNKNOWN"]))
statuses = st.tuples(st.just("status"), st.sampled_from(list(DeviceStatus)))
ops = st.lists(st.one_of(readings, readings, readings, verdicts, verdicts, timers, results, statuses), max_size=60)


def assert_evidence_satisfies_class(event):
    data = event["data"]
    evidence = data["evidence"]
    assert evidence
    assert all(r["verdict_id"] is not None and r["weight"] > 0 for r in evidence)
    states = [r["trust_state"] for r in evidence]
    if data["risk_class"] == RiskClass.R1.value:
        assert all(s in COMMANDABLE for s in states) and "TRUSTED" in states
    else:
        assert data["risk_class"] == RiskClass.R2.value
        assert all(s == "TRUSTED" for s in states)


def run(plan, valve):
    """Apply `plan` after a primed channel; returns the DISPATCHED events."""
    level = HIGH if valve == "closed" else LOW
    s = state_with_registry(valve=valve)
    for _ in range(3):
        s = feed_tick(s, {DEVICE: level}, verdict=None)
    s = feed_tick(s, {DEVICE: level}, verdict=None, seq=s.global_tick + 60)  # past the sustain time
    sent = [1, 2, 3, s.global_tick]
    for op in plan:
        if op[0] == "reading":
            s = feed_tick(s, {DEVICE: level}, verdict=None, device_status=op[1])
            sent.append(s.global_tick)
        elif op[0] == "verdict":
            _, trust, back, expired = op
            seq = sent[-1 - back % len(sent)]
            at = s.sim_now - timedelta(seconds=1) if expired else s.sim_now + timedelta(seconds=30)
            s = apply(s, VerdictReceived(verdict_for(DEVICE, seq, trust, expires_at=at, issued_at=s.sim_now)))
        elif op[0] == "timer":
            for action_id in sorted(s.actions):
                s = apply(s, TimerFired(f"verdict:{action_id}", "verdict_wait"))
        elif op[0] == "result":
            for send in of_type(SendCommand, s.last_effects):
                s = apply(s, CommandResult(send.action_id, op[1], {}))
            s.last_effects = [e for e in s.last_effects if not isinstance(e, SendCommand)]
        else:
            device = s.registry[DEVICE]
            s.registry[DEVICE] = device.model_copy(update={"status": op[1]})
    return [e.event for e in of_type(Publish, s.last_effects)
            if e.event["type"].endswith(".command.v1") and e.event["data"]["status"] == "DISPATCHED"]


@settings(max_examples=120, deadline=None)
@given(plan=ops, valve=st.sampled_from(["closed", "open"]))
def test_every_dispatched_command_satisfies_its_class(plan, valve):
    for event in run(plan, valve):
        assert_evidence_satisfies_class(event)


def test_the_harness_reaches_dispatch_for_both_classes():
    """Guards the property against passing vacuously."""
    plan = [("verdict", TrustState.TRUSTED, back, False) for back in range(4)]
    assert run(plan, "closed")[0]["data"]["risk_class"] == RiskClass.R1.value
    assert run(plan, "open")[0]["data"]["risk_class"] == RiskClass.R2.value


