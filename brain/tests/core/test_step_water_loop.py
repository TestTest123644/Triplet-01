"""The water loop end to end through `step`: reading, verdict, command, result, verification."""

import json
from pathlib import Path

import jsonschema
import pytest

from city_brain.core.events import (
    CommandResult,
    Publish,
    SendCommand,
    TimerFired,
    VerdictReceived,
)
from city_brain.core.step import step
from city_brain.domain.types import ActionStatus, DeviceStatus, TrustState
from tests.helpers_coordinator import (
    CFG,
    apply,
    feed_channel,
    feed_falling_channel,
    feed_rising_channel,
    feed_tick,
    of_type,
    only,
    state_with_registry,
    verdict_for,
)

DEVICE = "z1-wl1"
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "brain-event.v1.schema.json"
BRAIN_EVENT_SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def published(effects, kind):
    return [e.event for e in of_type(Publish, effects) if e.event["type"] == f"tn.tsyp.brain.{kind}.v1"]


def holds(effects):
    return [e for e in published(effects, "decision") if e["data"]["outcome"] == "hold"]


def hold_reasons(effects):
    return [e["data"]["incident"]["hold_reason"] for e in holds(effects)]


def enough_readings(s=None, level=5.0, **tick_fields):
    """Fill the evidence buffer, so only the reason under test can hold."""
    s = state_with_registry() if s is None else s
    for _ in range(CFG.policy["water"]["evidence_count"]):
        s = feed_tick(s, {DEVICE: level}, **tick_fields)
    s.last_effects = []
    return s


def set_valve_state(s, valve):
    device = s.registry[DEVICE]
    actuator = device.actuator.model_copy(update={"current_state": valve})
    s.registry[DEVICE] = device.model_copy(update={"actuator": actuator})


def test_happy_path_open_then_verified():
    s = state_with_registry()
    s = feed_rising_channel(s, DEVICE, until_level=6.05, verdict=TrustState.TRUSTED)
    send = only(SendCommand, s.last_effects)
    assert (send.target, send.state) == (DEVICE, "open")
    assert send.capability == "valve" and send.idempotency_key == f"brain-0-{send.action_id}"
    s, _ = step(s, CommandResult(send.action_id, "ACCEPTED", {"status": "ok"}))
    assert s.registry[DEVICE].actuator.current_state == "open"
    s = feed_falling_channel(s, DEVICE, seconds=3.5)
    record = s.actions[send.action_id]
    assert record.status is ActionStatus.VERIFIED
    assert DEVICE not in s.in_flight and DEVICE not in s.suspect
    [verification] = published(s.last_effects, "verification")
    assert verification["data"]["verdict"] == "verified"
    assert verification["data"]["expected"]["threshold"] == -0.01
    assert verification["data"]["observed"]["slope"] == pytest.approx(-0.025, abs=1e-6)
    assert verification["correlationid"] == record.intent.incident_id
    assert verification["subject"] == DEVICE


def test_estimate_tracks_when_each_verdict_follows_its_reading():
    s = state_with_registry()
    level = 5.95
    while not s.actions:
        s = feed_tick(s, {DEVICE: level}, verdict=None)
        if not s.actions:
            s = apply(s, VerdictReceived(verdict_for(DEVICE, s.global_tick)))
            level = round(level + 0.002, 6)
    assert s.filters[DEVICE].level == pytest.approx(level, abs=0.01)
    [record] = s.actions.values()
    assert record.status is ActionStatus.AWAITING_TRUST
    assert [r.verdict_id is not None for r in record.intent.evidence] == [True, True, False]
    assert not of_type(SendCommand, s.last_effects)
    s = apply(s, VerdictReceived(verdict_for(DEVICE, s.global_tick)))
    assert only(SendCommand, s.last_effects).target == DEVICE
    assert record.status is ActionStatus.DISPATCHED


def test_hold_record_emitted_once_per_reason_change():
    s = state_with_registry()
    s = feed_rising_channel(s, DEVICE, until_level=6.05, verdict=None)
    [record] = s.actions.values()
    s = apply(s, TimerFired(f"verdict:{record.intent.action_id}", "verdict_wait"))
    assert record.status is ActionStatus.BLOCKED
    # The sensor freezes above the high band while the model still predicts a rise.
    frozen = s.levels[DEVICE]
    for _ in range(100):
        s = feed_tick(s, {DEVICE: frozen}, verdict=None)
    assert hold_reasons(s.last_effects) == ["INSUFFICIENT_EVIDENCE", "STUCK"]
    assert s.last_hold[DEVICE] == "STUCK"
    hold = holds(s.last_effects)[-1]
    assert hold["data"]["would_change_if"] and hold["subject"] == "zone-1"


def test_hold_cleared_then_same_reason_is_emitted_again():
    s = enough_readings()
    set_valve_state(s, "jammed")
    s = feed_tick(s, {DEVICE: 5.001})
    s = feed_tick(s, {DEVICE: 5.002})
    assert hold_reasons(s.last_effects) == ["UNKNOWN_VALVE_STATE"]
    set_valve_state(s, "closed")
    s = feed_tick(s, {DEVICE: 5.003})
    assert s.last_hold[DEVICE] is None
    set_valve_state(s, "jammed")
    s = feed_tick(s, {DEVICE: 5.004})
    assert hold_reasons(s.last_effects) == ["UNKNOWN_VALVE_STATE", "UNKNOWN_VALVE_STATE"]


def test_an_intent_ends_the_hold_so_the_same_reason_is_recorded_next_time():
    s = enough_readings(level=6.1, verdict=None)
    [record] = s.actions.values()
    s = apply(s, TimerFired(f"verdict:{record.intent.action_id}", "verdict_wait"))
    set_valve_state(s, "jammed")
    s = feed_tick(s, {DEVICE: 6.101}, verdict=None)
    assert s.last_hold[DEVICE] == "UNKNOWN_VALVE_STATE"
    set_valve_state(s, "closed")
    s = feed_tick(s, {DEVICE: 6.102}, verdict=None)
    assert s.last_hold[DEVICE] is None


def test_unregistered_channel_holds_once_and_never_acts():
    s = state_with_registry("z2-wl1")
    for _ in range(5):
        s = feed_tick(s, {DEVICE: 7.0})
    assert hold_reasons(s.last_effects) == ["INSUFFICIENT_EVIDENCE", "UNKNOWN_VALVE_STATE"]
    assert s.actions == {}


def test_one_reading_above_the_band_is_not_enough_to_act():
    s = state_with_registry()
    s = feed_tick(s, {DEVICE: 8.0})
    assert s.actions == {}
    assert hold_reasons(s.last_effects) == ["INSUFFICIENT_EVIDENCE"]
    s = feed_tick(s, {DEVICE: 8.0})
    assert s.actions == {}
    s = feed_tick(s, {DEVICE: 8.0})
    assert len(s.actions) == 1 and s.last_hold[DEVICE] is None


def test_hold_on_a_contained_first_reading_needs_no_estimate():
    s = state_with_registry()
    s = feed_tick(s, {DEVICE: 5.0}, device_status=DeviceStatus.ISOLATED)
    assert not s.filters[DEVICE].initialised
    assert hold_reasons(s.last_effects) == ["STALE"]


def test_out_of_range_reading_leaves_the_filter_unmoved():
    s = state_with_registry()
    s = feed_rising_channel(s, DEVICE, until_level=5.5)
    level, slope = s.filters[DEVICE].level, s.filters[DEVICE].observed_slope(0)
    evidence = list(s.recent[DEVICE])
    s = feed_tick(s, {DEVICE: 99.0}, flags=frozenset({"OUT_OF_RANGE"}))
    assert s.filters[DEVICE].level == pytest.approx(level, abs=0.01)
    assert s.filters[DEVICE].observed_slope(0) == slope
    assert "IMPLAUSIBLE_JUMP" not in s.filters[DEVICE].flags
    assert list(s.recent[DEVICE]) == evidence
    assert s.actions == {}


def test_recent_evidence_is_bounded_and_carries_a_verdict_seen_first():
    count = CFG.policy["water"]["evidence_count"]
    s = state_with_registry()
    for n in range(count + 2):
        s = feed_tick(s, {DEVICE: 5.0 + n * 0.002}, verdict_first=True)
    assert len(s.recent[DEVICE]) == count
    assert all(r.verdict_id is not None and r.weight == 1.0 for r in s.recent[DEVICE])


def test_expired_untrusted_verdict_seen_first_leaves_the_filter_unmoved():
    s = state_with_registry()
    s = feed_rising_channel(s, DEVICE, until_level=5.5)
    level, slope = s.filters[DEVICE].level, s.filters[DEVICE].observed_slope(0)
    seq = s.global_tick + 1
    expired = verdict_for(DEVICE, seq, TrustState.UNTRUSTED, trust=0.0, expires_at=s.sim_now)
    s = apply(s, VerdictReceived(expired))
    s = feed_tick(s, {DEVICE: s.levels[DEVICE] + 0.01}, verdict=None)
    assert "IMPLAUSIBLE_JUMP" not in s.filters[DEVICE].flags
    assert s.filters[DEVICE].level == pytest.approx(level, abs=0.005)
    assert s.filters[DEVICE].observed_slope(0) == slope


def test_commanded_ratio_drives_the_filter():
    s = state_with_registry(valve="ratio:40")
    s = feed_tick(s, {DEVICE: 5.0})
    s = feed_tick(s, {DEVICE: 5.0})
    b = CFG.params["filter"]["b_prior"]
    assert s.filters[DEVICE].rate == pytest.approx(s.filters[DEVICE].inflow - b * 0.4)


@pytest.mark.parametrize("valve", ["ratio:300", "ratio:", "jammed"])
def test_unparseable_valve_state_is_no_drain_and_a_hold(valve):
    s = enough_readings(state_with_registry(valve=valve))
    assert s.filters[DEVICE].rate == pytest.approx(s.filters[DEVICE].inflow)
    assert s.last_hold[DEVICE] == "UNKNOWN_VALVE_STATE"


def test_below_low_since_set_while_open_and_cleared_above_the_band():
    s = state_with_registry(valve="open")
    s = feed_tick(s, {DEVICE: 3.1})
    s = feed_channel(s, DEVICE, seconds=3.0, rate=-0.045)
    crossed = s.below_low_since[DEVICE]
    assert crossed is not None
    s = feed_channel(s, DEVICE, seconds=0.5, rate=-0.045)
    assert s.below_low_since[DEVICE] == crossed
    s = feed_channel(s, DEVICE, seconds=3.0, rate=0.045)
    assert s.below_low_since[DEVICE] is None


def test_below_low_since_stays_none_with_the_valve_closed():
    s = state_with_registry()
    s = feed_tick(s, {DEVICE: 2.5})
    s = feed_channel(s, DEVICE, seconds=1.0, rate=0.001)
    assert s.below_low_since.get(DEVICE) is None


def test_full_cycle_open_verified_then_close_verified():
    s = state_with_registry()
    s = feed_rising_channel(s, DEVICE, until_level=6.05)
    opened = only(SendCommand, s.last_effects)
    s = apply(s, CommandResult(opened.action_id, "ACCEPTED", {}))
    s = feed_falling_channel(s, DEVICE, seconds=3.5)
    s = feed_channel(s, DEVICE, seconds=125.0, rate=-0.025)
    assert s.filters[DEVICE].level < 3.0
    s = feed_channel(s, DEVICE, seconds=6.0, rate=-0.025)
    closing = of_type(SendCommand, s.last_effects)[1]
    assert closing.state == "closed" and closing.action_id != opened.action_id
    s = apply(s, CommandResult(closing.action_id, "ACCEPTED", {}))
    s = feed_channel(s, DEVICE, seconds=5.5, rate=0.02)
    assert s.actions[closing.action_id].status is ActionStatus.VERIFIED
    assert s.registry[DEVICE].actuator.current_state == "closed"


def scripted_run():
    s = state_with_registry("z1-wl1", "z2-wl1")
    for n in range(120):
        s = feed_tick(s, {"z1-wl1": 5.9 + 0.002 * n, "z2-wl1": 5.9 + 0.0025 * n})
    for send in of_type(SendCommand, s.last_effects):
        s = apply(s, CommandResult(send.action_id, "ACCEPTED", {"status": "ok"}))
    for n in range(50):
        s = feed_tick(s, {"z1-wl1": 6.1 - 0.0025 * n, "z2-wl1": 6.2 + 0.002 * n})
    return s.last_effects


def test_same_events_give_same_effects():
    first, second = scripted_run(), scripted_run()
    assert len(of_type(SendCommand, first)) == 2
    assert first == second


def test_every_published_event_matches_the_schema():
    for effect in of_type(Publish, scripted_run()):
        jsonschema.validate(effect.event, BRAIN_EVENT_SCHEMA)
