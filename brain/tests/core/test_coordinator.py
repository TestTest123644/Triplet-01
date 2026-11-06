"""Action lifecycle: trust gate, dispatch, results, verification, epochs (spec 8.6, 8.8)."""

from datetime import timedelta

import pytest

from city_brain.core.coordinator import ActionRecord, dispatch
from city_brain.core.events import (
    CancelTimer,
    CommandResult,
    Publish,
    SendCommand,
    StartTimer,
    TimerFired,
    VerdictReceived,
)
from city_brain.domain.types import (
    ActionStatus,
    CanonicalUnit,
    DeviceStatus,
    SensorKind,
    StateKind,
    TrustState,
)
from tests.factories import T0
from tests.helpers_coordinator import (
    CFG,
    PERIOD_S,
    apply,
    feed_channel,
    feed_falling_channel,
    feed_rising_channel,
    feed_tick,
    of_type,
    only,
    seq_of,
    state_with_registry,
    verdict_for,
)

DEVICE = "z1-wl1"
COOLDOWN_S = CFG.policy["shield"]["cooldown_s"]["valve"]
BURST = CFG.policy["shield"]["rate_limit"]["burst"]


def alerts(effects, code):
    return [e.event["data"]["audience"] for e in of_type(Publish, effects)
            if e.event["type"].endswith(".alert.v1") and e.event["data"]["code"] == code]


def commands(effects, status):
    return [e.event for e in of_type(Publish, effects)
            if e.event["type"].endswith(".command.v1") and e.event["data"]["status"] == status]


def act_decisions(effects):
    return [e.event for e in of_type(Publish, effects)
            if e.event["type"].endswith(".decision.v1") and e.event["data"]["outcome"] == "act"]


def records(s, status):
    return [s.actions[a] for a in sorted(s.actions) if s.actions[a].status is status]


def awaiting_open():
    """A rising channel with no verdicts: one open action waits for trust."""
    s = state_with_registry()
    s = feed_rising_channel(s, DEVICE, until_level=6.05, verdict=None)
    return s, records(s, ActionStatus.AWAITING_TRUST)[0]


def dispatched_open():
    s = state_with_registry()
    s = feed_rising_channel(s, DEVICE, until_level=6.05)
    return s, only(SendCommand, s.last_effects)


def verifying_open():
    s, send = dispatched_open()
    return apply(s, CommandResult(send.action_id, "ACCEPTED", {"status": "ok"})), send


def expire_timer(s, record):
    return apply(s, TimerFired(f"verdict:{record.intent.action_id}", "verdict_wait"))


def trust_all(s, record, state=TrustState.TRUSTED, **fields):
    for ref in record.intent.evidence:
        s = apply(s, VerdictReceived(verdict_for(DEVICE, seq_of(ref), state, **fields)))
    return s


def close_awaiting_newest_verdict():
    """Valve open, level long below the low band: a close (R2) waits on its newest reading."""
    s = state_with_registry(valve="open")
    level = 2.9
    while not s.actions:
        s = feed_tick(s, {DEVICE: level}, verdict=None)
        if not s.actions:
            s = apply(s, VerdictReceived(verdict_for(DEVICE, s.global_tick)))
        level = round(level - 0.0045, 6)
    return s, records(s, ActionStatus.AWAITING_TRUST)[0]


# ----- trust gate -----


def test_awaiting_trust_starts_the_verdict_timer():
    s, record = awaiting_open()
    action_id = record.intent.action_id
    assert only(StartTimer, s.last_effects) == StartTimer(f"verdict:{action_id}", "verdict_wait", 2.0)
    [decision] = act_decisions(s.last_effects)
    assert decision["actionid"] == action_id
    assert decision["correlationid"] == record.intent.incident_id and decision["subject"] == "zone-1"
    assert len(commands(s.last_effects, "AWAITING_TRUST")) == 1


def test_missing_verdict_times_out_to_blocked():
    s, record = awaiting_open()
    s = expire_timer(s, record)
    assert record.status is ActionStatus.BLOCKED and record.reasons == ("VERDICT_TIMEOUT",)
    assert not of_type(SendCommand, s.last_effects)
    assert all(r.state_kind is StateKind.UNKNOWN for r in record.intent.evidence)
    assert commands(s.last_effects, "BLOCKED")[0]["data"]["reasons"] == ["VERDICT_TIMEOUT"]


def test_timer_after_a_decision_changes_nothing():
    s, send = dispatched_open()
    n = len(s.last_effects)
    s = apply(s, TimerFired(f"verdict:{send.action_id}", "verdict_wait"))
    assert s.actions[send.action_id].status is ActionStatus.DISPATCHED
    assert len(s.last_effects) == n


def test_unknown_timer_is_ignored():
    s, _ = awaiting_open()
    n = len(s.last_effects)
    s = apply(s, TimerFired("verdict:nope", "verdict_wait"))
    s = apply(s, TimerFired("other", "other_kind"))
    assert len(s.last_effects) == n


def test_verdicts_decide_and_cancel_the_timer():
    s, record = awaiting_open()
    s = trust_all(s, record)
    action_id = record.intent.action_id
    assert only(CancelTimer, s.last_effects) == CancelTimer(f"verdict:{action_id}")
    assert only(SendCommand, s.last_effects).action_id == action_id
    assert record.status is ActionStatus.DISPATCHED
    assert DEVICE in s.in_flight and s.last_command_at[DEVICE] == s.sim_now
    assert commands(s.last_effects, "DISPATCHED")[0]["data"]["tool"] is None


def test_untrusted_cited_reading_blocks_at_once():
    s, record = awaiting_open()
    seq = seq_of(record.intent.evidence[0])
    s = apply(s, VerdictReceived(verdict_for(DEVICE, seq, TrustState.UNTRUSTED, trust=0.0)))
    assert record.status is ActionStatus.BLOCKED and "TRUST_INSUFFICIENT" in record.reasons
    assert only(CancelTimer, s.last_effects)
    assert not of_type(SendCommand, s.last_effects)


def test_all_doubtful_never_opens():
    s, record = awaiting_open()
    s = trust_all(s, record, TrustState.DOUBTFUL, trust=0.5)
    assert record.status is ActionStatus.BLOCKED and record.reasons == ("TRUST_INSUFFICIENT",)
    assert not of_type(SendCommand, s.last_effects)


def test_doubtful_with_one_trusted_opens():
    s, record = awaiting_open()
    first, *rest = record.intent.evidence
    for ref in rest:
        s = apply(s, VerdictReceived(verdict_for(DEVICE, seq_of(ref), TrustState.DOUBTFUL, trust=0.5)))
    assert record.status is ActionStatus.AWAITING_TRUST
    s = apply(s, VerdictReceived(verdict_for(DEVICE, seq_of(first))))
    assert record.status is ActionStatus.DISPATCHED


def test_downgrade_between_decision_and_dispatch_blocks():
    s, record = close_awaiting_newest_verdict()
    assert record.intent.state == "closed"
    assert [r.verdict_id is not None for r in record.intent.evidence] == [True, True, False]
    seq = seq_of(record.intent.evidence[0])
    newer = verdict_for(DEVICE, seq, TrustState.DOUBTFUL, trust=0.5, verdict_id="v-newer",
                        issued_at=T0 + timedelta(seconds=seq * PERIOD_S + 1))
    s = apply(s, VerdictReceived(newer))
    assert record.status is ActionStatus.BLOCKED and "TRUST_INSUFFICIENT" in record.reasons
    assert not of_type(SendCommand, s.last_effects)


def test_dispatch_rechecks_trust_with_freshly_resolved_refs():
    s, record = awaiting_open()
    s = trust_all(s, record)
    assert record.status is ActionStatus.DISPATCHED
    n = len(of_type(SendCommand, s.last_effects))
    record.status = ActionStatus.AWAITING_TRUST
    s.ledger.add(verdict_for(DEVICE, seq_of(record.intent.evidence[0]), TrustState.UNTRUSTED, trust=0.0,
                             issued_at=s.sim_now + timedelta(seconds=1)))
    effects = dispatch(s, record)
    assert record.status is ActionStatus.BLOCKED and "TRUST_INSUFFICIENT" in record.reasons
    assert len(of_type(SendCommand, s.last_effects + effects)) == n


def test_verdict_for_an_uncited_reading_leaves_awaiting_actions_alone():
    s, record = awaiting_open()
    for ref in record.intent.evidence:
        s.ledger.add(verdict_for(DEVICE, seq_of(ref)))
    s = apply(s, VerdictReceived(verdict_for(DEVICE, 1)))
    assert record.status is ActionStatus.AWAITING_TRUST


def test_late_verdict_after_blocked_does_not_revive_the_action():
    s, record = awaiting_open()
    s = expire_timer(s, record)
    s = trust_all(s, record)
    assert record.status is ActionStatus.BLOCKED
    assert not of_type(SendCommand, s.last_effects)


def test_verdict_cited_then_expired_blocks_with_verdict_expired():
    s = state_with_registry()
    level = 5.97
    while not s.actions:
        s = feed_tick(s, {DEVICE: level}, verdict=None)
        if not s.actions:
            soon = s.sim_now + timedelta(seconds=0.5)
            s = apply(s, VerdictReceived(verdict_for(DEVICE, s.global_tick, expires_at=soon)))
        level = round(level + 0.004, 6)
    [record] = s.actions.values()
    assert record.intent.evidence[0].verdict_id is not None
    newest = s.global_tick
    s = feed_channel(s, DEVICE, seconds=1.0, rate=0.02, verdict=None)
    s = apply(s, VerdictReceived(verdict_for(DEVICE, newest)))
    assert record.status is ActionStatus.BLOCKED and "VERDICT_EXPIRED" in record.reasons
    assert only(CancelTimer, s.last_effects)
    assert not of_type(SendCommand, s.last_effects)


def test_fresh_identical_verdict_replacing_an_expired_one_still_re_evaluates():
    s, record = awaiting_open()
    first, second, third = (seq_of(r) for r in record.intent.evidence)
    s = apply(s, VerdictReceived(verdict_for(DEVICE, first)))
    s = apply(s, VerdictReceived(verdict_for(DEVICE, second)))
    s = apply(s, VerdictReceived(verdict_for(DEVICE, third, expires_at=s.sim_now - timedelta(seconds=1))))
    assert record.status is ActionStatus.AWAITING_TRUST
    s = apply(s, VerdictReceived(verdict_for(DEVICE, third)))
    assert record.status is ActionStatus.DISPATCHED


def test_contained_while_awaiting_fails_the_dispatch_time_check():
    s, record = awaiting_open()
    s = feed_tick(s, {DEVICE: 6.06}, verdict=None, device_status=DeviceStatus.QUARANTINED)
    assert s.registry[DEVICE].status is DeviceStatus.QUARANTINED
    s = trust_all(s, record)
    assert record.status is ActionStatus.BLOCKED and "TRUST_INSUFFICIENT" in record.reasons
    assert not of_type(SendCommand, s.last_effects)


# ----- shield and planning gates -----


def test_refused_action_is_recorded_with_its_codes():
    s = state_with_registry(allowlist={})
    s = feed_rising_channel(s, DEVICE, until_level=6.05)
    [record] = s.actions.values()
    assert isinstance(record, ActionRecord)
    assert record.status is ActionStatus.REFUSED and record.reasons == ("STATE_NOT_ALLOWED",)
    [refused] = commands(s.last_effects, "REFUSED")
    assert refused["data"]["shield"]["passed"] is False
    assert len(act_decisions(s.last_effects)) == 1
    assert not of_type(SendCommand, s.last_effects)


def test_one_active_action_per_target():
    s, _ = awaiting_open()
    s = feed_channel(s, DEVICE, seconds=1.0, rate=0.02, verdict=None)
    assert len(s.actions) == 1 and s.plan_version[DEVICE] == 1
    assert s.counters["REFUSED_REPEAT"] == 0


def test_dispatched_and_verifying_actions_stop_planning_on_their_target():
    s, send = dispatched_open()
    s = feed_channel(s, DEVICE, seconds=1.0, rate=0.02)
    assert len(s.actions) == 1
    s = apply(s, CommandResult(send.action_id, "ACCEPTED", {}))
    s = feed_channel(s, DEVICE, seconds=1.0, rate=-0.025)
    assert len(s.actions) == 1


def test_plan_version_counts_records_not_silent_refusals():
    s = state_with_registry(allowlist={})
    s = feed_rising_channel(s, DEVICE, until_level=6.05)
    assert len(s.actions) == 1 and s.plan_version[DEVICE] == 1
    assert s.counters["REFUSED_REPEAT"] > 0


def test_replanned_action_after_a_terminal_one_gets_a_new_action_id():
    s, record = awaiting_open()
    s = expire_timer(s, record)
    s = feed_channel(s, DEVICE, seconds=COOLDOWN_S + 0.2, rate=0.02, verdict=None)
    assert s.plan_version[DEVICE] == 2 and len(s.actions) == 2
    assert records(s, ActionStatus.AWAITING_TRUST)[0].intent.action_id != record.intent.action_id


def test_identical_refusal_is_silent_and_a_different_one_is_not():
    s, send = dispatched_open()
    s = apply(s, CommandResult(send.action_id, "REJECTED", {"error": "nope"}))
    # Still above the band and closed, inside the cooldown: refused once.
    s = feed_channel(s, DEVICE, seconds=1.0, rate=0.02)
    assert [c["data"]["reasons"] for c in commands(s.last_effects, "REFUSED")] == [["COOLDOWN"]]
    assert s.counters["REFUSED_REPEAT"] == 9
    s.suspect.add(DEVICE)
    s = feed_channel(s, DEVICE, seconds=0.2, rate=0.02)
    assert [c["data"]["reasons"] for c in commands(s.last_effects, "REFUSED")] == [
        ["COOLDOWN"], ["ACTUATOR_SUSPECT", "COOLDOWN"],
    ]


def test_refusal_memory_is_forgotten_once_the_shield_passes():
    s, first = dispatched_open()
    s = apply(s, CommandResult(first.action_id, "REJECTED", {}))
    s = feed_channel(s, DEVICE, seconds=COOLDOWN_S + 0.2, rate=0.02)
    second = of_type(SendCommand, s.last_effects)[1]
    s = apply(s, CommandResult(second.action_id, "REJECTED", {}))
    s = feed_channel(s, DEVICE, seconds=0.5, rate=0.02)
    assert len(commands(s.last_effects, "REFUSED")) == 2


def crossing(devices, **tick_fields):
    s = state_with_registry(*devices)
    level = 5.95
    while not s.actions:
        s = feed_tick(s, dict.fromkeys(devices, level), **tick_fields)
        level = round(level + 0.002, 6)
    return s


def test_rate_limited_valves_refuse_once_then_dispatch_as_tokens_return():
    devices = [f"z{i}-wl1" for i in range(1, BURST + 3)]
    s = crossing(devices, verdict_first=True)
    assert len(of_type(SendCommand, s.last_effects)) == BURST
    assert len(commands(s.last_effects, "REFUSED")) == 2
    level = s.levels[devices[0]]
    for _ in range(12):
        level = round(level + 0.002, 6)
        s = feed_tick(s, dict.fromkeys(devices, level), verdict_first=True)
    assert len(commands(s.last_effects, "REFUSED")) == 2
    assert s.counters["REFUSED_REPEAT"] > 0
    assert sorted(c.target for c in of_type(SendCommand, s.last_effects)) == sorted(devices)


def test_no_token_at_dispatch_blocks_and_sends_nothing():
    devices = [f"z{i}-wl1" for i in range(1, BURST + 2)]
    s = crossing(devices, verdict=None)
    assert len(records(s, ActionStatus.AWAITING_TRUST)) == len(devices)
    tick = s.global_tick
    for device in devices:
        for seq in (tick - 2, tick - 1, tick):
            s = apply(s, VerdictReceived(verdict_for(device, seq)))
    assert len(of_type(SendCommand, s.last_effects)) == BURST
    [record] = records(s, ActionStatus.BLOCKED)
    assert record.reasons == ("RATE_LIMIT",) and record.intent.target not in s.in_flight
    assert record.intent.target not in s.last_command_at


def test_blocked_target_is_retried_once_per_cooldown():
    s, record = awaiting_open()
    s = expire_timer(s, record)
    s = feed_channel(s, DEVICE, seconds=COOLDOWN_S - 0.3, rate=0.02, verdict=None)
    assert len(s.actions) == 1
    s = feed_channel(s, DEVICE, seconds=0.5, rate=0.02, verdict=None)
    assert len(s.actions) == 2
    s = feed_channel(s, DEVICE, seconds=1.0, rate=0.02, verdict=None)
    assert len(s.actions) == 2


# ----- results -----


def test_rejected_clears_in_flight_and_does_not_resend():
    s, send = dispatched_open()
    n = len(of_type(SendCommand, s.last_effects))
    s = apply(s, CommandResult(send.action_id, "REJECTED", {"error": "bad"}))
    assert s.actions[send.action_id].status is ActionStatus.REJECTED and DEVICE not in s.in_flight
    assert s.registry[DEVICE].actuator.current_state == "closed"
    assert commands(s.last_effects, "REJECTED")[0]["data"]["twin_reply"] == {"error": "bad"}
    assert len(of_type(SendCommand, s.last_effects)) == n


def test_accepted_records_the_tick_and_the_reply():
    s, send = verifying_open()
    record = s.actions[send.action_id]
    assert record.status is ActionStatus.VERIFYING
    assert record.dispatched_at == s.sim_now and record.dispatch_tick == s.global_tick
    assert DEVICE in s.in_flight
    assert commands(s.last_effects, "VERIFYING")[0]["data"]["twin_reply"] == {"status": "ok"}


def test_timeout_is_unknown_and_never_resent():
    s, send = dispatched_open()
    s = apply(s, CommandResult(send.action_id, "UNKNOWN", {}))
    assert s.actions[send.action_id].status is ActionStatus.UNKNOWN and DEVICE not in s.in_flight
    assert alerts(s.last_effects, "COMMAND_RESULT_UNKNOWN") == ["operator"]
    assert s.registry[DEVICE].actuator.current_state == "closed"
    s = feed_channel(s, DEVICE, seconds=30.0, rate=0.02)
    keys = [c.idempotency_key for c in of_type(SendCommand, s.last_effects)]
    assert keys == [send.idempotency_key]
    assert len(s.actions) == 1


def test_lost_reply_frees_the_target_only_when_the_twin_restarts():
    s, send = dispatched_open()
    s = apply(s, CommandResult(send.action_id, "UNKNOWN", {}))
    s = restart(later(s))
    record = s.actions[send.action_id]
    assert record.status is ActionStatus.UNKNOWN and "EPOCH_CHANGED" in record.reasons
    assert alerts(s.last_effects, "EPOCH_CHANGED_ACTION_UNKNOWN") == []
    s.last_effects = []
    s = feed_tick(s, {DEVICE: 5.95}, verdict_first=True)
    s = feed_rising_channel(s, DEVICE, until_level=6.1, verdict_first=True)
    assert only(SendCommand, s.last_effects).idempotency_key != send.idempotency_key


def test_results_outside_dispatched_are_counted_and_ignored():
    s, send = verifying_open()
    s = feed_falling_channel(s, DEVICE, seconds=3.5)
    assert s.actions[send.action_id].status is ActionStatus.VERIFIED
    n = len(s.last_effects)
    s = apply(s, CommandResult(send.action_id, "UNKNOWN", {}))
    s = apply(s, CommandResult("act-unknown", "ACCEPTED", {}))
    assert s.actions[send.action_id].status is ActionStatus.VERIFIED
    assert len(s.last_effects) == n and s.counters["LATE_COMMAND_RESULT"] == 2


# ----- verification -----


def test_actuator_noop_is_unverified_with_two_alerts_and_no_resend():
    s, send = verifying_open()
    s = feed_channel(s, DEVICE, seconds=3.5, rate=0.02)
    assert s.actions[send.action_id].status is ActionStatus.UNVERIFIED
    assert alerts(s.last_effects, "ACTUATOR_NO_EFFECT") == ["operator", "guardian"]
    assert DEVICE in s.suspect and DEVICE not in s.in_flight
    s = feed_channel(s, DEVICE, seconds=30.0, rate=0.02)
    assert only(SendCommand, s.last_effects) == send


def test_verification_waits_for_the_whole_window():
    s, send = verifying_open()
    s = feed_falling_channel(s, DEVICE, seconds=2.9)
    assert s.actions[send.action_id].status is ActionStatus.VERIFYING


def test_no_measurements_in_the_window_is_inconclusive_and_alerts():
    s, send = verifying_open()
    s = feed_channel(s, DEVICE, seconds=3.5, rate=-0.025, device_status=DeviceStatus.ISOLATED)
    assert s.actions[send.action_id].status is ActionStatus.UNVERIFIED
    [verification] = [e.event for e in of_type(Publish, s.last_effects)
                      if e.event["type"].endswith(".verification.v1")]
    assert verification["data"]["verdict"] == "inconclusive"
    assert verification["data"]["observed"]["slope"] is None
    assert alerts(s.last_effects, "ACTUATOR_NO_EFFECT") == ["operator", "guardian"]


def alert_messages(effects):
    return [e.event["data"]["message"] for e in of_type(Publish, effects)
            if e.event["type"].endswith(".alert.v1")]


@pytest.mark.parametrize("reading_fields", [
    {},
    {"kind": SensorKind.TRAFFIC_FLOW, "unit": CanonicalUnit.VEH_PER_MIN},
], ids=["water", "other_kind"])
def test_silent_sensor_ends_verification_on_any_other_reading(reading_fields):
    s, send = verifying_open()
    for _ in range(35):
        s = feed_tick(s, {"z2-wl1": 40.0}, verdict=None, **reading_fields)
    record = s.actions[send.action_id]
    assert record.status is ActionStatus.UNVERIFIED and DEVICE not in s.in_flight
    assert DEVICE in s.suspect
    messages = alert_messages(s.last_effects)
    assert len(messages) == 2 and all("No measurements were available" in m for m in messages)


def test_alert_for_a_measured_failure_says_no_effect():
    s, send = verifying_open()
    s = feed_channel(s, DEVICE, seconds=3.5, rate=0.02)
    assert all("no observed effect" in m for m in alert_messages(s.last_effects))


@pytest.mark.parametrize("verdict_first", [False, True])
def test_forged_falling_readings_cannot_verify_a_dead_valve(verdict_first):
    s, send = verifying_open()
    s = feed_channel(s, DEVICE, seconds=3.5, rate=-0.025, verdict=TrustState.UNTRUSTED,
                     verdict_first=verdict_first)
    record = s.actions[send.action_id]
    assert record.status is ActionStatus.UNVERIFIED
    [verification] = [e.event for e in of_type(Publish, s.last_effects)
                      if e.event["type"].endswith(".verification.v1")]
    assert verification["data"]["verdict"] == "inconclusive"
    assert DEVICE in s.suspect


def test_readings_awaiting_a_verdict_at_close_do_not_count():
    s, send = verifying_open()
    s = feed_channel(s, DEVICE, seconds=3.5, rate=-0.025, verdict=None)
    assert s.actions[send.action_id].status is ActionStatus.UNVERIFIED


def test_trusted_readings_decide_when_forgeries_are_mixed_in():
    """Dead valve: true readings keep rising, forged ones between them fall."""
    s, send = verifying_open()
    start = s.levels[DEVICE]
    for n in range(1, 36):
        forged = n % 3 == 0
        value = start + 0.002 * n - (0.015 if forged else 0.0)
        s = feed_tick(s, {DEVICE: value}, TrustState.UNTRUSTED if forged else TrustState.TRUSTED)
    record = s.actions[send.action_id]
    assert record.status is ActionStatus.UNVERIFIED
    [verification] = [e.event for e in of_type(Publish, s.last_effects)
                      if e.event["type"].endswith(".verification.v1")]
    assert verification["data"]["verdict"] == "unverified"
    assert verification["data"]["observed"]["slope"] == pytest.approx(0.02, abs=1e-6)


def test_another_channel_on_the_same_tick_does_not_affect_verification():
    other = "z2-wl1"
    s = state_with_registry(DEVICE, other)
    for n in range(120):
        s = feed_tick(s, {DEVICE: 5.8 + 0.002 * n, other: 4.0 + 0.001 * n})
    send = only(SendCommand, s.last_effects)
    s = apply(s, CommandResult(send.action_id, "ACCEPTED", {}))
    start = s.levels[DEVICE]
    for n in range(1, 36):
        seq = s.global_tick + 1
        s = feed_tick(s, {other: 4.1 + 0.001 * n}, verdict=None, seq=seq)
        s = feed_tick(s, {DEVICE: start - 0.0025 * n}, seq=seq)
    assert s.actions[send.action_id].status is ActionStatus.VERIFIED


def test_doubtful_readings_count_for_verification():
    s, send = verifying_open()
    s = feed_channel(s, DEVICE, seconds=3.5, rate=-0.025, verdict=TrustState.DOUBTFUL)
    assert s.actions[send.action_id].status is ActionStatus.VERIFIED


def test_first_half_second_after_accept_is_not_measured():
    """The level keeps rising until the valve bites; only what follows counts."""
    s, send = verifying_open()
    s = feed_channel(s, DEVICE, seconds=0.4, rate=0.05)
    s = feed_channel(s, DEVICE, seconds=3.1, rate=-0.011)
    assert s.actions[send.action_id].status is ActionStatus.VERIFIED


def test_close_with_the_level_still_falling_is_unverified():
    s, record = close_awaiting_newest_verdict()
    s = trust_all(s, record)
    send = only(SendCommand, s.last_effects)
    assert (send.state, record.status) == ("closed", ActionStatus.DISPATCHED)
    s = apply(s, CommandResult(send.action_id, "ACCEPTED", {}))
    s = feed_channel(s, DEVICE, seconds=5.5, rate=-0.045)
    assert record.status is ActionStatus.UNVERIFIED and DEVICE in s.suspect


# ----- epochs -----


def restart(s, level=3.2):
    """The Twin restarts: `seq` and sim time go back to the start."""
    return feed_tick(s, {DEVICE: level}, verdict=None, seq=3)


def later(s):
    """Far enough along that a restart trips the epoch rule."""
    return feed_tick(s, {DEVICE: 6.1}, seq=s.global_tick + 1200)


def test_new_epoch_marks_active_actions_unknown_and_cancels_timers():
    s, record = awaiting_open()
    action_id = record.intent.action_id
    s = restart(later(s))
    assert s.epoch == 1 and record.status is ActionStatus.UNKNOWN
    assert CancelTimer(f"verdict:{action_id}") in s.last_effects
    assert alerts(s.last_effects, "EPOCH_CHANGED_ACTION_UNKNOWN") == ["operator"]
    assert action_id in s.actions


def test_new_epoch_resolves_a_dispatched_action_and_a_late_result_stays_ignored():
    s, send = dispatched_open()
    s = restart(later(s))
    assert s.actions[send.action_id].status is ActionStatus.UNKNOWN
    s = apply(s, CommandResult(send.action_id, "ACCEPTED", {}))
    assert s.actions[send.action_id].status is ActionStatus.UNKNOWN
    assert s.counters["LATE_COMMAND_RESULT"] == 1


def test_new_epoch_leaves_terminal_actions_alone():
    s, send = verifying_open()
    s = feed_falling_channel(s, DEVICE, seconds=3.5)
    s = restart(later(s))
    assert s.actions[send.action_id].status is ActionStatus.VERIFIED
    assert alerts(s.last_effects, "EPOCH_CHANGED_ACTION_UNKNOWN") == []


def test_new_epoch_clears_per_epoch_state_and_keeps_plan_versions():
    s, send = dispatched_open()
    s.below_low_since[DEVICE] = s.sim_now
    s.last_blocked_at[DEVICE] = s.sim_now
    s.last_refusal[(DEVICE, "open")] = ("COOLDOWN",)
    s.below_low_since["z2-wl1"] = s.sim_now
    s.last_hold["z2-wl1"] = "STUCK"
    s = restart(later(s))
    assert "z2-wl1" not in s.below_low_since and "z2-wl1" not in s.last_hold
    assert s.in_flight == set() and s.last_command_at == {} and s.last_blocked_at == {}
    assert s.last_refusal == {} and s.below_low_since.get(DEVICE) is None
    assert [r.reading_id for r in s.recent[DEVICE]] == [f"r-{DEVICE}-3"]
    assert s.plan_version[DEVICE] == 1 and send.action_id in s.actions


def test_valve_can_be_commanded_after_a_restart_rewinds_sim_time():
    s, send = dispatched_open()
    s = later(s)
    s.last_effects = []
    s = feed_tick(s, {DEVICE: 5.95}, seq=3, verdict_first=True)
    s = feed_rising_channel(s, DEVICE, until_level=6.1, verdict_first=True)
    fresh = only(SendCommand, s.last_effects)
    assert fresh.idempotency_key == f"brain-1-{fresh.action_id}"
    assert fresh.action_id != send.action_id


def test_rate_limit_refills_after_a_restart_rewinds_sim_time():
    s, _ = dispatched_open()
    s = restart(later(s))
    while s.bucket.take(s.sim_now):
        pass
    s = feed_channel(s, DEVICE, seconds=1.0, rate=0.02)
    assert s.bucket.take(s.sim_now)


def test_suspect_clears_on_a_new_epoch():
    s, send = verifying_open()
    s = feed_channel(s, DEVICE, seconds=3.5, rate=0.02)
    assert DEVICE in s.suspect
    s = restart(later(s))
    assert s.suspect == set()


# ----- registry status -----


def test_shield_refuses_a_contained_target_after_a_status_sync():
    s = state_with_registry()
    s = feed_rising_channel(s, DEVICE, until_level=5.995, verdict=None)
    s = feed_channel(s, DEVICE, seconds=1.0, rate=0.02, verdict=None, device_status=DeviceStatus.ISOLATED)
    [record] = s.actions.values()
    assert record.status is ActionStatus.REFUSED and "TARGET_CONTAINED" in record.reasons
    assert not of_type(SendCommand, s.last_effects)


def test_reading_status_updates_the_registry():
    s = state_with_registry()
    s = feed_tick(s, {DEVICE: 5.0}, device_status=DeviceStatus.ISOLATED)
    assert s.registry[DEVICE].status is DeviceStatus.ISOLATED
    assert s.registry[DEVICE].actuator.current_state == "closed"
    s = feed_tick(s, {DEVICE: 5.0})
    assert s.registry[DEVICE].status is DeviceStatus.OK


# ----- evidence from a contained device -----


@pytest.mark.parametrize("verdict_first", [False, True])
def test_reading_from_a_contained_device_stays_weightless_after_release(verdict_first):
    s = state_with_registry()
    for seq in range(1, 25):
        s = feed_tick(s, {DEVICE: 5.75 + 0.01 * seq}, verdict_first=verdict_first)
    s = feed_tick(s, {DEVICE: 6.0}, verdict_first=verdict_first, device_status=DeviceStatus.QUARANTINED)
    s = feed_tick(s, {DEVICE: 6.01}, verdict_first=verdict_first)
    s = feed_tick(s, {DEVICE: 6.02}, verdict_first=verdict_first)
    assert s.global_tick == 27 and s.registry[DEVICE].status is DeviceStatus.OK
    assert not of_type(SendCommand, s.last_effects)
    [record] = s.actions.values()
    assert record.status is ActionStatus.BLOCKED and "TRUST_INSUFFICIENT" in record.reasons
    assert any(r.device_status is DeviceStatus.QUARANTINED for r in record.intent.evidence)


def test_dispatched_is_published_before_the_command_is_sent():
    s, _ = dispatched_open()
    published = s.last_effects.index(Publish(commands(s.last_effects, "DISPATCHED")[0]))
    assert published < s.last_effects.index(only(SendCommand, s.last_effects))
