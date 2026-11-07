"""Verdict revocation, Guardian containment and the no-retroactive-trust rule (spec 8.9)."""

from datetime import timedelta

import pytest

from city_brain.core.events import GuardianEventReceived, Publish, VerdictReceived
from city_brain.core.trust import in_outage
from city_brain.domain.types import (
    ActionStatus,
    BrainMode,
    DeviceStatus,
    GuardianContainment,
    GuardianHeartbeat,
    GuardianRevocation,
    TrustState,
)
from tests.factories import T0
from tests.helpers_coordinator import (
    PERIOD_S,
    RISE_RATE,
    apply,
    feed_tick,
    state_with_registry,
    verdict_for,
)

# ----- the ledger -----


def ledger_with(*verdicts):
    state = state_with_registry("z1-wl1")
    for verdict in verdicts:
        state.ledger.add(verdict)
    return state.ledger


def test_revoke_marks_the_verdict_unknown_and_returns_its_key():
    verdict = verdict_for("z1-wl1", 1)
    ledger = ledger_with(verdict)
    assert ledger.revoke(verdict.verdict_id) == [(verdict.reading_id, verdict.sig_hash)]
    found = ledger.latest(verdict.reading_id, verdict.sig_hash)
    assert found.state is TrustState.UNKNOWN and found.trust == 0.0


def test_revoke_keeps_the_revocation_reason_and_the_verdict_id():
    verdict = verdict_for("z1-wl1", 1)
    ledger = ledger_with(verdict)
    ledger.revoke(verdict.verdict_id)
    found = ledger.latest(verdict.reading_id, verdict.sig_hash)
    assert found.verdict_id == verdict.verdict_id and "REVOKED" in found.reasons


def test_revoke_an_unknown_verdict_changes_nothing():
    verdict = verdict_for("z1-wl1", 1)
    ledger = ledger_with(verdict)
    assert ledger.revoke("v-nobody") == []
    assert ledger.latest(verdict.reading_id, verdict.sig_hash).state is TrustState.TRUSTED


def test_revoke_touches_only_the_named_verdict():
    first, second = verdict_for("z1-wl1", 1), verdict_for("z1-wl1", 2)
    ledger = ledger_with(first, second)
    ledger.revoke(first.verdict_id)
    assert ledger.latest(first.reading_id, first.sig_hash).state is TrustState.UNKNOWN
    assert ledger.latest(second.reading_id, second.sig_hash).state is TrustState.TRUSTED


def test_a_revoked_verdict_cannot_be_re_added():
    verdict = verdict_for("z1-wl1", 1)
    ledger = ledger_with(verdict)
    ledger.revoke(verdict.verdict_id)
    assert ledger.add(verdict) == []
    assert ledger.latest(verdict.reading_id, verdict.sig_hash).state is TrustState.UNKNOWN


def test_revoked_keys_are_returned_in_sorted_order():
    shared = "v-shared"
    first = verdict_for("z1-wl1", 2, verdict_id=shared)
    second = verdict_for("z1-wl1", 1, verdict_id=shared)
    ledger = ledger_with(first, second)
    keys = ledger.revoke(shared)
    assert keys == sorted(keys) and len(keys) == 2


def test_watermark_starts_unset_and_only_moves_forward():
    ledger = ledger_with()
    assert ledger.watermark is None
    ledger.note_heartbeat(T0 + timedelta(seconds=5))
    assert ledger.watermark == T0 + timedelta(seconds=5)
    ledger.note_heartbeat(T0 + timedelta(seconds=1))
    assert ledger.watermark == T0 + timedelta(seconds=5)


def test_clear_forgets_the_watermark_and_the_revocations():
    verdict = verdict_for("z1-wl1", 1)
    ledger = ledger_with(verdict)
    ledger.note_heartbeat(T0)
    ledger.revoke(verdict.verdict_id)
    ledger.clear()
    assert ledger.watermark is None
    assert ledger.add(verdict) == [(verdict.reading_id, verdict.sig_hash)]


# ----- containment -----


def notice(state, notice_obj):
    return apply(state, GuardianEventReceived(notice_obj))


def containment(action, device_id="z1-wl1", at=T0):
    return GuardianContainment(
        device_id=device_id, action=action, effective_at=at,
        reasons=frozenset({"SCRIPTED"}), issued_at=at,
    )


@pytest.mark.parametrize(
    "action, expected",
    [
        ("isolated", DeviceStatus.ISOLATED),
        ("quarantined", DeviceStatus.QUARANTINED),
        ("rolled_back", DeviceStatus.ROLLED_BACK),
        ("released", DeviceStatus.OK),
    ],
)
def test_containment_sets_the_registry_status(action, expected):
    state = notice(state_with_registry("z1-wl1"), containment(action))
    assert state.registry["z1-wl1"].status is expected


def test_containment_for_an_unknown_device_is_recorded_without_crashing():
    state = notice(state_with_registry("z1-wl1"), containment("quarantined", device_id="z9-wl9"))
    assert "z9-wl9" not in state.registry and state.contained["z9-wl9"] is DeviceStatus.QUARANTINED


def test_containment_publishes_a_status_event():
    state = notice(state_with_registry("z1-wl1"), containment("quarantined"))
    reasons = [e.event["data"]["reason"] for e in state.last_effects if isinstance(e, Publish)]
    assert reasons[-1] == "containment"


def test_containment_survives_a_reading_that_still_reports_the_device_as_ok():
    state = notice(state_with_registry("z1-wl1"), containment("quarantined"))
    state = feed_tick(state, {"z1-wl1": 4.0})
    assert state.registry["z1-wl1"].status is DeviceStatus.QUARANTINED


def test_release_lets_the_device_report_its_own_status_again():
    state = notice(state_with_registry("z1-wl1"), containment("quarantined"))
    state = notice(state, containment("released", at=T0 + timedelta(seconds=1)))
    assert "z1-wl1" not in state.contained
    state = feed_tick(state, {"z1-wl1": 4.0}, device_status=DeviceStatus.ISOLATED)
    assert state.registry["z1-wl1"].status is DeviceStatus.ISOLATED


# ----- revocation through step -----


def rise_to_awaiting_action(state, device_id="z1-wl1", start=5.4):
    """Rise until a plan appears, judging each reading only after it has been ingested.

    The reading that triggers the plan is therefore still unjudged, so the action waits for
    trust while citing two verdicts that a revocation can withdraw.
    """
    level = start
    for _ in range(400):
        state = feed_tick(state, {device_id: level}, verdict=None)
        if state.actions:
            return state
        state = apply(state, VerdictReceived(verdict_for(device_id, state.global_tick)))
        level = round(level + RISE_RATE * PERIOD_S, 6)
    raise AssertionError("the channel never crossed the high band")


def awaiting_action(state):
    actions = [r for r in state.actions.values() if r.status is ActionStatus.AWAITING_TRUST]
    assert len(actions) == 1, [(a.intent.action_id, a.status) for a in state.actions.values()]
    return actions[0]


def test_revoking_a_cited_verdict_blocks_the_action():
    state = rise_to_awaiting_action(state_with_registry("z1-wl1", valve="closed"))
    record = awaiting_action(state)
    revoked = next(ref.verdict_id for ref in record.intent.evidence if ref.verdict_id is not None)
    state = notice(state, GuardianRevocation(verdict_id=revoked, issued_at=state.sim_now))
    assert state.actions[record.intent.action_id].status is ActionStatus.BLOCKED
    assert "TRUST_INSUFFICIENT" in state.actions[record.intent.action_id].reasons


def test_revoking_an_uncited_verdict_leaves_the_action_waiting():
    state = rise_to_awaiting_action(state_with_registry("z1-wl1", valve="closed"))
    record = awaiting_action(state)
    state = notice(state, GuardianRevocation(verdict_id="v-nobody", issued_at=state.sim_now))
    assert state.actions[record.intent.action_id].status is ActionStatus.AWAITING_TRUST


# ----- heartbeats and no retroactive trust -----


def test_heartbeat_is_heard_and_moves_the_watermark():
    state = state_with_registry("z1-wl1", mode=BrainMode.WARMUP)
    assert state.guardian_heard is False
    through = T0 + timedelta(seconds=3)
    state = notice(state, GuardianHeartbeat(evaluated_through=through, issued_at=through))
    assert state.guardian_heard is True and state.ledger.watermark == through


def test_a_verdict_also_counts_as_hearing_from_the_guardian():
    state = state_with_registry("z1-wl1", mode=BrainMode.WARMUP)
    state = feed_tick(state, {"z1-wl1": 4.0})
    assert state.guardian_heard is True


def test_in_outage_covers_open_and_closed_windows():
    closed = (T0, T0 + timedelta(seconds=10))
    open_window = (T0 + timedelta(seconds=20), None)
    windows = [closed, open_window]
    assert in_outage(T0 + timedelta(seconds=5), windows)
    assert not in_outage(T0 + timedelta(seconds=15), windows)
    assert in_outage(T0 + timedelta(seconds=999), windows)
    assert not in_outage(T0 - timedelta(seconds=1), windows)


def test_unjudged_readings_inside_an_outage_window_carry_no_weight():
    state = state_with_registry("z1-wl1")
    state.outage_windows = [(T0, None)]
    state = feed_tick(state, {"z1-wl1": 4.0}, verdict=None)
    assert state.filters["z1-wl1"].initialised is False
    assert state.recent["z1-wl1"][-1].weight == 0.0


def test_unjudged_readings_outside_any_outage_window_keep_their_pending_weight():
    state = state_with_registry("z1-wl1")
    state = feed_tick(state, {"z1-wl1": 4.0}, verdict=None)
    assert state.filters["z1-wl1"].initialised is True


def test_an_explicit_verdict_inside_an_outage_window_is_still_honoured():
    state = state_with_registry("z1-wl1")
    state.outage_windows = [(T0, None)]
    state = feed_tick(state, {"z1-wl1": 4.0}, verdict=TrustState.TRUSTED, verdict_first=True)
    assert state.filters["z1-wl1"].initialised is True
    assert state.recent["z1-wl1"][-1].weight == 1.0


def test_a_later_heartbeat_never_trusts_readings_from_the_outage_window():
    state = state_with_registry("z1-wl1")
    state.outage_windows = [(T0, T0 + timedelta(seconds=2))]
    state = feed_tick(state, {"z1-wl1": 4.0}, verdict=None, seq=5)
    assert state.filters["z1-wl1"].initialised is False
    through = T0 + timedelta(seconds=30)
    state = notice(state, GuardianHeartbeat(evaluated_through=through, issued_at=through))
    assert state.recent["z1-wl1"][-1].weight == 0.0
    assert state.filters["z1-wl1"].initialised is False


def test_readings_after_the_window_closes_count_again():
    state = state_with_registry("z1-wl1")
    state.outage_windows = [(T0, T0 + timedelta(seconds=0.4))]
    state = feed_tick(state, {"z1-wl1": 4.0}, verdict=None, seq=2)
    assert state.filters["z1-wl1"].initialised is False
    state = feed_tick(state, {"z1-wl1": 4.0}, verdict=None, seq=20)
    assert state.filters["z1-wl1"].initialised is True
