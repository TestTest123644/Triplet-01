"""Action lifecycle: Shield, trust gate, dispatch, results and effect verification (spec 8.6, 8.8).

Every function mutates `state` on behalf of `step` and returns the effects to emit.
"""

from collections.abc import Iterable

from city_brain.core.events import (
    CancelTimer,
    CommandResult,
    Effect,
    OperatorCommand,
    Publish,
    SendCommand,
    StartTimer,
    TimerFired,
)
from city_brain.core import catalog
from city_brain.core.records import alert_event, command_event, decision_event, verification_event
from city_brain.core.shield import ShieldContext, evaluate
from city_brain.core.water import commanded_ratio
from city_brain.core.state import ActionRecord, BrainState
from city_brain.core.trust import evidence_ref, meets_requirement, trust_weight
from city_brain.domain.ids import idempotency_key
from city_brain.domain.types import (
    ActionIntent,
    ActionStatus,
    DeviceStatus,
    EvidenceRef,
    Observation,
    RiskClass,
    StateKind,
    TrustState,
)

RULE = "water_t0"
VERDICT_WAIT = "verdict_wait"
VERDICT_TIMER_PREFIX = "verdict:"
APPROVAL_WAIT = "approval_wait"
APPROVAL_TIMER_PREFIX = "approval:"
SETTLE_TICKS = 5  # skip the first 0.5 s after acceptance while the valve takes effect
ACTIVE = frozenset({
    ActionStatus.PROPOSED,
    ActionStatus.AWAITING_TRUST,
    ActionStatus.AWAITING_APPROVAL,
    ActionStatus.DISPATCHED,
    ActionStatus.VERIFYING,
})
# A lost reply freezes the target until reconciliation (P7): the Twin may have obeyed.
GATES_PLANNING = ACTIVE | {ActionStatus.UNKNOWN}
EPOCH_CHANGED = "EPOCH_CHANGED"  # the restart settled the question, so the target is free again
COUNTS_FOR_VERIFICATION = (TrustState.TRUSTED, TrustState.DOUBTFUL)


# ----- planning gates -----


def may_propose(state: BrainState, intent: ActionIntent) -> bool:
    """One open action per target, and one attempt per cooldown after BLOCKED."""
    if any(_gates_planning(r) and r.intent.target == intent.target for r in state.actions.values()):
        return False
    blocked_at = state.last_blocked_at.get(intent.target)
    if blocked_at is None:
        return True
    cooldown = state.policy["shield"]["cooldown_s"].get(intent.capability, 0.0)
    return (state.sim_now - blocked_at).total_seconds() >= cooldown


# ----- transitions -----


def propose(state: BrainState, intent: ActionIntent) -> list[Effect]:
    context = ShieldContext(
        registry=state.registry,
        in_flight=state.in_flight,
        cycle_intents=(),
        last_command_at=state.last_command_at,
        bucket=state.bucket,
        suspect=state.suspect,
        now=state.sim_now,
        mode=state.mode,
        zones=state.zones,
    )
    shield = evaluate(intent, context, state.allowlist, state.policy)
    refusal_key = (intent.target, intent.state)
    if shield.passed:
        state.last_refusal.pop(refusal_key, None)
    elif state.last_refusal.get(refusal_key) == shield.refusals:
        state.counters["REFUSED_REPEAT"] += 1
        return []
    else:
        state.last_refusal[refusal_key] = shield.refusals

    record = ActionRecord(
        intent=intent,
        status=ActionStatus.PROPOSED,
        key=idempotency_key(state.epoch, intent.action_id),
        dispatched_at=None,
        reasons=(),
        shield=shield,
    )
    state.actions[intent.action_id] = record
    state.plan_version[intent.target] = intent.plan_version

    passes, reasons = _check_trust(state, record)
    effects: list[Effect] = [_decision(state, record)]
    if not shield.passed:
        effects.append(_transition(state, record, ActionStatus.REFUSED, shield.refusals))
    elif passes is None:
        effects.append(_transition(state, record, ActionStatus.AWAITING_TRUST, reasons))
        wait_s = float(state.policy["trust"]["verdict_wait_s"])
        effects.append(StartTimer(_timer_id(intent.action_id), VERDICT_WAIT, wait_s))
    else:
        effects.extend(_conclude(state, record, passes, reasons))
    return effects


def on_verdict(state: BrainState, reading_key: tuple[str, str]) -> list[Effect]:
    """Re-check every action citing the reading, whatever the ledger said about the verdict."""
    effects: list[Effect] = []
    for action_id in sorted(state.actions):
        record = state.actions[action_id]
        if record.status is not ActionStatus.AWAITING_TRUST or not _cites(record, reading_key):
            continue
        passes, reasons = _check_trust(state, record)
        if passes is None:
            continue
        effects.append(CancelTimer(_timer_id(action_id)))
        effects.extend(_conclude(state, record, passes, reasons))
    return effects


def on_operator(state: BrainState, command: OperatorCommand) -> list[Effect]:
    """An operator's answer to an approval request (spec 8.7). Pause and resume are the modes'."""
    if command.action not in ("approve", "deny") or command.action_id is None:
        return []
    record = state.actions.get(command.action_id)
    if record is None or record.status is not ActionStatus.AWAITING_APPROVAL:
        return []
    state.pending_approvals.pop(command.action_id, None)
    effects: list[Effect] = [CancelTimer(_approval_timer_id(command.action_id))]
    if command.action == "deny":
        return [*effects, _block(state, record, ("OPERATOR_DENIED",))]
    return [*effects, *dispatch(state, record, approved=True)]


def on_timer(state: BrainState, timer: TimerFired) -> list[Effect]:
    if timer.kind == APPROVAL_WAIT:
        action_id = timer.timer_id.removeprefix(APPROVAL_TIMER_PREFIX)
        record = state.actions.get(action_id)
        if record is None or record.status is not ActionStatus.AWAITING_APPROVAL:
            return []
        state.pending_approvals.pop(action_id, None)
        return [_block(state, record, ("APPROVAL_TIMEOUT",))]
    if timer.kind != VERDICT_WAIT:
        return []
    record = state.actions.get(timer.timer_id.removeprefix(VERDICT_TIMER_PREFIX))
    if record is None or record.status is not ActionStatus.AWAITING_TRUST:
        return []
    passes, reasons = _check_trust(state, record)
    if passes is None:
        # A reading without a verdict after the wait counts as UNKNOWN (spec 8.6).
        evidence = tuple(
            ref.model_copy(update={"state_kind": StateKind.UNKNOWN}) if ref.verdict_id is None else ref
            for ref in record.intent.evidence
        )
        record.intent = record.intent.model_copy(update={"evidence": evidence})
        passes, reasons = False, ("VERDICT_TIMEOUT",)
    return _conclude(state, record, passes, reasons)


def dispatch(state: BrainState, record: ActionRecord, approved: bool = False) -> list[Effect]:
    passes, reasons = _check_trust(state, record)
    if not passes:
        return [_block(state, record, reasons)]
    if not approved and _needs_approval(state, record):
        return _ask_operator(state, record)
    if not state.bucket.take(state.sim_now):
        return [_block(state, record, ("RATE_LIMIT",))]
    intent = record.intent
    state.in_flight.add(intent.target)
    state.last_command_at[intent.target] = state.sim_now
    # Published first: the log must show the dispatch even if the send never returns.
    return [
        _transition(state, record, ActionStatus.DISPATCHED, (), evidence=intent.evidence),
        SendCommand(intent.action_id, record.key, intent.capability, intent.target, intent.state),
    ]


def _needs_approval(state: BrainState, record: ActionRecord) -> bool:
    """An R3 acts only with an operator behind it, and is asked for only once (spec 8.7)."""
    if record.intent.risk_class is not RiskClass.R3:
        return False
    return record.intent.action_id not in state.pending_approvals


def _ask_operator(state: BrainState, record: ActionRecord) -> list[Effect]:
    intent = record.intent
    state.pending_approvals[intent.action_id] = state.sim_now
    wait_s = float(state.policy["approvals"]["wait_s"])
    return [
        _transition(state, record, ActionStatus.AWAITING_APPROVAL, (), evidence=intent.evidence),
        StartTimer(_approval_timer_id(intent.action_id), APPROVAL_WAIT, wait_s),
        Publish(alert_event(
            state, "operator", "critical", "APPROVAL_REQUIRED",
            f"{intent.state} on {intent.target} needs an operator: {intent.rationale}",
            _refs(intent),
        )),
    ]


def _approval_timer_id(action_id: str) -> str:
    return f"{APPROVAL_TIMER_PREFIX}{action_id}"


def on_result(state: BrainState, result: CommandResult) -> list[Effect]:
    """Record the Twin's answer. No branch ever resends."""
    record = state.actions.get(result.action_id)
    if record is None or record.status is not ActionStatus.DISPATCHED:
        state.counters["LATE_COMMAND_RESULT"] += 1
        return []
    intent = record.intent
    if result.outcome == "ACCEPTED":
        _adopt_state(state, intent)
        record.dispatched_at = state.sim_now
        record.dispatch_tick = state.global_tick
        return [_transition(state, record, ActionStatus.VERIFYING, (), result.reply)]
    state.in_flight.discard(intent.target)
    if result.outcome == "REJECTED":
        return [_transition(state, record, ActionStatus.REJECTED, (), result.reply)]
    return [
        _transition(state, record, ActionStatus.UNKNOWN, (), result.reply),
        Publish(alert_event(
            state, "operator", "warning", "COMMAND_RESULT_UNKNOWN",
            f"No result for {intent.state} on {intent.target}; the command is not resent.",
            _refs(intent),
        )),
    ]


def note_reading(state: BrainState, obs: Observation) -> None:
    """Remember the channel's readings during verification, to check their verdicts at close."""
    for record in state.actions.values():
        if record.status is ActionStatus.VERIFYING and record.intent.expected_effect.channel == obs.device_id:
            record.window_readings.append((obs.seq, obs.reading_id, obs.sig_hash))


def on_estimate(state: BrainState) -> list[Effect]:
    """Close every window that is due, on any reading: a silent sensor must not leave one open."""
    effects: list[Effect] = []
    for action_id in sorted(state.actions):
        record = state.actions[action_id]
        if record.status is not ActionStatus.VERIFYING:
            continue
        # The wait lets the verdicts on the window's readings land before they are counted.
        due_s = record.intent.expected_effect.window_s + float(state.policy["trust"]["verdict_wait_s"])
        if (state.sim_now - record.dispatched_at).total_seconds() >= due_s:
            effects.extend(_verify(state, record))
    return effects


def on_new_epoch(state: BrainState) -> list[Effect]:
    """Nothing dangles across a Twin restart: active actions end as UNKNOWN."""
    effects: list[Effect] = []
    for action_id in sorted(state.actions):
        record = state.actions[action_id]
        if record.status is ActionStatus.UNKNOWN:
            record.reasons = (*record.reasons, EPOCH_CHANGED)
        if record.status not in ACTIVE:
            continue
        if record.status is ActionStatus.AWAITING_TRUST:
            effects.append(CancelTimer(_timer_id(action_id)))
        effects.append(_transition(state, record, ActionStatus.UNKNOWN, (EPOCH_CHANGED,)))
        effects.append(Publish(alert_event(
            state, "operator", "warning", "EPOCH_CHANGED_ACTION_UNKNOWN",
            f"The Twin restarted during {record.intent.state} on {record.intent.target}.",
            _refs(record.intent),
        )))
    return effects


# ----- helpers -----


def _gates_planning(record: ActionRecord) -> bool:
    return record.status in GATES_PLANNING and EPOCH_CHANGED not in record.reasons


def _timer_id(action_id: str) -> str:
    return f"{VERDICT_TIMER_PREFIX}{action_id}"


def _cites(record: ActionRecord, reading_key: tuple[str, str]) -> bool:
    return any((r.reading_id, r.sig_hash) == reading_key for r in record.intent.evidence)


def _refs(intent: ActionIntent) -> dict:
    return {"action_id": intent.action_id, "incident_id": intent.incident_id, "target": intent.target}


def _resolve(
    state: BrainState, ref: EvidenceRef, honour_history: bool = False
) -> tuple[EvidenceRef, bool]:
    """The ref as the ledger and registry see it now, and whether its verdict has gone.

    Contained at arrival or contained now both mean zero weight: release does not launder a
    reading taken during containment. An unregistered device carries no weight either.

    `honour_history` is the containment exception (spec 8.7). The request cites readings from
    before the containment, so by the time an operator sees it their verdicts may have
    expired or been pruned entirely. On this path the reading is weighed as it was taken: the
    device's status at the time, and the newest verdict the ledger still holds whether or not
    it has expired. A later downgrade or revocation therefore still bites, and a verdict the
    ledger has forgotten leaves the record standing as it was made. Expiry alone does not
    block the request, because the whole point of an R3 is that a person decides.
    """
    device = state.registry.get(ref.device_id)
    current = device.status if device is not None else DeviceStatus.ISOLATED
    if honour_history:
        verdict = state.ledger.latest(ref.reading_id, ref.sig_hash)
        if verdict is None:
            return ref, False
        weight = trust_weight(verdict, ref.device_status, state.policy["trust"]["caps"])
        return evidence_ref(ref, verdict, weight), False
    verdict = state.ledger.lookup(ref.reading_id, ref.sig_hash, state.sim_now)
    status = current if ref.device_status is DeviceStatus.OK else ref.device_status
    weight = trust_weight(verdict, status, state.policy["trust"]["caps"])
    known = ref.verdict_id is not None or state.ledger.latest(ref.reading_id, ref.sig_hash) is not None
    return evidence_ref(ref, verdict, weight), verdict is None and known


def _check_trust(state: BrainState, record: ActionRecord) -> tuple[bool | None, tuple[str, ...]]:
    """Refresh the cited evidence. True: requirement met; False: failed; None: verdicts pending."""
    history = record.intent.containment_override
    resolved = [_resolve(state, ref, honour_history=history) for ref in record.intent.evidence]
    evidence = tuple(ref for ref, _ in resolved)
    record.intent = record.intent.model_copy(update={"evidence": evidence})
    met, reasons = meets_requirement(evidence, record.intent.risk_class)
    if any(expired for _, expired in resolved):
        return False, ("VERDICT_EXPIRED", *reasons)
    if met:
        return True, ()
    return (False if "TRUST_INSUFFICIENT" in reasons else None), reasons


def _conclude(
    state: BrainState, record: ActionRecord, passes: bool, reasons: tuple[str, ...]
) -> list[Effect]:
    return dispatch(state, record) if passes else [_block(state, record, reasons)]


def _block(state: BrainState, record: ActionRecord, reasons: tuple[str, ...]) -> Publish:
    state.last_blocked_at[record.intent.target] = state.sim_now
    return _transition(state, record, ActionStatus.BLOCKED, reasons)


def _transition(
    state: BrainState,
    record: ActionRecord,
    status: ActionStatus,
    reasons: tuple[str, ...],
    reply: dict | None = None,
    evidence: tuple[EvidenceRef, ...] = (),
) -> Publish:
    record.status = status
    record.reasons = reasons
    # `tool_call` is the runtime's: the core knows capabilities, not partner tools.
    return Publish(command_event(state, record.intent, record.shield, status, None, reply, reasons, evidence))


def _decision(state: BrainState, record: ActionRecord) -> Publish:
    intent = record.intent
    device = state.registry.get(intent.target)
    incident = {"incident_id": intent.incident_id, "zone": device.zone_id if device is not None else None}
    return Publish(decision_event(
        state, intent, "act", incident, RULE, intent.evidence, intent.alternatives, intent.rationale, (),
    ))


def _adopt_state(state: BrainState, intent: ActionIntent) -> None:
    """Every device that reports this actuator now reads the commanded state."""
    for device_id, device in state.registry.items():
        if device.actuator is not None and device.actuator.target == intent.target:
            actuator = device.actuator.model_copy(update={"current_state": intent.state})
            state.registry[device_id] = device.model_copy(update={"actuator": actuator})


def untrusted_ticks(state: BrainState, readings: Iterable[tuple[int, str, str]]) -> list[int]:
    """Ticks whose reading has no TRUSTED or DOUBTFUL verdict right now (spec 8.8)."""
    ticks = []
    for tick, reading_id, sig_hash in readings:
        verdict = state.ledger.lookup(reading_id, sig_hash, state.sim_now)
        if verdict is None or verdict.state not in COUNTS_FOR_VERIFICATION:
            ticks.append(tick)
    return ticks


def _learn_gain(state: BrainState, intent: ActionIntent, slope: float) -> None:
    """A settled valve position and the slope it produced teach the channel its gain (spec 8.4)."""
    channel = state.filters.get(intent.expected_effect.channel)
    ratio = commanded_ratio(intent.state)
    if channel is None or ratio is None or not hasattr(channel, "identify_gain"):
        return
    channel.identify_gain(u=ratio, slope=slope)


def _has_untrusted_verdict(state: BrainState, record: ActionRecord) -> bool:
    for _, reading_id, sig_hash in record.window_readings:
        verdict = state.ledger.lookup(reading_id, sig_hash, state.sim_now)
        if verdict is not None and verdict.state not in COUNTS_FOR_VERIFICATION:
            return True
    return False


def _verify(state: BrainState, record: ActionRecord) -> list[Effect]:
    intent = record.intent
    expected = intent.expected_effect
    since_tick = record.dispatch_tick + SETTLE_TICKS
    excluded = untrusted_ticks(state, record.window_readings)
    channel = state.channel_filter(expected.channel)
    slope = None if channel is None else channel.observed_slope(
        since_tick, exclude_ticks=frozenset(excluded))
    if slope is None:
        verdict = "inconclusive"
    elif expected.comparator == "<=":
        verdict = "verified" if slope <= expected.threshold else "unverified"
    else:
        verdict = "verified" if slope >= expected.threshold else "unverified"
    if slope is not None and intent.capability == "valve":
        _learn_gain(state, intent, slope)
    record.status = ActionStatus.VERIFIED if verdict == "verified" else ActionStatus.UNVERIFIED
    state.in_flight.discard(intent.target)
    effects: list[Effect] = [Publish(verification_event(
        state, intent.action_id, expected,
        {"slope": slope, "since_tick": since_tick, "excluded_ticks": excluded}, verdict,
        target=intent.target, incident_id=intent.incident_id,
    ))]
    if verdict == "verified":
        return effects
    if intent.incident_id in state.incidents.incidents:  # the next catalog step is next (spec 8.5)
        catalog.note_step_failed(state, intent.incident_id)
    if slope is None and _has_untrusted_verdict(state, record):
        message = f"Measurements for {intent.state} on {intent.target} were excluded as untrusted."
    elif slope is None:
        message = f"No measurements were available to check {intent.state} on {intent.target}."
    else:
        state.suspect.add(intent.target)  # only a measured failure condemns the actuator
        message = f"{intent.state} on {intent.target} had no observed effect."
    message += " The command is not resent."
    refs = {**_refs(intent), "observed_slope": slope}
    for audience in ("operator", "guardian"):
        effects.append(Publish(alert_event(state, audience, "critical", "ACTUATOR_NO_EFFECT", message, refs)))
    return effects
