"""The pure transition function: (state, event) -> (state, effects)."""

from collections import deque
from collections.abc import Sequence

from city_brain.core import catalog, coordinator, modes
from city_brain.core.assessor import assess
from city_brain.core.channel_filter import LagFilter, lag_params
from city_brain.core.detectors import detect_findings
from city_brain.core.estimator import WaterFilter
from city_brain.core.epochs import confirm_candidate, is_new_epoch, start_new_epoch
from city_brain.core.events import (
    CommandResult,
    Effect,
    Event,
    GuardianEventReceived,
    OperatorCommand,
    Publish,
    ReadingReceived,
    ReadingRejected,
    RegistryLoaded,
    TimerFired,
    VerdictReceived,
)
from city_brain.core.records import alert_event, decision_event, incident_event, status_event
from city_brain.core.state import BrainState, DeviceCursor
from city_brain.core.trust import estimate_weight, evidence_ref, in_outage, trust_weight
from city_brain.core.water import (
    WaterContext,
    commanded_ratio,
    matches_commanded,
    plan_water_t0,
    water_hold_reason,
)
from city_brain.core.water_mpc import plan_water_mpc
from city_brain.domain.types import (
    ActionIntent,
    DeviceStatus,
    EvidenceRef,
    GuardianContainment,
    GuardianHeartbeat,
    GuardianRevocation,
    Observation,
    SensorKind,
    TrustState,
    Verdict,
)

UNKNOWN_VALVE_STATE = ""  # no registry entry or no actuator: the planner holds on it
WOULD_CHANGE_IF = {
    "STALE": "weighted readings arrive on this channel again",
    "UNTRUSTED_READINGS": "the Guardian trusts this device again",
    "INSUFFICIENT_EVIDENCE": "enough readings arrive on this channel to cite",
    "STUCK": "the sensor value moves again",
    "IMPLAUSIBLE_JUMP": "readings agree with the estimate again",
    "MODEL_MISMATCH": "readings stay consistent with the model",
    "UNKNOWN_VALVE_STATE": "the registry reports a known valve state",
    "TARGET_CONTAINED": "the Guardian releases the device, or the level turns critical",
}
CONTAINMENT_STATUS = {
    "isolated": DeviceStatus.ISOLATED,
    "quarantined": DeviceStatus.QUARANTINED,
    "rolled_back": DeviceStatus.ROLLED_BACK,
    "released": DeviceStatus.OK,
}


def _status(state: BrainState, reason: str) -> Publish:
    return Publish(status_event(state, reason, {}))


def step(state: BrainState, event: Event) -> tuple[BrainState, list[Effect]]:
    effects: list[Effect] = []
    if isinstance(event, RegistryLoaded):
        reason = "registry_reloaded" if state.registry else "startup"
        state.registry = {d.device_id: d for d in event.devices}
        state.allowlist = event.allowlist
        effects.append(_status(state, reason))
        effects.extend(modes.on_registry(state))
        if event.keys_changed:
            _start_epoch(state, effects)
    elif isinstance(event, ReadingRejected):
        state.counters[event.code] += 1
    elif isinstance(event, ReadingReceived):
        _ingest(state, event, effects)
    elif isinstance(event, VerdictReceived):
        state.guardian_heard = True
        state.ledger.add(event.verdict)
        _note_device_verdict(state, event.verdict)
        _note_trusted_evidence(state, event.verdict)
        # sim_now is undefined before the first accepted reading; park without pruning.
        if state.epoch_origin is not None:
            state.ledger.prune(state.sim_now)
        effects.extend(modes.on_guardian_heard(state))
        effects.extend(coordinator.on_verdict(state, (event.verdict.reading_id, event.verdict.sig_hash)))
    elif isinstance(event, GuardianEventReceived):
        effects.extend(_guardian_notice(state, event.notice))
    elif isinstance(event, OperatorCommand):
        effects.extend(modes.on_operator(state, event))
        effects.extend(coordinator.on_operator(state, event))
    elif isinstance(event, CommandResult):
        effects.extend(coordinator.on_result(state, event))
    elif isinstance(event, TimerFired):
        effects.extend(modes.on_timer(state, event))
        effects.extend(coordinator.on_timer(state, event))
    return state, effects


def _guardian_notice(state: BrainState, notice) -> list[Effect]:
    """A `guardian.events` notice: the Guardian is alive, and may contain or withdraw (spec 8.9)."""
    state.guardian_heard = True
    if isinstance(notice, GuardianHeartbeat):
        state.ledger.note_heartbeat(notice.evaluated_through)
    effects: list[Effect] = list(modes.on_guardian_heard(state))
    if isinstance(notice, GuardianHeartbeat):
        return effects
    if isinstance(notice, GuardianContainment):
        _apply_containment(state, notice)
        effects.append(Publish(status_event(state, "containment", {
            "device_id": notice.device_id, "action": notice.action, "reasons": notice.reasons,
        })))
        return effects
    if isinstance(notice, GuardianRevocation):
        affected = state.ledger.revoke(notice.verdict_id)
        effects.append(Publish(status_event(state, "verdict_revoked", {
            "verdict_id": notice.verdict_id, "readings": len(affected),
        })))
        _refresh_device_verdicts(state, notice.verdict_id)
        for key in affected:
            effects.extend(coordinator.on_verdict(state, key))
    return effects


def _apply_containment(state: BrainState, notice: GuardianContainment) -> None:
    """The Guardian's word outranks what a reading reports, until it releases the device."""
    status = CONTAINMENT_STATUS[notice.action]
    if status is DeviceStatus.OK:
        state.contained.pop(notice.device_id, None)
    else:
        state.contained[notice.device_id] = status
    device = state.registry.get(notice.device_id)
    if device is not None and device.status is not status:
        state.registry[notice.device_id] = device.model_copy(update={"status": status})


def _refresh_device_verdicts(state: BrainState, verdict_id: str) -> None:
    for device_id in sorted(state.device_verdicts):
        verdict = state.device_verdicts[device_id]
        if verdict.verdict_id != verdict_id:
            continue
        current = state.ledger.latest(verdict.reading_id, verdict.sig_hash)
        if current is not None:
            state.device_verdicts[device_id] = current


def _ingest(state: BrainState, event: ReadingReceived, effects: list[Effect]) -> None:
    obs = event.obs
    epochs = {**state.policy.get("epochs", {}), **state.params.get("epochs", {})}
    cursor = state.cursors.get(obs.device_id)
    key = (obs.reading_id, obs.sig_hash)
    if key in state.seen:  # before the epoch check: a replay is not a restart
        state.counters["DUPLICATE"] += 1
        return
    if is_new_epoch(cursor, obs, int(epochs["seq_drop"]), float(epochs["ts_back_s"])):
        state.counters["EPOCH_CANDIDATE"] += 1
        confirm = int(epochs["confirm_devices"]), int(epochs["confirm_ticks"])
        if not confirm_candidate(state, obs.device_id, *confirm):
            return
        _start_epoch(state, effects)
        cursor = None
    if _seq_jumped(state, cursor, obs, int(epochs["seq_jump_max"])):
        state.counters["SEQ_JUMP"] += 1
        return
    if cursor is not None and obs.seq <= cursor.seq:
        state.counters["OUT_OF_ORDER"] += 1
        return
    state.seen.add(key)
    state.cursors[obs.device_id] = DeviceCursor(seq=obs.seq, sim_time=obs.sim_time)
    if state.epoch_origin is None:
        state.epoch_origin = (obs.sim_time, obs.seq)
    advanced = obs.seq > state.global_tick
    state.global_tick = max(state.global_tick, obs.seq)
    _note_device_status(state, obs)
    effects.extend(modes.on_reading(state))
    if obs.kind is SensorKind.WATER_LEVEL:
        state.last_heard[obs.device_id] = obs.seq
        state.silent.discard(obs.device_id)
    if advanced:
        _sweep_silent_channels(state, effects)
        _sweep_zones(state, effects)
    if obs.kind is SensorKind.WATER_LEVEL:
        _water_reading(state, obs, effects)
    else:
        _channel_reading(state, obs)
        effects.extend(coordinator.on_estimate(state))


def _channel_reading(state: BrainState, obs: Observation) -> None:
    """Track a non-water channel and remember its evidence, the way water channels are tracked."""
    channel = state.lag_filters.get(obs.device_id)
    if channel is None:
        device = state.registry.get(obs.device_id)
        low = device.min_val if device is not None else 0.0
        high = device.max_val if device is not None else 0.0
        channel = state.lag_filters[obs.device_id] = LagFilter(
            lag_params(state.params, obs.kind), min_val=low, max_val=high)
    trust = state.policy["trust"]
    in_range = "OUT_OF_RANGE" not in obs.flags
    weight = 0.0
    if in_range:
        latest = state.ledger.latest(obs.reading_id, obs.sig_hash)
        weight = estimate_weight(latest, obs.device_status, trust["caps"], _pending_weight(state, obs))
    channel.update(obs.value, obs.seq, weight, at_ceiling="AT_CEILING" in obs.flags)
    # So that a verification window can exclude the ticks the Guardian later distrusts.
    coordinator.note_reading(state, obs)
    if in_range:
        _remember_evidence(state, obs)


def _remember_evidence(state: BrainState, obs: Observation) -> None:
    """Record what this reading is worth, and keep the last ones that were worth something.

    `trusted_recent` is what the containment exception cites: once a channel is contained its
    readings carry nothing, so the only honest thing to show an operator is the last window
    that did (spec 8.7).
    """
    depth = int(state.policy["water"]["evidence_count"])
    trust = state.policy["trust"]
    verdict = state.ledger.lookup(obs.reading_id, obs.sig_hash, state.sim_now)
    ref = evidence_ref(obs, verdict, trust_weight(verdict, obs.device_status, trust["caps"]))
    state.recent.setdefault(obs.device_id, deque(maxlen=depth)).append(ref)
    if ref.weight > 0.0:  # the verdict was already here when the reading arrived
        _keep_trusted(state, obs.device_id, ref)


def _keep_trusted(state: BrainState, device_id: str, ref: EvidenceRef) -> None:
    """Add a ref to the trusted window, unless it is already the newest one in it."""
    depth = int(state.policy["water"]["evidence_count"])
    window = state.trusted_recent.setdefault(device_id, deque(maxlen=depth))
    key = (ref.reading_id, ref.sig_hash)
    if window and (window[-1].reading_id, window[-1].sig_hash) == key:
        return
    window.append(ref)


def _note_trusted_evidence(state: BrainState, verdict: Verdict) -> None:
    """A verdict landing on a reading we kept: record what that reading was worth.

    Readings almost always arrive before the verdict that judges them, so this is where the
    trusted window actually fills. It is what an R3 cites once the channel has been contained
    and its later readings carry nothing (spec 8.7).
    """
    if verdict.state is not TrustState.TRUSTED:
        return
    window = state.recent.get(verdict.device_id)
    if not window:
        return
    key = (verdict.reading_id, verdict.sig_hash)
    for ref in window:
        if (ref.reading_id, ref.sig_hash) != key:
            continue
        if ref.device_status is not DeviceStatus.OK:
            return  # the device said it was contained when it spoke; that reading is not evidence
        weight = trust_weight(verdict, ref.device_status, state.policy["trust"]["caps"])
        if weight > 0.0:
            _keep_trusted(state, verdict.device_id, evidence_ref(ref, verdict, weight))
        return


def _device_trust(state: BrainState, device_id: str) -> float:
    """How far the Guardian's latest word on this device lets its findings count (spec 8.3.2)."""
    device = state.registry.get(device_id)
    status = device.status if device is not None else DeviceStatus.ISOLATED
    verdict = state.device_verdicts.get(device_id)
    if verdict is None:
        if status is not DeviceStatus.OK:
            return 0.0
        return float(state.policy["trust"]["pending_weight"])
    return trust_weight(verdict, status, state.policy["trust"]["caps"])


def _sweep_zones(state: BrainState, effects: list[Effect]) -> None:
    """Once per tick: what each zone's channels say, what it means, and what it asks for."""
    trust = {device_id: _device_trust(state, device_id) for device_id in sorted(state.registry)}
    reasons = {
        device_id: verdict.reasons for device_id, verdict in sorted(state.device_verdicts.items())
    }
    for zone_id in sorted(state.zones):
        findings = detect_findings(
            zone_id, state.registry, state.filters, state.lag_filters, state.policy,
            state.global_tick, now=state.sim_now,
        )
        changed = assess(
            state.incidents, zone_id, findings, trust, reasons, state.registry,
            state.policy, state.sim_now,
        )
        for incident in changed:
            effects.append(Publish(incident_event(state, incident)))
            if incident.status in ("resolved", "dismissed"):
                state.plan_step.pop(incident.incident_id, None)
        _respond(state, zone_id, effects)


def _respond(state: BrainState, zone_id: str, effects: list[Effect]) -> None:
    for incident_id in sorted(state.incidents.incidents):
        incident = state.incidents.incidents[incident_id]
        if incident.zone_id != zone_id:
            continue
        for intent in catalog.plan_catalog(incident, state, state.policy):
            if not coordinator.may_propose(state, intent):
                continue
            effects.extend(coordinator.propose(state, intent))
            record = state.actions.get(intent.action_id)
            if record is not None and record.status in catalog.UNDERWAY:
                updated = state.incidents.mitigating(incident.incident_id, state.sim_now)
                if updated is not None:
                    effects.append(Publish(incident_event(state, updated)))


def _start_epoch(state: BrainState, effects: list[Effect]) -> None:
    start_new_epoch(state)
    effects.append(_status(state, "epoch_changed"))
    effects.extend(coordinator.on_new_epoch(state))


def _seq_jumped(state: BrainState, cursor: DeviceCursor | None, obs: Observation, jump_max: int) -> bool:
    if cursor is not None:  # against the global tick too: a long silence must not lock a device out
        return obs.seq > max(cursor.seq, state.global_tick) + jump_max
    return state.epoch_origin is not None and obs.seq > state.global_tick + jump_max


def _note_device_verdict(state: BrainState, verdict: Verdict) -> None:
    current = state.device_verdicts.get(verdict.device_id)
    if current is None or verdict.issued_at >= current.issued_at:
        state.device_verdicts[verdict.device_id] = verdict


def _sweep_silent_channels(state: BrainState, effects: list[Effect]) -> None:
    """A channel with no readings has no reading to hold on, so the tick itself records it."""
    silent_ticks = float(state.params["filter"]["stale_s"]) / state.sample_period_s
    for device_id in sorted(state.registry):
        device = state.registry[device_id]
        if device.kind is not SensorKind.WATER_LEVEL:
            continue
        heard = state.last_heard.get(device_id, state.epoch_origin[1])
        if state.global_tick - heard < silent_ticks:
            continue
        recent = tuple(state.recent.get(device_id, ()))
        _record_hold(state, device_id, device.zone_id, recent, "STALE", effects)
        if device_id not in state.silent:
            state.silent.add(device_id)
            effects.append(Publish(alert_event(
                state, "operator", "warning", "CHANNEL_SILENT",
                f"No readings from {device_id} for {state.global_tick - heard} ticks.",
                {"device_id": device_id},
            )))


def _note_device_status(state: BrainState, obs: Observation) -> None:
    device = state.registry.get(obs.device_id)
    if device is None:
        return
    status = state.contained.get(obs.device_id, obs.device_status)
    if device.status is not status:
        state.registry[obs.device_id] = device.model_copy(update={"status": status})


def _water_reading(state: BrainState, obs: Observation, effects: list[Effect]) -> None:
    device_id = obs.device_id
    valve_state = _valve_state(state, device_id)
    # A state we cannot read is treated as no drain; the planner holds on it.
    u = commanded_ratio(valve_state) or 0.0
    water_filter = _update_water_filter(state, obs, u)
    _track_low_band(state, device_id, water_filter, u)
    _track_trust_run(state, obs)
    device = state.registry.get(device_id)
    target = device.actuator.target if device is not None and device.actuator is not None else device_id
    verdict = state.device_verdicts.get(device_id)
    context = WaterContext(
        device_id=device_id,
        zone_id=obs.zone_id,
        filter=water_filter,
        valve_state=valve_state,
        recent=tuple(state.recent.get(device_id, ())),
        below_low_since=state.below_low_since.get(device_id),
        now=state.sim_now,
        plan_version=state.plan_version.get(target, 0) + 1,
        global_tick=state.global_tick,
        target=target,
        untrusted=verdict is not None and verdict.state is TrustState.UNTRUSTED,
        contained=device is not None and device.status is not DeviceStatus.OK,
        min_val=device.min_val if device is not None else 0.0,
        max_val=device.max_val if device is not None else 0.0,
        trusted_since=state.trusted_since.get(device_id),
        trusted_recent=tuple(state.trusted_recent.get(device_id, ())),
    )
    coordinator.note_reading(state, obs)
    effects.extend(coordinator.on_estimate(state))
    window_ticks = round(float(state.policy["verification"]["close_window_s"]) / state.sample_period_s)
    readings = state.window_readings.setdefault(device_id, deque(maxlen=window_ticks + 1))
    readings.append((obs.seq, obs.reading_id, obs.sig_hash))
    _check_recovery(state, target, water_filter, valve_state, readings, effects)
    intent = _plan_water(context, state.policy)
    if intent is not None and coordinator.may_propose(state, intent):
        effects.extend(coordinator.propose(state, intent))
    _note_hold(state, context, effects)


def _plan_water(context: WaterContext, policy) -> "ActionIntent | None":
    """The configured planner: model predictive control, or the T0 fallback (spec 10.3)."""
    if policy["water"].get("planner", "mpc") == "mpc":
        return plan_water_mpc(context, policy)
    return plan_water_t0(context, policy)


def _check_recovery(
    state: BrainState,
    target: str,
    water_filter: WaterFilter,
    valve_state: str,
    readings: Sequence[tuple[int, str, str]],
    effects: list[Effect],
) -> None:
    """A suspect actuator is cleared by behaving as commanded, not by waiting (spec 8.8)."""
    if target not in state.suspect:
        return
    window_s = float(state.policy["verification"]["close_window_s"])
    since_tick = state.global_tick - round(window_s / state.sample_period_s)
    excluded = frozenset(coordinator.untrusted_ticks(state, readings))
    slope = water_filter.observed_slope(since_tick, exclude_ticks=excluded)  # None under 10 usable points
    if slope is not None and matches_commanded(slope, valve_state, state.policy):
        state.suspect.discard(target)
        effects.append(Publish(status_event(state, "actuator_recovered", {"target": target})))


def _valve_state(state: BrainState, device_id: str) -> str:
    device = state.registry.get(device_id)
    if device is None or device.actuator is None or device.actuator.current_state is None:
        return UNKNOWN_VALVE_STATE
    return device.actuator.current_state


def _update_water_filter(state: BrainState, obs: Observation, u: float) -> WaterFilter:
    water_filter = state.filters.get(obs.device_id)
    if water_filter is None:
        water_filter = state.filters[obs.device_id] = WaterFilter(state.params["filter"])
    trust = state.policy["trust"]
    in_range = "OUT_OF_RANGE" not in obs.flags
    weight = 0.0
    if in_range:
        # An expired verdict is still not "no verdict": expired UNTRUSTED must not count as pending.
        # A verdict landing after its reading does not undo that reading's update: rollback is P3.
        latest = state.ledger.latest(obs.reading_id, obs.sig_hash)
        weight = estimate_weight(latest, obs.device_status, trust["caps"], _pending_weight(state, obs))
    water_filter.update(obs.value, obs.seq, weight, u=u, at_ceiling="AT_CEILING" in obs.flags)
    if in_range:
        _remember_evidence(state, obs)
    return water_filter


def _pending_weight(state: BrainState, obs: Observation) -> float:
    """No retroactive trust: an unjudged reading from a Guardian outage never counts (spec 8.9)."""
    if in_outage(obs.sim_time, state.outage_windows):
        return 0.0
    return float(state.policy["trust"]["pending_weight"])


def _track_low_band(state: BrainState, device_id: str, water_filter: WaterFilter, u: float) -> None:
    if not water_filter.initialised:
        return
    low_band = state.policy["water"]["low_band"]
    if water_filter.level > low_band:
        state.below_low_since[device_id] = None
    elif u > 0.0 and state.below_low_since.get(device_id) is None:
        state.below_low_since[device_id] = state.sim_now


def _track_trust_run(state: BrainState, obs: Observation) -> None:
    """How long this channel has been reading trusted, for the moves that require it."""
    verdict = state.ledger.latest(obs.reading_id, obs.sig_hash)
    trusted = (
        verdict is not None
        and verdict.state is TrustState.TRUSTED
        and obs.device_status is DeviceStatus.OK
        and "OUT_OF_RANGE" not in obs.flags
    )
    if not trusted:
        state.trusted_since[obs.device_id] = None
    elif state.trusted_since.get(obs.device_id) is None:
        state.trusted_since[obs.device_id] = state.sim_now


def _note_hold(state: BrainState, context: WaterContext, effects: list[Effect]) -> None:
    """One decision record per change of hold reason, not one per reading."""
    reason = water_hold_reason(context, state.policy)
    _record_hold(state, context.device_id, context.zone_id, context.recent, reason, effects)


def _record_hold(
    state: BrainState,
    device_id: str,
    zone_id: str,
    recent: Sequence[EvidenceRef],
    reason: str | None,
    effects: list[Effect],
) -> None:
    if reason == state.last_hold.get(device_id):
        return
    state.last_hold[device_id] = reason
    if reason is None:
        return
    incident = {"zone": zone_id, "device_id": device_id, "hold_reason": reason}
    effects.append(Publish(decision_event(
        state, None, "hold", incident, coordinator.RULE, recent, (),
        f"Not planning on {device_id}: {reason}.", (WOULD_CHANGE_IF[reason],),
    )))
