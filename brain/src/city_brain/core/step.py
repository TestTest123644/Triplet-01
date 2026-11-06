"""The pure transition function: (state, event) -> (state, effects)."""

from collections import deque
from collections.abc import Sequence

from city_brain.core import coordinator
from city_brain.core.estimator import WaterFilter
from city_brain.core.epochs import confirm_candidate, is_new_epoch, start_new_epoch
from city_brain.core.events import (
    CommandResult,
    Effect,
    Event,
    Publish,
    ReadingReceived,
    ReadingRejected,
    RegistryLoaded,
    TimerFired,
    VerdictReceived,
)
from city_brain.core.records import alert_event, decision_event, status_event
from city_brain.core.state import BrainState, DeviceCursor
from city_brain.core.trust import estimate_weight, evidence_ref, trust_weight
from city_brain.core.water import (
    WaterContext,
    commanded_ratio,
    matches_commanded,
    plan_water_t0,
    water_hold_reason,
)
from city_brain.domain.types import EvidenceRef, Observation, SensorKind, TrustState, Verdict

UNKNOWN_VALVE_STATE = ""  # no registry entry or no actuator: the planner holds on it
WOULD_CHANGE_IF = {
    "STALE": "weighted readings arrive on this channel again",
    "UNTRUSTED_READINGS": "the Guardian trusts this device again",
    "INSUFFICIENT_EVIDENCE": "enough readings arrive on this channel to cite",
    "STUCK": "the sensor value moves again",
    "IMPLAUSIBLE_JUMP": "readings agree with the estimate again",
    "MODEL_MISMATCH": "readings stay consistent with the model",
    "UNKNOWN_VALVE_STATE": "the registry reports a known valve state",
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
        if event.keys_changed:
            _start_epoch(state, effects)
    elif isinstance(event, ReadingRejected):
        state.counters[event.code] += 1
    elif isinstance(event, ReadingReceived):
        _ingest(state, event, effects)
    elif isinstance(event, VerdictReceived):
        state.ledger.add(event.verdict)
        _note_device_verdict(state, event.verdict)
        # sim_now is undefined before the first accepted reading; park without pruning.
        if state.epoch_origin is not None:
            state.ledger.prune(state.sim_now)
        effects.extend(coordinator.on_verdict(state, (event.verdict.reading_id, event.verdict.sig_hash)))
    elif isinstance(event, CommandResult):
        effects.extend(coordinator.on_result(state, event))
    elif isinstance(event, TimerFired):
        effects.extend(coordinator.on_timer(state, event))
    return state, effects


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
    if obs.kind is SensorKind.WATER_LEVEL:
        state.last_heard[obs.device_id] = obs.seq
        state.silent.discard(obs.device_id)
    if advanced:
        _sweep_silent_channels(state, effects)
    if obs.kind is SensorKind.WATER_LEVEL:
        _water_reading(state, obs, effects)
    else:
        effects.extend(coordinator.on_estimate(state))


def _start_epoch(state: BrainState, effects: list[Effect]) -> None:
    start_new_epoch(state)
    effects.append(_status(state, "epoch_changed"))
    effects.extend(coordinator.on_new_epoch(state))


def _seq_jumped(state: BrainState, cursor: DeviceCursor | None, obs: Observation, jump_max: int) -> bool:
    if cursor is not None:
        return obs.seq > cursor.seq + jump_max
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
    if device is not None and device.status is not obs.device_status:
        state.registry[obs.device_id] = device.model_copy(update={"status": obs.device_status})


def _water_reading(state: BrainState, obs: Observation, effects: list[Effect]) -> None:
    device_id = obs.device_id
    valve_state = _valve_state(state, device_id)
    # A state we cannot read is treated as no drain; the planner holds on it.
    u = commanded_ratio(valve_state) or 0.0
    water_filter = _update_water_filter(state, obs, u)
    _track_low_band(state, device_id, water_filter, u)
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
    )
    coordinator.note_reading(state, obs)
    effects.extend(coordinator.on_estimate(state))
    _check_recovery(state, target, water_filter, valve_state, effects)
    intent = plan_water_t0(context, state.policy)
    if intent is not None and coordinator.may_propose(state, intent):
        effects.extend(coordinator.propose(state, intent))
    _note_hold(state, context, effects)


def _check_recovery(
    state: BrainState, target: str, water_filter: WaterFilter, valve_state: str, effects: list[Effect]
) -> None:
    """A suspect actuator is cleared by behaving as commanded, not by waiting (spec 8.8)."""
    if target not in state.suspect:
        return
    window_s = float(state.policy["verification"]["close_window_s"])
    slope = water_filter.observed_slope(state.global_tick - round(window_s / state.sample_period_s))
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
        weight = estimate_weight(latest, obs.device_status, trust["caps"], float(trust["pending_weight"]))
    water_filter.update(obs.value, obs.seq, weight, u=u, at_ceiling="AT_CEILING" in obs.flags)
    if in_range:
        recent = state.recent.setdefault(
            obs.device_id, deque(maxlen=int(state.policy["water"]["evidence_count"]))
        )
        verdict = state.ledger.lookup(obs.reading_id, obs.sig_hash, state.sim_now)
        recent.append(evidence_ref(obs, verdict, trust_weight(verdict, obs.device_status, trust["caps"])))
    return water_filter


def _track_low_band(state: BrainState, device_id: str, water_filter: WaterFilter, u: float) -> None:
    if not water_filter.initialised:
        return
    low_band = state.policy["water"]["low_band"]
    if water_filter.level > low_band:
        state.below_low_since[device_id] = None
    elif u > 0.0 and state.below_low_since.get(device_id) is None:
        state.below_low_since[device_id] = state.sim_now


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
