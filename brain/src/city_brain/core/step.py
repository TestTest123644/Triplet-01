"""The pure transition function: (state, event) -> (state, effects)."""

from collections import deque

from city_brain.core import coordinator
from city_brain.core.estimator import WaterFilter
from city_brain.core.epochs import is_new_epoch, start_new_epoch
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
from city_brain.core.records import decision_event, status_event
from city_brain.core.state import BrainState, DeviceCursor
from city_brain.core.trust import estimate_weight, evidence_ref, trust_weight
from city_brain.core.water import WaterContext, commanded_ratio, plan_water_t0, water_hold_reason
from city_brain.domain.types import Observation, SensorKind

UNKNOWN_VALVE_STATE = ""  # no registry entry or no actuator: the planner holds on it
WOULD_CHANGE_IF = {
    "STALE": "weighted readings arrive on this channel again",
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
    elif isinstance(event, ReadingRejected):
        state.counters[event.code] += 1
    elif isinstance(event, ReadingReceived):
        _ingest(state, event, effects)
    elif isinstance(event, VerdictReceived):
        state.ledger.add(event.verdict)
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
    if is_new_epoch(cursor, obs, int(epochs["seq_drop"]), float(epochs["ts_back_s"])):
        start_new_epoch(state)
        effects.append(_status(state, "epoch_changed"))
        effects.extend(coordinator.on_new_epoch(state))
        cursor = None
    key = (obs.reading_id, obs.sig_hash)
    if key in state.seen:
        state.counters["DUPLICATE"] += 1
        return
    if cursor is not None and obs.seq <= cursor.seq:
        state.counters["OUT_OF_ORDER"] += 1
        return
    state.seen.add(key)
    state.cursors[obs.device_id] = DeviceCursor(seq=obs.seq, sim_time=obs.sim_time)
    if state.epoch_origin is None:
        state.epoch_origin = (obs.sim_time, obs.seq)
    state.global_tick = max(state.global_tick, obs.seq)
    _note_device_status(state, obs)
    if obs.kind is SensorKind.WATER_LEVEL:
        _water_reading(state, obs, effects)
    else:
        effects.extend(coordinator.on_estimate(state))


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
    context = WaterContext(
        device_id=device_id,
        zone_id=obs.zone_id,
        filter=water_filter,
        valve_state=valve_state,
        recent=tuple(state.recent.get(device_id, ())),
        below_low_since=state.below_low_since.get(device_id),
        now=state.sim_now,
        plan_version=state.plan_version.get(device_id, 0) + 1,
        global_tick=state.global_tick,
    )
    coordinator.note_reading(state, obs)
    effects.extend(coordinator.on_estimate(state))
    intent = plan_water_t0(context, state.policy)
    if intent is not None and coordinator.may_propose(state, intent):
        effects.extend(coordinator.propose(state, intent))
    _note_hold(state, context, effects)


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
    device_id = context.device_id
    reason = water_hold_reason(context, state.policy)
    if reason == state.last_hold.get(device_id):
        return
    state.last_hold[device_id] = reason
    if reason is None:
        return
    incident = {"zone": context.zone_id, "device_id": device_id, "hold_reason": reason}
    effects.append(Publish(decision_event(
        state, None, "hold", incident, coordinator.RULE, context.recent, (),
        f"Not planning on {device_id}: {reason}.", (WOULD_CHANGE_IF[reason],),
    )))
