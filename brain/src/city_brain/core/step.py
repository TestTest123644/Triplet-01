"""The pure transition function: (state, event) -> (state, effects)."""

from city_brain.core.estimator import WaterFilter
from city_brain.core.epochs import is_new_epoch, start_new_epoch
from city_brain.core.events import (
    Effect,
    Event,
    Publish,
    ReadingReceived,
    ReadingRejected,
    RegistryLoaded,
    VerdictReceived,
)
from city_brain.core.state import BrainState, DeviceCursor
from city_brain.domain.types import DeviceStatus, Observation, SensorKind


def _status(state: BrainState, reason: str) -> Publish:
    # Placeholder until Task 14 supplies the CloudEvents builder.
    return Publish({"type": "status", "data": {"reason": reason, "epoch": state.epoch}})


def step(state: BrainState, event: Event) -> tuple[BrainState, list[Effect]]:
    effects: list[Effect] = []
    if isinstance(event, RegistryLoaded):
        state.registry = {d.device_id: d for d in event.devices}
        effects.append(_status(state, "startup"))
    elif isinstance(event, ReadingRejected):
        state.counters[event.code] += 1
    elif isinstance(event, ReadingReceived):
        _ingest(state, event, effects)
    elif isinstance(event, VerdictReceived):
        state.ledger.add(event.verdict)
        # sim_now is undefined before the first accepted reading; park without pruning.
        if state.epoch_origin is not None:
            state.ledger.prune(state.sim_now)
        # Task 15 re-evaluates pending actions here.
    return state, effects


def _ingest(state: BrainState, event: ReadingReceived, effects: list[Effect]) -> None:
    obs = event.obs
    epochs = {**state.policy.get("epochs", {}), **state.params.get("epochs", {})}
    cursor = state.cursors.get(obs.device_id)
    if is_new_epoch(cursor, obs, int(epochs["seq_drop"]), float(epochs["ts_back_s"])):
        start_new_epoch(state)
        effects.append(_status(state, "epoch_changed"))
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
    if obs.kind is SensorKind.WATER_LEVEL:
        _update_water_filter(state, obs)


def _update_water_filter(state: BrainState, obs: Observation) -> None:
    water_filter = state.filters.get(obs.device_id)
    if water_filter is None:
        water_filter = state.filters[obs.device_id] = WaterFilter(state.params["filter"])
    # Task 15 replaces this weight with `estimate_weight` and `u` with the commanded ratio.
    weight = 1.0 if obs.device_status is DeviceStatus.OK else 0.0
    water_filter.update(
        obs.value, obs.seq, weight, u=0.0, at_ceiling="AT_CEILING" in obs.flags
    )
