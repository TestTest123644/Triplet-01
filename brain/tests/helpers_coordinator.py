"""Drive `step` with water channels, for the coordinator tests.

`state.last_effects` accumulates every effect since the state was built; `state.levels` holds
the last value fed per device. Both live on the state only so the tests read like the brief.
"""

from datetime import timedelta

from city_brain.config import load_config
from city_brain.core.events import ReadingReceived, RegistryLoaded, VerdictReceived
from city_brain.core.state import BrainState
from city_brain.core.step import step
from city_brain.domain.types import (
    ActuatorCapability,
    CanonicalUnit,
    DeviceProfile,
    DeviceStatus,
    SensorKind,
    TrustState,
)
from tests.factories import T0, make_observation, make_verdict

CFG = load_config("config/standalone.yaml")
ALLOWLIST = {"valve": frozenset({"open", "closed", "ratio"})}
PERIOD_S = CFG.params["filter"]["sample_period_s"]
RISE_RATE = 0.02  # m/s, valve closed
DRAIN_RATE = -0.025  # m/s, valve open and working


def water_device(device_id, valve="closed"):
    return DeviceProfile(
        device_id=device_id, zone_id="zone-1", kind=SensorKind.WATER_LEVEL, unit=CanonicalUnit.M,
        min_val=0.0, max_val=12.0, status=DeviceStatus.OK,
        actuator=ActuatorCapability(capability="valve", target=device_id,
                                    states=frozenset({"open", "closed"}), current_state=valve),
    )


def state_with_registry(*device_ids, valve="closed", allowlist=ALLOWLIST):
    state = BrainState.new(CFG.policy, CFG.params)
    devices = tuple(water_device(d, valve) for d in (device_ids or ("z1-wl1",)))
    state, effects = step(state, RegistryLoaded(devices, allowlist))
    state.last_effects = list(effects)
    state.levels = {}
    return state


def of_type(kind, effects):
    return [e for e in effects if isinstance(e, kind)]


def only(kind, effects):
    matches = of_type(kind, effects)
    assert len(matches) == 1, matches
    return matches[0]


def reading_id(device_id, seq):
    return f"r-{device_id}-{seq}"


def seq_of(ref):
    return int(ref.reading_id.rsplit("-", 1)[1])


def verdict_for(device_id, seq, state=TrustState.TRUSTED, **overrides):
    issued = T0 + timedelta(seconds=seq * PERIOD_S)
    fields = dict(
        verdict_id=f"v-{device_id}-{seq}", reading_id=reading_id(device_id, seq),
        sig_hash=f"sig-{seq}", device_id=device_id, state=state, issued_at=issued,
        expires_at=issued + timedelta(seconds=30),
    )
    fields.update(overrides)
    return make_verdict(**fields)


def apply(state, event):
    """One `step`, adding its effects to `state.last_effects`."""
    so_far, levels = state.last_effects, state.levels
    state, effects = step(state, event)
    state.last_effects, state.levels = so_far + effects, levels
    return state


def feed_tick(state, values, verdict=TrustState.TRUSTED, verdict_first=False, seq=None, **obs_fields):
    """One sample period: a reading per device, each with its verdict (`None` for no verdict)."""
    seq = state.global_tick + 1 if seq is None else seq
    for device_id, value in values.items():
        observation = ReadingReceived(make_observation(
            reading_id=reading_id(device_id, seq), sig_hash=f"sig-{seq}", device_id=device_id,
            seq=seq, sim_time=T0 + timedelta(seconds=seq * PERIOD_S), value=value, **obs_fields,
        ))
        arrival = [observation]
        if verdict is not None:
            event = VerdictReceived(verdict_for(device_id, seq, verdict))
            arrival = [event, observation] if verdict_first else [observation, event]
        for event in arrival:
            state = apply(state, event)
        state.levels[device_id] = value
    return state


def feed_channel(state, device_id, seconds, rate, verdict=TrustState.TRUSTED, **tick_fields):
    start = state.levels[device_id]
    for n in range(1, round(seconds / PERIOD_S) + 1):
        state = feed_tick(state, {device_id: start + rate * PERIOD_S * n}, verdict, **tick_fields)
    return state


def feed_rising_channel(state, device_id, until_level, verdict=TrustState.TRUSTED, rate=RISE_RATE,
                        **tick_fields):
    level = state.levels.get(device_id, until_level - 0.3)
    while level < until_level:
        state = feed_tick(state, {device_id: level}, verdict, **tick_fields)
        level = round(level + rate * PERIOD_S, 6)
    return state


def feed_falling_channel(state, device_id, seconds, rate=DRAIN_RATE, verdict=TrustState.TRUSTED):
    return feed_channel(state, device_id, seconds, rate, verdict)
