"""Drive `step` with a whole zone: cameras, tanks, a feeder, air quality and the call centre.

Like `helpers_coordinator`, the Brain is put past WARMUP on purpose: the mode machine has its
own tests, and the Shield would otherwise refuse every R1 before the gate under test.
"""

from datetime import timedelta

from city_brain.config import load_config
from city_brain.core.events import (
    CommandResult,
    Publish,
    ReadingReceived,
    RegistryLoaded,
    SendCommand,
    VerdictReceived,
)
from city_brain.core.state import BrainState
from city_brain.core.step import step
from city_brain.domain.types import (
    ActuatorCapability,
    BrainMode,
    CanonicalUnit,
    DeviceProfile,
    DeviceStatus,
    SensorKind,
    TrustState,
)
from tests.factories import T0, make_observation, make_verdict

CFG = load_config("config/standalone.yaml")
POLICY, PARAMS = CFG.policy, CFG.params
PERIOD_S = PARAMS["filter"]["sample_period_s"]
ZONE = "zone-1"
# A dead-flat channel reads as STUCK, so every channel dithers; the water filter is far
# tighter than the others, so its dither has to be far smaller than theirs.
NOISE = 0.05
WATER_NOISE = 0.002

ALLOWLIST = {
    "valve": frozenset({"open", "closed", "ratio"}),
    "signal": frozenset({"boost", "normal", "restrict"}),
    "grid": frozenset({"primary", "secondary", "shed", "standby"}),
    "dispatch": frozenset({"flood", "traffic_accident", "medical_surge", "fire", "police"}),
}
SPEC = {
    # suffix: (kind, unit, min, max, calm value, capability, initial state)
    "tc1": (SensorKind.TRAFFIC_FLOW, CanonicalUnit.VEH_PER_MIN, 0.0, 200.0, 45.0, "signal", "normal"),
    "tc2": (SensorKind.TRAFFIC_FLOW, CanonicalUnit.VEH_PER_MIN, 0.0, 200.0, 38.0, "signal", "normal"),
    "wl1": (SensorKind.WATER_LEVEL, CanonicalUnit.M, 0.0, 12.0, 3.2, "valve", "closed"),
    "pl1": (SensorKind.POWER_LOAD, CanonicalUnit.MW, 5.0, 120.0, 48.0, "grid", "primary"),
    "aq1": (SensorKind.AIR_QUALITY, CanonicalUnit.AQI, 10.0, 350.0, 42.0, None, None),
    "ec1": (SensorKind.CALL_RATE, CanonicalUnit.CALLS_PER_MIN, 0.0, 30.0, 1.0, None, None),
}


def device_id(suffix, zone=ZONE):
    return f"{zone}:{suffix}"


def profile(suffix, zone=ZONE, status=DeviceStatus.OK, state=None):
    kind, unit, low, high, _, capability, initial = SPEC[suffix]
    actuator = None
    if capability is not None:
        actuator = ActuatorCapability(
            capability=capability, target=device_id(suffix, zone),
            states=ALLOWLIST[capability], current_state=initial if state is None else state,
        )
    return DeviceProfile(
        device_id=device_id(suffix, zone), zone_id=zone, kind=kind, unit=unit,
        min_val=low, max_val=high, status=status, actuator=actuator,
    )


def calm(suffix):
    return SPEC[suffix][4]


def city_state(*zones, mode=BrainMode.NORMAL, **overrides):
    """A registry of whole zones, with the Brain past WARMUP."""
    zones = zones or (ZONE,)
    devices = tuple(
        profile(suffix, zone, **overrides.get(suffix, {})) for zone in zones for suffix in SPEC
    )
    state = BrainState.new(POLICY, PARAMS)
    state, effects = step(state, RegistryLoaded(devices, ALLOWLIST))
    state.mode = mode
    state.guardian_heard = mode is not BrainMode.WARMUP
    state.last_effects = list(effects)
    state.levels = {}
    return state


def apply(state, event, reply="ACCEPTED"):
    """One `step`. A cooperative Twin answers every command it is sent, as the runtime would."""
    so_far, levels = state.last_effects, state.levels
    state, effects = step(state, event)
    state.last_effects, state.levels = so_far + effects, levels
    if reply is not None:
        for effect in effects:
            if isinstance(effect, SendCommand):
                state = apply(state, CommandResult(effect.action_id, reply, {}), reply=None)
    return state


def tick(state, values=None, verdict=TrustState.TRUSTED, seq=None, zones=(ZONE,), reasons=(),
         reply="ACCEPTED", **obs_fields):
    """One sample period for every registered device. `values` overrides a device's reading."""
    values = values or {}
    seq = state.global_tick + 1 if seq is None else seq
    sim_time = T0 + timedelta(seconds=seq * PERIOD_S)
    for zone in zones:
        for suffix in SPEC:
            name = device_id(suffix, zone)
            if name not in state.registry:
                continue
            step_size = WATER_NOISE if SPEC[suffix][0] is SensorKind.WATER_LEVEL else NOISE
            dither = step_size if seq % 2 else -step_size
            # Every channel dithers, overridden or not: a dead-flat one would read as STUCK.
            value = values.get(name, values.get(suffix, calm(suffix))) + dither
            observation = ReadingReceived(make_observation(
                reading_id=f"r-{name}-{seq}", sig_hash=f"sig-{name}-{seq}", device_id=name,
                zone_id=zone, kind=SPEC[suffix][0], unit=SPEC[suffix][1], seq=seq,
                sim_time=sim_time, value=value, **obs_fields,
            ))
            state = apply(state, observation, reply=reply)
            if verdict is not None:
                state = apply(state, reply=reply, event=VerdictReceived(make_verdict(
                    verdict_id=f"v-{name}-{seq}", reading_id=f"r-{name}-{seq}",
                    sig_hash=f"sig-{name}-{seq}", device_id=name, state=verdict,
                    trust=1.0 if verdict is TrustState.TRUSTED else 0.5,
                    reasons=frozenset(reasons),
                    issued_at=sim_time, expires_at=sim_time + timedelta(seconds=30),
                )))
            state.levels[name] = value
    return state


def ticks(state, n, values=None, **kw):
    for _ in range(n):
        state = tick(state, values, **kw)
    return state


def published(state, kind):
    return [
        e.event for e in state.last_effects
        if isinstance(e, Publish) and e.event["type"] == f"tn.tsyp.brain.{kind}.v1"
    ]


def commands(state, capability=None):
    out = [e["data"] for e in published(state, "command")]
    return [c for c in out if capability is None or c["capability"] == capability]


def dispatched(state, capability=None):
    return [c for c in commands(state, capability) if c["status"] == "DISPATCHED"]
