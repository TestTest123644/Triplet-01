"""Degradation modes: what the Brain may still do when a partner goes quiet (spec 8.9).

`outage_windows` is the record of when the Guardian was known to be down. A window opens when
the Guardian is confirmed silent (its watchdog fires, or its watermark falls too far behind)
and closes only when the Guardian is heard from again with an acceptable lag — never because
the mode moved for some other reason. Readings taken inside a window are never trusted
retroactively (`core.trust.in_outage`).
"""

from collections.abc import Mapping
from datetime import datetime

from city_brain.core.events import Effect, OperatorCommand, Publish, StartTimer, TimerFired
from city_brain.core.records import status_event
from city_brain.core.state import BrainState
from city_brain.domain.types import BrainMode

GUARDIAN_HB_TIMER = "guardian_hb"
GUARDIAN_HB_KIND = "guardian_heartbeat"
TWIN_STALE_TIMER = "twin_stale"
TWIN_STALE_KIND = "twin_stale"
# Rearming on every verdict would cost one effect per reading; the watchdogs are seconds long.
REARM_MIN_S = 1.0


def _modes(state: BrainState) -> Mapping:
    return state.policy["modes"]


def _sim_now(state: BrainState) -> datetime | None:
    """Sim time, or None before the first accepted reading places the timeline."""
    return None if state.epoch_origin is None else state.sim_now


def guardian_down(state: BrainState) -> bool:
    return bool(state.outage_windows) and state.outage_windows[-1][1] is None


def resting_mode(state: BrainState) -> BrainMode:
    """The mode to settle in once nothing else is wrong."""
    if not state.guardian_heard or not state.registry:
        return BrainMode.WARMUP
    return BrainMode.GUARDIAN_DEGRADED if guardian_down(state) else BrainMode.NORMAL


def _open_outage(state: BrainState) -> None:
    now = _sim_now(state)
    if not guardian_down(state) and now is not None:
        state.outage_windows.append((now, None))


def _close_outage(state: BrainState) -> None:
    now = _sim_now(state)
    if guardian_down(state) and now is not None:
        start, _ = state.outage_windows[-1]
        state.outage_windows[-1] = (start, now)


def _lag_ok(state: BrainState, key: str) -> bool:
    """Whether the Guardian's watermark is close enough to now. No watermark: nothing to judge."""
    watermark, now = state.ledger.watermark, _sim_now(state)
    if watermark is None or now is None:
        return True
    return (now - watermark).total_seconds() <= float(_modes(state)[key])


def _rearm(state: BrainState, timer_id: str, kind: str, timeout_s: float) -> list[Effect]:
    now = _sim_now(state)
    last = state.timers_armed.get(timer_id)
    if now is not None and last is not None and (now - last).total_seconds() < REARM_MIN_S:
        return []
    state.timers_armed[timer_id] = now
    return [StartTimer(timer_id, kind, timeout_s)]


def transition_mode(state: BrainState, reason: str, new_mode: BrainMode) -> list[Effect]:
    """Move to `new_mode`, announcing it. A move to the current mode is not a change."""
    if state.mode is new_mode:
        return []
    previous, state.mode = state.mode, new_mode
    effects: list[Effect] = [Publish(status_event(state, "mode_changed", {
        "mode": new_mode, "from": previous, "cause": reason,
    }))]
    if new_mode is BrainMode.NORMAL:  # healthy again: arm the Guardian watchdog
        state.timers_armed[GUARDIAN_HB_TIMER] = _sim_now(state)
        effects.append(StartTimer(
            GUARDIAN_HB_TIMER, GUARDIAN_HB_KIND, float(_modes(state)["guardian_heartbeat_timeout_s"])))
    return effects


def on_registry(state: BrainState) -> list[Effect]:
    if state.mode is BrainMode.WARMUP and state.guardian_heard:
        return transition_mode(state, "registry_loaded", BrainMode.NORMAL)
    return []


def on_guardian_heard(state: BrainState) -> list[Effect]:
    """A heartbeat, verdict or notice arrived: the Guardian is alive."""
    effects = _rearm(state, GUARDIAN_HB_TIMER, GUARDIAN_HB_KIND,
                     float(_modes(state)["guardian_heartbeat_timeout_s"]))
    if state.mode is BrainMode.WARMUP:
        if state.registry:
            effects.extend(transition_mode(state, "guardian_heard", BrainMode.NORMAL))
        return effects
    if guardian_down(state) and _lag_ok(state, "verdict_lag_recover_s"):
        _close_outage(state)
        if state.mode is BrainMode.GUARDIAN_DEGRADED:
            effects.extend(transition_mode(state, "guardian_recovered", BrainMode.NORMAL))
    return effects


def on_reading(state: BrainState) -> list[Effect]:
    """An accepted reading: the Twin is alive, and the Guardian's lag can be checked."""
    effects = _rearm(state, TWIN_STALE_TIMER, TWIN_STALE_KIND,
                     float(_modes(state)["twin_stale_timeout_s"]))
    if state.mode is BrainMode.TWIN_DEGRADED:
        effects.extend(transition_mode(state, "twin_recovered", resting_mode(state)))
    elif state.mode is BrainMode.NORMAL and not _lag_ok(state, "verdict_lag_degraded_s"):
        _open_outage(state)
        effects.extend(transition_mode(state, "verdict_lag", BrainMode.GUARDIAN_DEGRADED))
    return effects


def on_timer(state: BrainState, timer: TimerFired) -> list[Effect]:
    if timer.kind == GUARDIAN_HB_KIND:
        _open_outage(state)
        if state.mode in (BrainMode.WARMUP, BrainMode.NORMAL):
            return transition_mode(state, "guardian_silent", BrainMode.GUARDIAN_DEGRADED)
        return []
    if timer.kind == TWIN_STALE_KIND and state.mode is not BrainMode.PAUSED:
        return transition_mode(state, "twin_silent", BrainMode.TWIN_DEGRADED)
    return []


def on_operator(state: BrainState, command: OperatorCommand) -> list[Effect]:
    if command.action == "pause":
        return transition_mode(state, f"operator_pause:{command.operator_id}", BrainMode.PAUSED)
    if command.action == "resume" and state.mode is BrainMode.PAUSED:
        return transition_mode(state, f"operator_resume:{command.operator_id}", resting_mode(state))
    return []
