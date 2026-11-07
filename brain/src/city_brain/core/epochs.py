"""Run-epoch detection and reset (spec 9.3)."""

from city_brain.core.assessor import IncidentBook
from city_brain.core.state import BrainState, DeviceCursor, new_token_bucket
from city_brain.domain.types import Observation


def is_new_epoch(cursor: DeviceCursor | None, obs: Observation, seq_drop: int, ts_back_s: float) -> bool:
    if cursor is None:
        return False
    if cursor.seq - obs.seq > seq_drop:
        return True
    return (cursor.sim_time - obs.sim_time).total_seconds() > ts_back_s


def confirm_candidate(state: BrainState, device_id: str, confirm_devices: int, confirm_ticks: int) -> bool:
    """Count one device going backwards; True once enough distinct devices did so in the window.

    One device alone proves nothing: a replayed or misrouted reading must not wipe safety state.
    """
    first = state.epoch_candidate_tick
    if first is None or state.global_tick - first > confirm_ticks:
        state.epoch_candidate_tick = state.global_tick
        state.epoch_candidates.clear()
    state.epoch_candidates.add(device_id)
    return len(state.epoch_candidates) >= confirm_devices


def start_new_epoch(state: BrainState) -> None:
    """Advance the epoch and clear per-epoch state.

    `actions` and `plan_version` survive: history stays and action ids stay unique. Sim time
    rewinds, so everything stamped with it goes too. A restarted Twin starts its actuators afresh,
    so `suspect` goes as well.
    """
    state.epoch += 1
    state.event_seq = 0
    state.seen.clear()
    state.cursors.clear()
    state.global_tick = 0
    state.epoch_origin = None
    state.filters.clear()
    state.lag_filters.clear()
    state.ledger.clear()
    state.in_flight.clear()
    state.suspect.clear()
    state.last_command_at.clear()
    state.last_blocked_at.clear()
    state.last_refusal.clear()
    state.below_low_since.clear()
    state.trusted_since.clear()
    state.last_hold.clear()
    state.recent.clear()
    state.epoch_candidates.clear()
    state.epoch_candidate_tick = None
    state.device_verdicts.clear()
    state.last_heard.clear()
    state.window_readings.clear()
    state.silent.clear()
    state.timers_armed.clear()
    # The incidents were read off the old run's channels; the new run has to earn them again.
    state.incidents = IncidentBook()
    state.plan_step.clear()
    state.bucket = new_token_bucket(state.policy)
