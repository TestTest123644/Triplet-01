"""Run-epoch detection and reset (spec 9.3)."""

from city_brain.core.state import BrainState, DeviceCursor, new_token_bucket
from city_brain.domain.types import Observation


def is_new_epoch(cursor: DeviceCursor | None, obs: Observation, seq_drop: int, ts_back_s: float) -> bool:
    if cursor is None:
        return False
    if cursor.seq - obs.seq > seq_drop:
        return True
    return (cursor.sim_time - obs.sim_time).total_seconds() > ts_back_s


def start_new_epoch(state: BrainState) -> None:
    """Advance the epoch and clear per-epoch state.

    `actions`, `plan_version` and `suspect` survive: history stays, action ids stay unique, and a
    suspect actuator stays suspect. Sim time rewinds, so everything stamped with it goes too.
    """
    state.epoch += 1
    state.event_seq = 0
    state.seen.clear()
    state.cursors.clear()
    state.global_tick = 0
    state.epoch_origin = None
    state.filters.clear()
    state.ledger.clear()
    state.in_flight.clear()
    state.last_command_at.clear()
    state.last_blocked_at.clear()
    state.last_refusal.clear()
    state.below_low_since.clear()
    state.last_hold.clear()
    state.recent.clear()
    state.bucket = new_token_bucket(state.policy)
