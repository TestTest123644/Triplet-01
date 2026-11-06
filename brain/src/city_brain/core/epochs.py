"""Run-epoch detection and reset (spec 9.3)."""

from city_brain.core.state import BrainState, DeviceCursor
from city_brain.domain.types import Observation


def is_new_epoch(cursor: DeviceCursor | None, obs: Observation, seq_drop: int, ts_back_s: float) -> bool:
    if cursor is None:
        return False
    if cursor.seq - obs.seq > seq_drop:
        return True
    return (cursor.sim_time - obs.sim_time).total_seconds() > ts_back_s


def start_new_epoch(state: BrainState) -> None:
    """Advance the epoch and clear per-epoch state. Later tasks extend this."""
    state.epoch += 1
    state.event_seq = 0
    state.seen.clear()
    state.cursors.clear()
    state.global_tick = 0
    state.epoch_origin = None
    state.filters.clear()
    state.ledger.clear()
