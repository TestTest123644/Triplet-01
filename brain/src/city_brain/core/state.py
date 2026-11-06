"""Brain state: owned and mutated only by `step`."""

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from city_brain.core.estimator import WaterFilter
from city_brain.domain.types import DeviceProfile


@dataclass
class DeviceCursor:
    seq: int
    sim_time: datetime


@dataclass
class BrainState:
    policy: dict
    params: dict
    epoch: int = 0
    registry: dict[str, DeviceProfile] = field(default_factory=dict)
    cursors: dict[str, DeviceCursor] = field(default_factory=dict)
    # Unbounded within an epoch; bound it in P7.
    seen: set[tuple[str, str]] = field(default_factory=set)
    global_tick: int = 0
    epoch_origin: tuple[datetime, int] | None = None
    filters: dict[str, WaterFilter] = field(default_factory=dict)
    counters: Counter[str] = field(default_factory=Counter)
    event_seq: int = 0

    @classmethod
    def new(cls, policy: dict, params: dict) -> "BrainState":
        return cls(policy=policy, params=params)

    @property
    def sample_period_s(self) -> float:
        return float(self.params["filter"]["sample_period_s"])

    @property
    def sim_now(self) -> datetime:
        """Fine-grained sim time: `ts` has whole seconds, so extrapolate by tick."""
        if self.epoch_origin is None:
            raise ValueError("sim_now undefined before the first accepted reading")
        origin_time, origin_seq = self.epoch_origin
        return origin_time + timedelta(seconds=(self.global_tick - origin_seq) * self.sample_period_s)
