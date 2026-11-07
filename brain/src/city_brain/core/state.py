"""Brain state: owned and mutated only by `step`."""

from collections import Counter, deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from city_brain.core.assessor import IncidentBook
from city_brain.core.channel_filter import LagFilter
from city_brain.core.estimator import WaterFilter
from city_brain.core.shield import ShieldResult, TokenBucket
from city_brain.core.trust import VerdictLedger
from city_brain.domain.types import (
    ActionIntent,
    ActionStatus,
    BrainMode,
    DeviceProfile,
    DeviceStatus,
    EvidenceRef,
    Verdict,
)


@dataclass
class DeviceCursor:
    seq: int
    sim_time: datetime


@dataclass
class ActionRecord:
    """One action's lifecycle. `key` is the idempotency key; `shield` the static checks' result."""

    intent: ActionIntent
    status: ActionStatus
    key: str
    dispatched_at: datetime | None
    reasons: tuple[str, ...]
    shield: ShieldResult
    dispatch_tick: int | None = None
    # (tick, reading_id, sig_hash) of the channel's readings while VERIFYING.
    window_readings: list[tuple[int, str, str]] = field(default_factory=list)


def new_token_bucket(policy: Mapping) -> TokenBucket:
    rate_limit = policy["shield"]["rate_limit"]
    return TokenBucket(int(rate_limit["burst"]), float(rate_limit["per_s"]))


@dataclass
class BrainState:
    policy: dict
    params: dict
    bucket: TokenBucket
    epoch: int = 0
    registry: dict[str, DeviceProfile] = field(default_factory=dict)
    cursors: dict[str, DeviceCursor] = field(default_factory=dict)
    # Unbounded within an epoch; bound it in P7.
    seen: set[tuple[str, str]] = field(default_factory=set)
    global_tick: int = 0
    epoch_origin: tuple[datetime, int] | None = None
    filters: dict[str, WaterFilter] = field(default_factory=dict)
    lag_filters: dict[str, LagFilter] = field(default_factory=dict)
    incidents: IncidentBook = field(default_factory=IncidentBook)
    plan_step: dict[str, int] = field(default_factory=dict)  # incident_id -> catalog step
    # R3 actions waiting for an operator: action_id -> the sim time it was asked for (spec 8.7).
    pending_approvals: dict[str, datetime] = field(default_factory=dict)
    counters: Counter[str] = field(default_factory=Counter)
    event_seq: int = 0
    ledger: VerdictLedger = field(default_factory=VerdictLedger)
    allowlist: Mapping[str, frozenset[str]] = field(default_factory=dict)
    actions: dict[str, ActionRecord] = field(default_factory=dict)
    in_flight: set[str] = field(default_factory=set)
    suspect: set[str] = field(default_factory=set)
    last_command_at: dict[str, datetime] = field(default_factory=dict)
    last_blocked_at: dict[str, datetime] = field(default_factory=dict)
    last_refusal: dict[tuple[str, str], tuple[str, ...]] = field(default_factory=dict)
    below_low_since: dict[str, datetime | None] = field(default_factory=dict)
    last_hold: dict[str, str | None] = field(default_factory=dict)
    plan_version: dict[str, int] = field(default_factory=dict)
    recent: dict[str, deque[EvidenceRef]] = field(default_factory=dict)
    # Devices seen going backwards, and the global tick of the first: a restart needs several.
    epoch_candidates: set[str] = field(default_factory=set)
    epoch_candidate_tick: int | None = None
    device_verdicts: dict[str, Verdict] = field(default_factory=dict)  # newest per device
    last_heard: dict[str, int] = field(default_factory=dict)  # tick of the channel's last reading
    # (tick, reading_id, sig_hash) per channel, one recovery window long.
    window_readings: dict[str, deque[tuple[int, str, str]]] = field(default_factory=dict)
    silent: set[str] = field(default_factory=set)  # channels already alerted as silent
    mode: BrainMode = BrainMode.WARMUP
    guardian_heard: bool = False
    # Sim-time intervals the Brain spent in GUARDIAN_DEGRADED; the open one ends with None.
    outage_windows: list[tuple[datetime, datetime | None]] = field(default_factory=list)
    # Devices the Guardian has contained: its word outranks what a reading reports.
    contained: dict[str, DeviceStatus] = field(default_factory=dict)
    # Sim time each watchdog timer was last armed, so a rearm costs one effect per second.
    timers_armed: dict[str, datetime | None] = field(default_factory=dict)

    @classmethod
    def new(cls, policy: dict, params: dict) -> "BrainState":
        return cls(policy=policy, params=params, bucket=new_token_bucket(policy))

    @property
    def zones(self) -> frozenset[str]:
        return frozenset(device.zone_id for device in self.registry.values())

    def channel_filter(self, device_id: str):
        """The filter for a channel, whichever kind of channel it is."""
        found = self.filters.get(device_id)
        return found if found is not None else self.lag_filters.get(device_id)

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
