"""Core input events and output effects (spec 6.2). All frozen."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

from city_brain.domain.types import DeviceProfile, Observation, Verdict


@dataclass(frozen=True)
class RegistryLoaded:
    devices: tuple[DeviceProfile, ...]
    allowlist: Mapping[str, frozenset[str]] = field(default_factory=dict)  # empty refuses every state


@dataclass(frozen=True)
class ReadingReceived:
    obs: Observation


@dataclass(frozen=True)
class ReadingRejected:
    code: str
    device_id: str | None


@dataclass(frozen=True)
class VerdictReceived:
    verdict: Verdict


@dataclass(frozen=True)
class CommandResult:
    action_id: str
    outcome: Literal["ACCEPTED", "REJECTED", "UNKNOWN"]
    reply: dict


@dataclass(frozen=True)
class TimerFired:
    timer_id: str
    kind: str


Event = RegistryLoaded | ReadingReceived | ReadingRejected | VerdictReceived | CommandResult | TimerFired


@dataclass(frozen=True)
class Publish:
    event: dict


@dataclass(frozen=True)
class SendCommand:
    action_id: str
    idempotency_key: str
    capability: str
    target: str
    state: str


@dataclass(frozen=True)
class StartTimer:
    timer_id: str
    kind: str
    wall_seconds: float


@dataclass(frozen=True)
class CancelTimer:
    timer_id: str


Effect = Publish | SendCommand | StartTimer | CancelTimer
