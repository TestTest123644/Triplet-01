"""Canonical, immutable domain types."""

from datetime import datetime
from enum import StrEnum, auto
from typing import Literal

from pydantic import BaseModel, ConfigDict


class SensorKind(StrEnum):
    WATER_LEVEL = auto()
    TRAFFIC_FLOW = auto()
    POWER_LOAD = auto()
    AIR_QUALITY = auto()
    CALL_RATE = auto()


class CanonicalUnit(StrEnum):
    M = auto()
    VEH_PER_MIN = auto()
    MW = auto()
    AQI = auto()
    CALLS_PER_MIN = auto()


class DeviceStatus(StrEnum):
    OK = auto()
    ISOLATED = auto()
    QUARANTINED = auto()
    ROLLED_BACK = auto()
    MODELLED = auto()


class TrustState(StrEnum):
    TRUSTED = auto()
    DOUBTFUL = auto()
    UNTRUSTED = auto()
    UNKNOWN = auto()


class StateKind(StrEnum):
    OBSERVED = auto()
    VALIDATED = auto()
    ESTIMATED = auto()
    UNKNOWN = auto()


class RiskClass(StrEnum):
    R0 = "R0"
    R1 = "R1"
    R2 = "R2"
    R3 = "R3"


class ActionStatus(StrEnum):
    PROPOSED = auto()
    REFUSED = auto()
    AWAITING_TRUST = auto()
    BLOCKED = auto()
    DISPATCHED = auto()
    REJECTED = auto()
    UNKNOWN = auto()
    VERIFYING = auto()
    VERIFIED = auto()
    UNVERIFIED = auto()
    SUPERSEDED = auto()


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True)


class Observation(_Frozen):
    reading_id: str
    sig_hash: str
    device_id: str
    zone_id: str
    kind: SensorKind
    seq: int
    sim_time: datetime
    value: float
    unit: CanonicalUnit
    device_status: DeviceStatus
    flags: frozenset[str]


class Verdict(_Frozen):
    verdict_id: str
    reading_id: str
    sig_hash: str
    device_id: str
    state: TrustState
    trust: float | None
    reasons: frozenset[str]
    issued_at: datetime
    expires_at: datetime


class ActuatorCapability(_Frozen):
    capability: Literal["valve", "signal", "grid", "dispatch"]
    target: str
    states: frozenset[str]
    current_state: str | None


class DeviceProfile(_Frozen):
    device_id: str
    zone_id: str
    kind: SensorKind
    unit: CanonicalUnit
    min_val: float
    max_val: float
    status: DeviceStatus
    actuator: ActuatorCapability | None


class EvidenceRef(_Frozen):
    reading_id: str
    sig_hash: str
    device_id: str
    value: float
    sim_time: datetime
    state_kind: StateKind
    verdict_id: str | None
    trust_state: TrustState
    weight: float


class ExpectedEffect(_Frozen):
    channel: str
    metric: Literal["rate", "level"]
    comparator: Literal["<=", ">="]
    threshold: float
    window_s: float


class Alternative(_Frozen):
    option: str
    rejected_because: str


class ActionIntent(_Frozen):
    action_id: str
    incident_id: str
    plan_version: int
    capability: str
    target: str
    state: str
    risk_class: RiskClass
    expected_effect: ExpectedEffect
    evidence: tuple[EvidenceRef, ...]
    rationale: str
    alternatives: tuple[Alternative, ...]
