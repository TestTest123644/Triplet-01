"""Canonical, immutable domain types."""

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict


class SensorKind(StrEnum):
    WATER_LEVEL = "WATER_LEVEL"
    TRAFFIC_FLOW = "TRAFFIC_FLOW"
    POWER_LOAD = "POWER_LOAD"
    AIR_QUALITY = "AIR_QUALITY"
    CALL_RATE = "CALL_RATE"


class CanonicalUnit(StrEnum):
    M = "m"
    VEH_PER_MIN = "veh_per_min"
    MW = "MW"
    AQI = "AQI"
    CALLS_PER_MIN = "calls_per_min"


class DeviceStatus(StrEnum):
    OK = "OK"
    ISOLATED = "ISOLATED"
    QUARANTINED = "QUARANTINED"
    ROLLED_BACK = "ROLLED_BACK"
    MODELLED = "MODELLED"


class TrustState(StrEnum):
    TRUSTED = "TRUSTED"
    DOUBTFUL = "DOUBTFUL"
    UNTRUSTED = "UNTRUSTED"
    UNKNOWN = "UNKNOWN"


class StateKind(StrEnum):
    OBSERVED = "OBSERVED"
    VALIDATED = "VALIDATED"
    ESTIMATED = "ESTIMATED"
    UNKNOWN = "UNKNOWN"


class RiskClass(StrEnum):
    R0 = "R0"
    R1 = "R1"
    R2 = "R2"
    R3 = "R3"


class IncidentKind(StrEnum):
    FLOOD_RISK = "FLOOD_RISK"
    CONGESTION = "CONGESTION"
    GRID_STRESS = "GRID_STRESS"
    AIR_QUALITY = "AIR_QUALITY"
    CALL_SURGE = "CALL_SURGE"


class CorrelationLabel(StrEnum):
    CONFIRMED = "CONFIRMED"
    PROBABLE = "PROBABLE"
    HYPOTHETICAL = "HYPOTHETICAL"


class BrainMode(StrEnum):
    WARMUP = "WARMUP"
    NORMAL = "NORMAL"
    GUARDIAN_DEGRADED = "GUARDIAN_DEGRADED"
    TWIN_DEGRADED = "TWIN_DEGRADED"
    PAUSED = "PAUSED"


class CapabilityTier(StrEnum):
    T0 = "T0"
    T1 = "T1"
    T2 = "T2"
    T3 = "T3"


class ActionStatus(StrEnum):
    PROPOSED = "PROPOSED"
    REFUSED = "REFUSED"
    AWAITING_TRUST = "AWAITING_TRUST"
    BLOCKED = "BLOCKED"
    DISPATCHED = "DISPATCHED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    VERIFYING = "VERIFYING"
    VERIFIED = "VERIFIED"
    UNVERIFIED = "UNVERIFIED"
    SUPERSEDED = "SUPERSEDED"


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


class GuardianHeartbeat(_Frozen):
    """The Guardian says it has judged every reading up to `evaluated_through` (spec 8.9)."""

    evaluated_through: datetime
    issued_at: datetime


class GuardianContainment(_Frozen):
    """The Guardian has contained (or released) a device."""

    device_id: str
    action: Literal["isolated", "quarantined", "released", "rolled_back"]
    effective_at: datetime
    reasons: frozenset[str]
    issued_at: datetime


class GuardianRevocation(_Frozen):
    """The Guardian withdraws a verdict it issued earlier."""

    verdict_id: str
    issued_at: datetime


GuardianNotice = GuardianHeartbeat | GuardianContainment | GuardianRevocation


class CorrelationLink(_Frozen):
    """Why two findings are being read together, and how firmly (spec 8.3.4)."""

    source_id: str
    target_id: str
    label: CorrelationLabel
    reason: str


class Incident(_Frozen):
    """One situation the Brain believes it is looking at, in one zone (spec 8.3.3)."""

    incident_id: str
    zone_id: str
    kind: IncidentKind
    belief: float
    severity: Literal["low", "medium", "high", "critical"]
    status: Literal["suspected", "confirmed", "mitigating", "resolved", "dismissed"]
    findings: tuple[str, ...]
    correlations: tuple[CorrelationLink, ...]
    opened_at: datetime
    updated_at: datetime


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
    device_status: DeviceStatus = DeviceStatus.OK  # as reported when the reading arrived


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
