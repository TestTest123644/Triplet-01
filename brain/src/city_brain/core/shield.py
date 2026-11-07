"""Shield static checks (spec 8.6, checks 1-8) and the sim-time token bucket."""

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from city_brain.core.water import commanded_ratio
from city_brain.domain.types import ActionIntent, BrainMode, DeviceProfile, DeviceStatus, RiskClass

CHECK_NAMES = (
    "STATE_NOT_ALLOWED",
    "UNKNOWN_TARGET",
    "ACTUATOR_SUSPECT",
    "TARGET_CONTAINED",
    "NO_OP",
    "IN_FLIGHT",
    "CONFLICT",
    "COOLDOWN",
    "RATE_LIMIT",
    "MODE_FORBIDS",
)
# What each mode may actuate (spec 8.9). Everything outside NORMAL is baseline-only.
RISK_BY_MODE = {
    BrainMode.NORMAL: frozenset(RiskClass),
    BrainMode.WARMUP: frozenset({RiskClass.R0}),
    BrainMode.GUARDIAN_DEGRADED: frozenset({RiskClass.R0}),
    BrainMode.TWIN_DEGRADED: frozenset({RiskClass.R0}),
    BrainMode.PAUSED: frozenset({RiskClass.R0}),
}

# Canonical ratio form: no leading zeros, 0..100.
_RATIO = re.compile(r"ratio:(0|[1-9][0-9]?|100)")


class TokenBucket:
    """Token bucket refilled on sim time, so it is deterministic."""

    def __init__(self, burst: int, per_s: float) -> None:
        self._burst = float(burst)
        self._per_s = per_s
        self._tokens = float(burst)
        self._last: datetime | None = None

    def _refilled(self, now: datetime) -> float:
        if self._last is None or now <= self._last:
            return self._tokens
        elapsed = (now - self._last).total_seconds()
        return min(self._burst, self._tokens + elapsed * self._per_s)

    def peek(self, now: datetime) -> bool:
        return self._refilled(now) >= 1.0

    def take(self, now: datetime) -> bool:
        tokens = self._refilled(now)
        if self._last is None or now > self._last:
            self._last = now
        if tokens >= 1.0:
            self._tokens = tokens - 1.0
            return True
        self._tokens = tokens
        return False


@dataclass(frozen=True)
class ShieldContext:
    registry: Mapping[str, DeviceProfile]
    in_flight: set[str]
    cycle_intents: Sequence[ActionIntent]
    last_command_at: Mapping[str, datetime]
    bucket: TokenBucket
    suspect: set[str]
    now: datetime
    mode: BrainMode = BrainMode.NORMAL


@dataclass(frozen=True)
class CheckOutcome:
    name: str
    passed: bool
    code: str | None


@dataclass(frozen=True)
class ShieldResult:
    passed: bool
    refusals: tuple[str, ...]
    checks: tuple[CheckOutcome, ...]


def _state_allowed(intent: ActionIntent, allowlist: Mapping[str, frozenset[str]]) -> bool:
    allowed = allowlist.get(intent.capability)
    if allowed is None:
        return False
    if _RATIO.fullmatch(intent.state):
        return "ratio" in allowed
    return intent.state != "ratio" and intent.state in allowed


def _cooldown_ok(intent: ActionIntent, ctx: ShieldContext, policy: Mapping) -> bool:
    cooldown = policy["shield"]["cooldown_s"].get(intent.capability)
    if cooldown is None:
        # Fail closed: a capability with no configured cooldown is refused, never passed.
        return False
    last = ctx.last_command_at.get(intent.target)
    if last is None:
        return True
    return (ctx.now - last).total_seconds() >= cooldown


def _drains(intent: ActionIntent, ctx: ShieldContext) -> bool:
    """Whether the intent opens its actuator further than it is open now."""
    device = ctx.registry.get(intent.target)
    if device is None or device.actuator is None:
        return False
    wanted = commanded_ratio(intent.state)
    current = commanded_ratio(device.actuator.current_state or "")
    return wanted is not None and current is not None and wanted > current


def _mode_forbids(intent: ActionIntent, ctx: ShieldContext, policy: Mapping) -> bool:
    """Check 9: the mode decides how much risk the Brain may take (spec 8.9).

    While the Guardian is down, the one exception is the safe baseline: a drain-direction R1
    the planner only proposes when the level is predicted to cross the critical band.
    """
    if intent.risk_class in RISK_BY_MODE.get(ctx.mode, frozenset({RiskClass.R0})):
        return False
    if ctx.mode is BrainMode.GUARDIAN_DEGRADED and intent.risk_class is RiskClass.R1:
        baseline = bool(policy.get("modes", {}).get("safe_baseline_r1", False))
        return not (baseline and _drains(intent, ctx))
    return True


def evaluate(
    intent: ActionIntent,
    ctx: ShieldContext,
    allowlist: Mapping[str, frozenset[str]],
    policy: Mapping,
) -> ShieldResult:
    """Run every static check in order. Peeks the rate limit; the coordinator takes the token."""
    device = ctx.registry.get(intent.target)
    known = device is not None
    # Checks that need the device cannot be evaluated for an unknown target: they record a
    # pass with no code. UNKNOWN_TARGET already refuses the intent.
    contained = known and device.status != DeviceStatus.OK
    no_op = known and device.actuator is not None and device.actuator.current_state == intent.state
    conflict = any(
        other.action_id != intent.action_id
        and other.target == intent.target
        and other.state != intent.state
        for other in ctx.cycle_intents
    )
    failed = {
        "STATE_NOT_ALLOWED": not _state_allowed(intent, allowlist),
        "UNKNOWN_TARGET": not known,
        "ACTUATOR_SUSPECT": intent.target in ctx.suspect,
        "TARGET_CONTAINED": contained,
        "NO_OP": no_op,
        "IN_FLIGHT": intent.target in ctx.in_flight,
        "CONFLICT": conflict,
        "COOLDOWN": not _cooldown_ok(intent, ctx, policy),
        "RATE_LIMIT": not ctx.bucket.peek(ctx.now),
        "MODE_FORBIDS": _mode_forbids(intent, ctx, policy),
    }
    checks = tuple(CheckOutcome(n, not failed[n], n if failed[n] else None) for n in CHECK_NAMES)
    refusals = tuple(c.code for c in checks if c.code is not None)
    return ShieldResult(passed=not refusals, refusals=refusals, checks=checks)
