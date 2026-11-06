"""T0 water planner: bang-bang with hysteresis, no model (spec 8.5)."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from city_brain.core.estimator import WaterFilter
from city_brain.domain.ids import action_id
from city_brain.domain.types import (
    ActionIntent,
    Alternative,
    EvidenceRef,
    ExpectedEffect,
    RiskClass,
)

CLOSED = "closed"
OPEN = "open"


@dataclass(frozen=True)
class WaterContext:
    device_id: str
    zone_id: str
    filter: WaterFilter
    valve_state: str
    recent: Sequence[EvidenceRef]
    below_low_since: datetime | None
    now: datetime
    plan_version: int


def _is_stale(ctx: WaterContext, policy: Mapping) -> bool:
    if not ctx.filter.initialised or not ctx.recent:
        return True
    stale_s = policy.get("filter", {}).get("stale_s")
    if stale_s is None:
        return False
    return (ctx.now - ctx.recent[-1].sim_time).total_seconds() >= float(stale_s)


def water_hold_reason(ctx: WaterContext, policy: Mapping) -> str | None:
    """Why this channel must not be planned on: STALE, STUCK, IMPLAUSIBLE_JUMP, MODEL_MISMATCH, or None."""
    if _is_stale(ctx, policy):
        return "STALE"
    flags = ctx.filter.flags
    for reason in ("STUCK", "IMPLAUSIBLE_JUMP", "MODEL_MISMATCH"):
        if reason in flags:
            return reason
    return None


def plan_water_t0(ctx: WaterContext, policy: Mapping) -> ActionIntent | None:
    if water_hold_reason(ctx, policy) is not None:
        return None
    water = policy["water"]
    verification = policy["verification"]
    level = ctx.filter.level
    if ctx.valve_state == CLOSED:
        if level >= water["high_band"] or "CEILING" in ctx.filter.flags:
            return _intent(
                ctx, water, OPEN, RiskClass.R1,
                ExpectedEffect(
                    channel=ctx.device_id, metric="rate", comparator="<=",
                    threshold=verification["open_rate_max"], window_s=verification["open_window_s"],
                ),
                f"Level {level:.2f} m is at or above the high band {water['high_band']} m "
                "(or the sensor is at its ceiling) with the valve closed; open it to drain.",
                Alternative(option="hold", rejected_because="level at or above high band and rising"),
            )
        return None
    if ctx.below_low_since is None or level > water["low_band"]:
        return None
    if (ctx.now - ctx.below_low_since).total_seconds() < water["close_sustain_s"]:
        return None
    return _intent(
        ctx, water, CLOSED, RiskClass.R2,
        ExpectedEffect(
            channel=ctx.device_id, metric="rate", comparator=">=",
            threshold=verification["close_rate_min"], window_s=verification["close_window_s"],
        ),
        f"Level {level:.2f} m has stayed at or below the low band {water['low_band']} m "
        f"for {water['close_sustain_s']} s; close the valve to stop draining.",
        Alternative(option="keep open", rejected_because="level below low band for close_sustain_s"),
    )


def _intent(ctx, water, state, risk, effect, rationale, alternative) -> ActionIntent:
    incident_id = f"flood-risk:{ctx.device_id}:{ctx.plan_version}"
    return ActionIntent(
        action_id=action_id(incident_id, "valve", ctx.device_id, state, ctx.plan_version),
        incident_id=incident_id,
        plan_version=ctx.plan_version,
        capability="valve",
        target=ctx.device_id,
        state=state,
        risk_class=risk,
        expected_effect=effect,
        evidence=tuple(ctx.recent[-int(water["evidence_count"]):]),
        rationale=rationale,
        alternatives=(alternative,),
    )
