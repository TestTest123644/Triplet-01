"""Enumerative model predictive control for a water channel (spec 8.5, 10.3).

The T0 planner swings the valve fully open at the high band and fully shut below the low
band, which holds the level but works the actuator hard. With a model — an inflow estimate
and a learned valve gain (spec 8.4) — the Brain can instead try each valve position it is
allowed to ask for, simulate the next minute, and pick the one that keeps the level nearest
the target while leaving the bands alone. Moving the valve costs `lambda_change`, so a
position is only proposed when it beats staying put by more than the move is worth.

This is the Tier T2 planner; `policy.water.planner` picks it or the T0 one.
"""

from collections.abc import Mapping, Sequence

from city_brain.core.water import (
    CLOSED,
    OPEN,
    WaterContext,
    commanded_ratio,
    critical_ahead,
    water_hold_reason,
)
from city_brain.domain.ids import action_id
from city_brain.domain.types import (
    ActionIntent,
    Alternative,
    CapabilityTier,
    ExpectedEffect,
    RiskClass,
)

RULE = "water_mpc"


def planner_tier(policy: Mapping) -> CapabilityTier:
    """T2 while the model-based planner is in charge, T0 for bang-bang (spec 10.3)."""
    return CapabilityTier.T2 if policy["water"].get("planner", "mpc") == "mpc" else CapabilityTier.T0


def valve_state_for(pct: int) -> str:
    """The canonical actuator state for a valve opening in percent."""
    if pct <= 0:
        return CLOSED
    if pct >= 100:
        return OPEN
    return f"ratio:{pct}"


def plan_water_mpc(ctx: WaterContext, policy: Mapping) -> ActionIntent | None:
    """The valve position the next minute argues for, or None to leave the valve alone."""
    if water_hold_reason(ctx, policy) is not None:
        return None
    mpc = policy["water"]["mpc"]
    if ctx.global_tick % max(1, round(float(mpc["replan_s"]) / _period(ctx))) != 0:
        return None
    current = _current_pct(ctx)
    if current is None:
        return None
    candidates = sorted({int(pct) for pct in mpc["candidates"]} | {current})
    costs = {pct: _cost(ctx, policy, pct, current) for pct in candidates}
    best = min(candidates, key=lambda pct: (costs[pct], abs(pct - current), pct))
    if best == current:
        return None
    forced = ctx.filter.level >= float(policy["water"]["high_band"]) and current == 0
    if not forced and costs[best] >= costs[current]:
        return None
    if best > current:
        return _intent(ctx, policy, best, current, draining=True)
    if not _sustained_trust(ctx, mpc):
        return None  # a closing move is R2: it needs trusted data behind it
    return _intent(ctx, policy, best, current, draining=False)


# ----- the objective -----


def _period(ctx: WaterContext) -> float:
    return ctx.filter.period_s


def _current_pct(ctx: WaterContext) -> int | None:
    ratio = commanded_ratio(ctx.valve_state)
    return None if ratio is None else round(ratio * 100)


def inflow_estimate(ctx: WaterContext) -> float:
    """The inflow the plan is built on: the channel's smoothed estimate (spec 8.4)."""
    return ctx.filter.inflow_mean


def _cost(ctx: WaterContext, policy: Mapping, pct: int, current: int) -> float:
    """What holding `pct` would cost: distance from the target, band breaches, and moving.

    The fine horizon is the next minute, second by second. A coarse tail follows it, because
    a minute is not long enough to tell a position that balances the inflow from one that
    merely points the level the right way: without the tail the planner would swing between
    two positions that both look good for a minute and overshoot afterwards.
    """
    water = policy["water"]
    mpc = water["mpc"]
    target = float(mpc["target"])
    high, low = float(water["high_band"]), float(water["low_band"])
    mu_high, mu_low = float(mpc["mu_high"]), float(mpc["mu_low"])
    rate = inflow_estimate(ctx) - ctx.filter.b_hat * (pct / 100.0)
    level = ctx.filter.level
    total = 0.0 if pct == current else float(mpc["lambda_change"])
    for step_s, until_s in (
        (float(mpc["step_s"]), float(mpc["horizon_s"])),
        (float(mpc["tail_step_s"]), float(mpc["tail_s"]) - float(mpc["horizon_s"])),
    ):
        for _ in range(max(0, round(until_s / step_s))):
            level = min(ctx.max_val, max(ctx.min_val, level + rate * step_s))
            total += (level - target) ** 2
            total += mu_high * max(0.0, level - high) ** 2
            total += mu_low * max(0.0, low - level) ** 2
    return total


def _sustained_trust(ctx: WaterContext, mpc: Mapping) -> bool:
    if ctx.trusted_since is None:
        return False
    return (ctx.now - ctx.trusted_since).total_seconds() >= float(mpc["close_sustain_s"])


# ----- the intent -----


def _intent(
    ctx: WaterContext, policy: Mapping, pct: int, current: int, draining: bool
) -> ActionIntent:
    mpc = policy["water"]["mpc"]
    tolerance = float(mpc["tolerance"])
    predicted = inflow_estimate(ctx) - ctx.filter.b_hat * (pct / 100.0)
    if draining:
        risk = RiskClass.R3 if ctx.contained else RiskClass.R1
        effect = ExpectedEffect(
            channel=ctx.device_id, metric="rate", comparator="<=",
            threshold=predicted + tolerance, window_s=float(mpc["open_window_s"]),
        )
    else:
        risk = RiskClass.R2
        effect = ExpectedEffect(
            channel=ctx.device_id, metric="rate", comparator=">=",
            threshold=predicted - tolerance, window_s=float(mpc["close_window_s"]),
        )
    state = valve_state_for(pct)
    incident_id = f"flood-risk:{ctx.device_id}:{ctx.plan_version}"
    rationale = (
        f"Level {ctx.filter.level:.2f} m with inflow {inflow_estimate(ctx):.4f} m/s and gain "
        f"{ctx.filter.b_hat:.4f}: holding the valve at {pct}% keeps the next "
        f"{mpc['horizon_s']:.0f} s nearest {mpc['target']} m."
    )
    return ActionIntent(
        action_id=action_id(incident_id, "valve", ctx.target, state, ctx.plan_version),
        incident_id=incident_id,
        plan_version=ctx.plan_version,
        capability="valve",
        target=ctx.target,
        state=state,
        risk_class=risk,
        expected_effect=effect,
        evidence=tuple(ctx.recent),
        rationale=rationale,
        alternatives=_alternatives(ctx, policy, pct, current),
        containment_override=ctx.contained and draining,
    )


def _alternatives(
    ctx: WaterContext, policy: Mapping, chosen: int, current: int
) -> tuple[Alternative, ...]:
    """The runner-up positions and why they lost, so the record shows the whole choice."""
    mpc = policy["water"]["mpc"]
    costs = {int(pct): _cost(ctx, policy, int(pct), current) for pct in mpc["candidates"]}
    best = costs[chosen] if chosen in costs else _cost(ctx, policy, chosen, current)
    others: Sequence[int] = sorted(pct for pct in costs if pct != chosen)
    return tuple(
        Alternative(
            option=valve_state_for(pct),
            rejected_because=f"cost {costs[pct]:.1f} against {best:.1f}",
        )
        for pct in others
    )
