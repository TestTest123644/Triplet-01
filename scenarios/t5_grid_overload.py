"""Scenario t5: Grid Overload Incident and Automated Load Shedding.

Spec: zone-3; reaction switch_grid before blackout; without a reaction, blackout + dispatch blackout.
"""

from __future__ import annotations

import os
import sys
import time

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser


def run(ctx: ScenarioContext) -> bool:
    print(f"[{ctx.scenario_id}] Injecting grid_overload in zone-3...")
    inj_res = ctx.rest.inject_event({
        "type": "grid_overload",
        "zone_id": "zone-3",
        "intensity": 1.5,
        "duration_s": 600.0,
    })
    event_id = inj_res.get("event_id")
    ctx.log_event("", "scenario", "inject_event", {"event_id": event_id, "response": inj_res})
    ctx.record_check("grid_overload_injected", bool(event_id), f"event_id: {event_id}")

    if ctx.no_brain:
        print(f"[{ctx.scenario_id}] Running unmitigated overload (--no-brain); awaiting blackout cascade...")
        # Overload escalates to blackout after 60 sim-seconds of continuous load > 114 MW
        timeout_wall = max(15.0, 90.0 / max(0.1, ctx.sim_speed))
        blackout_entry = ctx.wait_for_journal(
            predicate=lambda e: (
                e.get("source") == "event"
                and (
                    e.get("key") == f"event_escalated:{event_id}"
                    or (
                        e.get("detail", {}).get("action") == "escalated"
                        and e.get("detail", {}).get("type") == "grid_overload"
                        and e.get("detail", {}).get("to") == "blackout"
                        and e.get("detail", {}).get("zone_id") == "zone-3"
                    )
                )
            ),
            timeout=timeout_wall,
        )
        ctx.record_check("unmitigated_blackout_triggered", blackout_entry is not None, f"blackout: {blackout_entry.get('detail') if blackout_entry else 'None'}")
    else:
        print(f"[{ctx.scenario_id}] Waiting for Brain switch_grid load shedding in zone-3...")
        shed_entry = ctx.wait_for_journal(
            predicate=lambda e: (
                e.get("source") == "command"
                and e.get("detail", {}).get("tool") == "switch_grid"
                and e.get("detail", {}).get("args", {}).get("target_state") == "shed"
                and "z3-" in str(e.get("detail", {}).get("args", {}).get("device_id", ""))
            ),
            timeout=30.0,
        )
        has_shed = shed_entry is not None
        ctx.record_check("switch_grid_shed_commanded", has_shed, f"shed command: {shed_entry.get('detail') if shed_entry else 'None'}")

        # Check resolution of grid overload in journal
        res_entry = ctx.wait_for_journal(
            predicate=lambda e: (
                e.get("source") == "event"
                and e.get("detail", {}).get("event_id") == event_id
                and e.get("detail", {}).get("action") == "resolved"
                and "switch_grid" in str(e.get("detail", {}).get("resolved_by", ""))
            ),
            timeout=20.0,
        )
        ctx.record_check("grid_overload_resolved_by_shed", res_entry is not None, f"resolved_by: {res_entry.get('detail', {}).get('resolved_by') if res_entry else 'None'}")

    # False-positive guard: no commands outside zone-3, no containments
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices=set(), allowed_zones={"zone-3"})
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    return ctx.finish()


if __name__ == "__main__":
    parser = make_common_parser("t5_grid_overload: zone-3, reaction switch_grid before blackout")
    args = parser.parse_args()
    ctx = ScenarioContext.from_args(args, "t5", "t5_grid_overload")
    with ctx:
        ok = run(ctx)
        sys.exit(0 if ok else 1)

