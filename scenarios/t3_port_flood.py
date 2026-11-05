"""Scenario t3: Port Flood Storm and Valve Mitigation.

Spec: rain_storm x3 in zone-2. With --no-brain, z2-wl1 crosses 8 m. With mocks or partners, set_valve open happens and the level stays < 8 m.
"""

from __future__ import annotations

import os
import sys
import time

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser


def run(ctx: ScenarioContext) -> bool:
    print(f"[{ctx.scenario_id}] Injecting rain_storm x3 in zone-2...")
    inj_res = ctx.rest.inject_event({
        "type": "rain_storm",
        "zone_id": "zone-2",
        "intensity": 3.0,
        "duration_s": 1800.0,
    })
    event_id = inj_res.get("event_id")
    ctx.log_event("", "scenario", "inject_event", {"event_id": event_id, "response": inj_res})
    ctx.record_check("storm_injected", bool(event_id), f"event_id: {event_id}")

    if ctx.no_brain:
        print(f"[{ctx.scenario_id}] Running unmitigated storm (--no-brain); monitoring flood rise...")
        # Monitor water level via wire or status until > 8.0m
        max_level = 0.0
        deadline = time.time() + 40.0
        while time.time() < deadline:
            frames = ctx.wire.drain(max_seconds=0.5, max_items=100)
            for _, p in frames:
                if p.get("device_id") == "z2-wl1":
                    v = float(p.get("value", 0.0))
                    if v > max_level:
                        max_level = v
            if max_level > 8.0:
                break
            time.sleep(0.5)

        ctx.record_check("unmitigated_storm_crossed_8m", max_level > 8.0, f"peak level: {max_level:.2f} m (> 8.0m)")
    else:
        print(f"[{ctx.scenario_id}] Waiting for Brain to open flood valve via MCP set_valve...")
        valve_entry = ctx.wait_for_journal(
            predicate=lambda e: (
                e.get("source") == "command"
                and e.get("detail", {}).get("tool") == "set_valve"
                and e.get("detail", {}).get("args", {}).get("target_state") == "open"
                and "z2-wl" in str(e.get("detail", {}).get("args", {}).get("device_id", ""))
            ),
            timeout=35.0,
        )
        has_valve = valve_entry is not None
        ctx.record_check("set_valve_open_commanded", has_valve, f"valve command: {valve_entry.get('detail') if valve_entry else 'None'}")

        # Verify level is kept under 8.0 m
        max_level = 0.0
        deadline = time.time() + 15.0
        while time.time() < deadline:
            frames = ctx.wire.drain(max_seconds=0.5, max_items=100)
            for _, p in frames:
                if p.get("device_id") in ("z2-wl1", "z2-wl2"):
                    v = float(p.get("value", 0.0))
                    if v > max_level:
                        max_level = v
            time.sleep(0.5)

        ctx.record_check("mitigated_storm_level_under_8m", max_level < 8.0, f"peak level: {max_level:.2f} m (< 8.0m)")

    # False-positive guard: no commands outside zone-2, no containments
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices=set(), allowed_zones={"zone-2"})
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    return ctx.finish()


if __name__ == "__main__":
    parser = make_common_parser("t3_port_flood: rain_storm x3 in zone-2, set_valve open, level < 8m")
    args = parser.parse_args()
    ctx = ScenarioContext.from_args(args, "t3", "t3_port_flood")
    with ctx:
        ok = run(ctx)
        sys.exit(0 if ok else 1)

