"""Scenario t4: Zone Fire Incident and Emergency Dispatch.

Spec: fire in zone-5; AQI > 150; dispatch fire; resolved after 180 sim-s.
"""

from __future__ import annotations

import os
import sys
import time

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser


def run(ctx: ScenarioContext) -> bool:
    print(f"[{ctx.scenario_id}] Injecting fire incident in zone-5...")
    inj_res = ctx.rest.inject_event({
        "type": "fire",
        "zone_id": "zone-5",
        "intensity": 2.0,
        "duration_s": 1200.0,
    })
    event_id = inj_res.get("event_id")
    ctx.log_event("", "scenario", "inject_event", {"event_id": event_id, "response": inj_res})
    ctx.record_check("fire_injected", bool(event_id), f"event_id: {event_id}")

    # Monitor wire until AQI on z5-aq1 exceeds 150
    print(f"[{ctx.scenario_id}] Monitoring z5-aq1 for fire plume (AQI > 150)...")
    aqi_elevated = False
    peak_aqi = 0.0
    deadline = time.time() + 25.0
    while time.time() < deadline:
        frames = ctx.wire.drain(max_seconds=0.5, max_items=100)
        for _, p in frames:
            if p.get("device_id") == "z5-aq1":
                val = float(p.get("value", 0.0))
                if val > peak_aqi:
                    peak_aqi = val
                if val > 150.0:
                    aqi_elevated = True
                    break
        if aqi_elevated:
            break
        time.sleep(0.5)

    ctx.record_check("aqi_exceeded_150", aqi_elevated, f"peak AQI: {peak_aqi:.1f} (> 150)")

    # Wait for Brain fire dispatch in journal
    print(f"[{ctx.scenario_id}] Waiting for Brain dispatch fire in zone-5...")
    dispatch_entry = ctx.wait_for_journal(
        predicate=lambda e: (
            e.get("source") == "command"
            and e.get("detail", {}).get("tool") == "dispatch"
            and e.get("detail", {}).get("args", {}).get("incident_type") == "fire"
            and e.get("detail", {}).get("args", {}).get("zone_id") == "zone-5"
        ),
        timeout=30.0,
    )
    has_dispatch = dispatch_entry is not None
    ctx.record_check(
        "fire_dispatched_in_zone5",
        has_dispatch,
        f"dispatch entry: {dispatch_entry.get('detail') if dispatch_entry else 'None'}",
    )

    # Wait for fire incident resolution in journal
    print(f"[{ctx.scenario_id}] Waiting for fire incident resolution (resolved_by dispatch)...")
    resolved_entry = ctx.wait_for_journal(
        predicate=lambda e: (
            e.get("source") == "event"
            and e.get("detail", {}).get("event_id") == event_id
            and e.get("detail", {}).get("action") == "resolved"
            and str(e.get("detail", {}).get("resolved_by", "")).startswith("dispatch:")
        ),
        timeout=25.0,
    )
    has_resolution = resolved_entry is not None
    ctx.record_check(
        "fire_resolved_by_dispatch",
        has_resolution,
        f"resolved_by: {resolved_entry.get('detail', {}).get('resolved_by') if resolved_entry else 'None'}",
    )

    # False-positive guard: no actions outside zone-5, no containments
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices=set(), allowed_zones={"zone-5"})
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    return ctx.finish()


if __name__ == "__main__":
    parser = make_common_parser("t4_fire: fire in zone-5, AQI > 150, dispatch fire, resolved after 180 sim-s")
    args = parser.parse_args()
    ctx = ScenarioContext.from_args(args, "t4", "t4_fire")
    with ctx:
        ok = run(ctx)
        sys.exit(0 if ok else 1)

