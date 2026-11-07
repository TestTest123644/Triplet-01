"""Scenario t2: Traffic Accident Incident Response.

Spec: accident on z1-tc1; camera drops, calls rise; reaction: dispatch medical_emergency; event resolved_by dispatch.
"""

from __future__ import annotations

import os
import sys
import time

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser


def run(ctx: ScenarioContext) -> bool:
    print(f"[{ctx.scenario_id}] Injecting traffic_accident on z1-tc1...")
    inj_res = ctx.rest.inject_event({
        "type": "traffic_accident",
        "zone_id": "zone-1",
        "device_id": "z1-tc1",
        "intensity": 2.0,
        "duration_s": 600.0,
    })
    event_id = inj_res.get("event_id")
    ctx.log_event("", "scenario", "inject_event", {"event_id": event_id, "response": inj_res})
    ctx.record_check("accident_injected", bool(event_id), f"event_id: {event_id}")
    if not event_id:
        return ctx.finish()

    # Wait for Brain dispatch reaction in journal
    print(f"[{ctx.scenario_id}] Waiting for medical_emergency dispatch in zone-1...")
    dispatch_entry = ctx.wait_for_journal(
        predicate=lambda e: (
            e.get("source") == "command"
            and e.get("detail", {}).get("tool") == "dispatch"
            and e.get("detail", {}).get("args", {}).get("incident_type") == "medical_emergency"
            and e.get("detail", {}).get("args", {}).get("zone_id") == "zone-1"
        ),
        timeout=30.0,
    )
    has_dispatch = dispatch_entry is not None
    ctx.record_check(
        "medical_emergency_dispatched",
        has_dispatch,
        f"dispatch entry: {dispatch_entry.get('detail') if dispatch_entry else 'None'}",
    )

    # Wait for accident incident event to be marked resolved_by dispatch in journal
    print(f"[{ctx.scenario_id}] Waiting for event resolution in journal...")
    resolved_entry = ctx.wait_for_journal(
        predicate=lambda e: (
            e.get("source") == "event"
            and e.get("detail", {}).get("event_id") == event_id
            and e.get("detail", {}).get("action") == "resolved"
            and str(e.get("detail", {}).get("resolved_by", "")).startswith("dispatch:")
        ),
        timeout=35.0,
    )
    has_resolution = resolved_entry is not None
    ctx.record_check(
        "incident_resolved_by_dispatch",
        has_resolution,
        f"resolved_by: {resolved_entry.get('detail', {}).get('resolved_by') if resolved_entry else 'None'}",
    )

    # False-positive guard: no actions outside zone-1, no device containments
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices=set(), allowed_zones={"zone-1"})
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    return ctx.finish()


if __name__ == "__main__":
    parser = make_common_parser("t2_traffic_accident: accident on z1-tc1, dispatch medical_emergency, resolved_by")
    args = parser.parse_args()
    ctx = ScenarioContext.from_args(args, "t2", "t2_traffic_accident")
    with ctx:
        ok = run(ctx)
        sys.exit(0 if ok else 1)

