"""Scenario t7: Stuck Sensor Fault Detection and Isolation.

Spec: sensor_fault stuck on z4-aq1; reaction: flag then isolate.
"""

from __future__ import annotations

import os
import sys
import time

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser


def run(ctx: ScenarioContext) -> bool:
    print(f"[{ctx.scenario_id}] Injecting sensor_fault stuck on z4-aq1...")
    inj_res = ctx.rest.inject_event({
        "capability": "sensor_fault",
        "target_device_id": "z4-aq1",
        "params": {"fault_mode": "stuck", "stuck_value": 42.0},
    })
    attack_id = inj_res.get("attack_id") or inj_res.get("event_id")
    ctx.log_event("", "scenario", "inject_attack", {"attack_id": attack_id, "response": inj_res})
    ctx.record_check("stuck_fault_injected", bool(attack_id), f"attack_id: {attack_id}")
    if not attack_id:
        return ctx.finish()

    # Wait for Guardian stuck sensor isolation (50 identical frames)
    print(f"[{ctx.scenario_id}] Waiting for Guardian stuck sensor rule to isolate z4-aq1...")
    isolation_entry = ctx.wait_for_journal(
        predicate=lambda e: (
            e.get("source") == "containment"
            and e.get("detail", {}).get("tool") == "isolate_device"
            and (e.get("detail", {}).get("target") == "z4-aq1" or e.get("detail", {}).get("device_id") == "z4-aq1")
        ),
        timeout=30.0,
    )
    has_isolation = isolation_entry is not None
    ctx.record_check("z4_aq1_isolated", has_isolation, f"isolation entry: {isolation_entry.get('detail') if isolation_entry else 'None'}")

    # Check that rollback was performed for z4-aq1
    journal = ctx.rest.get_journal(since_ts=ctx.start_sim_time, limit=1000)
    rollbacks = [
        e for e in journal
        if e.get("source") == "containment" and e.get("detail", {}).get("tool") == "rollback_reading"
        and (not ctx.start_sim_time or e.get("sim_ts", "") >= ctx.start_sim_time)
    ]
    rollback_ok = any("z4-aq1" in str(r.get("detail", {}).get("target", "")) for r in rollbacks)
    ctx.record_check("stuck_reading_rolled_back", rollback_ok, f"found {len(rollbacks)} rollback events")

    # False-positive guard: no other devices contained, no commands
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices={"z4-aq1"}, allowed_zones=set())
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    return ctx.finish()


if __name__ == "__main__":
    parser = make_common_parser("t7_stuck_sensor: sensor_fault stuck on z4-aq1, flag then isolate")
    args = parser.parse_args()
    ctx = ScenarioContext.from_args(args, "t7", "t7_stuck_sensor")
    with ctx:
        ok = run(ctx)
        sys.exit(0 if ok else 1)

