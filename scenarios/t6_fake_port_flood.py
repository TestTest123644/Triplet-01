"""Scenario t6: Fake Port Flood Sensor Spoofing Defense.

Spec: device_physical z2-wl1 spoofed 9.5 m; signatures verify; z2-wl2 stays nominal; reaction: isolate z2-wl1; the Brain must NOT open the valve or dispatch flood.
"""

from __future__ import annotations

import os
import sys
import time

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser, verify_payload_signature


def run(ctx: ScenarioContext) -> bool:
    print(f"[{ctx.scenario_id}] Injecting device_physical attack on z2-wl1 (spoofed 9.5m)...")
    inj_res = ctx.rest.inject_event({
        "capability": "device_physical",
        "target_device_id": "z2-wl1",
        "params": {"spoofed_value": 9.5, "value": 9.5},
    })
    attack_id = inj_res.get("attack_id") or inj_res.get("event_id")
    ctx.log_event("", "scenario", "inject_attack", {"attack_id": attack_id, "response": inj_res})
    ctx.record_check("physical_spoof_injected", bool(attack_id), f"attack_id: {attack_id}")

    # Observe wire: z2-wl1 broadcasts 9.5m with valid signature, z2-wl2 stays nominal
    print(f"[{ctx.scenario_id}] Verifying wire telemetry and signatures...")
    pub_key_wl1 = ctx.mcp.get_device_key("z2-wl1")
    wl1_spoofed = False
    wl1_sig_valid = False
    wl2_nominal = False

    deadline = time.time() + 15.0
    while time.time() < deadline:
        frames = ctx.wire.drain(max_seconds=0.5, max_items=1000)
        for _, p in frames:
            did = p.get("device_id")
            val = float(p.get("value", 0.0))
            if did == "z2-wl1":
                if val >= 9.0:
                    wl1_spoofed = True
                    if pub_key_wl1 and verify_payload_signature(p, pub_key_wl1):
                        wl1_sig_valid = True
            elif did == "z2-wl2":
                if val < 5.0:
                    wl2_nominal = True
        if wl1_spoofed and wl1_sig_valid and wl2_nominal:
            break
        time.sleep(0.2)

    ctx.record_check("spoofed_reading_broadcast", wl1_spoofed, "z2-wl1 value >= 9.0m")
    ctx.record_check("spoofed_reading_signature_verified", wl1_sig_valid, "Ed25519 signature verified on spoofed frame")
    ctx.record_check("counterpart_z2_wl2_nominal", wl2_nominal, "z2-wl2 reading nominal < 5.0m")

    # Wait for Guardian reaction in journal: isolate z2-wl1 and rollback
    print(f"[{ctx.scenario_id}] Waiting for Guardian paired water divergence isolation on z2-wl1...")
    isolation_entry = ctx.wait_for_journal(
        predicate=lambda e: (
            e.get("source") == "containment"
            and e.get("detail", {}).get("tool") == "isolate_device"
            and (e.get("detail", {}).get("target") == "z2-wl1" or e.get("detail", {}).get("device_id") == "z2-wl1")
        ),
        timeout=30.0,
    )
    has_isolation = isolation_entry is not None
    ctx.record_check("z2_wl1_isolated", has_isolation, f"isolation: {isolation_entry.get('detail') if isolation_entry else 'None'}")

    # Check rollbacks belong only to z2-wl1
    journal = ctx.rest.get_journal(since_ts=ctx.start_sim_time, limit=1000)
    rollbacks = [
        e for e in journal
        if e.get("source") == "containment" and e.get("detail", {}).get("tool") == "rollback_reading"
        and (not ctx.start_sim_time or e.get("sim_ts", "") >= ctx.start_sim_time)
    ]
    rollback_ok = len(rollbacks) > 0 and all(r.get("detail", {}).get("target", "").startswith("r-z2-wl1-") for r in rollbacks)
    ctx.record_check("rollback_targets_outlier_only", rollback_ok, f"{len(rollbacks)} rollbacks targeting r-z2-wl1-*")

    # Crucial brain check: Brain must NOT open valve or dispatch flood
    commands = [
        e for e in journal
        if e.get("source") == "command"
        and (not ctx.start_sim_time or e.get("sim_ts", "") >= ctx.start_sim_time)
    ]
    valve_cmds = [c for c in commands if c.get("detail", {}).get("tool") == "set_valve"]
    dispatch_cmds = [c for c in commands if c.get("detail", {}).get("tool") == "dispatch"]
    ctx.record_check("brain_held_valve_closed", len(valve_cmds) == 0, f"found {len(valve_cmds)} set_valve commands (expected 0)")
    ctx.record_check("brain_refrained_from_dispatch", len(dispatch_cmds) == 0, f"found {len(dispatch_cmds)} dispatch commands (expected 0)")

    # False-positive guard: counterpart z2-wl2 MUST NOT be isolated
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices={"z2-wl1"}, allowed_zones=set())
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    return ctx.finish()


if __name__ == "__main__":
    parser = make_common_parser("t6_fake_port_flood: device_physical z2-wl1 spoofed 9.5m, isolate z2-wl1, no valve open")
    args = parser.parse_args()
    ctx = ScenarioContext.from_args(args, "t6", "t6_fake_port_flood")
    with ctx:
        ok = run(ctx)
        sys.exit(0 if ok else 1)

