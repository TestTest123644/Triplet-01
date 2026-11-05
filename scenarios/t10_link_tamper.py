"""Scenario t10: Link-Layer Adversary Tampering Modes.

Spec: link_tamper --mode corrupt_bytes|replay|delay|drop on z3-tc1: wire checks per mode; reaction per mode.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser, verify_payload_signature


def run(ctx: ScenarioContext, mode: str = "corrupt_bytes") -> bool:
    print(f"[{ctx.scenario_id}] Injecting link attack on z3-tc1 with mode='{mode}'...")
    inj_res = ctx.rest.inject_event({
        "capability": "link",
        "target_device_id": "z3-tc1",
        "params": {"link_mode": mode},
    })
    attack_id = inj_res.get("attack_id") or inj_res.get("event_id")
    ctx.log_event("", "scenario", "inject_attack", {"attack_id": attack_id, "mode": mode, "response": inj_res})
    ctx.record_check(f"link_attack_injected_{mode}", bool(attack_id), f"attack_id: {attack_id}")

    pub_key = ctx.mcp.get_device_key("z3-tc1")

    if mode == "corrupt_bytes":
        print(f"[{ctx.scenario_id}] Verifying corrupt bytes / signature failures on wire...")
        corrupt_seen = False
        deadline = time.time() + 15.0
        while time.time() < deadline:
            frames = ctx.wire.drain(max_seconds=0.5, max_items=100)
            for raw_b, p in frames:
                if p.get("device_id") == "z3-tc1":
                    if p.get("_corrupted") or not verify_payload_signature(p, pub_key):
                        corrupt_seen = True
                        break
            if corrupt_seen:
                break
            time.sleep(0.2)
        ctx.record_check("corrupt_bytes_detected_on_wire", corrupt_seen, "Wire payload corrupted or invalid signature")

        print(f"[{ctx.scenario_id}] Waiting for Guardian quarantine on z3-tc1...")
        entry = ctx.wait_for_journal(
            predicate=lambda e: (
                e.get("source") == "containment"
                and e.get("detail", {}).get("tool") == "quarantine_device"
                and e.get("detail", {}).get("target") in ("z3-tc1", None)
                and (e.get("detail", {}).get("target") == "z3-tc1" or e.get("detail", {}).get("device_id") == "z3-tc1")
            ),
            timeout=30.0,
        )
        ctx.record_check("z3_tc1_quarantined_for_corrupt_bytes", entry is not None, f"containment: {entry.get('detail') if entry else 'None'}")

    elif mode == "replay":
        print(f"[{ctx.scenario_id}] Waiting for Guardian replay detection and isolation...")
        entry = ctx.wait_for_journal(
            predicate=lambda e: (
                e.get("source") == "containment"
                and e.get("detail", {}).get("tool") == "isolate_device"
                and (e.get("detail", {}).get("target") == "z3-tc1" or e.get("detail", {}).get("device_id") == "z3-tc1")
            ),
            timeout=30.0,
        )
        ctx.record_check("z3_tc1_isolated_for_replay", entry is not None, f"isolation: {entry.get('detail') if entry else 'None'}")

    elif mode == "delay":
        print(f"[{ctx.scenario_id}] Waiting for Guardian late frame detection and isolation...")
        entry = ctx.wait_for_journal(
            predicate=lambda e: (
                e.get("source") == "containment"
                and e.get("detail", {}).get("tool") == "isolate_device"
                and (e.get("detail", {}).get("target") == "z3-tc1" or e.get("detail", {}).get("device_id") == "z3-tc1")
            ),
            timeout=35.0,
        )
        ctx.record_check("z3_tc1_isolated_for_delay", entry is not None, f"isolation: {entry.get('detail') if entry else 'None'}")

    elif mode == "drop":
        print(f"[{ctx.scenario_id}] Verifying frames for z3-tc1 cease on wire...")
        received_z3 = 0
        deadline = time.time() + 5.0
        while time.time() < deadline:
            frames = ctx.wire.drain(max_seconds=0.5, max_items=100)
            for _, p in frames:
                if p.get("device_id") == "z3-tc1":
                    received_z3 += 1
            time.sleep(0.2)
        ctx.record_check("z3_tc1_frames_dropped_on_wire", received_z3 == 0, f"received {received_z3} frames (expected 0)")

    # False-positive guard: no other devices contained, no commands
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices={"z3-tc1"}, allowed_zones=set())
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    return ctx.finish()


if __name__ == "__main__":
    parser = make_common_parser("t10_link_tamper: link_tamper --mode corrupt_bytes|replay|delay|drop on z3-tc1")
    parser.add_argument("--mode", default="corrupt_bytes", choices=["corrupt_bytes", "replay", "delay", "drop"], help="Link tampering adversary mode")
    args = parser.parse_args()
    ctx = ScenarioContext.from_args(args, "t10", f"t10_link_tamper_{args.mode}")
    with ctx:
        ok = run(ctx, mode=args.mode)
        sys.exit(0 if ok else 1)

