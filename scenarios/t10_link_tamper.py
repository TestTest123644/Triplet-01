"""Scenario t10: Link-Layer Adversary Tampering Modes.

Spec: link_tamper --mode corrupt_bytes|corrupt_sig|replay|delay|drop|all on z3-tc1: wire checks per mode; reaction per mode.
"""

from __future__ import annotations

import argparse
import collections
from datetime import datetime
import os
import sys
import time
from typing import List

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser, verify_payload_signature


def _parse_ts(ts_str: str) -> float:
    try:
        s = ts_str.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return 0.0


def run_mode(ctx: ScenarioContext, mode: str) -> bool:
    print(f"[{ctx.scenario_id}] --- Testing Link Mode: {mode} on z3-tc1 ---")
    inj_res = ctx.rest.inject_event({
        "capability": "link",
        "target_device_id": "z3-tc1",
        "params": {"link_mode": mode},
    })
    attack_id = inj_res.get("attack_id") or inj_res.get("event_id")
    ctx.log_event("", "scenario", "inject_attack", {"attack_id": attack_id, "mode": mode, "response": inj_res})
    ctx.record_check(f"link_attack_injected_{mode}", bool(attack_id), f"attack_id: {attack_id}")

    pub_key = ctx.mcp.get_device_key("z3-tc1")

    if mode in ("corrupt_bytes", "corrupt_sig"):
        print(f"[{ctx.scenario_id}] [{mode}] Verifying signature failures / corruption on wire...")
        sig_failed = False
        deadline = time.time() + 15.0
        while time.time() < deadline:
            frames = ctx.wire.drain(max_seconds=0.5, max_items=100)
            for raw_b, p in frames:
                if p.get("device_id") == "z3-tc1":
                    if p.get("_corrupted") or not verify_payload_signature(p, pub_key):
                        sig_failed = True
                        break
            if sig_failed:
                break
            time.sleep(0.2)
        ctx.record_check(f"{mode}_sig_failure_on_wire", sig_failed, "Wire payload corrupted or invalid signature")

        print(f"[{ctx.scenario_id}] [{mode}] Waiting for Guardian quarantine on z3-tc1...")
        entry = ctx.wait_for_journal(
            predicate=lambda e: (
                e.get("source") == "containment"
                and e.get("detail", {}).get("tool") == "quarantine_device"
                and (e.get("detail", {}).get("target") == "z3-tc1" or e.get("detail", {}).get("device_id") == "z3-tc1")
            ),
            timeout=30.0,
        )
        ctx.record_check(f"z3_tc1_quarantined_for_{mode}", entry is not None, f"containment: {entry.get('detail') if entry else 'None'}")

    elif mode == "replay":
        print(f"[{ctx.scenario_id}] [replay] Verifying duplicate reading_id on wire...")
        seen_ids = collections.defaultdict(int)
        duplicate_seen = False
        deadline = time.time() + 15.0
        while time.time() < deadline:
            frames = ctx.wire.drain(max_seconds=0.5, max_items=200)
            for _, p in frames:
                if p.get("device_id") == "z3-tc1":
                    rid = p.get("reading_id")
                    if rid:
                        seen_ids[rid] += 1
                        if seen_ids[rid] >= 2:
                            duplicate_seen = True
                            break
            if duplicate_seen:
                break
            time.sleep(0.2)
        ctx.record_check("z3_tc1_reading_id_seen_twice_on_wire", duplicate_seen, f"duplicate seen: {duplicate_seen}")

        print(f"[{ctx.scenario_id}] [replay] Waiting for Guardian replay detection and isolation...")
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
        print(f"[{ctx.scenario_id}] [delay] Verifying frame ts lags stream head on wire...")
        head_ts = 0.0
        tc1_ts = 0.0
        deadline = time.time() + 15.0
        lag_detected = False
        while time.time() < deadline:
            frames = ctx.wire.drain(max_seconds=0.5, max_items=200)
            for _, p in frames:
                t = _parse_ts(p.get("ts", ""))
                if t > head_ts:
                    head_ts = t
                if p.get("device_id") == "z3-tc1":
                    tc1_ts = t
            if head_ts > 0 and tc1_ts > 0 and (head_ts - tc1_ts) >= 1.0:
                lag_detected = True
                break
            time.sleep(0.2)
        ctx.record_check("z3_tc1_frame_ts_lags_stream_head", lag_detected, f"head_ts - tc1_ts = {head_ts - tc1_ts:.2f}s")

        print(f"[{ctx.scenario_id}] [delay] Waiting for Guardian late frame detection and isolation...")
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
        print(f"[{ctx.scenario_id}] [drop] Verifying frames for z3-tc1 cease on wire...")
        received_z3 = 0
        deadline = time.time() + 5.0
        while time.time() < deadline:
            frames = ctx.wire.drain(max_seconds=0.5, max_items=100)
            for _, p in frames:
                if p.get("device_id") == "z3-tc1":
                    received_z3 += 1
            time.sleep(0.2)
        ctx.record_check("z3_tc1_frames_dropped_on_wire", received_z3 == 0, f"received {received_z3} frames (expected 0)")

    return True


def run(ctx: ScenarioContext, mode: str = "all") -> bool:
    modes_to_run: List[str]
    if mode == "all":
        modes_to_run = ["corrupt_bytes", "corrupt_sig", "replay", "delay", "drop"]
    else:
        modes_to_run = [mode]

    for idx, m in enumerate(modes_to_run):
        run_mode(ctx, m)
        # Clear attack and restore device before running next mode
        if idx < len(modes_to_run) - 1:
            try:
                ctx.rest.clear_event("all")
                ctx.mcp.call_tool("reinstate_device", {"device_id": "z3-tc1"})
                ctx.sim_sleep(1.0)
            except Exception as e:
                ctx.log_event("", "scenario", "mode_transition_error", {"error": str(e)})

    # False-positive guard: no other devices contained, no commands
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices={"z3-tc1"}, allowed_zones=set())
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    return ctx.finish()


if __name__ == "__main__":
    parser = make_common_parser("t10_link_tamper: link_tamper --mode all|corrupt_bytes|corrupt_sig|replay|delay|drop on z3-tc1")
    parser.add_argument(
        "--mode",
        default="all",
        choices=["all", "corrupt_bytes", "corrupt_sig", "replay", "delay", "drop"],
        help="Link tampering adversary mode (default: all)",
    )
    args = parser.parse_args()
    ctx = ScenarioContext.from_args(args, "t10", f"t10_link_tamper_{args.mode}")
    with ctx:
        ok = run(ctx, mode=args.mode)
        sys.exit(0 if ok else 1)
