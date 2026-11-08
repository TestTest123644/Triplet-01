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
from _twin_common import ScenarioContext, make_common_parser, verify_payload_signature, parse_sim_timestamp


def _parse_ts(ts_str: str) -> float:
    return parse_sim_timestamp(ts_str)


def run_mode(ctx: ScenarioContext, mode: str) -> bool:
    print(f"[{ctx.scenario_id}] --- Testing Link Mode: {mode} on z3-tc1 ---")

    # Before each mode: clear previous attack and sync wire stream
    try:
        ctx.rest.clear_event("all")
        ctx.rest.clear_attack("all")
    except Exception as e:
        ctx.log_event("", "scenario", "pre_mode_clear_error", {"error": str(e), "mode": mode})

    # Wait until z3-tc1 frames are current again (ts within 2 sim-s of stream head)
    # AND at least 25 sim-seconds (12-13 ticks) have passed to fill streamer history with clean frames
    clear_sim_time = ctx.get_sim_time_seconds()
    head_ts = 0.0
    tc1_ts = 0.0
    sync_start = time.time()
    while time.time() - sync_start < 15.0:
        frames = ctx.wire.drain(max_seconds=0.5)
        for _, p in frames:
            t = _parse_ts(p.get("ts", ""))
            if t > head_ts:
                head_ts = t
            if p.get("device_id") == "z3-tc1":
                if t > tc1_ts:
                    tc1_ts = t
        if head_ts > 0 and tc1_ts > 0 and (head_ts - tc1_ts) < 2.0 and (tc1_ts >= clear_sim_time + 25.0):
            break

    # Reinstate device if needed once wire frames are clean and current
    try:
        ctx.reinstate_if_needed("z3-tc1")
    except Exception as e:
        ctx.log_event("", "scenario", "pre_mode_reinstate_error", {"error": str(e), "mode": mode})

    # Record injection sim time before injecting attack
    inj_sim_time = ctx.get_sim_time_seconds()

    inj_params = {"link_mode": mode}
    if mode == "delay":
        inj_params["delay_ticks"] = 50

    inj_res = ctx.rest.inject_event({
        "capability": "link",
        "target_device_id": "z3-tc1",
        "params": inj_params,
    })
    attack_id = inj_res.get("attack_id") or inj_res.get("event_id")
    ctx.log_event("", "scenario", "inject_attack", {"attack_id": attack_id, "mode": mode, "response": inj_res})
    ctx.record_check(f"link_attack_injected_{mode}", bool(attack_id), f"attack_id: {attack_id}")
    if not attack_id:
        return ctx.finish()

    pub_key = ctx.mcp.get_device_key("z3-tc1")

    if mode in ("corrupt_bytes", "corrupt_sig"):
        print(f"[{ctx.scenario_id}] [{mode}] Verifying signature failures / corruption on wire...")
        sig_failed = False
        start_sim = ctx.get_sim_time_seconds()
        sim_duration = max(15.0 * max(0.1, ctx.sim_speed), 300.0)
        deadline_sim = start_sim + sim_duration
        wall_safety = time.time() + 20.0
        while True:
            frames = ctx.wire.drain(max_seconds=0.5)
            for raw_b, p in frames:
                if p.get("device_id") == "z3-tc1":
                    if p.get("_corrupted") or not verify_payload_signature(p, pub_key):
                        sig_failed = True
                        break
            if sig_failed:
                break
            if ctx.get_sim_time_seconds() >= deadline_sim or time.time() >= wall_safety:
                break
        ctx.record_check(f"{mode}_sig_failure_on_wire", sig_failed, "Wire payload corrupted or invalid signature")

        print(f"[{ctx.scenario_id}] [{mode}] Waiting for Guardian quarantine on z3-tc1...")
        entry = ctx.wait_for_journal(
            predicate=lambda e: (
                e.get("source") == "containment"
                and e.get("detail", {}).get("tool") == "quarantine_device"
                and (e.get("detail", {}).get("target") == "z3-tc1" or e.get("detail", {}).get("device_id") == "z3-tc1")
                and _parse_ts(e.get("sim_ts", "")) >= inj_sim_time - 1.0
            ),
            timeout=35.0,
        )
        ctx.record_check(f"z3_tc1_quarantined_for_{mode}", entry is not None, f"containment: {entry.get('detail') if entry else 'None'}")

    elif mode == "replay":
        print(f"[{ctx.scenario_id}] [replay] Verifying duplicate reading_id on wire...")
        seen_ids = collections.defaultdict(int)
        duplicate_seen = False
        start_sim = ctx.get_sim_time_seconds()
        sim_duration = max(15.0 * max(0.1, ctx.sim_speed), 300.0)
        deadline_sim = start_sim + sim_duration
        wall_safety = time.time() + 20.0
        while True:
            frames = ctx.wire.drain(max_seconds=0.5)
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
            if ctx.get_sim_time_seconds() >= deadline_sim or time.time() >= wall_safety:
                break
        ctx.record_check("z3_tc1_reading_id_seen_twice_on_wire", duplicate_seen, f"duplicate seen: {duplicate_seen}")

        print(f"[{ctx.scenario_id}] [replay] Waiting for Guardian replay detection and isolation...")
        entry = ctx.wait_for_journal(
            predicate=lambda e: (
                e.get("source") == "containment"
                and e.get("detail", {}).get("tool") == "isolate_device"
                and (e.get("detail", {}).get("target") == "z3-tc1" or e.get("detail", {}).get("device_id") == "z3-tc1")
                and _parse_ts(e.get("sim_ts", "")) >= inj_sim_time - 1.0
            ),
            timeout=35.0,
        )
        ctx.record_check("z3_tc1_isolated_for_replay", entry is not None, f"isolation: {entry.get('detail') if entry else 'None'}")

    elif mode == "delay":
        print(f"[{ctx.scenario_id}] [delay] Verifying frame ts lags stream head on wire...")
        head_ts = 0.0
        tc1_ts = 0.0
        lag_detected = False
        start_sim = ctx.get_sim_time_seconds()
        sim_duration = max(15.0 * max(0.1, ctx.sim_speed), 300.0)
        deadline_sim = start_sim + sim_duration
        wall_safety = time.time() + 20.0
        while True:
            frames = ctx.wire.drain(max_seconds=0.2)
            for _, p in frames:
                t = _parse_ts(p.get("ts", ""))
                if t > head_ts:
                    head_ts = t
                if p.get("device_id") == "z3-tc1":
                    tc1_ts = t
                    if head_ts > 0:
                        lag = (head_ts - tc1_ts) / max(0.1, ctx.sim_speed)
                        if 4.0 <= lag <= 7.0:
                            lag_detected = True
                            break
            if lag_detected:
                break
            if ctx.get_sim_time_seconds() >= deadline_sim or time.time() >= wall_safety:
                break
        measured_lag = ((head_ts - tc1_ts) / max(0.1, ctx.sim_speed)) if (head_ts > 0 and tc1_ts > 0) else 0.0
        ctx.record_check("z3_tc1_frame_ts_lags_stream_head", lag_detected, f"head_ts - tc1_ts = {measured_lag:.2f}s")

        print(f"[{ctx.scenario_id}] [delay] Waiting for Guardian late frame detection and isolation...")
        entry = ctx.wait_for_journal(
            predicate=lambda e: (
                e.get("source") == "containment"
                and e.get("detail", {}).get("tool") == "isolate_device"
                and (e.get("detail", {}).get("target") == "z3-tc1" or e.get("detail", {}).get("device_id") == "z3-tc1")
                and _parse_ts(e.get("sim_ts", "")) >= inj_sim_time - 1.0
            ),
            timeout=35.0,
        )
        ctx.record_check("z3_tc1_isolated_for_delay", entry is not None, f"isolation: {entry.get('detail') if entry else 'None'}")

    elif mode == "drop":
        print(f"[{ctx.scenario_id}] [drop] Verifying frames for z3-tc1 cease on wire...")
        received_z3 = 0
        start_sim = ctx.get_sim_time_seconds()
        sim_duration = max(5.0 * max(0.1, ctx.sim_speed), 100.0)
        deadline_sim = start_sim + sim_duration
        wall_safety = time.time() + 5.0
        while True:
            frames = ctx.wire.drain(max_seconds=0.5)
            for _, p in frames:
                if p.get("device_id") == "z3-tc1":
                    t = _parse_ts(p.get("ts", ""))
                    if t > inj_sim_time:
                        received_z3 += 1
            if ctx.get_sim_time_seconds() >= deadline_sim or time.time() >= wall_safety:
                break
        ctx.record_check("z3_tc1_frames_dropped_on_wire", received_z3 == 0, f"received {received_z3} frames with ts > injection sim time (expected 0)")

    return True


def run(ctx: ScenarioContext, mode: str = "all") -> bool:
    modes_to_run: List[str]
    if mode == "all":
        modes_to_run = ["corrupt_bytes", "corrupt_sig", "replay", "delay", "drop"]
    else:
        modes_to_run = [mode]

    for idx, m in enumerate(modes_to_run):
        run_mode(ctx, m)
        # Clear attack and restore device after running mode
        try:
            ctx.rest.clear_event("all")
            ctx.rest.clear_attack("all")
            if m == "delay":
                # Before reinstating after delay mode, wait (sim-time deadline)
                # until z3-tc1 frames on the wire are within 2 sim-s of stream head, then reinstate
                start_sim = ctx.get_sim_time_seconds()
                max_sim_wait = 30.0
                deadline_sim = start_sim + max_sim_wait
                wall_deadline = time.time() + (max_sim_wait / max(0.1, ctx.sim_speed)) + 15.0
                head_ts = 0.0
                tc1_ts = 0.0
                while ctx.get_sim_time_seconds() < deadline_sim and time.time() < wall_deadline:
                    frames = ctx.wire.drain(max_seconds=0.5)
                    for _, p in frames:
                        t = _parse_ts(p.get("ts", ""))
                        if t > head_ts:
                            head_ts = t
                        if p.get("device_id") == "z3-tc1":
                            if t > tc1_ts:
                                tc1_ts = t
                    if head_ts > 0 and tc1_ts > 0 and (head_ts - tc1_ts) <= 2.0:
                        break
                ctx.sim_sleep(6.0)
                ctx.reinstate_if_needed("z3-tc1")
            else:
                ctx.reinstate_if_needed("z3-tc1")
            ctx.sim_sleep(1.0)
        except Exception as e:
            ctx.log_event("", "scenario", "mode_transition_error", {"error": str(e)})

    # False-positive guard: no other devices contained, no commands
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices={"z3-tc1"}, allowed_zones=set())
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    # Settle step at end of t10 ensures delay buffer is fully drained and leaves Twin clean
    ctx.settle()

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
