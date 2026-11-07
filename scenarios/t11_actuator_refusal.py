"""Scenario t11: Cyber-Physical Actuator Tampering and Deceit Detection.

Spec: actuator attack on z2-wl1 during a storm; set_valve returns ok but the journal shows executed=false and the level keeps rising; the reported state says open while the truth is half.
"""

from __future__ import annotations

import os
import sys
import time

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser


def run(ctx: ScenarioContext) -> bool:
    print(f"[{ctx.scenario_id}] Injecting rain_storm and actuator attack on z2-wl1...")
    storm_res = ctx.rest.inject_event({
        "type": "rain_storm",
        "zone_id": "zone-2",
        "intensity": 3.0,
        "duration_s": 1800.0,
    })
    storm_id = storm_res.get("event_id")

    act_res = ctx.rest.inject_event({
        "capability": "actuator",
        "target_device_id": "z2-wl1",
    })
    attack_id = act_res.get("attack_id") or act_res.get("event_id")
    ctx.log_event("", "scenario", "inject_actuator_attack", {"storm_id": storm_id, "attack_id": attack_id})
    ctx.record_check("actuator_attack_injected", bool(attack_id), f"attack_id: {attack_id}")

    # Invoke MCP set_valve open on z2-wl1
    print(f"[{ctx.scenario_id}] Invoking MCP set_valve target_state='open' on z2-wl1...")
    mcp_res = ctx.mcp.call_tool("set_valve", {"device_id": "z2-wl1", "target_state": "open"})
    ctx.log_event("", "scenario", "mcp_set_valve_response", {"response": mcp_res})

    # Tool returns ok to caller
    is_ok = isinstance(mcp_res, dict) and mcp_res.get("status") == "ok" and mcp_res.get("valve_position") == "open"
    ctx.record_check("mcp_tool_returns_ok_to_caller", is_ok, f"tool response: {mcp_res}")

    # Query MCP get_device_state: reported state says open
    dev_state = ctx.mcp.get_device_state("z2-wl1")
    act_state = dev_state.get("actuator_state", {})
    reported_pos = act_state.get("valve_position")
    reported_open = reported_pos == "open"
    ctx.record_check("reported_state_claims_open", reported_open, f"reported position: {reported_pos}")

    # Ground-truth journal check: command shows executed=false and refused_code=ACTUATOR_ATTACK
    print(f"[{ctx.scenario_id}] Verifying journal records executed=false for tampered actuator...")
    journal = ctx.rest.get_journal(since_ts=ctx.start_sim_time, limit=1000)
    refused_cmd = None
    for e in reversed(journal):
        if e.get("source") == "command" and e.get("detail", {}).get("tool") == "set_valve":
            dt = e.get("detail", {})
            if dt.get("args", {}).get("device_id") == "z2-wl1":
                refused_cmd = dt
                break

    executed_flag = refused_cmd.get("executed") if refused_cmd else None
    refused_code = refused_cmd.get("refused_code") if refused_cmd else None

    ctx.record_check(
        "journal_records_executed_false",
        executed_flag is False,
        f"executed: {executed_flag}, refused_code: {refused_code}",
    )
    ctx.record_check(
        "journal_records_actuator_attack_refusal",
        refused_code == "ACTUATOR_ATTACK",
        f"refused_code: {refused_code}",
    )

    # Physical verification: water level continues rising because truth valve remained half
    print(f"[{ctx.scenario_id}] Verifying physical level rises >= 0.3m over 5 sim-min despite reported open...")
    level_start = 0.0
    level_end = 0.0
    frames = ctx.wire.drain(max_seconds=1.0)
    for _, p in frames:
        if p.get("device_id") == "z2-wl1":
            level_start = float(p.get("value", 0.0))
            break
    if level_start <= 0.0:
        dev_st = ctx.mcp.get_device_state("z2-wl1")
        level_start = float(dev_st.get("current_value", 3.5))

    # Advance at least 5 sim-minutes (300 sim-seconds)
    ctx.sim_sleep(300.0)

    # Drain to head of wire stream to capture latest post-storm rise reading
    if getattr(ctx.wire, "queue_source", None):
        wq = getattr(ctx.wire.queue_source, "wire_q", None)
        if wq:
            while not wq.empty():
                try:
                    raw = wq.get_nowait()
                    if isinstance(raw, bytes):
                        p = json.loads(raw.decode("utf-8"))
                        if p.get("device_id") == "z2-wl1":
                            level_end = float(p.get("value", 0.0))
                except Exception:
                    break

    if level_end <= 0.0:
        frames = ctx.wire.drain(max_seconds=2.0)
        for _, p in frames:
            if p.get("device_id") == "z2-wl1":
                level_end = float(p.get("value", 0.0))

    if level_end <= 0.0:
        dev_st = ctx.mcp.get_device_state("z2-wl1")
        level_end = float(dev_st.get("current_value", level_start))

    level_rise = level_end - level_start
    level_rose = level_rise >= 0.30
    ctx.record_check(
        "physical_water_level_rise_ge_0_3m",
        level_rose,
        f"rise: {level_rise:.2f}m ({level_start:.2f}m -> {level_end:.2f}m, expected >= 0.30m over 5 sim-min)",
    )

    # False-positive guard: no commands outside zone-2, no containments
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices=set(), allowed_zones={"zone-2"})
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    return ctx.finish()


if __name__ == "__main__":
    parser = make_common_parser("t11_actuator_refusal: actuator attack on z2-wl1, executed=false in journal, reported open")
    args = parser.parse_args()
    ctx = ScenarioContext.from_args(args, "t11", "t11_actuator_refusal")
    with ctx:
        ok = run(ctx)
        sys.exit(0 if ok else 1)

