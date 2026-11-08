"""Scenario t8: Rogue Unregistered Device Detection and Quarantine.

Spec: rogue rg-p1 in zone-2; get_device_key null; reaction: quarantine; blocked frames > 0.
"""

from __future__ import annotations

import os
import sys
import time

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser


def run(ctx: ScenarioContext) -> bool:
    print(f"[{ctx.scenario_id}] Injecting rogue device rg-p1 in zone-2...")
    inj_res = ctx.rest.inject_event({
        "capability": "rogue",
        "params": {
            "rogue_device_id": "rg-p1",
            "zone_id": "zone-2",
            "device_type": "power_load",
            "value": 88.88,
        },
    })
    attack_id = inj_res.get("attack_id") or inj_res.get("event_id")
    ctx.log_event("", "scenario", "inject_attack", {"attack_id": attack_id, "response": inj_res})
    ctx.record_check("rogue_device_injected", bool(attack_id), f"attack_id: {attack_id}")
    if not attack_id:
        return ctx.finish()

    # Verify MCP query get_device_key returns null for unknown rogue device
    print(f"[{ctx.scenario_id}] Verifying MCP get_device_key returns null for rg-p1...")
    rogue_pubkey = ctx.mcp.get_device_key("rg-p1")
    ctx.record_check("get_device_key_null_for_rogue", rogue_pubkey is None, f"key: {rogue_pubkey} (expected None)")

    # Wait for Guardian rogue detection and quarantine in journal
    print(f"[{ctx.scenario_id}] Waiting for Guardian quarantine on rg-p1...")
    quarantine_entry = ctx.wait_for_journal(
        predicate=lambda e: (
            e.get("source") == "containment"
            and e.get("detail", {}).get("tool") == "quarantine_device"
            and (e.get("detail", {}).get("target") == "rg-p1" or e.get("detail", {}).get("device_id") == "rg-p1")
        ),
        timeout=25.0,
    )
    has_quarantine = quarantine_entry is not None
    ctx.record_check("rg_p1_quarantined", has_quarantine, f"quarantine entry: {quarantine_entry.get('detail') if quarantine_entry else 'None'}")

    # Wait 2 sim-seconds for Twin runtime to drop subsequent rogue frames
    print(f"[{ctx.scenario_id}] Verifying blocked rogue frames counter > 0...")
    blocked_count = 0
    deadline = time.time() + 10.0
    while time.time() < deadline:
        st = ctx.rest.get_status()
        blocked_map = st.get("blocked_frames", {})
        blocked_count = blocked_map.get("rg-p1", 0)
        if blocked_count > 0:
            break
        time.sleep(0.5)

    ctx.record_check("blocked_frames_greater_than_zero", blocked_count > 0, f"blocked frames: {blocked_count}")

    # False-positive guard: registered devices not quarantined, no commands
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices={"rg-p1"}, allowed_zones=set())
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    return ctx.finish()


if __name__ == "__main__":
    parser = make_common_parser("t8_rogue_device: rogue rg-p1 in zone-2, get_device_key null, quarantine, blocked frames > 0")
    args = parser.parse_args()
    ctx = ScenarioContext.from_args(args, "t8", "t8_rogue_device")
    with ctx:
        ok = run(ctx)
        sys.exit(0 if ok else 1)

