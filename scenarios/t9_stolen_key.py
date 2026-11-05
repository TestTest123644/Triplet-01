"""Scenario t9: Compromised Device Key Attack and Cryptographic Quarantine.

Spec: device_key on z1-pl1 spoofed; signatures VERIFY; reaction: quarantine + rollback.
"""

from __future__ import annotations

import os
import sys
import time

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser, verify_payload_signature


def run(ctx: ScenarioContext) -> bool:
    print(f"[{ctx.scenario_id}] Injecting device_key attack on z1-pl1 (spoofed value 180 MW)...")
    inj_res = ctx.rest.inject_event({
        "capability": "device_key",
        "target_device_id": "z1-pl1",
        "params": {"spoofed_value": 180.0},
    })
    attack_id = inj_res.get("attack_id") or inj_res.get("event_id")
    ctx.log_event("", "scenario", "inject_attack", {"attack_id": attack_id, "response": inj_res})
    ctx.record_check("device_key_attack_injected", bool(attack_id), f"attack_id: {attack_id}")

    # Observe wire: signatures mathematically VERIFY despite malicious spoofed reading
    print(f"[{ctx.scenario_id}] Verifying signatures on spoofed wire telemetry...")
    pub_key = ctx.mcp.get_device_key("z1-pl1")
    sig_verified = False
    spoofed_seen = False

    deadline = time.time() + 15.0
    while time.time() < deadline:
        frames = ctx.wire.drain(max_seconds=0.5, max_items=1000)
        for _, p in frames:
            if p.get("device_id") == "z1-pl1":
                v = float(p.get("value", 0.0))
                if v >= 150.0:
                    spoofed_seen = True
                    if pub_key and verify_payload_signature(p, pub_key):
                        sig_verified = True
                        break
        if spoofed_seen and sig_verified:
            break
        time.sleep(0.2)

    ctx.record_check("spoofed_reading_broadcast", spoofed_seen, "z1-pl1 power >= 150 MW")
    ctx.record_check("signature_valid_on_wire", sig_verified, "Signature verified with legitimate public key")

    # Wait for Guardian reaction in journal: quarantine z1-pl1 and rollback
    print(f"[{ctx.scenario_id}] Waiting for Guardian quarantine on z1-pl1...")
    quarantine_entry = ctx.wait_for_journal(
        predicate=lambda e: (
            e.get("source") == "containment"
            and e.get("detail", {}).get("tool") == "quarantine_device"
            and (e.get("detail", {}).get("target") == "z1-pl1" or e.get("detail", {}).get("device_id") == "z1-pl1")
        ),
        timeout=30.0,
    )
    has_quarantine = quarantine_entry is not None
    ctx.record_check("z1_pl1_quarantined", has_quarantine, f"quarantine entry: {quarantine_entry.get('detail') if quarantine_entry else 'None'}")

    # Check rollbacks belong to z1-pl1
    journal = ctx.rest.get_journal(since_ts=ctx.start_sim_time, limit=1000)
    rollbacks = [
        e for e in journal
        if e.get("source") == "containment" and e.get("detail", {}).get("tool") == "rollback_reading"
        and (not ctx.start_sim_time or e.get("sim_ts", "") >= ctx.start_sim_time)
    ]
    rollback_ok = any("z1-pl1" in str(r.get("detail", {}).get("target", "")) for r in rollbacks)
    ctx.record_check("spoofed_reading_rolled_back", rollback_ok, f"found {len(rollbacks)} rollbacks targeting z1-pl1")

    # False-positive guard: other devices not quarantined, no commands outside zone-1
    fp_ok, fp_msg = ctx.verify_false_positive_guard(allowed_devices={"z1-pl1"}, allowed_zones={"zone-1"})
    ctx.record_check("false_positive_guard", fp_ok, fp_msg)

    return ctx.finish()


if __name__ == "__main__":
    parser = make_common_parser("t9_stolen_key: device_key on z1-pl1 spoofed, signatures VERIFY, quarantine + rollback")
    args = parser.parse_args()
    ctx = ScenarioContext.from_args(args, "t9", "t9_stolen_key")
    with ctx:
        ok = run(ctx)
        sys.exit(0 if ok else 1)

