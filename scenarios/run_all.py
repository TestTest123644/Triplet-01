"""Master runner for all Layer 1 Digital Twin scenarios.

Executes scenarios t1 through t11 sequentially and outputs consolidated summary.
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
import time

# Zero twin.* imports
sys.path.insert(0, os.path.dirname(__file__))
from _twin_common import ScenarioContext, make_common_parser

SCENARIOS = [
    ("t1", "t1_baseline_stream"),
    ("t2", "t2_traffic_accident"),
    ("t3", "t3_port_flood"),
    ("t4", "t4_fire"),
    ("t5", "t5_grid_overload"),
    ("t6", "t6_fake_port_flood"),
    ("t7", "t7_stuck_sensor"),
    ("t8", "t8_rogue_device"),
    ("t9", "t9_stolen_key"),
    ("t10", "t10_link_tamper"),
    ("t11", "t11_actuator_refusal"),
]


def main() -> int:
    parser = make_common_parser("Run all Layer 1 Digital Twin scenarios (t1 through t11)")
    args = parser.parse_args()

    results = []
    print("=" * 70)
    print("  LAYER 1 DIGITAL TWIN SCENARIO SUITE (t1 - t11)")
    print("=" * 70)

    for sc_id, sc_mod_name in SCENARIOS:
        mod = importlib.import_module(sc_mod_name)
        ctx = ScenarioContext.from_args(args, sc_id, sc_mod_name)
        print(f"\n--- Running {sc_id} ({sc_mod_name}) ---")
        try:
            with ctx:
                ok = mod.run(ctx)
                results.append((sc_id, sc_mod_name, ok, ctx.log_file))
        except Exception as e:
            print(f"[{sc_id}] UNHANDLED EXCEPTION: {e}", file=sys.stderr)
            results.append((sc_id, sc_mod_name, False, getattr(ctx, "log_file", "none")))
        time.sleep(1.0)

    print("\n" + "=" * 70)
    print("  SCENARIO EXECUTION SUMMARY")
    print("=" * 70)
    all_passed = True
    for sc_id, sc_mod, passed, log_f in results:
        status_str = "PASS" if passed else "FAIL"
        if not passed:
            all_passed = False
        print(f"[{status_str:4s}] {sc_id:4s} {sc_mod:25s} -> {log_f}")

    print("=" * 70)
    print(f"Overall Result: {'ALL SCENARIOS PASSED' if all_passed else 'SOME SCENARIOS FAILED'}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())

