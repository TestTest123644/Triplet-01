"""`python -m city_brain`: P1 runs standalone scenarios from brain/; triplet mode is P4."""

import argparse
import sys
from pathlib import Path

from city_brain.config import load_config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="city_brain")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--scenario")
    args = parser.parse_args(argv)
    config = load_config(args.config)
    if config.mode == "triplet":
        print("triplet mode lands in P4", file=sys.stderr)
        return 2
    if args.scenario is None:
        parser.error("standalone mode needs --scenario")
    from scenarios.runner import run  # the harness brings the mocks; only this path may load them

    print(run(args.scenario, "standalone", config=config))
    return 0
