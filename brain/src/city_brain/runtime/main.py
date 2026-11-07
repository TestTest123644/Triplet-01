"""`python -m city_brain`: a scenario in standalone mode, or the Brain on the triplet network."""

import argparse
import os
import sys
from pathlib import Path

from city_brain.config import load_config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="city_brain")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--scenario")
    parser.add_argument("--journal", type=Path, default=None, help="SQLite journal to run from")
    parser.add_argument(
        "--operator-token", default=os.environ.get("BRAIN_OPERATOR_TOKEN", ""),
        help="the token the approval and control endpoints require")
    args = parser.parse_args(argv)
    config = load_config(args.config, os.environ)
    if config.mode == "triplet":
        if not args.operator_token:
            parser.error("triplet mode needs --operator-token (or BRAIN_OPERATOR_TOKEN)")
        import asyncio

        from city_brain.runtime.triplet import serve

        return asyncio.run(serve(config, args.operator_token, journal_path=args.journal))
    if args.scenario is None:
        parser.error("standalone mode needs --scenario")
    from scenarios.runner import run  # the harness brings the mocks; only this path may load them

    print(run(args.scenario, "standalone", config=config))
    return 0
