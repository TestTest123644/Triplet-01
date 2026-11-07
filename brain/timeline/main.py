"""`python -m timeline`: serve the Timeline page over a directory of run logs (spec 12.3)."""

import argparse
from pathlib import Path

import uvicorn

from timeline.server import build_timeline_app

DEFAULT_PORT = 8120


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="timeline")
    parser.add_argument("--logs", type=Path, default=Path("logs"), help="directory of run logs")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--brain-api", default="http://localhost:8100")
    args = parser.parse_args(argv)
    uvicorn.run(
        build_timeline_app(args.logs, brain_api_url=args.brain_api),
        host=args.host, port=args.port, log_level="info",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
