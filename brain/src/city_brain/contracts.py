"""The contracts the Brain publishes: its HTTP API and the topics it reads and writes.

`python -m city_brain.contracts --out contracts` writes them, and
`tests/conformance/test_self_conformance.py` fails if what is committed no longer matches
what the code serves.
"""

import argparse
import json
from pathlib import Path

ASYNCAPI = Path(__file__).resolve().parents[2] / "contracts" / "asyncapi.yaml"


def openapi_document() -> str:
    """The OpenAPI document for the query and operator API, as it will be served."""
    from city_brain.adapters.rest.api import build_rest_app

    class _NoBrain:
        """The document describes the routes, which do not depend on any live state."""

        state = None

    app = build_rest_app(_NoBrain(), "documented-token")
    return json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="city_brain.contracts")
    parser.add_argument("--out", type=Path, default=Path("contracts"))
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / "openapi.json"
    path.write_text(openapi_document(), encoding="utf-8")
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
