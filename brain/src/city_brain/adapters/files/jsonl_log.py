"""Hash-chained JSONL run log (spec 12.2): one canonical JSON event per line, LF on every platform.

Each record links to the one before it through `prev_hash`, and its `hash` covers that link.
When the Brain holds a signing key, each record also carries an Ed25519 `sig` over everything
but the hash (spec 5.4, 11.1): the chain proves nothing was dropped or reordered, and the
signature proves the Brain wrote it. Anyone can check both with

    python -m city_brain.adapters.files.jsonl_log --verify <path> [--pubkey <b64>]
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from city_brain.edge.signatures import sign, verify

GENESIS_HASH = "0" * 64


def _canonical(record: dict[str, Any]) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")


def signed_view(record: dict[str, Any]) -> dict[str, Any]:
    """The part of a record its signature covers: everything but the hash that wraps it."""
    return {k: v for k, v in record.items() if k != "hash"}


def _digest(record: dict[str, Any]) -> str:
    """SHA-256 of the canonical record, which covers `prev_hash` and `sig` but not `hash`."""
    return hashlib.sha256(_canonical({k: v for k, v in record.items() if k != "hash"})).hexdigest()


class JsonlLog:
    """Owns a new log file; refuses an existing one, because a second chain would corrupt it."""

    def __init__(self, path: Path, private_key_b64: str | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.open("xb").close()
        self._prev_hash = GENESIS_HASH
        self._key = private_key_b64

    def append(self, event: dict) -> dict:
        record = {**event, "time": datetime.now(timezone.utc).isoformat(), "prev_hash": self._prev_hash}
        if self._key is not None:  # signed before hashing, so the hash covers the signature
            record["sig"] = sign(record, self._key)
        record["hash"] = _digest(record)
        with self.path.open("ab") as fh:
            fh.write(_canonical(record) + b"\n")
        self._prev_hash = record["hash"]
        return record


def verify_chain(path: Path, public_key_b64: str | None = None) -> bool:
    """True only if there are records, each links to the one before it and matches its hash,
    and — when a public key is given — each carries a valid Ed25519 signature. Never raises."""
    prev_hash = GENESIS_HASH
    try:
        lines = Path(path).read_bytes().splitlines()
        if not lines:
            return False
        for line in lines:
            record = json.loads(line)
            if record["prev_hash"] != prev_hash or record["hash"] != _digest(record):
                return False
            if public_key_b64 is not None and not verify(signed_view(record), public_key_b64):
                return False
            prev_hash = record["hash"]
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="city_brain.adapters.files.jsonl_log")
    parser.add_argument("--verify", required=True, metavar="PATH", help="run log to check")
    parser.add_argument("--pubkey", default=None, help="base64 Ed25519 key the events were signed with")
    args = parser.parse_args(argv)
    ok = verify_chain(Path(args.verify), args.pubkey)
    signed = " and signatures" if args.pubkey else ""
    print(f"{'ok' if ok else 'FAILED'}: {args.verify} chain{signed}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
