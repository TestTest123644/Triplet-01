"""Hash-chained JSONL run log (spec 12.2): one canonical JSON event per line, LF on every platform."""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

GENESIS_HASH = "0" * 64


def _canonical(record: dict[str, Any]) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")


def _digest(record: dict[str, Any]) -> str:
    """SHA-256 of the canonical record, which covers `prev_hash` but not `hash`."""
    return hashlib.sha256(_canonical({k: v for k, v in record.items() if k != "hash"})).hexdigest()


class JsonlLog:
    """Owns a new log file; refuses an existing one, because a second chain would corrupt it."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.open("xb").close()
        self._prev_hash = GENESIS_HASH

    def append(self, event: dict) -> dict:
        record = {**event, "time": datetime.now(timezone.utc).isoformat(), "prev_hash": self._prev_hash}
        record["hash"] = _digest(record)
        with self.path.open("ab") as fh:
            fh.write(_canonical(record) + b"\n")
        self._prev_hash = record["hash"]
        return record


def verify_chain(path: Path) -> bool:
    """True only if there are records and each links to the one before it and matches its hash. Never raises."""
    prev_hash = GENESIS_HASH
    try:
        lines = Path(path).read_bytes().splitlines()
        if not lines:
            return False
        for line in lines:
            record = json.loads(line)
            if record["prev_hash"] != prev_hash or record["hash"] != _digest(record):
                return False
            prev_hash = record["hash"]
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        return False
    return True
