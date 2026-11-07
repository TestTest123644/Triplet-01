"""Queries over a run log's events. A field is matched in the envelope, then in `data`."""

import json
from pathlib import Path

COMMAND = "tn.tsyp.brain.command.v1"
VERIFICATION = "tn.tsyp.brain.verification.v1"
ALERT = "tn.tsyp.brain.alert.v1"


def read_log(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="ascii").splitlines()]


def _field(event: dict, key: str):
    return event[key] if key in event else event["data"].get(key)


def matching(events: list[dict], **fields) -> list[dict]:
    return [e for e in events if all(_field(e, k) == v for k, v in fields.items())]


def first(events: list[dict], **fields) -> dict:
    found = matching(events, **fields)
    assert found, f"no event matches {fields}"
    return found[0]


def count(events: list[dict], **fields) -> int:
    return len(matching(events, **fields))


def commands(events: list[dict]) -> list[dict]:
    return matching(events, type=COMMAND)


def verdicts_for(events: list[dict], target: str) -> list[str]:
    return [e["data"]["verdict"] for e in matching(events, type=VERIFICATION, subject=target)]


def max_level_before(events: list[dict], device_id: str, simtime: str) -> float:
    """The highest reading of the channel cited by any record up to `simtime`."""
    levels = [ref["value"] for e in events if e["simtime"] is not None and e["simtime"] <= simtime
              for ref in e["data"].get("evidence", ()) if ref["device_id"] == device_id]
    assert levels, f"no evidence for {device_id} before {simtime}"
    return max(levels)
