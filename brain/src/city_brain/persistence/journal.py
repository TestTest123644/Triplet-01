"""A write-ahead journal and a transactional outbox, in one SQLite file (spec 9.4).

Every step is written in one transaction: the event that caused it and the effects it
produced. A Brain that comes back after a crash replays the journal to rebuild exactly the
state it had, and finds in the outbox the commands that were written but may never have been
sent. Those are the dangerous ones, so the runtime asks the Twin whether it already has a
result for the idempotency key before sending anything again (spec 5.2, 8.8).
"""

import json
import sqlite3
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from city_brain.core.events import (
    CommandResult,
    Effect,
    Event,
    GuardianEventReceived,
    OperatorCommand,
    Publish,
    ReadingReceived,
    ReadingRejected,
    RegistryLoaded,
    SendCommand,
    TimerFired,
    VerdictReceived,
)
from city_brain.domain.types import (
    DeviceProfile,
    GuardianContainment,
    GuardianHeartbeat,
    GuardianRevocation,
    Observation,
    Verdict,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS journal_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_effects (
    effect_id INTEGER PRIMARY KEY AUTOINCREMENT,
    journal_seq INTEGER NOT NULL,
    effect_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    dispatched INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox_effects (dispatched, effect_id);
"""
# Only the effects that leave the process are worth replaying.
JOURNALLED_EFFECTS = (Publish, SendCommand)


# ----- events on the wire of the journal -----


def _guardian_notice(payload: dict) -> Any:
    kind = payload["kind"]
    body = payload["notice"]
    if kind == "heartbeat":
        return GuardianHeartbeat.model_validate(body)
    if kind == "containment":
        return GuardianContainment.model_validate(body)
    return GuardianRevocation.model_validate(body)


def _notice_kind(notice: Any) -> str:
    if isinstance(notice, GuardianHeartbeat):
        return "heartbeat"
    if isinstance(notice, GuardianContainment):
        return "containment"
    return "verdict_revoked"


def encode_event(event: Event) -> tuple[str, str]:
    """An event as (type name, JSON), losing nothing the core will need back."""
    if isinstance(event, RegistryLoaded):
        payload = {
            "devices": [d.model_dump(mode="json") for d in event.devices],
            "allowlist": {cap: sorted(states) for cap, states in event.allowlist.items()},
            "keys_changed": event.keys_changed,
        }
        return "RegistryLoaded", json.dumps(payload, sort_keys=True)
    if isinstance(event, ReadingReceived):
        return "ReadingReceived", json.dumps({"obs": event.obs.model_dump(mode="json")}, sort_keys=True)
    if isinstance(event, ReadingRejected):
        return "ReadingRejected", json.dumps(
            {"code": event.code, "device_id": event.device_id}, sort_keys=True)
    if isinstance(event, VerdictReceived):
        return "VerdictReceived", json.dumps(
            {"verdict": event.verdict.model_dump(mode="json")}, sort_keys=True)
    if isinstance(event, GuardianEventReceived):
        return "GuardianEventReceived", json.dumps(
            {"kind": _notice_kind(event.notice), "notice": event.notice.model_dump(mode="json")},
            sort_keys=True)
    if isinstance(event, OperatorCommand):
        return "OperatorCommand", json.dumps(
            {"action": event.action, "action_id": event.action_id,
             "operator_id": event.operator_id}, sort_keys=True)
    if isinstance(event, CommandResult):
        return "CommandResult", json.dumps(
            {"action_id": event.action_id, "outcome": event.outcome, "reply": event.reply},
            sort_keys=True)
    if isinstance(event, TimerFired):
        return "TimerFired", json.dumps(
            {"timer_id": event.timer_id, "kind": event.kind}, sort_keys=True)
    raise TypeError(f"not a journalled event: {type(event).__name__}")


def decode_event(event_type: str, payload_json: str) -> Event:
    payload = json.loads(payload_json)
    if event_type == "RegistryLoaded":
        return RegistryLoaded(
            devices=tuple(DeviceProfile.model_validate(d) for d in payload["devices"]),
            allowlist={cap: frozenset(states) for cap, states in payload["allowlist"].items()},
            keys_changed=bool(payload["keys_changed"]),
        )
    if event_type == "ReadingReceived":
        return ReadingReceived(Observation.model_validate(payload["obs"]))
    if event_type == "ReadingRejected":
        return ReadingRejected(payload["code"], payload["device_id"])
    if event_type == "VerdictReceived":
        return VerdictReceived(Verdict.model_validate(payload["verdict"]))
    if event_type == "GuardianEventReceived":
        return GuardianEventReceived(_guardian_notice(payload))
    if event_type == "OperatorCommand":
        return OperatorCommand(
            action=payload["action"], action_id=payload["action_id"],
            operator_id=payload["operator_id"])
    if event_type == "CommandResult":
        return CommandResult(payload["action_id"], payload["outcome"], payload["reply"])
    if event_type == "TimerFired":
        return TimerFired(payload["timer_id"], payload["kind"])
    raise ValueError(f"unknown journalled event type '{event_type}'")


def encode_effect(effect: Effect) -> tuple[str, str]:
    if isinstance(effect, Publish):
        return "Publish", json.dumps({"event": effect.event}, sort_keys=True)
    if isinstance(effect, SendCommand):
        return "SendCommand", json.dumps({
            "action_id": effect.action_id, "idempotency_key": effect.idempotency_key,
            "capability": effect.capability, "target": effect.target, "state": effect.state,
        }, sort_keys=True)
    raise TypeError(f"not a journalled effect: {type(effect).__name__}")


def decode_effect(effect_type: str, payload_json: str) -> Effect:
    payload = json.loads(payload_json)
    if effect_type == "Publish":
        return Publish(payload["event"])
    if effect_type == "SendCommand":
        return SendCommand(
            payload["action_id"], payload["idempotency_key"], payload["capability"],
            payload["target"], payload["state"])
    raise ValueError(f"unknown journalled effect type '{effect_type}'")


# ----- the journal -----


class SqliteJournal:
    """One SQLite file in WAL mode: the events the Brain saw and the effects it owes."""

    def __init__(self, db_path: Path):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")  # a crash must not lose a dispatched command
        self._db.executescript(SCHEMA)

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> "SqliteJournal":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def append_step(self, epoch: int, event: Event, effects: Sequence[Effect]) -> int:
        """Write one step — the event and the effects it produced — atomically."""
        event_type, payload = encode_event(event)
        rows = [encode_effect(e) for e in effects if isinstance(e, JOURNALLED_EFFECTS)]
        cursor = self._db.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            cursor.execute(
                "INSERT INTO journal_events (epoch, event_type, payload_json) VALUES (?, ?, ?)",
                (int(epoch), event_type, payload))
            seq = int(cursor.lastrowid)
            cursor.executemany(
                "INSERT INTO outbox_effects (journal_seq, effect_type, payload_json) VALUES (?, ?, ?)",
                [(seq, kind, body) for kind, body in rows])
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        return seq

    def pending_outbox(self) -> list[tuple[int, Effect]]:
        """The effects written but not yet confirmed as sent, oldest first."""
        rows = self._db.execute(
            "SELECT effect_id, effect_type, payload_json FROM outbox_effects "
            "WHERE dispatched = 0 ORDER BY effect_id").fetchall()
        return [(int(r["effect_id"]), decode_effect(r["effect_type"], r["payload_json"])) for r in rows]

    def mark_dispatched(self, effect_id: int) -> None:
        self._db.execute(
            "UPDATE outbox_effects SET dispatched = 1 WHERE effect_id = ?", (int(effect_id),))

    def mark_all_dispatched(self) -> None:
        self._db.execute("UPDATE outbox_effects SET dispatched = 1 WHERE dispatched = 0")

    def replay_events(self, epoch: int | None = None) -> list[Event]:
        """Every event the Brain saw, in the order it saw them."""
        if epoch is None:
            rows = self._db.execute(
                "SELECT event_type, payload_json FROM journal_events ORDER BY seq").fetchall()
        else:
            rows = self._db.execute(
                "SELECT event_type, payload_json FROM journal_events WHERE epoch = ? ORDER BY seq",
                (int(epoch),)).fetchall()
        return [decode_event(r["event_type"], r["payload_json"]) for r in rows]

    def last_epoch(self) -> int:
        row = self._db.execute("SELECT MAX(epoch) AS epoch FROM journal_events").fetchone()
        return 0 if row is None or row["epoch"] is None else int(row["epoch"])

    def event_count(self) -> int:
        return int(self._db.execute("SELECT COUNT(*) AS n FROM journal_events").fetchone()["n"])
