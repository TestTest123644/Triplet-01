"""A scripted Guardian stand-in: signed verdicts (spec 5.3) under a fixed policy, plus containment events."""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from city_brain.domain.ids import sig_hash
from city_brain.edge.signatures import sign

_TS = "%Y-%m-%dT%H:%M:%SZ"


@dataclass(frozen=True)
class GuardianPolicy:
    kind: str
    device: str | None = None
    seconds: float = 0.0
    count: int = 0


def trust_all() -> GuardianPolicy:
    return GuardianPolicy("trust_all")


def delay(n_s: float) -> GuardianPolicy:
    return GuardianPolicy("delay", seconds=n_s)


def silent() -> GuardianPolicy:
    return GuardianPolicy("silent")


def flip_after(n: int) -> GuardianPolicy:
    return GuardianPolicy("flip_after", count=n)


def untrusted(device: str) -> GuardianPolicy:
    return GuardianPolicy("untrusted", device=device)


def quarantine_at(device: str, t: float) -> GuardianPolicy:
    return GuardianPolicy("quarantine_at", device=device, seconds=t)


def expired() -> GuardianPolicy:
    return GuardianPolicy("expired")


def unsigned() -> GuardianPolicy:
    return GuardianPolicy("unsigned")


def _parse(ts: str) -> datetime:
    return datetime.strptime(ts, _TS).replace(tzinfo=timezone.utc)


def _fmt(dt: datetime) -> str:
    return dt.strftime(_TS)


class MockGuardian:
    def __init__(self, private_key_b64: str, policy: GuardianPolicy = trust_all(), ttl_s: float = 30.0):
        self._key = private_key_b64
        self._policy = policy
        self._ttl = timedelta(seconds=ttl_s)
        self._counter = 0
        self._first: datetime | None = None
        self._held: list[tuple[datetime, bytes]] = []  # (due sim time, verdict bytes)
        self._events: list[bytes] = []
        self._quarantined = False

    def _state(self, device_id: str, now: datetime) -> str:
        p = self._policy
        if p.kind == "flip_after":
            return "TRUSTED" if self._counter <= p.count else "UNTRUSTED"
        if p.kind == "untrusted":
            return "UNTRUSTED" if device_id == p.device else "TRUSTED"
        if p.kind == "quarantine_at" and device_id == p.device and self._first is not None:
            return "UNTRUSTED" if now - self._first >= timedelta(seconds=p.seconds) else "TRUSTED"
        return "TRUSTED"

    def _verdict(self, raw: dict[str, Any], now: datetime) -> bytes:
        self._counter += 1
        state = self._state(raw["device_id"], now)
        issued = _parse(raw["ts"])
        expires = issued - timedelta(seconds=1) if self._policy.kind == "expired" else issued + self._ttl
        verdict = {
            "verdict_id": f"v-{self._counter:06d}", "reading_id": raw["reading_id"],
            "sig_hash": sig_hash(raw["sig"]), "device_id": raw["device_id"], "state": state,
            "trust": 1.0 if state == "TRUSTED" else 0.0,
            "reasons": [] if state == "TRUSTED" else ["SCRIPTED"],
            "issued_at": _fmt(issued), "expires_at": _fmt(expires)}
        if self._policy.kind != "unsigned":
            verdict["sig"] = sign(verdict, self._key)
        return json.dumps(verdict, separators=(",", ":")).encode("utf-8")

    def _maybe_quarantine(self, now: datetime) -> None:
        p = self._policy
        if p.kind != "quarantine_at" or self._quarantined or now - self._first < timedelta(seconds=p.seconds):
            return
        self._quarantined = True
        event = {"kind": "containment", "device_id": p.device, "action": "quarantined",
                 "effective_at": _fmt(now), "reasons": ["SCRIPTED"]}
        event["sig"] = sign(event, self._key)
        self._events.append(json.dumps(event, separators=(",", ":")).encode("utf-8"))

    def on_reading(self, raw: dict[str, Any]) -> list[bytes]:
        """Verdicts due at this reading's simulation time (usually one)."""
        now = _parse(raw["ts"])
        if self._first is None:
            self._first = now
        self._maybe_quarantine(now)
        kind = self._policy.kind
        if kind == "silent":
            return []
        verdict = self._verdict(raw, now)
        if kind != "delay":
            return [verdict]
        self._held.append((now + timedelta(seconds=self._policy.seconds), verdict))
        due = [v for t, v in self._held if t <= now]
        self._held = [(t, v) for t, v in self._held if t > now]
        return due

    def drain_events(self) -> list[bytes]:
        """Signed `guardian.events` messages queued since the last drain."""
        out, self._events = self._events, []
        return out
