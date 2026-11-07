"""Brain event builders: CloudEvents 1.0 dicts for `brain.events` (spec 5.4).

The core never sets `time`; the log sink adds it. `data` holds JSON-native values only
(spec 9.2): enums as strings, datetimes as UTC strings, floats rounded, sets sorted.
"""

import dataclasses
import math
from collections.abc import Sequence
from datetime import datetime, timezone
from enum import Enum
from typing import Literal

from pydantic import BaseModel

from city_brain.core.shield import ShieldResult
from city_brain.core.state import BrainState
from city_brain.domain.types import ActionIntent, ActionStatus, ExpectedEffect

SOURCE = "urn:tsyp14:triplet-07:brain"
FLOAT_DIGITS = 6
_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(_TIME_FORMAT)


def _native(value):
    """Recursively convert to values `json.dumps` serialises deterministically."""
    if isinstance(value, Enum):
        return _native(value.value)
    if isinstance(value, bool) or value is None or isinstance(value, (int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("NaN and infinity cannot appear in a record")
        return round(value, FLOAT_DIGITS)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("naive datetime cannot appear in a record")
        return format_time(value)
    if isinstance(value, BaseModel):
        return {name: _native(getattr(value, name)) for name in type(value).model_fields}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: _native(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, dict):
        return {str(k): _native(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (set, frozenset)):
        return sorted((_native(v) for v in value), key=lambda v: (str(type(v)), v))
    if isinstance(value, (list, tuple)):
        return [_native(v) for v in value]
    raise TypeError(f"not JSON-native: {type(value).__name__}")


def _envelope(
    state: BrainState,
    kind: str,
    subject: str,
    data: dict,
    correlation_id: str | None,
    action_id: str | None,
) -> dict:
    native = _native(data)  # may raise; do it before consuming a sequence number
    event_id = f"{state.epoch}-{state.event_seq}"
    state.event_seq += 1
    return {
        "specversion": "1.0",
        "id": event_id,
        "source": SOURCE,
        "type": f"tn.tsyp.brain.{kind}.v1",
        "subject": subject,
        "simtime": None if state.epoch_origin is None else format_time(state.sim_now),
        "epoch": state.epoch,
        "correlationid": correlation_id,
        "actionid": action_id,
        "schemaversion": "1",
        "datacontenttype": "application/json",
        "data": native,
    }


def status_event(state: BrainState, reason: str, data: dict) -> dict:
    return _envelope(state, "status", "city", {**data, "reason": reason}, None, None)


def decision_event(
    state: BrainState,
    intent: ActionIntent | None,
    outcome: Literal["act", "hold", "notify", "dismiss"],
    incident: dict,
    rule_or_plan: str,
    evidence: Sequence,
    alternatives: Sequence,
    rationale: str,
    would_change_if: Sequence[str],
) -> dict:
    data = {
        "outcome": outcome,
        "incident": incident,
        "rule_or_plan": rule_or_plan,
        "evidence": evidence,
        "alternatives": alternatives,
        "rationale": rationale,
        "would_change_if": would_change_if,
    }
    correlation_id = intent.incident_id if intent is not None else incident.get("incident_id")
    return _envelope(
        state,
        "decision",
        str(incident.get("zone") or "city"),
        data,
        correlation_id,
        None if intent is None else intent.action_id,
    )


def command_event(
    state: BrainState,
    intent: ActionIntent,
    shield: ShieldResult,
    status: ActionStatus,
    tool_call: dict | None,
    twin_reply: dict | None,
    reasons: Sequence[str],
    evidence: Sequence = (),
) -> dict:
    data = {
        "status": status,
        "capability": intent.capability,
        "target": intent.target,
        "state": intent.state,
        "tool": tool_call,
        "risk_class": intent.risk_class,
        "shield": {"passed": shield.passed, "refusals": shield.refusals, "checks": shield.checks},
        "twin_reply": twin_reply,
        "reasons": reasons,
        "expected": intent.expected_effect,
    }
    if evidence:
        data["evidence"] = evidence
    return _envelope(state, "command", intent.target, data, intent.incident_id, intent.action_id)


def verification_event(
    state: BrainState,
    action_id: str,
    expected: ExpectedEffect,
    observed: dict,
    verdict: Literal["verified", "unverified", "inconclusive"],
    *,
    target: str,
    incident_id: str,
) -> dict:
    data = {"expected": expected, "observed": observed, "verdict": verdict}
    return _envelope(state, "verification", target, data, incident_id, action_id)


def incident_event(state: BrainState, incident) -> dict:
    """One incident as the Brain now reads it (spec 8.3.3)."""
    return _envelope(
        state, "incident", incident.zone_id, dict(incident), incident.incident_id, None)


def alert_event(
    state: BrainState,
    audience: Literal["operator", "guardian"],
    severity: str,
    code: str,
    message: str,
    refs: dict,
) -> dict:
    data = {"audience": audience, "severity": severity, "code": code, "message": message, "refs": refs}
    subject = str(refs.get("target") or refs.get("device_id") or "city")
    return _envelope(state, "alert", subject, data, refs.get("incident_id"), refs.get("action_id"))
