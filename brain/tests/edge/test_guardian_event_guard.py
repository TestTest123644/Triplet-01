"""Edge guard for `guardian.events`: heartbeats, containment and verdict revocations (spec 5.3, 8.9)."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from city_brain.domain.types import GuardianContainment, GuardianHeartbeat, GuardianRevocation
from city_brain.edge.guard import AcceptedGuardianEvent, GuardianEventGuard, Rejected
from city_brain.edge.profiles import GuardianProfile
from city_brain.edge.signatures import sign
from mocks.guardian.mock_guardian import MockGuardian, quarantine_at
from mocks.keys import derive_keypair

GUARDIAN_PROFILE = Path(__file__).resolve().parents[2] / "profiles" / "guardian-triplet07.yaml"
PRIV, PUB = derive_keypair("test-guardian-events")
OTHER_PRIV, _ = derive_keypair("not-the-guardian")

guard = GuardianEventGuard(GuardianProfile.load(GUARDIAN_PROFILE, public_key_b64=PUB))

HEARTBEAT = {
    "kind": "heartbeat",
    "evaluated_through": "2026-10-17T12:00:03Z",
    "issued_at": "2026-10-17T12:00:04Z",
}
CONTAINMENT = {
    "kind": "containment",
    "device_id": "z2-wl1",
    "action": "quarantined",
    "effective_at": "2026-10-17T12:00:05Z",
    "reasons": ["CROSS_SENSOR_CONFLICT"],
    "issued_at": "2026-10-17T12:00:05Z",
}
REVOCATION = {
    "kind": "verdict_revoked",
    "verdict_id": "v-000042",
    "issued_at": "2026-10-17T12:00:06Z",
}


def signed(payload: dict, key: str = PRIV) -> bytes:
    return json.dumps({**payload, "sig": sign(payload, key)}, separators=(",", ":")).encode("utf-8")


def accepted(payload: dict):
    result = guard.check(signed(payload))
    assert isinstance(result, AcceptedGuardianEvent), result
    return result.notice


def rejected(raw: bytes, code: str) -> Rejected:
    result = guard.check(raw)
    assert isinstance(result, Rejected) and result.code == code, result
    return result


def test_heartbeat_is_accepted_and_canonical():
    notice = accepted(HEARTBEAT)
    assert isinstance(notice, GuardianHeartbeat)
    assert notice.evaluated_through == datetime(2026, 10, 17, 12, 0, 3, tzinfo=timezone.utc)
    assert notice.issued_at == datetime(2026, 10, 17, 12, 0, 4, tzinfo=timezone.utc)


def test_containment_is_accepted_and_canonical():
    notice = accepted(CONTAINMENT)
    assert isinstance(notice, GuardianContainment)
    assert notice.device_id == "z2-wl1" and notice.action == "quarantined"
    assert notice.effective_at == datetime(2026, 10, 17, 12, 0, 5, tzinfo=timezone.utc)
    assert notice.reasons == frozenset({"CROSS_SENSOR_CONFLICT"})


@pytest.mark.parametrize("action", ["isolated", "quarantined", "released", "rolled_back"])
def test_every_containment_action_is_accepted(action):
    notice = accepted({**CONTAINMENT, "action": action})
    assert notice.action == action


def test_revocation_is_accepted_and_canonical():
    notice = accepted(REVOCATION)
    assert isinstance(notice, GuardianRevocation)
    assert notice.verdict_id == "v-000042"
    assert notice.issued_at == datetime(2026, 10, 17, 12, 0, 6, tzinfo=timezone.utc)


def test_malformed_json():
    rejected(b"{not json", "MALFORMED_JSON")


def test_oversized():
    rejected(b'{"kind":"heartbeat","pad":"' + b"x" * 5000 + b'"}', "OVERSIZED")


def test_unknown_kind_violates_schema():
    rejected(signed({**HEARTBEAT, "kind": "gossip"}), "SCHEMA_VIOLATION")


def test_missing_kind_violates_schema():
    rejected(signed({k: v for k, v in HEARTBEAT.items() if k != "kind"}), "SCHEMA_VIOLATION")


@pytest.mark.parametrize(
    "payload, dropped",
    [
        (HEARTBEAT, "evaluated_through"),
        (HEARTBEAT, "issued_at"),
        (CONTAINMENT, "device_id"),
        (CONTAINMENT, "action"),
        (CONTAINMENT, "effective_at"),
        (CONTAINMENT, "reasons"),
        (REVOCATION, "verdict_id"),
    ],
)
def test_missing_required_field_violates_schema(payload, dropped):
    rejected(signed({k: v for k, v in payload.items() if k != dropped}), "SCHEMA_VIOLATION")


def test_unknown_field_violates_schema():
    rejected(signed({**HEARTBEAT, "severity": "high"}), "SCHEMA_VIOLATION")


def test_fields_from_another_kind_violate_schema():
    rejected(signed({**HEARTBEAT, "verdict_id": "v-1"}), "SCHEMA_VIOLATION")


def test_bad_containment_action_violates_schema():
    rejected(signed({**CONTAINMENT, "action": "burned"}), "SCHEMA_VIOLATION")


def test_bad_timestamp_violates_schema():
    rejected(signed({**HEARTBEAT, "issued_at": "yesterday"}), "SCHEMA_VIOLATION")


def test_bad_reason_token_violates_schema():
    rejected(signed({**CONTAINMENT, "reasons": ["not a token"]}), "SCHEMA_VIOLATION")


def test_schema_violation_carries_the_device_id():
    result = rejected(signed({**CONTAINMENT, "action": "burned"}), "SCHEMA_VIOLATION")
    assert result.device_id == "z2-wl1"


def test_missing_signature_is_bad_signature():
    raw = json.dumps(HEARTBEAT, separators=(",", ":")).encode("utf-8")
    rejected(raw, "BAD_SIGNATURE")


def test_signature_from_another_key_is_bad_signature():
    rejected(signed(CONTAINMENT, OTHER_PRIV), "BAD_SIGNATURE")


def test_tampering_after_signing_is_bad_signature():
    payload = {**CONTAINMENT, "sig": sign(CONTAINMENT, PRIV), "action": "released"}
    rejected(json.dumps(payload, separators=(",", ":")).encode("utf-8"), "BAD_SIGNATURE")


def test_guard_without_a_guardian_key_rejects_every_event():
    blind = GuardianEventGuard(GuardianProfile.load(GUARDIAN_PROFILE))
    result = blind.check(signed(HEARTBEAT))
    assert isinstance(result, Rejected) and result.code == "BAD_SIGNATURE"


def test_mock_guardian_containment_event_is_accepted():
    mock_priv, mock_pub = derive_keypair("mock-guardian-events")
    g = MockGuardian(mock_priv, quarantine_at("z1-wl1", 1.0))
    for i in range(3):
        g.on_reading({
            "reading_id": f"r-{i}", "device_id": "z1-wl1",
            "ts": f"2026-10-17T12:00:{i:02d}Z", "sig": "c2ln",
        })
    events = g.drain_events()
    assert events
    mock_guard = GuardianEventGuard(GuardianProfile.load(GUARDIAN_PROFILE, public_key_b64=mock_pub))
    for raw in events:
        result = mock_guard.check(raw)
        assert isinstance(result, AcceptedGuardianEvent), result
        assert isinstance(result.notice, GuardianContainment)
        assert result.notice.device_id == "z1-wl1" and result.notice.action == "quarantined"
