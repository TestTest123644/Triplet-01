import json
from pathlib import Path

from city_brain.domain.types import TrustState
from city_brain.edge.guard import Accepted, AcceptedVerdict, Rejected, ReadingGuard, VerdictGuard
from city_brain.edge.profiles import GuardianProfile
from city_brain.edge.signatures import sign
from mocks.guardian.mock_guardian import MockGuardian, expired, trust_all, unsigned
from mocks.keys import derive_keypair
from mocks.twin import MockTwin
from tests.helpers_mock import guard_for_mock

GUARDIAN_PROFILE = Path(__file__).resolve().parents[2] / "profiles" / "guardian-triplet07.yaml"
G_PRIV, G_PUB = derive_keypair("test-guardian")

twin = MockTwin()
guard = guard_for_mock(twin)
READINGS = twin.tick()
WATER = next(r for r in READINGS if r["device_id"] == "z1-wl1")
WATER_PRIV, _ = derive_keypair(f"{twin.seed}:0:z1-wl1")


def b(obj) -> bytes:
    return json.dumps(obj).encode()


def signed_bytes(**changes) -> bytes:
    payload = {k: v for k, v in WATER.items() if k != "sig"}
    payload.update(changes)
    return b({**payload, "sig": sign(payload, WATER_PRIV)})


def rejected(raw: bytes, code: str, g=guard) -> Rejected:
    r = g.check(raw)
    assert isinstance(r, Rejected) and r.code == code, r
    return r


def test_mock_clean_stream_has_zero_rejects():
    t = MockTwin()
    g = guard_for_mock(t)
    results = [g.check(json.dumps(r).encode()) for _ in range(500) for r in t.tick()]
    assert len(results) == 21_000 and [r for r in results if isinstance(r, Rejected)] == []


def test_accepted_observation_is_canonical():
    r = guard.check(b(WATER))
    assert isinstance(r, Accepted)
    o = r.observation
    assert o.reading_id == WATER["reading_id"] and o.device_id == "z1-wl1" and o.zone_id == "zone-1"
    assert o.kind == "WATER_LEVEL" and o.unit == "m" and o.sim_time.year == 2026
    assert len(o.sig_hash) == 16 and o.flags == frozenset()


def test_malformed_json():
    rejected(b"{not json", "MALFORMED_JSON")
    rejected(b"\xff\xfe", "MALFORMED_JSON")


def test_oversized():
    r = rejected(b" " * 4097 + b(WATER), "OVERSIZED")
    assert r.device_id is None


def test_schema_violation_missing_field_and_extra():
    rejected(b({k: v for k, v in WATER.items() if k != "value"}), "SCHEMA_VIOLATION")
    r = rejected(b({**WATER, "extra": 1}), "SCHEMA_VIOLATION")
    assert r.device_id == "z1-wl1"
    rejected(b"[1, 2]", "SCHEMA_VIOLATION")


def test_bad_date_time_is_schema_violation():
    r = rejected(b({**WATER, "ts": "yesterday"}), "SCHEMA_VIOLATION")
    assert "ts" in r.detail


def test_unknown_device():
    r = rejected(signed_bytes(device_id="z9-xx9"), "UNKNOWN_DEVICE")
    assert r.device_id == "z9-xx9"


def test_type_mismatch_kind_and_unit():
    rejected(signed_bytes(device_type="power_load", unit="MW"), "TYPE_MISMATCH")
    rejected(signed_bytes(unit="MW"), "TYPE_MISMATCH")


def test_unknown_unit_is_type_mismatch_not_crash():
    rejected(signed_bytes(unit="furlongs"), "TYPE_MISMATCH")


def test_bad_signature_tampered_value():
    rejected(b({**WATER, "value": WATER["value"] + 1.0}), "BAD_SIGNATURE")


def test_bad_signature_wrong_key_and_missing_key():
    t = MockTwin()
    g = guard_for_mock(t)
    t.restart()  # new keys; the guard still holds the old ones
    rejected(b(t.tick()[0]), "BAD_SIGNATURE", g)
    g._keyring.clear()
    rejected(b(WATER), "BAD_SIGNATURE", g)


def test_stuck_42_on_water_is_accepted_with_out_of_range_flag():
    r = guard.check(signed_bytes(value=42.0))
    assert isinstance(r, Accepted) and "OUT_OF_RANGE" in r.observation.flags
    assert r.observation.flags == frozenset({"OUT_OF_RANGE", "AT_CEILING"})


def test_at_ceiling_flag_only_at_max():
    r = guard.check(signed_bytes(value=12.0))
    assert isinstance(r, Accepted) and r.observation.flags == frozenset({"AT_CEILING"})


def test_below_min_is_out_of_range():
    r = guard.check(signed_bytes(value=-0.5))
    assert isinstance(r, Accepted) and r.observation.flags == frozenset({"OUT_OF_RANGE"})


def test_guard_keeps_references_for_in_place_reload():
    t = MockTwin()
    g = guard_for_mock(t)
    t.restart()
    for raw in t.list_devices():
        g._keyring.set(raw["device_id"], t.get_device_key(raw["device_id"]))
    assert isinstance(g.check(b(t.tick()[0])), Accepted)


# ----- verdicts -----

gprofile = GuardianProfile.load(GUARDIAN_PROFILE, public_key_b64=G_PUB)
vguard = VerdictGuard(gprofile)


def verdict_bytes(policy=None) -> bytes:
    g = MockGuardian(G_PRIV, policy) if policy else MockGuardian(G_PRIV)
    return g.on_reading(WATER)[0]


def vrejected(raw: bytes, code: str) -> Rejected:
    r = vguard.check(raw)
    assert isinstance(r, Rejected) and r.code == code, r
    return r


def test_trusted_verdict_accepted():
    r = vguard.check(verdict_bytes(trust_all()))
    assert isinstance(r, AcceptedVerdict)
    assert r.verdict.state is TrustState.TRUSTED and r.verdict.device_id == "z1-wl1"
    assert r.verdict.issued_at.tzinfo is not None


def test_expired_verdict_passes_the_guard():
    assert isinstance(vguard.check(verdict_bytes(expired())), AcceptedVerdict)


def test_unsigned_verdict_rejected():
    assert vguard.check(verdict_bytes(unsigned())).code == "BAD_SIGNATURE"


def test_verdict_malformed_json():
    vrejected(b"nope", "MALFORMED_JSON")


def test_verdict_schema_violation():
    v = json.loads(verdict_bytes())
    r = vrejected(b({**v, "issued_at": "yesterday"}), "SCHEMA_VIOLATION")
    assert "issued_at" in r.detail
    vrejected(b({k: x for k, x in v.items() if k != "state"}), "SCHEMA_VIOLATION")


def test_unknown_verdict_state():
    v = json.loads(verdict_bytes())
    v["state"] = "WHATEVER"
    v["sig"] = sign(v, G_PRIV)
    vrejected(b(v), "UNKNOWN_VERDICT_STATE")


def test_verdict_bad_signature_tampered_and_wrong_key():
    v = json.loads(verdict_bytes())
    vrejected(b({**v, "trust": 0.5}), "BAD_SIGNATURE")
    other = MockGuardian(derive_keypair("other")[0]).on_reading(WATER)[0]
    vrejected(other, "BAD_SIGNATURE")
