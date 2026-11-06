import json

from city_brain.domain.ids import sig_hash
from city_brain.edge.signatures import verify
from mocks.guardian.mock_guardian import (
    MockGuardian, delay, expired, flip_after, quarantine_at, silent, trust_all, unsigned, untrusted)
from mocks.keys import derive_keypair

PRIV, PUB = derive_keypair("test-guardian")


def reading_for(device, ts="2026-10-17T12:00:00Z", seq=1):
    return {"reading_id": f"r-{device}-{seq}", "device_id": device, "ts": ts, "sig": "c2ln" + device}


def at_ts(i, device="z1-wl1"):
    return reading_for(device, f"2026-10-17T12:00:{i:02d}Z", i + 1)


reading = reading_for("z1-wl1")


def test_trust_all_verdict_binds_reading():
    v = json.loads(MockGuardian(PRIV).on_reading(reading)[0])
    assert v["reading_id"] == reading["reading_id"] and v["sig_hash"] == sig_hash(reading["sig"])
    assert verify(v, PUB)


def test_verdict_format_and_counter():
    g = MockGuardian(PRIV, trust_all(), ttl_s=30.0)
    v1, v2 = (json.loads(g.on_reading(at_ts(i))[0]) for i in range(2))
    assert v1["verdict_id"] == "v-000001" and v2["verdict_id"] == "v-000002"
    assert v1["state"] == "TRUSTED" and v1["device_id"] == "z1-wl1" and v1["reasons"] == []
    assert v1["issued_at"] == "2026-10-17T12:00:00Z" and v1["expires_at"] == "2026-10-17T12:00:30Z"
    assert 0.0 <= v1["trust"] <= 1.0


def test_same_inputs_give_identical_bytes():
    assert MockGuardian(PRIV).on_reading(reading) == MockGuardian(PRIV).on_reading(reading)


def test_silent_emits_nothing():
    assert MockGuardian(PRIV, silent()).on_reading(reading) == []


def test_delay_holds_until_sim_time_passes():
    g = MockGuardian(PRIV, delay(2.0))
    assert g.on_reading(at_ts(0)) == [] and g.on_reading(at_ts(1)) == []
    assert len(g.on_reading(at_ts(2))) >= 1


def test_delay_releases_verdict_for_the_held_reading():
    g = MockGuardian(PRIV, delay(2.0)); g.on_reading(at_ts(0)); g.on_reading(at_ts(1))
    out = [json.loads(b) for b in g.on_reading(at_ts(2))]
    assert [v["reading_id"] for v in out] == [at_ts(0)["reading_id"]]
    assert out[0]["issued_at"] == "2026-10-17T12:00:00Z"


def test_flip_after_turns_untrusted():
    g = MockGuardian(PRIV, flip_after(1))
    states = [json.loads(g.on_reading(at_ts(i))[0])["state"] for i in range(2)]
    assert states == ["TRUSTED", "UNTRUSTED"]


def test_untrusted_applies_to_one_device_only():
    g = MockGuardian(PRIV, untrusted("z1-wl1"))
    assert json.loads(g.on_reading(reading_for("z1-wl1"))[0])["state"] == "UNTRUSTED"
    assert json.loads(g.on_reading(reading_for("z2-wl1"))[0])["state"] == "TRUSTED"


def test_quarantine_at_queues_one_event():
    g = MockGuardian(PRIV, quarantine_at("z1-wl1", 1.0)); g.on_reading(at_ts(0)); g.on_reading(at_ts(2)); g.on_reading(at_ts(3))
    events = [json.loads(e) for e in g.drain_events()]
    assert [e["action"] for e in events] == ["quarantined"] and events[0]["device_id"] == "z1-wl1"


def test_quarantine_at_flips_verdicts_and_events_are_signed_and_drained():
    g = MockGuardian(PRIV, quarantine_at("z1-wl1", 1.0))
    states = [json.loads(g.on_reading(at_ts(i))[0])["state"] for i in range(3)]
    assert states == ["TRUSTED", "UNTRUSTED", "UNTRUSTED"]
    ev = [json.loads(e) for e in g.drain_events()]
    assert ev[0]["kind"] == "containment" and verify(ev[0], PUB) and g.drain_events() == []


def test_expired_verdict_is_already_past():
    v = json.loads(MockGuardian(PRIV, expired()).on_reading(reading)[0])
    assert v["expires_at"] < v["issued_at"]


def test_unsigned_has_no_sig():
    assert "sig" not in json.loads(MockGuardian(PRIV, unsigned()).on_reading(reading)[0])


def test_derive_keypair_is_deterministic_and_distinct():
    assert derive_keypair("a") == derive_keypair("a") and derive_keypair("a") != derive_keypair("b")


def test_unknown_policy_kind_raises():
    import pytest
    from mocks.guardian.mock_guardian import GuardianPolicy
    with pytest.raises(ValueError):
        GuardianPolicy("trsut_all")
