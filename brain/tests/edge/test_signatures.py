import json

from city_brain.edge.signatures import KeyRing, generate_keypair, sign, verify

PAYLOAD = {"reading_id": "r-z1-wl1-7", "device_id": "z1-wl1", "device_type": "water_level",
           "ts": "2026-10-17T12:00:00Z", "seq": 7, "value": 12.0, "unit": "meters",
           "location": "zone-1", "status": "ok"}


def test_roundtrip():
    priv, pub = generate_keypair()
    signed = {**PAYLOAD, "sig": sign(PAYLOAD, priv)}
    assert verify(signed, pub)


def test_roundtrip_survives_json_reparse():  # float 12.0 and 0.0 must re-serialize identically
    priv, pub = generate_keypair()
    for v in (12.0, 0.0, 5.42):
        p = {**PAYLOAD, "value": v}
        wire = json.dumps({**p, "sig": sign(p, priv)})
        assert verify(json.loads(wire), pub)


def test_one_changed_byte_fails():
    priv, pub = generate_keypair()
    signed = {**PAYLOAD, "sig": sign(PAYLOAD, priv)}
    assert not verify({**signed, "value": 12.01}, pub)


def test_garbage_signature_or_key_returns_false():
    assert not verify({**PAYLOAD, "sig": "AAAA"}, generate_keypair()[1])
    assert not verify({**PAYLOAD, "sig": "AAAA"}, "not-base64!")


def test_missing_signature_returns_false():
    assert not verify(PAYLOAD, generate_keypair()[1])


def test_canonical_bytes_matches_twin_form():
    from city_brain.edge.signatures import canonical_bytes
    expected = json.dumps({k: v for k, v in PAYLOAD.items()}, sort_keys=True,
                          separators=(",", ":")).encode("utf-8")
    assert canonical_bytes({**PAYLOAD, "sig": "x"}) == expected


def test_key_ring_set_get_clear():
    ring = KeyRing()
    assert ring.get("d1") is None
    ring.set("d1", "pub")
    assert ring.get("d1") == "pub"
    ring.clear()
    assert ring.get("d1") is None
