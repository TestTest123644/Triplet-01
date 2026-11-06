import hashlib

from city_brain.domain.ids import action_id, idempotency_key, sig_hash


def test_sig_hash_is_first_16_hex_of_sha256():
    assert sig_hash("abc") == hashlib.sha256(b"abc").hexdigest()[:16]


def test_action_id_is_deterministic_and_sensitive():
    a = action_id("inc-1", "valve", "z1-wl1", "open", 1)
    assert a == action_id("inc-1", "valve", "z1-wl1", "open", 1)
    assert a != action_id("inc-1", "valve", "z1-wl1", "open", 2)
    assert a.startswith("act-") and len(a) == 20


def test_action_id_hashes_pipe_joined_parts():
    expected = hashlib.sha256(b"inc-1|valve|z1-wl1|open|1").hexdigest()[:16]
    assert action_id("inc-1", "valve", "z1-wl1", "open", 1) == "act-" + expected


def test_idempotency_key_format():
    assert idempotency_key(3, "act-0123456789abcdef") == "brain-3-act-0123456789abcdef"
