"""Ed25519 signatures over `brain.events`, on top of the hash chain (spec 5.4, 11.1)."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from city_brain.adapters.files.jsonl_log import JsonlLog, signed_view, verify_chain
from city_brain.edge.signatures import canonical_bytes, verify
from mocks.keys import derive_keypair

BRAIN = Path(__file__).resolve().parents[2]
PRIV, PUB = derive_keypair("test:brain:signing")
OTHER_PRIV, OTHER_PUB = derive_keypair("test:someone:else")


def event(n=0):
    return {
        "specversion": "1.0", "id": f"0-{n}", "source": "urn:tsyp14:triplet-07:brain",
        "type": "tn.tsyp.brain.status.v1", "subject": "city", "simtime": None, "epoch": 0,
        "correlationid": None, "actionid": None, "schemaversion": "1",
        "datacontenttype": "application/json", "data": {"reason": "startup"},
    }


def signed_log(tmp_path, name="run.jsonl", events=2):
    log = JsonlLog(tmp_path / name, private_key_b64=PRIV)
    records = [log.append(event(n)) for n in range(events)]
    return log, records


def lines(path):
    return [json.loads(line) for line in Path(path).read_bytes().splitlines()]


def rewrite(path, index, **changes):
    records = lines(path)
    records[index] = {**records[index], **changes}
    Path(path).write_bytes(b"".join(
        json.dumps(r, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
        for r in records
    ))


# ----- signing -----


def test_an_unsigned_log_still_works(tmp_path):
    log = JsonlLog(tmp_path / "run.jsonl")
    record = log.append(event())
    assert "sig" not in record and verify_chain(log.path)


def test_every_record_is_signed(tmp_path):
    log, records = signed_log(tmp_path)
    assert all("sig" in r for r in records)
    for record in records:
        assert verify(signed_view(record), PUB)
        assert not verify(record, PUB), "the hash is added after signing, so it is not covered"


def test_the_signature_covers_the_chain_link(tmp_path):
    log, records = signed_log(tmp_path)
    assert "prev_hash" in canonical_bytes(records[1]).decode("ascii")


def test_the_hash_covers_the_signature(tmp_path):
    log, records = signed_log(tmp_path, events=1)
    without = {k: v for k, v in records[0].items() if k not in ("hash", "sig")}
    from city_brain.adapters.files.jsonl_log import _digest

    assert _digest(records[0]) != _digest(without)


def test_the_chain_verifies_with_the_public_key(tmp_path):
    log, _ = signed_log(tmp_path)
    assert verify_chain(log.path, PUB)


def test_the_chain_verifies_without_the_public_key(tmp_path):
    log, _ = signed_log(tmp_path)
    assert verify_chain(log.path)


def test_the_wrong_public_key_fails(tmp_path):
    log, _ = signed_log(tmp_path)
    assert not verify_chain(log.path, OTHER_PUB)


def test_an_unsigned_record_fails_when_a_key_is_given(tmp_path):
    log = JsonlLog(tmp_path / "run.jsonl")
    log.append(event())
    assert verify_chain(log.path) and not verify_chain(log.path, PUB)


# ----- tampering -----


@pytest.mark.parametrize("field, value", [
    ("subject", "elsewhere"),
    ("epoch", 7),
    ("type", "tn.tsyp.brain.alert.v1"),
    ("data", {"reason": "nothing happened"}),
])
def test_tampering_breaks_both_the_hash_and_the_signature(tmp_path, field, value):
    log, _ = signed_log(tmp_path)
    rewrite(log.path, 0, **{field: value})
    assert not verify_chain(log.path), "the hash chain must notice"
    record = signed_view(lines(log.path)[0])
    assert not verify(record, PUB), "the signature must notice"


def test_rehashing_a_tampered_record_still_fails_the_signature(tmp_path):
    """An attacker who can recompute the chain still cannot forge the signature."""
    from city_brain.adapters.files.jsonl_log import _digest

    log, _ = signed_log(tmp_path)
    records = lines(log.path)
    forged = {**records[0], "subject": "elsewhere"}
    forged["hash"] = _digest(forged)
    records[0] = forged
    records[1] = {**records[1], "prev_hash": forged["hash"]}
    records[1]["hash"] = _digest(records[1])
    Path(log.path).write_bytes(b"".join(
        json.dumps(r, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
        for r in records))
    assert verify_chain(log.path), "the chain alone cannot tell"
    assert not verify_chain(log.path, PUB), "the signature can"


def test_a_removed_record_breaks_the_chain(tmp_path):
    log, _ = signed_log(tmp_path, events=3)
    records = lines(log.path)
    del records[1]
    Path(log.path).write_bytes(b"".join(
        json.dumps(r, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
        for r in records))
    assert not verify_chain(log.path, PUB)


def test_a_swapped_signature_from_another_record_fails(tmp_path):
    log, records = signed_log(tmp_path)
    rewrite(log.path, 0, sig=records[1]["sig"])
    assert not verify_chain(log.path, PUB)


# ----- the CLI -----


def run_cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "city_brain.adapters.files.jsonl_log", *args],
        cwd=BRAIN, capture_output=True, text=True,
        env={"PYTHONPATH": str(BRAIN / "src"), "SYSTEMROOT": "C:\\Windows", "PATH": ""},
    )


def test_the_cli_verifies_a_good_log(tmp_path):
    log, _ = signed_log(tmp_path)
    done = run_cli("--verify", str(log.path), "--pubkey", PUB)
    assert done.returncode == 0, done.stderr
    assert "ok" in done.stdout.lower()


def test_the_cli_rejects_a_tampered_log(tmp_path):
    log, _ = signed_log(tmp_path)
    rewrite(log.path, 0, subject="elsewhere")
    done = run_cli("--verify", str(log.path), "--pubkey", PUB)
    assert done.returncode == 1
    assert "fail" in done.stdout.lower() or "fail" in done.stderr.lower()


def test_the_cli_works_without_a_key(tmp_path):
    log, _ = signed_log(tmp_path)
    assert run_cli("--verify", str(log.path)).returncode == 0


def test_the_cli_rejects_a_missing_file(tmp_path):
    assert run_cli("--verify", str(tmp_path / "nope.jsonl")).returncode == 1
