import json

import pytest

from city_brain.adapters.files.jsonl_log import GENESIS_HASH, JsonlLog, verify_chain


def edit_line(path, index, fields):
    lines = path.read_bytes().split(b"\n")
    record = json.loads(lines[index])
    record.update(fields)
    lines[index] = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("ascii")
    path.write_bytes(b"\n".join(lines))


def write_three(path):
    log = JsonlLog(path)
    return [log.append({"n": i}) for i in range(3)]


def test_chain_detects_tampering(tmp_path):
    path = tmp_path / "a.jsonl"
    write_three(path)
    assert verify_chain(path)
    edit_line(path, 1, {"n": 99})
    assert not verify_chain(path)


def test_append_returns_the_record_as_written(tmp_path):
    path = tmp_path / "a.jsonl"
    records = write_three(path)
    assert records[0]["prev_hash"] == GENESIS_HASH == "0" * 64
    assert records[1]["prev_hash"] == records[0]["hash"]
    assert "time" in records[0]
    assert [json.loads(line) for line in path.read_text(encoding="ascii").splitlines()] == records


def test_lines_are_canonical_with_lf_newlines(tmp_path):
    path = tmp_path / "a.jsonl"
    JsonlLog(path).append({"b": 1, "a": 2})
    raw = path.read_bytes()
    assert raw.endswith(b"\n") and b"\r" not in raw
    assert raw.startswith(b'{"a":2,"b":1,"hash":')


def test_nan_is_refused(tmp_path):
    log = JsonlLog(tmp_path / "a.jsonl")
    with pytest.raises(ValueError):
        log.append({"n": float("nan")})


def test_an_existing_log_is_never_reopened(tmp_path):
    path = tmp_path / "a.jsonl"
    write_three(path)
    with pytest.raises(FileExistsError):
        JsonlLog(path)


def test_creates_missing_directories(tmp_path):
    path = tmp_path / "logs" / "deep" / "a.jsonl"
    JsonlLog(path).append({"n": 1})
    assert verify_chain(path)


@pytest.mark.parametrize("damage", [
    lambda raw: raw.replace(b"\n", b"\nnot json\n", 1),
    lambda raw: raw + b"[1,2]\n",
    lambda raw: raw + b'{"n":3}\n',
    lambda raw: b"\xff" + raw,
    lambda raw: raw[:-5],
    lambda raw: b"\n".join(raw.split(b"\n")[1:]),  # first record removed
])
def test_verify_chain_returns_false_on_damage(tmp_path, damage):
    path = tmp_path / "a.jsonl"
    write_three(path)
    path.write_bytes(damage(path.read_bytes()))
    assert verify_chain(path) is False


def test_verify_chain_survives_crlf_conversion(tmp_path):
    path = tmp_path / "a.jsonl"
    write_three(path)
    path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
    assert verify_chain(path)


def test_verify_chain_of_an_empty_log_is_false(tmp_path):
    path = tmp_path / "a.jsonl"
    JsonlLog(path)
    assert verify_chain(path) is False


def test_verify_chain_of_a_missing_file_is_false(tmp_path):
    assert verify_chain(tmp_path / "nope.jsonl") is False
