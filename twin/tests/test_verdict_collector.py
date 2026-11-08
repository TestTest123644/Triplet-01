"""Tests for twin/console/verdict_collector.py."""

import json
import os
from pathlib import Path
import sqlite3
import sys
import pytest

try:
    from console.timeline import build_timeline_rows, load_partner_log_entries
    from console.verdict_collector import (
        VerdictCollector,
        get_verdict_config,
        normalize_verdict_payload,
    )
except ImportError:
    from twin.console.timeline import build_timeline_rows, load_partner_log_entries
    from twin.console.verdict_collector import (
        VerdictCollector,
        get_verdict_config,
        normalize_verdict_payload,
    )

from twin.db_ledger import GroundTruthLedger


@pytest.fixture
def temp_ledger(tmp_path):
    """Create a temporary SQLite ground-truth ledger populated with known test readings."""
    db_file = tmp_path / "test_ground_truth.db"
    ledger = GroundTruthLedger(db_path=str(db_file))
    ledger.close()

    conn = sqlite3.connect(str(db_file))
    conn.execute("""
        INSERT INTO readings_truth (reading_id, device_id, ts, seq, true_value, broadcast_value, unit, status, created_at_wall_clock)
        VALUES ('r-z1-tc1-100', 'z1-tc1', '2026-10-17T12:00:10Z', 100, 45.0, 45.0, 'veh/min', 'ok', '2026-11-08T18:00:00Z'),
               ('r-z1-tc1-101', 'z1-tc1', '2026-10-17T12:00:11Z', 101, 48.0, 48.0, 'veh/min', 'ok', '2026-11-08T18:00:01Z'),
               ('r-z2-wl1-50',  'z2-wl1', '2026-10-17T12:00:20Z', 50,  3.5,  3.5,  'meters',  'ok', '2026-11-08T18:00:02Z')
    """)
    conn.commit()
    conn.close()
    return db_file


def test_missing_verdict_topic_exits_2(capsys):
    """Missing VERDICT_TOPIC causes get_verdict_config to exit with code 2."""
    env = {"TIMELINE_LOG_DIR": "/tmp/logs"}
    with pytest.raises(SystemExit) as exc:
        get_verdict_config(env)
    assert exc.value.code == 2
    captured = capsys.readouterr()
    assert "Missing required environment variable: VERDICT_TOPIC" in captured.err


def test_missing_timeline_log_dir_exits_2(capsys):
    """Missing TIMELINE_LOG_DIR causes get_verdict_config to exit with code 2."""
    env = {"VERDICT_TOPIC": "guardian-verdicts"}
    with pytest.raises(SystemExit) as exc:
        get_verdict_config(env)
    assert exc.value.code == 2
    captured = capsys.readouterr()
    assert "Missing required environment variable: TIMELINE_LOG_DIR" in captured.err


def test_password_never_in_errors_or_logs(tmp_path, capsys):
    """SASL password value is never printed or logged on missing/invalid file."""
    secret = "SUPER_SECRET_VERDICT_PASSWORD_42"
    pw_file = tmp_path / "empty_pw.txt"
    pw_file.write_text("   \n", encoding="utf-8")

    env = {
        "VERDICT_TOPIC": "guardian-verdicts",
        "TIMELINE_LOG_DIR": str(tmp_path),
        "VERDICT_KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "VERDICT_KAFKA_USERNAME": "test_user",
        "VERDICT_KAFKA_PASSWORD_FILE": str(pw_file),
        "VERDICT_KAFKA_PASSWORD": secret,
    }

    with pytest.raises(SystemExit) as exc:
        get_verdict_config(env)
    assert exc.value.code == 2
    captured = capsys.readouterr()
    assert secret not in captured.err
    assert secret not in captured.out
    assert "is empty" in captured.err


def test_normalization_aliases():
    """Verify tolerant field alias mapping for unknown partners."""
    # 1. Device aliases
    for k in ("device_id", "deviceId", "device", "target", "target_device_id"):
        raw = json.dumps({k: "z1-tc1", "reading_id": "r-1"}).encode("utf-8")
        norm = normalize_verdict_payload(raw)
        assert norm is not None
        assert norm["device_id"] == "z1-tc1"

    # 2. Reading aliases
    for k in ("reading_id", "readingId"):
        raw = json.dumps({"device_id": "z1-tc1", k: "r-999"}).encode("utf-8")
        norm = normalize_verdict_payload(raw)
        assert norm is not None
        assert norm["reading_id"] == "r-999"

    # 3. Event aliases and default
    for k in ("label", "verdict", "event", "type", "classification", "action"):
        raw = json.dumps({"device_id": "z1-tc1", k: "anomaly"}).encode("utf-8")
        norm = normalize_verdict_payload(raw)
        assert norm is not None
        assert norm["event"] == "anomaly"

    default_event_raw = json.dumps({"device_id": "z1-tc1"}).encode("utf-8")
    assert normalize_verdict_payload(default_event_raw)["event"] == "verdict"

    # 4. Reason aliases
    for k in ("reason", "details", "message", "explanation"):
        raw = json.dumps({"device_id": "z1-tc1", k: "high drift"}).encode("utf-8")
        norm = normalize_verdict_payload(raw)
        assert norm is not None
        assert norm["reason"] == "high drift"

    # 5. Wall time aliases
    for k in ("detected_at", "timestamp", "ts", "time"):
        raw = json.dumps({"device_id": "z1-tc1", k: "2026-11-08T18:00:00Z"}).encode("utf-8")
        norm = normalize_verdict_payload(raw)
        assert norm is not None
        assert norm["wall_ts"] == "2026-11-08T18:00:00Z"


def test_normalization_skipped_and_counted():
    """Verify non-JSON, list, 70 KB payload, and missing identifiers are rejected."""
    # Non-JSON
    assert normalize_verdict_payload(b"not-json-at-all") is None

    # List payload
    assert normalize_verdict_payload(b'[{"device_id": "z1-tc1"}]') is None

    # > 64 KB payload
    big_payload = json.dumps({"device_id": "z1-tc1", "data": "x" * 70000}).encode("utf-8")
    assert len(big_payload) > 65536
    assert normalize_verdict_payload(big_payload) is None

    # Missing both device and reading
    assert normalize_verdict_payload(b'{"label": "sensor_malfunction"}') is None


def test_reading_id_resolves_ledger_ts(temp_ledger, tmp_path):
    """reading_id in payload resolves sim_ts from readings_truth row with source reading_id."""
    config = {
        "topic": "verdicts.topic",
        "layer": "guardian",
        "log_dir": str(tmp_path),
        "db_path": str(temp_ledger),
    }
    collector = VerdictCollector(config)

    # detected_at is set far in the future to prove it is NEVER used as sim_ts
    payload = json.dumps({
        "device_id": "z1-tc1",
        "reading_id": "r-z1-tc1-100",
        "label": "anomaly",
        "detected_at": "2099-01-01T00:00:00Z",
    }).encode("utf-8")

    success = collector.process_message_bytes(payload)
    assert success is True
    assert collector.written == 1
    assert collector.by_source["reading_id"] == 1

    collector.close()

    out_file = tmp_path / "verdicts-guardian.jsonl"
    assert out_file.is_file()
    lines = out_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])

    assert record["sim_ts"] == "2026-10-17T12:00:10Z"
    assert record["sim_ts"] != "2099-01-01T00:00:00Z"
    assert record["details"]["sim_ts_source"] == "reading_id"
    assert record["details"]["wall_ts"] == "2099-01-01T00:00:00Z"


def test_null_reading_id_falls_back_to_device_latest(temp_ledger, tmp_path):
    """null reading_id resolves to latest ts for that device in readings_truth."""
    config = {
        "topic": "verdicts.topic",
        "layer": "guardian",
        "log_dir": str(tmp_path),
        "db_path": str(temp_ledger),
    }
    collector = VerdictCollector(config)

    # z1-tc1 has two readings in fixture: 100 at 12:00:10Z and 101 at 12:00:11Z
    payload = json.dumps({
        "device_id": "z1-tc1",
        "reading_id": None,
        "label": "drift",
        "detected_at": "2026-11-08T18:00:00Z",
    }).encode("utf-8")

    success = collector.process_message_bytes(payload)
    assert success is True
    assert collector.written == 1
    assert collector.by_source["device_latest"] == 1

    collector.close()

    out_file = tmp_path / "verdicts-guardian.jsonl"
    lines = out_file.read_text(encoding="utf-8").strip().splitlines()
    record = json.loads(lines[0])

    assert record["sim_ts"] == "2026-10-17T12:00:11Z"
    assert record["details"]["sim_ts_source"] == "device_latest"


def test_unknown_device_and_reading_unresolved(temp_ledger, tmp_path):
    """Unknown device and reading cannot resolve sim_ts and are counted as unresolved."""
    config = {
        "topic": "verdicts.topic",
        "layer": "guardian",
        "log_dir": str(tmp_path),
        "db_path": str(temp_ledger),
    }
    collector = VerdictCollector(config)

    payload = json.dumps({
        "device_id": "unknown-device-99",
        "reading_id": "r-unknown-99",
        "label": "error",
    }).encode("utf-8")

    success = collector.process_message_bytes(payload)
    assert success is False
    assert collector.written == 0
    assert collector.unresolved == 1

    collector.close()

    out_file = tmp_path / "verdicts-guardian.jsonl"
    assert not out_file.exists() or len(out_file.read_text().strip()) == 0


def test_collector_output_loads_into_timeline(temp_ledger, tmp_path):
    """A written verdict line is loaded by load_partner_log_entries and mapped in build_timeline_rows."""
    config = {
        "topic": "verdicts.topic",
        "layer": "guardian",
        "log_dir": str(tmp_path),
        "db_path": str(temp_ledger),
    }
    collector = VerdictCollector(config)

    # 1. reading_id source
    collector.process_message_bytes(json.dumps({
        "device_id": "z1-tc1",
        "reading_id": "r-z1-tc1-100",
        "label": "tamper_detected",
        "reason": "signature mismatch",
    }).encode("utf-8"))

    # 2. device_latest source
    collector.process_message_bytes(json.dumps({
        "device_id": "z2-wl1",
        "label": "flood_alert",
        "reason": "water level surge",
    }).encode("utf-8"))

    collector.close()

    entries = load_partner_log_entries(str(tmp_path))
    assert len(entries) == 2

    rows = build_timeline_rows([], log_entries=entries)
    assert len(rows) == 2

    r0 = rows[0]
    assert r0.lane == "Guardian"
    assert "tamper_detected" in r0.summary
    assert "(sim time from reading_id)" in r0.summary

    r1 = rows[1]
    assert r1.lane == "Guardian"
    assert "flood_alert" in r1.summary
    assert "(sim time approximate)" in r1.summary


def test_ledger_stat_unchanged_read_only(temp_ledger, tmp_path):
    """The ground-truth SQLite database file is accessed in mode=ro and its stat is unchanged."""
    before_stat = os.stat(temp_ledger)
    before_size = before_stat.st_size
    before_mtime = before_stat.st_mtime_ns

    config = {
        "topic": "verdicts.topic",
        "layer": "guardian",
        "log_dir": str(tmp_path),
        "db_path": str(temp_ledger),
    }
    collector = VerdictCollector(config)

    collector.process_message_bytes(json.dumps({
        "device_id": "z1-tc1",
        "reading_id": "r-z1-tc1-100",
        "label": "check",
    }).encode("utf-8"))
    collector.close()

    after_stat = os.stat(temp_ledger)
    assert after_stat.st_size == before_size
    assert after_stat.st_mtime_ns == before_mtime


def test_db_error_counted_as_unresolved_and_db_errors(tmp_path, capsys):
    """sqlite3.Error during sim_ts lookup increments unresolved and db_errors counters without raising."""
    class FakeCursor:
        def execute(self, *args, **kwargs):
            raise sqlite3.OperationalError("disk I/O error or table locked")

    class FakeConn:
        def cursor(self):
            return FakeCursor()

    config = {
        "topic": "verdicts.topic",
        "layer": "guardian",
        "log_dir": str(tmp_path),
        "db_path": "/path/does/not/matter",
    }
    collector = VerdictCollector(config, db_conn=FakeConn())
    payload = json.dumps({"device_id": "z1-tc1", "reading_id": "r-100"}).encode("utf-8")

    success = collector.process_message_bytes(payload)
    assert success is False
    assert collector.unresolved == 1
    assert collector.db_errors == 1
    assert collector.written == 0

    captured = capsys.readouterr()
    assert "Database error resolving sim_ts: OperationalError" in captured.err
