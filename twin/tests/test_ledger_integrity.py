"""Tests for Phase 4: Ledger Integrity, Provenance, Migration, and Keystore.

Verifies:
1. Continuity across restarts (keys, sequence monotonic increment, sim clock resumption, runs table).
2. Deduplication & strict provenance (IntegrityError on duplicate, [CRITICAL THREAT] logging, run_id).
3. Keystore security (mode 600, persistence across restarts, non-derivation from public seed).
4. Migration safety against ground_truth.db copy (preserves rows, adds runs/run_id/wire cols, resumes seq/clock).
5. Wire reconciliation (wire_sig and wire_bytes recorded for wire messages, NULL for dropped link attack).
6. RESET_LEDGER=true wipes readings/runs/attacks while keeping devices and keystore intact.
"""

from __future__ import annotations

import logging
import os
import shutil
import sqlite3
from datetime import datetime, timezone

import pytest

from twin.db_ledger import (
    DeviceRecord,
    GroundTruthLedger,
    TruthReadingRecord,
    parse_sim_time,
)
from twin.kafka_streamer import KafkaStreamer
from twin.keystore import DeviceKeystore
from twin.simulation import CitySimulator
from twin.wire_limits import SEQ_SAFE_MAX


def test_continuity_and_runs_across_restarts(tmp_path):
    """Test 7.a: Two consecutive simulator runs on the same DB.
    
    Verifies keys are identical, seq continues from last + 1, sim clock continues
    after previous run, and runs table records both runs with wall & sim timestamps.
    """
    db_path = str(tmp_path / "continuity.db")
    keystore_path = str(tmp_path / "device_keys.json")

    # Run 1: 50 ticks
    sim1 = CitySimulator(seed=42, ledger=GroundTruthLedger(db_path), keystore_path=keystore_path)
    run1_id = sim1.run_id
    run1_start_sim = sim1.get_sim_time()

    for _ in range(50):
        sim1.tick()

    run1_end_sim = sim1.get_sim_time()
    keys1 = {d: sim1.signing_key_for(d) for d in sim1.devices}
    pubkeys1 = {d: sim1.devices[d].public_key for d in sim1.devices}
    seqs1 = {d: sim1.devices[d].seq for d in sim1.devices}
    assert all(s == 50 for s in seqs1.values())

    sim1.close()

    # Run 2: starts on same DB and keystore
    sim2 = CitySimulator(seed=42, ledger=GroundTruthLedger(db_path), keystore_path=keystore_path)
    run2_id = sim2.run_id
    assert run2_id != run1_id

    # Verify keys are identical across restarts
    keys2 = {d: sim2.signing_key_for(d) for d in sim2.devices}
    pubkeys2 = {d: sim2.devices[d].public_key for d in sim2.devices}
    assert keys1 == keys2
    assert pubkeys1 == pubkeys2

    # Verify seq resumed from 50
    seqs2 = {d: sim2.devices[d].seq for d in sim2.devices}
    assert seqs1 == seqs2

    # Verify sim clock resumed after run 1's last reading
    last_reading_ts1 = GroundTruthLedger(db_path).get_latest_sim_time()
    run2_start_sim = sim2.get_sim_time()
    assert parse_sim_time(run2_start_sim) > parse_sim_time(last_reading_ts1)

    # First tick of run 2 should increment seq to 51 and timestamp after last reading of run 1
    r2_readings = sim2.tick()
    assert len(r2_readings) == 42
    for r in r2_readings:
        assert r["seq"] == 51
        assert parse_sim_time(r["ts"]) > parse_sim_time(last_reading_ts1)

    sim2.close()

    # Verify runs table in DB
    ledger = GroundTruthLedger(db_path)
    with ledger._get_connection(read_only=True) as conn:
        cur = conn.execute("SELECT * FROM runs ORDER BY started_wall_clock ASC;")
        runs = [dict(r) for r in cur.fetchall()]
        assert len(runs) == 2

        assert runs[0]["run_id"] == run1_id
        assert runs[0]["seed"] == 42
        assert runs[0]["started_wall_clock"] is not None
        assert runs[0]["ended_wall_clock"] is not None
        assert runs[0]["started_sim_ts"] == run1_start_sim
        assert runs[0]["ended_sim_ts"] is not None

        assert runs[1]["run_id"] == run2_id
        assert runs[1]["seed"] == 42
        assert runs[1]["started_wall_clock"] is not None
        assert runs[1]["ended_wall_clock"] is not None
        assert runs[1]["started_sim_ts"] == run2_start_sim

        # Total readings count == (50 * 42) + (1 * 42) = 2142
        cur = conn.execute("SELECT COUNT(*) AS total FROM readings_truth;")
        assert cur.fetchone()["total"] == 51 * 42
    ledger.close()


def test_duplicate_reading_id_integrity_error_and_tick_recovery(tmp_path, caplog):
    """Test 7.b: Duplicate reading_id raises IntegrityError on plain INSERT,
    and simulation tick loop catches it, logs [CRITICAL THREAT], and continues.
    """
    db_path = str(tmp_path / "dedup.db")
    ledger = GroundTruthLedger(db_path)

    # 1. Direct duplicate insert raises IntegrityError (DROP ON CONFLICT DO UPDATE)
    rec1 = TruthReadingRecord(
        reading_id="r-z1-tc1-1",
        device_id="z1-tc1",
        ts="2026-10-17T12:00:00Z",
        seq=1,
        true_value=45.0,
        broadcast_value=45.0,
        unit="veh/min",
        status="ok",
    )
    ledger.record_reading_truth(rec1)

    with pytest.raises(sqlite3.IntegrityError):
        ledger.record_reading_truth(rec1)

    # 2. In simulation tick loop, duplicate insert is caught and logged
    sim = CitySimulator(seed=42, ledger=ledger, keystore_path=str(tmp_path / "keys.json"))
    
    # Tick 1 succeeds normally
    sim.tick()

    # Artificially reset seq of all devices to collide with existing reading_ids
    for dev in sim.devices.values():
        dev.seq = 0

    with caplog.at_level(logging.ERROR):
        # Tick 2 should catch duplicate insert, log [CRITICAL THREAT], and continue
        readings = sim.tick()
        assert len(readings) == 42
        assert any(
            "[CRITICAL THREAT] Duplicate reading_id detected in ground truth ledger" in rec.message
            for rec in caplog.records
        )

    sim.close()


def test_keystore_permissions_persistence_and_no_seed_derivation(tmp_path):
    """Test 7.c: Keystore permissions (600), key persistence, key generation for new devices,
    and verifies keys are not derived from SIM_SEED.
    """
    keystore_path = str(tmp_path / "device_keys.json")
    ks = DeviceKeystore(keystore_path=keystore_path)

    # Key generation
    priv1, pub1 = ks.get_or_create_keypair("z1-tc1")
    assert priv1 and pub1

    # Verify file mode 600 (-rw-------)
    stat = os.stat(keystore_path)
    file_mode = stat.st_mode & 0o777
    assert file_mode == 0o600, f"Expected 0o600, got {oct(file_mode)}"

    # Key persistence across new keystore instance
    ks2 = DeviceKeystore(keystore_path=keystore_path)
    priv1_loaded, pub1_loaded = ks2.get_or_create_keypair("z1-tc1")
    assert priv1 == priv1_loaded
    assert pub1 == pub1_loaded

    # Generate key for new device
    priv_new, pub_new = ks2.get_or_create_keypair("z9-new")
    assert priv_new != priv1
    assert "z9-new" in ks2.keys

    # Verify keys are NOT derived from seed: two simulators with identical seeds have different keys
    sim_a = CitySimulator(seed=42, keystore_path=str(tmp_path / "ks_a.json"), ledger=GroundTruthLedger(str(tmp_path / "a.db")))
    sim_b = CitySimulator(seed=42, keystore_path=str(tmp_path / "ks_b.json"), ledger=GroundTruthLedger(str(tmp_path / "b.db")))

    for dev_id in sim_a.devices:
        assert sim_a.devices[dev_id].public_key != sim_b.devices[dev_id].public_key
        assert sim_a.devices[dev_id].private_key != sim_b.devices[dev_id].private_key

    sim_a.close()
    sim_b.close()


def test_migration_against_existing_ground_truth_copy(tmp_path):
    """Test 7.d: Migration against a read-only COPY of ./ground_truth.db.
    
    Verifies old rows preserved, runs/wire columns added, max seq resumed at 14,195,
    and sim clock resumed after latest timestamp.
    """
    root_db = os.path.abspath("ground_truth.db")
    if not os.path.isfile(root_db):
        root_db = os.path.abspath("../ground_truth.db")
    if not os.path.isfile(root_db):
        pytest.skip("ground_truth.db not present at root")

    copy_path = str(tmp_path / "ground_truth_copy.db")
    shutil.copyfile(root_db, copy_path)

    # Initial read-only count on copy before opening through GroundTruthLedger
    ro_conn = sqlite3.connect(f"file:{copy_path}?mode=ro", uri=True)
    initial_count = ro_conn.execute("SELECT COUNT(*) FROM readings_truth;").fetchone()[0]
    initial_max_seq = ro_conn.execute("SELECT MAX(seq) FROM readings_truth;").fetchone()[0]
    ro_conn.close()

    assert initial_count > 500_000
    assert initial_max_seq == 14194

    # Open with GroundTruthLedger (applies backward-compatible migration)
    ledger = GroundTruthLedger(copy_path)

    # Verify new table and columns exist
    with ledger._get_connection(read_only=True) as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()}
        assert "runs" in tables

        cur = conn.execute("PRAGMA table_info(readings_truth);")
        r_cols = {row["name"] for row in cur.fetchall()}
        assert "run_id" in r_cols
        assert "wire_sig" in r_cols
        assert "wire_bytes" in r_cols

        cur = conn.execute("PRAGMA table_info(attacks_injected);")
        a_cols = {row["name"] for row in cur.fetchall()}
        assert "run_id" in a_cols

        # Old rows preserved
        new_count = conn.execute("SELECT COUNT(*) FROM readings_truth;").fetchone()[0]
        assert new_count == initial_count

    # Resumption test
    sim = CitySimulator(
        seed=42,
        ledger=ledger,
        keystore_path=str(tmp_path / "mig_keys.json"),
    )

    # Check sequence resumption: devices should start at 14,194, next tick seq is 14,195
    for dev in sim.devices.values():
        assert dev.seq == 14194

    readings = sim.tick()
    for r in readings:
        assert r["seq"] == 14195

    # Check clock resumption: should resume after "2026-10-17T12:23:39Z"
    assert parse_sim_time(sim.get_sim_time()) >= parse_sim_time("2026-10-17T12:23:40Z")

    sim.close()


def test_synthetic_schema_migration_and_dual_timestamp_formats(tmp_path):
    """Test 7.d part 2: Synthetic migration from legacy schema with microsecond timestamps."""
    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""
    CREATE TABLE devices (
        device_id TEXT PRIMARY KEY,
        zone_id TEXT NOT NULL,
        device_type TEXT NOT NULL,
        unit TEXT NOT NULL,
        lat REAL NOT NULL,
        lon REAL NOT NULL,
        public_key TEXT,
        status TEXT NOT NULL DEFAULT 'ok',
        min_val REAL NOT NULL,
        max_val REAL NOT NULL,
        created_at TEXT NOT NULL
    );
    """)
    conn.execute("""
    CREATE TABLE readings_truth (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reading_id TEXT UNIQUE NOT NULL,
        device_id TEXT NOT NULL,
        ts TEXT NOT NULL,
        seq INTEGER NOT NULL,
        true_value REAL NOT NULL,
        broadcast_value REAL NOT NULL,
        unit TEXT NOT NULL,
        status TEXT NOT NULL,
        sig TEXT,
        attack_type TEXT,
        is_attacked INTEGER NOT NULL DEFAULT 0,
        created_at_wall_clock TEXT NOT NULL
    );
    """)
    conn.execute("""
    CREATE TABLE attacks_injected (
        attack_id TEXT PRIMARY KEY,
        capability TEXT NOT NULL,
        target_device_id TEXT,
        target_zone_id TEXT,
        params_json TEXT NOT NULL,
        start_sim_time TEXT NOT NULL,
        end_sim_time TEXT,
        active INTEGER NOT NULL DEFAULT 1,
        description TEXT
    );
    """)
    # Insert legacy rows with both microsecond format and standard format
    conn.execute("""
    INSERT INTO readings_truth (reading_id, device_id, ts, seq, true_value, broadcast_value, unit, status, created_at_wall_clock)
    VALUES ('r-z1-tc1-1', 'z1-tc1', '2026-10-17T12:00:00.000000Z', 1, 45.0, 45.0, 'veh/min', 'ok', '2026-10-17T12:00:00Z');
    """)
    conn.execute("""
    INSERT INTO readings_truth (reading_id, device_id, ts, seq, true_value, broadcast_value, unit, status, created_at_wall_clock)
    VALUES ('r-z1-tc1-2', 'z1-tc1', '2026-10-17T12:00:01Z', 2, 46.0, 46.0, 'veh/min', 'ok', '2026-10-17T12:00:01Z');
    """)
    conn.commit()
    conn.close()

    # Open with GroundTruthLedger
    ledger = GroundTruthLedger(db_path)
    latest_ts = ledger.get_latest_sim_time()
    assert latest_ts == "2026-10-17T12:00:01Z"
    assert parse_sim_time(latest_ts) == datetime(2026, 10, 17, 12, 0, 1, tzinfo=timezone.utc)
    ledger.close()


def test_wire_reconciliation_sig_and_bytes(tmp_path):
    """Test 7.e: wire_sig and wire_bytes recorded for wire messages, NULL for dropped (link attack)."""
    db_path = str(tmp_path / "wire.db")
    ledger = GroundTruthLedger(db_path)
    sim = CitySimulator(seed=42, ledger=ledger, keystore_path=str(tmp_path / "keys.json"))
    streamer = KafkaStreamer(mock_mode=True)

    # Tick 1: Normal tick
    readings = sim.tick()
    wire_updates = []
    for raw in readings:
        dev_id = raw["device_id"]
        key = sim.signing_key_for(dev_id)
        res = streamer.stream_reading(raw, key)
        assert res is not None
        reading, raw_bytes = res
        wire_updates.append((reading.reading_id, reading.sig, len(raw_bytes)))

    sim.ledger.record_wire_batch(wire_updates)

    # Verify normal reading has non-null wire_sig and wire_bytes
    norm_truth = ledger.get_reading_truth("r-z1-tc1-1")
    assert norm_truth is not None
    assert norm_truth["wire_sig"] is not None
    assert norm_truth["wire_bytes"] is not None
    assert 0 < norm_truth["wire_bytes"] <= 300

    # Tick 2: Inject link attack with drop mode on z1-tc1
    sim.inject_attack("link", target_device_id="z1-tc1", params={"link_mode": "drop"})
    readings2 = sim.tick()

    # z1-tc1 should be dropped from broadcast readings
    assert not any(r["device_id"] == "z1-tc1" for r in readings2)

    # Stream remaining readings
    wire_updates2 = []
    for raw in readings2:
        dev_id = raw["device_id"]
        key = sim.signing_key_for(dev_id)
        res = streamer.stream_reading(raw, key)
        if res:
            reading, raw_bytes = res
            wire_updates2.append((reading.reading_id, reading.sig, len(raw_bytes)))

    sim.ledger.record_wire_batch(wire_updates2)

    # Verify z1-tc1 reading exists in ground truth but wire_sig and wire_bytes are NULL
    dropped_truth = ledger.get_reading_truth("r-z1-tc1-2")
    assert dropped_truth is not None
    assert dropped_truth["attack_type"] == "link"
    assert dropped_truth["is_attacked"] == 1
    assert dropped_truth["wire_sig"] is None
    assert dropped_truth["wire_bytes"] is None

    sim.close()


def test_reset_ledger_flag_wipes_readings_keeps_devices_and_keys(tmp_path, monkeypatch):
    """Test 7.f: RESET_LEDGER=true wipes readings/runs/attacks, keeps devices/keys intact."""
    db_path = str(tmp_path / "reset.db")
    keystore_path = str(tmp_path / "reset_keys.json")

    # 1. Populate DB with 10 ticks and an attack
    sim1 = CitySimulator(seed=42, ledger=GroundTruthLedger(db_path), keystore_path=keystore_path)
    for _ in range(10):
        sim1.tick()
    sim1.inject_attack("sensor_fault", target_device_id="z1-tc1", params={"fault_mode": "stuck", "stuck_value": 50.0})
    sim1.tick()

    keys_before = {d: sim1.signing_key_for(d) for d in sim1.devices}
    pubkeys_before = {d: sim1.devices[d].public_key for d in sim1.devices}
    sim1.close()

    # Verify initial non-zero counts
    check_ledger = GroundTruthLedger(db_path)
    with check_ledger._get_connection(read_only=True) as conn:
        assert conn.execute("SELECT COUNT(*) FROM readings_truth;").fetchone()[0] > 0
        assert conn.execute("SELECT COUNT(*) FROM attacks_injected;").fetchone()[0] > 0
        assert conn.execute("SELECT COUNT(*) FROM runs;").fetchone()[0] > 0
        assert conn.execute("SELECT COUNT(*) FROM devices;").fetchone()[0] == 42
    check_ledger.close()

    # 2. Restart with RESET_LEDGER=true
    monkeypatch.setenv("RESET_LEDGER", "true")
    reset_ledger = GroundTruthLedger(db_path)

    with reset_ledger._get_connection(read_only=True) as conn:
        assert conn.execute("SELECT COUNT(*) FROM readings_truth;").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM attacks_injected;").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM runs;").fetchone()[0] == 0
        # Devices table preserved!
        assert conn.execute("SELECT COUNT(*) FROM devices;").fetchone()[0] == 42
    reset_ledger.close()

    # 3. Simulator restart with RESET_LEDGER keeps keystore keys and resets clock to default
    sim2 = CitySimulator(seed=42, ledger=GroundTruthLedger(db_path), keystore_path=keystore_path)
    keys_after = {d: sim2.signing_key_for(d) for d in sim2.devices}
    pubkeys_after = {d: sim2.devices[d].public_key for d in sim2.devices}
    assert keys_before == keys_after
    assert pubkeys_before == pubkeys_after

    # Resumed clock should be default 2026-10-17T12:00:00Z
    assert sim2.get_sim_time() == "2026-10-17T12:00:00Z"
    # Sequences reset to 0
    assert sim2.devices["z1-tc1"].seq == 0

    sim2.close()
