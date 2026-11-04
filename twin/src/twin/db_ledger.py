"""Ground Truth SQLite Database Ledger for Twin Layer.

This module is strictly isolated to Agent_Physics and verification tooling.
Neither City Brain (Layer 2) nor Guardian (Layer 3) MCP endpoints are permitted
direct access to this ground-truth database.
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class DeviceRecord:
    device_id: str
    zone_id: str
    device_type: str
    unit: str
    lat: float
    lon: float
    public_key: Optional[str] = None
    status: str = "ok"
    min_val: float = 0.0
    max_val: float = 100.0
    created_at: Optional[str] = None


@dataclass
class TruthReadingRecord:
    reading_id: str
    device_id: str
    ts: str  # Simulation time in RFC 3339 UTC
    seq: int
    true_value: float
    broadcast_value: float
    unit: str
    status: str
    sig: Optional[str] = None
    attack_type: Optional[str] = None
    is_attacked: bool = False
    created_at_wall_clock: Optional[str] = None


@dataclass
class InjectedAttackRecord:
    attack_id: str
    capability: str  # link, rogue, device_key, device_physical, actuator, sensor_fault
    target_device_id: Optional[str]
    target_zone_id: Optional[str]
    params_json: str
    start_sim_time: str
    end_sim_time: Optional[str] = None
    active: bool = True
    description: Optional[str] = None


class GroundTruthLedger:
    """Manages the isolated SQLite database tracking true physical states and attacks."""

    SCHEMA_SQL = """
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS devices (
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

    CREATE TABLE IF NOT EXISTS readings_truth (
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

    CREATE TABLE IF NOT EXISTS attacks_injected (
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

    CREATE INDEX IF NOT EXISTS idx_readings_device_ts ON readings_truth (device_id, ts);
    CREATE INDEX IF NOT EXISTS idx_readings_seq ON readings_truth (device_id, seq);
    CREATE INDEX IF NOT EXISTS idx_readings_attacked ON readings_truth (is_attacked);
    CREATE INDEX IF NOT EXISTS idx_attacks_capability ON attacks_injected (capability, active);
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.getenv("GROUND_TRUTH_DB_PATH", "ground_truth.db")
        self._init_db()

    def _get_connection(self, read_only: bool = False) -> sqlite3.Connection:
        if read_only and os.path.exists(self.db_path):
            abs_path = os.path.abspath(self.db_path)
            conn = sqlite3.connect(f"file:{abs_path}?mode=ro", uri=True, timeout=10.0)
        else:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA busy_timeout = 10000;")
        return conn

    def _init_db(self) -> None:
        """Initialize database tables and indexes."""
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        with self._get_connection() as conn:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.executescript(self.SCHEMA_SQL)
            conn.commit()

    def register_device(self, device: DeviceRecord) -> None:
        """Register a device in the ledger."""
        created_at = device.created_at or datetime.now(timezone.utc).isoformat()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO devices (
                    device_id, zone_id, device_type, unit, lat, lon,
                    public_key, status, min_val, max_val, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(device_id) DO UPDATE SET
                    zone_id=excluded.zone_id,
                    device_type=excluded.device_type,
                    unit=excluded.unit,
                    lat=excluded.lat,
                    lon=excluded.lon,
                    public_key=excluded.public_key,
                    status=excluded.status,
                    min_val=excluded.min_val,
                    max_val=excluded.max_val;
                """,
                (
                    device.device_id,
                    device.zone_id,
                    device.device_type,
                    device.unit,
                    device.lat,
                    device.lon,
                    device.public_key,
                    device.status,
                    device.min_val,
                    device.max_val,
                    created_at,
                ),
            )
            conn.commit()

    def register_devices_batch(self, devices: List[DeviceRecord]) -> None:
        """Batch insert or update devices."""
        with self._get_connection() as conn:
            for dev in devices:
                created_at = dev.created_at or datetime.now(timezone.utc).isoformat()
                conn.execute(
                    """
                    INSERT INTO devices (
                        device_id, zone_id, device_type, unit, lat, lon,
                        public_key, status, min_val, max_val, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(device_id) DO UPDATE SET
                        zone_id=excluded.zone_id,
                        device_type=excluded.device_type,
                        unit=excluded.unit,
                        lat=excluded.lat,
                        lon=excluded.lon,
                        public_key=excluded.public_key,
                        status=excluded.status,
                        min_val=excluded.min_val,
                        max_val=excluded.max_val;
                    """,
                    (
                        dev.device_id,
                        dev.zone_id,
                        dev.device_type,
                        dev.unit,
                        dev.lat,
                        dev.lon,
                        dev.public_key,
                        dev.status,
                        dev.min_val,
                        dev.max_val,
                        created_at,
                    ),
                )
            conn.commit()

    def get_device(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single device record by ID."""
        with self._get_connection() as conn:
            cur = conn.execute("SELECT * FROM devices WHERE device_id = ?", (device_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def list_devices(self, zone_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List registered devices, optionally filtered by zone_id."""
        with self._get_connection() as conn:
            if zone_id:
                cur = conn.execute("SELECT * FROM devices WHERE zone_id = ?", (zone_id,))
            else:
                cur = conn.execute("SELECT * FROM devices ORDER BY device_id ASC")
            return [dict(row) for row in cur.fetchall()]

    def record_reading_truth(self, record: TruthReadingRecord) -> None:
        """Record the physical truth and broadcast value of a sensor reading."""
        wall_clock = record.created_at_wall_clock or datetime.now(timezone.utc).isoformat()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO readings_truth (
                    reading_id, device_id, ts, seq, true_value, broadcast_value,
                    unit, status, sig, attack_type, is_attacked, created_at_wall_clock
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.reading_id,
                    record.device_id,
                    record.ts,
                    record.seq,
                    record.true_value,
                    record.broadcast_value,
                    record.unit,
                    record.status,
                    record.sig,
                    record.attack_type,
                    1 if record.is_attacked else 0,
                    wall_clock,
                ),
            )
            conn.commit()

    def record_readings_truth_batch(self, records: List[TruthReadingRecord]) -> None:
        """Record batch of ground truth readings."""
        with self._get_connection() as conn:
            for rec in records:
                wall_clock = rec.created_at_wall_clock or datetime.now(timezone.utc).isoformat()
                conn.execute(
                    """
                    INSERT INTO readings_truth (
                        reading_id, device_id, ts, seq, true_value, broadcast_value,
                        unit, status, sig, attack_type, is_attacked, created_at_wall_clock
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(reading_id) DO UPDATE SET
                        true_value=excluded.true_value,
                        broadcast_value=excluded.broadcast_value,
                        status=excluded.status,
                        sig=excluded.sig,
                        attack_type=excluded.attack_type,
                        is_attacked=excluded.is_attacked;
                    """,
                    (
                        rec.reading_id,
                        rec.device_id,
                        rec.ts,
                        rec.seq,
                        rec.true_value,
                        rec.broadcast_value,
                        rec.unit,
                        rec.status,
                        rec.sig,
                        rec.attack_type,
                        1 if rec.is_attacked else 0,
                        wall_clock,
                    ),
                )
            conn.commit()

    def record_attack(self, attack: InjectedAttackRecord) -> None:
        """Record an injected adversary capability."""
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO attacks_injected (
                    attack_id, capability, target_device_id, target_zone_id,
                    params_json, start_sim_time, end_sim_time, active, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(attack_id) DO UPDATE SET
                    end_sim_time=excluded.end_sim_time,
                    active=excluded.active;
                """,
                (
                    attack.attack_id,
                    attack.capability,
                    attack.target_device_id,
                    attack.target_zone_id,
                    attack.params_json,
                    attack.start_sim_time,
                    attack.end_sim_time,
                    1 if attack.active else 0,
                    attack.description,
                ),
            )
            conn.commit()

    def deactivate_attack(self, attack_id: str, end_sim_time: Optional[str] = None) -> bool:
        """Mark a specific attack as inactive in the attacks_injected table."""
        with self._get_connection() as conn:
            cur = conn.execute(
                "UPDATE attacks_injected SET active = 0, end_sim_time = COALESCE(?, end_sim_time) WHERE attack_id = ?",
                (end_sim_time, attack_id),
            )
            conn.commit()
            return cur.rowcount > 0

    def deactivate_all_attacks(self, end_sim_time: Optional[str] = None) -> int:
        """Mark all active attacks in attacks_injected table as inactive."""
        with self._get_connection() as conn:
            cur = conn.execute(
                "UPDATE attacks_injected SET active = 0, end_sim_time = COALESCE(?, end_sim_time) WHERE active = 1",
                (end_sim_time,),
            )
            conn.commit()
            return cur.rowcount

    def get_reading_truth(self, reading_id: str) -> Optional[Dict[str, Any]]:
        """Fetch truth reading by reading_id."""
        with self._get_connection() as conn:
            cur = conn.execute("SELECT * FROM readings_truth WHERE reading_id = ?", (reading_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_readings_truth(
        self, device_id: Optional[str] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Fetch recent truth readings."""
        with self._get_connection() as conn:
            if device_id:
                cur = conn.execute(
                    "SELECT * FROM readings_truth WHERE device_id = ? ORDER BY id DESC LIMIT ?",
                    (device_id, limit),
                )
            else:
                cur = conn.execute(
                    "SELECT * FROM readings_truth ORDER BY id DESC LIMIT ?",
                    (limit,),
                )
            return [dict(row) for row in cur.fetchall()]

    def get_active_attacks(self, read_only: bool = False) -> List[Dict[str, Any]]:
        """Fetch all currently active injected attacks."""
        with self._get_connection(read_only=read_only) as conn:
            cur = conn.execute("SELECT * FROM attacks_injected WHERE active = 1")
            return [dict(row) for row in cur.fetchall()]

    def get_ledger_stats(self, read_only: bool = True) -> Dict[str, Any]:
        """Fetch aggregated scoreboard metrics for the Adversarial Console."""
        with self._get_connection(read_only=read_only) as conn:
            cur = conn.execute("SELECT COUNT(*) AS total FROM readings_truth")
            total_readings = cur.fetchone()["total"]

            cur = conn.execute("SELECT COUNT(*) AS total FROM readings_truth WHERE is_attacked = 1")
            active_incidents = cur.fetchone()["total"]

            cur = conn.execute("SELECT COUNT(*) AS total FROM attacks_injected WHERE active = 1")
            active_attacks = cur.fetchone()["total"]

            cur = conn.execute("SELECT ts FROM readings_truth ORDER BY id DESC LIMIT 1")
            latest_row = cur.fetchone()
            latest_sim_time = latest_row["ts"] if latest_row else "2026-10-17T12:00:00Z"

            return {
                "total_readings": total_readings,
                "active_incidents": active_incidents,
                "active_attacks": active_attacks,
                "latest_sim_time": latest_sim_time,
            }

    def get_recent_timeline(self, limit: int = 30, read_only: bool = True) -> List[Dict[str, Any]]:
        """Fetch recent events timeline from ground truth readings and attacks."""
        with self._get_connection(read_only=read_only) as conn:
            cur = conn.execute(
                """
                SELECT id, reading_id, device_id, ts, true_value, broadcast_value,
                       unit, attack_type, is_attacked, status
                FROM readings_truth
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]

