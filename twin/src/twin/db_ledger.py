"""Ground Truth SQLite Database Ledger for Twin Layer.

This module is strictly isolated to Agent_Physics and verification tooling.
Neither City Brain (Layer 2) nor Guardian (Layer 3) MCP endpoints are permitted
direct access to this ground-truth database.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("twin.db_ledger")


def parse_sim_time(ts_str: str) -> datetime:
    """Parse simulation timestamp supporting both RFC 3339 formats."""
    s = ts_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


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
    run_id: Optional[str] = None
    wire_sig: Optional[str] = None
    wire_bytes: Optional[int] = None
    link_mode: Optional[str] = None


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
    run_id: Optional[str] = None


class GroundTruthLedger:
    """Manages the isolated SQLite database tracking true physical states, attacks, and runs."""

    SCHEMA_SQL = """
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        started_wall_clock TEXT NOT NULL,
        ended_wall_clock TEXT,
        started_sim_ts TEXT NOT NULL,
        ended_sim_ts TEXT,
        seed INTEGER NOT NULL
    );

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
        created_at_wall_clock TEXT NOT NULL,
        run_id TEXT,
        wire_sig TEXT,
        wire_bytes INTEGER,
        link_mode TEXT
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
        description TEXT,
        run_id TEXT
    );

    CREATE TABLE IF NOT EXISTS commands_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT,
        sim_ts TEXT NOT NULL,
        caller TEXT NOT NULL,
        tool TEXT NOT NULL,
        args_json TEXT NOT NULL,
        executed INTEGER NOT NULL,
        refused_code TEXT,
        result_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS containment_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT,
        sim_ts TEXT NOT NULL,
        caller TEXT NOT NULL,
        tool TEXT NOT NULL,
        target TEXT NOT NULL,
        from_status TEXT NOT NULL,
        to_status TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS physical_events (
        event_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        type TEXT NOT NULL,
        zone_id TEXT NOT NULL,
        device_id TEXT,
        params_json TEXT NOT NULL,
        start_sim_ts TEXT NOT NULL,
        resolved_sim_ts TEXT,
        resolved_by TEXT,
        escalated_sim_ts TEXT,
        escalated_to TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_readings_device_ts ON readings_truth (device_id, ts);
    CREATE INDEX IF NOT EXISTS idx_readings_seq ON readings_truth (device_id, seq);
    CREATE INDEX IF NOT EXISTS idx_readings_attacked ON readings_truth (is_attacked);
    CREATE INDEX IF NOT EXISTS idx_attacks_capability ON attacks_injected (capability, active);
    CREATE INDEX IF NOT EXISTS idx_commands_sim_ts ON commands_log (sim_ts);
    CREATE INDEX IF NOT EXISTS idx_containment_target ON containment_log (target);
    CREATE INDEX IF NOT EXISTS idx_physical_events_zone ON physical_events (zone_id, resolved_sim_ts);
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.getenv("GROUND_TRUTH_DB_PATH", "ground_truth.db")

        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)) or ".", exist_ok=True)

        self._writer_lock = threading.Lock()
        # 4.a: Persistent writer connection configured once
        self._writer_conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
        self._writer_conn.row_factory = sqlite3.Row
        self._writer_conn.execute("PRAGMA journal_mode = WAL;")
        self._writer_conn.execute("PRAGMA synchronous = NORMAL;")
        self._writer_conn.execute("PRAGMA foreign_keys = ON;")
        self._writer_conn.execute("PRAGMA busy_timeout = 10000;")

        self._init_and_migrate_db()

        # 6. RESET_LEDGER handling: wipe readings_truth, attacks_injected, runs, commands_log, containment_log, KEEP devices intact
        if os.getenv("RESET_LEDGER", "false").lower() in ("true", "1", "yes"):
            with self._writer_lock:
                self._writer_conn.execute("DELETE FROM readings_truth;")
                self._writer_conn.execute("DELETE FROM attacks_injected;")
                self._writer_conn.execute("DELETE FROM runs;")
                self._writer_conn.execute("DELETE FROM commands_log;")
                self._writer_conn.execute("DELETE FROM containment_log;")
                self._writer_conn.execute("DELETE FROM physical_events;")
                self._writer_conn.commit()

    def _get_connection(self, read_only: bool = False) -> sqlite3.Connection:
        """Provide a connection for read operations."""
        if read_only and os.path.exists(self.db_path):
            abs_path = os.path.abspath(self.db_path)
            conn = sqlite3.connect(f"file:{abs_path}?mode=ro", uri=True, timeout=10.0, check_same_thread=False)
        else:
            conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA busy_timeout = 10000;")
        return conn

    def _init_and_migrate_db(self) -> None:
        """Initialize database tables, indexes, and apply backward-compatible schema migrations."""
        self._writer_conn.executescript(self.SCHEMA_SQL)

        # Migration: ensure runs table exists
        self._writer_conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            started_wall_clock TEXT NOT NULL,
            ended_wall_clock TEXT,
            started_sim_ts TEXT NOT NULL,
            ended_sim_ts TEXT,
            seed INTEGER NOT NULL
        );
        """)

        # Migration: ensure readings_truth columns exist
        cur = self._writer_conn.execute("PRAGMA table_info(readings_truth);")
        cols = {row["name"] for row in cur.fetchall()}
        if "run_id" not in cols:
            self._writer_conn.execute("ALTER TABLE readings_truth ADD COLUMN run_id TEXT;")
        if "wire_sig" not in cols:
            self._writer_conn.execute("ALTER TABLE readings_truth ADD COLUMN wire_sig TEXT;")
        if "wire_bytes" not in cols:
            self._writer_conn.execute("ALTER TABLE readings_truth ADD COLUMN wire_bytes INTEGER;")
        if "link_mode" not in cols:
            self._writer_conn.execute("ALTER TABLE readings_truth ADD COLUMN link_mode TEXT;")

        # Migration: ensure attacks_injected columns exist
        cur = self._writer_conn.execute("PRAGMA table_info(attacks_injected);")
        cols_atk = {row["name"] for row in cur.fetchall()}
        if "run_id" not in cols_atk:
            self._writer_conn.execute("ALTER TABLE attacks_injected ADD COLUMN run_id TEXT;")

        # Migration: ensure commands_log and containment_log tables exist
        self._writer_conn.execute("""
        CREATE TABLE IF NOT EXISTS commands_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            sim_ts TEXT NOT NULL,
            caller TEXT NOT NULL,
            tool TEXT NOT NULL,
            args_json TEXT NOT NULL,
            executed INTEGER NOT NULL,
            refused_code TEXT,
            result_json TEXT NOT NULL
        );
        """)
        self._writer_conn.execute("""
        CREATE TABLE IF NOT EXISTS containment_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            sim_ts TEXT NOT NULL,
            caller TEXT NOT NULL,
            tool TEXT NOT NULL,
            target TEXT NOT NULL,
            from_status TEXT NOT NULL,
            to_status TEXT NOT NULL
        );
        """)

        # Migration: ensure physical_events table exists
        self._writer_conn.execute("""
        CREATE TABLE IF NOT EXISTS physical_events (
            event_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            type TEXT NOT NULL,
            zone_id TEXT NOT NULL,
            device_id TEXT,
            params_json TEXT NOT NULL,
            start_sim_ts TEXT NOT NULL,
            resolved_sim_ts TEXT,
            resolved_by TEXT,
            escalated_sim_ts TEXT,
            escalated_to TEXT
        );
        """)
        try:
            self._writer_conn.execute("ALTER TABLE physical_events ADD COLUMN escalated_sim_ts TEXT;")
        except Exception:
            pass
        try:
            self._writer_conn.execute("ALTER TABLE physical_events ADD COLUMN escalated_to TEXT;")
        except Exception:
            pass

        # Migration: ensure secret_rotations table exists
        self._writer_conn.execute("""
        CREATE TABLE IF NOT EXISTS secret_rotations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sim_ts TEXT NOT NULL,
            wall_ts TEXT NOT NULL,
            results_json TEXT NOT NULL
        );
        """)
        self._writer_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_secret_rotations_sim_ts ON secret_rotations(sim_ts);"
        )

        self._writer_conn.commit()

    # Run lifecycle tracking
    def start_run(self, run_id: str, seed: int, started_sim_ts: str) -> None:
        """Record run initiation in runs table."""
        wall_now = datetime.now(timezone.utc).isoformat()
        with self._writer_lock:
            self._writer_conn.execute(
                """
                INSERT INTO runs (run_id, started_wall_clock, started_sim_ts, seed)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    started_wall_clock=excluded.started_wall_clock,
                    started_sim_ts=excluded.started_sim_ts,
                    seed=excluded.seed;
                """,
                (run_id, wall_now, started_sim_ts, seed),
            )
            self._writer_conn.commit()

    def end_run(self, run_id: str, ended_sim_ts: str) -> None:
        """Record run termination in runs table."""
        wall_now = datetime.now(timezone.utc).isoformat()
        with self._writer_lock:
            self._writer_conn.execute(
                "UPDATE runs SET ended_wall_clock = ?, ended_sim_ts = ? WHERE run_id = ?",
                (wall_now, ended_sim_ts, run_id),
            )
            self._writer_conn.commit()

    # Device registration
    def register_device(self, device: DeviceRecord) -> None:
        """Register a device in the ledger."""
        created_at = device.created_at or datetime.now(timezone.utc).isoformat()
        with self._writer_lock:
            self._writer_conn.execute(
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
            self._writer_conn.commit()

    def register_devices_batch(self, devices: List[DeviceRecord]) -> None:
        """Batch insert or update devices metadata."""
        params = [
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
                dev.created_at or datetime.now(timezone.utc).isoformat(),
            )
            for dev in devices
        ]
        with self._writer_lock:
            self._writer_conn.executemany(
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
                params,
            )
            self._writer_conn.commit()

    def get_device(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single device record by ID."""
        with self._get_connection(read_only=True) as conn:
            cur = conn.execute("SELECT * FROM devices WHERE device_id = ?", (device_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def list_devices(self, zone_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List registered devices, optionally filtered by zone_id."""
        with self._get_connection(read_only=True) as conn:
            if zone_id:
                cur = conn.execute("SELECT * FROM devices WHERE zone_id = ?", (zone_id,))
            else:
                cur = conn.execute("SELECT * FROM devices ORDER BY device_id ASC")
            return [dict(row) for row in cur.fetchall()]

    # Telemetry and wire truth recording
    def record_reading_truth(self, record: TruthReadingRecord) -> None:
        """Record a single ground truth reading via plain INSERT."""
        wall_clock = record.created_at_wall_clock or datetime.now(timezone.utc).isoformat()
        with self._writer_lock:
            self._writer_conn.execute(
                """
                INSERT INTO readings_truth (
                    reading_id, device_id, ts, seq, true_value, broadcast_value,
                    unit, status, sig, attack_type, is_attacked, created_at_wall_clock,
                    run_id, wire_sig, wire_bytes, link_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    record.run_id,
                    record.wire_sig,
                    record.wire_bytes,
                    record.link_mode,
                ),
            )
            self._writer_conn.commit()

    def record_readings_truth_batch(self, records: List[TruthReadingRecord]) -> None:
        """Record batch of ground truth readings via plain INSERT (raises sqlite3.IntegrityError on duplicate)."""
        params = [
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
                rec.created_at_wall_clock or datetime.now(timezone.utc).isoformat(),
                rec.run_id,
                rec.wire_sig,
                rec.wire_bytes,
                rec.link_mode,
            )
            for rec in records
        ]
        with self._writer_lock:
            try:
                self._writer_conn.executemany(
                    """
                    INSERT INTO readings_truth (
                        reading_id, device_id, ts, seq, true_value, broadcast_value,
                        unit, status, sig, attack_type, is_attacked, created_at_wall_clock,
                        run_id, wire_sig, wire_bytes, link_mode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    params,
                )
                self._writer_conn.commit()
            except sqlite3.IntegrityError:
                self._writer_conn.rollback()
                raise

    def record_wire_batch(self, wire_records: List[Tuple[str, str, int]]) -> None:
        """Update wire_sig and wire_bytes for emitted readings in a single executemany call."""
        params = [(sig, size, rid) for rid, sig, size in wire_records]
        with self._writer_lock:
            self._writer_conn.executemany(
                "UPDATE readings_truth SET wire_sig = ?, wire_bytes = ? WHERE reading_id = ?",
                params,
            )
            self._writer_conn.commit()

    # Adversary logging
    def record_attack(self, attack: InjectedAttackRecord) -> None:
        """Record an injected adversary capability."""
        with self._writer_lock:
            self._writer_conn.execute(
                """
                INSERT INTO attacks_injected (
                    attack_id, capability, target_device_id, target_zone_id,
                    params_json, start_sim_time, end_sim_time, active, description, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(attack_id) DO UPDATE SET
                    end_sim_time=excluded.end_sim_time,
                    active=excluded.active,
                    run_id=COALESCE(excluded.run_id, attacks_injected.run_id);
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
                    attack.run_id,
                ),
            )
            self._writer_conn.commit()

    def deactivate_attack(self, attack_id: str, end_sim_time: Optional[str] = None) -> bool:
        """Mark a specific attack as inactive in the attacks_injected table."""
        with self._writer_lock:
            cur = self._writer_conn.execute(
                "UPDATE attacks_injected SET active = 0, end_sim_time = COALESCE(?, end_sim_time) WHERE attack_id = ?",
                (end_sim_time, attack_id),
            )
            self._writer_conn.commit()
            return cur.rowcount > 0

    def deactivate_all_attacks(self, end_sim_time: Optional[str] = None) -> int:
        """Mark all active attacks in attacks_injected table as inactive."""
        with self._writer_lock:
            cur = self._writer_conn.execute(
                "UPDATE attacks_injected SET active = 0, end_sim_time = COALESCE(?, end_sim_time) WHERE active = 1",
                (end_sim_time,),
            )
            self._writer_conn.commit()
            return cur.rowcount

    # Journals (truth side; MCP never reads them)
    def log_command(
        self,
        run_id: Optional[str],
        sim_ts: str,
        caller: str,
        tool: str,
        args: Dict[str, Any],
        executed: bool,
        refused_code: Optional[str],
        result: Dict[str, Any],
    ) -> None:
        """Record an actuation or tool command in commands_log."""
        with self._writer_lock:
            self._writer_conn.execute(
                """
                INSERT INTO commands_log (
                    run_id, sim_ts, caller, tool, args_json, executed, refused_code, result_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    sim_ts,
                    caller,
                    tool,
                    json.dumps(args),
                    1 if executed else 0,
                    refused_code,
                    json.dumps(result),
                ),
            )
            self._writer_conn.commit()

    def log_containment(
        self,
        run_id: Optional[str],
        sim_ts: str,
        caller: str,
        tool: str,
        target: str,
        from_status: str,
        to_status: str,
    ) -> None:
        """Record a containment action in containment_log."""
        with self._writer_lock:
            self._writer_conn.execute(
                """
                INSERT INTO containment_log (
                    run_id, sim_ts, caller, tool, target, from_status, to_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, sim_ts, caller, tool, target, from_status, to_status),
            )
            self._writer_conn.commit()

    def get_recent_commands_log(self, limit: int = 30) -> List[Dict[str, Any]]:
        """Fetch recent entries from commands_log for verification."""
        with self._get_connection(read_only=True) as conn:
            cur = conn.execute("SELECT * FROM commands_log ORDER BY id DESC LIMIT ?", (limit,))
            return [dict(row) for row in cur.fetchall()]

    def get_recent_containment_log(self, limit: int = 30) -> List[Dict[str, Any]]:
        """Fetch recent entries from containment_log for verification."""
        with self._get_connection(read_only=True) as conn:
            cur = conn.execute("SELECT * FROM containment_log ORDER BY id DESC LIMIT ?", (limit,))
            return [dict(row) for row in cur.fetchall()]

    def log_secret_rotation(
        self, sim_ts: str, results: Dict[str, str], wall_ts: Optional[str] = None
    ) -> None:
        """Record a secret rotation event in secret_rotations table."""
        if wall_ts is None:
            wall_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        with self._writer_lock:
            self._writer_conn.execute(
                """
                INSERT INTO secret_rotations (
                    sim_ts, wall_ts, results_json
                ) VALUES (?, ?, ?)
                """,
                (sim_ts, wall_ts, json.dumps(results, separators=(",", ":"))),
            )
            self._writer_conn.commit()

    # Physical events tracking (Phase 6)
    def record_physical_event(
        self,
        event_id: str,
        run_id: str,
        event_type: str,
        zone_id: str,
        device_id: Optional[str],
        params_json: str,
        start_sim_ts: str,
    ) -> None:
        """Record initiation of a physical incident event."""
        with self._writer_lock:
            self._writer_conn.execute(
                """
                INSERT INTO physical_events (
                    event_id, run_id, type, zone_id, device_id, params_json, start_sim_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    run_id=excluded.run_id,
                    type=excluded.type,
                    zone_id=excluded.zone_id,
                    device_id=excluded.device_id,
                    params_json=excluded.params_json,
                    start_sim_ts=excluded.start_sim_ts;
                """,
                (event_id, run_id, event_type, zone_id, device_id, params_json, start_sim_ts),
            )
            self._writer_conn.commit()

    def resolve_physical_event(
        self,
        event_id: str,
        resolved_sim_ts: str,
        resolved_by: str,
    ) -> bool:
        """Mark a physical incident event as resolved or expired."""
        with self._writer_lock:
            cur = self._writer_conn.execute(
                """
                UPDATE physical_events
                SET resolved_sim_ts = ?, resolved_by = ?
                WHERE event_id = ?
                """,
                (resolved_sim_ts, resolved_by, event_id),
            )
            self._writer_conn.commit()
            return cur.rowcount > 0

    def escalate_physical_event(
        self,
        event_id: str,
        escalated_sim_ts: str,
        escalated_to: str,
    ) -> bool:
        """Mark a physical incident event as escalated (e.g. grid_overload -> blackout)."""
        with self._writer_lock:
            cur = self._writer_conn.execute(
                """
                UPDATE physical_events
                SET escalated_sim_ts = ?, escalated_to = ?
                WHERE event_id = ?
                """,
                (escalated_sim_ts, escalated_to, event_id),
            )
            self._writer_conn.commit()
            return cur.rowcount > 0

    def get_recent_physical_events(self, limit: int = 30) -> List[Dict[str, Any]]:
        """Fetch recent entries from physical_events for verification."""
        with self._get_connection(read_only=True) as conn:
            cur = conn.execute("SELECT * FROM physical_events ORDER BY rowid DESC LIMIT ?", (limit,))
            return [dict(row) for row in cur.fetchall()]

    # State inspection and recovery
    def get_max_sequences_per_device(self) -> Dict[str, int]:
        """Fetch maximum sequence per device (including non-registered rogue devices)."""
        with self._get_connection(read_only=True) as conn:
            cur = conn.execute(
                "SELECT device_id, MAX(seq) AS max_seq FROM readings_truth GROUP BY device_id;"
            )
            return {row["device_id"]: int(row["max_seq"]) for row in cur.fetchall()}

    def get_latest_sim_time(self) -> Optional[str]:
        """Fetch latest simulation timestamp in readings_truth."""
        with self._get_connection(read_only=True) as conn:
            cur1 = conn.execute("SELECT ts FROM readings_truth ORDER BY id DESC LIMIT 100;")
            cur2 = conn.execute("SELECT ts FROM readings_truth ORDER BY ts DESC LIMIT 100;")
            candidates = set(r["ts"] for r in cur1.fetchall()) | set(r["ts"] for r in cur2.fetchall())
            if not candidates:
                return None
            best_dt = None
            best_ts = None
            for r in candidates:
                try:
                    dt = parse_sim_time(r)
                    if best_dt is None or dt > best_dt:
                        best_dt = dt
                        best_ts = r
                except Exception:
                    continue
            return best_ts

    def get_reading_truth(self, reading_id: str) -> Optional[Dict[str, Any]]:
        """Fetch truth reading by reading_id."""
        with self._get_connection(read_only=True) as conn:
            cur = conn.execute("SELECT * FROM readings_truth WHERE reading_id = ?", (reading_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_readings_truth(
        self, device_id: Optional[str] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Fetch recent truth readings."""
        with self._get_connection(read_only=True) as conn:
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
                       unit, attack_type, is_attacked, status, run_id, wire_sig, wire_bytes
                FROM readings_truth
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]

    def close(self) -> None:
        """Close persistent writer connection."""
        if hasattr(self, "_writer_conn") and self._writer_conn:
            try:
                with self._writer_lock:
                    self._writer_conn.close()
            except Exception:
                pass
