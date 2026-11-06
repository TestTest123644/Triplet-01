"""Twin operations console (Layer 1).

Fulfills Challenge Rule R3:
Geospatial city map, correlated multi-lane timeline (reading -> Guardian verdict ->
Brain decision -> actuator action), truth vs broadcast telemetry divergence, and red team controls.
Adheres strictly to the humanized style guide: sentence case, no emojis, clean dark theme.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

import altair as alt
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st
from streamlit.runtime.scriptrunner import get_script_run_ctx

try:
    from twin.console.timeline import (
        TimelineRow,
        build_timeline_rows,
        filter_timeline_rows,
        format_reaction_delta,
        load_partner_log_entries,
        parse_rfc3339_to_seconds,
    )
except ImportError:
    from console.timeline import (
        TimelineRow,
        build_timeline_rows,
        filter_timeline_rows,
        format_reaction_delta,
        load_partner_log_entries,
        parse_rfc3339_to_seconds,
    )

logger = logging.getLogger("twin.console")

# Environment and Connection Configuration
DB_PATH = os.getenv("GROUND_TRUTH_DB_PATH", "ground_truth.db")
TWIN_API_URL = os.getenv("TWIN_API_URL", "http://localhost:8100").rstrip("/")
TIMELINE_LOG_DIR = os.getenv("TIMELINE_LOG_DIR", "logs")

NEIGHBORHOODS = [
    {"zone_id": "zone-1", "name": "Downtown Financial", "lat": 36.800, "lon": 10.180},
    {"zone_id": "zone-2", "name": "Port & Coastal Harbor", "lat": 36.815, "lon": 10.305},
    {"zone_id": "zone-3", "name": "North Industrial Park", "lat": 36.855, "lon": 10.190},
    {"zone_id": "zone-4", "name": "West Residential Hills", "lat": 36.810, "lon": 10.115},
    {"zone_id": "zone-5", "name": "Central University & Med", "lat": 36.835, "lon": 10.145},
    {"zone_id": "zone-6", "name": "South Logistics Hub", "lat": 36.755, "lon": 10.220},
]
ZONE_NAME_MAP = {n["zone_id"]: n["name"] for n in NEIGHBORHOODS}


# ==============================================================================
# Database Access Helpers (Strictly Read-Only SQLite)
# ==============================================================================
def get_db_connection() -> sqlite3.Connection:
    """Open read-only SQLite URI connection to the ground truth database."""
    abs_path = os.path.abspath(DB_PATH)
    conn = sqlite3.connect(f"file:{abs_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _get_db_mtime(path: str) -> float:
    """Return mtime of database file and its WAL file for cache keying."""
    try:
        abs_p = os.path.abspath(path)
        if not os.path.exists(abs_p):
            return 0.0
        mt = os.path.getmtime(abs_p)
        wal = abs_p + "-wal"
        if os.path.exists(wal):
            mt = max(mt, os.path.getmtime(wal))
        return mt
    except OSError:
        return 0.0


def get_red_team_token() -> str:
    """Retrieve red-team API token from env or token file."""
    env_token = os.getenv("RED_TEAM_TOKEN")
    if env_token:
        return env_token.strip()

    token_file = os.getenv("RED_TEAM_TOKEN_FILE")
    if not token_file:
        db_dir = os.path.dirname(os.path.abspath(DB_PATH)) or "."
        token_file = os.path.join(db_dir, "red_team.token")

    if os.path.isfile(token_file):
        try:
            with open(token_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    return content
        except OSError:
            pass
    return ""


def get_red_team_headers() -> Dict[str, str]:
    """Build authorization headers for the red-team REST API."""
    token = get_red_team_token()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Red-Team-Token"] = token
    return headers


def check_twin_daemon_online() -> bool:
    """Check connectivity to the Twin red-team REST API."""
    try:
        resp = requests.get(f"{TWIN_API_URL}/api/status", headers=get_red_team_headers(), timeout=1.5)
        return resp.status_code == 200
    except requests.RequestException:
        return False


# ==============================================================================
# Cached Data Fetchers (ttl 1-3 seconds, fast MAX(id) queries)
# ==============================================================================
@st.cache_data(ttl=2)
def _fetch_scoreboard_metrics_cached(db_path: str, mtime: float) -> Dict[str, Any]:
    """Internal cached implementation of scoreboard metrics query."""
    metrics: Dict[str, Any] = {
        "total_readings": 0,
        "attacked_readings": 0,
        "active_incidents": 0,  # Backward compatibility for existing tests
        "active_attacks": 0,
        "contained_devices": 0,
        "latest_sim_time": "2026-10-17T12:00:00Z",
        "sim_speed": 1.0,
        "msgs_per_sec": 0.0,
        "kafka_state": "Offline",
        "mcp_state": "Offline",
        "recent_callers": "None",
    }

    if not os.path.isfile(db_path):
        return metrics

    try:
        abs_path = os.path.abspath(db_path)
        conn = sqlite3.connect(f"file:{abs_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            # 1. Total readings via MAX(id) instead of COUNT(*)
            r_max = conn.execute("SELECT COALESCE(MAX(id), 0) FROM readings_truth").fetchone()
            if r_max:
                metrics["total_readings"] = int(r_max[0])

            # 2. Attacked readings count
            r_atk = conn.execute("SELECT COUNT(*) FROM readings_truth WHERE is_attacked = 1").fetchone()
            if r_atk:
                metrics["attacked_readings"] = int(r_atk[0])
                metrics["active_incidents"] = int(r_atk[0])

            # 3. Active attacks from attacks_injected
            r_act = conn.execute("SELECT COUNT(*) FROM attacks_injected WHERE active = 1").fetchone()
            if r_act:
                metrics["active_attacks"] = int(r_act[0])

            # 4. Devices contained count
            r_cnt = conn.execute(
                "SELECT COUNT(*) FROM devices WHERE status IN ('isolated', 'quarantined')"
            ).fetchone()
            if r_cnt:
                metrics["contained_devices"] = int(r_cnt[0])

            # 5. Latest sim time
            r_time = conn.execute("SELECT ts FROM readings_truth ORDER BY id DESC LIMIT 1").fetchone()
            if r_time and r_time[0]:
                metrics["latest_sim_time"] = str(r_time[0])

            # 6. Recent callers in the last 5 sim-min
            try:
                latest_sec = parse_rfc3339_to_seconds(metrics["latest_sim_time"])
                five_min_ago = latest_sec - 300.0
                cutoff_iso = datetime.fromtimestamp(five_min_ago, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                callers = set()
                cur = conn.execute("SELECT DISTINCT caller FROM commands_log WHERE sim_ts >= ?", (cutoff_iso,))
                for r in cur.fetchall():
                    if r[0]:
                        callers.add(r[0])
                cur = conn.execute("SELECT DISTINCT caller FROM containment_log WHERE sim_ts >= ?", (cutoff_iso,))
                for r in cur.fetchall():
                    if r[0]:
                        callers.add(r[0])
                if callers:
                    metrics["recent_callers"] = ", ".join(sorted(callers))
            except Exception:
                pass
        finally:
            conn.close()
    except sqlite3.Error as e:
        logger.warning(f"Error reading scoreboard metrics from ledger: {e}")

    # Query API status for live sim speed, msgs/s, and active attacks
    try:
        resp = requests.get(f"{TWIN_API_URL}/api/status", headers=get_red_team_headers(), timeout=1.5)
        if resp.status_code == 200:
            data = resp.json()
            if "active_attacks_count" in data:
                metrics["active_attacks"] = data["active_attacks_count"]
            metrics["sim_speed"] = data.get("sim_speed", 1.0)
            if data.get("sim_time"):
                metrics["latest_sim_time"] = data["sim_time"]
            stream = data.get("stream", {})
            metrics["msgs_per_sec"] = stream.get("msgs_per_sec", 0.0)
            metrics["kafka_state"] = "Streaming" if stream.get("produced", 0) > 0 or metrics["msgs_per_sec"] > 0 else "Ready"
            metrics["mcp_state"] = "Listening"
    except requests.RequestException:
        pass

    return metrics


def fetch_scoreboard_metrics() -> Dict[str, Any]:
    """Retrieve KPI metrics and system status (cached with DB_PATH keying)."""
    return _fetch_scoreboard_metrics_cached(DB_PATH, _get_db_mtime(DB_PATH))


@st.cache_data(ttl=2)
def _fetch_active_attacks_cached(db_path: str, mtime: float) -> List[Dict[str, Any]]:
    """Internal cached active attacks query."""
    try:
        resp = requests.get(f"{TWIN_API_URL}/api/attack/active", headers=get_red_team_headers(), timeout=1.5)
        if resp.status_code == 200:
            return resp.json().get("active_attacks", [])
    except requests.RequestException:
        pass

    if not os.path.isfile(db_path):
        return []
    try:
        abs_path = os.path.abspath(db_path)
        conn = sqlite3.connect(f"file:{abs_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                "SELECT attack_id, capability, target_device_id, target_zone_id, params_json, start_sim_time, description "
                "FROM attacks_injected WHERE active = 1 ORDER BY start_sim_time DESC"
            )
            rows = []
            for r in cur.fetchall():
                try:
                    params = json.loads(r["params_json"]) if r["params_json"] else {}
                except Exception:
                    params = {}
                rows.append({
                    "attack_id": r["attack_id"],
                    "capability": r["capability"],
                    "target_device_id": r["target_device_id"],
                    "target_zone_id": r["target_zone_id"],
                    "params": params,
                    "start_sim_time": r["start_sim_time"],
                    "description": r["description"],
                })
            return rows
        finally:
            conn.close()
    except sqlite3.Error:
        return []


def fetch_active_attacks() -> List[Dict[str, Any]]:
    """Retrieve currently active cyber-physical attacks."""
    return _fetch_active_attacks_cached(DB_PATH, _get_db_mtime(DB_PATH))


@st.cache_data(ttl=2)
def _fetch_active_events_cached(db_path: str, mtime: float) -> List[Dict[str, Any]]:
    """Internal cached active physical events query."""
    try:
        resp = requests.get(f"{TWIN_API_URL}/api/event/active", headers=get_red_team_headers(), timeout=1.5)
        if resp.status_code == 200:
            return resp.json().get("active_events", [])
    except requests.RequestException:
        pass

    if not os.path.isfile(db_path):
        return []
    try:
        abs_path = os.path.abspath(db_path)
        conn = sqlite3.connect(f"file:{abs_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                "SELECT event_id, type, zone_id, device_id, params_json, start_sim_ts "
                "FROM physical_events WHERE resolved_sim_ts IS NULL ORDER BY start_sim_ts DESC"
            )
            rows = []
            for r in cur.fetchall():
                try:
                    params = json.loads(r["params_json"]) if r["params_json"] else {}
                except Exception:
                    params = {}
                rows.append({
                    "event_id": r["event_id"],
                    "type": r["type"],
                    "zone_id": r["zone_id"],
                    "device_id": r["device_id"],
                    "params": params,
                    "start_sim_ts": r["start_sim_ts"],
                })
            return rows
        finally:
            conn.close()
    except sqlite3.Error:
        return []


def fetch_active_events() -> List[Dict[str, Any]]:
    """Retrieve currently active physical events."""
    return _fetch_active_events_cached(DB_PATH, _get_db_mtime(DB_PATH))


@st.cache_data(ttl=2)
def _fetch_geospatial_sensors_cached(db_path: str, mtime: float) -> pd.DataFrame:
    """Internal cached geospatial sensors query with normalised elevations."""
    if not os.path.isfile(db_path):
        return pd.DataFrame()

    try:
        abs_path = os.path.abspath(db_path)
        conn = sqlite3.connect(f"file:{abs_path}?mode=ro", uri=True)
        try:
            query = """
            SELECT d.device_id, d.zone_id, d.device_type, d.unit, d.lat, d.lon, d.status, d.min_val, d.max_val,
                   COALESCE(r.true_value, 0.0) AS true_value,
                   COALESCE(r.broadcast_value, 0.0) AS broadcast_value,
                   COALESCE(r.is_attacked, 0) AS is_attacked,
                   COALESCE(r.attack_type, 'none') AS attack_type
            FROM devices d
            LEFT JOIN (
                SELECT r1.*
                FROM readings_truth r1
                JOIN (
                    SELECT device_id, MAX(id) as max_id
                    FROM readings_truth
                    GROUP BY device_id
                ) r2 ON r1.id = r2.max_id
            ) r ON d.device_id = r.device_id
            WHERE d.device_id LIKE 'z%'
            ORDER BY d.device_id ASC
            """
            df = pd.read_sql_query(query, conn)
        finally:
            conn.close()

        if df.empty:
            return df

        df["zone_name"] = df["zone_id"].map(ZONE_NAME_MAP).fillna(df["zone_id"])

        # Cross reference with active attacks
        active_atks = fetch_active_attacks()
        targeted_devices = {a["target_device_id"]: a for a in active_atks if a.get("target_device_id")}
        targeted_zones = {a["target_zone_id"]: a for a in active_atks if a.get("target_zone_id")}

        def evaluate_discrepancy(r: pd.Series) -> Tuple[bool, str]:
            dev_id = str(r["device_id"])
            zone_id = str(r["zone_id"])
            if dev_id in targeted_devices:
                atk = targeted_devices[dev_id]
                return True, atk.get("capability", "attack")
            if zone_id in targeted_zones:
                atk = targeted_zones[zone_id]
                return True, atk.get("capability", "attack")
            is_atk = bool(r.get("is_attacked", 0))
            drift = abs(float(r.get("broadcast_value", 0.0)) - float(r.get("true_value", 0.0))) > 0.05
            return (is_atk or drift), str(r.get("attack_type", "none"))

        eval_res = [evaluate_discrepancy(r) for _, r in df.iterrows()]
        df["discrepancy"] = [e[0] for e in eval_res]
        df["attack_type"] = [e[1] for e in eval_res]

        # Height normalisation to device's own [min_val, max_val]
        def calc_elevation(row: pd.Series) -> float:
            val = float(row["broadcast_value"])
            min_v = float(row.get("min_val", 0.0))
            max_v = float(row.get("max_val", 100.0))
            denom = max(max_v - min_v, 1e-6)
            norm = (val - min_v) / denom
            norm = max(0.0, min(1.0, norm))
            return 50.0 + norm * 450.0

        def calc_color(row: pd.Series) -> List[int]:
            st_val = str(row.get("status", "ok"))
            if bool(row.get("discrepancy")):
                return [239, 68, 68, 240]  # Attack red #ef4444
            if st_val in ("isolated", "quarantined"):
                return [245, 158, 11, 240]  # Contained amber #f59e0b
            return [148, 163, 184, 210]  # Neutral slate-400

        def calc_radius(row: pd.Series) -> int:
            st_val = str(row.get("status", "ok"))
            if bool(row.get("discrepancy")):
                return 90
            if st_val in ("isolated", "quarantined"):
                return 75
            return 45

        df["elevation"] = [calc_elevation(r) for _, r in df.iterrows()]
        df["fill_color"] = [calc_color(r) for _, r in df.iterrows()]
        df["radius"] = [calc_radius(r) for _, r in df.iterrows()]
        return df
    except sqlite3.Error as e:
        logger.warning(f"Error fetching geospatial sensors: {e}")
        return pd.DataFrame()


def fetch_geospatial_sensors() -> pd.DataFrame:
    """Retrieve device registry with latest readings and normalised heights."""
    return _fetch_geospatial_sensors_cached(DB_PATH, _get_db_mtime(DB_PATH))


def fetch_neighborhood_summaries(sensors_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Compute summary statistics per zone."""
    summaries = []
    for n in NEIGHBORHOODS:
        zid = n["zone_id"]
        z_df = sensors_df[sensors_df["zone_id"] == zid] if not sensors_df.empty else pd.DataFrame()
        threats = int(z_df["discrepancy"].sum()) if not z_df.empty else 0
        contained = int(z_df["status"].isin(["isolated", "quarantined"]).sum()) if not z_df.empty else 0
        summaries.append({
            "zone_id": zid,
            "name": n["name"],
            "total_sensors": len(z_df),
            "active_threats": threats,
            "contained": contained,
            "status": "threat" if threats > 0 else "contained" if contained > 0 else "nominal",
        })
    return summaries


def fetch_device_time_series(device_id: str, limit: int = 60) -> pd.DataFrame:
    """Retrieve historical time series for truth vs broadcast comparison."""
    if not os.path.isfile(DB_PATH):
        return pd.DataFrame()

    try:
        conn = get_db_connection()
        try:
            query = """
            SELECT id, seq, ts, true_value, broadcast_value, is_attacked, attack_type, unit, status
            FROM readings_truth
            WHERE device_id = ?
            ORDER BY id DESC
            LIMIT ?
            """
            df = pd.read_sql_query(query, conn, params=(device_id, limit))
        finally:
            conn.close()

        if df.empty:
            return df
        df = df.iloc[::-1].reset_index(drop=True)
        df["discrepancy"] = (df["broadcast_value"] - df["true_value"]).abs()
        return df
    except sqlite3.Error as e:
        logger.warning(f"Error fetching time series: {e}")
        return pd.DataFrame()


def fetch_registered_devices() -> List[str]:
    """Retrieve list of device identifiers."""
    if not os.path.isfile(DB_PATH):
        return []
    try:
        conn = get_db_connection()
        try:
            cur = conn.execute("SELECT device_id FROM devices ORDER BY zone_id, device_id")
            return [r[0] for r in cur.fetchall()]
        finally:
            conn.close()
    except sqlite3.Error:
        return []


def fetch_truth_ledger(limit: int = 50) -> pd.DataFrame:
    """Retrieve raw ground truth ledger entries."""
    if not os.path.isfile(DB_PATH):
        return pd.DataFrame()
    try:
        conn = get_db_connection()
        try:
            query = """
            SELECT id, reading_id, device_id, ts, seq, true_value, broadcast_value, unit, status, attack_type, is_attacked
            FROM readings_truth
            ORDER BY id DESC
            LIMIT ?
            """
            return pd.read_sql_query(query, conn, params=(limit,))
        finally:
            conn.close()
    except sqlite3.Error:
        return pd.DataFrame()


def fetch_timeline_events(limit: int = 100, since_sim_ts: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Retrieve journal entries from the red-team REST API or local database fallback."""
    params: Dict[str, Any] = {"limit": limit}
    if since_sim_ts:
        params["since_sim_ts"] = since_sim_ts

    try:
        resp = requests.get(f"{TWIN_API_URL}/api/journal", headers=get_red_team_headers(), params=params, timeout=2.5)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("journal", []), None
        elif resp.status_code == 401:
            return [], f"Authentication failed (401). Check RED_TEAM_TOKEN or RED_TEAM_TOKEN_FILE for {TWIN_API_URL}."
    except requests.RequestException:
        pass

    # Read-only SQLite fallback for tests and offline inspections
    if os.path.isfile(DB_PATH):
        try:
            conn = get_db_connection()
            try:
                entries: List[Dict[str, Any]] = []
                # commands_log
                cur = conn.execute("SELECT id, sim_ts, caller, tool, args_json, executed, refused_code FROM commands_log ORDER BY id DESC LIMIT ?", (limit,))
                for r in cur.fetchall():
                    try:
                        args = json.loads(r["args_json"]) if r["args_json"] else {}
                    except Exception:
                        args = {}
                    entries.append({
                        "key": f"command:{r['id']}",
                        "sim_ts": r["sim_ts"],
                        "source": "command",
                        "detail": {
                            "caller": r["caller"],
                            "tool": r["tool"],
                            "args": args,
                            "executed": bool(r["executed"]),
                            "refused_code": r["refused_code"],
                        },
                    })
                # containment_log
                cur = conn.execute("SELECT id, sim_ts, caller, tool, target, from_status, to_status FROM containment_log ORDER BY id DESC LIMIT ?", (limit,))
                for r in cur.fetchall():
                    entries.append({
                        "key": f"containment:{r['id']}",
                        "sim_ts": r["sim_ts"],
                        "source": "containment",
                        "detail": {
                            "caller": r["caller"],
                            "tool": r["tool"],
                            "target": r["target"],
                            "from_status": r["from_status"],
                            "to_status": r["to_status"],
                        },
                    })
                # physical_events
                cur = conn.execute("SELECT event_id, type, zone_id, device_id, params_json, start_sim_ts, resolved_sim_ts, resolved_by, escalated_sim_ts, escalated_to FROM physical_events ORDER BY event_id DESC LIMIT ?", (limit,))
                for r in cur.fetchall():
                    try:
                        p = json.loads(r["params_json"]) if r["params_json"] else {}
                    except Exception:
                        p = {}
                    entries.append({
                        "key": f"event_start:{r['event_id']}",
                        "sim_ts": r["start_sim_ts"],
                        "source": "event",
                        "detail": {
                            "event_id": r["event_id"],
                            "action": "start",
                            "type": r["type"],
                            "zone_id": r["zone_id"],
                            "device_id": r["device_id"],
                            "params": p,
                        },
                    })
                # attacks_injected
                cur = conn.execute("SELECT attack_id, capability, target_device_id, target_zone_id, params_json, start_sim_time, end_sim_time, description FROM attacks_injected ORDER BY start_sim_time DESC LIMIT ?", (limit,))
                for r in cur.fetchall():
                    try:
                        p = json.loads(r["params_json"]) if r["params_json"] else {}
                    except Exception:
                        p = {}
                    entries.append({
                        "key": f"attack_inject:{r['attack_id']}",
                        "sim_ts": r["start_sim_time"],
                        "source": "attack",
                        "detail": {
                            "attack_id": r["attack_id"],
                            "action": "inject",
                            "capability": r["capability"],
                            "target_device_id": r["target_device_id"],
                            "target_zone_id": r["target_zone_id"],
                            "params": p,
                            "description": r["description"],
                        },
                    })
                entries.sort(key=lambda x: str(x.get("sim_ts", "")))
                return entries, None
            finally:
                conn.close()
        except sqlite3.Error:
            pass

    return [], f"Cannot connect to journal API at {TWIN_API_URL}. Ensure the Twin engine is running (PYTHONPATH=src python -m twin.main) and TWIN_API_URL is configured."


def fetch_attacked_ledger_readings(limit: int = 100) -> List[Dict[str, Any]]:
    """Retrieve attacked readings from the ledger."""
    if not os.path.isfile(DB_PATH):
        return []
    try:
        conn = get_db_connection()
        try:
            cur = conn.execute(
                "SELECT reading_id, device_id, ts, seq, true_value, broadcast_value, unit, status, attack_type, is_attacked "
                "FROM readings_truth WHERE is_attacked = 1 ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except sqlite3.Error:
        return []


# ==============================================================================
# Red Team Control Actions (Interfacing with port 8100 REST API)
# ==============================================================================
def inject_attack_to_simulator(
    capability: str,
    target_device_id: Optional[str] = None,
    target_zone_id: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None,
) -> Tuple[bool, str]:
    """Inject an attack via the isolated Red Team REST API."""
    if not params:
        params = {}
    payload = {
        "capability": capability,
        "target_device_id": target_device_id,
        "target_zone_id": target_zone_id,
        "params": params,
        "description": description or f"Console injection: {capability}",
    }
    try:
        resp = requests.post(f"{TWIN_API_URL}/api/attack/inject", headers=get_red_team_headers(), json=payload, timeout=2.0)
        if resp.status_code == 200:
            aid = resp.json().get("attack_id", "atk-unknown")
            return True, f"Attack {aid} injected successfully."
        else:
            err = resp.json().get("message", resp.text)
            return False, f"[ALERT] API error ({resp.status_code}): {err}"
    except requests.RequestException as e:
        return False, f"[ALERT] Connection refused: {e}"


def clear_attacks_in_simulator(attack_id: str = "all") -> Tuple[bool, str]:
    """Clear active attacks via the isolated Red Team REST API."""
    payload = {"attack_id": attack_id}
    try:
        resp = requests.post(f"{TWIN_API_URL}/api/attack/clear", headers=get_red_team_headers(), json=payload, timeout=2.0)
        if resp.status_code == 200:
            return True, "Attacks cleared successfully."
        else:
            err = resp.json().get("message", resp.text)
            return False, f"[ALERT] API error ({resp.status_code}): {err}"
    except requests.RequestException as e:
        return False, f"[ALERT] Connection refused: {e}"


def inject_event_to_simulator(
    event_type: str,
    zone_id: str,
    intensity: float = 1.0,
    duration_s: int = 180,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    """Inject a physical event via the Red Team REST API."""
    payload = {
        "type": event_type,
        "zone_id": zone_id,
        "intensity": intensity,
        "duration_s": duration_s,
        "params": params or {},
    }
    try:
        resp = requests.post(f"{TWIN_API_URL}/api/event/inject", headers=get_red_team_headers(), json=payload, timeout=2.0)
        if resp.status_code == 200:
            eid = resp.json().get("event_id", "ev-unknown")
            return True, f"Physical event {eid} injected successfully."
        else:
            err = resp.json().get("message", resp.text)
            return False, f"[ALERT] API error ({resp.status_code}): {err}"
    except requests.RequestException as e:
        return False, f"[ALERT] Connection refused: {e}"


def clear_events_in_simulator(event_id: str = "all") -> Tuple[bool, str]:
    """Clear physical events via the Red Team REST API."""
    payload = {"event_id": event_id}
    try:
        resp = requests.post(f"{TWIN_API_URL}/api/event/clear", headers=get_red_team_headers(), json=payload, timeout=2.0)
        if resp.status_code == 200:
            return True, "Physical events cleared successfully."
        else:
            err = resp.json().get("message", resp.text)
            return False, f"[ALERT] API error ({resp.status_code}): {err}"
    except requests.RequestException as e:
        return False, f"[ALERT] Connection refused: {e}"


# ==============================================================================
# PyDeck 3D Map Visualisation Builder
# ==============================================================================
def build_spatial_twin_deck(sensors_df: pd.DataFrame, map_pitch: int = 50) -> pdk.Deck:
    """Build PyDeck 3D map from sensors dataframe with normalised pillar elevations."""
    df = sensors_df.copy()
    if df.empty:
        df = pd.DataFrame(columns=["lon", "lat", "elevation", "fill_color", "radius", "device_id", "device_type", "zone_name", "true_value", "broadcast_value", "unit", "status", "attack_type"])

    # 1. 3D Column Layer for Sensors
    sensor_column_layer = pdk.Layer(
        "ColumnLayer",
        data=df,
        get_position=["lon", "lat"],
        get_elevation="elevation",
        elevation_scale=1.0,
        radius=40,
        get_fill_color="fill_color",
        pickable=True,
        auto_highlight=True,
        extruded=True,
    )

    # 2. Scatter Beacon Layer (Radial marker)
    sensor_scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["lon", "lat"],
        get_radius="radius",
        get_fill_color="fill_color",
        get_line_color=[255, 255, 255, 180],
        line_width_min_pixels=1,
        pickable=True,
    )

    # 3. Neighborhood Centroids and Halo Radar Layer
    neighborhoods_df = pd.DataFrame(NEIGHBORHOODS)
    active_threat_zones = set(df[df["discrepancy"] == True]["zone_id"].unique()) if "discrepancy" in df.columns else set()
    neighborhoods_df["halo_color"] = neighborhoods_df["zone_id"].apply(
        lambda zid: [239, 68, 68, 70] if zid in active_threat_zones else [59, 130, 246, 30]
    )
    neighborhoods_df["halo_radius"] = neighborhoods_df["zone_id"].apply(
        lambda zid: 650 if zid in active_threat_zones else 450
    )

    zone_halo_layer = pdk.Layer(
        "ScatterplotLayer",
        data=neighborhoods_df,
        get_position=["lon", "lat"],
        get_radius="halo_radius",
        get_fill_color="halo_color",
        get_line_color=[255, 255, 255, 50],
        line_width_min_pixels=1,
        pickable=False,
    )

    # 4. Zone Labels Layer
    zone_text_layer = pdk.Layer(
        "TextLayer",
        data=neighborhoods_df,
        get_position=["lon", "lat"],
        get_text="name",
        get_size=12,
        get_color=[230, 237, 243, 240],
        get_angle=0,
        get_text_anchor="'middle'",
        get_alignment_baseline="'center'",
        background=True,
        get_background_color=[11, 15, 20, 200],
        pickable=False,
    )

    view_state = pdk.ViewState(
        latitude=36.808,
        longitude=10.198,
        zoom=12.1,
        pitch=map_pitch,
        bearing=-15,
    )

    return pdk.Deck(
        layers=[zone_halo_layer, sensor_scatter_layer, sensor_column_layer, zone_text_layer],
        initial_view_state=view_state,
        tooltip={
            "html": """
            <div style="background: #121821; padding: 10px 14px; border: 1px solid #30363d; border-radius: 6px; color: #e6edf3; font-family: 'JetBrains Mono', monospace; font-size: 0.8rem;">
                <b style="color: #3b82f6; font-size: 0.9rem;">{device_id}</b> ({device_type})<br/>
                <b>Sector:</b> {zone_name}<br/>
                <b>Broadcast:</b> {broadcast_value} {unit}<br/>
                <b>Physical truth:</b> {true_value} {unit}<br/>
                <b>Status:</b> {status}<br/>
                <b>Active attack:</b> {attack_type}
            </div>
            """,
            "style": {"color": "#e6edf3"},
        },
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-nolabels-gl-style/style.json",
    )


# ==============================================================================
# Main Console Rendering Layout
# ==============================================================================
def render_status_bar_and_metrics(metrics: Dict[str, Any]) -> None:
    """Render the one-line status bar and the 4 KPI tiles."""
    sim_time = metrics.get("latest_sim_time", "2026-10-17T12:00:00Z")
    sim_speed = metrics.get("sim_speed", 1.0)
    msgs_sec = metrics.get("msgs_per_sec", 0.0)
    kafka_st = metrics.get("kafka_state", "Offline")
    mcp_st = metrics.get("mcp_state", "Offline")
    callers = metrics.get("recent_callers", "None")

    # One-line status bar (never truncated)
    st.markdown(
        f"""
        <div class="status-bar">
            <div class="status-item">Simulation time: <span class="status-value">{sim_time}</span></div>
            <div class="status-item">Speed: <span class="status-value">{sim_speed:.0f}x</span></div>
            <div class="status-item">Throughput: <span class="status-value">{msgs_sec:.1f} msg/s</span></div>
            <div class="status-item">Kafka: <span class="status-value state-nominal">{kafka_st}</span></div>
            <div class="status-item">MCP: <span class="status-value state-nominal">{mcp_st}</span></div>
            <div class="status-item">Recent callers: <span class="status-value">{callers}</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 4 KPI Tiles (Zero deltas)
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric(label="Readings", value=f"{metrics.get('total_readings', 0):,}")
    with k2:
        st.metric(label="Attacked readings", value=f"{metrics.get('attacked_readings', 0):,}")
    with k3:
        st.metric(label="Active attacks", value=str(metrics.get("active_attacks", 0)))
    with k4:
        st.metric(label="Devices contained", value=str(metrics.get("contained_devices", 0)))


def render_app() -> None:
    """Mount and render the full Streamlit operations console."""
    # Render Top Header
    st.subheader("Twin operations console", divider=False)

    # Fetch Metrics and Render Top Bar
    metrics = fetch_scoreboard_metrics()
    render_status_bar_and_metrics(metrics)

    # Four Tabs in Required Order
    tab_timeline, tab_map, tab_divergence, tab_ledger = st.tabs([
        "Timeline",
        "City map",
        "Truth vs broadcast",
        "Ledger",
    ])

    devices_list = fetch_registered_devices()

    # --------------------------------------------------------------------------
    # TAB 1: Correlated Operations Timeline (Rule R3)
    # --------------------------------------------------------------------------
    with tab_timeline:
        st.markdown("#### Operational timeline")
        st.caption(
            "Correlated sequence of telemetry readings, Guardian containment actions, "
            "City Brain actuation commands, and actuator results with simulation timestamps."
        )

        # Controls: Time window, zone, device, and follow live toggle
        c_ctrl1, c_ctrl2, c_ctrl3, c_ctrl4 = st.columns([2, 2, 2, 1])
        with c_ctrl1:
            window_opt = st.selectbox(
                "Time window",
                options=["Last 5 sim-min", "Last 15 sim-min", "Last 60 sim-min", "All time"],
                index=0,
            )
        with c_ctrl2:
            zone_filter = st.selectbox(
                "Filter by zone",
                options=["All zones"] + [n["zone_id"] for n in NEIGHBORHOODS],
                index=0,
            )
        with c_ctrl3:
            device_filter = st.selectbox(
                "Filter by device",
                options=["All devices"] + devices_list,
                index=0,
            )
        with c_ctrl4:
            st.toggle("Follow live", value=True)

        window_minutes = 5 if window_opt == "Last 5 sim-min" else 15 if window_opt == "Last 15 sim-min" else 60 if window_opt == "Last 60 sim-min" else None

        # Calculate cutoff sim_ts for journal fetch to avoid over-fetching
        since_ts: Optional[str] = None
        if window_minutes:
            cur_sec = parse_rfc3339_to_seconds(metrics.get("latest_sim_time", "2026-10-17T12:00:00Z"))
            cutoff_sec = cur_sec - (window_minutes * 60.0)
            since_ts = datetime.fromtimestamp(cutoff_sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Fetch Journal Entries
        journal_entries, journal_error = fetch_timeline_events(limit=500, since_sim_ts=since_ts)

        if journal_error:
            st.warning(journal_error)

        # Fetch Attacked Readings and Device Map
        attacked_readings = fetch_attacked_ledger_readings(limit=100)
        device_zone_map = {}
        if os.path.isfile(DB_PATH):
            try:
                conn = get_db_connection()
                try:
                    for r in conn.execute("SELECT device_id, zone_id FROM devices").fetchall():
                        device_zone_map[r[0]] = r[1]
                finally:
                    conn.close()
            except Exception:
                pass

        # Optional Partner Logs from TIMELINE_LOG_DIR
        partner_logs = load_partner_log_entries(TIMELINE_LOG_DIR) if os.path.isdir(TIMELINE_LOG_DIR) else []

        # Build and Filter Timeline Rows
        raw_rows = build_timeline_rows(
            journal_entries=journal_entries,
            attacked_readings=attacked_readings,
            device_zone_map=device_zone_map,
            log_entries=partner_logs,
        )

        filtered_rows = filter_timeline_rows(
            rows=raw_rows,
            window_sim_minutes=window_minutes,
            zone_id=zone_filter,
            device_id=device_filter,
            current_sim_ts=metrics.get("latest_sim_time"),
        )

        # Separate Main Lanes from Test Harness
        main_rows = [r for r in filtered_rows if r.lane != "Test harness"]
        test_rows = [r for r in filtered_rows if r.lane == "Test harness"]

        # Render Active Incident Chains Summary
        active_chains = {}
        for r in main_rows:
            if r.chain_id and r.chain_summary and r.chain_id not in active_chains:
                active_chains[r.chain_id] = r.chain_summary

        if active_chains:
            st.markdown("##### Correlated incident chains")
            for cid, ctext in active_chains.items():
                st.info(f"**Chain {cid}:** {ctext}")

        # Build DataFrame for Display
        table_data = []
        for r in main_rows:
            rx_time_str = format_reaction_delta(r.reaction_time_s) if r.reaction_time_s is not None and r.reaction_time_s > 0 else "-"
            table_data.append({
                "Sim time": r.sim_ts,
                "Lane": r.lane,
                "Event": r.event_type,
                "Target": r.device_id or r.zone_id or "-",
                "Reaction": rx_time_str,
                "Summary": r.summary,
                "Chain": r.chain_id or "-",
            })

        if table_data:
            df_table = pd.DataFrame(table_data)
            st.markdown("##### Correlated event sequence")
            st.caption("Select any row to inspect its underlying JSON payload.")
            
            event_selection = st.dataframe(
                df_table,
                width="stretch",
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                column_config={
                    "Sim time": st.column_config.TextColumn("Sim time", width="medium"),
                    "Lane": st.column_config.TextColumn("Lane", width="small"),
                    "Event": st.column_config.TextColumn("Event", width="small"),
                    "Target": st.column_config.TextColumn("Target", width="small"),
                    "Reaction": st.column_config.TextColumn("Reaction", width="small"),
                    "Summary": st.column_config.TextColumn("Summary", width="large"),
                    "Chain": st.column_config.TextColumn("Chain", width="small"),
                },
            )

            # Show JSON inspector when a row is selected
            selected_rows = event_selection.selection.rows if hasattr(event_selection, "selection") else []
            if selected_rows:
                sel_idx = selected_rows[0]
                if 0 <= sel_idx < len(main_rows):
                    selected_row_obj = main_rows[sel_idx]
                    st.markdown("###### Selected event details")
                    st.json(selected_row_obj.raw_json or selected_row_obj.to_dict())
        else:
            st.info("No timeline events recorded in the selected window.")

        # Test Harness Lane (Muted, Collapsed by Default)
        if test_rows:
            with st.expander(f"Test harness events ({len(test_rows)} test runner actions)", expanded=False):
                test_table = []
                for r in test_rows:
                    test_table.append({
                        "Sim time": r.sim_ts,
                        "Event": r.event_type,
                        "Target": r.device_id or r.zone_id or "-",
                        "Summary": r.summary,
                    })
                st.dataframe(pd.DataFrame(test_table), width="stretch", hide_index=True)

    # --------------------------------------------------------------------------
    # TAB 2: 3D City Map
    # --------------------------------------------------------------------------
    with tab_map:
        st.markdown("#### Greater Tunis 3D sensor map")
        st.caption(
            "Interactive geospatial map of 42 sensors across 6 sectors. "
            "Column height indicates measurement magnitude normalised to each device's operating range [min_val, max_val]."
        )

        # Compact Legend
        st.markdown(
            """
            <div style="display: flex; gap: 20px; font-size: 0.85rem; margin-bottom: 12px;">
                <span><span class="state-nominal">&#9632;</span> Nominal device</span>
                <span><span class="state-contained">&#9632;</span> Contained (isolated / quarantined)</span>
                <span><span class="state-attack">&#9632;</span> Active threat / tampered</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        sensors_df = fetch_geospatial_sensors()
        if not sensors_df.empty:
            deck = build_spatial_twin_deck(sensors_df, map_pitch=50)
            st.pydeck_chart(deck)
        else:
            st.info("Awaiting sensor spatial registration from CitySimulator...")

        # Compact Sector Summaries Table
        st.markdown("##### Sector summaries")
        summaries = fetch_neighborhood_summaries(sensors_df)
        sum_df = pd.DataFrame(summaries)
        st.dataframe(
            sum_df,
            width="stretch",
            hide_index=True,
            column_config={
                "zone_id": st.column_config.TextColumn("Zone ID"),
                "name": st.column_config.TextColumn("Sector name"),
                "total_sensors": st.column_config.NumberColumn("Sensors"),
                "active_threats": st.column_config.NumberColumn("Active threats"),
                "contained": st.column_config.NumberColumn("Contained devices"),
                "status": st.column_config.TextColumn("Status"),
            },
        )

    # --------------------------------------------------------------------------
    # TAB 3: Truth vs Broadcast Divergence
    # --------------------------------------------------------------------------
    with tab_divergence:
        st.markdown("#### Truth vs broadcast divergence")
        st.caption(
            "Compare unpoisoned physical reality recorded in the local SQLite ledger "
            "against wire telemetry broadcast to partners over Kafka."
        )

        sensors_df = fetch_geospatial_sensors()
        c_sel1, c_sel2 = st.columns([3, 2])

        with c_sel1:
            # Device picker grouped by sector
            grouped_options = []
            dev_to_zone = {}
            for n in NEIGHBORHOODS:
                zid = n["zone_id"]
                zname = n["name"]
                z_devs = [d for d in devices_list if d.startswith(f"z{zid.split('-')[1]}-")]
                for dev in z_devs:
                    label = f"{zid} ({zname}): {dev}"
                    grouped_options.append((label, dev))
                    dev_to_zone[dev] = zid

            # Add rogue devices if any
            rogue_devs = [d for d in devices_list if d.startswith("rg-")]
            for r_dev in rogue_devs:
                grouped_options.append((f"Rogue: {r_dev}", r_dev))

            selected_label = st.selectbox(
                "Select sensor for time series analysis",
                options=[opt[0] for opt in grouped_options] if grouped_options else devices_list,
                index=0 if grouped_options else None,
            )
            target_dev = next((opt[1] for opt in grouped_options if opt[0] == selected_label), devices_list[0] if devices_list else None)

        with c_sel2:
            chart_points = st.slider("Time window (readings)", min_value=20, max_value=200, value=60, step=10)

        if target_dev:
            ts_df = fetch_device_time_series(target_dev, limit=chart_points)

            if ts_df.empty:
                st.info(f"No telemetry readings recorded yet for {target_dev}.")
            else:
                latest_true = ts_df["true_value"].iloc[-1]
                latest_bc = ts_df["broadcast_value"].iloc[-1]
                current_div = abs(latest_bc - latest_true)
                unit = ts_df["unit"].iloc[-1] if "unit" in ts_df.columns else ""
                tampered_count = int(ts_df["is_attacked"].sum())

                # Metric tiles (no deltas)
                m1, m2, m3, m4 = st.columns(4)
                with m1:
                    st.metric("Physical truth", f"{latest_true:.2f} {unit}")
                with m2:
                    st.metric("Broadcast value", f"{latest_bc:.2f} {unit}")
                with m3:
                    st.metric("Absolute divergence", f"{current_div:.2f} {unit}")
                with m4:
                    st.metric("Tampered frames in window", f"{tampered_count} / {len(ts_df)}")

                # Multi-layer Altair Chart
                base_chart = alt.Chart(ts_df).encode(
                    x=alt.X("seq:Q", title="Sequence counter (seq)", scale=alt.Scale(zero=False))
                )

                # Divergence area shaded in light red
                div_area = base_chart.mark_area(opacity=0.22, color="#ef4444").encode(
                    y=alt.Y("true_value:Q"),
                    y2=alt.Y2("broadcast_value:Q"),
                )

                # Solid line for physical truth
                truth_line = base_chart.mark_line(strokeWidth=2.2, color="#22c55e").encode(
                    y=alt.Y("true_value:Q", title=f"Measurement ({unit})", scale=alt.Scale(zero=False)),
                    tooltip=["seq:Q", "ts:N", "true_value:Q", "broadcast_value:Q", "is_attacked:Q"],
                )

                # Dashed line for broadcast telemetry
                bc_line = base_chart.mark_line(strokeWidth=2.2, color="#3b82f6", strokeDash=[4, 4]).encode(
                    y=alt.Y("broadcast_value:Q", scale=alt.Scale(zero=False)),
                    tooltip=["seq:Q", "ts:N", "true_value:Q", "broadcast_value:Q", "is_attacked:Q"],
                )

                final_chart = (div_area + truth_line + bc_line).properties(height=380).interactive()
                st.altair_chart(final_chart, width="stretch")

    # --------------------------------------------------------------------------
    # TAB 4: Ground Truth Ledger
    # --------------------------------------------------------------------------
    with tab_ledger:
        st.markdown("#### Isolated ground truth ledger")
        st.caption(
            "Read-only query of the internal SQLite ledger (`readings_truth`). "
            "Attacked rows with deliberate discrepancies are highlighted."
        )

        ledger_df = fetch_truth_ledger(limit=50)
        if not ledger_df.empty:
            # Style attacked rows with light reddish tint
            def highlight_attacks(row: pd.Series) -> List[str]:
                if row.get("is_attacked") == 1:
                    return ["background-color: rgba(239, 68, 68, 0.12)"] * len(row)
                return [""] * len(row)

            styled_df = ledger_df.style.apply(highlight_attacks, axis=1)

            st.dataframe(
                styled_df,
                width="stretch",
                hide_index=True,
                column_config={
                    "id": st.column_config.NumberColumn("ID", width="small"),
                    "reading_id": st.column_config.TextColumn("Reading ID", width="medium"),
                    "device_id": st.column_config.TextColumn("Device", width="small"),
                    "ts": st.column_config.TextColumn("Timestamp", width="medium"),
                    "seq": st.column_config.NumberColumn("Sequence", width="small"),
                    "true_value": st.column_config.NumberColumn("Physical truth", format="%.2f"),
                    "broadcast_value": st.column_config.NumberColumn("Broadcast", format="%.2f"),
                    "unit": st.column_config.TextColumn("Unit", width="small"),
                    "status": st.column_config.TextColumn("Status", width="small"),
                    "attack_type": st.column_config.TextColumn("Attack capability", width="medium"),
                    "is_attacked": st.column_config.CheckboxColumn("Attacked", width="small"),
                },
            )
        else:
            st.info("No ledger entries found.")

    # ==========================================================================
    # Sidebar: Red Team Controls (Forms, Active Threats, and Physical Events)
    # ==========================================================================
    st.sidebar.subheader("Red team adversary controls")

    # 1. Manual Attack Injection Form
    with st.sidebar.form("manual_injection_form"):
        st.markdown("**Manual injection**")
        cap_choice = st.selectbox(
            "Adversary capability",
            options=[
                "device_physical",
                "device_key",
                "link",
                "rogue",
                "sensor_fault",
                "actuator",
            ],
            format_func=lambda c: {
                "device_physical": "device_physical (sensor spoofing)",
                "device_key": "device_key (compromised key)",
                "link": "link (network tamper)",
                "rogue": "rogue (unregistered sensor)",
                "sensor_fault": "sensor_fault (hardware drift/freeze)",
                "actuator": "actuator (command disobedience)",
            }.get(c, c),
        )

        target_device = st.selectbox(
            "Target device",
            options=devices_list if devices_list else ["z1-tc1"],
            index=0,
        )

        params: Dict[str, Any] = {}
        if cap_choice == "device_physical":
            spoofed_v = st.number_input("Spoofed value", value=185.0, step=1.0)
            params["value"] = spoofed_v
        elif cap_choice == "link":
            link_mode = st.selectbox("Link tamper mode", options=["corrupt_bytes", "corrupt_sig", "replay", "delay", "drop"])
            params["link_mode"] = link_mode
        elif cap_choice == "sensor_fault":
            fault_mode = st.selectbox("Fault mode", options=["drift", "freeze", "spike"])
            params["fault_mode"] = fault_mode
            if fault_mode == "drift":
                params["drift_rate"] = 2.0
        elif cap_choice == "actuator":
            act_state = st.selectbox("Refused state", options=["stuck_closed", "stuck_open"])
            params["forced_physical_state"] = act_state
        elif cap_choice == "rogue":
            r_id = st.text_input("Rogue device identifier", value="rg-x9")
            params["rogue_device_id"] = r_id

        desc = st.text_input("Description", value=f"Console injected {cap_choice}")

        submit_attack = st.form_submit_button("Inject attack")
        if submit_attack:
            ok, msg = inject_attack_to_simulator(
                capability=cap_choice,
                target_device_id=target_device,
                params=params,
                description=desc,
            )
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)

    # 2. Physical Events Form
    with st.sidebar.form("physical_events_form"):
        st.markdown("**Physical events**")
        ev_type = st.selectbox(
            "Event type",
            options=["traffic_accident", "rain_storm", "fire", "grid_overload", "air_hazard"],
        )
        ev_zone = st.selectbox(
            "Target zone",
            options=[n["zone_id"] for n in NEIGHBORHOODS],
            format_func=lambda z: f"{z} ({ZONE_NAME_MAP.get(z, z)})",
        )
        ev_intensity = st.slider("Intensity multiplier", min_value=0.5, max_value=5.0, value=1.5, step=0.5)
        ev_duration = st.number_input("Duration (sim-seconds)", min_value=30, max_value=3600, value=180, step=30)

        submit_event = st.form_submit_button("Inject event")
        if submit_event:
            ok, msg = inject_event_to_simulator(
                event_type=ev_type,
                zone_id=ev_zone,
                intensity=ev_intensity,
                duration_s=int(ev_duration),
            )
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)

    # 3. Active Attacks and Events Manifest
    st.sidebar.markdown("**Active attacks and events**")
    active_atks = fetch_active_attacks()
    active_evts = fetch_active_events()

    if not active_atks and not active_evts:
        st.sidebar.caption("Zero active attacks or physical events.")
    else:
        for atk in active_atks:
            aid = atk.get("attack_id", "atk")
            acap = atk.get("capability", "attack")
            adev = atk.get("target_device_id") or atk.get("target_zone_id") or "device"
            c_txt, c_btn = st.sidebar.columns([3, 1])
            c_txt.caption(f"{acap} on {adev}")
            if c_btn.button("Clear", key=f"btn_clr_{aid}"):
                ok, msg = clear_attacks_in_simulator(attack_id=aid)
                if ok:
                    st.sidebar.success(msg)
                    st.rerun()
                else:
                    st.sidebar.error(msg)

        for evt in active_evts:
            eid = evt.get("event_id", "ev")
            etype = evt.get("type", "event")
            ezone = evt.get("zone_id", "city")
            c_txt, c_btn = st.sidebar.columns([3, 1])
            c_txt.caption(f"{etype} in {ezone}")
            if c_btn.button("Clear", key=f"btn_clr_ev_{eid}"):
                ok, msg = clear_events_in_simulator(event_id=eid)
                if ok:
                    st.sidebar.success(msg)
                    st.rerun()
                else:
                    st.sidebar.error(msg)

    # Clear all threats button
    if st.sidebar.button("Clear all threats"):
        ok_a, msg_a = clear_attacks_in_simulator("all")
        ok_e, msg_e = clear_events_in_simulator("all")
        if ok_a and ok_e:
            st.sidebar.success("All attacks and events cleared.")
            st.rerun()
        else:
            st.sidebar.error(f"{msg_a} {msg_e}")


# Execute UI mounting only when executed within a Streamlit run context
if get_script_run_ctx() is not None or __name__ == "__main__":
    render_app()
