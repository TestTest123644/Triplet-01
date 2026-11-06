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
import socket
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple
import urllib.parse

import altair as alt
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st
from streamlit.runtime.scriptrunner import get_script_run_ctx

# 1. First Streamlit call: Set page configuration immediately (Finding 11)
st.set_page_config(
    page_title="Twin operations console",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
MCP_URL = os.getenv("TWIN_MCP_URL", os.getenv("MCP_URL", "http://localhost:8000")).rstrip("/")
TIMELINE_LOG_DIR = os.getenv("TIMELINE_LOG_DIR")  # Off unless explicitly set (Finding 8)

NEIGHBORHOODS = [
    {"zone_id": "zone-1", "name": "Downtown Financial", "lat": 36.800, "lon": 10.180},
    {"zone_id": "zone-2", "name": "Port & Coastal Harbor", "lat": 36.815, "lon": 10.305},
    {"zone_id": "zone-3", "name": "North Industrial Park", "lat": 36.855, "lon": 10.190},
    {"zone_id": "zone-4", "name": "West Residential Hills", "lat": 36.810, "lon": 10.115},
    {"zone_id": "zone-5", "name": "Central University & Med", "lat": 36.835, "lon": 10.145},
    {"zone_id": "zone-6", "name": "South Logistics Hub", "lat": 36.755, "lon": 10.220},
]
ZONE_NAME_MAP = {n["zone_id"]: n["name"] for n in NEIGHBORHOODS}

# 2. Modern, clean CSS block under 60 lines (Finding 12)
CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');
html, body, [class*="css"], .stMarkdown, p, div, span, label {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}
code, pre, .mono, [data-testid="stMetricValue"] {
    font-family: 'JetBrains Mono', monospace !important;
}
.status-bar {
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    align-items: center;
    column-gap: 1.1rem;
    row-gap: 0.4rem;
    padding: 0.6rem 1rem;
    background: #121821;
    border: 1px solid #1e293b;
    border-radius: 6px;
    margin-bottom: 1rem;
    font-size: 0.88rem;
}
@media (max-width: 900px) {
    .status-bar { gap: 0.75rem; }
}
.status-bar .item { display: flex; align-items: center; gap: 0.4rem; white-space: nowrap; }
.status-bar .lbl { color: #94a3b8; }
.status-bar .val { font-family: 'JetBrains Mono', monospace; font-weight: 500; color: #e6edf3; }
.state-nominal { color: #22c55e !important; }
.state-contained { color: #f59e0b !important; }
.state-attack { color: #ef4444 !important; }
.state-refused { color: #a855f7 !important; }
.legend-swatch {
    display: inline-block;
    width: 12px;
    height: 12px;
    border-radius: 2px;
    margin-right: 6px;
    vertical-align: middle;
}
.swatch-nominal { background: #dce2eb; }
.swatch-contained { background: #f59e0b; }
.swatch-attack { background: #ef4444; }
.swatch-refused { background: #a855f7; }
</style>
"""


# ==============================================================================
# Database Access Helpers (Strictly Read-Only SQLite)
# ==============================================================================
def get_db_connection(path: Optional[str] = None) -> sqlite3.Connection:
    """Open read-only SQLite URI connection to the ground truth database."""
    abs_path = os.path.abspath(path or DB_PATH)
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


def check_mcp_connection() -> bool:
    """Probe MCP command server via 0.5s TCP connection check (Finding 10)."""
    try:
        parsed = urllib.parse.urlparse(MCP_URL)
        host = parsed.hostname or "localhost"
        port = parsed.port or 8000
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except Exception:
        return False


def fetch_api_status() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Fetch status from red-team REST API on port 8100."""
    try:
        resp = requests.get(f"{TWIN_API_URL}/api/status", headers=get_red_team_headers(), timeout=1.0)
        if resp.status_code == 200:
            return resp.json(), None
        return None, f"HTTP {resp.status_code}"
    except requests.RequestException as e:
        return None, f"connection failed ({e})"


# ==============================================================================
# Cached Data Fetchers (ttl 1-3 seconds, fast MAX(id) queries)
# ==============================================================================
@st.cache_data(ttl=2)
def _fetch_scoreboard_metrics_cached(db_path: str, mtime: float) -> Dict[str, Any]:
    """Internal cached implementation of scoreboard metrics query."""
    metrics: Dict[str, Any] = {
        "total_readings": 0,
        "attacked_readings": 0,
        "active_incidents": 0,
        "active_attacks": 0,
        "contained_devices": 0,
        "latest_sim_time": "2026-10-17T12:00:00Z",
        "sim_speed": 1.0,
        "msgs_per_sec": 0.0,
        "kafka_state": "unknown",
        "mcp_state": "unknown",
        "twin_api_online": False,
        "recent_callers": "None",
    }

    # 1. Check real MCP server status via TCP
    metrics["mcp_state"] = "ok" if check_mcp_connection() else "down"

    # 2. Check real Twin API status
    status_data, api_err = fetch_api_status()
    if status_data:
        metrics["twin_api_online"] = True
        metrics["latest_sim_time"] = status_data.get("sim_time", metrics["latest_sim_time"])
        metrics["sim_speed"] = float(status_data.get("sim_speed", 1.0))
        st_info = status_data.get("stream", {})
        metrics["msgs_per_sec"] = float(st_info.get("msgs_per_sec", st_info.get("throughput_msgs_sec", 0.0)))
        metrics["kafka_state"] = st_info.get("kafka_state", "unknown")
    else:
        metrics["twin_api_online"] = False
        metrics["kafka_state"] = "unknown"

    if not os.path.isfile(db_path):
        return metrics

    conn = None
    try:
        conn = get_db_connection(db_path)

        # O(1) query using MAX(id) on indexed primary key
        r_row = conn.execute("SELECT MAX(id) AS max_id, MAX(ts) AS max_ts FROM readings_truth").fetchone()
        if r_row and r_row["max_id"]:
            metrics["total_readings"] = int(r_row["max_id"])
            if not metrics["twin_api_online"] and r_row["max_ts"]:
                metrics["latest_sim_time"] = str(r_row["max_ts"])

        # Indexed count of attacked readings
        r_atk = conn.execute("SELECT COUNT(*) AS c FROM readings_truth WHERE is_attacked = 1").fetchone()
        if r_atk:
            metrics["attacked_readings"] = int(r_atk["c"])
            metrics["active_incidents"] = metrics["attacked_readings"]

        # Active attacks from attacks_injected (column name is active, not is_active)
        r_live_atk = conn.execute("SELECT COUNT(*) AS c FROM attacks_injected WHERE active = 1").fetchone()
        if r_live_atk:
            metrics["active_attacks"] = int(r_live_atk["c"])

        # Contained devices (quarantined or isolated)
        r_cont = conn.execute("SELECT COUNT(*) AS c FROM devices WHERE status IN ('isolated', 'quarantined')").fetchone()
        if r_cont:
            metrics["contained_devices"] = int(r_cont["c"])

    except Exception as e:
        logger.warning(f"Error reading scoreboard metrics from ledger: {e}")
    finally:
        if conn:
            conn.close()

    return metrics


def fetch_scoreboard_metrics() -> Dict[str, Any]:
    """Retrieve fast headline metrics for the status bar and KPI cards."""
    return _fetch_scoreboard_metrics_cached(DB_PATH, _get_db_mtime(DB_PATH))


def fetch_active_attacks() -> List[Dict[str, Any]]:
    """Retrieve currently active red-team attacks from API or ledger."""
    try:
        resp = requests.get(f"{TWIN_API_URL}/api/attack/active", headers=get_red_team_headers(), timeout=1.0)
        if resp.status_code == 200:
            return resp.json().get("active_attacks", [])
    except requests.RequestException:
        pass

    if not os.path.isfile(DB_PATH):
        return []
    conn = None
    try:
        conn = get_db_connection()
        rows = conn.execute(
            "SELECT attack_id, capability, target_device_id, target_zone_id, start_sim_time, params_json "
            "FROM attacks_injected WHERE active = 1 ORDER BY rowid DESC"
        ).fetchall()
        active = []
        for r in rows:
            p_val = {}
            if r["params_json"]:
                try:
                    p_val = json.loads(r["params_json"])
                except Exception:
                    pass
            active.append({
                "attack_id": r["attack_id"],
                "capability": r["capability"],
                "target_device_id": r["target_device_id"],
                "target_zone_id": r["target_zone_id"],
                "start_sim_time": r["start_sim_time"],
                "params": p_val,
            })
        return active
    except Exception:
        return []
    finally:
        if conn:
            conn.close()


def fetch_active_physical_events() -> List[Dict[str, Any]]:
    """Retrieve currently active physical environmental events."""
    try:
        resp = requests.get(f"{TWIN_API_URL}/api/event/active", headers=get_red_team_headers(), timeout=1.0)
        if resp.status_code == 200:
            return resp.json().get("active_events", [])
    except requests.RequestException:
        pass

    if not os.path.isfile(DB_PATH):
        return []
    conn = None
    try:
        conn = get_db_connection()
        rows = conn.execute(
            "SELECT event_id, event_type, zone_id, intensity, start_sim_time "
            "FROM physical_events WHERE is_active = 1 ORDER BY id DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        if conn:
            conn.close()


def fetch_journal_entries(
    since_sim_ts: Optional[str] = None, limit: int = 1000
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """Fetch journal entries from red-team REST API.

    Finding 7: Strictly never falls back to the database. If unreachable or non-200,
    returns (None, clear_error_message).
    """
    token_val = get_red_team_token()
    token_state = "set" if token_val else "missing"

    headers = get_red_team_headers()
    url = f"{TWIN_API_URL}/api/journal"
    params: Dict[str, Any] = {"limit": limit}
    if since_sim_ts:
        params["since_sim_ts"] = since_sim_ts

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=2.0)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("journal", []), None
        else:
            msg = (
                f"Cannot connect to journal API: HTTP {resp.status_code} at {TWIN_API_URL}. "
                f"Check TWIN_API_URL ({TWIN_API_URL}) and RED_TEAM_TOKEN or RED_TEAM_TOKEN_FILE ({token_state})."
            )
            return None, msg
    except requests.RequestException as e:
        err_str = str(e).lower()
        if "connection refused" in err_str or "errno 61" in err_str or "errno 111" in err_str:
            reason = "connection refused"
        elif "timed out" in err_str or "timeout" in err_str:
            reason = "timed out"
        else:
            reason = "cannot reach host"
        msg = (
            f"Cannot connect to journal API: {reason} at {TWIN_API_URL}. "
            f"Check TWIN_API_URL ({TWIN_API_URL}) and RED_TEAM_TOKEN or RED_TEAM_TOKEN_FILE ({token_state})."
        )
        return None, msg


def fetch_timeline_events(limit: int = 50) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Compatibility wrapper for tests and external callers."""
    entries, err = fetch_journal_entries(limit=limit)
    if err:
        return [], err
    return entries or [], None


def fetch_first_attacked_readings_for_attacks(
    db_path: str, attacks: List[TimelineRow]
) -> List[Dict[str, Any]]:
    """Query ledger for the FIRST attacked reading for each attack in the window (Finding 1).

    Uses idx_readings_device_ts index:
    device_id = target AND is_attacked = 1 AND ts >= inject_sim_ts ORDER BY id ASC LIMIT 1.
    """
    if not os.path.isfile(db_path):
        return []

    conn = None
    first_readings: List[Dict[str, Any]] = []
    try:
        conn = get_db_connection()
        for atk in attacks:
            target_dev = atk.device_id
            if not target_dev:
                continue
            row = conn.execute(
                """
                SELECT id, seq, reading_id, device_id, ts, true_value, broadcast_value, unit, is_attacked, attack_type
                FROM readings_truth
                WHERE device_id = ? AND is_attacked = 1 AND ts >= ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (target_dev, atk.sim_ts),
            ).fetchone()
            if row:
                first_readings.append(dict(row))
    except Exception as e:
        logger.warning(f"Error querying first attacked reading: {e}")
    finally:
        if conn:
            conn.close()
    return first_readings


def fetch_device_zone_map(db_path: str) -> Dict[str, str]:
    """Query registered devices to map device_id to zone_id from SQLite ledger (Finding 4)."""
    if not os.path.isfile(db_path):
        return {}
    conn = None
    try:
        conn = get_db_connection()
        rows = conn.execute("SELECT device_id, zone_id FROM devices").fetchall()
        return {r["device_id"]: r["zone_id"] for r in rows if r["device_id"] and r["zone_id"]}
    except Exception as e:
        logger.warning(f"Error fetching device zone map: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def inject_attack_to_simulator(
    capability: str,
    target_device_id: Optional[str] = None,
    target_zone_id: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    description: str = "",
) -> Tuple[bool, str]:
    """Inject attack via red team REST API."""
    payload = {
        "capability": capability,
        "target_device_id": target_device_id,
        "target_zone_id": target_zone_id,
        "params": params or {},
        "description": description,
    }
    try:
        resp = requests.post(
            f"{TWIN_API_URL}/api/attack/inject",
            headers=get_red_team_headers(),
            json=payload,
            timeout=2.0,
        )
        if resp.status_code == 200:
            return True, f"Injected {capability} on {target_device_id}"
        return False, f"[ALERT] Injection failed: HTTP {resp.status_code}"
    except Exception as e:
        return False, f"[ALERT] Connection refused: {e}"


def clear_attacks_in_simulator(attack_id: Optional[str] = None) -> Tuple[bool, str]:
    """Clear one or all attacks via red team REST API."""
    payload: Dict[str, Any] = {"attack_id": attack_id} if attack_id else {"clear_all": True}
    try:
        resp = requests.post(
            f"{TWIN_API_URL}/api/attack/clear",
            headers=get_red_team_headers(),
            json=payload,
            timeout=2.0,
        )
        if resp.status_code == 200:
            return True, "All attacks cleared successfully"
        return False, f"[ALERT] Clear failed: HTTP {resp.status_code}"
    except Exception as e:
        return False, f"[ALERT] Connection refused: {e}"


@st.cache_data(ttl=3)
def _fetch_geospatial_sensors_cached(db_path: str, mtime: float) -> pd.DataFrame:
    """Load latest sensor readings and compute normalised 3D heights."""
    cols = ["device_id", "zone_id", "device_type", "unit", "lat", "lon", "min_val", "max_val", "status"]
    if not os.path.isfile(db_path):
        return pd.DataFrame(columns=cols + ["broadcast_value", "true_value", "discrepancy", "zone_name", "attack_type", "elevation", "fill_color", "radius"])

    conn = None
    try:
        conn = get_db_connection(db_path)
        devices = conn.execute(
            "SELECT device_id, zone_id, device_type, unit, lat, lon, min_val, max_val, status FROM devices"
        ).fetchall()
        if not devices:
            return pd.DataFrame(columns=cols + ["broadcast_value", "true_value", "discrepancy", "zone_name", "attack_type", "elevation", "fill_color", "radius"])

        dev_list = [dict(d) for d in devices]
        df = pd.DataFrame(dev_list)

        # Get latest broadcast reading per device
        latest_rows = conn.execute(
            """
            SELECT r.device_id, r.true_value, r.broadcast_value, r.is_attacked, r.attack_type
            FROM readings_truth r
            INNER JOIN (
                SELECT device_id, MAX(id) AS max_id
                FROM readings_truth
                GROUP BY device_id
            ) latest ON r.id = latest.max_id
            """
        ).fetchall()
        readings_map = {r["device_id"]: dict(r) for r in latest_rows}

        true_vals = []
        vals = []
        is_atks = []
        atk_types = []
        zone_names = []
        discrepancies = []
        elevations = []
        colors = []
        radii = []

        for _, row in df.iterrows():
            did = row["device_id"]
            min_v = float(row["min_val"])
            max_v = float(row["max_val"])
            st_val = str(row["status"]).lower()

            r_info = readings_map.get(did, {})
            t_val = float(r_info.get("true_value", min_v))
            b_val = float(r_info.get("broadcast_value", min_v))
            is_atk = int(r_info.get("is_attacked", 0))
            atk_type = str(r_info.get("attack_type", "") or "")
            zone_name = ZONE_NAME_MAP.get(row["zone_id"], row["zone_id"])
            discrepancy = abs(b_val - t_val)

            # Normalised height formula: 50.0 + norm * 450.0 (Finding 14 & test compatibility)
            norm = 0.0
            if max_v > min_v:
                clamped = max(min_v, min(max_v, b_val))
                norm = (clamped - min_v) / (max_v - min_v)
            elevation = 50.0 + (norm * 450.0)

            # Colors (RGBA): nominal light neutral with alpha >= 200 (Finding 14)
            if is_atk or st_val == "tampered":
                c = [239, 68, 68, 230]      # Red (attacked)
            elif st_val in ("isolated", "quarantined"):
                c = [245, 158, 11, 230]     # Amber (contained)
            elif st_val == "refused":
                c = [168, 85, 247, 230]     # Purple (refused)
            else:
                c = [220, 226, 235, 215]     # Light neutral with alpha 215

            true_vals.append(t_val)
            vals.append(b_val)
            is_atks.append(is_atk)
            atk_types.append(atk_type)
            zone_names.append(zone_name)
            discrepancies.append(discrepancy)
            elevations.append(elevation)
            colors.append(c)
            radii.append(100.0)             # 100m radius (Finding 14)

        df["true_value"] = true_vals
        df["broadcast_value"] = vals
        df["is_attacked"] = is_atks
        df["attack_type"] = atk_types
        df["zone_name"] = zone_names
        df["discrepancy"] = discrepancies
        df["elevation"] = elevations
        df["fill_color"] = colors
        df["radius"] = radii
        df["halo_radius"] = [r * 1.5 for r in radii]
        return df
    except Exception as e:
        logger.warning(f"Error fetching geospatial sensors: {e}")
        return pd.DataFrame(columns=cols + ["broadcast_value", "true_value", "discrepancy", "zone_name", "attack_type", "elevation", "fill_color", "radius"])
    finally:
        if conn:
            conn.close()


def fetch_geospatial_sensors() -> pd.DataFrame:
    """Retrieve sensor telemetry formatted for PyDeck geospatial map."""
    return _fetch_geospatial_sensors_cached(DB_PATH, _get_db_mtime(DB_PATH))


def fetch_neighborhood_summaries(sensors_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Summarise sensor metrics and active threat state per municipal sector."""
    summaries = []
    for n in NEIGHBORHOODS:
        zid = n["zone_id"]
        z_df = sensors_df[sensors_df["zone_id"] == zid] if not sensors_df.empty else pd.DataFrame()
        total = len(z_df)
        active_threats = int(z_df["is_attacked"].sum()) if not z_df.empty else 0
        contained = len(z_df[z_df["status"].isin(["isolated", "quarantined"])]) if not z_df.empty else 0
        status = "threat" if active_threats > 0 else ("contained" if contained > 0 else "nominal")
        summaries.append({
            "zone_id": zid,
            "name": n["name"],
            "total_sensors": total,
            "active_threats": active_threats,
            "contained": contained,
            "status": status,
        })
    return summaries


def fetch_device_time_series(device_id: str, limit: int = 120) -> pd.DataFrame:
    """Retrieve recent ground truth vs broadcast telemetry records for device."""
    cols = ["ts", "true_value", "broadcast_value", "discrepancy", "is_attacked"]
    if not os.path.isfile(DB_PATH) or not device_id:
        return pd.DataFrame(columns=cols)

    conn = None
    try:
        conn = get_db_connection()
        rows = conn.execute(
            """
            SELECT ts, true_value, broadcast_value, is_attacked
            FROM readings_truth
            WHERE device_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (device_id, limit),
        ).fetchall()
        if not rows:
            return pd.DataFrame(columns=cols)

        data = [dict(r) for r in reversed(rows)]
        df = pd.DataFrame(data)
        df["discrepancy"] = (df["broadcast_value"] - df["true_value"]).abs()
        return df
    except Exception as e:
        logger.warning(f"Error fetching time series for {device_id}: {e}")
        return pd.DataFrame(columns=cols)
    finally:
        if conn:
            conn.close()


def fetch_truth_ledger(limit: int = 200, offset: int = 0) -> pd.DataFrame:
    """Retrieve audit rows from readings_truth ledger."""
    cols = ["id", "reading_id", "device_id", "ts", "true_value", "broadcast_value", "unit", "status", "is_attacked", "attack_type"]
    if not os.path.isfile(DB_PATH):
        return pd.DataFrame(columns=cols)

    conn = None
    try:
        conn = get_db_connection()
        rows = conn.execute(
            """
            SELECT id, reading_id, device_id, ts, true_value, broadcast_value, unit, status, is_attacked, attack_type
            FROM readings_truth
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
        if not rows:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame([dict(r) for r in rows])
    except Exception as e:
        logger.warning(f"Error querying readings_truth: {e}")
        return pd.DataFrame(columns=cols)
    finally:
        if conn:
            conn.close()


def normalise_elevation(val: float, min_val: float, max_val: float) -> float:
    """Calculate normalised 3D column height [50.0, 500.0] for sensor value."""
    if max_val <= min_val:
        return 50.0
    clamped = max(min_val, min(max_val, val))
    norm = (clamped - min_val) / (max_val - min_val)
    return 50.0 + (norm * 450.0)


def build_spatial_twin_deck(sensors_df: pd.DataFrame, map_pitch: int = 50) -> pdk.Deck:
    """Build PyDeck 3D visualization of Greater Tunis with column heights and offset zone labels."""
    view_state = pdk.ViewState(
        latitude=36.818,
        longitude=10.205,
        zoom=11.2,
        pitch=map_pitch,
        bearing=-15,
    )

    layers = []

    # 1. 3D Elevated Columns with visible height scale (Finding 14)
    if not sensors_df.empty:
        col_layer = pdk.Layer(
            "ColumnLayer",
            data=sensors_df,
            get_position=["lon", "lat"],
            get_elevation="elevation",
            elevation_scale=1.8,  # Tall clearly visible towers at default zoom
            radius=100,            # 100m radius
            get_fill_color="fill_color",
            pickable=True,
            auto_highlight=True,
            extruded=True,
        )
        layers.append(col_layer)

        # 2. Sensor base halos
        scatter_layer = pdk.Layer(
            "ScatterplotLayer",
            data=sensors_df,
            get_position=["lon", "lat"],
            get_radius="radius",
            get_fill_color="fill_color",
            pickable=False,
        )
        layers.append(scatter_layer)

        halo_layer = pdk.Layer(
            "ScatterplotLayer",
            data=sensors_df,
            get_position=["lon", "lat"],
            get_radius="halo_radius",
            get_fill_color=[255, 255, 255, 30],
            pickable=False,
        )
        layers.append(halo_layer)

    # 3. Municipal Sector Centroids with offset positions away from columns (Finding 7)
    zone_offsets = {
        "zone-1": {"lat": 36.814, "lon": 10.170},
        "zone-2": {"lat": 36.832, "lon": 10.292},
        "zone-3": {"lat": 36.872, "lon": 10.180},
        "zone-4": {"lat": 36.828, "lon": 10.102},
        "zone-5": {"lat": 36.852, "lon": 10.130},
        "zone-6": {"lat": 36.772, "lon": 10.205},
    }
    zone_records = []
    for z in NEIGHBORHOODS:
        pos = zone_offsets.get(z["zone_id"], {"lat": z["lat"] + 0.010, "lon": z["lon"] - 0.015})
        zone_records.append({
            "name": z["name"],
            "zone_id": z["zone_id"],
            "lat": pos["lat"],
            "lon": pos["lon"],
        })
    zone_df = pd.DataFrame(zone_records)

    text_layer = pdk.Layer(
        "TextLayer",
        data=zone_df,
        get_position=["lon", "lat"],
        get_text="name",
        get_size=13,
        get_color=[241, 245, 249, 230],
        get_angle=0,
        get_text_anchor="'start'",
        get_alignment_baseline="'bottom'",
        get_pixel_offset=[0, -20],
        pickable=False,
    )
    layers.append(text_layer)

    return pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-nolabels-gl-style/style.json",
        map_provider="carto",
        tooltip={
            "html": "<b>Device:</b> {device_id}<br/>"
                    "<b>Type:</b> {device_type}<br/>"
                    "<b>Value:</b> {broadcast_value} {unit}<br/>"
                    "<b>Status:</b> {status}",
            "style": {"backgroundColor": "#0b0f14", "color": "#e6edf3", "fontSize": "12px", "border": "1px solid #1e293b"}
        },
    )


# ==============================================================================
# UI Fragments & Tab Rendering
# ==============================================================================

# 6. Status bar and KPI tiles fragment (Finding 6)
@st.fragment(run_every=2)
def render_status_and_kpis(db_path: str):
    """Render auto-refreshing top status bar and 4 headline KPI tiles."""
    sb = _fetch_scoreboard_metrics_cached(db_path, _get_db_mtime(db_path))

    # Real state colors (Finding 10)
    twin_state_cls = "state-nominal" if sb["twin_api_online"] else "state-attack"
    twin_label = "Twin online" if sb["twin_api_online"] else "Twin API down"

    kafka_state = sb.get("kafka_state", "unknown")
    kafka_cls = "state-nominal" if kafka_state == "ok" else "state-attack"
    kafka_label = "Kafka ok" if kafka_state == "ok" else f"Kafka {kafka_state}"

    mcp_state = sb.get("mcp_state", "unknown")
    mcp_cls = "state-nominal" if mcp_state == "ok" else "state-attack"
    mcp_label = "MCP up" if mcp_state == "ok" else "MCP down"

    if not sb["twin_api_online"]:
        sim_time_display = "-"
        speed_display = "-"
        throughput_display = "-"
    else:
        sim_time_display = sb["latest_sim_time"]
        speed_display = f"{sb['sim_speed']:.0f}x"
        throughput_display = f"{sb['msgs_per_sec']:.1f} msg/s"

    # Status Bar (Finding 1 & 6)
    status_bar_html = f"""
    <div class="status-bar">
        <div class="item"><span class="lbl">Sim time:</span> <span class="val">{sim_time_display}</span></div>
        <div class="item"><span class="lbl">Speed:</span> <span class="val">{speed_display}</span></div>
        <div class="item"><span class="lbl">Throughput:</span> <span class="val">{throughput_display}</span></div>
        <div class="item"><span class="lbl">API:</span> <span class="val {twin_state_cls}">{twin_label}</span></div>
        <div class="item"><span class="lbl">Kafka:</span> <span class="val {kafka_cls}">{kafka_label}</span></div>
        <div class="item"><span class="lbl">MCP:</span> <span class="val {mcp_cls}">{mcp_label}</span></div>
        <div class="item"><span class="lbl">Recent callers:</span> <span class="val">{sb['recent_callers']}</span></div>
    </div>
    """
    st.markdown(status_bar_html, unsafe_allow_html=True)

    # 4 Headline KPI Tiles (No deltas, sentence-case)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Readings", f"{sb['total_readings']:,}")
    with col2:
        st.metric("Attacked readings", f"{sb['attacked_readings']:,}")
    with col3:
        st.metric("Active attacks", f"{sb['active_attacks']:,}")
    with col4:
        st.metric("Devices contained", f"{sb['contained_devices']:,}")


def render_sidebar():
    """Render red-team attack injection, physical events, and threat manifest forms."""
    st.sidebar.title("Red team adversary controls")

    # 1. Manual Threat Injection Form
    with st.sidebar.form("form_manual_attack"):
        st.subheader("Manual injection")
        capability = st.selectbox(
            "Adversary capability",
            [
                "device_physical",
                "device_key",
                "link",
                "rogue",
                "sensor_fault",
                "actuator",
            ],
            key="sb_attack_cap",
        )
        target_device = st.selectbox(
            "Target device",
            [
                "z1-tc1", "z1-tc2", "z1-wl1", "z1-wl2", "z1-pl1", "z1-aq1", "z1-ec1",
                "z2-tc1", "z2-tc2", "z2-wl1", "z2-wl2", "z2-pl1", "z2-aq1", "z2-ec1",
                "z3-tc1", "z3-tc2", "z3-wl1", "z3-wl2", "z3-pl1", "z3-aq1", "z3-ec1",
                "z4-tc1", "z4-tc2", "z4-wl1", "z4-wl2", "z4-pl1", "z4-aq1", "z4-ec1",
                "z5-tc1", "z5-tc2", "z5-wl1", "z5-wl2", "z5-pl1", "z5-aq1", "z5-ec1",
                "z6-tc1", "z6-tc2", "z6-wl1", "z6-wl2", "z6-pl1", "z6-aq1", "z6-ec1",
            ],
            key="sb_target_device",
        )
        spoofed_value = st.number_input("Spoofed value", value=185.0, step=5.0, key="sb_spoof_val")
        description = st.text_input("Description", value="Manual console exploit", key="sb_desc")
        submitted_attack = st.form_submit_button("Inject attack")

        if submitted_attack:
            payload = {
                "capability": capability,
                "target_device_id": target_device,
                "params": {"value": spoofed_value, "spoofed_value": spoofed_value},
                "description": description,
            }
            try:
                resp = requests.post(
                    f"{TWIN_API_URL}/api/attack/inject",
                    headers=get_red_team_headers(),
                    json=payload,
                    timeout=2.0,
                )
                if resp.status_code == 200:
                    st.success(f"Injected {capability} on {target_device}")
                else:
                    st.error(f"Injection failed: HTTP {resp.status_code} - {resp.text}")
            except Exception as e:
                st.error(f"Failed to communicate with red-team API: {e}")

    # 2. Physical Environmental Events Form
    with st.sidebar.form("form_physical_event"):
        st.subheader("Physical events")
        event_type = st.selectbox(
            "Event type",
            ["rain_storm", "traffic_accident", "fire", "grid_overload", "air_hazard"],
            key="sb_event_type",
        )
        event_zone = st.selectbox(
            "Target zone",
            ["zone-1", "zone-2", "zone-3", "zone-4", "zone-5", "zone-6"],
            key="sb_event_zone",
        )
        intensity = st.slider("Intensity multiplier", min_value=1.0, max_value=5.0, value=1.5, step=0.1, key="sb_intensity")
        duration_s = st.number_input("Duration (sim-seconds)", min_value=60.0, max_value=7200.0, value=1800.0, step=300.0, key="sb_duration")
        submitted_event = st.form_submit_button("Inject event")

        if submitted_event:
            payload = {
                "type": event_type,
                "zone_id": event_zone,
                "intensity": intensity,
                "duration_s": duration_s,
            }
            try:
                resp = requests.post(
                    f"{TWIN_API_URL}/api/event/inject",
                    headers=get_red_team_headers(),
                    json=payload,
                    timeout=2.0,
                )
                if resp.status_code == 200:
                    st.success(f"Started {event_type} in {event_zone}")
                else:
                    st.error(f"Event failed: HTTP {resp.status_code} - {resp.text}")
            except Exception as e:
                st.error(f"Failed to communicate with red-team API: {e}")

    # 3. Active Threats and Clearance
    st.sidebar.subheader("Active attacks and events")

    @st.fragment(run_every=3)
    def render_active_threats_fragment():
        active_attacks = fetch_active_attacks()
        active_events = fetch_active_physical_events()

        if not active_attacks and not active_events:
            st.caption("Zero active attacks or physical events.")
        else:
            for atk in active_attacks:
                aid = atk.get("attack_id")
                cap = atk.get("capability")
                tgt = atk.get("target_device_id") or atk.get("target_zone_id")
                col_a, col_b = st.columns([2.0, 1.5])
                with col_a:
                    st.caption(f"{cap} on {tgt}")
                with col_b:
                    if st.button("Clear", key=f"btn_clear_atk_{aid}"):
                        try:
                            requests.post(
                                f"{TWIN_API_URL}/api/attack/clear",
                                headers=get_red_team_headers(),
                                json={"attack_id": aid},
                                timeout=2.0,
                            )
                            st.rerun()
                        except Exception:
                            pass

            for ev in active_events:
                eid = ev.get("event_id")
                etype = ev.get("type") or ev.get("event_type")
                ezone = ev.get("zone_id")
                col_a, col_b = st.columns([2.0, 1.5])
                with col_a:
                    st.caption(f"{etype} in {ezone}")
                with col_b:
                    if st.button("Clear", key=f"btn_clear_ev_{eid}"):
                        try:
                            requests.post(
                                f"{TWIN_API_URL}/api/event/clear",
                                headers=get_red_team_headers(),
                                json={"event_id": eid},
                                timeout=2.0,
                            )
                            st.rerun()
                        except Exception:
                            pass

        if st.button("Clear all threats", key="btn_clear_all_threats"):
            try:
                requests.post(f"{TWIN_API_URL}/api/attack/clear", headers=get_red_team_headers(), json={"clear_all": True}, timeout=2.0)
                requests.post(f"{TWIN_API_URL}/api/event/clear", headers=get_red_team_headers(), json={"clear_all": True}, timeout=2.0)
                st.success("All attacks and events cleared.")
                st.rerun()
            except Exception as e:
                st.error(f"Error clearing threats: {e}")

    with st.sidebar:
        render_active_threats_fragment()


def render_timeline_tab(db_path: str):
    """Render Tab 1: Correlated Operational Timeline with dynamic auto-refresh fragment."""
    st.subheader("Operational timeline")
    st.caption("Correlated sequence of telemetry readings, Guardian containment actions, City Brain actuation commands, and actuator results with simulation timestamps.")

    # Fetch current simulation time for time window calculations (Finding 2)
    sb_metrics = _fetch_scoreboard_metrics_cached(db_path, _get_db_mtime(db_path))
    sim_now_ts = sb_metrics.get("latest_sim_time")

    # Top Controls
    col_win, col_zone, col_dev, col_live = st.columns([1.6, 1.2, 1.2, 1.0])

    with col_win:
        # Time window options (Finding 2 & 9)
        window_options = ["Last 5 sim-min", "Last 15 sim-min", "Last 60 sim-min", "Since a sim time"]
        window_choice = st.selectbox("Time window", window_options, index=2, key="tl_win_choice")

    since_sim_ts: Optional[str] = None
    window_sim_minutes: Optional[int] = None

    if window_choice in ("Last 5 sim-min", "Last 15 sim-min", "Last 60 sim-min"):
        window_sim_minutes = int(window_choice.split()[1])
        if sim_now_ts and sim_now_ts != "-":
            try:
                now_sec = parse_rfc3339_to_seconds(sim_now_ts)
                since_sec = now_sec - (window_sim_minutes * 60)
                since_sim_ts = datetime.fromtimestamp(since_sec, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                since_sim_ts = None
    elif window_choice == "Since a sim time":
        custom_ts = st.text_input("Enter simulation timestamp", value="2026-10-17T12:00:00Z", key="tl_custom_ts")
        if custom_ts:
            try:
                datetime.strptime(custom_ts.strip(), "%Y-%m-%dT%H:%M:%SZ")
                since_sim_ts = custom_ts.strip()
            except ValueError:
                st.error("Simulation timestamp must follow RFC 3339 format (%Y-%m-%dT%H:%M:%SZ)")
                since_sim_ts = None

    with col_zone:
        zone_filter = st.selectbox("Filter by zone", ["All zones"] + [n["zone_id"] for n in NEIGHBORHOODS], key="tl_zone_filter")

    with col_dev:
        dev_choices = ["All devices"]
        if os.path.isfile(db_path):
            try:
                conn = get_db_connection()
                d_rows = conn.execute("SELECT device_id FROM devices ORDER BY device_id").fetchall()
                dev_choices += [r["device_id"] for r in d_rows]
                conn.close()
            except Exception:
                pass
        device_filter = st.selectbox("Filter by device", dev_choices, key="tl_dev_filter")

    with col_live:
        # Follow live toggle: controls fragment run_every (Finding 6)
        follow_live = st.toggle("Follow live", value=st.session_state.get("follow_live", True), key="follow_live")

    # Fragment with run_every=2 when follow_live is True, or None when False
    tl_interval = 2 if follow_live else None

    @st.fragment(run_every=tl_interval)
    def render_timeline_fragment(
        db_p: str,
        w_min: Optional[int],
        s_ts: Optional[str],
        z_filt: str,
        d_filt: str,
        curr_sim_ts: Optional[str],
    ):
        # 1. Fetch Journal Entries (Finding 2 & 7: since_sim_ts used for Last N; strictly no fallback to ledger)
        journal, err = fetch_journal_entries(since_sim_ts=s_ts, limit=1000)
        if err:
            st.error(err)
            return

        journal_entries = journal or []

        # 2. Identify attack_inject rows to query the FIRST attacked reading from ledger (Finding 1 & 4)
        temp_attacks = []
        for idx, entry in enumerate(journal_entries):
            if entry.get("source") == "attack" and entry.get("detail", {}).get("action") == "inject":
                detail = entry.get("detail", {})
                tgt = detail.get("target_device_id")
                if not tgt and detail.get("capability") == "rogue":
                    tgt = detail.get("params", {}).get("rogue_device_id")
                if tgt:
                    temp_attacks.append(
                        TimelineRow(
                            row_id=entry.get("key", f"a:{idx}"),
                            sim_ts=str(entry.get("sim_ts", "")),
                            lane="Twin",
                            event_type="attack_inject",
                            device_id=tgt,
                            zone_id=detail.get("target_zone_id") or detail.get("params", {}).get("zone_id"),
                            summary="",
                        )
                    )

        first_attacked_readings = fetch_first_attacked_readings_for_attacks(db_p, temp_attacks)

        # 3. Optional partner logs (Finding 8: off unless TIMELINE_LOG_DIR is set)
        partner_logs = load_partner_log_entries(TIMELINE_LOG_DIR) if TIMELINE_LOG_DIR else []

        # 4. Build correlated 5-lane timeline rows (Finding 4: query real device to zone map from devices table)
        device_zone_map = fetch_device_zone_map(db_p)
        all_rows = build_timeline_rows(
            journal_entries=journal_entries,
            attacked_readings=first_attacked_readings,
            device_zone_map=device_zone_map,
            log_entries=partner_logs,
        )

        # 5. Apply user filters (Finding 2: pass current_sim_ts to exclude rows older than cutoff)
        filtered_rows = filter_timeline_rows(
            all_rows,
            window_sim_minutes=w_min,
            since_sim_ts=s_ts,
            zone_id=z_filt,
            device_id=d_filt,
            current_sim_ts=curr_sim_ts,
        )

        # 6. Separate Test Harness rows from primary 4 operational lanes
        operational_rows = [r for r in filtered_rows if r.lane != "Test harness"]
        harness_rows = [r for r in filtered_rows if r.lane == "Test harness"]

        if not operational_rows:
            st.info("No operational events observed in the selected time window.")
        else:
            # Incident chains summary list above table (Finding 5: newest first)
            chain_summaries_dict: Dict[str, str] = {}
            for r in operational_rows:
                if r.chain_id and r.chain_summary and r.chain_id not in chain_summaries_dict:
                    chain_summaries_dict[r.chain_id] = r.chain_summary

            if chain_summaries_dict:
                st.markdown("**Incident chains**")
                def chain_num_key(cid: str) -> int:
                    parts = cid.split("-")
                    return int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
                for cid in sorted(chain_summaries_dict.keys(), key=chain_num_key, reverse=True):
                    st.markdown(f"- **{cid}**: {chain_summaries_dict[cid]}")

            table_records = []
            for r in operational_rows:
                # Format time as HH:MM:SS for compact display; full RFC3339 ts remains in _raw JSON
                time_disp = r.sim_ts[11:19] if (r.sim_ts and len(r.sim_ts) >= 19 and "T" in r.sim_ts) else r.sim_ts
                table_records.append({
                    "Time": time_disp,
                    "Lane": r.lane,
                    "Event": r.event_type,
                    "Target": r.target,
                    "Reaction": r.formatted_reaction,
                    "Summary": r.summary,
                    "Chain": r.chain_id or "-",
                    "_raw": r.raw_json,
                })

            df_display = pd.DataFrame(table_records)

            # Table column configuration with proper widths at 1280px (Finding 5)
            # Fits within 818px content box without horizontal scrolling
            column_config = {
                "Time": st.column_config.TextColumn("Time", width=70),
                "Lane": st.column_config.TextColumn("Lane", width=60),
                "Event": st.column_config.TextColumn("Event", width=100),
                "Target": st.column_config.TextColumn("Target", width=55),
                "Reaction": st.column_config.TextColumn("Reaction", width=65),
                "Summary": st.column_config.TextColumn("Summary", width=340),
                "Chain": st.column_config.TextColumn("Chain", width=65),
                "_raw": None,
            }

            selection_event = st.dataframe(
                df_display,
                column_config=column_config,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key="tl_dataframe_table",
            )

            # Inspect selected row JSON payload (follow row_id across refreshes)
            selected_indices = getattr(selection_event.selection, "rows", [])
            if selected_indices:
                sel_idx = selected_indices[0]
                if 0 <= sel_idx < len(operational_rows):
                    st.session_state["selected_timeline_row_id"] = operational_rows[sel_idx].row_id

            sel_row_id = st.session_state.get("selected_timeline_row_id")
            if sel_row_id:
                matching_row = next((r for r in operational_rows if r.row_id == sel_row_id), None)
                if matching_row:
                    st.markdown("**Inspect selected event payload**")
                    st.json(matching_row.raw_json or {})
                else:
                    st.info("Selected row is outside the current window")

        # Test harness diagnostics expander (collapsed by default)
        if harness_rows:
            with st.expander(f"Test harness diagnostics ({len(harness_rows)} entries)"):
                st.caption("Diagnostic events originating from automated test runners.")
                h_records = [{
                    "Time": r.sim_ts,
                    "Lane": r.lane,
                    "Event": r.event_type,
                    "Target": r.target,
                    "Summary": r.summary,
                } for r in harness_rows]
                st.dataframe(pd.DataFrame(h_records), use_container_width=True, hide_index=True)

    render_timeline_fragment(db_path, window_sim_minutes, since_sim_ts, zone_filter, device_filter, sim_now_ts)


def render_city_map_tab(db_path: str):
    """Render Tab 2: 3D Tactical City Map with 5s auto-refresh fragment."""
    st.subheader("3D city map")
    st.caption("Geospatial visualization of Greater Tunis sensors with normalised column heights, color-coded health status, and sector centroids.")

    # Colored Map Legend Swatches (Finding 12 & 14)
    legend_html = """
    <div style="display: flex; gap: 1.5rem; margin-bottom: 0.8rem; font-size: 0.85rem;">
        <span><span class="legend-swatch swatch-nominal"></span>Nominal</span>
        <span><span class="legend-swatch swatch-contained"></span>Contained / isolated</span>
        <span><span class="legend-swatch swatch-attack"></span>Active attack</span>
        <span><span class="legend-swatch swatch-refused"></span>Refused command</span>
    </div>
    """
    st.markdown(legend_html, unsafe_allow_html=True)

    @st.fragment(run_every=5)
    def render_map_fragment(db_p: str):
        sensors_df = _fetch_geospatial_sensors_cached(db_p, _get_db_mtime(db_p))
        deck = build_spatial_twin_deck(sensors_df)
        st.pydeck_chart(deck, use_container_width=True)

    render_map_fragment(db_path)


def render_truth_vs_broadcast_tab(db_path: str):
    """Render Tab 3: Ground Truth vs Broadcast Telemetry Divergence Curves."""
    st.subheader("Truth vs broadcast divergence")
    st.caption("Comparison of true physical simulation metrics against broadcast wire telemetry.")

    if not os.path.isfile(db_path):
        st.info("No ledger records available.")
        return

    # Device selector grouped by zone
    conn = get_db_connection()
    devices = conn.execute("SELECT device_id, zone_id, device_type, unit FROM devices ORDER BY zone_id, device_id").fetchall()
    conn.close()

    if not devices:
        st.info("No registered devices found.")
        return

    dev_options = [f"{d['zone_id']} - {d['device_id']} ({d['device_type']})" for d in devices]
    default_idx = next((i for i, opt in enumerate(dev_options) if "z1-pl1" in opt), 0)
    sel_option = st.selectbox("Select device", dev_options, index=default_idx, key="ts_dev_select")
    selected_device_id = sel_option.split(" - ")[1].split(" ")[0]

    ts_df = fetch_device_time_series(selected_device_id, limit=150)
    if ts_df.empty:
        st.info(f"No telemetry samples recorded yet for {selected_device_id}.")
        return

    # Maximum discrepancy metric
    max_disc = ts_df["discrepancy"].max()
    curr_truth = ts_df["true_value"].iloc[-1]
    curr_broadcast = ts_df["broadcast_value"].iloc[-1]

    mcol1, mcol2, mcol3 = st.columns(3)
    with mcol1:
        st.metric("Current physical truth", f"{curr_truth:.2f}")
    with mcol2:
        st.metric("Current broadcast wire", f"{curr_broadcast:.2f}")
    with mcol3:
        st.metric("Maximum divergence", f"{max_disc:.2f}")

    # Altair time-series chart
    base = alt.Chart(ts_df).encode(x=alt.X("ts:T", title="Simulation time"))

    truth_line = base.mark_line(color="#22c55e", strokeWidth=2.5).encode(
        y=alt.Y("true_value:Q", title="Value"),
        tooltip=["ts", "true_value", "broadcast_value", "discrepancy"],
    )

    broadcast_line = base.mark_line(color="#3b82f6", strokeDash=[4, 4], strokeWidth=2).encode(
        y=alt.Y("broadcast_value:Q"),
        tooltip=["ts", "true_value", "broadcast_value", "discrepancy"],
    )

    divergence_area = base.mark_area(color="#ef4444", opacity=0.2).encode(
        y=alt.Y("true_value:Q"),
        y2=alt.Y2("broadcast_value:Q"),
    )

    chart = (divergence_area + truth_line + broadcast_line).properties(height=380).interactive()
    st.altair_chart(chart, use_container_width=True)


def render_ledger_tab(db_path: str):
    """Render Tab 4: Read-Only Forensic Ledger Audit Trail with auto-refresh fragment."""
    st.subheader("Ground truth ledger")
    st.caption("Immutable forensic audit trail directly queried from read-only SQLite storage.")

    @st.fragment(run_every=5)
    def render_ledger_fragment(db_p: str):
        df_ledger = fetch_truth_ledger(limit=250)
        if df_ledger.empty:
            st.info("Ground truth database is empty or offline.")
            return

        st.dataframe(
            df_ledger,
            use_container_width=True,
            hide_index=True,
            column_config={
                "id": st.column_config.NumberColumn("ID", width="small"),
                "reading_id": st.column_config.TextColumn("Reading ID", width="medium"),
                "device_id": st.column_config.TextColumn("Device", width="small"),
                "ts": st.column_config.TextColumn("Timestamp", width="medium"),
                "true_value": st.column_config.NumberColumn("True value", format="%.2f"),
                "broadcast_value": st.column_config.NumberColumn("Broadcast value", format="%.2f"),
                "unit": st.column_config.TextColumn("Unit", width="small"),
                "status": st.column_config.TextColumn("Status", width="small"),
                "is_attacked": st.column_config.CheckboxColumn("Attacked", width="small"),
                "attack_type": st.column_config.TextColumn("Attack type", width="medium"),
            },
        )

    render_ledger_fragment(db_path)


def render_app():
    """Main application layout and execution flow."""
    # Inject Custom CSS
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    # Render Sidebar Controls
    render_sidebar()

    # Main Area Visible Heading
    st.title("Twin operations console")

    # Render Top Status Bar & 4 KPI Tiles (auto-refreshing fragment)
    render_status_and_kpis(DB_PATH)

    # Four Main Operational Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["Timeline", "City map", "Truth vs broadcast", "Ledger"])

    with tab1:
        render_timeline_tab(DB_PATH)

    with tab2:
        render_city_map_tab(DB_PATH)

    with tab3:
        render_truth_vs_broadcast_tab(DB_PATH)

    with tab4:
        render_ledger_tab(DB_PATH)


# Execute render_app if running under Streamlit or as main script
if get_script_run_ctx() is not None or __name__ == "__main__":
    render_app()
