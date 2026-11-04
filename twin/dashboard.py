"""City Brain Convergence - 3D Digital Twin & Adversarial Operations Console.

Assigned to: Agent_UI
Fulfills: Challenge Rule R3 (3D Geospatial Digital Twin, Real-Time Timeline, & Red Team Controls).
Provides interactive 3D spatial visualization of all 42 IoT sensors across 6 Tunis neighborhoods,
real-time telemetry divergence charts, live Truth Ledger audits, and red team exploit injection.
Zero emoji usage; military/enterprise-grade cybersecurity operations interface.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

import altair as alt
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

# ==============================================================================
# Page Configuration & Professional Cyber Operations Styling
# ==============================================================================
st.set_page_config(
    page_title="City Brain // Digital Twin Operations Console",
    page_icon="[TWIN]",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom High-End Cyber Operations Styling (Zero Emojis, Sleek Aesthetics)
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Inter:wght@300;400;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .stApp {
        background: radial-gradient(circle at 50% 5%, #0c1427 0%, #050811 100%);
        color: #e2e8f0;
    }

    /* Top Command Header */
    .command-header {
        font-family: 'JetBrains Mono', monospace;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #00e5ff;
        font-size: 1.4rem;
        font-weight: 700;
        margin-bottom: 2px;
        text-shadow: 0 0 12px rgba(0, 229, 255, 0.4);
    }
    .command-subtitle {
        font-family: 'JetBrains Mono', monospace;
        color: #64748b;
        font-size: 0.78rem;
        letter-spacing: 0.05em;
        margin-bottom: 14px;
    }

    /* Metric Cards */
    div[data-testid="stMetric"] {
        background: rgba(12, 22, 43, 0.7);
        border: 1px solid rgba(0, 229, 255, 0.22);
        border-radius: 8px;
        padding: 12px 18px;
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.4);
        backdrop-filter: blur(12px);
        transition: all 0.25s ease-in-out;
    }
    div[data-testid="stMetric"]:hover {
        border-color: rgba(0, 229, 255, 0.6);
        box-shadow: 0 6px 25px rgba(0, 229, 255, 0.2);
        transform: translateY(-1px);
    }
    div[data-testid="stMetricLabel"] {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem !important;
        letter-spacing: 0.06em;
        color: #94a3b8 !important;
        text-transform: uppercase;
    }
    div[data-testid="stMetricValue"] {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.45rem !important;
        font-weight: 700;
        color: #f8fafc !important;
    }

    /* Threat Status Badges */
    .badge-nominal {
        font-family: 'JetBrains Mono', monospace;
        color: #00e676;
        font-weight: 600;
        background-color: rgba(0, 230, 118, 0.12);
        padding: 3px 8px;
        border-radius: 4px;
        border: 1px solid rgba(0, 230, 118, 0.4);
        font-size: 0.75rem;
        letter-spacing: 0.05em;
        display: inline-block;
    }
    .badge-critical {
        font-family: 'JetBrains Mono', monospace;
        color: #ff2a5f;
        font-weight: 700;
        background-color: rgba(255, 42, 95, 0.18);
        padding: 3px 8px;
        border-radius: 4px;
        border: 1px solid #ff2a5f;
        font-size: 0.75rem;
        letter-spacing: 0.05em;
        display: inline-block;
        animation: pulse-border 1.8s infinite;
    }
    @keyframes pulse-border {
        0% { box-shadow: 0 0 0 0 rgba(255, 42, 95, 0.6); }
        70% { box-shadow: 0 0 0 8px rgba(255, 42, 95, 0); }
        100% { box-shadow: 0 0 0 0 rgba(255, 42, 95, 0); }
    }

    /* Neighborhood Cards */
    .neighborhood-card {
        background: rgba(13, 23, 44, 0.65);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 8px;
        padding: 14px 16px;
        margin-bottom: 12px;
        backdrop-filter: blur(10px);
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
        transition: border-color 0.2s ease;
    }
    .neighborhood-card:hover {
        border-color: rgba(0, 229, 255, 0.45);
    }
    .neighborhood-card-alert {
        background: rgba(40, 14, 25, 0.75);
        border: 1px solid rgba(255, 42, 95, 0.6);
        border-radius: 8px;
        padding: 14px 16px;
        margin-bottom: 12px;
        backdrop-filter: blur(10px);
        box-shadow: 0 4px 22px rgba(255, 42, 95, 0.25);
        animation: pulse-border 2s infinite;
    }

    /* Live Security Feed Terminal */
    .terminal-box {
        background-color: #070c18;
        color: #38bdf8;
        font-family: 'JetBrains Mono', monospace;
        padding: 14px;
        border-radius: 6px;
        border: 1px solid rgba(0, 229, 255, 0.25);
        height: 300px;
        overflow-y: scroll;
        font-size: 0.8rem;
        line-height: 1.55;
        box-shadow: inset 0 2px 10px rgba(0, 0, 0, 0.7);
    }
    .terminal-entry-alert {
        color: #ff477e;
        background: rgba(255, 42, 95, 0.08);
        padding: 3px 6px;
        border-left: 3px solid #ff2a5f;
        margin-bottom: 4px;
    }
    .terminal-entry-info {
        color: #00e676;
        padding: 2px 6px;
        margin-bottom: 4px;
    }

    /* Engine Status Banner */
    .engine-banner {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.76rem;
        letter-spacing: 0.05em;
        padding: 6px 12px;
        border-radius: 6px;
        margin-bottom: 14px;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .engine-banner-online {
        background: rgba(0, 230, 118, 0.1);
        border: 1px solid rgba(0, 230, 118, 0.4);
        color: #00e676;
    }
    .engine-banner-local {
        background: rgba(0, 229, 255, 0.1);
        border: 1px solid rgba(0, 229, 255, 0.4);
        color: #00e5ff;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Environment and Path Resolution
DB_PATH = os.getenv("GROUND_TRUTH_DB_PATH", "ground_truth.db")
TWIN_API_URL = os.getenv("TWIN_API_URL", "http://localhost:8000")

# Verified Real-World Coordinates for Greater Tunis (Dry Land, Zero Water Collision)
NEIGHBORHOODS = [
    {"zone_id": "zone-1", "name": "Downtown Financial", "lat": 36.8000, "lon": 10.1800, "color": [0, 229, 255]},
    {"zone_id": "zone-2", "name": "Port & Coastal Harbor", "lat": 36.8050, "lon": 10.2700, "color": [0, 153, 255]},
    {"zone_id": "zone-3", "name": "North Industrial Park", "lat": 36.8480, "lon": 10.2050, "color": [255, 171, 0]},
    {"zone_id": "zone-4", "name": "West Residential Hills", "lat": 36.8150, "lon": 10.1300, "color": [0, 230, 118]},
    {"zone_id": "zone-5", "name": "Central University & Med", "lat": 36.8320, "lon": 10.1550, "color": [179, 136, 255]},
    {"zone_id": "zone-6", "name": "South Logistics Hub", "lat": 36.7550, "lon": 10.2200, "color": [255, 110, 64]},
]
ZONE_NAME_MAP = {z["zone_id"]: z["name"] for z in NEIGHBORHOODS}


# ==============================================================================
# Database Access Functions (Strictly Read-Only)
# ==============================================================================
def get_db_connection() -> Optional[sqlite3.Connection]:
    """Open a read-only SQLite connection to ground_truth.db."""
    if not os.path.exists(DB_PATH):
        return None
    try:
        abs_path = os.path.abspath(DB_PATH)
        conn = sqlite3.connect(f"file:{abs_path}?mode=ro", uri=True, timeout=3.0)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        st.error(f"Failed to connect to ground-truth database (read-only): {e}")
        return None


def fetch_scoreboard_metrics() -> Dict[str, Any]:
    """Fetch live counts for the header scoreboard."""
    conn = get_db_connection()
    if not conn:
        return {
            "total_readings": 0,
            "active_incidents": 0,
            "active_attacks": 0,
            "latest_sim_time": "Connecting...",
        }
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS total FROM readings_truth")
        total_readings = cur.fetchone()["total"]

        cur.execute("SELECT COUNT(*) AS total FROM readings_truth WHERE is_attacked = 1")
        active_incidents = cur.fetchone()["total"]

        cur.execute("SELECT COUNT(*) AS total FROM attacks_injected WHERE active = 1")
        active_attacks = cur.fetchone()["total"]

        cur.execute("SELECT ts FROM readings_truth ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        latest_sim_time = row["ts"] if row else "N/A"

        return {
            "total_readings": total_readings,
            "active_incidents": active_incidents,
            "active_attacks": active_attacks,
            "latest_sim_time": latest_sim_time,
        }
    except Exception as e:
        return {
            "total_readings": 0,
            "active_incidents": 0,
            "active_attacks": 0,
            "latest_sim_time": f"Error: {e}",
        }
    finally:
        conn.close()


def fetch_active_attacks() -> List[Dict[str, Any]]:
    """Fetch all active adversary capabilities from attacks_injected table."""
    conn = get_db_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM attacks_injected WHERE active = 1 ORDER BY start_sim_time DESC")
        return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


def fetch_geospatial_sensors() -> pd.DataFrame:
    """Fetch all 42 registered IoT sensors cross-referenced with active attack manifest."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    try:
        query = """
            SELECT d.device_id, d.zone_id, d.device_type, d.unit, d.lat, d.lon,
                   COALESCE(r.true_value, 0.0) AS true_value,
                   COALESCE(r.broadcast_value, 0.0) AS broadcast_value,
                   COALESCE(r.is_attacked, 0) AS is_attacked,
                   COALESCE(r.attack_type, 'none') AS attack_type,
                   COALESCE(r.ts, 'N/A') AS ts
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
        if not df.empty:
            df["zone_name"] = df["zone_id"].map(ZONE_NAME_MAP).fillna(df["zone_id"])

            # Cross-reference with active attacks from attacks_injected table
            active_atks = fetch_active_attacks()
            targeted_devices = {a["target_device_id"]: a for a in active_atks if a.get("target_device_id")}
            targeted_zones = {a["target_zone_id"]: a for a in active_atks if a.get("target_zone_id")}

            def evaluate_discrepancy(r: pd.Series) -> Tuple[bool, int, str]:
                dev_id = str(r["device_id"])
                zone_id = str(r["zone_id"])
                
                # Check direct or zone attack
                if dev_id in targeted_devices:
                    atk = targeted_devices[dev_id]
                    return True, 1, str(atk.get("capability", "exploit"))
                if zone_id in targeted_zones:
                    atk = targeted_zones[zone_id]
                    return True, 1, str(atk.get("capability", "zone_exploit"))

                # Check readings_truth recorded discrepancy
                has_history_attack = bool(r["is_attacked"]) or abs(float(r["true_value"]) - float(r["broadcast_value"])) > 1e-3
                return has_history_attack, 1 if has_history_attack else 0, str(r["attack_type"])

            evals = [evaluate_discrepancy(row) for _, row in df.iterrows()]
            df["discrepancy"] = [e[0] for e in evals]
            df["is_attacked"] = [e[1] for e in evals]
            df["attack_type"] = [e[2] for e in evals]

        return df
    except Exception as e:
        st.warning(f"Error fetching sensor spatial telemetry: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def fetch_neighborhood_summaries(sensor_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Compute aggregate real-time health metrics per neighborhood sector."""
    active_atks = fetch_active_attacks()
    summaries = []
    for n in NEIGHBORHOODS:
        zid = n["zone_id"]
        zname = n["name"]
        z_df = sensor_df[sensor_df["zone_id"] == zid] if not sensor_df.empty else pd.DataFrame()

        total_sensors = len(z_df)
        
        # Cross-reference active attacks targeting this zone or its devices
        zone_active_atks = [
            a for a in active_atks
            if a.get("target_zone_id") == zid
            or (a.get("target_device_id") and a.get("target_device_id").startswith(f"z{zid.split('-')[-1]}-"))
        ]
        
        discrepant_count = len(z_df[z_df["discrepancy"] == True]) if not z_df.empty else 0
        active_threat_count = max(discrepant_count, len(zone_active_atks))

        traffic_avg = z_df[z_df["device_type"] == "traffic_camera"]["broadcast_value"].mean() if not z_df.empty else 0.0
        water_max = z_df[z_df["device_type"] == "water_level"]["broadcast_value"].max() if not z_df.empty else 0.0
        power_sum = z_df[z_df["device_type"] == "power_load"]["broadcast_value"].sum() if not z_df.empty else 0.0
        aqi_avg = z_df[z_df["device_type"] == "air_quality"]["broadcast_value"].mean() if not z_df.empty else 0.0

        summaries.append({
            "zone_id": zid,
            "name": zname,
            "lat": n["lat"],
            "lon": n["lon"],
            "total_sensors": total_sensors,
            "active_threats": active_threat_count,
            "traffic_avg": traffic_avg if pd.notna(traffic_avg) else 0.0,
            "water_max": water_max if pd.notna(water_max) else 0.0,
            "power_sum": power_sum if pd.notna(power_sum) else 0.0,
            "aqi_avg": aqi_avg if pd.notna(aqi_avg) else 0.0,
            "status": "threat" if active_threat_count > 0 else "nominal",
        })
    return summaries


def fetch_device_time_series(device_id: str, limit: int = 60) -> pd.DataFrame:
    """Fetch historical readings for a specific device to plot divergence curves."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    try:
        query = """
            SELECT ts, seq, true_value, broadcast_value, is_attacked, attack_type
            FROM readings_truth
            WHERE device_id = ?
            ORDER BY id DESC
            LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(device_id, limit))
        if not df.empty:
            df = df.iloc[::-1].reset_index(drop=True)
            df["discrepancy"] = df["broadcast_value"] - df["true_value"]
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def fetch_registered_devices() -> List[str]:
    """Fetch list of device IDs registered in the ledger."""
    conn = get_db_connection()
    fallback_devices = [f"z{z}-{t}" for z in range(1, 7) for t in ["tc1", "tc2", "wl1", "wl2", "pl1", "aq1", "ec1"]]
    if not conn:
        return fallback_devices
    try:
        cur = conn.cursor()
        cur.execute("SELECT device_id FROM devices WHERE device_id LIKE 'z%' ORDER BY device_id ASC")
        rows = cur.fetchall()
        if rows:
            return [r["device_id"] for r in rows]
        return fallback_devices
    except Exception:
        return fallback_devices
    finally:
        conn.close()


def fetch_truth_ledger(limit: int = 25) -> pd.DataFrame:
    """Fetch last N rows from readings_truth table."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    try:
        query = """
            SELECT reading_id, device_id, ts, seq, true_value, broadcast_value,
                   unit, attack_type, is_attacked, status
            FROM readings_truth
            ORDER BY id DESC
            LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(limit,))
        return df
    except Exception as e:
        st.warning(f"Waiting for readings in ground_truth.db: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def fetch_timeline_events(limit: int = 40) -> List[Dict[str, Any]]:
    """Fetch recent timeline events (sim time, device readings, and attacks)."""
    conn = get_db_connection()
    if not conn:
        return []
    try:
        query = """
            SELECT ts, device_id, true_value, broadcast_value, unit, attack_type, is_attacked, status
            FROM readings_truth
            ORDER BY id DESC
            LIMIT ?
        """
        cur = conn.cursor()
        cur.execute(query, (limit,))
        return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


# ==============================================================================
# Simulation Engine Coordinator (Dual Mode: REST Daemon or Standalone)
# ==============================================================================
def check_twin_daemon_online() -> bool:
    """Check if the twin.main uvicorn daemon is actively listening on port 8000."""
    try:
        resp = requests.get(f"{TWIN_API_URL}/api/status", timeout=0.8)
        return resp.status_code == 200
    except Exception:
        return False


def advance_simulation_ticks(ticks_count: int = 5) -> int:
    """Advance local simulator ticks to produce fresh readings in standalone mode."""
    try:
        from twin.mcp_server import get_active_simulator
        sim = get_active_simulator()
        if sim:
            for _ in range(ticks_count):
                sim.tick()
            return ticks_count
    except Exception:
        pass
    return 0


def inject_attack_to_simulator(
    capability: str,
    target_device_id: Optional[str] = None,
    target_zone_id: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None,
) -> Tuple[bool, str]:
    """Interface with running simulation.py to inject adversary capability."""
    payload = {
        "capability": capability,
        "target_device_id": target_device_id,
        "target_zone_id": target_zone_id,
        "params": params or {},
        "description": description or f"Injected from Red Team Console: {capability}",
    }

    # 1. Attempt internal HTTP REST endpoint
    try:
        resp = requests.post(f"{TWIN_API_URL}/api/attack/inject", json=payload, timeout=2.0)
        if resp.status_code == 200:
            data = resp.json()
            return True, f"Injected attack {data.get('attack_id')} ({capability}) on {target_device_id or target_zone_id}"
    except Exception:
        pass

    # 2. Standalone in-process simulation runner
    try:
        from twin.mcp_server import get_active_simulator
        sim = get_active_simulator()
        if sim:
            aid = sim.inject_attack(
                capability=capability,
                target_device_id=target_device_id,
                target_zone_id=target_zone_id,
                params=params,
                description=description,
            )
            # Advance 5 simulation ticks immediately so ground_truth.db gets the attacked readings
            for _ in range(5):
                sim.tick()
            return True, f"Injected attack {aid} ({capability}) into local simulation engine [5 Ticks Generated]."
    except Exception as e:
        return False, f"Failed to inject attack: {e}"

    return False, "Failed to connect to simulation engine."


def clear_attacks_in_simulator(attack_id: Optional[str] = None) -> Tuple[bool, str]:
    """Interface with running simulation.py to clear active attacks."""
    payload = {"attack_id": attack_id or "all"}
    try:
        resp = requests.post(f"{TWIN_API_URL}/api/attack/clear", json=payload, timeout=2.0)
        if resp.status_code == 200:
            return True, "Active attacks cleared successfully."
    except Exception:
        pass

    try:
        from twin.mcp_server import get_active_simulator
        sim = get_active_simulator()
        if sim:
            if attack_id and attack_id != "all":
                sim.clear_attack(attack_id)
            else:
                sim.clear_all_attacks()
            # Advance 3 ticks to restore nominal baseline in database
            for _ in range(3):
                sim.tick()
            return True, "Cleared active attacks in local simulation engine [Nominal Baseline Restored]."
    except Exception as e:
        return False, f"Failed to clear attacks: {e}"

    return False, "Failed to clear attacks."


# ==============================================================================
# Streamlit UI Rendering (Professional Tactical Layout, Zero Emojis)
# ==============================================================================

daemon_online = check_twin_daemon_online()

# 1. Top Command Header
col_title, col_controls = st.columns([3, 1])

with col_title:
    st.markdown("<div class='command-header'>CITY BRAIN CONVERGENCE // DIGITAL TWIN OPERATIONS CONSOLE</div>", unsafe_allow_html=True)
    st.markdown("<div class='command-subtitle'>LAYER 1 TWIN // 3D GEOSPATIAL TELEMETRY, PHYSICAL GROUND-TRUTH & THREAT MATRIX</div>", unsafe_allow_html=True)

with col_controls:
    is_testing = os.getenv("STREAMLIT_TESTING", "0") == "1"
    auto_refresh = st.toggle("Auto-Refresh (1s)", value=not is_testing)
    if st.button("REFRESH TELEMETRY", use_container_width=True):
        st.rerun()

# Engine Status Banner
if daemon_online:
    st.markdown(
        "<div class='engine-banner engine-banner-online'>"
        "<span>[SYSTEM STATUS: ONLINE] // DISTRIBUTED TWIN_LAYER CONNECTED (10 Hz KAFKA + MCP SERVER)</span>"
        "<span>PORT: 8000 | TOPIC: twin.sensors</span>"
        "</div>",
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        "<div class='engine-banner engine-banner-local'>"
        "<span>[SYSTEM STATUS: AUTONOMOUS] // LOCAL SIMULATION RUNTIME ACTIVE (STANDALONE TESTING MODE)</span>"
        "<span>READY FOR RED TEAM THREAT INJECTION</span>"
        "</div>",
        unsafe_allow_html=True,
    )

# 2. Scoreboard Metrics
metrics = fetch_scoreboard_metrics()
m_col1, m_col2, m_col3, m_col4 = st.columns(4)

with m_col1:
    st.metric(
        label="Total Telemetry Frames",
        value=f"{metrics['total_readings']:,}",
        help="Cumulative ground-truth sensor telemetry frames recorded in SQLite.",
    )

with m_col2:
    st.metric(
        label="Active Incidents / Tampered",
        value=f"{metrics['active_incidents']:,}",
        delta=f"{metrics['active_incidents']} Discrepancies" if metrics['active_incidents'] > 0 else "Nominal State",
        delta_color="inverse" if metrics['active_incidents'] > 0 else "normal",
        help="Total telemetry cycles where physical ground truth deviated from broadcast stream.",
    )

with m_col3:
    st.metric(
        label="Active Red Team Attacks",
        value=metrics["active_attacks"],
        delta="Threat Active" if metrics["active_attacks"] > 0 else "Zero Threats",
        delta_color="inverse" if metrics["active_attacks"] > 0 else "off",
        help="Active adversary capabilities being dynamically injected into the physics engine.",
    )

with m_col4:
    st.metric(
        label="Simulation Clock (UTC)",
        value=metrics["latest_sim_time"],
        help="Strict RFC 3339 UTC Simulation Clock advancing at 10 Hz (Twin Layer Contract).",
    )

st.markdown("<hr style='border: 0; border-top: 1px solid rgba(255,255,255,0.08); margin: 12px 0 18px 0;'/>", unsafe_allow_html=True)


# ==============================================================================
# Sidebar: Red Team Threat Injection Matrix
# ==============================================================================
st.sidebar.markdown("<h3 style='font-family: JetBrains Mono; color: #00e5ff; letter-spacing: 0.05em;'>RED TEAM THREAT MATRIX</h3>", unsafe_allow_html=True)
st.sidebar.caption("Dynamically inject adversary capabilities to validate Layer 2 (City Brain) and Layer 3 (Guardian) resilience.")

# Manual Simulation Clock Step
if not daemon_online:
    st.sidebar.markdown("---")
    st.sidebar.markdown("<b style='font-family: JetBrains Mono; font-size: 0.8rem; color: #94a3b8;'>SIMULATION CLOCK CONTROLS</b>", unsafe_allow_html=True)
    if st.sidebar.button("PULSE 10 TICKS (+1.0s)", use_container_width=True):
        count = advance_simulation_ticks(10)
        st.sidebar.success(f"Generated {count * 42} sensor frames across 42 devices.")
        st.rerun()

# ------------------------------------------------------------------------------
# Hackathon Pitch Demo Presets (One-Click Scenarios)
# ------------------------------------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.markdown("<b style='font-family: JetBrains Mono; font-size: 0.8rem; color: #00e5ff;'>HACKATHON DEMO PRESETS</b>", unsafe_allow_html=True)
st.sidebar.caption("Instant one-click exploit deployment for hackathon pitch demonstration:")

demo_preset_options = [
    "Preset 1: Physical Spoof (z1-tc1 = 185 veh/min in Downtown)",
    "Preset 2: Compromised Key Forgery (z3-pl1 = 112 MW in Industrial)",
    "Preset 3: Link Signature Corruption (z2-wl1 in Harbor)",
    "Preset 4: Unauthorized Rogue Transmitter (rogue-x9 in West Hills)",
    "Preset 5: Sensor Drift Fault (z5-aq1 AQI +2.5/s in University)",
    "Preset 6: Actuator Non-Execution (z1-wl1 Valve Lock)",
]

selected_demo = st.sidebar.selectbox("Select Pitch Demo", options=demo_preset_options, index=0)

if st.sidebar.button("LAUNCH SELECTED DEMO", type="primary", use_container_width=True):
    succ = False
    msg = ""
    if "Preset 1" in selected_demo:
        succ, msg = inject_attack_to_simulator(
            capability="device_physical",
            target_device_id="z1-tc1",
            params={"spoofed_value": 185.0},
            description="Pitch Demo: Physical hardware spoofing on Downtown traffic camera (185 veh/min)",
        )
    elif "Preset 2" in selected_demo:
        succ, msg = inject_attack_to_simulator(
            capability="device_key",
            target_device_id="z3-pl1",
            params={"spoofed_value": 112.5},
            description="Pitch Demo: Compromised Ed25519 key signed forgery on Industrial substation (112.5 MW)",
        )
    elif "Preset 3" in selected_demo:
        succ, msg = inject_attack_to_simulator(
            capability="link",
            target_device_id="z2-wl1",
            params={"link_mode": "corrupt_bytes"},
            description="Pitch Demo: Link layer signature corruption on Harbor water level sensor",
        )
    elif "Preset 4" in selected_demo:
        succ, msg = inject_attack_to_simulator(
            capability="rogue",
            params={"rogue_device_id": "rogue-x9", "value": 888.0, "zone_id": "zone-4"},
            description="Pitch Demo: Rogue transmitter injecting unauthorized frames into West Residential Hills",
        )
    elif "Preset 5" in selected_demo:
        succ, msg = inject_attack_to_simulator(
            capability="sensor_fault",
            target_device_id="z5-aq1",
            params={"fault_mode": "drift", "drift_rate": 2.5},
            description="Pitch Demo: Sensor drift fault on Central University AQI sensor (+2.5 AQI/s)",
        )
    elif "Preset 6" in selected_demo:
        succ, msg = inject_attack_to_simulator(
            capability="actuator",
            target_device_id="z1-wl1",
            target_zone_id="zone-1",
            params={"suppress_actuation": True},
            description="Pitch Demo: Actuator silent command disobedience on Downtown flood valve",
        )
    if succ:
        st.sidebar.success(msg)
        time.sleep(0.4)
        st.rerun()
    else:
        st.sidebar.error(msg)

st.sidebar.markdown("---")
st.sidebar.markdown("<b style='font-family: JetBrains Mono; font-size: 0.8rem; color: #94a3b8;'>MANUAL THREAT MATRIX</b>", unsafe_allow_html=True)
devices_list = fetch_registered_devices()

selected_device = st.sidebar.selectbox(
    "Target Sensor Identifier",
    options=devices_list,
    index=0 if devices_list else None,
    help="Select the IoT device to target with the adversary capability.",
)

# Derive zone from device_id
device_zone = "zone-1"
if selected_device and selected_device.startswith("z"):
    try:
        device_zone = f"zone-{selected_device[1]}"
    except Exception:
        device_zone = "zone-1"

st.sidebar.caption(f"Sector: **{ZONE_NAME_MAP.get(device_zone, device_zone)}** (<code>{device_zone}</code>)")

st.sidebar.markdown("<b style='font-family: JetBrains Mono; font-size: 0.8rem; color: #94a3b8;'>ADVERSARY CAPABILITY MODEL</b>", unsafe_allow_html=True)

capability_options = [
    ("link", "1. Link Layer (Drop / Corrupt Sig / Replay)"),
    ("rogue", "2. Rogue Device (Unauthorized Broadcast)"),
    ("device_key", "3. Compromised Key (Signed Forgery)"),
    ("device_physical", "4. Physical Hardware Spoof (Sensor Tampering)"),
    ("actuator", "5. Actuator Non-Execution (Silent Disobedience)"),
    ("sensor_fault", "6. Hardware Sensor Fault (Drift / Stuck / Dropout)"),
]

selected_cap = st.sidebar.selectbox(
    "Select Capability Trigger",
    options=[c[0] for c in capability_options],
    format_func=lambda x: dict(capability_options)[x],
    label_visibility="collapsed",
)

# Dynamic Parameter Configuration based on Capability
attack_params: Dict[str, Any] = {}
attack_desc = ""

if selected_cap == "link":
    st.sidebar.markdown("<b style='font-size: 0.82rem; color: #38bdf8;'>Link Layer Network Interception</b>", unsafe_allow_html=True)
    link_mode = st.sidebar.selectbox(
        "Interception Mode",
        options=["corrupt_bytes", "drop", "replay"],
        index=0,
        help="'corrupt_bytes': corrupts Ed25519 signature. 'drop': drops packet. 'replay': resends stale packet.",
    )
    attack_params = {"link_mode": link_mode}
    attack_desc = f"Link layer attack on {selected_device} ({link_mode})"

elif selected_cap == "rogue":
    st.sidebar.markdown("<b style='font-size: 0.82rem; color: #38bdf8;'>Rogue Unauthorized Transmitter</b>", unsafe_allow_html=True)
    rogue_id = st.sidebar.text_input("Rogue Device ID", value="rogue-x1")
    rogue_val = st.sidebar.number_input("Spoofed Reading Value", value=999.0, step=10.0)
    rogue_zone = st.sidebar.selectbox("Target Sector", options=[f"zone-{i}" for i in range(1, 7)], index=0)
    attack_params = {"rogue_device_id": rogue_id, "value": rogue_val, "zone_id": rogue_zone}
    attack_desc = f"Rogue transmitter {rogue_id} injecting unauthorized telemetry into {rogue_zone}"

elif selected_cap == "device_key":
    st.sidebar.markdown("<b style='font-size: 0.82rem; color: #38bdf8;'>Compromised Key Forgery</b>", unsafe_allow_html=True)
    key_spoofed_val = st.sidebar.number_input("Forged Telemetry Value (Valid Signature)", value=150.0, step=5.0)
    attack_params = {"spoofed_value": key_spoofed_val}
    attack_desc = f"Stolen key forgery on {selected_device} with forged value {key_spoofed_val}"

elif selected_cap == "device_physical":
    st.sidebar.markdown("<b style='font-size: 0.82rem; color: #38bdf8;'>Hardware Perception Spoofing</b>", unsafe_allow_html=True)
    phys_spoofed_val = st.sidebar.number_input("Physical Transducer Tampered Value", value=185.0, step=5.0)
    attack_params = {"spoofed_value": phys_spoofed_val}
    attack_desc = f"Physical sensor tampering on {selected_device} with spoofed value {phys_spoofed_val}"

elif selected_cap == "actuator":
    st.sidebar.markdown("<b style='font-size: 0.82rem; color: #38bdf8;'>Actuator Command Disobedience</b>", unsafe_allow_html=True)
    st.sidebar.info("Device returns MCP 'ok' to City Brain dispatches/valves, but silently suppresses physical execution.")
    attack_params = {"suppress_actuation": True, "target_zone_id": device_zone}
    attack_desc = f"Actuator non-execution attack on {selected_device} ({device_zone})"

elif selected_cap == "sensor_fault":
    st.sidebar.markdown("<b style='font-size: 0.82rem; color: #38bdf8;'>Hardware Physical Fault</b>", unsafe_allow_html=True)
    fault_mode = st.sidebar.selectbox("Fault Mode", options=["drift", "stuck", "dropout"])
    if fault_mode == "drift":
        drift_rate = st.sidebar.slider("Drift Rate (units/sec)", min_value=0.1, max_value=5.0, value=1.0, step=0.1)
        attack_params = {"fault_mode": "drift", "drift_rate": drift_rate}
        attack_desc = f"Sensor drift fault on {selected_device} at {drift_rate} units/s"
    elif fault_mode == "stuck":
        stuck_val = st.sidebar.number_input("Stuck Sensor Value", value=42.0, step=1.0)
        attack_params = {"fault_mode": "stuck", "stuck_value": stuck_val}
        attack_desc = f"Sensor stuck fault on {selected_device} locked at {stuck_val}"
    else:  # dropout
        attack_params = {"fault_mode": "dropout"}
        attack_desc = f"Sensor dropout (zero-output) fault on {selected_device}"

# Action Buttons
st.sidebar.markdown("---")
if st.sidebar.button("EXECUTE ATTACK", type="primary", use_container_width=True):
    target_dev = None if selected_cap == "rogue" else selected_device
    target_z = device_zone if selected_cap in ("rogue", "actuator") else None
    success, msg = inject_attack_to_simulator(
        capability=selected_cap,
        target_device_id=target_dev,
        target_zone_id=target_z,
        params=attack_params,
        description=attack_desc,
    )
    if success:
        st.sidebar.success(msg)
        time.sleep(0.4)
        st.rerun()
    else:
        st.sidebar.error(msg)

if st.sidebar.button("DISARM ALL THREATS", use_container_width=True):
    success, msg = clear_attacks_in_simulator()
    if success:
        st.sidebar.info(msg)
        time.sleep(0.4)
        st.rerun()
    else:
        st.sidebar.error(msg)

# Active Threat Registry
active_atks = fetch_active_attacks()
if active_atks:
    st.sidebar.markdown("<hr style='border-top: 1px solid rgba(255,255,255,0.1); margin: 16px 0;'/>", unsafe_allow_html=True)
    st.sidebar.markdown("<b style='font-family: JetBrains Mono; font-size: 0.8rem; color: #ff2a5f;'>ACTIVE THREAT REGISTRY</b>", unsafe_allow_html=True)
    for atk in active_atks:
        with st.sidebar.expander(f"[THREAT] {atk['capability'].upper()} ({atk['target_device_id'] or atk['target_zone_id']})"):
            st.write(f"**ID:** ` {atk['attack_id']} `")
            st.write(f"**Started:** ` {atk['start_sim_time']} `")
            st.write(f"**Desc:** {atk['description']}")
            if st.button("TERMINATE EXPLOIT", key=f"btn_clear_{atk['attack_id']}", use_container_width=True):
                clear_attacks_in_simulator(atk["attack_id"])
                st.rerun()


# ==============================================================================
# Main View: High-End Operations Console Tabs
# ==============================================================================
tab_map, tab_divergence, tab_ledger, tab_terminal = st.tabs([
    "// 3D GEOSPATIAL DIGITAL TWIN",
    "// TELEMETRY & ATTACK DIVERGENCE",
    "// TRUTH LEDGER & INTEGRITY AUDIT",
    "// LIVE SECURITY AUDIT LOG",
])

# ------------------------------------------------------------------------------
# TAB 1: 3D Geospatial Digital Twin Map & Neighborhood Sectors
# ------------------------------------------------------------------------------
with tab_map:
    st.markdown("<h4 style='font-family: JetBrains Mono; color: #00e5ff; margin-bottom: 2px;'>CITY-WIDE 3D GEOSPATIAL DIGITAL TWIN // GREATER TUNIS METROPOLITAN AREA</h4>", unsafe_allow_html=True)
    st.caption(
        "Interactive 3D vector map of the 42 IoT sensors across 6 metropolitan sectors. "
        "Basemap rendered with CARTO Dark Vector tiles without text collision; 3D columns indicate live physical measurements; crimson beacons signify active exploits."
    )

    sensors_df = fetch_geospatial_sensors()

    if sensors_df.empty:
        st.info("Awaiting sensor spatial registration from CitySimulator...")
    else:
        # Spatial Filter Controls
        f_col1, f_col2, f_col3 = st.columns([2, 2, 2])
        with f_col1:
            zone_filter = st.selectbox(
                "Filter Neighborhood Sector",
                options=["All Sectors"] + [z["name"] for z in NEIGHBORHOODS],
                index=0,
            )
        with f_col2:
            type_filter = st.selectbox(
                "Filter Sensor Subsystem",
                options=["All Subsystems", "traffic_camera", "water_level", "power_load", "air_quality", "emergency_call"],
                index=0,
            )
        with f_col3:
            map_pitch = st.slider("3D Camera Elevation Pitch", min_value=0, max_value=70, value=50, step=5)

        # Apply Filters
        filtered_df = sensors_df.copy()
        if zone_filter != "All Sectors":
            filtered_df = filtered_df[filtered_df["zone_name"] == zone_filter]
        if type_filter != "All Subsystems":
            filtered_df = filtered_df[filtered_df["device_type"] == type_filter]

        # Prepare PyDeck 3D Layer Data
        def compute_sensor_style(row: pd.Series) -> Tuple[List[int], float]:
            val = float(row["broadcast_value"])
            is_threat = bool(row["discrepancy"])
            if is_threat:
                # Crimson Red for Active Threat
                color = [255, 42, 95, 255]
                elevation = max(240.0, val * 6.5)
            else:
                # Neon Cyan for Nominal
                color = [0, 229, 255, 210]
                elevation = max(40.0, val * 3.5)
            return color, elevation

        styles = [compute_sensor_style(row) for _, row in filtered_df.iterrows()]
        filtered_df["fill_color"] = [s[0] for s in styles]
        filtered_df["elevation"] = [s[1] for s in styles]
        filtered_df["status_label"] = filtered_df["discrepancy"].apply(
            lambda d: "[CRITICAL THREAT ACTIVE]" if d else "[NOMINAL INTEGRITY]"
        )

        # 3D Column Layer for Sensors
        sensor_column_layer = pdk.Layer(
            "ColumnLayer",
            data=filtered_df,
            get_position=["lon", "lat"],
            get_elevation="elevation",
            elevation_scale=1.5,
            radius=40,
            get_fill_color="fill_color",
            pickable=True,
            auto_highlight=True,
            extruded=True,
        )

        # Scatter Beacon Layer (Radial pulse marker)
        sensor_scatter_layer = pdk.Layer(
            "ScatterplotLayer",
            data=filtered_df,
            get_position=["lon", "lat"],
            get_radius=filtered_df["discrepancy"].apply(lambda d: 110 if d else 55),
            get_fill_color="fill_color",
            get_line_color=[255, 255, 255, 200],
            line_width_min_pixels=1,
            pickable=True,
        )

        # Neighborhood Centroids and Halo Radar Layer
        neighborhoods_df = pd.DataFrame(NEIGHBORHOODS)
        active_threat_zones = set(filtered_df[filtered_df["discrepancy"] == True]["zone_id"].unique())
        neighborhoods_df["halo_color"] = neighborhoods_df["zone_id"].apply(
            lambda zid: [255, 42, 95, 80] if zid in active_threat_zones else [0, 229, 255, 30]
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
            get_line_color=[255, 255, 255, 70],
            line_width_min_pixels=1,
            pickable=False,
        )

        # Crisp Vector Labels Layer (English Only, Zero Text Collision)
        zone_text_layer = pdk.Layer(
            "TextLayer",
            data=neighborhoods_df,
            get_position=["lon", "lat"],
            get_text="name",
            get_size=12,
            get_color=[240, 246, 252, 240],
            get_angle=0,
            get_text_anchor="'middle'",
            get_alignment_baseline="'center'",
            background=True,
            get_background_color=[7, 12, 24, 210],
            pickable=False,
        )

        # Camera View State centered over Greater Tunis
        view_state = pdk.ViewState(
            latitude=36.808,
            longitude=10.198,
            zoom=12.1,
            pitch=map_pitch,
            bearing=-15,
        )

        # Render PyDeck 3D Map using Clean Dark Basemap without broken Arabic fonts
        deck = pdk.Deck(
            layers=[zone_halo_layer, sensor_scatter_layer, sensor_column_layer, zone_text_layer],
            initial_view_state=view_state,
            tooltip={
                "html": """
                <div style="background: rgba(7, 12, 24, 0.96); padding: 12px 16px; border: 1px solid rgba(0, 229, 255, 0.5); border-radius: 6px; color: #fff; font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; box-shadow: 0 4px 20px rgba(0,0,0,0.8);">
                    <b style="color: #00e5ff; font-size: 0.95rem;">{device_id}</b> // {device_type}<br/>
                    <b>Sector:</b> {zone_name}<br/>
                    <b>Physical Truth:</b> <span style="color: #00e676; font-weight: bold;">{true_value} {unit}</span><br/>
                    <b>Broadcast Stream:</b> <span style="color: #ffab00; font-weight: bold;">{broadcast_value} {unit}</span><br/>
                    <b>Integrity:</b> {status_label}<br/>
                    <b>Attack Type:</b> <code>{attack_type}</code>
                </div>
                """,
                "style": {"color": "white"},
            },
            map_style="https://basemaps.cartocdn.com/gl/dark-matter-nolabels-gl-style/style.json",
        )

        st.pydeck_chart(deck)

        # Neighborhoods Aggregate Status Cards (6 Grid Layout)
        st.markdown("<h4 style='font-family: JetBrains Mono; color: #00e5ff; margin-top: 18px; margin-bottom: 8px;'>METROPOLITAN NEIGHBORHOOD SECTORS // TELEMETRY SUMMARY</h4>", unsafe_allow_html=True)
        summaries = fetch_neighborhood_summaries(sensors_df)

        for row_idx in range(0, len(summaries), 3):
            cols = st.columns(3)
            for c_idx in range(3):
                item_idx = row_idx + c_idx
                if item_idx < len(summaries):
                    s = summaries[item_idx]
                    with cols[c_idx]:
                        is_threat = s["active_threats"] > 0
                        card_class = "neighborhood-card-alert" if is_threat else "neighborhood-card"
                        status_html = (
                            f"<span class='badge-critical'>[CRITICAL THREAT: {s['active_threats']} EXPLOIT ACTIVE]</span>"
                            if is_threat
                            else "<span class='badge-nominal'>[NOMINAL INTEGRITY // 7/7 ONLINE]</span>"
                        )
                        st.markdown(
                            f"""
                            <div class='{card_class}'>
                                <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;'>
                                    <h5 style='margin: 0; color: #00e5ff; font-family: JetBrains Mono; font-size: 0.95rem;'>{s['name']}</h5>
                                    <span style='color: #64748b; font-family: JetBrains Mono; font-size: 0.75rem;'><code>{s['zone_id']}</code></span>
                                </div>
                                <div style='margin-bottom: 10px;'>{status_html}</div>
                                <div style='font-family: JetBrains Mono; font-size: 0.78rem; color: #cbd5e1; line-height: 1.6;'>
                                    • Traffic Flow: <b>{s['traffic_avg']:.1f}</b> veh/min<br/>
                                    • Water Level: <b>{s['water_max']:.2f}</b> m<br/>
                                    • Power Load: <b>{s['power_sum']:.1f}</b> MW<br/>
                                    • Air Quality (AQI): <b>{s['aqi_avg']:.1f}</b>
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )


# ------------------------------------------------------------------------------
# TAB 2: Telemetry & Attack Divergence (Real-Time Curves)
# ------------------------------------------------------------------------------
with tab_divergence:
    st.markdown("<h4 style='font-family: JetBrains Mono; color: #00e5ff; margin-bottom: 2px;'>REAL-TIME TELEMETRY & ATTACK DIVERGENCE CURVES</h4>", unsafe_allow_html=True)
    st.caption(
        "Compare the physical ground-truth baseline against the broadcast telemetry stream over time. "
        "Visualizes sensor drift, perceptual spoofing, and signature forgery divergence."
    )

    c_sel1, c_sel2 = st.columns([2, 3])
    with c_sel1:
        target_chart_dev = st.selectbox(
            "Select Sensor for Time-Series Analysis",
            options=devices_list,
            index=0 if devices_list else None,
            key="chart_device_selector",
        )
    with c_sel2:
        chart_points = st.slider("Historical Time Window (Ticks)", min_value=20, max_value=120, value=60, step=10)

    if target_chart_dev:
        ts_df = fetch_device_time_series(target_chart_dev, limit=chart_points)

        if ts_df.empty:
            st.info(f"No telemetry samples yet for {target_chart_dev}. (Click 'PULSE 10 TICKS' in sidebar to generate readings).")
        else:
            max_div = (ts_df["broadcast_value"] - ts_df["true_value"]).abs().max()
            current_div = abs(ts_df["broadcast_value"].iloc[-1] - ts_df["true_value"].iloc[-1])
            tampered_count = int(ts_df["is_attacked"].sum())

            d_col1, d_col2, d_col3, d_col4 = st.columns(4)
            with d_col1:
                st.metric("Physical Ground Truth", f"{ts_df['true_value'].iloc[-1]:.2f}")
            with d_col2:
                st.metric("Broadcast Stream Value", f"{ts_df['broadcast_value'].iloc[-1]:.2f}")
            with d_col3:
                st.metric(
                    "Current Discrepancy",
                    f"{current_div:.2f}",
                    delta=f"{current_div:.2f} Drift" if current_div > 0.01 else "Zero Deviation",
                    delta_color="inverse" if current_div > 0.01 else "normal",
                )
            with d_col4:
                st.metric("Tampered Frames in Window", f"{tampered_count} / {len(ts_df)}")

            # Multi-line Altair Chart
            chart_data = []
            for _, r in ts_df.iterrows():
                chart_data.append({
                    "seq": int(r["seq"]),
                    "value": float(r["true_value"]),
                    "channel": "Physical Ground Truth",
                })
                chart_data.append({
                    "seq": int(r["seq"]),
                    "value": float(r["broadcast_value"]),
                    "channel": "Broadcast Telemetry Stream",
                })

            chart_melted = pd.DataFrame(chart_data)

            line_chart = (
                alt.Chart(chart_melted)
                .mark_line(strokeWidth=2.4)
                .encode(
                    x=alt.X("seq:Q", title="Simulation Sequence Tick (seq)", scale=alt.Scale(zero=False)),
                    y=alt.Y("value:Q", title="Sensor Output", scale=alt.Scale(zero=False)),
                    color=alt.Color(
                        "channel:N",
                        scale=alt.Scale(
                            domain=["Physical Ground Truth", "Broadcast Telemetry Stream"],
                            range=["#00e676", "#ff2a5f"],
                        ),
                        legend=alt.Legend(title="Telemetry Channel", orient="top"),
                    ),
                    strokeDash=alt.condition(
                        alt.datum.channel == "Broadcast Telemetry Stream",
                        alt.value([4, 4]),
                        alt.value([0]),
                    ),
                    tooltip=["seq", "channel", "value"],
                )
                .properties(height=380)
                .interactive()
            )

            st.altair_chart(line_chart, width="stretch")


# ------------------------------------------------------------------------------
# TAB 3: The Truth Ledger & Integrity Audit
# ------------------------------------------------------------------------------
with tab_ledger:
    st.markdown("<h4 style='font-family: JetBrains Mono; color: #00e5ff; margin-bottom: 2px;'>THE TRUTH LEDGER // GROUND TRUTH AUDIT LEDGER</h4>", unsafe_allow_html=True)
    st.caption(
        "Cryptographic and physical state ledger reading directly from isolated `ground_truth.db`. "
        "Any variance between physical truth and broadcast stream indicates an active adversary exploit."
    )

    ledger_df = fetch_truth_ledger(limit=30)

    if ledger_df.empty:
        st.info("Awaiting telemetry stream from CitySimulator...")
    else:
        display_df = ledger_df.copy()
        display_df["discrepancy"] = display_df.apply(
            lambda r: abs(float(r["true_value"]) - float(r["broadcast_value"])) > 1e-3 or bool(r["is_attacked"]),
            axis=1,
        )
        display_df["status_indicator"] = display_df["discrepancy"].apply(
            lambda d: "[CRITICAL THREAT]" if d else "[NOMINAL]"
        )

        columns_to_show = [
            "ts",
            "device_id",
            "true_value",
            "broadcast_value",
            "unit",
            "status_indicator",
            "attack_type",
            "reading_id",
        ]
        renamed_df = display_df[columns_to_show].rename(
            columns={
                "ts": "Sim Time (UTC)",
                "device_id": "Device ID",
                "true_value": "Physical Truth",
                "broadcast_value": "Broadcast Stream",
                "unit": "Unit",
                "status_indicator": "Integrity State",
                "attack_type": "Attack Type",
                "reading_id": "Reading ID",
            }
        )

        def highlight_threats(row: pd.Series) -> List[str]:
            if "CRITICAL" in str(row["Integrity State"]):
                return [
                    "background-color: rgba(255, 42, 95, 0.22); color: #ff477e; font-weight: bold; border-left: 4px solid #ff2a5f"
                ] * len(row)
            return [""] * len(row)

        styled_df = (
            renamed_df.style
            .apply(highlight_threats, axis=1)
            .format({"Physical Truth": "{:.2f}", "Broadcast Stream": "{:.2f}"})
        )

        st.dataframe(styled_df, width="stretch", height=480)


# ------------------------------------------------------------------------------
# TAB 4: Live Security Terminal & Timeline (Rule R3)
# ------------------------------------------------------------------------------
with tab_terminal:
    st.markdown("<h4 style='font-family: JetBrains Mono; color: #00e5ff; margin-bottom: 2px;'>LIVE SECURITY AUDIT LOG // SIMULATION TIME AUDIT STREAM (RULE R3)</h4>", unsafe_allow_html=True)
    st.caption("Chronological security audit stream ordered strictly by Simulation Time (`ts`).")

    timeline_events = fetch_timeline_events(limit=40)

    if timeline_events:
        terminal_html = "<div class='terminal-box'>"
        for ev in timeline_events:
            ts = ev.get("ts", "")
            dev = ev.get("device_id", "")
            t_val = ev.get("true_value", 0.0)
            b_val = ev.get("broadcast_value", 0.0)
            unit = ev.get("unit", "")
            is_atk = ev.get("is_attacked", 0)
            atk_type = ev.get("attack_type", "")

            if is_atk or abs(t_val - b_val) > 1e-3:
                terminal_html += (
                    f"<div class='terminal-entry-alert'>[{ts}] [ALERT_EXPLOIT] Target: <code>{dev}</code> "
                    f"| Tampered: Truth={t_val:.2f} {unit} != Broadcast={b_val:.2f} {unit} "
                    f"| Model: <code>{atk_type or 'anomaly'}</code></div>"
                )
            else:
                terminal_html += (
                    f"<div class='terminal-entry-info'>[{ts}] [SYS_NOMINAL] Device: <code>{dev}</code> "
                    f"| Telemetry={b_val:.2f} {unit} | Integrity=OK</div>"
                )
        terminal_html += "</div>"
        st.markdown(terminal_html, unsafe_allow_html=True)
    else:
        st.info("No timeline events logged yet in ground_truth.db.")

st.markdown("<hr style='border: 0; border-top: 1px solid rgba(255,255,255,0.08); margin: 20px 0 10px 0;'/>", unsafe_allow_html=True)
st.caption(
    "TSYP 14 CS CHALLENGE // CITY BRAIN CONVERGENCE // LAYER 1 DIGITAL TWIN ARCHITECTURE"
)

# Live auto-refresh interval execution
if auto_refresh and not is_testing:
    if not daemon_online:
        advance_simulation_ticks(1)
    time.sleep(1.0)
    st.rerun()
