"""Unit and Integration Tests for Dashboard and Adversary Console (Layer 1).

Assigned to: Agent_QA
Tests:
1. Read-only SQLite isolation for the Dashboard.
2. Scoreboard metric aggregation and truth ledger retrieval.
3. Isolated Red Team REST API routes via build_red_team_app (/api/status, /api/attack/inject, /api/attack/clear, /api/attack/active).
4. Threat injection and clearing routines with token authentication and error handling.
"""

import os
import sqlite3
import pytest
import requests
from starlette.testclient import TestClient

from twin.db_ledger import GroundTruthLedger
from twin.red_team_api import build_red_team_app
from twin.simulation import CitySimulator
import dashboard


@pytest.fixture
def test_sim_and_db(tmp_path):
    """Create isolated GroundTruthLedger and CitySimulator in tmp directory."""
    db_path = str(tmp_path / "test_truth.db")
    ledger = GroundTruthLedger(db_path=db_path)
    sim = CitySimulator(seed=123, ledger=ledger)

    # Populate sample readings
    sim.tick()
    sim.tick()

    # Inject one sample attack
    sim.inject_attack(
        capability="device_physical",
        target_device_id="z1-tc1",
        params={"spoofed_value": 180.0},
        description="Test physical attack",
    )
    sim.tick()

    return sim, ledger, db_path


def test_dashboard_read_only_isolation(test_sim_and_db, monkeypatch):
    """Verify that dashboard functions connect strictly in read-only mode."""
    sim, ledger, db_path = test_sim_and_db
    monkeypatch.setattr(dashboard, "DB_PATH", db_path)

    # Scoreboard metrics should reflect populated data
    metrics = dashboard.fetch_scoreboard_metrics()
    assert metrics["total_readings"] > 0
    assert metrics["active_attacks"] == 1
    assert metrics["active_incidents"] > 0
    assert "Z" in metrics["latest_sim_time"]

    # Truth ledger retrieval
    df = dashboard.fetch_truth_ledger(limit=10)
    assert not df.empty
    assert "true_value" in df.columns
    assert "broadcast_value" in df.columns
    assert "is_attacked" in df.columns

    # Active attacks
    active = dashboard.fetch_active_attacks()
    assert len(active) == 1
    assert active[0]["capability"] == "device_physical"

    # Timeline events
    timeline = dashboard.fetch_timeline_events(limit=10)
    assert len(timeline) > 0

    # Ensure a write query using dashboard's read-only connection is rejected
    conn = dashboard.get_db_connection()
    assert conn is not None
    with pytest.raises(sqlite3.OperationalError):
        conn.execute(
            "INSERT INTO readings_truth (reading_id, device_id, ts, seq, true_value, broadcast_value, unit, status, created_at_wall_clock) "
            "VALUES ('bad', 'bad', 'bad', 1, 0, 0, 'u', 'ok', 'now')"
        )
    conn.close()


def test_real_red_team_app_endpoints(test_sim_and_db):
    """Test the real build_red_team_app Starlette application with authentication."""
    sim, ledger, db_path = test_sim_and_db
    token = "test-dashboard-token-42"
    app = build_red_team_app(sim, token=token)
    client = TestClient(app)

    # 1. Requests without token or with wrong token must be rejected with 401
    unauth_res = client.get("/api/status")
    assert unauth_res.status_code == 401
    assert unauth_res.json()["code"] == "UNAUTHORIZED"

    bad_token_res = client.get("/api/status", headers={"X-Red-Team-Token": "invalid-token"})
    assert bad_token_res.status_code == 401

    headers = {"X-Red-Team-Token": token}

    # 2. Status endpoint returns ground truth simulation overview
    status_res = client.get("/api/status", headers=headers)
    assert status_res.status_code == 200
    status_data = status_res.json()
    assert status_data["status"] == "ok"
    assert status_data["total_devices"] == 42
    assert status_data["active_attacks_count"] == 1

    # 3. Inject attack via real API
    inject_res = client.post(
        "/api/attack/inject",
        headers=headers,
        json={
            "capability": "sensor_fault",
            "target_device_id": "z2-wl1",
            "params": {"fault_mode": "drift", "drift_rate": 2.0},
            "description": "Test drift attack via real API",
        },
    )
    assert inject_res.status_code == 200
    aid = inject_res.json()["attack_id"]
    assert aid.startswith("atk-")
    assert "z2-wl1" in sim.devices
    assert sim.devices["z2-wl1"].active_attack is not None

    # Missing capability validation
    invalid_inject = client.post("/api/attack/inject", headers=headers, json={})
    assert invalid_inject.status_code == 400

    # 4. Fetch active attacks
    active_res = client.get("/api/attack/active", headers=headers)
    assert active_res.status_code == 200
    assert len(active_res.json()["active_attacks"]) == 2

    # 5. Clear specific attack
    clear_res = client.post("/api/attack/clear", headers=headers, json={"attack_id": aid})
    assert clear_res.status_code == 200
    assert clear_res.json()["cleared"] is True

    # 6. Clear all attacks
    clear_all_res = client.post("/api/attack/clear", headers=headers, json={"attack_id": "all"})
    assert clear_all_res.status_code == 200
    assert len(sim.active_attacks) == 0


def test_dashboard_injection_interfacing(test_sim_and_db, monkeypatch):
    """Test dashboard.inject_attack_to_simulator and clear_attacks_in_simulator with simulated API responses."""
    sim, ledger, db_path = test_sim_and_db
    monkeypatch.setattr(dashboard, "DB_PATH", db_path)

    token = "test-token"
    app = build_red_team_app(sim, token=token)
    client = TestClient(app)

    def mock_post(url, *args, **kwargs):
        kwargs.pop("timeout", None)
        if url.startswith(dashboard.TWIN_API_URL):
            path = url[len(dashboard.TWIN_API_URL):]
            return client.post(path, *args, **kwargs)
        raise requests.exceptions.ConnectionError("Connection refused")

    def mock_get(url, *args, **kwargs):
        kwargs.pop("timeout", None)
        if url.startswith(dashboard.TWIN_API_URL):
            path = url[len(dashboard.TWIN_API_URL):]
            return client.get(path, *args, **kwargs)
        raise requests.exceptions.ConnectionError("Connection refused")

    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(dashboard, "get_red_team_token", lambda: token)

    # 1. Successful injection
    success, msg = dashboard.inject_attack_to_simulator(
        capability="link",
        target_device_id="z3-pl1",
        params={"link_mode": "corrupt_bytes"},
        description="Dashboard test link attack",
    )
    assert success is True
    assert "z3-pl1" in sim.devices
    assert sim.devices["z3-pl1"].active_attack is not None

    # 2. Successful clear
    success_clear, clear_msg = dashboard.clear_attacks_in_simulator()
    assert success_clear is True
    assert len(sim.active_attacks) == 0

    # 3. Connection error handling produces [ALERT]
    def mock_post_fail(url, *args, **kwargs):
        raise requests.exceptions.ConnectionError("Connection refused")

    monkeypatch.setattr(requests, "post", mock_post_fail)
    fail_succ, fail_msg = dashboard.inject_attack_to_simulator(capability="link")
    assert fail_succ is False
    assert "[ALERT]" in fail_msg
    assert "Connection refused" in fail_msg


def test_dashboard_geospatial_and_divergence(test_sim_and_db, monkeypatch):
    """Test 3D geospatial sensor queries, neighborhood summaries, and time-series divergence."""
    sim, ledger, db_path = test_sim_and_db
    monkeypatch.setattr(dashboard, "DB_PATH", db_path)

    # 1. Test geospatial sensors query
    sensors_df = dashboard.fetch_geospatial_sensors()
    assert not sensors_df.empty
    assert "lat" in sensors_df.columns
    assert "lon" in sensors_df.columns
    assert "zone_name" in sensors_df.columns
    assert "discrepancy" in sensors_df.columns
    assert len(sensors_df) == 42

    # Check that z1-tc1 was marked as an active attack from fixture
    z1_tc1 = sensors_df[sensors_df["device_id"] == "z1-tc1"]
    assert not z1_tc1.empty
    assert bool(z1_tc1["discrepancy"].iloc[0]) is True
    assert z1_tc1["attack_type"].iloc[0] == "device_physical"

    # 2. Test neighborhood summaries
    summaries = dashboard.fetch_neighborhood_summaries(sensors_df)
    assert len(summaries) == 6
    zone1_sum = next(s for s in summaries if s["zone_id"] == "zone-1")
    assert zone1_sum["active_threats"] >= 1
    assert zone1_sum["status"] == "threat"
    assert zone1_sum["total_sensors"] == 7

    # 3. Test device time series divergence
    ts_df = dashboard.fetch_device_time_series("z1-tc1", limit=10)
    assert not ts_df.empty
    assert "true_value" in ts_df.columns
    assert "broadcast_value" in ts_df.columns
    assert "discrepancy" in ts_df.columns
    assert (ts_df["broadcast_value"] - ts_df["true_value"]).abs().max() > 0


def test_spatial_twin_deck_to_json(test_sim_and_db, monkeypatch):
    """Test building PyDeck 3D map from sensors dataframe and serializing via to_json().

    Regression test for pandas 2.x Series vs string accessor crash in pydeck.
    """
    import json
    sim, ledger, db_path = test_sim_and_db
    monkeypatch.setattr(dashboard, "DB_PATH", db_path)

    sensors_df = dashboard.fetch_geospatial_sensors()
    assert not sensors_df.empty

    deck = dashboard.build_spatial_twin_deck(sensors_df, map_pitch=50)
    assert deck is not None

    json_str = deck.to_json()
    assert isinstance(json_str, str)
    parsed = json.loads(json_str)
    assert "layers" in parsed
    assert len(parsed["layers"]) == 4

    # Verify that scatterplot layer uses radius column name accessor, not a Series
    scatter_layers = [l for l in parsed["layers"] if l.get("@@type") == "ScatterplotLayer"]
    assert len(scatter_layers) == 2
    get_radii = [l.get("getRadius") for l in scatter_layers]
    assert "@@=radius" in get_radii
    assert "@@=halo_radius" in get_radii


def test_normalised_map_heights(test_sim_and_db, monkeypatch):
    """Verify that 3D pillar elevations are normalised to each device's operating range [min_val, max_val]."""
    sim, ledger, db_path = test_sim_and_db
    monkeypatch.setattr(dashboard, "DB_PATH", db_path)

    sensors_df = dashboard.fetch_geospatial_sensors()
    assert not sensors_df.empty
    assert "elevation" in sensors_df.columns
    assert "min_val" in sensors_df.columns
    assert "max_val" in sensors_df.columns

    # Elevate should strictly stay within [50.0, 500.0]
    elevations = sensors_df["elevation"].values
    assert (elevations >= 50.0).all()
    assert (elevations <= 500.0).all()

    # Verify normalisation formula across diverse sensor units
    for _, row in sensors_df.iterrows():
        b_val = float(row["broadcast_value"])
        min_v = float(row["min_val"])
        max_v = float(row["max_val"])
        norm = max(0.0, min(1.0, (b_val - min_v) / (max_v - min_v)))
        expected_elevation = 50.0 + norm * 450.0
        assert abs(row["elevation"] - expected_elevation) < 1e-4


def test_missing_journal_error_message(monkeypatch):
    """Verify that a missing or unreachable journal API returns a clear error message with env vars."""
    def mock_get_fail(url, *args, **kwargs):
        raise requests.exceptions.ConnectionError("Connection refused")

    monkeypatch.setattr(requests, "get", mock_get_fail)
    monkeypatch.setattr(dashboard, "DB_PATH", "/nonexistent/test.db")

    entries, error_msg = dashboard.fetch_timeline_events()
    assert len(entries) == 0
    assert error_msg is not None
    assert "TWIN_API_URL" in error_msg
    assert "Cannot connect to journal API" in error_msg


def test_console_never_opens_ledger_writable(test_sim_and_db, monkeypatch):
    """Verify that get_db_connection() strictly opens SQLite in read-only mode."""
    sim, ledger, db_path = test_sim_and_db
    monkeypatch.setattr(dashboard, "DB_PATH", db_path)

    conn = dashboard.get_db_connection()
    try:
        # All write operations must be rejected by sqlite3
        with pytest.raises(sqlite3.OperationalError, match="readonly database"):
            conn.execute("DELETE FROM readings_truth")

        with pytest.raises(sqlite3.OperationalError, match="readonly database"):
            conn.execute("UPDATE devices SET status = 'tampered'")

        with pytest.raises(sqlite3.OperationalError, match="readonly database"):
            conn.execute("INSERT INTO physical_events (event_id) VALUES ('fake')")
    finally:
        conn.close()
