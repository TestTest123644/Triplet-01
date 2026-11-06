"""Tests for Phase 9.5 operational fixes and security enhancements."""

import os
import signal
import subprocess
import sys
import time
import pytest
from starlette.testclient import TestClient

from twin.simulation import CitySimulator
from twin.red_team_api import build_red_team_app
from twin.kafka_streamer import KafkaStreamer
try:
    from dashboard import _fetch_scoreboard_metrics_cached, _fetch_geospatial_sensors_cached
except ImportError:
    from twin.dashboard import _fetch_scoreboard_metrics_cached, _fetch_geospatial_sensors_cached


def test_containment_persists_across_restart(tmp_path, monkeypatch):
    """Verify that isolated/quarantined devices and rogue blocks persist across simulator restarts."""
    db_path = str(tmp_path / "restart_test.db")
    keys_path = str(tmp_path / "restart_keys.json")
    monkeypatch.setenv("GROUND_TRUTH_DB_PATH", db_path)
    monkeypatch.setenv("KEYSTORE_PATH", keys_path)

    # 1. First simulator instance
    sim1 = CitySimulator(seed=42)
    # Isolate registered device
    res_iso = sim1.update_device_status(device_id="z1-tc1", new_status="isolated", tool_name="isolate_device")
    assert res_iso["status"] == "ok"
    assert sim1.devices["z1-tc1"].status == "isolated"

    # Inject and quarantine rogue device
    sim1.inject_attack(
        capability="rogue",
        params={"rogue_device_id": "rg-01", "zone_id": "zone-1"},
    )
    res_rg = sim1.update_device_status(device_id="rg-01", new_status="quarantined", tool_name="quarantine_device")
    assert res_rg["status"] == "ok"
    assert "rg-01" in sim1.blocked_rogue_ids

    sim1.close()

    # 2. Second simulator instance on the exact same database ledger
    sim2 = CitySimulator(seed=42)

    # Registered device must still be isolated
    assert sim2.devices["z1-tc1"].status == "isolated"
    st = sim2.get_device_state("z1-tc1")
    assert st["status"] == "isolated"

    # Rogue device must still be blocked
    assert "rg-01" in sim2.blocked_rogue_ids

    # Re-inject the rogue attack and tick: frames must be blocked
    sim2.inject_attack(
        capability="rogue",
        params={"rogue_device_id": "rg-01", "zone_id": "zone-1"},
    )
    sim2.tick()
    assert sim2.blocked_rogue_frames.get("rg-01", 0) > 0

    sim2.close()


def test_console_contained_state_kpi_and_amber_map(tmp_path, monkeypatch):
    """Verify that contained devices are reflected in KPI metric and 3D map columns as amber."""
    db_path = str(tmp_path / "console_test.db")
    keys_path = str(tmp_path / "console_keys.json")
    monkeypatch.setenv("GROUND_TRUTH_DB_PATH", db_path)
    monkeypatch.setenv("KEYSTORE_PATH", keys_path)

    sim = CitySimulator(seed=42)
    sim.update_device_status(device_id="z2-wl1", new_status="isolated", tool_name="isolate_device")
    sim.tick()
    sim.close()

    # Query KPI metrics
    mtime = os.path.getmtime(db_path)
    metrics = _fetch_scoreboard_metrics_cached(db_path, mtime)
    assert metrics["contained_devices"] == 1

    # Query geospatial sensors dataframe for map
    sensors_df = _fetch_geospatial_sensors_cached(db_path, mtime)
    assert not sensors_df.empty
    row_wl = sensors_df[sensors_df["device_id"] == "z2-wl1"].iloc[0]
    # Contained color must be Amber: [245, 158, 11, 230]
    assert row_wl["fill_color"] == [245, 158, 11, 230]


def test_spoof_value_alias_and_validation(tmp_path, monkeypatch):
    """Test spoofed_value / value alias handling and 400 rejection for non-numeric values."""
    db_path = str(tmp_path / "spoof_test.db")
    keys_path = str(tmp_path / "spoof_keys.json")
    monkeypatch.setenv("GROUND_TRUTH_DB_PATH", db_path)
    monkeypatch.setenv("KEYSTORE_PATH", keys_path)

    sim = CitySimulator(seed=42)
    streamer = KafkaStreamer(mock_mode=True)
    app = build_red_team_app(sim, token="test-token", streamer=streamer)
    client = TestClient(app)

    # 1. Non-numeric value rejected with 400
    bad_resp = client.post(
        "/api/attack/inject",
        headers={"X-Red-Team-Token": "test-token"},
        json={
            "capability": "device_physical",
            "target_device_id": "z1-aq1",
            "params": {"value": "not-a-number"},
        },
    )
    assert bad_resp.status_code == 400
    assert bad_resp.json()["code"] == "INVALID_ARGUMENT"

    # 2. Inject with only "value": 300 on an AQI device
    good_resp = client.post(
        "/api/attack/inject",
        headers={"X-Red-Team-Token": "test-token"},
        json={
            "capability": "device_physical",
            "target_device_id": "z1-aq1",
            "params": {"value": 300},
        },
    )
    assert good_resp.status_code == 200

    # Tick and verify broadcast value is 300.0
    readings = sim.tick()
    aq_reading = [r for r in readings if r["device_id"] == "z1-aq1"][0]
    assert aq_reading["value"] == 300.0
    assert sim.devices["z1-aq1"].last_broadcast_value == 300.0

    # 3. When both value and spoofed_value are given, spoofed_value wins
    both_resp = client.post(
        "/api/attack/inject",
        headers={"X-Red-Team-Token": "test-token"},
        json={
            "capability": "device_key",
            "target_device_id": "z1-tc1",
            "params": {"value": 50, "spoofed_value": 150},
        },
    )
    assert both_resp.status_code == 200
    readings2 = sim.tick()
    tc_reading = [r for r in readings2 if r["device_id"] == "z1-tc1"][0]
    assert tc_reading["value"] == 150.0
    assert sim.devices["z1-tc1"].last_broadcast_value == 150.0

    sim.close()


def test_orphan_mock_exit_with_parent():
    """Verify that mock script exits cleanly when launched with --exit-with-parent and parent terminates."""
    env = os.environ.copy()
    env["PYTHONPATH"] = f".:twin:twin/src:{env.get('PYTHONPATH', '')}".strip(":")

    # Launch mock_guardian subprocess with --live and --exit-with-parent
    cmd = [
        sys.executable,
        "-m", "mocks.mock_guardian",
        "--live",
        "--exit-with-parent",
    ]
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.5)
    assert proc.poll() is None, "Process should be running"

    # Send SIGINT to simulate scenario interruption
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2.0)
        pytest.fail("Mock did not terminate within 5 seconds on SIGINT")

    assert proc.poll() is not None
