"""Zero-Trust Isolation and Ground Truth Leakage Prevention Tests.

Verifies Phase 2 requirements:
1. Under device_physical attack: MCP get_device_state reports broadcast value, not ledger true_value.
2. Under actuator attack: set_valve returns ok, get_device_state/list_devices show commanded state,
   while physics maintains true unexecuted state, and get_city_state reports claimed dispatch.
3. Under link attack: get_reading and get_readings strip all '_'-prefixed internal keys.
4. MCP SSE app exposes no /api routes (returns 404).
5. Red Team API routes strictly enforce authentication (401 without/wrong token, 200 with valid token).
6. Token file is generated with 0o600 permissions when RED_TEAM_TOKEN is unset.
"""

import os
from pathlib import Path
import stat
import pytest
from starlette.testclient import TestClient

from twin.db_ledger import GroundTruthLedger
from twin.mcp_server import (
    dispatch,
    get_city_state,
    get_device_state,
    get_reading,
    get_readings,
    list_devices,
    mcp,
    set_active_simulator,
    set_valve,
)
from twin.red_team_api import build_red_team_app, get_or_create_red_team_token
from twin.simulation import CitySimulator


def test_device_physical_broadcast_vs_ledger_truth(tmp_path):
    """Under device_physical attack, MCP get_device_state must return broadcast value, never truth."""
    db_path = str(tmp_path / "truth_isolation.db")
    ledger = GroundTruthLedger(db_path=db_path)
    sim = CitySimulator(seed=101, ledger=ledger)
    set_active_simulator(sim)

    target_dev = "z1-tc1"
    spoofed_reading = 185.0
    sim.inject_attack(
        capability="device_physical",
        target_device_id=target_dev,
        params={"spoofed_value": spoofed_reading},
    )

    # Advance 1 tick
    readings = sim.tick()
    target_broadcast = next(r for r in readings if r["device_id"] == target_dev)
    assert target_broadcast["value"] == spoofed_reading

    # Query MCP endpoint
    mcp_state = get_device_state(target_dev)
    assert mcp_state is not None
    assert mcp_state["current_value"] == spoofed_reading

    # Query isolated SQLite ledger
    truth_record = ledger.get_reading_truth(target_broadcast["reading_id"])
    assert truth_record is not None
    assert truth_record["broadcast_value"] == spoofed_reading
    assert truth_record["true_value"] != spoofed_reading
    assert mcp_state["current_value"] != truth_record["true_value"]


def test_actuator_attack_commanded_vs_physics(tmp_path):
    """Under actuator attack, actuation returns ok and public endpoints report commanded state,

    while physics keeps the old unexecuted physical state.
    """
    db_path = str(tmp_path / "actuator_isolation.db")
    ledger = GroundTruthLedger(db_path=db_path)
    sim = CitySimulator(seed=202, ledger=ledger)
    set_active_simulator(sim)

    water_dev = "z1-wl1"
    init_valve = sim.devices[water_dev].actuator_state["valve_position"]
    assert init_valve == "half"

    # Inject actuator attack on zone-1
    sim.inject_attack(
        capability="actuator",
        target_zone_id="zone-1",
    )

    # Brain commands valve to open
    result = set_valve(water_dev, "open")
    assert result["status"] == "ok"
    assert result["valve_position"] == "open"

    # MCP get_device_state and list_devices must report what device claims ("open")
    dev_state = get_device_state(water_dev)
    assert dev_state["actuator_state"]["valve_position"] == "open"

    devices_list = list_devices(zone_id="zone-1")
    wl_entry = next(d for d in devices_list if d["device_id"] == water_dev)
    assert wl_entry["actuator_state"]["valve_position"] == "open"

    # Physical truth inside simulator must retain the original unexecuted state ("half")
    assert sim.devices[water_dev].actuator_state["valve_position"] == init_valve

    # Brain dispatches an emergency unit to zone-1
    disp_result = dispatch(incident_type="flood", zone_id="zone-1")
    assert disp_result["status"] == "ok"

    # MCP get_city_state must count the reported dispatch
    city_state = get_city_state()
    assert city_state["zones"]["zone-1"]["active_dispatches"] == 1

    # But physical truth dispatches list was blocked by the actuator attack
    assert len(sim.zone_dispatches["zone-1"]) == 0


def test_no_underscore_keys_leaked_under_link_attack(tmp_path):
    """MCP get_reading and get_readings must strip all '_'-prefixed keys."""
    db_path = str(tmp_path / "link_leakage.db")
    ledger = GroundTruthLedger(db_path=db_path)
    sim = CitySimulator(seed=303, ledger=ledger)
    set_active_simulator(sim)

    target_dev = "z2-wl1"
    sim.inject_attack(
        capability="link",
        target_device_id=target_dev,
        params={"link_mode": "corrupt_sig"},
    )

    readings = sim.tick()
    target_reading = next(r for r in readings if r["device_id"] == target_dev)
    reading_id = target_reading["reading_id"]

    # Internal buffer contains _corrupt_sig
    raw_cached = sim.recent_readings_buffer.get(reading_id)
    assert raw_cached is not None
    assert "_corrupt_sig" in raw_cached

    # MCP get_reading must return copy with '_' keys removed
    reading = get_reading(reading_id)
    assert reading is not None
    assert "_corrupt_sig" not in reading
    assert not any(k.startswith("_") for k in reading.keys())

    # MCP get_readings must also strip '_' keys
    readings = get_readings(device_id=target_dev)
    assert len(readings) > 0
    for r in readings:
        assert not any(k.startswith("_") for k in r.keys())


def test_mcp_app_exposes_no_api_routes():
    """MCP Starlette application on Port 8000 must not expose any /api control routes."""
    mcp_app = mcp.sse_app(host="0.0.0.0")
    client = TestClient(mcp_app)

    # All red-team API routes must 404 on the MCP server
    assert client.get("/api/status").status_code == 404
    assert client.post("/api/attack/inject").status_code == 404
    assert client.post("/api/attack/clear").status_code == 404
    assert client.get("/api/attack/active").status_code == 404

    # Verify no route paths on mcp_app begin with /api
    for route in mcp_app.routes:
        assert not getattr(route, "path", "").startswith("/api")


def test_red_team_routes_auth_enforcement(tmp_path):
    """Red Team API routes must require X-Red-Team-Token header matching token."""
    db_path = str(tmp_path / "auth_test.db")
    ledger = GroundTruthLedger(db_path=db_path)
    sim = CitySimulator(seed=404, ledger=ledger)

    token = "secret-red-team-token-999"
    app = build_red_team_app(sim, token=token)
    client = TestClient(app)

    routes = [
        ("GET", "/api/status"),
        ("POST", "/api/attack/inject"),
        ("POST", "/api/attack/clear"),
        ("GET", "/api/attack/active"),
    ]

    for method, path in routes:
        # 1. Missing header -> 401
        res_no_auth = client.request(method, path)
        assert res_no_auth.status_code == 401
        assert res_no_auth.json()["code"] == "UNAUTHORIZED"

        # 2. Invalid header -> 401
        res_bad_auth = client.request(method, path, headers={"X-Red-Team-Token": "wrong-token"})
        assert res_bad_auth.status_code == 401
        assert res_bad_auth.json()["code"] == "UNAUTHORIZED"

    # 3. Valid token returns non-401 (200 on status and active)
    auth_headers = {"X-Red-Team-Token": token}
    assert client.get("/api/status", headers=auth_headers).status_code == 200
    assert client.get("/api/attack/active", headers=auth_headers).status_code == 200


def test_token_file_creation_mode_600(tmp_path, monkeypatch):
    """When RED_TEAM_TOKEN is unset, get_or_create_red_team_token writes a token file with mode 600."""
    monkeypatch.delenv("RED_TEAM_TOKEN", raising=False)
    token_path = str(tmp_path / "secret_keys" / "red_team.token")
    monkeypatch.setenv("RED_TEAM_TOKEN_FILE", token_path)
    monkeypatch.setenv("GROUND_TRUTH_DB_PATH", str(tmp_path / "ledger.db"))

    token = get_or_create_red_team_token()
    assert isinstance(token, str) and len(token) > 20
    assert os.path.isfile(token_path)

    # Check file permissions (must be mode 600 = -rw-------)
    mode = stat.S_IMODE(os.stat(token_path).st_mode)
    assert mode == 0o600

    # Reading it again returns the same token without regenerating
    token_cached = get_or_create_red_team_token()
    assert token_cached == token

