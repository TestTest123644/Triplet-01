"""Tests for Phase 5: MCP interface quality, validation, idempotency, containment, journals, auth, and transports."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import tempfile
from typing import Generator
import pytest
import uvicorn
import httpx

from mcp.client.session import ClientSession
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamable_http_client

from twin.db_ledger import GroundTruthLedger
from twin.keystore import DeviceKeystore
from twin.mcp_server import (
    build_mcp_app,
    dispatch,
    get_caller,
    get_city_state,
    get_command_result,
    get_device_key,
    get_device_state,
    get_reading,
    get_readings,
    isolate_device,
    list_devices,
    quarantine_device,
    reinstate_device,
    release_device,
    rollback_reading,
    set_active_simulator,
    set_signal_plan,
    set_valve,
    switch_grid,
)
from twin.red_team_api import build_red_team_app
from twin.simulation import CitySimulator


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def sim_env(tmp_path) -> Generator[CitySimulator, None, None]:
    db_path = str(tmp_path / "test_mcp_quality.db")
    keystore_path = str(tmp_path / "device_keys.json")
    ledger = GroundTruthLedger(db_path)
    sim = CitySimulator(seed=42, ledger=ledger, keystore_path=keystore_path)
    set_active_simulator(sim)
    yield sim
    sim.close()


class TestErrorShapesAndValidation:
    """Requirement 8.a: Each error code shape, allowed sets, and get_device_key null on unknown."""

    def test_error_shape_and_codes(self, sim_env: CitySimulator):
        # 1. DEVICE_NOT_FOUND
        res = get_device_state("nonexistent_device")
        assert res["status"] == "error"
        assert res["code"] == "DEVICE_NOT_FOUND"
        assert isinstance(res["message"], str)

        res = set_signal_plan("nonexistent_device", "normal")
        assert res["status"] == "error"
        assert res["code"] == "DEVICE_NOT_FOUND"

        # 2. ZONE_NOT_FOUND
        res = dispatch("medical_surge", "zone-99")
        assert res["status"] == "error"
        assert res["code"] == "ZONE_NOT_FOUND"
        assert "allowed" in res
        assert "zone-1" in res["allowed"]

        res = list_devices(zone_id="zone-99")
        assert res["status"] == "error"
        assert res["code"] == "ZONE_NOT_FOUND"
        assert "allowed" in res

        # 3. WRONG_DEVICE_TYPE
        res = set_signal_plan("z1-wl1", "normal")  # water_level
        assert res["status"] == "error"
        assert res["code"] == "WRONG_DEVICE_TYPE"

        res = set_valve("z1-tc1", "open")  # traffic_camera
        assert res["status"] == "error"
        assert res["code"] == "WRONG_DEVICE_TYPE"

        res = switch_grid("z1-tc1", "grid_a")  # traffic_camera
        assert res["status"] == "error"
        assert res["code"] == "WRONG_DEVICE_TYPE"

        # 4. INVALID_TARGET_STATE
        res = set_signal_plan("z1-tc1", "bogus_plan")
        assert res["status"] == "error"
        assert res["code"] == "INVALID_TARGET_STATE"
        assert "allowed" in res
        assert "green_wave" in res["allowed"]

        res = set_valve("z1-wl1", "bogus_valve")
        assert res["status"] == "error"
        assert res["code"] == "INVALID_TARGET_STATE"
        assert "allowed" in res

        res = switch_grid("z1-pl1", "bogus_grid")
        assert res["status"] == "error"
        assert res["code"] == "INVALID_TARGET_STATE"
        assert "allowed" in res

        # 5. INVALID_INCIDENT_TYPE
        res = dispatch("INVALID_UPPERCASE", "zone-1")
        assert res["status"] == "error"
        assert res["code"] == "INVALID_INCIDENT_TYPE"

        res = dispatch("incident with space", "zone-1")
        assert res["status"] == "error"
        assert res["code"] == "INVALID_INCIDENT_TYPE"

        # 6. INVALID_ARGUMENT (length > 64)
        long_str = "x" * 65
        res = dispatch("medical_surge", long_str)
        assert res["status"] == "error"
        assert res["code"] == "INVALID_ARGUMENT"

        res = get_device_state(long_str)
        assert res["status"] == "error"
        assert res["code"] == "INVALID_ARGUMENT"

        res = get_command_result(long_str)
        assert res["status"] == "error"
        assert res["code"] == "INVALID_ARGUMENT"

        res = get_reading(long_str)
        assert res["status"] == "error"
        assert res["code"] == "INVALID_ARGUMENT"

        res = get_readings(device_id=long_str)
        assert res["status"] == "error"
        assert res["code"] == "INVALID_ARGUMENT"

        res = get_readings(limit=0)
        assert res["status"] == "error"
        assert res["code"] == "INVALID_ARGUMENT"

        res = get_readings(limit=501)
        assert res["status"] == "error"
        assert res["code"] == "INVALID_ARGUMENT"

        # 7. INVALID_TIMESTAMP
        res = get_readings(since_ts="not-a-timestamp")
        assert res["status"] == "error"
        assert res["code"] == "INVALID_TIMESTAMP"

        res = get_readings(since_ts="2026-10-17 12:00:00")  # missing Z
        assert res["status"] == "error"
        assert res["code"] == "INVALID_TIMESTAMP"

        # 8. COMMAND_NOT_FOUND
        res = get_command_result("nonexistent_idempotency_key")
        assert res["status"] == "error"
        assert res["code"] == "COMMAND_NOT_FOUND"

        # 9. READING_NOT_FOUND
        res = get_reading("r-nonexistent-1")
        assert res["status"] == "error"
        assert res["code"] == "READING_NOT_FOUND"

        res = rollback_reading("r-nonexistent-1")
        assert res["status"] == "error"
        assert res["code"] == "READING_NOT_FOUND"

    def test_get_device_key_unknown_returns_none(self, sim_env: CitySimulator):
        """get_device_key must return None for unknown device_id, never an error dict."""
        pub_key = get_device_key("nonexistent_device")
        assert pub_key is None

        # Registered device returns string
        pub_key = get_device_key("z1-tc1")
        assert isinstance(pub_key, str)
        assert len(pub_key) > 0

    def test_unrecognized_valid_incident_type_dispatched_as_generic(self, sim_env: CitySimulator):
        """Unrecognized valid incident_type dispatched as generic with note."""
        res = dispatch("chemical_spill", "zone-1")
        assert res["status"] == "ok"
        assert res["zone_id"] == "zone-1"
        assert res.get("note") == "unrecognized incident_type, dispatched as generic"


class TestActuationAndAllowedStates:
    """Requirement 8.b: Half valve ratio, allowed states accepted, bogus states rejected."""

    def test_valve_states_and_ratios(self, sim_env: CitySimulator):
        dev = sim_env.devices["z1-wl1"]

        # 'half' -> 0.5
        res = set_valve("z1-wl1", "half")
        assert res["status"] == "ok"
        assert res["valve_position"] == "half"
        assert dev.actuator_state["valve_ratio"] == 0.5

        # 'open' -> 1.0
        res = set_valve("z1-wl1", "open")
        assert res["status"] == "ok"
        assert dev.actuator_state["valve_ratio"] == 1.0

        # 'closed' -> 0.0
        res = set_valve("z1-wl1", "closed")
        assert res["status"] == "ok"
        assert dev.actuator_state["valve_ratio"] == 0.0

        # Percentages: 25%, 75%, 100%, 0%
        res = set_valve("z1-wl1", "25%")
        assert res["status"] == "ok"
        assert dev.actuator_state["valve_ratio"] == 0.25

        res = set_valve("z1-wl1", "100%")
        assert res["status"] == "ok"
        assert dev.actuator_state["valve_ratio"] == 1.0

        # Out of bounds percentage or bogus format
        res = set_valve("z1-wl1", "150%")
        assert res["status"] == "error"
        assert res["code"] == "INVALID_TARGET_STATE"
        assert "allowed" in res

        res = set_valve("z1-wl1", "-10%")
        assert res["status"] == "error"
        assert res["code"] == "INVALID_TARGET_STATE"

    def test_signal_plans(self, sim_env: CitySimulator):
        allowed_plans = ["normal", "green_wave", "red_restriction", "all_red", "adaptive"]
        for plan in allowed_plans:
            res = set_signal_plan("z1-tc1", plan)
            assert res["status"] == "ok"
            assert res["signal_plan"] == plan
            assert sim_env.devices["z1-tc1"].actuator_state["signal_plan"] == plan

        res = set_signal_plan("z1-tc1", "unsupported_plan")
        assert res["status"] == "error"
        assert res["code"] == "INVALID_TARGET_STATE"
        assert res["allowed"] == allowed_plans

    def test_grid_states(self, sim_env: CitySimulator):
        allowed_states = ["grid_a", "grid_b", "island", "shed", "standby"]
        for state in allowed_states:
            res = switch_grid("z1-pl1", state)
            assert res["status"] == "ok"
            assert res["grid_state"] == state
            assert sim_env.devices["z1-pl1"].actuator_state["grid_state"] == state

        res = switch_grid("z1-pl1", "solar_only")
        assert res["status"] == "error"
        assert res["code"] == "INVALID_TARGET_STATE"
        assert res["allowed"] == allowed_states


class TestIdempotency:
    """Requirement 8.c: Replay does not re-execute; conflict detected."""

    def test_idempotent_replay_and_conflict(self, sim_env: CitySimulator):
        # 1. First execution
        res1 = dispatch("medical_surge", "zone-1", idempotency_key="idemp-disp-1")
        assert res1["status"] == "ok"
        assert "idempotent_replay" not in res1
        disp_id1 = res1["dispatch_id"]
        assert len(sim_env.zone_dispatches["zone-1"]) == 1

        # 2. Replay with identical args
        res2 = dispatch("medical_surge", "zone-1", idempotency_key="idemp-disp-1")
        assert res2["status"] == "ok"
        assert res2["idempotent_replay"] is True
        assert res2["dispatch_id"] == disp_id1
        # Crucial check: physical execution count remains 1!
        assert len(sim_env.zone_dispatches["zone-1"]) == 1

        # 3. get_command_result returns the cached result
        cached = get_command_result("idemp-disp-1")
        assert cached["status"] == "ok"
        assert cached["dispatch_id"] == disp_id1

        # 4. Conflict: same key, different args (different zone)
        conflict = dispatch("medical_surge", "zone-2", idempotency_key="idemp-disp-1")
        assert conflict["status"] == "error"
        assert conflict["code"] == "IDEMPOTENCY_CONFLICT"

        # 5. Conflict: same key, different tool
        conflict2 = set_signal_plan("z1-tc1", "normal", idempotency_key="idemp-disp-1")
        assert conflict2["status"] == "error"
        assert conflict2["code"] == "IDEMPOTENCY_CONFLICT"


class TestContainment:
    """Requirement 8.d: Contained devices refuse actuation, active rogue quarantine, reinstate/release."""

    def test_contained_device_refuses_actuation(self, sim_env: CitySimulator):
        # Quarantine traffic camera
        res_q = quarantine_device("z1-tc1")
        assert res_q["status"] == "ok"
        assert res_q["new_status"] == "quarantined"

        # Attempt actuation -> DEVICE_CONTAINED
        res_act = set_signal_plan("z1-tc1", "green_wave")
        assert res_act["status"] == "error"
        assert res_act["code"] == "DEVICE_CONTAINED"
        assert "quarantined" in res_act["message"]

        # Isolate water level
        isolate_device("z1-wl1")
        res_act2 = set_valve("z1-wl1", "open")
        assert res_act2["status"] == "error"
        assert res_act2["code"] == "DEVICE_CONTAINED"
        assert "isolated" in res_act2["message"]

        # Isolate power load
        isolate_device("z1-pl1")
        res_act3 = switch_grid("z1-pl1", "grid_b")
        assert res_act3["status"] == "error"
        assert res_act3["code"] == "DEVICE_CONTAINED"

    @pytest.mark.asyncio
    async def test_active_rogue_quarantine_repeatable_and_network_blocklist(self, sim_env: CitySimulator):
        # 1. Inject rogue attack (max rogue_id length is 6)
        atk_id = sim_env.inject_attack(
            capability="rogue",
            params={"rogue_device_id": "rg-99", "zone_id": "zone-1", "frequency_hz": 10},
        )
        assert any(
            atk.get("params", {}).get("rogue_device_id") == "rg-99"
            for atk in sim_env.rogue_attacks.values()
        )

        # Tick 1: rogue device emits reading on wire
        readings = sim_env.tick()
        rogue_emitted = [r for r in readings if r["device_id"] == "rg-99"]
        assert len(rogue_emitted) == 1

        # 2. First quarantine
        res_q1 = quarantine_device("rg-99")
        assert res_q1["status"] == "ok"
        assert res_q1["device_id"] == "rg-99"
        assert res_q1.get("blocked") is True
        assert "note" not in res_q1

        # 3. Repeat quarantine (repeatable, returns ok + note: "already blocked")
        res_q2 = quarantine_device("rg-99")
        assert res_q2["status"] == "ok"
        assert res_q2["device_id"] == "rg-99"
        assert res_q2.get("blocked") is True
        assert res_q2.get("note") == "already blocked"

        # 4. Ticks 2 and 3: no wire frames while blocked, but truth rows continue with wire_sig NULL
        readings_b1 = sim_env.tick()
        assert not any(r["device_id"] == "rg-99" for r in readings_b1)
        readings_b2 = sim_env.tick()
        assert not any(r["device_id"] == "rg-99" for r in readings_b2)

        # Truth rows check
        with sim_env.ledger._get_connection(read_only=True) as conn:
            cur = conn.execute(
                "SELECT seq, status, wire_sig, is_attacked, attack_type FROM readings_truth WHERE device_id = 'rg-99' ORDER BY seq"
            )
            rows = [dict(r) for r in cur.fetchall()]
        assert len(rows) == 3
        # Tick 1 was emitted (status ok)
        assert rows[0]["status"] == "ok"
        assert rows[0]["seq"] == 1
        # Ticks 2 and 3 were blocked by network: status quarantined, wire_sig NULL, seq kept counting
        assert rows[1]["status"] == "quarantined"
        assert rows[1]["wire_sig"] is None
        assert rows[1]["seq"] == 2
        assert rows[2]["status"] == "quarantined"
        assert rows[2]["wire_sig"] is None
        assert rows[2]["seq"] == 3

        # 5. Red Team API inspection: attack stays active with blocked_frames rising
        red_team_app = build_red_team_app(sim_env, token="rt-secret")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=red_team_app), base_url="http://test") as client:
            res_active = await client.get("/api/attack/active", headers={"X-Red-Team-Token": "rt-secret"})
            assert res_active.status_code == 200
            active_data = res_active.json()
            assert len(active_data["active_attacks"]) == 1
            assert active_data["active_attacks"][0]["attack_id"] == atk_id
            assert active_data["active_attacks"][0]["blocked_frames"] == 2

            res_status = await client.get("/api/status", headers={"X-Red-Team-Token": "rt-secret"})
            assert res_status.status_code == 200
            status_data = res_status.json()
            assert status_data["blocked_frames"]["rg-99"] == 2

        # 6. Release device unblocks it and emission resumes
        res_rel = release_device("rg-99")
        assert res_rel["status"] == "ok"
        assert res_rel["device_id"] == "rg-99"
        assert res_rel.get("unblocked") is True

        readings_after_rel = sim_env.tick()
        rogue_resumed = [r for r in readings_after_rel if r["device_id"] == "rg-99"]
        assert len(rogue_resumed) == 1
        assert rogue_resumed[0]["seq"] == 4

        # 7. Containment log has all rows
        conts = sim_env.ledger.get_recent_containment_log(10)
        rg_conts = [c for c in conts if c["target"] == "rg-99"]
        assert len(rg_conts) == 3
        # In DESC order (newest first): release, quarantine 2, quarantine 1
        assert rg_conts[0]["tool"] == "release_device"
        assert rg_conts[1]["tool"] == "quarantine_device"
        assert rg_conts[2]["tool"] == "quarantine_device"

    def test_reinstate_and_release_lenient(self, sim_env: CitySimulator):
        # 1. Device already ok
        res = reinstate_device("z1-tc1")
        assert res["status"] == "ok"
        assert res["device_id"] == "z1-tc1"
        assert res.get("note") == "device already ok"

        res = release_device("z1-tc1")
        assert res["status"] == "ok"
        assert res.get("note") == "device already ok"

        # 2. Isolate then reinstate
        isolate_device("z1-tc1")
        assert sim_env.devices["z1-tc1"].status == "isolated"
        res = reinstate_device("z1-tc1")
        assert res["status"] == "ok"
        assert res["new_status"] == "ok"
        assert sim_env.devices["z1-tc1"].status == "ok"

        # 3. Quarantine then release
        quarantine_device("z1-tc1")
        assert sim_env.devices["z1-tc1"].status == "quarantined"
        res = release_device("z1-tc1")
        assert res["status"] == "ok"
        assert res["new_status"] == "ok"
        assert sim_env.devices["z1-tc1"].status == "ok"


class TestJournals:
    """Requirement 8.e: Ledger journals record commands and containment calls."""

    def test_commands_and_containment_logging(self, sim_env: CitySimulator):
        # 1. Normal executed command
        sim_env.handle_dispatch("medical_surge", "zone-1", caller="brain")
        cmds = sim_env.ledger.get_recent_commands_log(5)
        assert len(cmds) >= 1
        last = cmds[0]
        assert last["tool"] == "dispatch"
        assert last["caller"] == "brain"
        assert last["executed"] == 1
        assert last["refused_code"] is None

        # 2. Refused command on contained device
        sim_env.update_device_status("z1-tc1", "quarantined", tool_name="quarantine_device", caller="guardian")
        sim_env.handle_set_signal_plan("z1-tc1", "green_wave", caller="brain")
        cmds = sim_env.ledger.get_recent_commands_log(5)
        last = cmds[0]
        assert last["tool"] == "set_signal_plan"
        assert last["caller"] == "brain"
        assert last["executed"] == 0
        assert last["refused_code"] == "DEVICE_CONTAINED"

        # 3. Actuator attack: logged executed=0, refused_code="ACTUATOR_ATTACK", while response is "ok"
        sim_env.inject_attack(capability="actuator", target_device_id="z1-wl1")
        res = sim_env.handle_set_valve("z1-wl1", "open", caller="brain")
        assert res["status"] == "ok"  # Partner sees "ok"
        cmds = sim_env.ledger.get_recent_commands_log(5)
        last = cmds[0]
        assert last["tool"] == "set_valve"
        assert last["caller"] == "brain"
        assert last["executed"] == 0
        assert last["refused_code"] == "ACTUATOR_ATTACK"

        # 4. Containment log
        cont_entries = sim_env.ledger.get_recent_containment_log(5)
        assert len(cont_entries) >= 1
        q_entry = [c for c in cont_entries if c["target"] == "z1-tc1"][0]
        assert q_entry["tool"] == "quarantine_device"
        assert q_entry["caller"] == "guardian"
        assert q_entry["from_status"] == "ok"
        assert q_entry["to_status"] == "quarantined"


class TestEndToEndAuthAndTransports:
    """Requirement 8.f: Live uvicorn server testing over /sse and /mcp with bearer token auth."""

    def test_parse_auth_tokens_strict_principal_token(self, monkeypatch):
        """Verify parse_auth_tokens strictly parses principal:token into {token: principal}."""
        from twin.mcp_server import parse_auth_tokens

        monkeypatch.setenv("MCP_AUTH_TOKENS", "brain:tok-b,guardian:tok-g,scenario:tok-s")
        parsed = parse_auth_tokens()
        assert parsed == {
            "tok-b": "brain",
            "tok-g": "guardian",
            "tok-s": "scenario",
        }

    def test_parse_auth_tokens_newline_separated(self, monkeypatch):
        """Verify parse_auth_tokens handles newline and mixed delimiters."""
        from twin.mcp_server import parse_auth_tokens

        monkeypatch.setenv("MCP_AUTH_TOKENS", "brain:tok-b\nguardian:tok-g\nscenario:tok-s\n")
        parsed = parse_auth_tokens()
        assert parsed == {
            "tok-b": "brain",
            "tok-g": "guardian",
            "tok-s": "scenario",
        }

    @pytest.mark.asyncio
    async def test_live_auth_sse_and_mcp(self, tmp_path, monkeypatch):
        port = find_free_port()
        db_path = str(tmp_path / "live_mcp_test.db")
        keystore_path = str(tmp_path / "live_device_keys.json")

        monkeypatch.setenv("GROUND_TRUTH_DB_PATH", db_path)
        monkeypatch.setenv("KEYSTORE_PATH", keystore_path)
        monkeypatch.setenv("MCP_AUTH_TOKENS", "brain:brain-secret-token,guardian:guardian-secret-token")
        monkeypatch.setenv("MCP_ALLOWED_HOSTS", "localhost,127.0.0.1,twin_layer")

        ledger = GroundTruthLedger(db_path)
        sim = CitySimulator(seed=123, ledger=ledger, keystore_path=keystore_path)
        set_active_simulator(sim)

        app = build_mcp_app()
        config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="error")
        server = uvicorn.Server(config)
        server_task = asyncio.create_task(server.serve())
        await asyncio.sleep(0.4)

        base_url = f"http://127.0.0.1:{port}"

        try:
            # 1. Direct HTTP test on /sse and /mcp without auth -> 401
            async with httpx.AsyncClient() as client:
                res_no_auth = await client.get(f"{base_url}/sse")
                assert res_no_auth.status_code == 401
                body = res_no_auth.json()
                assert body["status"] == "error"
                assert body["code"] == "UNAUTHORIZED"

                res_bad_auth = await client.get(
                    f"{base_url}/sse", headers={"Authorization": "Bearer wrong-token"}
                )
                assert res_bad_auth.status_code == 401

                res_mcp_no_auth = await client.post(f"{base_url}/mcp", json={})
                assert res_mcp_no_auth.status_code == 401

            # 2. Official MCP client over /sse with Brain auth
            brain_headers = {"Authorization": "Bearer brain-secret-token"}
            async with sse_client(f"{base_url}/sse", headers=brain_headers) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    # Actuate valve
                    call_res = await session.call_tool("set_valve", {"device_id": "z1-wl1", "target_state": "half"})
                    assert not call_res.is_error
                    # Verify caller recorded in commands_log is "brain"
                    cmds = sim.ledger.get_recent_commands_log(5)
                    assert len(cmds) >= 1
                    assert cmds[-1]["tool"] == "set_valve"
                    assert cmds[-1]["caller"] == "brain"

            # 3. Official MCP client over /mcp (Streamable HTTP) with Guardian auth
            guardian_headers = {"Authorization": "Bearer guardian-secret-token"}
            async with httpx.AsyncClient(headers=guardian_headers) as http_client:
                async with streamable_http_client(f"{base_url}/mcp", http_client=http_client) as (read_s, write_s):
                    async with ClientSession(read_s, write_s) as session:
                        await session.initialize()
                        # Quarantine device
                        call_res = await session.call_tool("quarantine_device", {"device_id": "z1-tc2"})
                        assert not call_res.is_error
                        # Verify caller recorded in containment_log is "guardian"
                        conts = sim.ledger.get_recent_containment_log(5)
                        assert len(conts) >= 1
                        assert conts[-1]["tool"] == "quarantine_device"
                        assert conts[-1]["caller"] == "guardian"

            # 4. Host header check: "twin_layer:8000" accepted (allowed_hosts)
            async with httpx.AsyncClient(headers={"Host": "twin_layer:8000", **brain_headers}) as client:
                async with client.stream("GET", f"{base_url}/sse") as res_host:
                    assert res_host.status_code == 200

        finally:
            server.should_exit = True
            await server_task
            sim.close()

    @pytest.mark.asyncio
    async def test_lan_access_default_and_rejected_host(self, tmp_path, monkeypatch, caplog):
        """Regression test for LAN access (Phase 5.1):

        - Default config (MCP_ALLOWED_HOSTS unset) accepts Host '192.168.1.50:8000' over /sse and /mcp.
        - With MCP_ALLOWED_HOSTS='localhost', the same Host gets 421 and caplog has no ERROR record.
        """
        db_path1 = str(tmp_path / "lan_test1.db")
        keystore_path1 = str(tmp_path / "lan_keys1.json")
        monkeypatch.setenv("GROUND_TRUTH_DB_PATH", db_path1)
        monkeypatch.setenv("KEYSTORE_PATH", keystore_path1)

        # Part 1: Default config (MCP_ALLOWED_HOSTS unset) accepts LAN Host "192.168.1.50:8000"
        monkeypatch.delenv("MCP_ALLOWED_HOSTS", raising=False)
        monkeypatch.delenv("MCP_AUTH_TOKENS", raising=False)

        ledger1 = GroundTruthLedger(db_path1)
        sim1 = CitySimulator(seed=111, ledger=ledger1, keystore_path=keystore_path1)
        set_active_simulator(sim1)

        port1 = find_free_port()
        app1 = build_mcp_app()
        cfg1 = uvicorn.Config(app=app1, host="127.0.0.1", port=port1, log_level="error")
        svr1 = uvicorn.Server(cfg1)
        t1 = asyncio.create_task(svr1.serve())
        await asyncio.sleep(0.4)

        base_url1 = f"http://127.0.0.1:{port1}"
        lan_headers = {"Host": "192.168.1.50:8000"}

        try:
            # Over SSE: 200 and a tool call works
            async with sse_client(f"{base_url1}/sse", headers=lan_headers) as (r, w):
                async with ClientSession(r, w) as session:
                    await session.initialize()
                    res = await session.call_tool("list_devices", {})
                    assert not res.is_error
                    assert len(res.structured_content.get("result", [])) == 42

            # Over /mcp (Streamable HTTP): tool call works
            async with httpx.AsyncClient(headers=lan_headers) as http_client:
                async with streamable_http_client(f"{base_url1}/mcp", http_client=http_client) as (r, w):
                    async with ClientSession(r, w) as session:
                        await session.initialize()
                        res = await session.call_tool("list_devices", {})
                        assert not res.is_error
                        assert len(res.structured_content.get("result", [])) == 42
        finally:
            svr1.should_exit = True
            await t1
            sim1.close()

        # Part 2: With MCP_ALLOWED_HOSTS="localhost", Host "192.168.1.50:8000" gets 421 and no ERROR log
        monkeypatch.setenv("MCP_ALLOWED_HOSTS", "localhost")
        port2 = find_free_port()
        db_path2 = str(tmp_path / "lan_test2.db")
        keystore_path2 = str(tmp_path / "lan_keys2.json")
        ledger2 = GroundTruthLedger(db_path2)
        sim2 = CitySimulator(seed=222, ledger=ledger2, keystore_path=keystore_path2)
        set_active_simulator(sim2)

        app2 = build_mcp_app()
        cfg2 = uvicorn.Config(app=app2, host="127.0.0.1", port=port2, log_level="error")
        svr2 = uvicorn.Server(cfg2)
        t2 = asyncio.create_task(svr2.serve())
        await asyncio.sleep(0.4)

        base_url2 = f"http://127.0.0.1:{port2}"
        caplog.clear()

        try:
            async with httpx.AsyncClient(headers=lan_headers) as client:
                # SSE gets 421
                async with client.stream("GET", f"{base_url2}/sse") as res_sse:
                    assert res_sse.status_code == 421
                    body_sse = await res_sse.aread()
                    assert b"Invalid Host header" in body_sse

                # MCP gets 421
                res_mcp = await client.post(f"{base_url2}/mcp", json={})
                assert res_mcp.status_code == 421
                assert "Invalid Host header" in res_mcp.text

            # caplog must have no ERROR records
            error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
            assert len(error_records) == 0
        finally:
            svr2.should_exit = True
            await t2
            sim2.close()
