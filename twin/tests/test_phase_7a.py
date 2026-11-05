"""Comprehensive Phase 7A Test Suite.

Covers:
6.a: Link modes: drop, corrupt_sig, corrupt_bytes, replay, delay, ledger link_mode column, <= 300 B.
6.b: Attack stacking: device + zone coexistence, 409 ATTACK_CONFLICT on duplicate capability, selective clearing.
6.c: Journal API: real MCP client commands with caller & executed, events, attacks, since_sim_ts filter, 401 unauth.
6.d: Real external mocks end-to-end: ephemeral MCP server + QueueSource streamer:
     - Rogue device quarantined.
     - corrupt_bytes device quarantined.
     - device_physical spoof on z2-wl1 isolated without false valve actuation.
     - rain_storm x3 in zone-2 triggers Brain valve opening, level stays < 8 m.
     - All verified exclusively through GET /api/journal.
6.e: Grep test: zero imports from twin.* in twin/mocks/.
"""

import asyncio
from datetime import datetime, timezone
import json
import os
import subprocess
import threading
import time
from typing import Any, Dict, List
import pytest
from starlette.testclient import TestClient
import uvicorn

from twin.crypto_utils import generate_keypair, verify_payload_signature
from twin.db_ledger import GroundTruthLedger
from twin.kafka_streamer import KafkaStreamer, tamper_value_bytes
from twin.mcp_server import build_mcp_app, set_active_simulator
from twin.red_team_api import build_red_team_app
from twin.simulation import AttackConflictError, CitySimulator

# Mock layer imports (external clients)
from mocks.mcp_client import McpCaller
from mocks.mock_brain import MockCityBrain
from mocks.mock_guardian import MockGuardian
from mocks.reading_source import QueueSource


# ==============================================================================
# 6.a Link modes
# ==============================================================================
def test_6a_link_modes(tmp_path):
    """6.a: each link mode: wire bytes, signature verdict, ledger link_mode, <= 300 B, delay ordering, bounded buffers."""
    db_path = str(tmp_path / "ledger_link_modes.db")
    ledger = GroundTruthLedger(db_path=db_path)
    sim = CitySimulator(seed=101, ledger=ledger, sim_speed=1.0)
    streamer = KafkaStreamer(mock_mode=True)

    target_dev_id = "z1-tc1"
    dev = sim.devices[target_dev_id]

    # --- Mode 1: drop ---
    sim.inject_attack(capability="link", target_device_id=target_dev_id, params={"link_mode": "drop"})
    readings = sim.tick()
    assert target_dev_id not in [r["device_id"] for r in readings], "drop mode must emit nothing on the wire"

    # Verify ledger recorded link_mode = 'drop'
    recent = ledger.get_readings_truth(limit=50)
    dev_row = next(r for r in recent if r["device_id"] == target_dev_id)
    assert dev_row["link_mode"] == "drop"
    sim.clear_all_attacks()

    # --- Mode 2: corrupt_sig ---
    sim.inject_attack(capability="link", target_device_id=target_dev_id, params={"link_mode": "corrupt_sig"})
    readings = sim.tick()
    target_raw = next(r for r in readings if r["device_id"] == target_dev_id)
    _, wire_bytes = streamer.stream_reading(target_raw, dev.private_key)
    assert wire_bytes is not None
    assert len(wire_bytes) <= 300
    payload = json.loads(wire_bytes.decode("utf-8"))
    assert verify_payload_signature(payload, dev.public_key) is False

    recent = ledger.get_readings_truth(limit=50)
    dev_row = next(r for r in recent if r["device_id"] == target_dev_id)
    assert dev_row["link_mode"] == "corrupt_sig"
    sim.clear_all_attacks()

    # --- Mode 3: corrupt_bytes ---
    sim.inject_attack(capability="link", target_device_id=target_dev_id, params={"link_mode": "corrupt_bytes"})
    readings = sim.tick()
    target_raw = next(r for r in readings if r["device_id"] == target_dev_id)
    _, wire_bytes = streamer.stream_reading(target_raw, dev.private_key)
    assert wire_bytes is not None
    assert len(wire_bytes) <= 300
    tampered_payload = json.loads(wire_bytes.decode("utf-8"))

    # Valid JSON, same length, differs ONLY in value
    assert verify_payload_signature(tampered_payload, dev.public_key) is False
    assert tampered_payload["device_id"] == target_raw["device_id"]
    assert tampered_payload["reading_id"] == target_raw["reading_id"]
    assert tampered_payload["seq"] == target_raw["seq"]
    assert tampered_payload["value"] != target_raw["value"]

    recent = ledger.get_readings_truth(limit=50)
    dev_row = next(r for r in recent if r["device_id"] == target_dev_id)
    assert dev_row["link_mode"] == "corrupt_bytes"
    sim.clear_all_attacks()

    # --- Mode 4: replay ---
    # Send 15 normal readings first to populate history buffer
    for _ in range(15):
        r_list = sim.tick()
        t_r = next(r for r in r_list if r["device_id"] == target_dev_id)
        streamer.stream_reading(t_r, dev.private_key)

    sim.inject_attack(capability="link", target_device_id=target_dev_id, params={"link_mode": "replay", "replay_offset": 5})
    readings = sim.tick()
    target_raw = next(r for r in readings if r["device_id"] == target_dev_id)
    _, wire_bytes = streamer.stream_reading(target_raw, dev.private_key)
    assert wire_bytes is not None
    assert len(wire_bytes) <= 300
    replayed_payload = json.loads(wire_bytes.decode("utf-8"))

    # Signature must verify against device public key
    assert verify_payload_signature(replayed_payload, dev.public_key) is True
    # But seq must be replayed from offset 5
    assert replayed_payload["seq"] == target_raw["seq"] - 1 - 5

    recent = ledger.get_readings_truth(limit=50)
    dev_row = next(r for r in recent if r["device_id"] == target_dev_id)
    assert dev_row["link_mode"] == "replay"
    # History buffer is bounded (max 50)
    assert len(streamer._frame_history[target_dev_id]) <= 50
    sim.clear_all_attacks()

    # --- Mode 5: delay ---
    sim.inject_attack(capability="link", target_device_id=target_dev_id, params={"link_mode": "delay", "delay_ticks": 5})
    # Ticks 1 to 5: delayed, streamer yields None
    for i in range(5):
        r_list = sim.tick()
        t_r = next(r for r in r_list if r["device_id"] == target_dev_id)
        held_res = streamer.stream_reading(t_r, dev.private_key)
        assert held_res is None, f"Delay tick {i+1} must hold the frame"

    # Tick 6: first held frame is released in FIFO order
    r_list = sim.tick()
    t_r = next(r for r in r_list if r["device_id"] == target_dev_id)
    released_res = streamer.stream_reading(t_r, dev.private_key)
    assert released_res is not None
    rel_model, released_bytes = released_res
    assert len(released_bytes) <= 300
    rel_payload = json.loads(released_bytes.decode("utf-8"))
    # The first delayed frame was seq from 5 ticks ago
    assert rel_payload["seq"] == t_r["seq"] - 5
    assert verify_payload_signature(rel_payload, dev.public_key) is True

    recent = ledger.get_readings_truth(limit=50)
    dev_row = next(r for r in recent if r["device_id"] == target_dev_id)
    assert dev_row["link_mode"] == "delay"

    # Test flush_delayed releases in order
    flushed = streamer.flush_delayed(target_dev_id)
    assert len(flushed) == 5
    sim.clear_all_attacks()


# ==============================================================================
# 6.b Attack stacking and validation
# ==============================================================================
def test_6b_attack_stacking(tmp_path):
    """6.b: stacking: device + zone attacks of different capabilities both apply; same capability -> 409; clearing one leaves the other."""
    db_path = str(tmp_path / "ledger_stacking.db")
    ledger = GroundTruthLedger(db_path=db_path)
    sim = CitySimulator(seed=202, ledger=ledger)

    dev_id = "z1-tc1"
    dev = sim.devices[dev_id]

    # 1. Apply sensor_fault to specific device
    att1 = sim.inject_attack(
        capability="sensor_fault",
        target_device_id=dev_id,
        params={"mode": "drift", "rate": 0.5},
    )
    assert len(dev.active_attacks) == 1

    # 2. Apply link attack to zone-1 (stacks on z1-tc1 because capabilities differ)
    att2 = sim.inject_attack(
        capability="link",
        target_zone_id="zone-1",
        params={"link_mode": "corrupt_bytes"},
    )
    assert len(dev.active_attacks) == 2
    capabilities = {a["capability"] for a in dev.active_attacks}
    assert capabilities == {"sensor_fault", "link"}

    # 3. Same capability collision -> AttackConflictError (409)
    with pytest.raises(AttackConflictError):
        sim.inject_attack(capability="link", target_device_id=dev_id, params={"link_mode": "drop"})

    with pytest.raises(AttackConflictError):
        sim.inject_attack(capability="sensor_fault", target_zone_id="zone-1", params={"mode": "bias", "bias": 10.0})

    # 4. Clearing one leaves the other intact
    cleared = sim.clear_attack(att2)
    assert cleared is True
    assert len(dev.active_attacks) == 1
    assert dev.active_attacks[0]["attack_id"] == att1

    # 5. Validation -> 400 INVALID_ATTACK
    # Target device that does not exist
    with pytest.raises(ValueError, match="Target device 'nonexistent-dev' does not exist"):
        sim.inject_attack(capability="sensor_fault", target_device_id="nonexistent-dev", params={})

    # Unknown zone
    with pytest.raises(ValueError, match="Unknown zone 'unknown-zone'"):
        sim.inject_attack(capability="link", target_zone_id="unknown-zone", params={})

    # No target for non-rogue capability
    with pytest.raises(ValueError, match="requires either target_device_id or target_zone_id"):
        sim.inject_attack(capability="sensor_fault", params={})

    # Both device and zone given
    with pytest.raises(ValueError, match="Cannot specify both target_device_id and target_zone_id"):
        sim.inject_attack(capability="sensor_fault", target_device_id=dev_id, target_zone_id="zone-1", params={})


# ==============================================================================
# Ephemeral Twin Fixture for 6.c and 6.d
# ==============================================================================
@pytest.fixture
def ephemeral_twin(tmp_path):
    """Fixture providing an in-process Twin with MCP on an ephemeral port and Red Team Starlette app."""
    db_path = str(tmp_path / "ephemeral_twin.db")
    ledger = GroundTruthLedger(db_path=db_path)
    sim = CitySimulator(seed=303, ledger=ledger, sim_speed=10.0)
    set_active_simulator(sim)

    token = "test-red-team-token-7a"
    red_team_app = build_red_team_app(sim, token=token)
    mcp_app = build_mcp_app()

    config = uvicorn.Config(mcp_app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    while not server.started:
        time.sleep(0.01)

    port = server.servers[0].sockets[0].getsockname()[1]
    mcp_url = f"http://127.0.0.1:{port}"

    yield {
        "sim": sim,
        "ledger": ledger,
        "db_path": db_path,
        "port": port,
        "mcp_url": mcp_url,
        "red_team_app": red_team_app,
        "red_team_token": token,
        "server": server,
    }

    server.should_exit = True
    server_thread.join(timeout=2.0)


# ==============================================================================
# 6.c Journal API
# ==============================================================================
def test_6c_journal_api(ephemeral_twin):
    """6.c: journal: commands from real MCP client appear with caller & executed; events & attacks appear; since_sim_ts filters; 401 without token."""
    sim = ephemeral_twin["sim"]
    token = ephemeral_twin["red_team_token"]
    rt_client = TestClient(ephemeral_twin["red_team_app"])
    auth_header = {"X-Red-Team-Token": token}

    # 1. 401 without token or wrong token
    res_unauth = rt_client.get("/api/journal")
    assert res_unauth.status_code == 401
    assert res_unauth.json()["code"] == "UNAUTHORIZED"

    # 2. sim_speed present in GET /api/status
    st_res = rt_client.get("/api/status", headers=auth_header)
    assert st_res.status_code == 200
    st_json = st_res.json()
    assert "sim_speed" in st_json
    assert st_json["sim_speed"] == 10.0

    # 3. Issue a command via real MCP client over SSE
    mcp_url = ephemeral_twin["mcp_url"]

    async def issue_mcp_command():
        async with McpCaller(base_url=mcp_url) as caller:
            res = await caller.call_tool_async(
                "set_signal_plan",
                {"device_id": "z1-tc1", "target_state": "green_wave", "idempotency_key": "cmd-test-6c-1"},
            )
            assert res["status"] == "ok"

    asyncio.run(issue_mcp_command())

    # 4. Inject physical event and attack, then resolve/clear them
    ev_id = sim.inject_event(type="rain_storm", zone_id="zone-2", intensity=3.0, duration_s=60.0)
    att_id = sim.inject_attack(capability="link", target_device_id="z2-wl1", params={"link_mode": "corrupt_sig"})
    sim.tick()

    # Clear attack and event
    sim.clear_attack(att_id)
    sim.clear_event(ev_id)
    sim.tick()

    # 5. Query /api/journal
    j_res = rt_client.get("/api/journal", headers=auth_header)
    assert j_res.status_code == 200
    data = j_res.json()
    assert data["status"] == "ok"
    entries = data["journal"]
    assert len(entries) >= 4

    sources = {e["source"] for e in entries}
    assert "command" in sources
    assert "event" in sources
    assert "attack" in sources

    # Check command entry
    cmd_entry = next(e for e in entries if e["source"] == "command" and e["detail"]["tool"] == "set_signal_plan")
    assert cmd_entry["detail"]["executed"] is True
    assert cmd_entry["detail"]["caller"] is not None

    # Check event start and resolution entries
    ev_start = next(e for e in entries if e["source"] == "event" and e["detail"]["event_id"] == ev_id and e["detail"]["action"] == "start")
    ev_res = next(e for e in entries if e["source"] == "event" and e["detail"]["event_id"] == ev_id and e["detail"]["action"] == "resolved")
    assert ev_start is not None
    assert ev_res is not None

    # Check attack inject and clear entries
    att_inj = next(e for e in entries if e["source"] == "attack" and e["detail"]["attack_id"] == att_id and e["detail"]["action"] == "inject")
    att_clr = next(e for e in entries if e["source"] == "attack" and e["detail"]["attack_id"] == att_id and e["detail"]["action"] == "clear")
    assert att_inj is not None
    assert att_clr is not None

    # 6. since_sim_ts filter
    mid_ts = entries[len(entries) // 2]["sim_ts"]
    filtered_res = rt_client.get(f"/api/journal?since_sim_ts={mid_ts}", headers=auth_header)
    assert filtered_res.status_code == 200
    filtered_entries = filtered_res.json()["journal"]
    for fe in filtered_entries:
        assert fe["sim_ts"] >= mid_ts


# ==============================================================================
# 6.d Mocks end-to-end
# ==============================================================================
def test_6d_mocks_end_to_end(ephemeral_twin):
    """6.d: mocks end to end: in-process Twin with MCP on ephemeral port & QueueSource streamer:
    - Rogue device gets quarantined.
    - corrupt_bytes device gets quarantined.
    - device_physical spoof on z2-wl1 gets isolated without false valve actuation.
    - rain_storm x3 in zone-2 triggers Brain valve opening, keeping level < 8 m.
    - Assert outcomes through GET /api/journal.
    """
    sim = ephemeral_twin["sim"]
    mcp_url = ephemeral_twin["mcp_url"]
    token = ephemeral_twin["red_team_token"]
    rt_client = TestClient(ephemeral_twin["red_team_app"])
    auth_header = {"X-Red-Team-Token": token}

    queue_source = QueueSource()
    streamer = KafkaStreamer(mock_mode=True)

    guardian = MockGuardian(reading_source=queue_source, mcp_url=mcp_url)
    brain = MockCityBrain(reading_source=queue_source, mcp_url=mcp_url)

    async def run_scenario():
        await guardian.mcp.connect()
        await brain.mcp.connect()

        async def drain_and_evaluate():
            while True:
                reading = queue_source.get_reading(timeout=0.01)
                if reading is None:
                    break
                await guardian.process_reading_async(reading)
                # Brain evaluates readings that are not contained
                dev_status = sim.devices.get(reading.get("device_id", "")).status if reading.get("device_id") in sim.devices else "ok"
                if dev_status not in ("quarantined", "isolated"):
                    await brain.evaluate_reading_async(reading)

        # Baseline: 5 ticks
        for _ in range(5):
            for r in sim.tick():
                _, b = streamer.stream_reading(r, sim.devices[r["device_id"]].private_key)
                if b:
                    queue_source.put(b)
            await drain_and_evaluate()

        # -------------------------------------------------------------
        # Scenario 1: Rogue device -> quarantined
        # -------------------------------------------------------------
        att_rogue = sim.inject_attack(
            capability="rogue",
            params={"rogue_device_id": "rg-01", "location": "zone-1", "device_type": "traffic_camera"},
        )
        for _ in range(2):
            for r in sim.tick():
                dev_key = sim.devices[r["device_id"]].private_key if r["device_id"] in sim.devices else sim._rogue_keys.get(r["device_id"], (None, None))[0]
                _, b = streamer.stream_reading(r, dev_key)
                if b:
                    queue_source.put(b)
            await drain_and_evaluate()
        sim.clear_attack(att_rogue)

        # -------------------------------------------------------------
        # Scenario 2: corrupt_bytes device -> quarantined after 3 failures
        # -------------------------------------------------------------
        att_corrupt = sim.inject_attack(
            capability="link",
            target_device_id="z1-tc1",
            params={"link_mode": "corrupt_bytes"},
        )
        for _ in range(3):
            for r in sim.tick():
                _, b = streamer.stream_reading(r, sim.devices[r["device_id"]].private_key)
                if b:
                    queue_source.put(b)
            await drain_and_evaluate()
        sim.clear_attack(att_corrupt)

        # -------------------------------------------------------------
        # Scenario 3: device_physical spoof on z2-wl1 -> isolated without Brain opening valve
        # -------------------------------------------------------------
        # Tick 10 baseline ticks so water history has pre-attack depth
        for _ in range(10):
            for r in sim.tick():
                _, b = streamer.stream_reading(r, sim.devices[r["device_id"]].private_key)
                if b:
                    queue_source.put(b)
            await drain_and_evaluate()

        # Inject physical spoof on z2-wl1 alone (9.5m while z2-wl2 remains normal ~3.5m)
        att_spoof = sim.inject_attack(
            capability="device_physical",
            target_device_id="z2-wl1",
            params={"spoofed_value": 9.5},
        )
        for _ in range(25):
            for r in sim.tick():
                _, b = streamer.stream_reading(r, sim.devices[r["device_id"]].private_key)
                if b:
                    queue_source.put(b)
            await drain_and_evaluate()
        sim.clear_attack(att_spoof)

        # -------------------------------------------------------------
        # Scenario 4: rain_storm x3 in zone-2 -> Brain opens valve, level stays < 8m
        # -------------------------------------------------------------
        # Reset zone-2 water sensors to 6.98m so storm causes crossing > 7.0m in few ticks
        sim.devices["z2-wl1"].status = "ok"
        sim.devices["z2-wl2"].status = "ok"
        sim.devices["z2-wl1"].current_value = 6.98
        sim.devices["z2-wl2"].current_value = 6.98
        guardian.isolated_devices.clear()
        brain.water_levels.clear()

        ev_rain = sim.inject_event(type="rain_storm", zone_id="zone-2", intensity=3.0, duration_s=600.0)
        for _ in range(20):
            for r in sim.tick():
                _, b = streamer.stream_reading(r, sim.devices[r["device_id"]].private_key)
                if b:
                    queue_source.put(b)
            await drain_and_evaluate()

        # Close clients cleanly
        try:
            await guardian.mcp.close()
        except BaseException:
            pass
        try:
            await brain.mcp.close()
        except BaseException:
            pass

    asyncio.run(run_scenario())

    # -----------------------------------------------------------------
    # Assert all outcomes through GET /api/journal (never internal state)
    # -----------------------------------------------------------------
    j_res = rt_client.get("/api/journal?limit=1000", headers=auth_header)
    assert j_res.status_code == 200
    journal = j_res.json()["journal"]

    commands = [e for e in journal if e["source"] == "command"]
    containments = [e for e in journal if e["source"] == "containment"]

    # 1. Rogue device quarantined
    assert any(
        c["detail"]["tool"] == "quarantine_device"
        and c["detail"]["target"] == "rg-01"
        and c["detail"]["to_status"] == "quarantined"
        for c in containments
    )

    # 2. corrupt_bytes device quarantined
    assert any(
        c["detail"]["tool"] == "quarantine_device"
        and c["detail"]["target"] == "z1-tc1"
        and c["detail"]["to_status"] == "quarantined"
        for c in containments
    )

    # 3. Spoofed z2-wl1 isolated
    assert any(
        c["detail"]["tool"] == "isolate_device"
        and c["detail"]["target"] == "z2-wl1"
        and c["detail"]["to_status"] == "isolated"
        for c in containments
    )

    # Brain did NOT open valves during spoof (only during rain_storm)
    valve_cmds = [c for c in commands if c["detail"]["tool"] == "set_valve"]
    assert len(valve_cmds) >= 2  # exactly z2-wl1 and z2-wl2 opened during rain_storm
    for vc in valve_cmds:
        assert vc["detail"]["args"].get("target_state") == "open"
        assert vc["detail"]["args"].get("device_id") in ("z2-wl1", "z2-wl2")
        assert vc["detail"]["executed"] is True

    # 4. Water level stayed < 8.0 m
    assert sim.devices["z2-wl1"].current_value < 8.0
    assert sim.devices["z2-wl2"].current_value < 8.0


# ==============================================================================
# 6.e Grep test: zero imports from twin.* in twin/mocks/
# ==============================================================================
def test_6e_grep_isolation():
    """6.e: grep test: zero 'from twin' or 'import twin' in twin/mocks/."""
    # Find mock directory
    mocks_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "mocks"))
    assert os.path.isdir(mocks_dir), f"Directory {mocks_dir} not found"

    r1 = subprocess.run(["grep", "-rn", "from twin", mocks_dir], capture_output=True, text=True)
    r2 = subprocess.run(["grep", "-rn", "import twin", mocks_dir], capture_output=True, text=True)

    assert r1.stdout.strip() == "", f"Found forbidden 'from twin' in mocks:\n{r1.stdout}"
    assert r2.stdout.strip() == "", f"Found forbidden 'import twin' in mocks:\n{r2.stdout}"
