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
import contextlib
from datetime import datetime, timezone
import json
import os
import subprocess
import sys
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
@contextlib.contextmanager
def running_mcp_server(sim: CitySimulator):
    """Start an in-process MCP server on an ephemeral port with a fresh MCP app and session manager."""
    set_active_simulator(sim)
    app = build_mcp_app()
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)

    thread_exc: List[BaseException] = []

    def _runner():
        try:
            server.run()
        except BaseException as exc:
            thread_exc.append(exc)

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()

    deadline = time.time() + 10.0
    while not server.started and thread.is_alive() and time.time() < deadline:
        time.sleep(0.01)

    if not server.started:
        server.should_exit = True
        thread.join(timeout=1.0)
        if thread_exc:
            raise RuntimeError(f"MCP server thread died during startup: {thread_exc[0]}") from thread_exc[0]
        if not thread.is_alive():
            raise RuntimeError("MCP server thread exited before starting without capturing an exception.")
        raise TimeoutError("Timed out after 10s waiting for MCP server to start.")

    port = server.servers[0].sockets[0].getsockname()[1]
    mcp_url = f"http://127.0.0.1:{port}"

    try:
        yield {"server": server, "thread": thread, "port": port, "mcp_url": mcp_url}
    finally:
        server.should_exit = True
        thread.join(timeout=5.0)
        if thread.is_alive():
            raise RuntimeError("MCP server thread did not stop within 5s after should_exit=True")


@pytest.fixture
def ephemeral_twin(tmp_path):
    """Fixture providing an in-process Twin with MCP on an ephemeral port and Red Team Starlette app."""
    db_path = str(tmp_path / "ephemeral_twin.db")
    ledger = GroundTruthLedger(db_path=db_path)
    sim = CitySimulator(seed=303, ledger=ledger, sim_speed=10.0)
    token = "test-red-team-token-7a"
    red_team_app = build_red_team_app(sim, token=token)

    with running_mcp_server(sim) as s_info:
        yield {
            "sim": sim,
            "ledger": ledger,
            "db_path": db_path,
            "port": s_info["port"],
            "mcp_url": s_info["mcp_url"],
            "red_team_app": red_team_app,
            "red_team_token": token,
            "server": s_info["server"],
        }


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

    # 7. Check stable "key" per entry
    for e in entries:
        assert "key" in e
        prefix = e["key"].split(":")[0]
        assert prefix in ("command", "containment", "event_start", "event_resolved", "attack_inject", "attack_clear")

    # 8. Invalid since_sim_ts -> 400 INVALID_ARGUMENT
    inv_res = rt_client.get("/api/journal?since_sim_ts=not-rfc3339", headers=auth_header)
    assert inv_res.status_code == 400
    assert inv_res.json()["code"] == "INVALID_ARGUMENT"

    # 9. Unknown link_mode -> 400 INVALID_ATTACK
    bad_atk = rt_client.post(
        "/api/attack/inject",
        headers=auth_header,
        json={"capability": "link", "target_device_id": "z1-tc1", "params": {"link_mode": "foo"}},
    )
    assert bad_atk.status_code == 400
    assert bad_atk.json()["code"] == "INVALID_ATTACK"


# ==============================================================================
# Helper for feeding simulation stream to mocks without state staging
# ==============================================================================
async def stream_and_process_tick(sim, streamer, queue_source, guardian, brain=None):
    for r in sim.tick():
        dev_id = r["device_id"]
        dev_key = sim.signing_key_for(dev_id)
        if dev_key:
            res = streamer.stream_reading(r, dev_key)
            if res:
                queue_source.put(res[1])
            if streamer.last_emitted_backlog:
                for _, b in streamer.last_emitted_backlog:
                    queue_source.put(b)
                streamer.last_emitted_backlog.clear()

    while True:
        item = queue_source.get_reading(timeout=0.001)
        if not item:
            break
        await guardian.process_reading_async(item)
        if brain is not None:
            await brain.evaluate_reading_async(item)


# ==============================================================================
# 9a: False-positive guard
# ==============================================================================
def test_9a_false_positive_guard(tmp_path):
    """9a: False-positive guard: clean run (1 sim-min @ SIM_SPEED 1, 30 sim-min @ SIM_SPEED 60) -> 0 containment, 0 commands."""
    db_path = str(tmp_path / "fp_guard.db")
    ledger = GroundTruthLedger(db_path=db_path)
    sim = CitySimulator(seed=101, ledger=ledger, sim_speed=1.0)

    token = "test-token-9a"
    red_team_app = build_red_team_app(sim, token=token)

    with running_mcp_server(sim) as s_info:
        mcp_url = s_info["mcp_url"]
        queue_source = QueueSource()
        streamer = KafkaStreamer(mock_mode=True)
        guardian = MockGuardian(reading_source=queue_source, mcp_url=mcp_url)
        brain = MockCityBrain(reading_source=queue_source, mcp_url=mcp_url)

        async def run():
            await guardian.mcp.connect()
            await brain.mcp.connect()
            await brain.refresh_zone_cameras()

            # Part 1: 1 sim-min @ SIM_SPEED 1.0 (600 ticks = 60s)
            for _ in range(600):
                await stream_and_process_tick(sim, streamer, queue_source, guardian, brain)

            # Part 2: 30 sim-min @ SIM_SPEED 60.0 (300 ticks = 1800s)
            sim.clock.sim_speed = 60.0
            for _ in range(300):
                await stream_and_process_tick(sim, streamer, queue_source, guardian, brain)

            await brain.mcp.close()
            await guardian.mcp.close()

        asyncio.run(run())

    # Verify through journal
    rt_client = TestClient(red_team_app)
    j_res = rt_client.get("/api/journal?limit=1000", headers={"X-Red-Team-Token": token})
    assert j_res.status_code == 200
    journal = j_res.json()["journal"]

    containments = [e for e in journal if e["source"] == "containment"]
    commands = [e for e in journal if e["source"] == "command"]

    assert len(containments) == 0, f"Expected 0 containment rows, found {len(containments)}"
    assert len(commands) == 0, f"Expected 0 commands, found {len(commands)}"


# ==============================================================================
# 9b: Storm x3 in zone-2 mitigation
# ==============================================================================
def test_9b_storm_valve_mitigation(tmp_path):
    """9b: Storm x3 in zone-2 from natural start (3.5 m), SIM_SPEED 60, 30 sim-min:
    without Brain -> level > 8 m; with Brain -> set_valve open journaled and max level < 8 m.
    """
    # --- Variant 1: Without Brain ---
    db1 = str(tmp_path / "storm_no_brain.db")
    sim1 = CitySimulator(seed=404, ledger=GroundTruthLedger(db_path=db1), sim_speed=60.0)
    sim1.inject_event(type="rain_storm", zone_id="zone-2", intensity=3.0, duration_s=1800.0)
    max_level_no_brain = 0.0
    for _ in range(300):
        sim1.tick()
        lvl = max(sim1.devices["z2-wl1"].current_value, sim1.devices["z2-wl2"].current_value)
        if lvl > max_level_no_brain:
            max_level_no_brain = lvl

    assert max_level_no_brain > 8.0, f"Expected unmitigated storm level > 8.0m, got {max_level_no_brain}"

    # --- Variant 2: With Brain ---
    db2 = str(tmp_path / "storm_with_brain.db")
    sim2 = CitySimulator(seed=404, ledger=GroundTruthLedger(db_path=db2), sim_speed=60.0)
    token = "test-token-9b"
    red_team_app = build_red_team_app(sim2, token=token)

    with running_mcp_server(sim2) as s_info:
        mcp_url = s_info["mcp_url"]
        queue_source = QueueSource()
        streamer = KafkaStreamer(mock_mode=True)
        brain = MockCityBrain(reading_source=queue_source, mcp_url=mcp_url)

        sim2.inject_event(type="rain_storm", zone_id="zone-2", intensity=3.0, duration_s=1800.0)
        max_level_with_brain = 0.0

        async def run():
            await brain.mcp.connect()
            await brain.refresh_zone_cameras()
            nonlocal max_level_with_brain
            for _ in range(300):
                for r in sim2.tick():
                    res = streamer.stream_reading(r, sim2.devices[r["device_id"]].private_key)
                    if res:
                        queue_source.put(res[1])
                while True:
                    item = queue_source.get_reading(timeout=0.001)
                    if not item:
                        break
                    await brain.evaluate_reading_async(item)

                lvl = max(sim2.devices["z2-wl1"].current_value, sim2.devices["z2-wl2"].current_value)
                if lvl > max_level_with_brain:
                    max_level_with_brain = lvl

            await brain.mcp.close()

        asyncio.run(run())

    assert max_level_with_brain < 8.0, f"Expected mitigated storm level < 8.0m, got {max_level_with_brain}"

    # Check that set_valve open was journaled
    rt_client = TestClient(red_team_app)
    j_res = rt_client.get("/api/journal?limit=100", headers={"X-Red-Team-Token": token})
    assert j_res.status_code == 200
    valve_cmds = [
        e for e in j_res.json()["journal"]
        if e["source"] == "command"
        and e["detail"]["tool"] == "set_valve"
        and e["detail"]["args"].get("target_state") == "open"
        and e["detail"]["executed"] is True
    ]
    assert len(valve_cmds) >= 2, f"Expected set_valve open journaled, got {valve_cmds}"


# ==============================================================================
# 9c: Fake flood
# ==============================================================================
def test_9c_fake_flood_paired_water(ephemeral_twin):
    """9c: Fake flood (device_physical z2-wl1 = 9.5): z2-wl1 isolated, z2-wl2 NOT isolated, no set_valve, no dispatch."""
    sim = ephemeral_twin["sim"]
    mcp_url = ephemeral_twin["mcp_url"]
    token = ephemeral_twin["red_team_token"]
    rt_client = TestClient(ephemeral_twin["red_team_app"])
    auth_header = {"X-Red-Team-Token": token}

    queue_source = QueueSource()
    streamer = KafkaStreamer(mock_mode=True)
    guardian = MockGuardian(reading_source=queue_source, mcp_url=mcp_url)
    brain = MockCityBrain(reading_source=queue_source, mcp_url=mcp_url)

    async def run():
        await guardian.mcp.connect()
        await brain.mcp.connect()
        await brain.refresh_zone_cameras()

        # Baseline: 10 ticks
        for _ in range(10):
            await stream_and_process_tick(sim, streamer, queue_source, guardian, brain)

        # Inject physical spoof on z2-wl1
        sim.inject_attack(capability="device_physical", target_device_id="z2-wl1", params={"spoofed_value": 9.5})
        for _ in range(25):
            await stream_and_process_tick(sim, streamer, queue_source, guardian, brain)

        await brain.mcp.close()
        await guardian.mcp.close()

    asyncio.run(run())

    # Verify through journal
    j_res = rt_client.get("/api/journal?limit=500", headers=auth_header)
    assert j_res.status_code == 200
    journal = j_res.json()["journal"]

    containments = [e for e in journal if e["source"] == "containment"]
    commands = [e for e in journal if e["source"] == "command"]

    # z2-wl1 isolated, z2-wl2 NOT isolated
    isolated_targets = {
        c["detail"]["target"] for c in containments
        if c["detail"]["tool"] == "isolate_device" and c["detail"]["to_status"] == "isolated"
    }
    assert "z2-wl1" in isolated_targets
    assert "z2-wl2" not in isolated_targets

    # No set_valve, no dispatch
    valve_cmds = [c for c in commands if c["detail"]["tool"] == "set_valve"]
    dispatch_cmds = [c for c in commands if c["detail"]["tool"] == "dispatch"]
    assert len(valve_cmds) == 0, f"Expected 0 set_valve commands, found {valve_cmds}"
    assert len(dispatch_cmds) == 0, f"Expected 0 dispatch commands, found {dispatch_cmds}"


# ==============================================================================
# 9d: Replay and delay on z3-tc1
# ==============================================================================
def test_9d_replay_and_delay_containment(tmp_path):
    """9d: Replay and delay on z3-tc1: each ends with z3-tc1 isolated and no other device contained; <= 5 rollback rows per device."""
    # Part 1: Replay
    db1 = str(tmp_path / "replay_test.db")
    sim1 = CitySimulator(seed=601, ledger=GroundTruthLedger(db_path=db1), sim_speed=1.0)
    qs1 = QueueSource()
    streamer1 = KafkaStreamer(mock_mode=True)

    with running_mcp_server(sim1) as s_info1:
        guardian1 = MockGuardian(reading_source=qs1, mcp_url=s_info1["mcp_url"])

        async def run_replay():
            await guardian1.mcp.connect()
            for _ in range(15):
                await stream_and_process_tick(sim1, streamer1, qs1, guardian1)
            sim1.inject_attack(capability="link", target_device_id="z3-tc1", params={"link_mode": "replay", "replay_offset": 5})
            for _ in range(5):
                await stream_and_process_tick(sim1, streamer1, qs1, guardian1)
            await guardian1.mcp.close()

        asyncio.run(run_replay())

    assert guardian1.isolated_devices == ["z3-tc1"], f"Expected ['z3-tc1'] isolated, got {guardian1.isolated_devices}"
    assert len(guardian1.rolled_back_readings) <= 5, f"Expected <= 5 rollbacks, got {len(guardian1.rolled_back_readings)}"

    # Part 2: Delay
    db2 = str(tmp_path / "delay_test.db")
    sim2 = CitySimulator(seed=602, ledger=GroundTruthLedger(db_path=db2), sim_speed=1.0)
    qs2 = QueueSource()
    streamer2 = KafkaStreamer(mock_mode=True)

    with running_mcp_server(sim2) as s_info2:
        guardian2 = MockGuardian(reading_source=qs2, mcp_url=s_info2["mcp_url"])

        async def run_delay():
            await guardian2.mcp.connect()
            for _ in range(10):
                await stream_and_process_tick(sim2, streamer2, qs2, guardian2)
            sim2.inject_attack(capability="link", target_device_id="z3-tc1", params={"link_mode": "delay", "delay_ticks": 25})
            for _ in range(35):
                await stream_and_process_tick(sim2, streamer2, qs2, guardian2)
            await guardian2.mcp.close()

        asyncio.run(run_delay())

    assert guardian2.isolated_devices == ["z3-tc1"], f"Expected ['z3-tc1'] isolated, got {guardian2.isolated_devices}"
    assert len(guardian2.rolled_back_readings) <= 5, f"Expected <= 5 rollbacks, got {len(guardian2.rolled_back_readings)}"


# ==============================================================================
# 9e: Rogue and corrupt_bytes
# ==============================================================================
def test_9e_rogue_and_corrupt_bytes_quarantine(tmp_path):
    """9e: Rogue and corrupt_bytes: quarantined, nothing else contained."""
    db = str(tmp_path / "quarantine_test.db")
    sim = CitySimulator(seed=701, ledger=GroundTruthLedger(db_path=db), sim_speed=1.0)
    qs = QueueSource()
    streamer = KafkaStreamer(mock_mode=True)

    with running_mcp_server(sim) as s_info:
        guardian = MockGuardian(reading_source=qs, mcp_url=s_info["mcp_url"])

        async def run():
            await guardian.mcp.connect()

            # Rogue device
            sim.inject_attack(capability="rogue", params={"rogue_device_id": "rg-01", "location": "zone-1", "device_type": "traffic_camera"})
            for _ in range(3):
                await stream_and_process_tick(sim, streamer, qs, guardian)
            assert guardian.quarantined_devices == ["rg-01"]
            assert len(guardian.isolated_devices) == 0

            # Corrupt bytes on z1-tc1
            sim.inject_attack(capability="link", target_device_id="z1-tc1", params={"link_mode": "corrupt_bytes"})
            for _ in range(4):
                await stream_and_process_tick(sim, streamer, qs, guardian)
            assert set(guardian.quarantined_devices) == {"rg-01", "z1-tc1"}
            assert len(guardian.isolated_devices) == 0

            await guardian.mcp.close()

        asyncio.run(run())


# ==============================================================================
# 9f: Live subprocesses
# ==============================================================================
def test_9f_live_subprocesses():
    """9f: python -m mocks.mock_guardian --live and python -m mocks.mock_brain --live run for 5 s as subprocesses with unreachable Kafka without traceback and exit cleanly on SIGINT."""
    import signal
    env = os.environ.copy()
    env["PYTHONPATH"] = "twin:twin/src" + (":" + env["PYTHONPATH"] if "PYTHONPATH" in env else "")

    for module in ("mocks.mock_guardian", "mocks.mock_brain"):
        proc = subprocess.Popen(
            [sys.executable, "-m", module, "--live", "--kafka-bootstrap", "127.0.0.1:19092"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        try:
            time.sleep(5.0)
            proc.send_signal(signal.SIGINT)
            stdout, stderr = proc.communicate(timeout=10.0)
        except Exception:
            proc.kill()
            proc.wait(timeout=2.0)
            raise

        assert proc.returncode in (0, -signal.SIGINT), f"{module} exited with unexpected code {proc.returncode}"
        assert "Traceback (most recent call last)" not in stderr, f"{module} emitted unhandled traceback:\n{stderr}"


# ==============================================================================
# 9g: Incidents
# ==============================================================================
def test_9g_incident_engine_hysteresis_and_resolution(tmp_path):
    """9g: Incidents (fire, grid_overload, traffic_accident): matching command appears once (hysteresis) and event ends resolved_by in journal; no dispatch in other zones."""
    db = str(tmp_path / "incidents_test.db")
    sim = CitySimulator(seed=801, ledger=GroundTruthLedger(db_path=db), sim_speed=60.0)
    token = "test-token-9g"
    red_team_app = build_red_team_app(sim, token=token)

    with running_mcp_server(sim) as s_info:
        mcp_url = s_info["mcp_url"]
        qs = QueueSource()
        streamer = KafkaStreamer(mock_mode=True)
        brain = MockCityBrain(reading_source=qs, mcp_url=mcp_url)

        ev_fire = None
        ev_grid = None
        ev_acc = None

        async def run():
            nonlocal ev_fire, ev_grid, ev_acc
            await brain.mcp.connect()
            await brain.refresh_zone_cameras()

            # 1. Fire in zone-4 (intensity=2.0)
            ev_fire = sim.inject_event(type="fire", zone_id="zone-4", intensity=2.0, duration_s=1200.0)
            # Advance until AQI > 150 and dispatch countdown expires
            for _ in range(70):
                for r in sim.tick():
                    res = streamer.stream_reading(r, sim.devices[r["device_id"]].private_key)
                    if res: qs.put(res[1])
                while True:
                    item = qs.get_reading(timeout=0.001)
                    if not item: break
                    await brain.evaluate_reading_async(item)

            # 2. Grid overload in zone-5 (intensity=1.5)
            ev_grid = sim.inject_event(type="grid_overload", zone_id="zone-5", intensity=1.5, duration_s=600.0)
            for _ in range(40):
                for r in sim.tick():
                    res = streamer.stream_reading(r, sim.devices[r["device_id"]].private_key)
                    if res: qs.put(res[1])
                while True:
                    item = qs.get_reading(timeout=0.001)
                    if not item: break
                    await brain.evaluate_reading_async(item)

            # 3. Traffic accident in zone-3
            ev_acc = sim.inject_event(type="traffic_accident", zone_id="zone-3", device_id="z3-tc1", intensity=2.0, duration_s=600.0)
            for _ in range(110):
                for r in sim.tick():
                    res = streamer.stream_reading(r, sim.devices[r["device_id"]].private_key)
                    if res: qs.put(res[1])
                while True:
                    item = qs.get_reading(timeout=0.001)
                    if not item: break
                    await brain.evaluate_reading_async(item)

            await brain.mcp.close()

        asyncio.run(run())

    # Verify through journal
    rt_client = TestClient(red_team_app)
    j_res = rt_client.get("/api/journal?limit=1000", headers={"X-Red-Team-Token": token})
    assert j_res.status_code == 200
    journal = j_res.json()["journal"]

    commands = [e for e in journal if e["source"] == "command"]
    events_resolved = [e for e in journal if e["source"] == "event" and e["detail"]["action"] == "resolved"]

    # Fire dispatch: exactly once for zone-4, none for other zones
    fire_dispatches = [
        c for c in commands
        if c["detail"]["tool"] == "dispatch" and c["detail"]["args"].get("incident_type") == "fire"
    ]
    assert len(fire_dispatches) == 1, f"Expected exactly 1 fire dispatch, got {len(fire_dispatches)}"
    assert fire_dispatches[0]["detail"]["args"].get("zone_id") == "zone-4"

    # Grid overload switch_grid: exactly once shed for z5-pl1 (or zone-5)
    shed_cmds = [
        c for c in commands
        if c["detail"]["tool"] == "switch_grid" and c["detail"]["args"].get("target_state") == "shed"
    ]
    assert len(shed_cmds) == 1, f"Expected exactly 1 grid shed command, got {len(shed_cmds)}"
    assert "z5-" in shed_cmds[0]["detail"]["args"].get("device_id", "")

    # Traffic accident dispatch: exactly once for zone-3, none for other zones
    med_dispatches = [
        c for c in commands
        if c["detail"]["tool"] == "dispatch" and c["detail"]["args"].get("incident_type") == "medical_emergency"
    ]
    assert len(med_dispatches) == 1, f"Expected exactly 1 medical emergency dispatch, got {len(med_dispatches)}"
    assert med_dispatches[0]["detail"]["args"].get("zone_id") == "zone-3"

    # All dispatches strictly in target zone (no dispatch in other zones)
    all_dispatches = [c for c in commands if c["detail"]["tool"] == "dispatch"]
    for d in all_dispatches:
        assert d["detail"]["args"].get("zone_id") in ("zone-3", "zone-4")

    # Events end resolved_by in journal
    res_map = {e["detail"]["event_id"]: e["detail"]["resolved_by"] for e in events_resolved}
    assert ev_fire in res_map and res_map[ev_fire].startswith("dispatch:"), f"Fire event resolution missing: {res_map}"
    assert ev_grid in res_map and "switch_grid" in res_map[ev_grid], f"Grid event resolution missing: {res_map}"
    assert ev_acc in res_map and res_map[ev_acc].startswith("dispatch:"), f"Accident event resolution missing: {res_map}"


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
