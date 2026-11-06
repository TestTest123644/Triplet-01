"""Unit tests for the Scenario settle step and false-positive guard filtering."""

import pytest
from unittest.mock import MagicMock
from scenarios._twin_common import ScenarioContext


def test_settle_waits_for_pending_frames(tmp_path):
    """Verify settle waits for delayed_frames_pending to reach zero and resets actuators."""
    ctx = ScenarioContext(
        scenario_id="t_test",
        name="test_settle_pending",
        log_dir=str(tmp_path),
    )

    call_counts = {"status": 0}

    def mock_get_status():
        call_counts["status"] += 1
        if call_counts["status"] < 3:
            return {
                "sim_time": "2026-10-17T12:00:00Z",
                "sim_speed": 1.0,
                "delayed_frames_pending": 4,
            }
        return {
            "sim_time": "2026-10-17T12:00:02Z",
            "sim_speed": 1.0,
            "delayed_frames_pending": 0,
        }

    ctx.rest = MagicMock()
    ctx.rest.get_status.side_effect = mock_get_status
    ctx.rest.get_journal.return_value = []
    ctx.sim_sleep = MagicMock()

    ctx.mcp = MagicMock()
    ctx.mcp.list_devices.return_value = [
        {
            "device_id": "z1-tc1",
            "device_type": "traffic_camera",
            "status": "isolated",
            "actuator_state": {"signal_plan": "all_red"},
        },
        {
            "device_id": "z2-wl1",
            "device_type": "water_level",
            "status": "ok",
            "actuator_state": {"valve_position": "open"},
        },
    ]

    def mock_call_tool(tool_name, args):
        if tool_name == "get_device_state":
            return {"current_value": 3.5}
        return {"status": "ok"}

    ctx.mcp.call_tool.side_effect = mock_call_tool

    duration = ctx.settle(timeout_sim_seconds=30.0)
    assert call_counts["status"] >= 3
    assert ctx.rest.clear_attack.called
    assert ctx.rest.clear_event.called
    assert ctx.settle_end_sim_time == "2026-10-17T12:00:02Z"
    # Verify reinstatement and actuator reset were triggered
    tool_calls = [c[0][0] for c in ctx.mcp.call_tool.call_args_list]
    assert "reinstate_device" in tool_calls
    assert "set_signal_plan" in tool_calls
    assert "set_valve" in tool_calls


def test_settle_times_out_with_clear_error(tmp_path):
    """Verify settle times out with a clear descriptive message if deadline is exceeded."""
    from scenarios._twin_common import parse_sim_timestamp

    ctx = ScenarioContext(
        scenario_id="t_test",
        name="test_settle_timeout",
        log_dir=str(tmp_path),
    )

    sim_epoch = parse_sim_timestamp("2026-10-17T12:00:00Z")
    ctx.rest = MagicMock()
    ctx.rest.get_status.return_value = {
        "sim_time": "2026-10-17T12:00:00Z",
        "sim_speed": 1.0,
        "delayed_frames_pending": 10,
    }
    ctx.sim_sleep = MagicMock()

    sim_seconds_progression = [sim_epoch, sim_epoch + 2.0, sim_epoch + 15.0]

    def mock_sim_seconds():
        if sim_seconds_progression:
            return sim_seconds_progression.pop(0)
        return sim_epoch + 999.0

    ctx.get_sim_time_seconds = MagicMock(side_effect=mock_sim_seconds)

    with pytest.raises(TimeoutError) as exc_info:
        ctx.settle(timeout_sim_seconds=5.0)

    err_msg = str(exc_info.value)
    assert "Settle step timed out" in err_msg
    assert "5.0 sim-seconds" in err_msg


def test_false_positive_guard_ignores_entries_before_settle_end(tmp_path):
    """Verify false-positive guard ignores containment/commands timestamped before settle completed."""
    ctx = ScenarioContext(
        scenario_id="t_test",
        name="test_fp_settle_filter",
        log_dir=str(tmp_path),
    )

    pre_settle_entry = {
        "sim_ts": "2026-10-17T12:00:05Z",
        "source": "containment",
        "detail": {
            "device_id": "z1-tc1",
            "to_status": "isolated",
        },
    }
    pre_settle_command = {
        "sim_ts": "2026-10-17T12:00:06Z",
        "source": "command",
        "detail": {
            "tool": "dispatch",
            "args": {"zone_id": "zone-9"},
        },
    }
    nominal_entry = {
        "sim_ts": "2026-10-17T12:00:25Z",
        "source": "journal",
        "detail": {},
    }

    journal = [pre_settle_entry, pre_settle_command, nominal_entry]

    ctx.rest = MagicMock()
    ctx.rest.get_journal.return_value = journal
    ctx.settle_end_sim_time = "2026-10-17T12:00:15Z"

    # Pre-settle violations must be ignored by the false positive guard
    ok, msg = ctx.verify_false_positive_guard(allowed_devices=set(), allowed_zones=set())
    assert ok is True
    assert msg == "nominal"

    # Now add an entry that occurred AFTER settle end
    post_settle_violation = {
        "sim_ts": "2026-10-17T12:00:30Z",
        "source": "containment",
        "detail": {
            "device_id": "z2-tc1",
            "to_status": "quarantined",
        },
    }
    journal.append(post_settle_violation)

    ok2, msg2 = ctx.verify_false_positive_guard(allowed_devices=set(), allowed_zones=set())
    assert ok2 is False
    assert "False positive violation" in msg2
    assert "z2-tc1" in msg2


def test_delayed_frames_pending_api_and_flush_on_clear(tmp_path, monkeypatch):
    """Verify delayed_frames_pending reflects backlog in /api/status and drops to 0 on clear."""
    from starlette.testclient import TestClient
    from twin.simulation import CitySimulator
    from twin.kafka_streamer import KafkaStreamer
    from twin.red_team_api import build_red_team_app

    db_path = str(tmp_path / "delay_test.db")
    keys_path = str(tmp_path / "delay_keys.json")
    monkeypatch.setenv("GROUND_TRUTH_DB_PATH", db_path)
    monkeypatch.setenv("KEYSTORE_PATH", keys_path)

    sim = CitySimulator(seed=42)
    streamer = KafkaStreamer(mock_mode=True)
    app = build_red_team_app(sim, token="test-tok", streamer=streamer)
    client = TestClient(app)

    # Initially 0 delayed frames pending
    resp = client.get("/api/status", headers={"X-Red-Team-Token": "test-tok"})
    assert resp.status_code == 200
    assert resp.json()["delayed_frames_pending"] == 0

    # Inject link delay attack on z3-tc1
    inj_resp = client.post(
        "/api/attack/inject",
        headers={"X-Red-Team-Token": "test-tok"},
        json={
            "capability": "link",
            "target_device_id": "z3-tc1",
            "params": {"link_mode": "delay", "delay_ticks": 50},
        },
    )
    assert inj_resp.status_code == 200
    attack_id = inj_resp.json()["attack_id"]

    # Tick simulator 5 times and stream readings
    for _ in range(5):
        readings = sim.tick()
        for r in readings:
            dev_id = r["device_id"]
            k = sim.signing_key_for(dev_id)
            streamer.stream_reading(r, k)

    # Backlog must now be non-zero
    assert streamer.get_delayed_frames_pending() == 5
    st_resp = client.get("/api/status", headers={"X-Red-Team-Token": "test-tok"})
    assert st_resp.json()["delayed_frames_pending"] == 5

    # Clear attack: held frames must be flushed immediately in order
    clr_resp = client.post(
        "/api/attack/clear",
        headers={"X-Red-Team-Token": "test-tok"},
        json={"attack_id": attack_id},
    )
    assert clr_resp.status_code == 200

    # Backlog must immediately drop to 0
    assert streamer.get_delayed_frames_pending() == 0
    st_resp2 = client.get("/api/status", headers={"X-Red-Team-Token": "test-tok"})
    assert st_resp2.json()["delayed_frames_pending"] == 0

    sim.close()
