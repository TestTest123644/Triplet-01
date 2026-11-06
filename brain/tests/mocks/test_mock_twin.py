import json
import math

import pytest

from city_brain.edge.profiles import TwinProfile
from city_brain.edge.signatures import verify
from mocks.twin.mock_twin import MockDefects, MockTwin

WIRE_KEYS = ["reading_id", "device_id", "device_type", "ts", "seq", "value", "unit", "location", "status", "sig"]


def test_mock_water_rises_like_twin():
    t = MockTwin(); vals = [r["value"] for _ in range(100) for r in t.tick() if r["device_id"] == "z1-wl1"]
    assert vals[-1] - vals[0] == pytest.approx(0.015 * 9.9, abs=0.03)


def test_mock_reading_verifies_with_published_key():
    t = MockTwin(); r = t.tick()[0]
    assert verify(r, t.get_device_key(r["device_id"]))


def test_same_seed_gives_identical_signed_stream():
    a, b = MockTwin(seed=7), MockTwin(seed=7)
    assert [a.tick() for _ in range(5)] == [b.tick() for _ in range(5)]


def test_mock_reproduces_half_closes_valve():
    t = MockTwin(); t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "half"})
    assert t.devices["z1-wl1"].valve_ratio == 0.0


def test_actuator_attack_replies_ok_but_noop():
    t = MockTwin(); t.inject("actuator", "z4-wl1", {})
    assert t.handle_tool("set_valve", {"device_id": "z4-wl1", "target_state": "open"})["status"] == "ok"
    assert t.devices["z4-wl1"].valve_ratio == 0.0


def test_restart_changes_keys_and_resets_seq():
    t = MockTwin(); old = t.get_device_key("z1-wl1"); t.tick(); t.restart()
    assert t.get_device_key("z1-wl1") != old and t.tick()[0]["seq"] == 1


def test_wire_format_matches_twin():
    t = MockTwin(); out = t.tick()
    assert len(out) == 42 and list(out[0]) == WIRE_KEYS
    r = out[2]
    assert r["reading_id"] == "r-z1-wl1-1" and r["seq"] == 1 and r["location"] == "zone-1"
    assert r["ts"] == "2026-10-17T12:00:00Z" and r["status"] == "ok" and r["unit"] == "meters"
    assert r["device_type"] == "water_level" and r["value"] == round(r["value"], 2)
    assert all(len(json.dumps(x, separators=(",", ":"))) <= 300 for x in out)


def test_ts_has_second_precision_and_advances_every_ten_ticks():
    t = MockTwin(); ts = [t.tick()[0]["ts"] for _ in range(11)]
    assert ts[:10] == ["2026-10-17T12:00:00Z"] * 10 and ts[10] == "2026-10-17T12:00:01Z"


def test_inventory_comes_from_profile():
    t = MockTwin()
    devs = {d["device_id"]: d for d in t.list_devices()}
    assert len(devs) == 42 and devs["z3-pl1"]["zone_id"] == "zone-3"
    assert devs["z1-tc1"]["lat"] == round(36.8 + 0 * 0.0012 - 0.0036, 6)
    assert devs["z1-wl1"]["actuator_state"] == {"valve_position": "closed", "valve_ratio": 0.0}
    assert devs["z1-aq1"]["actuator_state"] == {} and devs["z1-wl1"]["min_val"] == 0.0


def test_profile_edit_changes_the_mock():
    p = TwinProfile.load("profiles/twin-triplet07.yaml")
    inv = dict(p.inventory); inv["zones"] = 2
    p2 = TwinProfile({**p._data, "inventory": inv}, p.schema)
    assert len(MockTwin(profile=p2).devices) == 14


def test_tools_set_actuators_and_reply_like_twin():
    t = MockTwin()
    assert t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "50%"}) == {
        "status": "ok", "device_id": "z1-wl1", "valve_position": "50%"}
    assert t.devices["z1-wl1"].valve_ratio == 0.5
    t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "150%"})
    assert t.devices["z1-wl1"].valve_ratio == 1.0
    t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "nan%"})
    assert t.devices["z1-wl1"].valve_ratio == 1.0 and not math.isnan(t.devices["z1-wl1"].valve_ratio)
    t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "open"})
    assert t.devices["z1-wl1"].valve_ratio == 1.0
    assert t.handle_tool("set_signal_plan", {"device_id": "z1-tc1", "target_state": "green_wave"})["signal_plan"] == "green_wave"
    assert t.devices["z1-tc1"].signal_plan == "green_wave"
    assert t.handle_tool("switch_grid", {"device_id": "z1-pl1", "target_state": "foo"})["status"] == "ok"
    assert t.devices["z1-pl1"].grid_state == "foo"
    assert t.handle_tool("set_valve", {"device_id": "z1-tc1", "target_state": "open"})["status"] == "error"
    assert t.handle_tool("dispatch", {"incident_type": "flood", "zone_id": "zone-9"})["status"] == "error"
    assert t.handle_tool("dispatch", {"incident_type": "flood", "zone_id": "zone-2"})["zone_id"] == "zone-2"


def test_unknown_states_rejected_when_defect_off():
    t = MockTwin(defects=MockDefects(accept_unknown_states=False))
    assert t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "half"})["status"] == "error"
    assert t.devices["z1-wl1"].valve_ratio == 0.0
    assert t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "40%"})["status"] == "ok"
    assert t.devices["z1-wl1"].valve_ratio == 0.4


def test_idempotency_key_reexecutes_by_default_and_can_be_fixed():
    t = MockTwin()
    t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "open", "idempotency_key": "k"})
    assert t.handle_tool("get_command_result", {"idempotency_key": "k"})["valve_position"] == "open"
    t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "closed", "idempotency_key": "k"})
    assert t.devices["z1-wl1"].valve_ratio == 0.0
    assert t.handle_tool("get_command_result", {"idempotency_key": "nope"}) is None
    f = MockTwin(defects=MockDefects(reexecute_repeated_keys=False))
    f.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "open", "idempotency_key": "k"})
    f.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "closed", "idempotency_key": "k"})
    assert f.devices["z1-wl1"].valve_ratio == 1.0


def test_contained_devices_are_actuated_unless_defect_off():
    t = MockTwin(); t.devices["z1-wl1"].status = "quarantined"
    t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "open"})
    assert t.devices["z1-wl1"].valve_ratio == 1.0
    assert t.tick()[2]["status"] == "quarantined"
    f = MockTwin(defects=MockDefects(actuate_contained=False)); f.devices["z1-wl1"].status = "isolated"
    assert f.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "open"})["status"] == "error"
    assert f.devices["z1-wl1"].valve_ratio == 0.0


def test_open_valve_drains_and_dispatch_lowers_calls():
    t = MockTwin(); t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "open"})
    vals = [r["value"] for _ in range(100) for r in t.tick() if r["device_id"] == "z1-wl1"]
    assert vals[-1] - vals[0] == pytest.approx(-0.03 * 9.9, abs=0.03)
    t.handle_tool("dispatch", {"incident_type": "flood", "zone_id": "zone-1"})
    for _ in range(200):
        t.tick()
    assert t.devices["z1-ec1"].value < 0.9 and t.devices["z2-ec1"].value > 1.0


def _stream(t, device, n):
    return [r for _ in range(n) for r in t.tick() if r["device_id"] == device]


def test_device_physical_and_device_key_stay_validly_signed():
    t = MockTwin(); t.inject("device_physical", "z1-tc1", {})
    r = _stream(t, "z1-tc1", 2)[-1]
    assert r["value"] > 100 and verify(r, t.get_device_key("z1-tc1"))
    t2 = MockTwin(); t2.inject("device_key", "z1-tc1", {"spoofed_value": 7.0})
    r2 = _stream(t2, "z1-tc1", 1)[0]
    assert r2["value"] == 7.0 and verify(r2, t2.get_device_key("z1-tc1"))


def test_sensor_fault_modes():
    t = MockTwin(); t.inject("sensor_fault", "z1-tc1", {"fault_mode": "stuck"})
    assert {r["value"] for r in _stream(t, "z1-tc1", 3)} == {42.0}
    t = MockTwin(); t.inject("sensor_fault", "z1-tc1", {"fault_mode": "dropout"})
    assert {r["value"] for r in _stream(t, "z1-tc1", 3)} == {0.0}
    t = MockTwin(); t.inject("sensor_fault", "z1-tc1", {})
    vals = [r["value"] for r in _stream(t, "z1-tc1", 100)]
    assert vals[-1] - vals[0] > 4.0


def test_link_corrupt_drop_replay():
    t = MockTwin(); t.inject("link", "z1-wl1", {"link_mode": "corrupt_bytes"})
    r = _stream(t, "z1-wl1", 1)[0]
    assert not verify(r, t.get_device_key("z1-wl1")) and r["sig"].endswith("AAAA") and len(r["sig"]) == 88
    t = MockTwin(); t.inject("link", "z1-wl1", {"link_mode": "drop"})
    out = t.tick()
    assert len(out) == 41 and all(r["device_id"] != "z1-wl1" for r in out)
    assert t.devices["z1-wl1"].seq == 1
    t = MockTwin(); first = _stream(t, "z1-wl1", 1)[0]
    t.inject("link", "z1-wl1", {"link_mode": "replay"})
    again = _stream(t, "z1-wl1", 3)
    assert all(r == first for r in again) and verify(again[0], t.get_device_key("z1-wl1"))


def test_zone_target_and_bad_capability():
    t = MockTwin(); t.inject("actuator", "zone-2", {})
    t.handle_tool("set_valve", {"device_id": "z2-wl2", "target_state": "open"})
    assert t.devices["z2-wl2"].valve_ratio == 0.0
    with pytest.raises(ValueError):
        t.inject("nonsense", "z1-wl1", {})


def test_city_state_and_keys():
    t = MockTwin(); s = t.get_city_state()
    assert s["total_devices"] == 42 and len(s["zones"]) == 6 and s["sim_time"] == "2026-10-17T12:00:00Z"
    assert t.get_device_key("nope") is None
