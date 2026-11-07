import shutil
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from city_brain.domain.types import CanonicalUnit, DeviceStatus, SensorKind, TrustState
from city_brain.edge.profiles import (
    GuardianProfile,
    ProfileError,
    StateNotAllowed,
    ToolCall,
    TwinProfile,
    UnknownVerdictState,
)

TWIN = "profiles/twin-triplet07.yaml"
ALT = "profiles/twin-alt-example.yaml"
GUARDIAN = "profiles/guardian-triplet07.yaml"

TWIN_RAW = {
    "reading_id": "r-z1-wl1-7", "device_id": "z1-wl1", "device_type": "water_level",
    "ts": "2026-10-17T12:00:00Z", "seq": 7, "value": 5.42, "unit": "meters",
    "location": "zone-1", "status": "ok", "sig": "c2ln",
}
ALT_RAW = {
    "reading_id": "r-z1-wl1-7", "device_id": "z1-wl1", "device_category": "flood_sensor",
    "ts": "2026-10-17T12:00:00Z", "seq": 7, "reading": 542, "unit": "cm",
    "location": "zone-1", "status": "ok",
}


def _write(tmp_path, data, name="p.yaml"):
    """Write a profile copy next to a copy of schemas/, as brain/profiles/ sits."""
    out = tmp_path / "profiles" / name
    out.parent.mkdir(exist_ok=True)
    if not (tmp_path / "schemas").exists():
        shutil.copytree("schemas", tmp_path / "schemas")
    out.write_text(yaml.safe_dump(data))
    return out


def _load_yaml(path):
    return yaml.safe_load(Path(path).read_text())


def test_twin_reading_maps_to_canonical():
    p = TwinProfile.load(TWIN)
    m = p.map_reading({**TWIN_RAW})
    assert m["kind"] == SensorKind.WATER_LEVEL and m["unit"] == CanonicalUnit.M
    assert m["zone_id"] == "zone-1" and m["value"] == 5.42
    assert m["device_status"] is DeviceStatus.OK
    assert m["sim_time"] == datetime.fromisoformat("2026-10-17T12:00:00+00:00")
    assert set(m) == {"reading_id", "device_id", "kind", "value", "unit", "zone_id",
                      "sim_time", "seq", "device_status"}
    assert "sig" not in m


@pytest.mark.parametrize("wire,canon", [
    ("isolated", DeviceStatus.ISOLATED), ("quarantined", DeviceStatus.QUARANTINED),
    ("rolled_back", DeviceStatus.ROLLED_BACK), ("modelled", DeviceStatus.MODELLED),
])
def test_statuses_map(wire, canon):
    assert TwinProfile.load(TWIN).map_reading({**TWIN_RAW, "status": wire})["device_status"] is canon


def test_unknown_wire_values_raise():
    p = TwinProfile.load(TWIN)
    for patch in ({"unit": "furlongs"}, {"device_type": "ufo"}, {"status": "weird"}):
        with pytest.raises(ProfileError):
            p.map_reading({**TWIN_RAW, **patch})


def test_alt_profile_gives_same_canonical_value():
    alt = TwinProfile.load(ALT)
    m = alt.map_reading({**ALT_RAW})
    assert m["kind"] == SensorKind.WATER_LEVEL and m["value"] == pytest.approx(5.42)
    assert m["unit"] == CanonicalUnit.M
    twin = TwinProfile.load(TWIN).map_reading({**TWIN_RAW})
    assert {k: v for k, v in m.items() if k != "value"} == {k: v for k, v in twin.items() if k != "value"}


def test_valve_ratio_tool_call():
    p = TwinProfile.load(TWIN)
    assert p.to_tool_call("valve", "z1-wl1", "ratio:33") == ToolCall(
        "set_valve", {"device_id": "z1-wl1", "target_state": "33%"})
    assert p.to_tool_call("valve", "z1-wl1", "open").args["target_state"] == "open"
    with pytest.raises(StateNotAllowed):
        p.to_tool_call("valve", "z1-wl1", "half")
    with pytest.raises(StateNotAllowed):
        p.to_tool_call("valve", "z1-wl1", "ratio:101")


def test_other_tool_calls():
    p = TwinProfile.load(TWIN)
    assert p.to_tool_call("signal", "z1-tc1", "boost") == ToolCall(
        "set_signal_plan", {"device_id": "z1-tc1", "target_state": "green_wave"})
    assert p.to_tool_call("grid", "z1-pl1", "secondary").args["target_state"] == "grid_b"
    assert p.to_tool_call("dispatch", "zone-1", "flood") == ToolCall(
        "dispatch", {"zone_id": "zone-1", "incident_type": "flood"})
    with pytest.raises(StateNotAllowed):
        p.to_tool_call("signal", "z1-tc1", "green_wave")


def test_toolcall_is_frozen():
    with pytest.raises(Exception):
        ToolCall("a", {}).tool = "b"  # type: ignore[misc]


def test_guardian_vocabulary():
    g = GuardianProfile.load(GUARDIAN)
    assert g.map_state("SUSPECT") is TrustState.DOUBTFUL
    assert g.map_state("MALICIOUS") is TrustState.UNTRUSTED
    assert g.map_state("TRUSTED") is TrustState.TRUSTED
    assert g.map_state("PENDING") is TrustState.UNKNOWN
    assert g.map_state(None) is TrustState.UNKNOWN
    with pytest.raises(UnknownVerdictState):
        g.map_state("MAYBE")


def test_guardian_public_key(tmp_path):
    g = GuardianProfile.load(GUARDIAN, public_key_b64="a2V5")
    assert g.public_key_b64 == "a2V5"
    with pytest.raises(ProfileError, match="public key"):
        _ = GuardianProfile.load(GUARDIAN).public_key_b64
    (tmp_path / "keys").mkdir()
    (tmp_path / "keys" / "g.pub").write_text("Zm9v\n")
    data = _load_yaml(GUARDIAN)
    data["public_key_file"] = "keys/g.pub"
    assert GuardianProfile.load(_write(tmp_path, data, "g.yaml")).public_key_b64 == "Zm9v"


def test_location_object_form_maps_to_zone():
    p = TwinProfile.load(TWIN)
    loc = {"zone_id": "zone-3", "lat": 1.0, "lon": 2.0}
    assert p.map_reading({**TWIN_RAW, "location": loc})["zone_id"] == "zone-3"


def test_observed_block_is_required(tmp_path):
    data = _load_yaml(TWIN)
    del data["observed"]
    bad = _write(tmp_path, data)
    with pytest.raises(ProfileError, match="observed"):
        TwinProfile.load(bad)
    with pytest.raises(ProfileError, match="observed"):
        GuardianProfile.load(bad)


@pytest.mark.parametrize("key", ["commit", "date", "by", "method"])
def test_observed_keys_are_required(tmp_path, key):
    data = _load_yaml(TWIN)
    del data["observed"][key]
    with pytest.raises(ProfileError, match=key):
        TwinProfile.load(_write(tmp_path, data))


def test_observed_method_must_be_known(tmp_path):
    data = _load_yaml(TWIN)
    data["observed"]["method"] = "guess"
    with pytest.raises(ProfileError, match="method"):
        TwinProfile.load(_write(tmp_path, data))


def test_copied_profile_loads(tmp_path):
    p = TwinProfile.load(_write(tmp_path, _load_yaml(TWIN)))
    assert p.observed["commit"] == "418a024"
    assert p.observed["date"] == "2026-11-04"


def test_assumed_groups_are_listed():
    assert "actuators" in TwinProfile.load(ALT).unverified_groups
    assert TwinProfile.load(TWIN).unverified_groups == ()
    assert GuardianProfile.load(GUARDIAN).observed["method"] == "assumed"


def test_assumed_groups_subset(tmp_path):
    data = _load_yaml(TWIN)
    data["observed"]["assumed_groups"] = ["actuators"]
    assert TwinProfile.load(_write(tmp_path, data)).unverified_groups == ("actuators",)
    data["observed"]["assumed_groups"] = ["nonsense"]
    with pytest.raises(ProfileError, match="nonsense"):
        TwinProfile.load(_write(tmp_path, data))


def test_inventory_and_registry():
    p = TwinProfile.load(TWIN)
    assert p.inventory["zones"] == 6 and len(p.inventory["devices"]) == 7
    assert p.registry["source"] == "mcp:list_devices"
    assert "lat" in p.registry["fields"] and "lon" in p.registry["fields"]
    assert p.fields["device_type"] == "kind" and p.kinds["water_level"] == "WATER_LEVEL"
    assert p.units["meters"] == "m" and p.statuses["ok"] == "OK"
    assert p.schema["title"] == "TwinSensorReading"


def test_map_device():
    p = TwinProfile.load(TWIN)
    raw = {"device_id": "z1-wl1", "zone_id": "zone-1", "device_type": "water_level", "unit": "meters",
           "lat": 36.8, "lon": 10.18, "min_val": 0.0, "max_val": 12.0, "status": "ok",
           "actuator_state": {"valve_position": "33%", "valve_ratio": 0.33}}
    d = p.map_device(raw)
    assert d.kind is SensorKind.WATER_LEVEL and d.unit is CanonicalUnit.M and d.status is DeviceStatus.OK
    assert d.actuator.capability == "valve" and d.actuator.target == "z1-wl1"
    assert d.actuator.current_state == "ratio:33"
    assert {"open", "closed", "ratio:0", "ratio:100"} <= d.actuator.states
    sensor = p.map_device({**raw, "device_id": "z1-aq1", "device_type": "air_quality",
                           "unit": "AQI", "actuator_state": {}})
    assert sensor.actuator is None
    alt = TwinProfile.load(ALT).map_device({**raw, "device_type": "flood_sensor", "unit": "cm",
                                            "min_val": 0.0, "max_val": 1200.0})
    assert alt.max_val == pytest.approx(12.0)


@pytest.mark.parametrize("pos,expected,flagged", [
    ("open", "open", False), ("closed", "closed", False), ("33%", "ratio:33", False),
    ("0%", "ratio:0", False), ("100%", "ratio:100", False),
    ("150%", "ratio:100", True), ("33.5%", "ratio:34", True), ("-20%", "ratio:0", True),
    ("weird", "closed", True), ("", "closed", True), ("x%", "closed", True), ("nan%", "closed", True),
    (None, "closed", True),
])
def test_canonical_actuator_state_valve(pos, expected, flagged):
    p = TwinProfile.load(TWIN)
    raw = {"device_type": "water_level", "actuator_state": {"valve_position": pos}}
    assert p.canonical_actuator_state(raw) == expected
    assert p.actuator_state_with_flag(raw) == (expected, flagged)


def test_ratio_must_be_canonical_form():
    p = TwinProfile.load(TWIN)
    with pytest.raises(StateNotAllowed):
        p.to_tool_call("valve", "z1-wl1", "ratio:007")
    assert p.to_tool_call("valve", "z1-wl1", "ratio:0").args["target_state"] == "0%"
    assert p.to_tool_call("valve", "z1-wl1", "ratio:100").args["target_state"] == "100%"


def test_inventory_carries_mock_data():
    inv = TwinProfile.load(TWIN).inventory
    assert inv["zone_coords"]["zone-1"] == {"lat": 36.8, "lon": 10.18}
    wl1 = next(d for d in inv["devices"] if d["suffix"] == "wl1")
    assert wl1["initial_actuator"] == {"valve_position": "closed", "valve_ratio": 0.0}
    assert {d["suffix"]: d["initial_actuator"] for d in inv["devices"] if "initial_actuator" in d} == {
        "tc1": {"signal_plan": "normal"}, "tc2": {"signal_plan": "normal"},
        "wl1": {"valve_position": "closed", "valve_ratio": 0.0},
        "wl2": {"valve_position": "closed", "valve_ratio": 0.0},
        "pl1": {"grid_state": "grid_a"}}


def test_canonical_actuator_state_others():
    p = TwinProfile.load(TWIN)
    assert p.canonical_actuator_state(
        {"device_type": "traffic_camera", "actuator_state": {"signal_plan": "green_wave"}}) == "boost"
    assert p.canonical_actuator_state(
        {"device_type": "power_load", "actuator_state": {"grid_state": "grid_b"}}) == "secondary"
    assert p.canonical_actuator_state({"device_type": "air_quality", "actuator_state": {}}) is None


def test_allowlist_and_partner_acceptance():
    p = TwinProfile.load(TWIN)
    assert p.allowlist["valve"] == frozenset({"open", "closed", "ratio"})
    assert p.allowlist["signal"] == frozenset({"boost", "normal", "restrict"})
    assert p.allowlist["grid"] == frozenset({"primary", "secondary", "shed", "standby"})
    assert p.allowlist["dispatch"] == frozenset(
        {"flood", "traffic_accident", "medical_surge", "fire", "police"})
    assert p.accepts_partner_state("valve", "open") and p.accepts_partner_state("valve", "40%")
    assert not p.accepts_partner_state("valve", "half")
    assert p.accepts_partner_state("signal", "green_wave")
    assert not p.accepts_partner_state("signal", "boost")
    assert p.accepts_partner_state("dispatch", "anything")  # the real Twin takes any string
    assert not p.accepts_partner_state("nope", "x")
