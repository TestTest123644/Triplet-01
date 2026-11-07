"""The mock is built from the profile, so a profile edit must change what the mock accepts (G3)."""

import json
from pathlib import Path

import pytest
import yaml

from city_brain.edge.guard import Rejected
from city_brain.edge.profiles import TwinProfile
from mocks.twin import MockDefects, MockTwin
from tests.helpers_mock import guard_for_mock

PROFILE = Path(__file__).resolve().parents[2] / "profiles" / "twin-triplet07.yaml"
STRICT = MockDefects(accept_unknown_states=False)


def load() -> TwinProfile:
    return TwinProfile.load(PROFILE)


def load_with(tmp_path, edit) -> TwinProfile:
    doc = yaml.safe_load(PROFILE.read_text(encoding="utf-8"))
    edit(doc)
    out = tmp_path / "profiles" / PROFILE.name
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    return TwinProfile.load(out)


def tool_and_target(profile, capability, twin):
    spec = profile.actuators[capability]
    device = next(d for d in twin.list_devices() if d["device_type"] == spec["device_type"])
    return spec["tool"], device["device_id"]


def listed_partner_states(profile, capability):
    states = []
    for wire in profile.actuators[capability]["states"].values():
        states.append(wire.format(pct=33) if "{pct}" in wire else wire)
    return states


@pytest.mark.parametrize("capability", ["valve", "signal", "grid"])
def test_mock_accepts_exactly_the_profile_states(capability):
    p = load(); t = MockTwin(p, defects=STRICT)
    tool, target = tool_and_target(p, capability, t)
    for partner_state in listed_partner_states(p, capability):
        assert t.handle_tool(tool, {"device_id": target, "target_state": partner_state})["status"] == "ok"
    assert t.handle_tool(tool, {"device_id": target, "target_state": "no-such-state"})["status"] == "error"


def test_default_defects_keep_the_real_twin_behaviour():
    assert MockTwin().handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "no-such-state"})["status"] == "ok"


def test_every_mock_reading_passes_the_guard():
    t = MockTwin(); g = guard_for_mock(t)
    assert not [r for _ in range(50) for r in t.tick() if isinstance(g.check(json.dumps(r).encode()), Rejected)]


def test_inventory_matches_registry_shape():
    p = load(); devices = MockTwin(p).list_devices()
    assert len(devices) == 42 and all(set(d) == set(p.registry["fields"]) for d in devices)
    assert all(p.map_device(d).device_id == d["device_id"] for d in devices)


def test_profile_edit_changes_the_mock(tmp_path):
    p = load_with(tmp_path, lambda doc: doc["actuators"]["valve"]["states"].update(closed="shut"))
    t = MockTwin(p, defects=STRICT)
    assert t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "shut"})["status"] == "ok"
    assert t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "closed"})["status"] == "error"
