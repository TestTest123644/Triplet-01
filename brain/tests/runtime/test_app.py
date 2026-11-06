import json
from pathlib import Path

import pytest

from city_brain.adapters.files.jsonl_log import JsonlLog
from city_brain.adapters.inproc_twin import InProcTwin
from city_brain.config import load_config
from city_brain.core.events import (
    CancelTimer,
    CommandResult,
    Publish,
    SendCommand,
    StartTimer,
    TimerFired,
    VerdictReceived,
)
from city_brain.domain.types import ActionStatus
from city_brain.edge.guard import ReadingGuard, VerdictGuard
from city_brain.edge.profiles import GuardianProfile, ProfileError
from city_brain.edge.signatures import KeyRing
from city_brain.runtime import app as app_module
from city_brain.runtime.app import BrainApp
from city_brain.runtime.clocks import SimClock
from mocks.guardian.mock_guardian import MockGuardian
from mocks.keys import derive_keypair
from mocks.twin import MockTwin
from tests.helpers_coordinator import feed_rising_channel, seq_of, state_with_registry, verdict_for

BRAIN = Path(__file__).resolve().parents[2]
DEVICES = ("z1-wl1", "z2-wl1", "z3-wl1", "z4-wl1", "z5-wl1", "z6-wl1")
COMMAND = "tn.tsyp.brain.command.v1"


class Rig:
    """A BrainApp over a mock Twin, with hand-driven wall time."""

    def __init__(self, tmp_path):
        self.twin = MockTwin()
        self.now = 0.0
        self.clock = SimClock()
        self.path = tmp_path / "run.jsonl"
        priv, pub = derive_keypair("test:guardian")
        self.guardian = MockGuardian(priv)
        self.keyring = KeyRing()
        self.registry = {}
        profile = self.twin.profile
        guardian_profile = GuardianProfile.load(BRAIN / "profiles" / "guardian-triplet07.yaml", public_key_b64=pub)
        self.app = BrainApp(
            load_config(BRAIN / "config" / "standalone.yaml"), InProcTwin(self.twin), self.clock,
            JsonlLog(self.path), ReadingGuard(profile, self.keyring, self.registry), VerdictGuard(guardian_profile),
            profile=profile, keyring=self.keyring, registry=self.registry, wall_now=lambda: self.now)

    def log(self):
        return [json.loads(line) for line in self.path.read_text(encoding="ascii").splitlines()]

    def reloads(self):
        return sum(e["data"].get("reason") == "registry_reloaded" for e in self.log())

    async def reject(self, *devices):
        """Readings signed with keys the Brain does not hold yet: the Twin has restarted."""
        readings = {r["device_id"]: r for r in self.twin.tick()}
        for device in devices:
            await self.app.submit_raw("readings", json.dumps(readings[device]).encode())


@pytest.fixture
async def rig(tmp_path):
    rig = Rig(tmp_path)
    await rig.app.start()
    return rig


def script(monkeypatch, *batches):
    """Replace `step`: each event gets the next batch of effects. Returns the events seen."""
    batches = list(batches)
    seen = []

    def fake_step(state, event):
        seen.append(event)
        return state, batches.pop(0) if batches else []

    monkeypatch.setattr(app_module, "step", fake_step)
    return seen


def command_event(status, state="open"):
    data = {"status": status, "capability": "valve", "target": "z1-wl1", "state": state, "tool": None}
    return {"type": COMMAND, "data": data}


def send(state="open"):
    return SendCommand("a1", "brain-0-a1", "valve", "z1-wl1", state)


async def test_start_fills_key_ring_and_registry_and_feeds_the_core(rig):
    assert set(rig.registry) == set(rig.twin.devices)
    assert rig.keyring.get("z1-wl1") == rig.twin.get_device_key("z1-wl1")
    assert rig.app.state.registry["z1-wl1"].actuator.current_state == "closed"
    assert rig.app.state.allowlist == rig.twin.profile.allowlist
    assert [e["data"]["reason"] for e in rig.log()] == ["startup"]


async def test_reading_then_verdict_reach_the_core(rig):
    reading = rig.twin.tick()[0]
    await rig.app.submit_raw("readings", json.dumps(reading).encode())
    for verdict in rig.guardian.on_reading(reading):
        await rig.app.submit_raw("verdicts", verdict)
    assert rig.app.state.global_tick == 1
    sig_hash = next(iter(rig.app.state.seen))[1]
    assert rig.app.state.ledger.latest(reading["reading_id"], sig_hash) is not None


async def test_rejected_messages_of_either_stream_are_counted(rig):
    await rig.app.submit_raw("readings", b"not json")
    await rig.app.submit_raw("verdicts", b"not json")
    assert rig.app.state.counters["MALFORMED_JSON"] == 2


async def test_unknown_stream_is_an_error(rig):
    with pytest.raises(ValueError, match="stream"):
        await rig.app.submit_raw("twin.sensors", b"{}")


async def test_five_distinct_devices_reload_keys(rig):
    old_key = rig.keyring.get("z1-wl1")
    rig.twin.restart()
    await rig.reject(*DEVICES[:4])
    await rig.reject(*DEVICES[:4])  # the same four again are still four
    assert rig.reloads() == 0
    await rig.reject(DEVICES[4])
    assert rig.reloads() == 1
    assert rig.keyring.get("z1-wl1") == rig.twin.get_device_key("z1-wl1") != old_key


async def test_reload_refreshes_the_registry_in_place(rig):
    del rig.twin.devices["z6-ec1"]
    registry = rig.registry
    await rig.app.reload_registry()
    assert "z6-ec1" not in registry
    assert "z6-ec1" not in rig.app.state.registry
    assert rig.keyring.get("z6-ec1") is None


async def test_rejects_spread_over_more_than_a_second_do_not_reload(rig):
    rig.twin.restart()
    for device in DEVICES[:5]:
        await rig.reject(device)
        rig.now += 0.3
    assert rig.reloads() == 0


async def test_reloads_at_most_once_every_ten_seconds(rig):
    rig.twin.restart()
    await rig.reject(*DEVICES[:5])
    assert rig.reloads() == 1
    rig.twin.restart()
    rig.now = 9.5
    await rig.reject(*DEVICES[:5])
    assert rig.reloads() == 1
    rig.now = 10.0
    await rig.reject(*DEVICES[:5])
    assert rig.reloads() == 2


async def test_bad_verdict_signatures_do_not_reload(rig):
    impostor = MockGuardian(derive_keypair("impostor")[0])
    readings = {r["device_id"]: r for r in rig.twin.tick()}
    for device in DEVICES:
        for verdict in impostor.on_reading(readings[device]):
            await rig.app.submit_raw("verdicts", verdict)
    assert rig.app.state.counters["BAD_SIGNATURE"] == len(DEVICES)
    assert rig.reloads() == 0


async def test_publish_fills_the_tool_of_a_dispatched_command_only(rig, monkeypatch):
    script(monkeypatch, [Publish(command_event("DISPATCHED")), Publish(command_event("REFUSED"))])
    await rig.app.feed(TimerFired("t", "k"))
    dispatched, refused = rig.log()[-2:]
    assert dispatched["data"]["tool"] == {"name": "set_valve", "args": {"device_id": "z1-wl1", "target_state": "open"}}
    assert refused["data"]["tool"] is None


async def test_command_goes_to_the_twin_and_its_result_is_fed_after_the_batch(rig, monkeypatch):
    first = [Publish(command_event("DISPATCHED")), send(), Publish(command_event("REFUSED", "closed"))]
    seen = script(monkeypatch, first, [Publish(command_event("VERIFYING"))])
    effects = await rig.app.feed(TimerFired("t", "k"))
    assert rig.twin.devices["z1-wl1"].valve_ratio == 1.0
    assert "brain-0-a1" in rig.twin.command_results
    assert isinstance(seen[1], CommandResult) and (seen[1].action_id, seen[1].outcome) == ("a1", "ACCEPTED")
    assert [e["data"]["status"] for e in rig.log()[-3:]] == ["DISPATCHED", "REFUSED", "VERIFYING"]
    assert len(effects) == 4


async def test_state_not_allowed_becomes_a_rejected_result(rig, monkeypatch):
    seen = script(monkeypatch, [Publish(command_event("DISPATCHED", "boil")), send("boil")])
    await rig.app.feed(TimerFired("t", "k"))
    assert seen[1] == CommandResult("a1", "REJECTED", {"error": "StateNotAllowed"})
    assert rig.log()[-1]["data"]["tool"] is None
    assert rig.twin.command_results == {}


async def test_unmappable_command_is_rejected_and_frees_the_target(rig, monkeypatch):
    def unmappable(*_):
        raise ProfileError("unknown capability 'valve'")

    monkeypatch.setattr(rig.twin.profile, "to_tool_call", unmappable)
    state = feed_rising_channel(state_with_registry(), "z1-wl1", until_level=6.05, verdict=None)
    [record] = state.actions.values()
    rig.app.state = state
    for ref in record.intent.evidence:
        await rig.app.feed(VerdictReceived(verdict_for("z1-wl1", seq_of(ref))))
    assert record.status is ActionStatus.REJECTED
    assert "z1-wl1" not in state.in_flight
    rejected = rig.log()[-1]
    assert rejected["data"]["status"] == "REJECTED"
    assert rejected["data"]["twin_reply"] == {"error": "ProfileError"}
    assert [e["data"]["tool"] for e in rig.log() if e["data"].get("status") == "DISPATCHED"] == [None]
    assert rig.twin.command_results == {}


async def test_timers_go_to_the_clock(rig, monkeypatch):
    script(monkeypatch, [StartTimer("t1", "k", 1.0), StartTimer("t2", "k", 1.0), CancelTimer("t1")])
    await rig.app.feed(TimerFired("t", "k"))
    assert rig.clock.advance(1.0) == [TimerFired("t2", "k")]


def statuses(rig):
    return [e["data"]["reason"] for e in rig.log() if "reason" in e["data"]]


async def test_reload_with_new_keys_starts_the_epoch_once(rig):
    rig.twin.restart()
    await rig.app.reload_registry()
    assert rig.app.state.epoch == 1
    assert statuses(rig) == ["startup", "registry_reloaded", "epoch_changed"]


async def test_reload_with_the_same_keys_keeps_the_epoch(rig):
    await rig.app.reload_registry()
    assert rig.app.state.epoch == 0


async def test_removed_device_is_not_a_key_change(rig):
    del rig.twin.devices["z6-ec1"]
    await rig.app.reload_registry()
    assert rig.app.state.epoch == 0


async def test_epoch_change_without_a_recent_reload_reloads_the_registry(rig, monkeypatch):
    script(monkeypatch, [Publish({"type": "status", "data": {"reason": "epoch_changed"}})])
    rig.now = 3.0
    await rig.app.feed(TimerFired("t", "k"))
    assert rig.app._last_reload == 3.0
    assert statuses(rig)[-1] == "epoch_changed"


async def test_epoch_change_right_after_a_reload_does_not_reload_again(rig, monkeypatch):
    await rig.app.reload_registry()
    rig.now = 9.9
    script(monkeypatch, [Publish({"type": "status", "data": {"reason": "epoch_changed"}})])
    await rig.app.feed(TimerFired("t", "k"))
    assert rig.app._last_reload == 0.0
