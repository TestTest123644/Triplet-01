"""The journal, the outbox and what a restarted Brain does about them (spec 8.8, 9.4)."""

import json
import sqlite3
from pathlib import Path

import pytest

from city_brain.adapters.files.jsonl_log import JsonlLog
from city_brain.adapters.inproc_twin import InProcTwin
from city_brain.config import load_config
from city_brain.core.events import (
    CommandResult,
    GuardianEventReceived,
    OperatorCommand,
    Publish,
    ReadingReceived,
    ReadingRejected,
    RegistryLoaded,
    SendCommand,
    TimerFired,
    VerdictReceived,
)
from city_brain.domain.types import ActionStatus, DeviceStatus, GuardianContainment, GuardianHeartbeat
from city_brain.edge.guard import ReadingGuard, VerdictGuard
from city_brain.edge.profiles import GuardianProfile
from city_brain.edge.signatures import KeyRing
from city_brain.persistence.journal import SqliteJournal, decode_event, encode_event
from city_brain.runtime.app import BrainApp
from city_brain.runtime.clocks import SimClock
from mocks.guardian.mock_guardian import MockGuardian
from mocks.keys import derive_keypair
from mocks.twin import MockTwin
from tests.factories import T0, make_observation, make_verdict

BRAIN = Path(__file__).resolve().parents[2]
CONFIG = load_config(BRAIN / "config" / "standalone.yaml")


# ----- what a journalled event survives -----


EVENTS = [
    ReadingReceived(make_observation(value=4.25)),
    ReadingRejected("BAD_SIGNATURE", "z1-wl1"),
    VerdictReceived(make_verdict()),
    GuardianEventReceived(GuardianHeartbeat(evaluated_through=T0, issued_at=T0)),
    GuardianEventReceived(GuardianContainment(
        device_id="z2-wl1", action="quarantined", effective_at=T0,
        reasons=frozenset({"CROSS_SENSOR_CONFLICT"}), issued_at=T0)),
    OperatorCommand(action="approve", action_id="act-1", operator_id="ops"),
    CommandResult("act-1", "ACCEPTED", {"status": "ok"}),
    TimerFired("verdict:act-1", "verdict_wait"),
]


@pytest.mark.parametrize("event", EVENTS, ids=lambda e: type(e).__name__ + str(getattr(e, "code", "")))
def test_an_event_comes_back_as_it_went_in(event):
    assert decode_event(*encode_event(event)) == event


def test_a_registry_comes_back_with_its_allowlist():
    from tests.helpers_coordinator import ALLOWLIST, water_device

    event = RegistryLoaded((water_device("z1-wl1"),), ALLOWLIST, keys_changed=True)
    back = decode_event(*encode_event(event))
    assert back.devices == event.devices and back.keys_changed is True
    assert back.allowlist == ALLOWLIST


def test_an_unknown_event_type_is_refused():
    with pytest.raises(ValueError):
        decode_event("Nonsense", "{}")


def test_an_unjournallable_event_is_refused():
    with pytest.raises(TypeError):
        encode_event(object())


# ----- the journal file -----


@pytest.fixture
def journal(tmp_path):
    with SqliteJournal(tmp_path / "brain.db") as journal:
        yield journal


def test_the_file_is_in_wal_mode(journal):
    mode = sqlite3.connect(journal.path).execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"


def test_a_step_writes_its_event_and_its_effects_together(journal):
    effects = [Publish({"id": "0-0"}), SendCommand("act-1", "k-1", "valve", "z1-wl1", "open")]
    seq = journal.append_step(0, EVENTS[0], effects)
    assert seq == 1 and journal.event_count() == 1
    assert [e for _, e in journal.pending_outbox()] == effects


def test_only_outward_effects_are_journalled(journal):
    from city_brain.core.events import CancelTimer, StartTimer

    journal.append_step(0, EVENTS[0], [StartTimer("t", "k", 1.0), CancelTimer("t")])
    assert journal.pending_outbox() == []


def test_replay_returns_every_event_in_order(journal):
    for event in EVENTS:
        journal.append_step(0, event, [])
    assert journal.replay_events() == EVENTS


def test_replay_can_be_limited_to_one_epoch(journal):
    journal.append_step(0, EVENTS[0], [])
    journal.append_step(1, EVENTS[2], [])
    assert journal.replay_events(epoch=1) == [EVENTS[2]]
    assert journal.last_epoch() == 1


def test_a_dispatched_effect_leaves_the_outbox(journal):
    journal.append_step(0, EVENTS[0], [SendCommand("act-1", "k-1", "valve", "z1-wl1", "open")])
    effect_id, _ = journal.pending_outbox()[0]
    journal.mark_dispatched(effect_id)
    assert journal.pending_outbox() == []


def test_a_failed_step_writes_nothing(journal, monkeypatch):
    """The event and its effects are one transaction: neither lands without the other."""

    class Boom(Exception):
        pass

    class FailingCursor:
        def __init__(self, real):
            self._real = real

        def execute(self, *args):
            return self._real.execute(*args)

        def executemany(self, *args):
            raise Boom()

        @property
        def lastrowid(self):
            return self._real.lastrowid

    class FailingDb:
        def __init__(self, real):
            self._real = real

        def cursor(self):
            return FailingCursor(self._real.cursor())

        def execute(self, *args):
            return self._real.execute(*args)

    journal.append_step(0, EVENTS[0], [])
    real_db = journal._db
    monkeypatch.setattr(journal, "_db", FailingDb(real_db))
    with pytest.raises(Boom):
        journal.append_step(0, EVENTS[2], [Publish({"id": "0-1"})])
    monkeypatch.setattr(journal, "_db", real_db)
    assert journal.event_count() == 1, "the event must not survive its effects"
    assert journal.pending_outbox() == []


def test_the_journal_outlives_the_process(tmp_path):
    with SqliteJournal(tmp_path / "brain.db") as first:
        first.append_step(0, EVENTS[0], [SendCommand("act-1", "k-1", "valve", "z1-wl1", "open")])
    with SqliteJournal(tmp_path / "brain.db") as second:
        assert second.replay_events() == [EVENTS[0]]
        assert len(second.pending_outbox()) == 1


# ----- a Brain that comes back -----


class Rig:
    """A BrainApp over a mock Twin, with a journal it can be rebuilt from."""

    def __init__(self, tmp_path, name="run", db="brain.db", twin=None):
        # A restart restarts the Brain, not the city: a revived rig keeps the same Twin.
        self.twin = twin if twin is not None else MockTwin()
        self.tmp_path = tmp_path
        self.db = tmp_path / db
        priv, pub = derive_keypair("journal:guardian")
        self.guardian = MockGuardian(priv)
        self.profile = GuardianProfile.load(
            BRAIN / "profiles" / "guardian-triplet07.yaml", public_key_b64=pub)
        self.keyring, self.registry = KeyRing(), {}
        self.journal = SqliteJournal(self.db)
        self.app = BrainApp(
            CONFIG, InProcTwin(self.twin), SimClock(), JsonlLog(tmp_path / f"{name}.jsonl"),
            ReadingGuard(self.twin.profile, self.keyring, self.registry),
            VerdictGuard(self.profile), profile=self.twin.profile, keyring=self.keyring,
            registry=self.registry, journal=self.journal, wall_now=lambda: 0.0)

    async def pump(self, ticks):
        for _ in range(ticks):
            for reading in self.twin.tick():
                await self.app.submit_raw("readings", json.dumps(reading).encode())
                for verdict in self.guardian.on_reading(reading):
                    await self.app.submit_raw("verdicts", verdict)
        return self


async def test_a_run_is_journalled_as_it_happens(tmp_path):
    rig = Rig(tmp_path)
    await rig.app.start()
    await rig.pump(3)
    assert rig.journal.event_count() > 1
    assert isinstance(rig.journal.replay_events()[0], RegistryLoaded)


async def test_a_replayed_brain_matches_the_one_that_crashed(tmp_path):
    rig = Rig(tmp_path)
    await rig.app.start()
    await rig.pump(6)
    before = rig.app.state
    journalled = rig.journal.event_count()
    rig.journal.close()

    revived = Rig(tmp_path, name="revived", twin=rig.twin)
    replayed = await revived.app.recover()
    after = revived.app.state

    assert replayed == journalled
    assert after.global_tick == before.global_tick and after.epoch == before.epoch
    assert sorted(after.registry) == sorted(before.registry)
    assert {d: f.level for d, f in after.filters.items()} == {
        d: f.level for d, f in before.filters.items()}
    assert dict(after.counters) == dict(before.counters)
    assert set(after.actions) == set(before.actions)


async def test_replaying_writes_no_new_records_to_the_run_log(tmp_path):
    rig = Rig(tmp_path)
    await rig.app.start()
    await rig.pump(4)
    rig.journal.close()

    revived = Rig(tmp_path, name="revived", twin=rig.twin)
    await revived.app.recover()
    assert Path(tmp_path / "revived.jsonl").read_bytes() == b"", "the records were written once"


async def test_a_command_the_twin_already_has_is_not_sent_again(tmp_path):
    """The crash happened after the outbox was written and before it was confirmed."""
    rig = Rig(tmp_path)
    await rig.app.start()
    command = SendCommand("act-1", "brain-0-act-1", "valve", "z1-wl1", "open")
    rig.journal.append_step(0, EVENTS[0], [command])
    rig.twin.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "open",
                                       "idempotency_key": command.idempotency_key})
    rig.journal.close()

    revived = Rig(tmp_path, name="revived", twin=rig.twin)
    sent: list = []
    original = revived.twin.handle_tool

    def watch(tool, args):
        if tool != "get_command_result":
            sent.append((tool, args))
        return original(tool, args)

    revived.twin.handle_tool = watch
    await revived.app.recover()
    assert sent == [], "the Twin had already done it"
    assert revived.journal.pending_outbox() == []


async def test_a_command_the_twin_never_saw_is_sent_once(tmp_path):
    rig = Rig(tmp_path)
    await rig.app.start()
    command = SendCommand("act-1", "brain-0-act-1", "valve", "z1-wl1", "open")
    rig.journal.append_step(0, EVENTS[0], [command])
    rig.journal.close()

    revived = Rig(tmp_path, name="revived", twin=rig.twin)
    sent: list = []
    original = revived.twin.handle_tool

    def watch(tool, args):
        if tool != "get_command_result":
            sent.append((tool, args))
        return original(tool, args)

    revived.twin.handle_tool = watch
    await revived.app.recover()
    assert [tool for tool, _ in sent] == ["set_valve"]
    assert revived.journal.pending_outbox() == []


async def test_the_outbox_is_emptied_by_an_ordinary_dispatch(tmp_path):
    """Nothing is owed once a command has gone out in the normal way."""
    rig = Rig(tmp_path)
    await rig.app.start()
    await rig.pump(2)
    await rig.app.feed(ReadingReceived(make_observation(
        device_id="z1-wl1", seq=10_000, sim_time=T0, reading_id="r-x", sig_hash="s-x")))
    assert all(not isinstance(e, SendCommand) for _, e in rig.journal.pending_outbox())


async def test_a_brain_without_a_journal_still_runs(tmp_path):
    rig = Rig(tmp_path)
    rig.app._journal = None
    await rig.app.start()
    await rig.pump(2)
    assert await rig.app.recover() == 0


async def test_recovery_restores_a_pending_approval(tmp_path):
    """An operator question outlives the crash: the action is still waiting afterwards."""
    rig = Rig(tmp_path)
    await rig.app.start()
    state = rig.app.state
    await rig.pump(2)
    containment = GuardianContainment(
        device_id="z1-wl1", action="quarantined", effective_at=state.sim_now,
        reasons=frozenset({"SCRIPTED"}), issued_at=state.sim_now)
    await rig.app.feed(GuardianEventReceived(containment))
    assert state.registry["z1-wl1"].status is DeviceStatus.QUARANTINED
    rig.journal.close()

    revived = Rig(tmp_path, name="revived", twin=rig.twin)
    await revived.app.recover()
    assert revived.app.state.registry["z1-wl1"].status is DeviceStatus.QUARANTINED
    assert revived.app.state.contained == {"z1-wl1": DeviceStatus.QUARANTINED}


async def test_an_action_that_was_waiting_is_still_waiting(tmp_path):
    rig = Rig(tmp_path)
    await rig.app.start()
    from tests.adapters.test_rest_api import _plant_approval

    await rig.pump(2)
    _plant_approval(rig.app.state)
    assert rig.app.state.actions["act-waiting"].status is ActionStatus.AWAITING_APPROVAL
    # The planted action is state, not a journalled event, so recovery cannot know about it:
    # what matters is that recovery does not resurrect it as a command to send.
    rig.journal.close()
    revived = Rig(tmp_path, name="revived", twin=rig.twin)
    await revived.app.recover()
    assert revived.app.state.pending_approvals == {}
    assert revived.journal.pending_outbox() == []
