"""The triplet loop: Kafka in, the core, the bus out (spec 5.1-5.5, 10.4)."""

import asyncio
import json
from pathlib import Path

import pytest

from city_brain.adapters.files.jsonl_log import JsonlLog
from city_brain.adapters.inproc_twin import InProcTwin
from city_brain.config import load_config
from city_brain.core.events import TimerFired
from city_brain.edge.guard import GuardianEventGuard, ReadingGuard, VerdictGuard
from city_brain.edge.profiles import GuardianProfile
from city_brain.edge.signatures import KeyRing
from city_brain.persistence.journal import SqliteJournal
from city_brain.runtime.app import BrainApp
from city_brain.runtime.clocks import RealClock
from city_brain.runtime.triplet import TripletRuntime, build_servers, log_path
from mocks.guardian.mock_guardian import MockGuardian
from mocks.keys import derive_keypair
from mocks.twin import MockTwin

BRAIN = Path(__file__).resolve().parents[2]
CONFIG = load_config(BRAIN / "config" / "standalone.yaml")


class FakeSource:
    """Stands in for Kafka: batches go in, offsets are counted, pauses are recorded."""

    def __init__(self, batches=()):
        self.batches = list(batches)
        self.connected = False
        self.commits = 0
        self.closed = False
        self.backpressure: list[int] = []

    def connect(self):
        self.connected = True

    def poll(self, timeout_s=0.1):
        return self.batches.pop(0) if self.batches else []

    def commit(self):
        self.commits += 1

    def apply_backpressure(self, depth):
        self.backpressure.append(depth)
        return False

    def close(self):
        self.closed = True


class FakeSink:
    def __init__(self):
        self.events: list[dict] = []
        self.connected = False
        self.closed = False

    def connect(self):
        self.connected = True

    def publish(self, event):
        self.events.append(event)
        return True

    def close(self):
        self.closed = True


def rig(tmp_path, batches=(), journal=None, name="triplet"):
    twin = MockTwin()
    priv, pub = derive_keypair("triplet:guardian")
    guardian = MockGuardian(priv)
    profile = GuardianProfile.load(
        BRAIN / "profiles" / "guardian-triplet07.yaml", public_key_b64=pub)
    keyring, registry = KeyRing(), {}
    timers: asyncio.Queue = asyncio.Queue()
    sink = FakeSink()
    app = BrainApp(
        CONFIG, InProcTwin(twin), RealClock(timers), JsonlLog(tmp_path / f"{name}.jsonl"),
        ReadingGuard(twin.profile, keyring, registry), VerdictGuard(profile),
        event_guard=GuardianEventGuard(profile), profile=twin.profile, keyring=keyring,
        registry=registry, journal=journal, event_sink=sink.publish, wall_now=lambda: 0.0)
    source = FakeSource(batches)
    runtime = TripletRuntime(app=app, source=source, sink=sink, timers=timers)
    return runtime, twin, guardian


def stream_of(twin, guardian, ticks=2):
    """One batch per tick, in the shape the source hands out."""
    batches = []
    for _ in range(ticks):
        batch = []
        for reading in twin.tick():
            batch.append(("readings", json.dumps(reading, separators=(",", ":")).encode()))
            for verdict in guardian.on_reading(reading):
                batch.append(("verdicts", verdict))
        batches.append(batch)
    return batches


# ----- the loop -----


async def test_starting_connects_both_partners_and_loads_the_registry(tmp_path):
    runtime, twin, _ = rig(tmp_path)
    await runtime.start()
    assert runtime.source.connected and runtime.sink.connected
    assert len(runtime.app.state.registry) == len(twin.devices)


async def test_a_batch_reaches_the_core_and_the_offsets_follow(tmp_path):
    runtime, twin, guardian = rig(tmp_path)
    await runtime.start()
    runtime.source.batches = stream_of(twin, guardian, ticks=1)
    taken = await runtime.pump_once()
    assert taken == 42 * 2, "every reading and its verdict"
    assert runtime.app.state.global_tick > 0
    assert runtime.source.commits == 1, "committed once, after the Brain took them on"


async def test_the_offsets_only_move_after_the_messages_are_handled(tmp_path):
    """The commit is the at-least-once boundary: it must come last."""
    runtime, twin, guardian = rig(tmp_path)
    await runtime.start()
    order: list[str] = []
    runtime.source.commit = lambda: order.append("commit")
    original = runtime.app.submit_raw

    async def watch(stream, raw):
        order.append("submit")
        return await original(stream, raw)

    runtime.app.submit_raw = watch
    runtime.source.batches = stream_of(twin, guardian, ticks=1)
    await runtime.pump_once()
    assert order[-1] == "commit" and order.count("submit") > 1


async def test_every_published_record_reaches_the_bus(tmp_path):
    runtime, twin, guardian = rig(tmp_path)
    await runtime.start()
    runtime.source.batches = stream_of(twin, guardian, ticks=3)
    for _ in range(3):
        await runtime.pump_once()
    logged = [json.loads(line) for line in (tmp_path / "triplet.jsonl").read_bytes().splitlines()]
    assert runtime.sink.events == logged, "the bus carries exactly what the log holds"
    assert all("hash" in event for event in runtime.sink.events)


async def test_backpressure_is_reported_every_turn(tmp_path):
    runtime, twin, guardian = rig(tmp_path)
    await runtime.start()
    runtime.source.batches = stream_of(twin, guardian, ticks=1)
    await runtime.pump_once()
    await runtime.pump_once()
    assert runtime.source.backpressure == [84, 0]


async def test_a_timer_fires_into_the_same_loop(tmp_path):
    runtime, twin, guardian = rig(tmp_path)
    await runtime.start()
    runtime.source.batches = stream_of(twin, guardian, ticks=1)
    await runtime.pump_once()
    runtime.timers.put_nowait(TimerFired("guardian_hb", "guardian_heartbeat"))
    await runtime.pump_once()
    assert str(runtime.app.state.mode) == "GUARDIAN_DEGRADED"


async def test_an_empty_poll_is_not_an_error(tmp_path):
    runtime, _, _ = rig(tmp_path)
    await runtime.start()
    assert await runtime.pump_once() == 0


async def test_the_loop_stops_when_asked(tmp_path):
    runtime, twin, guardian = rig(tmp_path)
    await runtime.start()
    runtime.source.batches = stream_of(twin, guardian, ticks=1)

    async def stop_soon():
        await asyncio.sleep(0.05)
        runtime.stop()

    await asyncio.gather(runtime.run(), stop_soon())
    assert runtime.stopped and runtime.pumped > 0


async def test_closing_closes_both_partners(tmp_path):
    runtime, _, _ = rig(tmp_path)
    await runtime.start()
    await runtime.close()
    assert runtime.source.closed and runtime.sink.closed


# ----- recovery, on the way up -----


async def test_starting_replays_a_journal_it_finds(tmp_path):
    first, twin, guardian = rig(tmp_path, journal=SqliteJournal(tmp_path / "brain.db"))
    await first.start()
    first.source.batches = stream_of(twin, guardian, ticks=2)
    await first.pump_once()
    await first.pump_once()
    before = first.app.state.global_tick
    assert before > 0
    first.app._journal.close()

    second, _, _ = rig(tmp_path, journal=SqliteJournal(tmp_path / "brain.db"), name="revived")
    await second.start()
    assert second.app.state.global_tick == before
    # The replay itself writes nothing; the only new record is the registry it reloads, so
    # that a Twin which restarted while the Brain was down is noticed on the way up.
    revived_log = [
        json.loads(line) for line in (tmp_path / "revived.jsonl").read_bytes().splitlines()]
    assert [e["data"]["reason"] for e in revived_log] == ["registry_reloaded"]


# ----- the faces -----


async def test_the_runtime_serves_both_faces(tmp_path):
    from fastapi import FastAPI
    from mcp.server.fastmcp import FastMCP

    runtime, _, _ = rig(tmp_path)
    await runtime.start()
    rest, mcp = build_servers(runtime, "token")
    assert isinstance(rest, FastAPI) and isinstance(mcp, FastMCP)
    tools = await mcp.list_tools()
    assert sorted(t.name for t in tools) == ["explain_decision", "get_zone_state", "list_incidents"]


def test_the_log_name_carries_the_epoch(tmp_path):
    config = CONFIG.model_copy(update={"logs_dir": tmp_path})
    path = log_path(config, "triplet")
    assert path.parent == tmp_path and path.name.startswith("triplet-e0-")
    assert path.suffix == ".jsonl"


# ----- the real adapters are wired from the config -----


def test_the_triplet_config_names_the_partner_topics(tmp_path):
    from city_brain.runtime.triplet import build_triplet_runtime

    config = load_config(BRAIN / "config" / "triplet.yaml").model_copy(
        update={"logs_dir": tmp_path})
    runtime = build_triplet_runtime(
        config, twin=object(), journal_path=None, guardian_key="x" * 44,
        log=JsonlLog(tmp_path / "wired.jsonl"))
    assert runtime.source.topics == ["guardian.events", "guardian.verdicts", "twin.sensors"]
    assert runtime.sink.topic == "brain.events"


def test_the_runtime_falls_back_to_the_standard_topics(tmp_path):
    from city_brain.runtime.triplet import build_triplet_runtime

    config = load_config(BRAIN / "config" / "triplet.yaml").model_copy(
        update={"logs_dir": tmp_path, "kafka": load_config(
            BRAIN / "config" / "triplet.yaml").kafka.model_copy(update={"topics": {}})})
    runtime = build_triplet_runtime(
        config, twin=object(), guardian_key="x" * 44, log=JsonlLog(tmp_path / "fallback.jsonl"))
    assert runtime.source.topics == ["guardian.events", "guardian.verdicts", "twin.sensors"]
