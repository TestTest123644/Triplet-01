"""The Brain on the triplet network: Kafka in, MCP out, HTTP and MCP for anyone asking.

One task owns the core. Messages come off Kafka in batches, go through the edge guard and
into `step`; offsets are committed only once the Brain has taken them on, and the source is
paused when the Brain falls behind. Timers fire into the same loop, so simulation order and
live order are the same order.

Every partner is injected, so the loop can be driven without a broker or a Twin.
"""

import asyncio
import contextlib
import time
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from city_brain.adapters.files.jsonl_log import JsonlLog
from city_brain.config import BrainConfig
from city_brain.core.events import Event
from city_brain.edge.guard import GuardianEventGuard, ReadingGuard, VerdictGuard
from city_brain.edge.profiles import GuardianProfile, TwinProfile
from city_brain.edge.signatures import KeyRing
from city_brain.persistence.journal import SqliteJournal
from city_brain.runtime.app import BrainApp
from city_brain.runtime.clocks import RealClock

REST_PORT = int(os.getenv("BRAIN_REST_PORT", os.getenv("BRAIN__REST_PORT", "8200")))
MCP_PORT = int(os.getenv("BRAIN_MCP_PORT", os.getenv("BRAIN__MCP_PORT", "8110")))
PROFILES = Path(__file__).resolve().parents[3] / "profiles"
SCHEMA_DIR = Path(__file__).resolve().parents[3] / "schemas"


@dataclass
class TripletRuntime:
    """The Brain, its partners and the loop that joins them."""

    app: BrainApp
    source: Any
    sink: Any | None = None
    timers: asyncio.Queue | None = None
    poll_timeout_s: float = 0.1
    stopped: bool = False
    pumped: int = 0
    queue_depth: int = 0
    _started_at: float = field(default_factory=time.monotonic)

    async def start(self) -> None:
        self.source.connect()
        if self.sink is not None:
            self.sink.connect()
        # Replay first, then look at the world: the journal says what the Brain knew, and
        # the registry load that follows reports the Twin as it is now, keys and all.
        await self.app.recover()
        await self.app.start()

    async def pump_once(self) -> int:
        """One turn of the loop: what is waiting, then what the clock says. Returns messages taken."""
        batch = self.source.poll(self.poll_timeout_s)
        self.queue_depth = len(batch)
        self.source.apply_backpressure(self.queue_depth)
        for stream, raw in batch:
            await self.app.submit_raw(stream, raw)
        # Only now: the Brain has journalled and acted on them, so the offsets may move.
        self.source.commit()
        if self.sink is not None:
            flush = getattr(self.sink, "flush", None)
            if flush is not None:
                flush(0.0)
        await self._fire_timers()
        self.pumped += 1
        return len(batch)

    async def _fire_timers(self) -> None:
        if self.timers is None:
            return
        while True:
            try:
                fired: Event = self.timers.get_nowait()
            except asyncio.QueueEmpty:
                return
            await self.app.feed(fired)

    async def run(self) -> None:
        while not self.stopped:
            taken = await self.pump_once()
            if taken == 0:
                await asyncio.sleep(0)  # let the other tasks breathe

    def stop(self) -> None:
        self.stopped = True

    async def close(self) -> None:
        self.stop()
        with contextlib.suppress(Exception):
            self.source.close()
        if self.sink is not None:
            with contextlib.suppress(Exception):
                self.sink.close()
        close = getattr(self.app._twin, "close", None)
        if close is not None:
            with contextlib.suppress(Exception):
                await close()


def log_path(config: BrainConfig, name: str = "triplet") -> Path:
    started = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return Path(config.logs_dir) / f"{name}-e0-{started}.jsonl"


def build_triplet_runtime(
    config: BrainConfig,
    twin: Any | None = None,
    source: Any | None = None,
    sink: Any | None = None,
    journal_path: Path | None = None,
    guardian_key: str | None = None,
    log: JsonlLog | None = None,
) -> TripletRuntime:
    """Wire the Brain to the triplet's Kafka topics and the Twin's MCP server (spec 5.1-5.5)."""
    from city_brain.adapters.kafka import KafkaSink, KafkaSource
    from city_brain.adapters.mcp_twin import McpTwinClient

    twin_profile = TwinProfile.load(profile_path(config.twin.profile))
    guardian_profile = GuardianProfile.load(
        profile_path(config.guardian.profile),
        public_key_b64=guardian_key or _guardian_key(config))
    keyring, registry = KeyRing(), {}
    twin = twin if twin is not None else McpTwinClient(
        config.twin.mcp_url,
        timeout_s=config.twin.call_timeout_s,
        auth_token=config.twin.resolve_auth_token(),
    )
    topics = dict(config.kafka.topics) or {
        "readings": "twin.sensors", "verdicts": "guardian.verdicts", "events": "guardian.events"}
    security_cfg = config.kafka.client_security_config()
    source = source if source is not None else KafkaSource(
        config.kafka.bootstrap, config.kafka.group, topics, security_config=security_cfg)
    sink = sink if sink is not None else KafkaSink(
        config.kafka.bootstrap, security_config=security_cfg, acks=config.kafka.acks)
    timers: asyncio.Queue = asyncio.Queue()
    journal = SqliteJournal(journal_path) if journal_path is not None else None
    app = BrainApp(
        config, twin, RealClock(timers), log if log is not None else JsonlLog(log_path(config)),
        ReadingGuard(twin_profile, keyring, registry), VerdictGuard(guardian_profile),
        event_guard=GuardianEventGuard(guardian_profile), profile=twin_profile,
        keyring=keyring, registry=registry, journal=journal,
        event_sink=sink.publish if sink is not None else None,
    )
    return TripletRuntime(app=app, source=source, sink=sink, timers=timers)


def profile_path(name: str) -> Path:
    """The profile file a config names, with or without its version suffix."""
    candidates = [f"{name}.yaml", f"{name.removesuffix('-v1')}.yaml", f"{name}-v1.yaml"]
    for candidate in candidates:
        found = PROFILES / candidate
        if found.is_file():
            return found
    raise FileNotFoundError(
        f"no profile for '{name}' in {PROFILES}: looked for {', '.join(candidates)}")


def _guardian_key(config: BrainConfig) -> str | None:
    path = config.guardian.public_key_file
    if not path:
        return None
    found = Path(path)
    if not found.is_absolute():
        found = PROFILES.parent / path
    return found.read_text(encoding="ascii").strip() if found.is_file() else None


def build_servers(runtime: TripletRuntime, operator_token: str, decisions: Callable | None = None):
    """The REST and read-only MCP faces over a running triplet runtime (spec 5.6)."""
    from city_brain.adapters.mcp_server import build_brain_mcp_server
    from city_brain.adapters.rest.api import build_rest_app

    records = decisions() if decisions is not None else ()
    return build_rest_app(runtime.app, operator_token, records), build_brain_mcp_server(
        runtime.app, records)


async def serve(config: BrainConfig, operator_token: str, journal_path: Path | None = None) -> int:
    """Run the Brain, its REST API and its MCP server until something stops them."""
    import uvicorn

    runtime = build_triplet_runtime(config, journal_path=journal_path)
    rest, mcp = build_servers(runtime, operator_token)
    mcp.settings.host, mcp.settings.port = "0.0.0.0", MCP_PORT
    await runtime.start()
    rest_server = uvicorn.Server(uvicorn.Config(
        rest, host="0.0.0.0", port=REST_PORT, log_level="info"))
    tasks = [
        asyncio.create_task(runtime.run(), name="brain"),
        asyncio.create_task(rest_server.serve(), name="rest"),
        asyncio.create_task(mcp.run_sse_async(), name="mcp"),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        runtime.stop()
        for task in tasks:
            task.cancel()
        await runtime.close()
    return 0
