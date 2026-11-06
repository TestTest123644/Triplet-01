"""The Brain process: edge guards in front of `step`, effect handlers behind it (spec 6.1)."""

import time
from collections import deque
from collections.abc import Callable, MutableMapping
from typing import Literal

from city_brain.adapters.files.jsonl_log import JsonlLog
from city_brain.config import BrainConfig
from city_brain.core.events import (
    CancelTimer,
    CommandResult,
    Effect,
    Event,
    Publish,
    ReadingReceived,
    ReadingRejected,
    RegistryLoaded,
    SendCommand,
    StartTimer,
    VerdictReceived,
)
from city_brain.core.state import BrainState
from city_brain.core.step import step
from city_brain.domain.types import DeviceProfile
from city_brain.edge.guard import Accepted, AcceptedVerdict, ReadingGuard, Rejected, VerdictGuard
from city_brain.edge.profiles import ProfileError, ToolCall, TwinProfile
from city_brain.edge.signatures import KeyRing
from city_brain.ports import TwinPort
from city_brain.runtime.clocks import Clock

BAD_SIGNATURE = "BAD_SIGNATURE"
RELOAD_DEVICES = 5  # distinct devices (spec 9.3)
RELOAD_WINDOW_S = 1.0
RELOAD_MIN_GAP_S = 10.0
DISPATCHED_COMMAND = ("tn.tsyp.brain.command.v1", "DISPATCHED")


class BrainApp:
    """`keyring` and `registry` are the objects `reading_guard` holds; reloads refresh them in place."""

    def __init__(
        self,
        config: BrainConfig,
        twin: TwinPort,
        clock: Clock,
        log: JsonlLog,
        reading_guard: ReadingGuard,
        verdict_guard: VerdictGuard,
        *,
        profile: TwinProfile,
        keyring: KeyRing,
        registry: MutableMapping[str, DeviceProfile],
        wall_now: Callable[[], float] = time.monotonic,
    ):
        self.state = BrainState.new(config.policy, config.params)
        self._twin = twin
        self._clock = clock
        self._log = log
        self._guards = {"readings": reading_guard, "verdicts": verdict_guard}
        self._profile = profile
        self._keyring = keyring
        self._registry = registry
        self._wall_now = wall_now
        self._bad_signatures: dict[str, float] = {}  # device id -> wall time of its latest reject
        self._last_reload: float | None = None

    async def start(self) -> None:
        await self._load_registry()

    async def reload_registry(self) -> None:
        self._last_reload = self._wall_now()
        self._bad_signatures.clear()
        await self._load_registry()

    async def submit_raw(self, stream: Literal["readings", "verdicts"], raw: bytes) -> list[Effect]:
        guard = self._guards.get(stream)
        if guard is None:
            raise ValueError(f"unknown stream '{stream}'")
        result = guard.check(raw)
        if isinstance(result, Accepted):
            return await self.feed(ReadingReceived(result.observation))
        if isinstance(result, AcceptedVerdict):
            return await self.feed(VerdictReceived(result.verdict))
        effects = await self.feed(ReadingRejected(result.code, result.device_id))
        if stream == "readings" and self._burst_of_bad_signatures(result):
            await self.reload_registry()
        return effects

    async def feed(self, event: Event) -> list[Effect]:
        """Step `event`, then each command result it causes, in order; returns every effect."""
        handled: list[Effect] = []
        pending: deque[Event] = deque([event])
        while pending:
            self.state, effects = step(self.state, pending.popleft())
            for effect in effects:
                result = await self._handle(effect)
                if result is not None:
                    pending.append(result)  # after this batch: the core has one writer and one order
            handled.extend(effects)
        return handled

    async def _load_registry(self) -> None:
        raw_devices, keys, _ = await self._twin.load_registry()
        devices = tuple(self._profile.map_device(raw) for raw in raw_devices)
        self._keyring.clear()
        for device_id, key in keys.items():
            self._keyring.set(device_id, key)
        self._registry.clear()
        self._registry.update({device.device_id: device for device in devices})
        await self.feed(RegistryLoaded(devices, allowlist=self._profile.allowlist))

    def _burst_of_bad_signatures(self, rejected: Rejected) -> bool:
        if rejected.code != BAD_SIGNATURE or rejected.device_id is None:
            return False
        now = self._wall_now()
        self._bad_signatures[rejected.device_id] = now
        self._bad_signatures = {d: t for d, t in self._bad_signatures.items() if now - t <= RELOAD_WINDOW_S}
        if len(self._bad_signatures) < RELOAD_DEVICES:
            return False
        return self._last_reload is None or now - self._last_reload >= RELOAD_MIN_GAP_S

    async def _handle(self, effect: Effect) -> CommandResult | None:
        if isinstance(effect, Publish):
            self._log.append(self._with_tool(effect.event))
        elif isinstance(effect, SendCommand):
            return await self._send(effect)
        elif isinstance(effect, StartTimer):
            self._clock.start(effect.timer_id, effect.kind, effect.wall_seconds)
        elif isinstance(effect, CancelTimer):
            self._clock.cancel(effect.timer_id)
        return None

    def _with_tool(self, event: dict) -> dict:
        """The core knows capabilities, not partner tools, so the runtime names the tool it calls."""
        data = event["data"]
        if (event["type"], data.get("status")) != DISPATCHED_COMMAND:
            return event
        call = self._tool_call(data["capability"], data["target"], data["state"])
        if isinstance(call, ProfileError):
            return event  # the SendCommand that follows reports it
        return {**event, "data": {**data, "tool": {"name": call.tool, "args": call.args}}}

    def _tool_call(self, capability: str, target: str, state: str) -> ToolCall | ProfileError:
        """The partner call, or why there is none: a bad mapping must end as REJECTED, not crash the loop."""
        try:
            return self._profile.to_tool_call(capability, target, state)
        except ProfileError as exc:
            return exc

    async def _send(self, command: SendCommand) -> CommandResult:
        call = self._tool_call(command.capability, command.target, command.state)
        if isinstance(call, ProfileError):
            return CommandResult(command.action_id, "REJECTED", {"error": type(call).__name__})
        outcome = await self._twin.call(call, command.idempotency_key)
        return CommandResult(command.action_id, outcome.outcome, outcome.reply)
