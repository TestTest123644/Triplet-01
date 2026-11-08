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
    GuardianEventReceived,
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
from city_brain.edge.guard import (
    Accepted,
    AcceptedGuardianEvent,
    AcceptedVerdict,
    GuardianEventGuard,
    ReadingGuard,
    Rejected,
    VerdictGuard,
)
from city_brain.edge.profiles import ProfileError, ToolCall, TwinProfile
from city_brain.edge.signatures import KeyRing
from city_brain.persistence.journal import SqliteJournal
from city_brain.ports import TwinPort
from city_brain.runtime.clocks import Clock

BAD_SIGNATURE = "BAD_SIGNATURE"
RELOAD_DEVICES = 5  # distinct devices (spec 9.3)
RELOAD_WINDOW_S = 1.0
RELOAD_MIN_GAP_S = 10.0
EPOCH_CHANGED = "epoch_changed"
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
        event_guard: GuardianEventGuard | None = None,
        profile: TwinProfile,
        keyring: KeyRing,
        registry: MutableMapping[str, DeviceProfile],
        journal: SqliteJournal | None = None,
        event_sink: Callable[[dict], None] | None = None,
        wall_now: Callable[[], float] = time.monotonic,
    ):
        self.state = BrainState.new(config.policy, config.params)
        self._twin = twin
        self._clock = clock
        self._log = log
        self._guards = {"readings": reading_guard, "verdicts": verdict_guard}
        if event_guard is not None:  # `guardian.events` is optional: a partner may send none
            self._guards["events"] = event_guard
        self._profile = profile
        self._keyring = keyring
        self._registry = registry
        self._journal = journal
        self._event_sink = event_sink
        self._replaying = False
        self._wall_now = wall_now
        self._bad_signatures: dict[str, float] = {}  # device id -> wall time of its latest reject
        self._last_reload: float | None = None
        self._keys: dict[str, str] = {}  # as last loaded

    async def start(self) -> None:
        await self._load_registry()

    async def recover(self) -> int:
        """Rebuild the state from the journal, then settle what the outbox still owes.

        Replaying must not publish the old records again or resend the old commands: the
        log and the Twin already have them. Only the commands the outbox never confirmed
        are in question, and the Twin is asked about each before anything is sent (spec 8.8).
        """
        if self._journal is None:
            return 0
        events = self._journal.replay_events()
        self._replaying = True
        try:
            for event in events:
                self.state, _ = step(self.state, event)
        finally:
            self._replaying = False
        await self._settle_outbox()
        return len(events)

    async def _settle_outbox(self) -> None:
        for effect_id, effect in self._journal.pending_outbox():
            if not isinstance(effect, SendCommand):
                self._journal.mark_dispatched(effect_id)  # the log already holds it
                continue
            cached = await self._twin.get_result(effect.idempotency_key)
            if cached is not None:
                self._journal.mark_dispatched(effect_id)
                await self.feed(CommandResult(effect.action_id, _outcome_of(cached), cached))
                continue
            result = await self._send(effect)
            self._journal.mark_dispatched(effect_id)
            await self.feed(result)

    async def reload_registry(self) -> None:
        self._last_reload = self._wall_now()
        self._bad_signatures.clear()
        await self._load_registry()

    async def submit_raw(self, stream: Literal["readings", "verdicts", "events"], raw: bytes) -> list[Effect]:
        guard = self._guards.get(stream)
        if guard is None:
            raise ValueError(f"unknown stream '{stream}'")
        result = guard.check(raw)
        if isinstance(result, Accepted):
            return await self.feed(ReadingReceived(result.observation))
        if isinstance(result, AcceptedVerdict):
            return await self.feed(VerdictReceived(result.verdict))
        if isinstance(result, AcceptedGuardianEvent):
            return await self.feed(GuardianEventReceived(result.notice))
        effects = await self.feed(ReadingRejected(result.code, result.device_id))
        if stream == "readings" and self._burst_of_bad_signatures(result):
            await self.reload_registry()
        return effects

    async def feed(self, event: Event) -> list[Effect]:
        """Step `event`, then each command result it causes, in order; returns every effect."""
        handled: list[Effect] = []
        pending: deque[Event] = deque([event])
        while pending:
            current = pending.popleft()
            self.state, effects = step(self.state, current)
            self._journal_step(current, effects)
            for effect in effects:
                result = await self._handle(effect)
                if result is not None:
                    pending.append(result)  # after this batch: the core has one writer and one order
            handled.extend(effects)
        if self._needs_reload(handled):
            await self.reload_registry()  # the restarted Twin's actuators and keys, not the old ones
        return handled

    def _journal_step(self, event: Event, effects: list[Effect]) -> None:
        if self._journal is not None and not self._replaying:
            self._journal.append_step(self.state.epoch, event, effects)

    async def _load_registry(self) -> None:
        raw_devices, keys, _ = await self._twin.load_registry()
        devices = tuple(self._profile.map_device(raw) for raw in raw_devices)
        keys_changed = any(self._keys[d] != keys[d] for d in self._keys.keys() & keys.keys())
        self._keys = dict(keys)  # only now: a failed load must not hide the change from the next one
        self._keyring.clear()
        for device_id, key in keys.items():
            self._keyring.set(device_id, key)
        self._registry.clear()
        self._registry.update({device.device_id: device for device in devices})
        await self.feed(RegistryLoaded(devices, allowlist=self._profile.allowlist, keys_changed=keys_changed))

    def _burst_of_bad_signatures(self, rejected: Rejected) -> bool:
        if rejected.code != BAD_SIGNATURE or rejected.device_id is None:
            return False
        now = self._wall_now()
        self._bad_signatures[rejected.device_id] = now
        self._bad_signatures = {d: t for d, t in self._bad_signatures.items() if now - t <= RELOAD_WINDOW_S}
        return len(self._bad_signatures) >= RELOAD_DEVICES and self._reload_allowed()

    def _reload_allowed(self) -> bool:
        return self._last_reload is None or self._wall_now() - self._last_reload >= RELOAD_MIN_GAP_S

    def _needs_reload(self, effects: list[Effect]) -> bool:
        changed = any(
            isinstance(e, Publish) and e.event["data"].get("reason") == EPOCH_CHANGED for e in effects)
        return changed and self._reload_allowed()

    async def _handle(self, effect: Effect) -> CommandResult | None:
        if self._replaying:
            return None  # the log and the Twin already have everything this step produced
        if isinstance(effect, Publish):
            record = self._log.append(self._with_tool(effect.event))
            published = True
            if self._event_sink is not None:
                res = self._event_sink(record)
                if res is False:
                    published = False
            if published:
                self._confirm_published(effect)
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
        self._confirm_sent(command)
        return CommandResult(command.action_id, outcome.outcome, outcome.reply)

    def _confirm_sent(self, command: SendCommand) -> None:
        """The Twin has the command: the outbox no longer owes it (spec 9.4)."""
        if self._journal is None:
            return
        for effect_id, effect in self._journal.pending_outbox():
            if isinstance(effect, SendCommand) and effect.idempotency_key == command.idempotency_key:
                self._journal.mark_dispatched(effect_id)

    def _confirm_published(self, effect: Publish) -> None:
        """The event has been written to the log: the outbox no longer owes it (spec 9.4)."""
        if self._journal is None:
            return
        for effect_id, eff in self._journal.pending_outbox():
            if isinstance(eff, Publish) and eff.event == effect.event:
                self._journal.mark_dispatched(effect_id)
                break


def _outcome_of(reply: dict) -> Literal["ACCEPTED", "REJECTED", "UNKNOWN"]:
    """What a cached Twin reply says happened to a command that may have been sent."""
    status = reply.get("status") if isinstance(reply, dict) else None
    if status == "ok":
        return "ACCEPTED"
    if status == "error":
        return "REJECTED"
    return "UNKNOWN"
