"""Wall-clock timers (spec 9.1): real ones for live runs, simulated ones for deterministic runs."""

import asyncio
from typing import Protocol

from city_brain.core.events import TimerFired


class Clock(Protocol):
    def start(self, timer_id: str, kind: str, wall_seconds: float) -> None: ...

    def cancel(self, timer_id: str) -> None: ...


class RealClock:
    """Timers on the running event loop; a fired timer lands in `queue` for the core task."""

    def __init__(self, queue: asyncio.Queue):
        self._queue = queue
        self._handles: dict[str, asyncio.TimerHandle] = {}

    def start(self, timer_id: str, kind: str, wall_seconds: float) -> None:
        self.cancel(timer_id)
        loop = asyncio.get_running_loop()
        self._handles[timer_id] = loop.call_later(wall_seconds, self._fire, timer_id, kind)

    def cancel(self, timer_id: str) -> None:
        handle = self._handles.pop(timer_id, None)
        if handle is not None:
            handle.cancel()

    def _fire(self, timer_id: str, kind: str) -> None:
        del self._handles[timer_id]
        self._queue.put_nowait(TimerFired(timer_id, kind))


class SimClock:
    """Timers on a hand-advanced clock. Fires in due-time order, ties in start order."""

    def __init__(self) -> None:
        self._now = 0.0
        self._order = 0
        self._timers: dict[str, tuple[float, int, str]] = {}  # timer_id -> (due, start order, kind)

    def start(self, timer_id: str, kind: str, wall_seconds: float) -> None:
        self._order += 1
        self._timers[timer_id] = (self._now + wall_seconds, self._order, kind)

    def cancel(self, timer_id: str) -> None:
        self._timers.pop(timer_id, None)

    def advance(self, seconds: float) -> list[TimerFired]:
        self._now += seconds
        due = sorted((t, timer_id) for timer_id, t in self._timers.items() if t[0] <= self._now)
        for _, timer_id in due:
            del self._timers[timer_id]
        return [TimerFired(timer_id, t[2]) for t, timer_id in due]
