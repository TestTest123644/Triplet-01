import asyncio

from city_brain.core.events import TimerFired
from city_brain.runtime.clocks import RealClock, SimClock


def test_sim_clock_fires_when_due_and_only_once():
    clock = SimClock()
    clock.start("t", "verdict_wait", 2.0)
    assert clock.advance(1.5) == []
    assert clock.advance(0.5) == [TimerFired("t", "verdict_wait")]
    assert clock.advance(10.0) == []


def test_sim_clock_cancel():
    clock = SimClock()
    clock.start("t", "k", 1.0)
    clock.cancel("t")
    clock.cancel("never-started")
    assert clock.advance(5.0) == []


def test_sim_clock_orders_by_due_time_then_start_order():
    clock = SimClock()
    clock.start("late", "k", 2.0)
    clock.start("b", "k", 1.0)
    clock.start("a", "k", 1.0)
    assert [t.timer_id for t in clock.advance(3.0)] == ["b", "a", "late"]


def test_sim_clock_restarting_a_timer_replaces_it():
    clock = SimClock()
    clock.start("t", "k", 1.0)
    clock.start("t", "k", 3.0)
    assert clock.advance(2.0) == []
    assert len(clock.advance(1.0)) == 1


async def test_real_clock_puts_timer_fired_on_the_queue():
    queue: asyncio.Queue = asyncio.Queue()
    RealClock(queue).start("t", "verdict_wait", 0.01)
    assert await asyncio.wait_for(queue.get(), 1.0) == TimerFired("t", "verdict_wait")


async def test_real_clock_cancel():
    queue: asyncio.Queue = asyncio.Queue()
    clock = RealClock(queue)
    clock.start("t", "k", 0.01)
    clock.cancel("t")
    clock.cancel("never-started")
    await asyncio.sleep(0.03)
    assert queue.empty()


async def test_real_clock_restarting_a_timer_replaces_it():
    queue: asyncio.Queue = asyncio.Queue()
    clock = RealClock(queue)
    clock.start("t", "first", 0.01)
    clock.start("t", "second", 0.02)
    assert (await asyncio.wait_for(queue.get(), 1.0)).kind == "second"
    await asyncio.sleep(0.03)
    assert queue.empty()
