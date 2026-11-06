from datetime import timedelta

from city_brain.core.events import Publish, ReadingReceived, ReadingRejected, RegistryLoaded
from city_brain.core.state import BrainState
from city_brain.core.step import step
from tests.factories import T0, make_observation

POLICY = {"epochs": {"seq_drop": 100, "ts_back_s": 60}}
PARAMS = {"filter": {"sample_period_s": 0.1}}


def fresh_state():
    return BrainState.new(POLICY, PARAMS)


def test_duplicate_reading_counted_not_processed():
    s = fresh_state()
    r = make_observation(seq=5)
    s, _ = step(s, ReadingReceived(r))
    s, fx = step(s, ReadingReceived(r))
    assert s.counters["DUPLICATE"] == 1 and fx == []
    assert s.counters["OUT_OF_ORDER"] == 0


def test_twin_restart_starts_new_epoch():
    s = fresh_state()
    s, _ = step(s, ReadingReceived(make_observation(seq=5000, sim_time=T0 + timedelta(seconds=500))))
    s, fx = step(
        s,
        ReadingReceived(make_observation(seq=3, sim_time=T0, reading_id="r-z1-wl1-3", sig_hash="new")),
    )
    assert s.epoch == 1 and s.counters["DUPLICATE"] == 0
    assert any(
        e.event.get("data", {}).get("reason") == "epoch_changed" for e in fx if isinstance(e, Publish)
    )
    assert s.cursors["z1-wl1"].seq == 3
    assert s.global_tick == 3
    assert s.epoch_origin == (T0, 3)


def test_restart_replaying_same_reading_key_is_not_duplicate():
    s = fresh_state()
    first = make_observation(seq=3)
    s, _ = step(s, ReadingReceived(first))
    s, _ = step(s, ReadingReceived(make_observation(seq=5000, reading_id="r-5000", sig_hash="x")))
    s, _ = step(s, ReadingReceived(first))
    assert s.epoch == 1
    assert s.counters["DUPLICATE"] == 0 and s.counters["OUT_OF_ORDER"] == 0
    assert s.cursors["z1-wl1"].seq == 3


def test_small_reorder_is_out_of_order_not_new_epoch():
    s = fresh_state()
    s, _ = step(s, ReadingReceived(make_observation(seq=500, reading_id="a", sig_hash="a")))
    s, fx = step(s, ReadingReceived(make_observation(seq=450, reading_id="b", sig_hash="b")))
    assert s.epoch == 0 and s.counters["OUT_OF_ORDER"] == 1 and fx == []
    assert s.cursors["z1-wl1"].seq == 500


def test_accepted_reading_updates_cursor_tick_and_sim_now():
    s = fresh_state()
    s, _ = step(s, ReadingReceived(make_observation(seq=10, reading_id="a", sig_hash="a")))
    s, _ = step(s, ReadingReceived(make_observation(seq=15, reading_id="b", sig_hash="b", sim_time=T0)))
    assert s.cursors["z1-wl1"].seq == 15 and s.global_tick == 15
    assert s.epoch_origin == (T0, 10)
    assert s.sim_now == T0 + timedelta(seconds=0.5)
    assert ("b", "b") in s.seen


def test_registry_loaded_stores_registry_and_emits_startup():
    from city_brain.domain.types import (
        CanonicalUnit, DeviceProfile, DeviceStatus, SensorKind,
    )

    dev = DeviceProfile(
        device_id="d1", zone_id="z", kind=SensorKind.WATER_LEVEL, unit=CanonicalUnit.M,
        min_val=0.0, max_val=10.0, status=DeviceStatus.OK, actuator=None,
    )
    s, fx = step(fresh_state(), RegistryLoaded((dev,)))
    assert s.registry == {"d1": dev}
    assert [e.event["data"]["reason"] for e in fx if isinstance(e, Publish)] == ["startup"]


def test_reading_rejected_counted_by_code():
    s, fx = step(fresh_state(), ReadingRejected("BAD_SIGNATURE", "d1"))
    s, _ = step(s, ReadingRejected("BAD_SIGNATURE", None))
    assert s.counters["BAD_SIGNATURE"] == 2 and fx == []
