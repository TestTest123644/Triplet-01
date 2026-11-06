from datetime import timedelta

from city_brain.config import load_config
from city_brain.core.events import Publish, ReadingReceived, ReadingRejected, RegistryLoaded
from city_brain.core.state import BrainState
from city_brain.core.step import step
from tests.factories import T0, make_observation

POLICY = load_config("config/standalone.yaml").policy
PARAMS = load_config("config/standalone.yaml").params


def fresh_state():
    return BrainState.new(POLICY, PARAMS)


def test_duplicate_reading_counted_not_processed():
    s = fresh_state()
    r = make_observation(seq=5)
    s, _ = step(s, ReadingReceived(r))
    s, fx = step(s, ReadingReceived(r))
    assert s.counters["DUPLICATE"] == 1 and fx == []
    assert s.counters["OUT_OF_ORDER"] == 0


def old_reading(device="z1-wl1", seq=3, sim_time=T0):
    """Going backwards, and not byte-identical to anything seen."""
    return ReadingReceived(make_observation(
        device_id=device, seq=seq, sim_time=sim_time, reading_id=f"old-{device}", sig_hash=f"old-{device}"))


def far_along(devices=("z1-wl1",)):
    s = fresh_state()
    for device in devices:
        s, _ = step(s, ReadingReceived(make_observation(
            device_id=device, seq=5000, sim_time=T0 + timedelta(seconds=500),
            reading_id=f"r-{device}", sig_hash=f"s-{device}")))
    return s


def test_key_change_starts_the_epoch_at_once():
    s = far_along()
    s, fx = step(s, RegistryLoaded((), keys_changed=True))
    assert s.epoch == 1
    assert [e.event["data"]["reason"] for e in fx if isinstance(e, Publish)] == ["startup", "epoch_changed"]
    s, _ = step(s, ReadingReceived(make_observation(seq=3, sim_time=T0, reading_id="r-z1-wl1-3", sig_hash="new")))
    assert s.cursors["z1-wl1"].seq == 3
    assert s.global_tick == 3
    assert s.epoch_origin == (T0, 3)


def test_restart_replaying_same_reading_key_is_not_duplicate():
    s = fresh_state()
    first = make_observation(seq=3)
    s, _ = step(s, ReadingReceived(first))
    s, _ = step(s, ReadingReceived(make_observation(seq=5000, reading_id="r-5000", sig_hash="x")))
    s, _ = step(s, RegistryLoaded((), keys_changed=True))
    s, _ = step(s, ReadingReceived(first))
    assert s.epoch == 1
    assert s.counters["DUPLICATE"] == 0 and s.counters["OUT_OF_ORDER"] == 0
    assert s.cursors["z1-wl1"].seq == 3


def test_one_device_going_backwards_is_a_candidate_and_changes_nothing():
    s = far_along()
    filters, seen, tick = dict(s.filters), set(s.seen), s.global_tick
    s.suspect.add("z1-wl1")
    s, fx = step(s, old_reading())
    assert fx == [] and s.epoch == 0 and s.counters["EPOCH_CANDIDATE"] == 1
    assert s.filters == filters and s.seen == seen and s.global_tick == tick
    assert s.suspect == {"z1-wl1"} and s.cursors["z1-wl1"].seq == 5000


def test_byte_identical_old_reading_is_only_a_duplicate():
    s = fresh_state()
    old = make_observation(seq=3)
    s, _ = step(s, ReadingReceived(old))
    s, _ = step(s, ReadingReceived(make_observation(seq=5000, reading_id="r-5000", sig_hash="x")))
    s.suspect.add("z1-wl1")
    filter_before, seen = s.filters["z1-wl1"], set(s.seen)
    s, fx = step(s, ReadingReceived(old))
    assert fx == [] and s.epoch == 0
    assert s.counters["DUPLICATE"] == 1 and s.counters["EPOCH_CANDIDATE"] == 0
    assert s.seen == seen and s.filters["z1-wl1"] is filter_before and s.suspect == {"z1-wl1"}


LATER = T0 + timedelta(seconds=500)
DEVICES = tuple(f"z{n}-wl1" for n in range(1, 7))
CONFIRM = POLICY["epochs"]["confirm_devices"]


def test_enough_distinct_devices_confirm_the_epoch_and_the_last_reading_opens_it():
    s = far_along(DEVICES)
    for device in DEVICES[:CONFIRM - 1]:
        s, _ = step(s, old_reading(device))
        assert s.epoch == 0
    s, fx = step(s, old_reading(DEVICES[CONFIRM - 1]))
    assert s.epoch == 1 and s.counters["EPOCH_CANDIDATE"] == CONFIRM
    assert any(isinstance(e, Publish) and e.event["data"].get("reason") == "epoch_changed" for e in fx)
    assert s.cursors == {DEVICES[CONFIRM - 1]: s.cursors[DEVICES[CONFIRM - 1]]}
    assert s.epoch_origin == (T0, 3) and s.epoch_candidates == set()


def test_one_device_repeating_is_still_one_candidate():
    s = far_along()
    for n in range(CONFIRM + 2):
        s, _ = step(s, ReadingReceived(make_observation(seq=3, reading_id=f"o{n}", sig_hash=f"o{n}")))
    assert s.epoch == 0 and s.counters["EPOCH_CANDIDATE"] == CONFIRM + 2


def test_candidates_spread_past_the_window_do_not_confirm():
    s = far_along(DEVICES)
    window = POLICY["epochs"]["confirm_ticks"]
    for n, device in enumerate(DEVICES[:CONFIRM - 1]):
        s, _ = step(s, old_reading(device))
    s, _ = step(s, ReadingReceived(make_observation(
        device_id=DEVICES[-1], seq=5001 + window, sim_time=T0 + timedelta(seconds=503),
        reading_id="fresh", sig_hash="fresh")))
    s, _ = step(s, old_reading(DEVICES[CONFIRM - 1]))
    assert s.epoch == 0 and s.epoch_candidates == {DEVICES[CONFIRM - 1]}


def test_a_big_seq_jump_is_dropped():
    s = far_along()
    jump = POLICY["epochs"]["seq_jump_max"]
    s, _ = step(s, ReadingReceived(make_observation(seq=5000 + jump, reading_id="ok", sig_hash="ok", sim_time=LATER)))
    assert s.cursors["z1-wl1"].seq == 5000 + jump
    limit = s.cursors["z1-wl1"].seq
    s, fx = step(s, ReadingReceived(make_observation(seq=limit + jump + 1, reading_id="j", sig_hash="j", sim_time=LATER)))
    assert fx == [] and s.counters["SEQ_JUMP"] == 1 and s.cursors["z1-wl1"].seq == limit
    assert ("j", "j") not in s.seen


def test_first_reading_of_a_device_is_checked_against_the_global_tick():
    s = far_along()
    jump = POLICY["epochs"]["seq_jump_max"]
    s, _ = step(s, ReadingReceived(make_observation(
        device_id="z2-wl1", seq=5000 + jump + 1, reading_id="j", sig_hash="j")))
    assert s.counters["SEQ_JUMP"] == 1 and "z2-wl1" not in s.cursors


def test_very_first_reading_sets_the_origin_whatever_its_seq():
    s, _ = step(fresh_state(), ReadingReceived(make_observation(seq=2**53)))
    assert s.counters["SEQ_JUMP"] == 0 and s.global_tick == 2**53
    s, _ = step(s, ReadingReceived(make_observation(seq=2**53 + 1, reading_id="n", sig_hash="n")))
    assert s.sim_now == T0 + timedelta(seconds=0.1)


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


def test_a_second_registry_load_is_reported_as_a_reload():
    from city_brain.domain.types import (
        CanonicalUnit, DeviceProfile, DeviceStatus, SensorKind,
    )

    dev = DeviceProfile(
        device_id="d1", zone_id="z", kind=SensorKind.WATER_LEVEL, unit=CanonicalUnit.M,
        min_val=0.0, max_val=10.0, status=DeviceStatus.OK, actuator=None,
    )
    s, _ = step(fresh_state(), RegistryLoaded((dev,)))
    _, fx = step(s, RegistryLoaded((dev,)))
    assert [e.event["data"]["reason"] for e in fx if isinstance(e, Publish)] == ["registry_reloaded"]
