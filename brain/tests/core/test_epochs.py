from city_brain.core.epochs import is_new_epoch
from city_brain.core.state import DeviceCursor
from tests.factories import T0, make_observation


def test_no_cursor_is_not_new_epoch():
    assert not is_new_epoch(None, make_observation(seq=1), 100, 60.0)


def test_seq_drop_beyond_threshold_is_new_epoch():
    cur = DeviceCursor(seq=5000, sim_time=T0)
    assert is_new_epoch(cur, make_observation(seq=3), 100, 60.0)


def test_seq_drop_within_threshold_is_not_new_epoch():
    cur = DeviceCursor(seq=500, sim_time=T0)
    assert not is_new_epoch(cur, make_observation(seq=450), 100, 60.0)
    assert not is_new_epoch(cur, make_observation(seq=400), 100, 60.0)


def test_ts_back_beyond_threshold_is_new_epoch():
    from datetime import timedelta

    cur = DeviceCursor(seq=500, sim_time=T0)
    assert is_new_epoch(cur, make_observation(seq=499, sim_time=T0 - timedelta(seconds=61)), 100, 60.0)
    assert not is_new_epoch(cur, make_observation(seq=499, sim_time=T0 - timedelta(seconds=60)), 100, 60.0)
