from city_brain.core.epochs import is_new_epoch, start_new_epoch
from city_brain.core.state import BrainState, DeviceCursor
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


def test_a_new_epoch_clears_the_non_water_channels_and_the_incidents():
    """A restarted Twin rewinds `seq`, so every per-epoch channel has to start again."""
    from city_brain.core.assessor import IncidentBook
    from city_brain.core.channel_filter import LagFilter, lag_params
    from city_brain.domain.types import Incident, IncidentKind, SensorKind
    from tests.factories import T0
    from tests.helpers_coordinator import CFG

    state = BrainState.new(CFG.policy, CFG.params)
    channel = LagFilter(lag_params(CFG.params, SensorKind.TRAFFIC_FLOW), 0.0, 200.0)
    channel.update(45.0, 1920, 1.0)
    state.lag_filters["tc"] = channel
    state.incidents = IncidentBook(incidents={"i": Incident(
        incident_id="i", zone_id="z", kind=IncidentKind.CONGESTION, belief=0.9, severity="high",
        status="confirmed", findings=(), correlations=(), opened_at=T0, updated_at=T0)})
    state.plan_step["i"] = 1
    state.timers_armed["guardian_hb"] = T0

    start_new_epoch(state)

    assert state.lag_filters == {} and state.plan_step == {}
    assert state.incidents.incidents == {} and state.timers_armed == {}
