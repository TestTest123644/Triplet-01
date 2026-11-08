import logging
from datetime import UTC, datetime

import pytest
from message_factory import make_reading, make_unvalidated_reading

from guardian_layer.pipelines.static_check import StaticCheck
from guardian_layer.utils.config import StaticCheckSettings
from guardian_layer.utils.schema import Anomalies, CheckResult

WINDOW_END = datetime(2026, 10, 2, 0, 0, 20, tzinfo=UTC)
RANGE = (0.0, 100.0)
NAN, INF = float("nan"), float("inf")
SENSOR = "water_level-01"


@pytest.fixture
def check():
    return StaticCheck(StaticCheckSettings(down_after_seconds=10.0))


def feature_row(delta: float, neighbors: float = 5.0, has_range: float = 1.0):
    """The three features spoofed_data reads, as PreProcess writes them."""
    return {
        "has_range": has_range,
        "neighbor_count": neighbors,
        "spatial_delta_mean": delta,
    }


# --- out_of_range ---------------------------------------------------------------


@pytest.mark.parametrize("value", [100.01, -0.01])
def test_out_of_range_flags_a_value_past_either_bound(check, value):
    readings = [make_reading(value=50.0, second=0), make_reading(value=value, second=1)]
    assert check.out_of_range(readings, RANGE) is Anomalies.OUT_OF_RANGE


def test_out_of_range_accepts_values_on_the_bounds(check):
    readings = [make_reading(value=0.0, second=0), make_reading(value=100.0, second=1)]
    assert check.out_of_range(readings, RANGE) is CheckResult.PASS


def test_out_of_range_is_skipped_without_a_range(check):
    result = check.out_of_range([make_reading(value=1e9)], None)
    assert result is CheckResult.NOT_CHECKED


@pytest.mark.parametrize("readings", [[], [make_unvalidated_reading(value=NAN)]])
def test_out_of_range_is_not_checked_with_nothing_to_judge(check, readings):
    assert check.out_of_range(readings, RANGE) is CheckResult.NOT_CHECKED


def test_out_of_range_ignores_non_finite_values(check):
    readings = [
        make_unvalidated_reading(value=INF),
        make_reading(value=50.0, second=1),
    ]
    assert check.out_of_range(readings, RANGE) is CheckResult.PASS


@pytest.mark.parametrize("value_range", [(0.0, INF), (NAN, 100.0)])
def test_out_of_range_is_not_checked_with_a_non_finite_bound(check, value_range):
    result = check.out_of_range([make_reading(value=1e9)], value_range)
    assert result is CheckResult.NOT_CHECKED


# --- sensor_is_down -------------------------------------------------------------


@pytest.mark.parametrize(
    "readings",
    [
        [],
        [make_unvalidated_reading(value=NAN, second=19)],
        [make_unvalidated_reading(value=INF, second=19)],
    ],
)
def test_sensor_is_down_without_a_finite_reading(check, readings):
    assert check.sensor_is_down(readings, WINDOW_END) is Anomalies.SENSOR_IS_DOWN


def test_sensor_is_down_when_the_newest_reading_is_too_old(check):
    readings = [make_reading(second=3), make_reading(second=9)]  # 11 s of silence
    assert check.sensor_is_down(readings, WINDOW_END) is Anomalies.SENSOR_IS_DOWN


def test_sensor_is_up_at_exactly_the_allowed_silence(check):
    assert (
        check.sensor_is_down([make_reading(second=10)], WINDOW_END) is CheckResult.PASS
    )


def test_sensor_is_up_with_a_recent_reading(check):
    readings = [make_reading(second=2), make_reading(second=18)]
    assert check.sensor_is_down(readings, WINDOW_END) is CheckResult.PASS


def test_sensor_is_down_when_only_a_nan_is_recent(check):
    readings = [make_reading(second=3), make_unvalidated_reading(value=NAN, second=19)]
    assert check.sensor_is_down(readings, WINDOW_END) is Anomalies.SENSOR_IS_DOWN


def test_sensor_is_down_reads_a_naive_window_end_as_utc(check):
    naive_end = WINDOW_END.replace(tzinfo=None)
    assert (
        check.sensor_is_down([make_reading(second=18)], naive_end) is CheckResult.PASS
    )
    assert (
        check.sensor_is_down([make_reading(second=3)], naive_end)
        is Anomalies.SENSOR_IS_DOWN
    )


# --- spoofed_data ---------------------------------------------------------------


@pytest.mark.parametrize("delta", [0.3, -0.3])
def test_spoofed_data_flags_a_mean_far_from_the_neighbours(check, delta):
    assert check.spoofed_data(SENSOR, feature_row(delta)) is Anomalies.SPOOFED_DATA


def test_spoofed_data_accepts_a_small_delta(check):
    assert (
        check.spoofed_data(SENSOR, feature_row(0.2)) is CheckResult.PASS
    )  # the threshold itself is fine


def test_spoofed_data_is_skipped_without_a_range(check):
    result = check.spoofed_data(SENSOR, feature_row(0.9, has_range=0.0))
    assert result is CheckResult.NOT_CHECKED


def test_spoofed_data_is_skipped_with_too_few_neighbours(check):
    result = check.spoofed_data(SENSOR, feature_row(0.9, neighbors=2.0))
    assert result is CheckResult.NOT_CHECKED
    assert (
        check.spoofed_data(SENSOR, feature_row(0.9, neighbors=3.0))
        is Anomalies.SPOOFED_DATA
    )


@pytest.mark.parametrize(
    "features",
    [
        {},
        feature_row(NAN),
        feature_row(INF),
        {"has_range": 1.0, "neighbor_count": 5.0},  # no delta at all
        {**feature_row(0.9), "spatial_delta_mean": "far"},  # not a number
    ],
)
def test_spoofed_data_is_not_checked_on_a_bad_row(check, features):
    # "could not check", never "no anomaly": a bad row must not look clean
    assert check.spoofed_data(SENSOR, features) is CheckResult.NOT_CHECKED


def test_thresholds_come_from_the_settings():
    strict = StaticCheck(StaticCheckSettings(spoof_delta=0.05, down_after_seconds=1.0))
    assert strict.spoofed_data(SENSOR, feature_row(0.1)) is Anomalies.SPOOFED_DATA
    assert (
        strict.sensor_is_down([make_reading(second=18)], WINDOW_END)
        is Anomalies.SENSOR_IS_DOWN
    )


# --- future timestamps ----------------------------------------------------------


def test_a_future_reading_does_not_hide_an_outage(check):
    # last real reading 17 s before the end, then one dated 30 s after it
    readings = [make_reading(second=3), make_reading(second=50)]
    assert check.sensor_is_down(readings, WINDOW_END) is Anomalies.SENSOR_IS_DOWN


def test_only_future_readings_mean_the_sensor_is_down(check):
    readings = [make_reading(second=50)]
    assert check.sensor_is_down(readings, WINDOW_END) is Anomalies.SENSOR_IS_DOWN


def test_small_clock_skew_is_tolerated(check):
    readings = [make_reading(second=21)]  # 1 s past the end, tolerance is 2 s
    assert check.sensor_is_down(readings, WINDOW_END) is CheckResult.PASS


# --- logging --------------------------------------------------------------------


@pytest.fixture
def check_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger("guardian_layer.pipelines.static_check")
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger="guardian_layer.pipelines.static_check")
    yield caplog
    module_logger.removeHandler(caplog.handler)


def only_record(caplog, level):
    # by identity: when the logger also propagates, root's caplog handler sees it too
    unique = {id(r): r for r in caplog.records}.values()
    records = [r for r in unique if r.levelno == level]
    assert len(records) == 1
    return records[0]


def test_a_fired_rule_logs_rule_sensor_value_and_threshold(check, check_log):
    check.out_of_range([make_reading(value=150.0)], RANGE)
    record = only_record(check_log, logging.WARNING)
    assert (record.rule, record.sensor_id, record.value, record.threshold) == (
        "out_of_range",
        SENSOR,
        150.0,
        RANGE,
    )


def test_a_down_sensor_logs_its_silence(check, check_log):
    check.sensor_is_down([make_reading(second=3)], WINDOW_END)
    record = only_record(check_log, logging.WARNING)
    assert (record.rule, record.sensor_id, record.value, record.threshold) == (
        "sensor_is_down",
        SENSOR,
        17.0,
        10.0,
    )


def test_a_spoof_logs_its_delta(check, check_log):
    check.spoofed_data(SENSOR, feature_row(0.5))
    record = only_record(check_log, logging.WARNING)
    assert (record.rule, record.sensor_id, record.value, record.threshold) == (
        "spoofed_data",
        SENSOR,
        0.5,
        0.2,
    )


def test_a_skipped_check_logs_its_reason(check, check_log):
    check.spoofed_data(SENSOR, feature_row(0.9, has_range=0.0))
    record = only_record(check_log, logging.DEBUG)
    assert (record.rule, record.sensor_id, record.reason) == (
        "spoofed_data",
        SENSOR,
        "no range",
    )


def test_ignored_future_readings_are_logged(check, check_log):
    check.sensor_is_down([make_reading(second=19), make_reading(second=50)], WINDOW_END)
    record = only_record(check_log, logging.WARNING)
    assert (record.sensor_id, record.ignored) == (SENSOR, 1)


def test_a_clean_check_logs_nothing(check, check_log):
    check.out_of_range([make_reading(value=50.0)], RANGE)
    assert check_log.records == []
