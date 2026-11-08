import statistics
import time
from datetime import datetime

import pytest
from message_factory import make_reading, make_unvalidated_reading

from guardian_layer.pipelines.static_check import StaticCheck
from guardian_layer.utils.schema import Anomalies, CheckResult


def median_time(fn, *args, repeats=120):
    timings = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn(*args)
        timings.append(time.perf_counter() - start)
    return statistics.median(timings)


def test_out_of_range_flags_values_outside_sensor_scale():
    check = StaticCheck()
    readings = [
        make_reading(sensor_id="s1", value=95.0),
        make_reading(sensor_id="s1", value=30.0),
    ]

    assert check.out_of_range(readings, (10.0, 80.0)) == Anomalies.OUT_OF_RANGE
    assert check.out_of_range(readings, (0.0, 100.0)) is CheckResult.PASS


def test_sensor_is_down_flags_stale_window():
    check = StaticCheck()
    sensor = [
        make_reading(sensor_id="s1", second=0, value=10.0),
        make_reading(sensor_id="s1", second=1, value=11.0),
    ]

    result = check.sensor_is_down(sensor, datetime(2026, 10, 2, 0, 1, 20))
    assert result == Anomalies.SENSOR_IS_DOWN


def test_spoofed_data_requires_range_and_delta_threshold():
    check = StaticCheck()
    features = {"has_range": 1.0, "neighbor_count": 3, "spatial_delta_mean": 0.9}

    assert check.spoofed_data("s1", features) == Anomalies.SPOOFED_DATA
    assert (
        check.spoofed_data(
            "s1", {"has_range": 0.0, "neighbor_count": 3, "spatial_delta_mean": 5.0}
        )
        == CheckResult.NOT_CHECKED
    )


def test_static_check_skips_when_values_or_ranges_are_missing():
    check = StaticCheck()

    assert (
        check.out_of_range([make_unvalidated_reading(value=float("nan"))], (0.0, 10.0))
        == CheckResult.NOT_CHECKED
    )
    assert (
        check.out_of_range([make_reading(value=5.0)], None) == CheckResult.NOT_CHECKED
    )
    assert (
        check.spoofed_data(
            "s1", {"has_range": 1.0, "neighbor_count": 1, "spatial_delta_mean": 0.2}
        )
        == CheckResult.NOT_CHECKED
    )


@pytest.mark.parametrize("size", [8, 32, 128])
def test_bench_static_check_methods(size):
    check = StaticCheck()
    readings = [
        make_reading(
            sensor_id="s1",
            second=i % 60,
            value=float(i % 10),
            lower_range=0.0,
            upper_range=100.0,
        )
        for i in range(size)
    ]
    features = {
        "has_range": 1.0,
        "neighbor_count": 3,
        "spatial_delta_mean": 0.15,
    }

    timings = {
        "fired": median_time(check.fired, Anomalies.SENSOR_IS_DOWN, "s1", 12.0, 10.0),
        "skipped": median_time(check.skipped, "out_of_range", "s1", "no range"),
        "sensor_of": median_time(check.sensor_of, readings),
        "out_of_range": median_time(check.out_of_range, readings, (0.0, 100.0)),
        "sensor_is_down": median_time(
            check.sensor_is_down, readings, datetime(2026, 10, 2, 0, 0, 20)
        ),
        "feature": median_time(check.feature, features, "spatial_delta_mean"),
        "spoofed_data": median_time(check.spoofed_data, "s1", features),
    }

    assert set(timings) == {
        "fired",
        "skipped",
        "sensor_of",
        "out_of_range",
        "sensor_is_down",
        "feature",
        "spoofed_data",
    }
    assert all(value >= 0.0 for value in timings.values())
