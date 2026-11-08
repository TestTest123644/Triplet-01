import itertools

import numpy as np
import pytest

from guardian_layer.pipelines.denoise import (
    CLEAN_REGISTRY,
    NOISE_ESTIMATORS,
    PASS_OUTPUTS,
    cleaning_setup,
    repeat_runs,
)
from guardian_layer.utils.config import (
    CLEAN_METHODS,
    KALMAN_MODES,
    KALMAN_NOISES,
    KalmanSettings,
)
from guardian_layer.utils.schema import CleanContext

MIN_SAMPLES = 4
SETUPS = [
    KalmanSettings(mode=mode, noise=noise, jump_sigmas=jump)
    for mode, noise, jump in itertools.product(KALMAN_MODES, KALMAN_NOISES, (None, 3.0))
]
NOISY = 0.5 + 0.01 * np.random.default_rng(0).standard_normal(200)

# section 10's kalman_filter (research/research.ipynb) on
# 50 + 0.01 * default_rng(0).standard_normal(20), printed once before it was replaced
TESTED_OUTPUT = [
    50.00125730221093,
    49.99990673748321,
    50.002402840303674,
    50.00196119074683,
    49.99977427055891,
    50.00086962957515,
    50.00425352207896,
    50.00568479884446,
    50.00221960833119,
    49.998183805261775,
    49.99698787381723,
    49.99791440822388,
    49.99219286080119,
    49.993711461488765,
    49.99204413177578,
    49.99221521028468,
    49.99284800369513,
    49.993925685850094,
    49.99667878644616,
    50.00039247731572,
]


def clean(values, settings=None, method="kalman"):
    return CLEAN_REGISTRY[method](
        CleanContext(values, settings or KalmanSettings(), MIN_SAMPLES)
    )


def test_registry_names_match_the_settings():
    assert tuple(CLEAN_REGISTRY) == CLEAN_METHODS
    assert tuple(NOISE_ESTIMATORS) == KALMAN_NOISES
    assert tuple(PASS_OUTPUTS) == KALMAN_MODES


@pytest.mark.parametrize("settings", SETUPS)
def test_every_setup_keeps_length_and_is_finite(settings):
    out = clean(NOISY, settings)
    assert len(out) == len(NOISY) and np.isfinite(out).all()


@pytest.mark.parametrize("settings", SETUPS)
def test_noisy_series_gets_smoother(settings):
    assert np.diff(clean(NOISY, settings)).std() < np.diff(NOISY).std()


@pytest.mark.parametrize("settings", SETUPS)
@pytest.mark.parametrize(
    "values",
    [
        np.array([1.0]),
        np.array([1.0, 2.0]),
        np.array([1.0, 3.0, 2.0]),
        np.full(20, 0.3),
    ],
)
def test_short_and_flat_series_are_unchanged(settings, values):
    out = clean(values, settings)
    assert np.array_equal(out, values) and out is not values


def test_fixed_noise_of_zero_leaves_the_series_unchanged():
    values = np.array([1.0, 1, 1, 1, 2, 2, 2, 2])  # median |step| is 0
    assert np.array_equal(clean(values), values)


def test_none_returns_an_equal_copy():
    out = clean(NOISY, method="none")
    assert np.array_equal(out, NOISY) and out is not NOISY


def test_tested_setup_reproduces_the_section_10_filter():
    values = 50 + 0.01 * np.random.default_rng(0).standard_normal(20)
    tested = KalmanSettings(keep_repeats=False)  # section 10 tested the plain filter
    assert np.allclose(clean(values, tested), TESTED_OUTPUT, rtol=0, atol=1e-12)


def test_smoother_lags_less_than_the_filter():
    step = np.repeat([0.0, 1.0], 10)
    values = step + 0.05 * np.random.default_rng(1).standard_normal(20)
    filtered = clean(values, KalmanSettings(mode="filter"))
    smoothed = clean(values, KalmanSettings(mode="smoother"))
    # where each first crosses the step's middle: the filter only after the step
    # (it lags), the smoother at it (it spreads the step evenly on both sides)
    filter_cross, smoother_cross = np.argmax(filtered > 0.5), np.argmax(smoothed > 0.5)
    assert smoother_cross <= 10 < filter_cross


@pytest.mark.parametrize("mode", KALMAN_MODES)
def test_jumps_pass_through_and_are_not_smeared(mode):
    values = 0.5 + 0.01 * np.random.default_rng(2).standard_normal(20)
    values[10] += 0.2  # a 20-sigma spike
    passed = clean(values, KalmanSettings(mode=mode, jump_sigmas=3.0))
    assert passed[10] == values[10]
    assert abs(passed[9] - 0.5) < 0.05 and abs(passed[11] - 0.5) < 0.05
    smoothed = clean(values, KalmanSettings(mode=mode))
    assert smoothed[10] < values[10]


@pytest.mark.parametrize(
    "values",
    [
        np.linspace(0.0, 1.0, 20),
        np.repeat([0.0, 1.0], 10),
        np.cumsum(np.random.default_rng(3).standard_normal(50)),
    ],
)
@pytest.mark.parametrize("mode", KALMAN_MODES)
def test_estimated_noise_handles_degenerate_windows(values, mode):
    out = clean(values, KalmanSettings(mode=mode, noise="estimated"))
    assert len(out) == len(values) and np.isfinite(out).all()


@pytest.mark.parametrize("settings", [*SETUPS, KalmanSettings(process_ratio=10.0)])
@pytest.mark.parametrize("scale", [1e154, 1e160])
def test_huge_finite_readings_neither_raise_nor_turn_non_finite(settings, scale):
    # finite, so they pass the readings check, but their variance (1e160) or its
    # process noise (1e154 at ratio 10) overflows a float
    values = scale * (1 + np.random.default_rng(0).standard_normal(50))
    out = clean(values, settings)
    assert len(out) == len(values) and np.isfinite(out).all()


def test_estimated_noise_with_more_process_scale_smooths_less():
    gentle = clean(NOISY, KalmanSettings(noise="estimated", process_scale=100.0))
    strong = clean(NOISY, KalmanSettings(noise="estimated", process_scale=0.01))
    assert np.diff(gentle).std() > np.diff(strong).std()


def test_cleaning_setup_names_the_kalman_settings_only_when_used():
    assert cleaning_setup("none", KalmanSettings()) == {"clean_method": "none"}
    setup = cleaning_setup("kalman", KalmanSettings(mode="smoother"))
    assert setup["clean_method"] == "kalman"
    assert setup["kalman"]["mode"] == "smoother"


def test_repeat_runs_marks_every_member():
    marks = repeat_runs(np.array([1.0, 2.0, 2.0, 3.0, 4.0, 4.0, 4.0, 5.0]))
    assert marks.tolist() == [False, True, True, False, True, True, True, False]
    ends = repeat_runs(np.array([1.0, 1.0, 2.0, 3.0, 3.0]))
    assert ends.tolist() == [True, True, False, True, True]
    assert not repeat_runs(np.array([1.0, 2.0, 3.0])).any()


def test_repeat_runs_tolerates_the_last_bit():
    assert repeat_runs(np.array([0.3, 0.1 + 0.2, 0.3])).all()  # 0.1 + 0.2 != 0.3


def stuck_series():
    values = 0.5 + 0.01 * np.random.default_rng(1).standard_normal(120)
    values[40:80] = values[40]
    return values


@pytest.mark.parametrize("settings", SETUPS)
def test_a_stuck_run_stays_raw(settings):
    values = stuck_series()
    out = clean(values, settings)
    assert np.array_equal(out[40:80], values[40:80])
    assert not np.array_equal(out[:40], values[:40])  # the rest is still cleaned


def test_repeats_are_smoothed_when_switched_off():
    values = stuck_series()
    out = clean(values, KalmanSettings(keep_repeats=False))
    assert not np.array_equal(out[40:80], values[40:80])
