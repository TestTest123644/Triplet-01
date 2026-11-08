import statistics
import time

import numpy as np
import pytest

from guardian_layer.pipelines.decompose import Decompose
from guardian_layer.utils.config import get_settings

CONFIG = get_settings().preprocess


def median_time(fn, *args, repeats=100):
    timings = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn(*args)
        timings.append(time.perf_counter() - start)
    return statistics.median(timings)


@pytest.mark.parametrize("method", ["fft", "haar", "db4", "ssa"])
@pytest.mark.parametrize("value", [0.5, 0.1])
def test_constant_signal_has_no_component_energy(method, value):
    features = Decompose().caller(method, np.full(16, value))
    assert all(v == 0.0 for v in features.values())


@pytest.mark.parametrize("method", list(CONFIG.method_keys))
def test_keys_are_fixed_per_method_even_with_too_few_samples(method):
    short = Decompose().caller(method, np.array([0.1, 0.9]))
    assert tuple(short) == CONFIG.method_keys[method]
    assert tuple(
        Decompose().caller(method, np.sin(np.arange(16, dtype=float)))
    ) == tuple(CONFIG.method_keys[method])


@pytest.mark.parametrize("method", list(CONFIG.method_keys))
def test_caller_dispatches_every_method_to_its_keys(method):
    values = np.random.default_rng(0).normal(size=16)
    assert tuple(Decompose().caller(method, values)) == CONFIG.method_keys[method]


def test_caller_rejects_an_unknown_method():
    with pytest.raises(ValueError, match="unknown decoupling method"):
        Decompose().caller("wavelet9000", np.arange(16, dtype=float))


def test_fft_puts_slow_and_fast_signals_in_different_bands():
    seconds = np.arange(16, dtype=float)
    slow = Decompose().caller("fft", np.sin(2 * np.pi * seconds / 16))
    fast = Decompose().caller("fft", np.where(seconds % 2 == 0, 1.0, -1.0))
    assert slow["fft_low"] > 0.9
    assert fast["fft_high"] > 0.9
    assert fast["fft_peak"] == pytest.approx(1.0)


def test_haar_spots_a_single_spike():
    smooth = np.linspace(0.3, 0.5, 16)
    spiky = smooth.copy()
    spiky[9] += 0.5
    assert Decompose().caller("haar", spiky)["haar_d1_max_abs"] > 0.3
    assert Decompose().caller("haar", smooth)["haar_d1_max_abs"] < 0.01


@pytest.mark.parametrize("value", [0.1, 0.7])
def test_fft_ignores_rounding_noise_on_a_constant_signal(value):
    features = Decompose().caller("fft", np.full(7, value))
    assert all(feature == 0.0 for feature in features.values())


def test_db4_spots_a_single_spike():
    smooth = np.linspace(0.3, 0.5, 16)
    spiky = smooth.copy()
    spiky[9] += 0.5
    spike = Decompose().caller("db4", spiky)["db4_d1_max_abs"]
    assert spike > 0.25
    assert Decompose().caller("db4", smooth)["db4_d1_max_abs"] < spike / 4


def test_db4_fills_only_the_levels_a_short_window_has():
    features = Decompose().caller("db4", np.array([0.1, 0.4, 0.2, 0.9, 0.3]))
    assert features["db4_d1_energy"] > 0
    assert features["db4_d3_energy"] == 0.0


def test_ssa_shares_sum_to_one_and_separate_noise_from_smooth_signals():
    smooth = Decompose().caller("ssa", np.linspace(0.3, 0.5, 16))
    noisy = Decompose().caller("ssa", np.random.default_rng(0).normal(size=16))
    for features in (smooth, noisy):
        shares = features["ssa_trend"] + features["ssa_oscillation"]
        assert shares + features["ssa_noise"] == pytest.approx(1.0)
    assert smooth["ssa_noise"] < 0.01
    assert noisy["ssa_noise"] > 0.3


def test_db4_ignores_a_steady_trend():
    features = Decompose().caller("db4", np.linspace(0.0, 1.0, 16))
    assert features["db4_d1_max_abs"] < 0.01
    assert features["db4_d2_energy"] < 0.01


def test_decompose_fft_constant_branch_is_zeroed():
    features = Decompose().fft(np.full(16, 0.8))
    assert all(value == 0.0 for value in features.values())


@pytest.mark.parametrize("size", [32, 128, 512])
def test_bench_decompose_methods(size):
    values = np.linspace(0.0, 1.0, size)
    decompose = Decompose()

    timings = {
        "caller": median_time(decompose.caller, "fft", values),
        "fft": median_time(decompose.fft, values),
        "haar": median_time(decompose.haar, values),
        "db4": median_time(decompose.db4, values),
        "ssa": median_time(decompose.ssa, values),
    }

    assert set(timings) == {"caller", "fft", "haar", "db4", "ssa"}
    assert all(value >= 0.0 for value in timings.values())
