import logging

import numpy as np
import pytest

from guardian_layer.ai.datasets.injection import FaultInjector
from guardian_layer.utils.dataset_config import InjectionSettings

RANGE = (-100.0, 100.0)


@pytest.fixture
def injection_log(caplog):
    # the module logger gets caplog's handler directly, and must not also reach it
    # through the root, or every record is captured twice
    name = "guardian_layer.ai.datasets.injection"
    module_logger = logging.getLogger(name)
    propagate = module_logger.propagate
    module_logger.propagate = False
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger=name)
    yield caplog
    module_logger.removeHandler(caplog.handler)
    module_logger.propagate = propagate


def records(log, level, message):
    return [r for r in log.records if r.levelno == level and r.getMessage() == message]


@pytest.fixture
def injector():
    return FaultInjector()


@pytest.fixture(params=[10, 200])  # 200: a 20 s window of 100 ms readings
def values(request):
    # 0..9 whatever the length: inside RANGE, which fabricated kinds are clipped to
    return np.linspace(0.0, 9.0, request.param)


def fault(injector, values, kind, seed=0, value_range=RANGE):
    return injector.apply_fault(
        values, kind, 1.0, np.random.default_rng(seed), value_range
    )


@pytest.mark.parametrize("seed", range(5))
def test_stuck_freezes_from_one_reading_to_the_end(injector, values, seed):
    out = fault(injector, values, "stuck", seed)
    n = len(values)
    assert any(
        np.array_equal(out[:s], values[:s]) and np.all(out[s:] == values[s])
        for s in range(n - 1)
    )


@pytest.mark.parametrize("seed", range(5))
def test_spike_moves_one_to_three_readings_by_five_to_ten_spreads(
    injector, values, seed
):
    delta = fault(injector, values, "spike", seed) - values
    moved = delta[delta != 0]
    assert 1 <= len(moved) <= 3
    assert len(set(np.sign(moved))) == 1
    assert np.all((np.abs(moved) >= 5) & (np.abs(moved) <= 10))


@pytest.mark.parametrize("seed", range(5))
def test_drift_grows_from_zero_to_three_to_six_spreads(injector, values, seed):
    delta = fault(injector, values, "drift", seed) - values
    assert delta[0] == 0
    assert np.all(np.diff(np.abs(delta)) >= 0)
    assert 3 <= abs(delta[-1]) <= 6


def test_noise_changes_values_and_keeps_the_length(injector, values):
    out = fault(injector, values, "noise")
    assert len(out) == len(values)
    assert not np.array_equal(out, values)


@pytest.mark.parametrize("seed", range(5))
def test_oscillation_stays_within_four_spreads(injector, values, seed):
    delta = fault(injector, values, "oscillation", seed) - values
    assert np.abs(delta).max() <= 4
    assert not np.allclose(delta, delta[0])


@pytest.mark.parametrize("seed", range(5))
def test_spoof_shifts_the_whole_window_by_three_to_six_spreads(injector, values, seed):
    delta = fault(injector, values, "spoof", seed) - values
    assert np.allclose(delta, delta[0])
    assert 3 <= abs(delta[0]) <= 6


def test_ramp_is_a_straight_line(injector, values):
    out = fault(injector, values, "ramp")
    assert np.allclose(np.diff(out, 2), 0)


@pytest.mark.parametrize("seed", range(5))
def test_pattern_repeats_two_to_four_values(injector, values, seed):
    out = fault(injector, values, "pattern", seed)
    assert any(np.allclose(out[p:], out[:-p]) for p in (2, 3, 4))


@pytest.mark.parametrize("kind", ["sine", "uniform", "ramp", "pattern"])
def test_sine_and_uniform_stay_in_range(injector, values, kind):
    out = fault(injector, values, kind, value_range=(0.0, 5.0))
    assert out.min() >= 0.0
    assert out.max() <= 5.0


def test_unknown_kind_raises(injector, values):
    with pytest.raises(ValueError, match="bogus"):
        fault(injector, values, "bogus")


def run_inject(injector, values, targets, kinds, seed=0):
    return injector.inject(
        values,
        targets,
        ["t"] * len(targets),
        {"t": 1.0},
        {"t": RANGE},
        np.random.default_rng(seed),
        kinds,
    )


def test_inject_faults_the_configured_share():
    injector = FaultInjector(InjectionSettings(inject_share=0.1))
    values = np.arange(500.0)
    original = values.copy()
    targets = [np.arange(i * 10, i * 10 + 10) for i in range(50)]
    faulty, kinds = run_inject(injector, values, targets, ("spoof",))
    assert sum(kind != "" for kind in kinds) == 5
    for target, kind in zip(targets, kinds, strict=True):
        if kind == "":
            assert np.array_equal(faulty[target], values[target])
    assert np.array_equal(values, original)


def test_a_fault_that_changes_nothing_is_not_counted():
    injector = FaultInjector(InjectionSettings(inject_share=1.0))
    values = np.full(10, 3.0)
    _, kinds = run_inject(injector, values, [np.arange(10)], ("stuck",))
    assert kinds == [""]


def test_same_seed_same_injection():
    injector = FaultInjector(InjectionSettings(inject_share=0.5))
    values = np.random.default_rng(9).normal(size=200)
    targets = [np.arange(i * 10, i * 10 + 10) for i in range(20)]
    kinds = injector.settings.fault_kinds
    a = run_inject(injector, values, targets, kinds, seed=3)
    b = run_inject(injector, values, targets, kinds, seed=3)
    c = run_inject(injector, values, targets, kinds, seed=4)
    assert np.array_equal(a[0], b[0])
    assert a[1] == b[1]
    assert not np.array_equal(a[0], c[0])


def test_spreads_skip_short_and_flat_windows(injector):
    short_and_flat = [[1.0, 2.0, 3.0, 4.0], [5.0, 5.0, 5.0, 5.0], [1.0, 9.0]]
    windows = {"t": [np.array(w) for w in short_and_flat]}
    assert injector.within_window_spreads(windows, min_samples=4)["t"] == pytest.approx(
        np.std([1.0, 2.0, 3.0, 4.0])
    )
    flat = {"t": [np.array([5.0, 5.0, 5.0, 5.0])]}
    assert injector.within_window_spreads(flat, min_samples=4) == {"t": 0.0}


# These pin the notebook's draw order (what is drawn, in which order), which the Intel
# Lab parity relies on: a refactor that reorders a draw changes these values.
GOLDEN = {
    "stuck": [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 7.0, 7.0, 7.0, 7.0],
    "spike": [
        0.0,
        1.0,
        2.0,
        8.0826381776,
        4.0,
        10.0826381776,
        6.0,
        7.0,
        8.0,
        9.0,
        10.0,
        11.0,
    ],  # fmt: skip
    "drift": [
        0.0,
        1.3463054674,
        2.6926109348,
        4.0389164022,
        5.3852218696,
        6.731527337,
        8.0778328043,
        9.4241382717,
        10.7704437391,
        12.1167492065,
        13.4630546739,
        14.8093601413,
    ],  # fmt: skip
    "noise": [
        0.985977695,
        1.1615014329,
        1.1752976671,
        3.5567021381,
        6.007603819,
        6.4581006846,
        4.9165479309,
        5.0517907286,
        7.0404231994,
        9.0636243797,
        6.4204444012,
        10.663154168,
    ],  # fmt: skip
    "oscillation": [
        0.517635913,
        3.0271407243,
        2.8104654621,
        1.5038437712,
        2.2093129277,
        5.3229698118,
        8.0022839516,
        7.9888464019,
        6.6455685879,
        7.1237844089,
        10.1252101367,
        12.9582482555,
    ],  # fmt: skip
    "spoof": [
        3.8093601413,
        4.8093601413,
        5.8093601413,
        6.8093601413,
        7.8093601413,
        8.8093601413,
        9.8093601413,
        10.8093601413,
        11.8093601413,
        12.8093601413,
        13.8093601413,
        14.8093601413,
    ],  # fmt: skip
    "sine": [
        6.017635913,
        7.5271407243,
        6.3104654621,
        4.0038437712,
        3.7093129277,
        5.8229698118,
        7.5022839516,
        6.4888464019,
        4.1455685879,
        3.6237844089,
        5.6252101367,
        7.4582482555,
    ],  # fmt: skip
    "ramp": [
        3.5953199294,
        3.9416253967,
        4.2879308641,
        4.6342363315,
        4.9805417989,
        5.3268472663,
        5.6731527337,
        6.0194582011,
        6.3657636685,
        6.7120691359,
        7.0583746033,
        7.4046800706,
    ],  # fmt: skip
    "pattern": [
        4.1187202826,
        2.7458411436,
        2.5991658132,
        4.1187202826,
        2.7458411436,
        2.5991658132,
        4.1187202826,
        2.7458411436,
        2.5991658132,
        4.1187202826,
        2.7458411436,
        2.5991658132,
    ],  # fmt: skip
    "uniform": [
        -46.0426572472,
        -91.8052952128,
        -96.6944728943,
        62.6540478401,
        82.5511154555,
        21.3271551534,
        45.8993121968,
        8.7249982931,
        87.0144847576,
        63.1707108243,
        -99.452299966,
        71.4808553175,
    ],  # fmt: skip
}


@pytest.mark.parametrize("kind", GOLDEN)
def test_each_fault_gives_the_golden_values(injector, kind):
    out = injector.apply_fault(
        np.arange(12.0), kind, 1.0, np.random.default_rng(0), RANGE
    )
    np.testing.assert_allclose(out, GOLDEN[kind], rtol=0, atol=1e-9)


def test_inject_gives_the_golden_kinds():
    injector = FaultInjector(InjectionSettings(inject_share=0.25))
    kinds = (*injector.settings.fault_kinds, *injector.settings.fabricated_kinds)
    assert len(kinds) == 10
    targets = [np.arange(i * 4, i * 4 + 4) for i in range(20)]
    _, applied = injector.inject(
        np.arange(80.0),
        targets,
        ["t"] * 20,
        {"t": 1.0},
        {"t": RANGE},
        np.random.default_rng(0),
        kinds,
    )
    assert applied == [
        *[""] * 5, "noise", "stuck", "", "", "drift", "pattern", "", "",
        "oscillation", *[""] * 6,
    ]  # fmt: skip


def test_inject_without_kinds_leaves_everything():
    injector = FaultInjector(InjectionSettings(inject_share=1.0))
    values = np.arange(20.0)
    faulty, applied = run_inject(injector, values, [np.arange(10)], ())
    assert applied == [""]
    assert np.array_equal(faulty, values)


def test_inject_logs_the_counts(injection_log):
    injector = FaultInjector(InjectionSettings(inject_share=0.1))
    targets = [np.arange(i * 10, i * 10 + 10) for i in range(50)]
    _, kinds = run_inject(injector, np.arange(500.0), targets, ("spoof", "ramp"))
    [record] = records(injection_log, logging.INFO, "Injected faults")
    assert record.targets == 50
    assert record.chosen == round(0.1 * 50)
    assert record.chosen == record.faulted + record.unchanged
    assert record.faulted == sum(kind != "" for kind in kinds)
    assert record.per_kind == {k: kinds.count(k) for k in set(kinds) if k}
    assert len(record.per_kind) >= 2
    assert type(record.faulted) is int


def test_a_fault_that_changes_nothing_is_logged_as_unchanged(injection_log):
    injector = FaultInjector(InjectionSettings(inject_share=1.0))
    run_inject(injector, np.full(10, 3.0), [np.arange(10)], ("stuck",))
    [record] = records(injection_log, logging.INFO, "Injected faults")
    assert record.chosen == 1
    assert record.unchanged == 1
    assert record.faulted == 0
    assert record.per_kind == {}


def test_inject_with_no_share_logs_zero_counts(injection_log):
    injector = FaultInjector(InjectionSettings(inject_share=0.0))
    values = np.arange(20.0)
    faulty, _ = run_inject(injector, values, [np.arange(10)], ("spoof",))
    [record] = records(injection_log, logging.INFO, "Injected faults")
    assert record.chosen == record.faulted == record.unchanged == 0
    assert record.per_kind == {}
    assert np.array_equal(faulty, values)


def test_inject_without_kinds_logs_nothing(injection_log):
    injector = FaultInjector(InjectionSettings(inject_share=1.0))
    run_inject(injector, np.arange(20.0), [np.arange(10)], ())
    assert not records(injection_log, logging.INFO, "Injected faults")


def test_flat_types_warn_about_their_spread(injection_log, injector):
    windows = {
        "flat": [np.full(4, 5.0)],
        "usable": [np.array([1.0, 2.0, 3.0, 4.0])],
    }
    injector.within_window_spreads(windows, min_samples=4)
    [warning] = records(
        injection_log,
        logging.WARNING,
        "Types without a spread: their spread-sized faults change nothing",
    )
    assert warning.types == ["flat"]
