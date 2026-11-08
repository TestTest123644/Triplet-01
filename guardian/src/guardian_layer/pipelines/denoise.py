"""Noise cleaning of one sensor's series before the short-window features (#66).

One function per method and a registry, as ``ai/datasets/fault.py`` does;
``PreProcess.clean`` runs the method the settings name. The Kalman method's two
choices (how the noise is estimated, filter or smoother) are registries of their own,
so no branch names a variant.
"""

import math
from collections.abc import Callable
from typing import Any

import numpy as np

from guardian_layer.utils.config import KalmanSettings
from guardian_layer.utils.schema import BoolArray, CleanContext, FloatArray, KalmanPass

CleanHandler = Callable[[CleanContext], FloatArray]
NoiseEstimator = Callable[[FloatArray, KalmanSettings], tuple[float, float]]
PassOutput = Callable[[KalmanPass], FloatArray]


def filterable(ctx: CleanContext) -> bool:
    """Enough readings to clean, and not flat: a stuck sensor must stay stuck."""
    return len(ctx.values) >= ctx.min_samples and float(np.ptp(ctx.values)) > 0


def no_clean(ctx: CleanContext) -> FloatArray:
    """The series as it is (a copy: callers may change it)."""
    return ctx.values.copy()


def robust_variance(values: FloatArray) -> float:
    """(1.4826 x the median absolute deviation) squared: a variance a few outliers
    (a spike) do not inflate."""
    deviation = 1.4826 * float(np.median(np.abs(values - np.median(values))))
    return deviation * deviation  # not **: a huge value gives inf, not OverflowError


def fixed_noise(values: FloatArray, kalman: KalmanSettings) -> tuple[float, float]:
    """(process, measurement) variance: the measurement noise is the robust std of
    the steps, median(|step|) / (0.6745 x sqrt 2), which ignores slow trends and
    single spikes; the process noise is process_ratio times it."""
    sigma = float(np.median(np.abs(np.diff(values)))) / (0.6745 * math.sqrt(2))
    measurement = sigma * sigma  # not **: a huge value gives inf, not OverflowError
    return kalman.process_ratio * measurement, measurement


def estimated_noise(values: FloatArray, kalman: KalmanSettings) -> tuple[float, float]:
    """(process, measurement) variance, both from the window. In the local-level
    model a one-step change has variance Q + 2R and a two-step change 2Q + 2R, so
    Q = V2 - V1 and R = (2 V1 - V2) / 2 (robust variances). White noise gives Q of
    about 0, which would flatten the window: Q is at least process_floor x R."""
    one = robust_variance(np.diff(values))
    two = robust_variance(values[2:] - values[:-2])
    measurement = (2 * one - two) / 2
    return max(two - one, kalman.process_floor * measurement), measurement


def forward_pass(
    values: FloatArray, process: float, measurement: float, jump_sigmas: float | None
) -> KalmanPass:
    """The Kalman filter, reading by reading: predict (the level may have moved by
    the process noise), then pull the level towards the reading by the gain. A
    reading more than jump_sigmas prediction sds away is taken as it is and restarts
    the filter, so spikes and steps pass through."""
    count = len(values)
    means, variances = np.empty(count), np.empty(count)
    predicted, jumps = np.zeros(count), np.zeros(count, dtype=bool)
    level, variance = float(values[0]), measurement
    means[0], variances[0] = level, variance
    for index in range(1, count):
        prior = variance + process
        total = prior + measurement
        innovation = float(values[index]) - level
        predicted[index] = prior
        if jump_sigmas is not None and abs(innovation) > jump_sigmas * math.sqrt(total):
            level, variance = float(values[index]), measurement
            jumps[index] = True
        else:
            gain = prior / total
            level += gain * innovation
            variance = (1 - gain) * prior
        means[index], variances[index] = level, variance
    return KalmanPass(means, variances, predicted, jumps)


def filtered(forward: KalmanPass) -> FloatArray:
    """The forward pass alone: each point sees only the readings before it."""
    return forward.means.copy()


def backward_pass(forward: KalmanPass) -> FloatArray:
    """Rauch-Tung-Striebel smoothing: each point also uses the readings after it, so
    the cleaned series does not lag behind a change. A jump keeps its reading and the
    smoothing does not cross it, so a spike is not smeared into its neighbours."""
    smoothed = forward.means.copy()
    for index in range(len(smoothed) - 2, -1, -1):
        if forward.jumps[index] or forward.jumps[index + 1]:
            continue
        gain = forward.variances[index] / forward.predicted[index + 1]
        smoothed[index] = forward.means[index] + gain * (
            smoothed[index + 1] - forward.means[index]
        )
    return smoothed


# KalmanSettings.noise -> estimator, KalmanSettings.mode -> output (names: config.py)
NOISE_ESTIMATORS: dict[str, NoiseEstimator] = {
    "fixed": fixed_noise,
    "estimated": estimated_noise,
}
PASS_OUTPUTS: dict[str, PassOutput] = {"filter": filtered, "smoother": backward_pass}


def repeat_runs(values: FloatArray) -> BoolArray:
    """Every reading in a run of two or more consecutive equal readings. Equal within
    1e-9 relative, as ``LongPreProcess.flat_run_share`` counts a stuck grid: bucket
    means of one repeated value can differ in the last bit."""
    same = np.isclose(values[1:], values[:-1], rtol=1e-9, atol=0.0)
    marks = np.zeros(len(values), dtype=np.bool_)
    marks[1:] |= same
    marks[:-1] |= same
    return marks


def kalman(ctx: CleanContext) -> FloatArray:
    """A local-level Kalman filter or smoother on one sensor's series: the true value
    moves as a random walk (process noise Q per reading) and each reading adds
    measurement noise R. Both come from the window itself, so no per-type value is
    needed and sensor types never seen get the same treatment. Short, flat or
    noise-free series come back unchanged, and so does every run of repeated
    readings (``keep_repeats``): a stuck value stays exactly flat."""
    cfg = ctx.kalman
    if not filterable(ctx):
        return ctx.values.copy()
    process, measurement = NOISE_ESTIMATORS[cfg.noise](ctx.values, cfg)
    process *= cfg.process_scale
    # no measurement noise (or it underflowed), or readings too large to square
    if not (math.isfinite(process) and math.isfinite(measurement) and measurement > 0):
        return ctx.values.copy()
    forward = forward_pass(ctx.values, process, measurement, cfg.jump_sigmas)
    out = PASS_OUTPUTS[cfg.mode](forward)
    if cfg.keep_repeats:
        repeats = repeat_runs(ctx.values)
        out[repeats] = ctx.values[repeats]
    return out


# PreprocessSettings.clean_method -> cleaning (names: config.CLEAN_METHODS)
CLEAN_REGISTRY: dict[str, CleanHandler] = {"none": no_clean, "kalman": kalman}


def cleaning_setup(clean_method: str, kalman: KalmanSettings) -> dict[str, Any]:
    """What a saved model records about its cleaning: the method, and the Kalman
    settings only when they are used."""
    if clean_method == "kalman":
        return {"clean_method": clean_method, "kalman": kalman.model_dump()}
    return {"clean_method": clean_method}
