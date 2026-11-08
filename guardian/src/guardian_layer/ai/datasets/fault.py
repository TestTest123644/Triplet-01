"""One function per fault kind: a handler takes a FaultContext, returns a faulty copy.

Each handler keeps the research notebook's random draw order (the sign is drawn
before the handler runs), so a seeded run gives the notebook's injected values.
"""

from collections.abc import Callable

import numpy as np

from guardian_layer.utils.schema import FaultContext, FloatArray

FaultHandler = Callable[[FaultContext], FloatArray]


def apply_stuck(ctx: FaultContext) -> FloatArray:
    """Frozen from a random point to the end."""
    out = ctx.values.copy()
    start = int(ctx.rng.integers(0, len(out) - 1))
    out[start:] = out[start]
    return out


def apply_spike(ctx: FaultContext) -> FloatArray:
    """A few readings jump several spreads."""
    s, rng, out = ctx.settings, ctx.rng, ctx.values.copy()
    count = int(rng.integers(1, min(s.spike_count_max, len(out)) + 1))
    picks = rng.choice(len(out), size=count, replace=False)
    out[picks] += ctx.sign * rng.uniform(*s.spike_spreads) * ctx.spread
    return out


def apply_drift(ctx: FaultContext) -> FloatArray:
    """Offset growing to its full size at the window end."""
    out = ctx.values.copy()
    out += (
        ctx.sign
        * np.linspace(0.0, 1.0, len(out))
        * ctx.rng.uniform(*ctx.settings.drift_spreads)
        * ctx.spread
    )
    return out


def apply_noise(ctx: FaultContext) -> FloatArray:
    """Extra jitter."""
    out = ctx.values.copy()
    out += ctx.rng.normal(
        0.0, ctx.rng.uniform(*ctx.settings.noise_spreads) * ctx.spread, len(out)
    )
    return out


def apply_oscillation(ctx: FaultContext) -> FloatArray:
    """A sine added on top of the readings."""
    s, rng, out = ctx.settings, ctx.rng, ctx.values.copy()
    period = rng.uniform(*s.period_readings)
    phase = rng.uniform(0, 2 * np.pi)
    out += (
        rng.uniform(*s.oscillation_spreads)
        * ctx.spread
        * np.sin(2 * np.pi * np.arange(len(out)) / period + phase)
    )
    return out


def apply_spoof(ctx: FaultContext) -> FloatArray:
    """Whole window shifted; neighbours unchanged."""
    out = ctx.values.copy()
    out += ctx.sign * ctx.rng.uniform(*ctx.settings.spoof_spreads) * ctx.spread
    return out


def apply_sine(ctx: FaultContext) -> FloatArray:
    """The real readings replaced by a noise-free sine."""
    s, rng = ctx.settings, ctx.rng
    period = rng.uniform(*s.period_readings)
    phase = rng.uniform(0, 2 * np.pi)
    return ctx.values.mean() + rng.uniform(*s.sine_spreads) * ctx.spread * np.sin(
        2 * np.pi * np.arange(len(ctx.values)) / period + phase
    )


def apply_ramp(ctx: FaultContext) -> FloatArray:
    """Replaced by a perfect straight line."""
    return (
        ctx.values.mean()
        + ctx.sign
        * np.linspace(-0.5, 0.5, len(ctx.values))
        * ctx.rng.uniform(*ctx.settings.ramp_spreads)
        * ctx.spread
    )


def apply_pattern(ctx: FaultContext) -> FloatArray:
    """Replaced by a short motif repeated to the window end."""
    s, rng = ctx.settings, ctx.rng
    length = int(rng.integers(s.pattern_values[0], s.pattern_values[1] + 1))
    motif = (
        ctx.values.mean()
        + rng.uniform(-s.pattern_spreads, s.pattern_spreads, length) * ctx.spread
    )
    return np.resize(motif, len(ctx.values))


def apply_uniform(ctx: FaultContext) -> FloatArray:
    """Replaced by random values over the type's whole range."""
    return ctx.rng.uniform(*ctx.value_range, len(ctx.values))


FAULT_REGISTRY: dict[str, FaultHandler] = {
    "stuck": apply_stuck,
    "spike": apply_spike,
    "drift": apply_drift,
    "noise": apply_noise,
    "oscillation": apply_oscillation,
    "spoof": apply_spoof,
    "sine": apply_sine,
    "ramp": apply_ramp,
    "pattern": apply_pattern,
    "uniform": apply_uniform,
}
