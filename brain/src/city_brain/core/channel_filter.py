"""First-order lag filter for the non-water channels (spec 8.2).

Traffic, power, air quality and emergency calls are driven towards a target rather than
integrated like a tank, so the model is `x = [level, drift]`: a level and the rate it is
moving at. The filter also keeps a rolling baseline of the measurements it accepted, which
is what the anomaly detectors compare against (spec 8.3.1).

Only weighted measurements count: an untrusted or unjudged reading advances the prediction
but never the estimate, the ring, the baseline or the freshness clock.
"""

import math
from collections import deque
from collections.abc import Collection, Mapping

import numpy as np

RING_SIZE = 600  # accepted measurements kept for the observed slope
MIN_SLOPE_POINTS = 10
MIN_BASELINE_POINTS = 10
GATED_RUN_FOR_RESET = 5  # consecutive gated innovations that mean the model is wrong
UNGATED_RUN_TO_CLEAR = 50  # ungated updates after a reset before MODEL_MISMATCH clears
RESET_SLOPE_WINDOW = 30  # ticks of measurements used to re-initialise the drift
DROPOUT_SIGMA = 3.0  # how far above the floor the estimate must be for a floor reading to be a dropout
INITIAL_P = (1.0, 1.0e-2)


def lag_params(params: Mapping, kind) -> dict:
    """The lag-filter parameters for a sensor kind: the defaults with the kind's overrides on top."""
    block = params["lag_filter"]
    by_kind = block.get("by_kind") or {}
    return {**block["defaults"], **by_kind.get(str(kind), {})}


class LagFilter:
    """State x = [level, drift] for one non-water channel."""

    def __init__(self, params: Mapping[str, float], min_val: float = 0.0, max_val: float = 0.0):
        self._period = float(params["sample_period_s"])
        self._r0 = float(params["r0"])
        self._q = np.diag([float(params["q_level"]), float(params["q_drift"])])
        self._jump_sigma = float(params["jump_sigma"])
        self._slew_limit = float(params["slew_limit"])
        self._stuck_ticks = int(params["stuck_ticks"])
        self._stale_ticks = float(params["stale_s"]) / self._period
        self._baseline_ticks = float(params["baseline_window_s"]) / self._period
        self._baseline_z_max = float(params["baseline_z_max"])
        self._baseline_shift_ticks = float(params["baseline_shift_s"]) / self._period
        self._min_val = float(min_val)
        self._max_val = float(max_val)
        self._x = np.zeros(2)
        self._p = np.diag(INITIAL_P)
        self._initialised = False
        self._last_tick = 0
        self._last_weighted_tick = 0
        self._flags: set[str] = set()
        self._ring: deque[tuple[int, float]] = deque(maxlen=RING_SIZE)
        self._baseline: deque[tuple[int, float]] = deque()
        # Running sums: the detectors ask for the mean and spread on every tick, so neither
        # may walk the window.
        self._baseline_sum = 0.0
        self._baseline_sumsq = 0.0
        self._pending: list[tuple[int, float]] = []
        self._value: float | None = None  # last weighted measurement
        self._same_run = 0
        self._outlier_run = 0
        self._gated_run = 0
        self._ungated_run = 0

    # ----- read-only state -----

    @property
    def initialised(self) -> bool:
        return self._initialised

    @property
    def level(self) -> float:
        """The filtered level."""
        return float(self._x[0]) if self._initialised else float("nan")

    @property
    def value(self) -> float:
        """The last measurement that carried weight: what the anomaly detectors compare."""
        return float("nan") if self._value is None else self._value

    @property
    def drift(self) -> float:
        """Rate of change, in units per second."""
        return float(self._x[1]) if self._initialised else float("nan")

    @property
    def var_level(self) -> float:
        return float(self._p[0, 0])

    @property
    def period_s(self) -> float:
        return self._period

    @property
    def last_tick(self) -> int:
        return self._last_tick

    @property
    def flags(self) -> frozenset[str]:
        return frozenset(self._flags)

    @property
    def baseline_count(self) -> int:
        return len(self._baseline)

    @property
    def baseline_mean(self) -> float:
        if not self._baseline:
            return float("nan")
        return self._baseline_sum / len(self._baseline)

    @property
    def baseline_std(self) -> float:
        if not self._baseline:
            return float("nan")
        n = len(self._baseline)
        variance = self._baseline_sumsq / n - (self._baseline_sum / n) ** 2
        return math.sqrt(max(0.0, variance))

    def zscore(self, value: float | None = None) -> float | None:
        """How far a value sits from the rolling baseline, or None when there is no spread yet."""
        value = self.value if value is None else value
        if len(self._baseline) < MIN_BASELINE_POINTS or math.isnan(value):
            return None
        std = self.baseline_std
        if not std > 0.0:
            return None
        return (value - self.baseline_mean) / std

    def is_stale(self, global_tick: int) -> bool:
        if not self._initialised:
            return True
        return global_tick - self._last_weighted_tick >= self._stale_ticks

    def observed_slope(
        self,
        since_tick: int,
        exclude_ticks: Collection[int] = (),
        until_tick: int | None = None,
    ) -> float | None:
        """Least-squares slope (units/s) over the accepted measurements in the tick window."""
        pts = [
            (t, v) for t, v in self._ring
            if t >= since_tick and t not in exclude_ticks and (until_tick is None or t <= until_tick)
        ]
        if len(pts) < MIN_SLOPE_POINTS:
            return None
        t = np.array([p[0] for p in pts], dtype=float)
        v = np.array([p[1] for p in pts], dtype=float)
        t -= t.mean()
        denom = float(t @ t)
        if denom == 0.0:
            return None
        return float(t @ (v - v.mean()) / denom / self._period)

    # ----- update -----

    def update(self, value: float, tick: int, weight: float, at_ceiling: bool = False) -> None:
        if not self._initialised:
            if weight <= 0.0:
                return
            self._initialise(value, tick)
            self._set(at_ceiling, "CEILING")
            self._accept(value, tick, at_ceiling)
            return
        if tick < self._last_tick:
            raise ValueError(f"tick {tick} is before last tick {self._last_tick}")
        n = tick - self._last_tick
        dt = self._period * n
        f = np.array([[1.0, dt], [0.0, 1.0]])
        self._x = f @ self._x
        self._p = f @ self._p @ f.T + self._q * n
        self._last_tick = tick
        if weight <= 0.0:
            return
        gap = tick - self._last_weighted_tick
        prior_level = float(self._x[0])
        self._last_weighted_tick = tick
        self._set(at_ceiling, "CEILING")
        self._set(gap >= self._stale_ticks, "STALE")
        self._set(self._is_dropout(value, prior_level), "DROPOUT")
        s = self._p[0, 0] + self._r0 / weight**2
        innovation = value - prior_level
        if self._gated(innovation, s, value):
            self._flags.add("IMPLAUSIBLE_JUMP")
            self._pending.append((tick, value))
            self._ungated_run = 0
            self._gated_run += 1
            if self._gated_run >= GATED_RUN_FOR_RESET:
                self._reinitialise(value, tick)
            return
        self._accept(value, tick, at_ceiling)
        gain = self._p[:, 0] / s
        self._x = self._x + gain * innovation
        self._p = self._p - np.outer(gain, self._p[0, :])
        self._p = (self._p + self._p.T) / 2.0
        self._flags.discard("IMPLAUSIBLE_JUMP")
        self._gated_run = 0
        self._pending.clear()
        if "MODEL_MISMATCH" in self._flags:
            self._ungated_run += 1
            if self._ungated_run >= UNGATED_RUN_TO_CLEAR:
                self._flags.discard("MODEL_MISMATCH")

    # ----- helpers -----

    def _set(self, present: bool, flag: str) -> None:
        self._flags.add(flag) if present else self._flags.discard(flag)

    def _gated(self, innovation: float, s: float, value: float) -> bool:
        if abs(innovation) > self._jump_sigma * float(np.sqrt(s)):
            return True
        return self._value is not None and abs(value - self._value) > self._slew_limit

    def _is_dropout(self, value: float, prior_level: float) -> bool:
        """A reading at the channel's floor while the estimate was well above it (spec 8.2)."""
        # The measurement noise is the floor on a plausible spread: a channel that has sat
        # on one value has a baseline spread near zero, which would make any step a dropout.
        spread = self.baseline_std
        sigma = math.sqrt(self._r0) if math.isnan(spread) else max(spread, math.sqrt(self._r0))
        return value <= self._min_val and prior_level > self._min_val + DROPOUT_SIGMA * sigma

    def _initialise(self, value: float, tick: int) -> None:
        self._x = np.array([value, 0.0])
        self._p = np.diag(INITIAL_P)
        self._initialised = True
        self._last_tick = tick
        self._last_weighted_tick = tick

    def _accept(self, value: float, tick: int, at_ceiling: bool) -> None:
        """Take the measurement into the record: only accepted readings move `value`."""
        self._ring.append((tick, value))
        self._admit_baseline(value, tick)
        if at_ceiling:  # a sensor clamped at its top is flat by design, not stuck
            self._same_run, self._value = 0, value
            self._flags.discard("STUCK")
        else:
            self._track_stuck(value)

    def _admit_baseline(self, value: float, tick: int) -> None:
        """Keep the baseline on what normal looks like.

        A measurement far outside the baseline is what the detectors are there to notice, so
        it is held out: letting it in would wash the anomaly away within a window. A shift
        that lasts `baseline_shift_s` is a new normal instead, and the baseline is re-seeded
        from the accepted record.
        """
        z = self.zscore(value)
        if z is not None and abs(z) > self._baseline_z_max:
            self._outlier_run += 1
            if self._outlier_run < self._baseline_shift_ticks:
                return
            self._outlier_run = 0
            self._reseed_baseline(tick)
            return
        self._outlier_run = 0
        self._add_baseline(tick, value)
        self._trim_baseline(tick)

    def _add_baseline(self, tick: int, value: float) -> None:
        self._baseline.append((tick, value))
        self._baseline_sum += value
        self._baseline_sumsq += value * value

    def _reseed_baseline(self, tick: int) -> None:
        self._baseline.clear()
        self._baseline_sum = self._baseline_sumsq = 0.0
        for t, v in self._ring:
            if tick - t < self._baseline_ticks:
                self._add_baseline(t, v)
        self._trim_baseline(tick)

    def _trim_baseline(self, tick: int) -> None:
        while self._baseline and tick - self._baseline[0][0] >= self._baseline_ticks:
            _, value = self._baseline.popleft()
            self._baseline_sum -= value
            self._baseline_sumsq -= value * value

    def _track_stuck(self, value: float) -> None:
        if self._value is not None and value == self._value:
            self._same_run += 1
        else:
            self._same_run = 1
            self._flags.discard("STUCK")
        self._value = value
        if self._same_run >= self._stuck_ticks:
            self._flags.add("STUCK")

    def _reinitialise(self, value: float, tick: int) -> None:
        """Sustained disagreement: trust the measurements over the model."""
        # The ring takes the disagreeing measurements, so the slope follows the new regime.
        # The baseline does not: what normal looked like is exactly what the detectors compare.
        pending = sorted(self._pending)
        self._ring.extend(pending)
        self._pending.clear()
        # Only the new regime's own measurements say how fast it is moving: a slope taken
        # across the step would read the step itself as a drift and run away from the level.
        slope = self.observed_slope(since_tick=pending[0][0]) if pending else None
        drift = 0.0 if slope is None else slope
        self._x = np.array([value, drift])
        self._p = np.diag(INITIAL_P)
        # The new regime is now what the channel reads: without this every later measurement
        # would still be a slew away from the old one, and the channel would never recover.
        self._value = value
        self._same_run = 1
        self._gated_run = 0
        self._ungated_run = 0
        self._flags.discard("IMPLAUSIBLE_JUMP")
        self._flags.add("MODEL_MISMATCH")
