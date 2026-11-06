"""Two-state Kalman filter for a water channel (spec 8.2)."""

from collections import deque
from collections.abc import Collection, Mapping

import numpy as np

RING_SIZE = 600  # accepted measurements kept for the observed slope
MIN_SLOPE_POINTS = 10
GATED_RUN_FOR_RESET = 5  # consecutive gated innovations that mean the model is wrong
UNGATED_RUN_TO_CLEAR = 50  # ungated updates after a reset before MODEL_MISMATCH clears
RESET_SLOPE_WINDOW = 30  # ticks of measurements used to re-initialise the inflow
STUCK_RATE_MIN = 0.005  # m/s: the model must predict at least this much change
INITIAL_P = (1.0e-2, 1.0e-4)


class WaterFilter:
    """State x = [level, inflow]; the net rate is `inflow - b * u`."""

    def __init__(self, params: Mapping[str, float]):
        self._period = float(params["sample_period_s"])
        self._r0 = float(params["r0"])
        self._q = np.diag([float(params["q_level"]), float(params["q_inflow"])])
        self._b = float(params["b_prior"])
        self._jump_sigma = float(params["jump_sigma"])
        self._stuck_ticks = int(params["stuck_ticks"])
        self._stale_ticks = float(params["stale_s"]) / self._period
        self._x = np.zeros(2)
        self._p = np.diag(INITIAL_P)
        self._u = 0.0
        self._initialised = False
        self._last_tick = 0
        self._last_weighted_tick = 0
        self._flags: set[str] = set()
        self._ring: deque[tuple[int, float]] = deque(maxlen=RING_SIZE)
        # Gated readings wait here: a spike is dropped, a sustained disagreement joins the ring on reset.
        self._pending: list[tuple[int, float]] = []
        self._last_value: float | None = None
        self._same_run = 0
        self._gated_run = 0
        self._ungated_run = 0

    # ----- read-only state -----

    @property
    def initialised(self) -> bool:
        return self._initialised

    @property
    def level(self) -> float:
        return float(self._x[0]) if self._initialised else float("nan")

    @property
    def inflow(self) -> float:
        return float(self._x[1])

    @property
    def rate(self) -> float:
        return float(self._x[1] - self._b * self._u)

    @property
    def var_level(self) -> float:
        return float(self._p[0, 0])

    @property
    def var_rate(self) -> float:
        return float(self._p[1, 1])

    @property
    def last_tick(self) -> int:
        return self._last_tick

    @property
    def flags(self) -> frozenset[str]:
        return frozenset(self._flags)

    def is_stale(self, global_tick: int) -> bool:
        if not self._initialised:
            return True
        return global_tick - self._last_weighted_tick >= self._stale_ticks

    def observed_slope(self, since_tick: int, exclude_ticks: Collection[int] = ()) -> float | None:
        """Least-squares slope (m/s) over the measurements since `since_tick`, less `exclude_ticks`."""
        pts = [(t, v) for t, v in self._ring if t >= since_tick and t not in exclude_ticks]
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

    def update(self, value: float, tick: int, weight: float, u: float, at_ceiling: bool = False) -> None:
        if not self._initialised:
            if weight <= 0.0:
                return
            self._initialise(value, tick, u)
            self._observe(value, tick, prior_rate=0.0, at_ceiling=at_ceiling)
            return
        if tick < self._last_tick:
            raise ValueError(f"tick {tick} is before last tick {self._last_tick}")
        n = tick - self._last_tick
        dt = self._period * n
        f = np.array([[1.0, dt], [0.0, 1.0]])
        self._x = np.array([self._x[0] + (self._x[1] - self._b * u) * dt, self._x[1]])
        self._p = f @ self._p @ f.T + self._q * n
        self._u = u
        self._last_tick = tick
        if weight <= 0.0:
            return
        prior_rate = float(self._x[1] - self._b * u)
        self._last_weighted_tick = tick
        self._flags.discard("CEILING")
        if at_ceiling:
            self._flags.add("CEILING")
        if not at_ceiling:  # a sensor clamped at its top is flat by design, not stuck
            self._track_stuck(value, prior_rate)
        s = self._p[0, 0] + self._r0 / weight**2
        innovation = value - self._x[0]
        if abs(innovation) > self._jump_sigma * float(np.sqrt(s)):
            self._flags.add("IMPLAUSIBLE_JUMP")
            self._pending.append((tick, value))
            self._ungated_run = 0
            self._gated_run += 1
            if self._gated_run >= GATED_RUN_FOR_RESET:
                self._reinitialise(value, tick, u)
            return
        self._pending.clear()
        self._ring.append((tick, value))
        gain = self._p[:, 0] / s
        self._x = self._x + gain * innovation
        self._p = self._p - np.outer(gain, self._p[0, :])
        self._p = (self._p + self._p.T) / 2.0
        self._flags.discard("IMPLAUSIBLE_JUMP")
        self._gated_run = 0
        if "MODEL_MISMATCH" in self._flags:
            self._ungated_run += 1
            if self._ungated_run >= UNGATED_RUN_TO_CLEAR:
                self._flags.discard("MODEL_MISMATCH")

    def _initialise(self, value: float, tick: int, u: float) -> None:
        self._x = np.array([value, 0.0])
        self._p = np.diag(INITIAL_P)
        self._u = u
        self._initialised = True
        self._last_tick = tick
        self._last_weighted_tick = tick

    def _observe(self, value: float, tick: int, prior_rate: float, at_ceiling: bool) -> None:
        self._ring.append((tick, value))
        if at_ceiling:
            self._flags.add("CEILING")
        else:
            self._track_stuck(value, prior_rate)

    def _track_stuck(self, value: float, prior_rate: float) -> None:
        if value == self._last_value:
            self._same_run += 1
        else:
            self._same_run = 1
            self._flags.discard("STUCK")
        self._last_value = value
        if self._same_run >= self._stuck_ticks and abs(prior_rate) > STUCK_RATE_MIN:
            self._flags.add("STUCK")

    def _reinitialise(self, value: float, tick: int, u: float) -> None:
        """Sustained disagreement: trust the measurements over the model."""
        self._ring.extend(sorted(self._pending))
        self._pending.clear()
        slope = self.observed_slope(since_tick=tick - RESET_SLOPE_WINDOW)
        inflow = float(self._x[1]) if slope is None else slope + self._b * u
        self._x = np.array([value, inflow])
        self._p = np.diag(INITIAL_P)
        self._gated_run = 0
        self._ungated_run = 0
        self._flags.discard("IMPLAUSIBLE_JUMP")
        self._flags.add("MODEL_MISMATCH")
