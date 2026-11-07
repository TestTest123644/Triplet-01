"""Preprocessing for #9: dedup, decoupling and features per 20 s window.

One class, ``PreProcess``: every stage is a method (dedup, normalization, features,
spatial deltas); chaining them per window is left to the orchestrator.
Which decoupling method to keep was decided by the research notebook.
"""

import itertools
import math
import statistics
from typing import Any, cast

import numpy as np
import numpy.typing as npt

from guardian_layer.pipelines.decompose import Decompose
from guardian_layer.pipelines.denoise import CLEAN_REGISTRY
from guardian_layer.utils.config import PreprocessSettings, get_settings
from guardian_layer.utils.helpers import epoch_seconds
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import CleanContext, Sensor, SensorSummary

logger = get_logger(__name__)

Array = npt.NDArray[np.float64]
IndexArray = npt.NDArray[np.intp]
Reading = Sensor


class PreProcess:
    """Turn one 20 s window of readings into features per sensor.

    Will implement the Dev team's pre_processing interface once it is merged.
    """

    def __init__(
        self, method: str | None = None, settings: PreprocessSettings | None = None
    ) -> None:
        self.settings = settings or get_settings().preprocess
        if method is None:
            method = self.settings.default_method
        if method not in self.settings.method_keys:
            raise ValueError(f"unknown decoupling method: {method}")
        self.method = method

    def feature_keys(self, method: str | None = None) -> tuple[str, ...]:
        """Every feature key of one sensor-window, in a fixed order.

        Without ``method``, the keys of this instance's own method.
        """
        cfg = self.settings
        if method is None:
            method = self.method
        return (
            *cfg.value_keys,
            *cfg.counter_keys,
            *cfg.method_keys[method],
            *cfg.spatial_keys,
        )

    def short_keys(self) -> tuple[str, ...]:
        """The keys short_features writes, in its order."""
        cfg = self.settings
        return (*cfg.robust_keys, *cfg.shape_keys, *cfg.native_keys)

    def known_keys(self) -> frozenset[str]:
        """Every key a feature row can hold: all methods' keys, the short and the long
        ones."""
        keys = {
            key
            for method in self.settings.method_keys
            for key in self.feature_keys(method)
        }
        return frozenset(keys | set(self.short_keys()) | set(self.settings.long_keys))

    # --- readings: fields of the Sensor readings a Buffer holds ---------------
    # ``metadata`` is optional and may be None, so every metadata read guards it.

    def require_value(self, reading: Reading) -> float:
        """The reading's value; None means the sensor is not working, so it raises."""
        # pydantic rejects None, this guards readings built without validation
        if reading.value is None:
            raise ValueError(
                f"sensor {reading.sensor_id} sent no value: it is not working"
            )
        return reading.value

    def finite(self, window: list[Reading]) -> list[Reading]:
        """The window without NaN or inf values (pydantic accepts both as floats).

        Raises ValueError if a reading has no value (see require_value).
        """
        # precondition guard, not payload validation (that is upstream): one NaN or
        # inf would spread through the means and spatial deltas of the whole cluster
        kept, dropped = [], []
        for reading in window:
            finite = math.isfinite(self.require_value(reading))
            (kept if finite else dropped).append(reading)
        if dropped:
            logger.warning(
                "Dropped non-finite readings",
                dropped=len(dropped),
                sensor_ids=sorted({r.sensor_id for r in dropped}),
            )
        return kept

    def cluster_key(self, reading: Reading) -> str:
        """Group of the sensor: its cluster_id."""
        return reading.cluster_id

    def timestamp_seconds(self, reading: Reading) -> float:
        """Timestamp as seconds since the epoch; no offset means UTC."""
        return epoch_seconds(reading.timestamp)

    # --- deduplication --------------------------------------------------------
    # Drop repeated readings within one window and count packet-level problems.
    # Stateless on purpose: consecutive 20 s windows overlap, so a reading
    # legitimately appears in two windows and must not be dropped the second time.

    def dedup_key(self, reading: Reading) -> tuple[Any, ...]:
        """(sensor_id, packet_order), or the exact content without metadata."""
        self.require_value(reading)
        if reading.metadata is not None:
            return (reading.sensor_id, reading.metadata.packet_order)
        return (reading.sensor_id, reading.timestamp, reading.value)

    def deduplicate(
        self,
        window: list[Reading],
    ) -> tuple[list[Reading], dict[str, dict[str, float]]]:
        """Return the window without repeats, plus packet counters per sensor.

        ``window`` must be in arrival order; the first arrival of each key is kept.
        A packet_order more than max_jump from its sensor's median in the window is
        corrupt: dropped and never used as the reference for later packets.
        """
        # distinct orders: a repeated bogus packet must not outvote the good ones
        by_sensor: dict[str, set[int]] = {}
        for reading in window:
            if reading.metadata is not None:
                by_sensor.setdefault(reading.sensor_id, set()).add(
                    reading.metadata.packet_order
                )
        # median_low: for (10, 99999) it keeps 10; the median would drop both
        medians = {
            sid: statistics.median_low(sorted(o)) for sid, o in by_sensor.items()
        }
        seen: set[tuple[Any, ...]] = set()
        unique: list[Reading] = []
        counters: dict[str, dict[str, float]] = {}
        highest: dict[str, int] = {}  # highest packet_order seen so far, per sensor
        received: dict[str, set[int]] = {}  # in-sequence packet_orders, for gaps

        for reading in window:
            sensor_id = reading.sensor_id
            counts = counters.setdefault(
                sensor_id, dict.fromkeys(self.settings.counter_keys, 0.0)
            )
            key = self.dedup_key(reading)
            if key in seen:
                counts["duplicate_count"] += 1
                continue
            seen.add(key)

            if reading.metadata is None:
                unique.append(reading)
                continue
            order = reading.metadata.packet_order
            # shortcut: a window where most of a sensor's packets are corrupt fools the
            # median; keep state across windows if that shows up
            if abs(order - medians[sensor_id]) > self.settings.max_jump:
                # replay_count = packets rejected for a bad order (replays + corrupt)
                counts["replay_count"] += 1
                continue
            behind = highest.get(sensor_id, order) - order
            if behind > self.settings.out_of_order_tolerance:
                # counted only: its stale value and old timestamp would corrupt the
                # series, and its packet_order would fake a huge gap
                counts["replay_count"] += 1
                continue
            unique.append(reading)
            if behind > 0:
                counts["out_of_order_count"] += 1
            highest[sensor_id] = max(highest.get(sensor_id, order), order)
            received.setdefault(sensor_id, set()).add(order)

        for sensor_id, orders in received.items():
            ordered = sorted(orders)
            gaps = [
                after - before - 1
                for before, after in zip(ordered, ordered[1:], strict=False)
            ]
            counters[sensor_id]["packet_gap_max"] = float(max(gaps, default=0))
        for sensor_id, counts in counters.items():
            if counts["replay_count"]:
                logger.warning(
                    "Dropped packets with a bad packet_order",
                    sensor_id=sensor_id,
                    rejected=int(counts["replay_count"]),
                )
        return unique, counters

    # --- value features: one sensor's readings as a normalized series ---------

    def series_key(self, reading: Reading) -> tuple[float, int]:
        """Sort key: time, then sending order (readings can share a timestamp)."""
        order = reading.metadata.packet_order if reading.metadata is not None else 0
        return self.timestamp_seconds(reading), order

    def sensor_series(self, readings: list[Reading]) -> tuple[Array, Array]:
        """Timestamps (seconds) and raw values of one sensor, sorted by time; ties in
        sending order (the sort is stable: ties without metadata keep arrival order)."""
        ordered = sorted(readings, key=self.series_key)
        times = np.array([self.timestamp_seconds(r) for r in ordered], dtype=np.float64)
        values = np.array([r.value for r in ordered], dtype=np.float64)
        return times, values

    def find_range(
        self, readings: list[Reading], cluster_readings: list[Reading]
    ) -> tuple[float, float] | None:
        """The sensor's own range, else its cluster's in this window, else None.

        A range with an inf or NaN bound is skipped: it would scale every value to 0
        or NaN.
        """
        # if cluster peers report different ranges, the first found (arrival order)
        # wins; open question with the lead
        own = len(readings)
        warned = False
        for index, reading in enumerate(itertools.chain(readings, cluster_readings)):
            if reading.metadata is None:
                continue
            low, high = reading.metadata.lower_range, reading.metadata.upper_range
            if low is None or high is None:
                continue
            if not (math.isfinite(low) and math.isfinite(high)):
                # only the sensor's own readings: a bad peer is logged when it is
                # processed itself, not once per sensor of its cluster
                if index < own and not warned:
                    logger.warning(
                        "Skipped a non-finite range",
                        sensor_id=reading.sensor_id,
                        lower_range=low,
                        upper_range=high,
                    )
                    warned = True
                continue
            if high > low:
                return low, high
        return None

    def normalize(
        self, values: Array, value_range: tuple[float, float] | None
    ) -> Array:
        """Scale to 0-1 inside the range; else z-score within the window."""
        if value_range is not None:
            low, high = value_range
            return (values - low) / (high - low)
        if np.ptp(values) == 0:  # constant (std can be ~1e-17 from rounding, not 0)
            return np.zeros_like(values)
        return (values - values.mean()) / values.std()

    def resample(self, times: Array, values: Array) -> Array:
        """Values on a 1 s grid over the last max_span_seconds, gaps interpolated."""
        if len(values) < 2:
            return values.copy()
        start = max(times[0], times[-1] - self.settings.max_span_seconds)
        grid = np.arange(start, times[-1] + 0.5, 1.0)
        return np.interp(grid, times, values)

    def value_features(
        self, times: Array, values: Array, has_range: bool
    ) -> dict[str, float]:
        """Level, spread, change and flatline features of one normalized series."""
        features = cast(dict[str, float], dict.fromkeys(self.settings.value_keys, 0.0))
        features["sample_count"] = float(len(values))
        features["has_range"] = float(has_range)
        if len(values) == 0:
            return features
        features["mean"] = float(values.mean())
        features["variance"] = float(values.var())
        if len(values) >= self.settings.rolling_samples:
            windows = np.lib.stride_tricks.sliding_window_view(
                values, self.settings.rolling_samples
            )
            features["rolling_variance_max"] = float(windows.var(axis=1).max())
        if len(values) >= 2:
            steps, changes = np.diff(times), np.diff(values)
            moving = steps > 0
            if moving.any():
                rates = np.abs(changes[moving] / steps[moving])
                features["rate_of_change_mean_abs"] = float(rates.mean())
                features["rate_of_change_max_abs"] = float(rates.max())
            features["flatline_ratio"] = float(np.mean(changes == 0))
        return features

    # --- short-window features: the same at any scale and sampling rate --------
    # The short-window model's columns (research/short_window/results.md 4.2): they
    # describe the fault, not the source, so the model holds up on unseen sources.

    def robust_z(self, values: Array) -> Array:
        """(values - median) / (1.4826 x MAD); with a MAD of 0, z-scored by the std
        (zeros when flat), as normalize does without a range."""
        median = np.median(values)
        mad = 1.4826 * float(np.median(np.abs(values - median)))
        if mad > 0:
            return (values - median) / mad
        return self.normalize(values, None)

    def lag_one_correlation(self, steps: Array) -> float:
        """Correlation of each step with the next (0 when either side is flat)."""
        first, second = steps[:-1] - steps[:-1].mean(), steps[1:] - steps[1:].mean()
        scale = float(np.sqrt((first @ first) * (second @ second)))
        return float(first @ second) / scale if scale > 0 else 0.0

    def shape_features(self, values: Array) -> dict[str, float]:
        """Shape of one sensor's raw window, the same at any scale: steps and levels in
        the window's own robust spread, how the steps alternate, how peaked the
        spectrum is. Per reading; zeros when too short or flat."""
        cfg = self.settings
        features = dict.fromkeys(cfg.shape_keys, 0.0)
        if len(values) < cfg.min_samples or np.ptp(values) == 0:
            return features
        scaled, steps = self.robust_z(values), np.diff(values)
        features["max_abs_step"] = float(np.abs(np.diff(scaled)).max())
        features["max_abs_level"] = float(np.abs(scaled).max())
        signs = np.sign(steps[steps != 0])
        if len(signs) > 1:
            features["sign_flip_share"] = float(np.mean(signs[1:] != signs[:-1]))
        if len(steps) > 2:
            features["diff_autocorr"] = self.lag_one_correlation(steps)
        power = np.abs(np.fft.rfft(values - values.mean()))[1:] ** 2  # DC dropped
        if power.sum() > 0:
            features["spectral_peak_share"] = float(power.max() / power.sum())
        if len(steps) >= 4 and steps.std() > 0:
            standard = (steps - steps.mean()) / steps.std()
            features["diff_kurtosis"] = float(np.mean(standard**4) - 3.0)
        if len(values) >= cfg.rolling_samples:
            variances = np.lib.stride_tricks.sliding_window_view(
                values, cfg.rolling_samples
            ).var(axis=1)
            top, middle = float(variances.max()), float(np.median(variances))
            if middle > 0:
                features["rolling_variance_ratio"] = top / middle
            elif top > 0:  # flat most of the time, then a burst
                features["rolling_variance_ratio"] = cfg.ratio_cap
        return features

    def native_grid(self, times: Array, values: Array) -> Array:
        """The values resampled on the sensor's own median step (resample's 1 s grid
        keeps 21 of a 100 ms window's 200 readings), over the last max_span_seconds
        as resample does (an old timestamp must not stretch the grid), and with at most
        native_grid_per_reading points a reading."""
        steps = np.diff(times)
        steps = steps[steps > 0]
        if len(values) < 2 or not len(steps):
            return values.copy()
        start = max(times[0], times[-1] - self.settings.max_span_seconds)
        # the median step, but no finer than native_grid_per_reading points a reading
        most = self.settings.native_grid_per_reading * len(values)  # points
        step = max(float(np.median(steps)), (times[-1] - start) / (most - 1))
        grid = np.arange(start, times[-1] + step / 2, step)
        return np.interp(grid, times, values)

    def native_decoupling(
        self, decompose: Decompose, method: str, times: Array, values: Array
    ) -> dict[str, float]:
        """``method`` on the robust z of the raw ``values`` at the sensor's own rate,
        keys prefixed n_."""
        grid = self.native_grid(times, self.robust_z(values))
        return {
            f"n_{key}": value
            for key, value in self.scale_free_decoupling(
                decompose, method, grid
            ).items()
        }

    def scale_free_decoupling(
        self, decompose: Decompose, method: str, grid: Array
    ) -> dict[str, float]:
        """``method`` on an evenly spaced series, free of its scale (the long-window
        features reuse it on their time grid). haar and db4 give raw energies, which
        carry the window's scale: each level's energy becomes its share of the total,
        the largest level-1 detail a multiple of the grid's spread (fft and ssa are
        shares)."""
        raw = decompose.caller(method, grid)
        if method in ("haar", "db4"):
            spread = float(grid.std())
            total = sum(v for k, v in raw.items() if k.endswith("_energy"))
            for key, value in raw.items():
                if key.endswith("_energy"):
                    raw[key] = value / total if total > 0 else 0.0
                else:
                    raw[key] = value / spread if spread > 0 else 0.0
        return raw

    def clean(self, values: Array) -> Array:
        """One sensor's series with the settings' noise cleaning (#66), a new array."""
        cfg = self.settings
        return CLEAN_REGISTRY[cfg.clean_method](
            CleanContext(values, cfg.kalman, cfg.min_samples)
        )

    def short_features(
        self, decompose: Decompose, times: Array, raw: Array
    ) -> dict[str, float]:
        """The short-window features of one sensor's raw series (keys: short_keys)."""
        cfg = self.settings
        # cleaned for the short-window model only: the section 4 model's live inputs
        # are slow sensors, about one reading a window, which a filter leaves as is
        raw = self.clean(raw)
        robust = self.value_features(times, self.robust_z(raw), False)
        return {
            **{key: robust[key.removeprefix("rz_")] for key in cfg.robust_keys},
            **self.shape_features(raw),
            **self.native_decoupling(decompose, cfg.native_method, times, raw),
        }

    # --- spatial deltas: distance to the same-cluster neighbours --------------

    def location(self, readings: list[Reading]) -> tuple[float | None, float | None]:
        """First valid lat/lon pair found in the sensor's readings.

        A NaN, inf or out-of-range pair (lat beyond ±90, lon beyond ±180) is skipped:
        it would give NaN distances and pick wrong neighbours.
        """
        warned = False
        for reading in readings:
            if reading.metadata is None:
                continue
            lat, lon = reading.metadata.lat, reading.metadata.lon
            if lat is None or lon is None:
                continue
            if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:  # NaN fails both
                return lat, lon
            if not warned:  # once per sensor, not per reading
                logger.warning(
                    "Skipped invalid coordinates",
                    sensor_id=reading.sensor_id,
                    lat=lat,
                    lon=lon,
                )
                warned = True
        return None, None

    def haversine_km(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Great-circle distance between two lat/lon points."""
        lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
        h = (
            math.sin((lat2 - lat1) / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
        )
        h = min(1.0, max(0.0, h))  # rounding can push h past [0, 1]: asin/sqrt raise
        return 2 * self.settings.earth_radius_km * math.asin(math.sqrt(h))

    # shortcut: O(cluster²) distances per window; a KD-tree (scipy cKDTree on unit
    # vectors, tie-aware) if one consumer holds more than ~3,000 sensors per cluster
    def cluster_neighbors(self, members: list[SensorSummary]) -> list[IndexArray]:
        """For each member, in order, the indices into ``members`` of its neighbours.

        The k nearest located members (equal distance: the earlier one in
        ``members``); the whole cluster when it or every peer has no location.
        """
        n = len(members)
        located = np.array([m.lat is not None and m.lon is not None for m in members])
        lat = np.radians([m.lat if m.lat is not None else 0.0 for m in members])
        lon = np.radians([m.lon if m.lon is not None else 0.0 for m in members])
        cand = np.flatnonzero(located)  # only located members search by distance
        nearest: dict[int, IndexArray] = {}
        chunk, k = self.settings.spatial_chunk_rows, self.settings.neighbors
        for start in range(0, len(cand), chunk):
            rows = cand[start : start + chunk]
            # the haversine of haversine_km, one block of rows at a time
            h = (
                np.sin((lat[cand][None, :] - lat[rows][:, None]) / 2) ** 2
                + np.cos(lat[rows])[:, None]
                * np.cos(lat[cand])[None, :]
                * np.sin((lon[cand][None, :] - lon[rows][:, None]) / 2) ** 2
            )
            # clip as in haversine_km: rounding can push h past [0, 1]
            dist = (
                2
                * self.settings.earth_radius_km
                * np.arcsin(np.sqrt(np.clip(h, 0.0, 1.0)))
            )
            dist[cand[None, :] == rows[:, None]] = np.inf  # never your own neighbour
            # stable: equal distances keep input order, as the sorted() it replaces
            order = np.argsort(dist, axis=1, kind="stable")[:, :k]
            for row, index in enumerate(rows):
                picked = order[row][np.isfinite(dist[row, order[row]])]
                if len(picked):
                    nearest[int(index)] = cand[picked]
        everyone = np.arange(n)
        result = []
        for index in range(n):
            if index not in nearest:
                logger.debug(
                    "Neighbours fell back to the whole cluster",
                    sensor_id=members[index].sensor_id,
                )
            result.append(nearest.get(index, np.delete(everyone, index)))
        return result

    def neighbors(
        self, sensor: SensorSummary, sensors: list[SensorSummary]
    ) -> list[SensorSummary]:
        """The k nearest located same-cluster sensors, else the whole cluster.

        For one-off lookups: it searches the whole cluster (and logs every member that
        falls back). ``spatial_features`` does all sensors in one pass.
        """
        peers = [
            peer
            for peer in sensors
            if peer.cluster == sensor.cluster and peer.sensor_id != sensor.sensor_id
        ]
        # self first: every peer index shifts by one, so the tie order is kept
        members = [sensor, *peers]
        return [members[i] for i in self.cluster_neighbors(members)[0]]

    def spatial_features(
        self, sensors: list[SensorSummary]
    ) -> dict[str, dict[str, float]]:
        """Delta of each sensor's mean and last value against its neighbours."""
        clusters: dict[str, list[SensorSummary]] = {}
        for sensor in sensors:  # input order kept inside each cluster: tie order
            clusters.setdefault(sensor.cluster, []).append(sensor)
        result = {}
        for members in clusters.values():
            for sensor, peers in zip(
                members, self.cluster_neighbors(members), strict=True
            ):
                if not len(peers):
                    result[sensor.sensor_id] = dict.fromkeys(
                        self.settings.spatial_keys, 0.0
                    )
                    continue
                # builtin sum, in the peers' order: it rounds like the code the model
                # was trained on (ndarray.sum would change the features by a few ulps)
                result[sensor.sensor_id] = {
                    "spatial_delta_mean": sensor.mean
                    - sum(members[i].mean for i in peers) / len(peers),
                    "spatial_delta_last": sensor.last
                    - sum(members[i].last for i in peers) / len(peers),
                    "neighbor_count": float(len(peers)),
                }
        return result
