"""Long-window features (#96): one sensor's readings over hours, on a time grid.

The live path (``PreProcess``) sees a 20 s window. Drift and a stuck value build up
over hours, so this module looks at 6 h or 24 h of readings. The readings are averaged
into buckets of ``grid_seconds`` first: a series then has the same shape at any
sampling rate, and an hourly sensor is not mostly interpolated (the step is never finer
than its own period).

``PreProcess.short_features`` is not reused: its decoupling keeps only the last
``max_span_seconds`` (20 s) of a window. The length-free parts are reused on the grid.
"""

import numpy as np
import numpy.typing as npt

from guardian_layer.pipelines.decompose import Decompose
from guardian_layer.pipelines.denoise import CLEAN_REGISTRY
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.utils.config import LongWindowSettings
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import CleanContext, Sensor

logger = get_logger(__name__)

Array = npt.NDArray[np.float64]


class LongPreProcess:
    """Turn one long window of a sensor's readings into its ``lw_`` features."""

    def __init__(
        self,
        preprocessor: PreProcess,
        decompose: Decompose,
        settings: LongWindowSettings,
    ) -> None:
        self.preprocessor = preprocessor
        self.decompose = decompose
        self.settings = settings
        # the keys of one sensor alone; neighbour_slopes writes the other two, once
        # every sensor's slope is known
        neighbour = ("lw_neighbour_slope_delta", "lw_neighbor_count")
        self.own_keys = tuple(
            key for key in preprocessor.settings.long_keys if key not in neighbour
        )

    def time_grid(self, times: Array, values: Array) -> tuple[Array, Array, int]:
        """Bucket centres, bucket means and the number of filled buckets.

        ``times`` are sorted. The step is ``grid_seconds``, or the sensor's median
        step when it is slower. An empty bucket (a gap) is interpolated from its
        neighbours but not counted.

        Only readings within the longest window of the median time are kept: one
        timestamp far back or ahead (a bad clock, a replay) must neither give millions
        of buckets nor push the real readings out. The first bucket is centred on the
        first reading, so a clock jitter under half a step leaves each reading of a
        slow sensor in its own bucket.
        """
        steps = np.diff(times)
        steps = steps[steps > 0]
        step = self.settings.grid_seconds
        if len(steps):
            step = max(step, float(np.median(steps)))
        span = max(self.settings.window_seconds)
        near = np.abs(times - np.median(times)) <= span
        start = max(float(times[near][0]), float(times[near][-1]) - span) - step / 2
        kept = near & (times >= start)
        bucket = np.floor((times[kept] - start) / step).astype(np.intp)
        size = int(bucket[-1]) + 1
        counts = np.bincount(bucket, minlength=size)
        # sums of differences from one reading: a constant series stays exactly
        # constant (bucket means of 0.1 would differ in the last bit)
        reference = float(values[kept][0])
        sums = np.bincount(bucket, values[kept] - reference, minlength=size)
        filled = counts > 0
        centres = start + (np.arange(size, dtype=np.float64) + 0.5) * step
        means = sums[filled] / counts[filled] + reference
        grid = np.interp(centres, centres[filled], means)
        return centres, grid, int(filled.sum())

    def line(self, grid_times: Array, grid_values: Array) -> tuple[float, Array]:
        """Least-squares slope per second, and the residuals around the line."""
        offsets = grid_times - grid_times.mean()
        scale = float(offsets @ offsets)
        centred = grid_values - grid_values.mean()
        slope = float(offsets @ centred) / scale if scale > 0 else 0.0
        return slope, centred - slope * offsets

    def slope(self, grid_times: Array, grid_values: Array) -> float:
        """The trend in robust z units per hour, so sensors of any unit and size
        compare (neighbour_slopes)."""
        robust = self.preprocessor.robust_z(grid_values)
        return self.line(grid_times, robust)[0] * 3600.0  # per second -> per hour

    def trend_scatter_ratio(self, grid_times: Array, grid_values: Array) -> float:
        """The line's change over the window over the spread around it: drift moves
        the level far more than the noise does. 0 when flat or under 3 points."""
        # a constant series: its mean can miss it in the last bit, which would fit a
        # line through rounding noise
        if len(grid_values) < 3 or np.ptp(grid_values) == 0:
            return 0.0
        slope, residuals = self.line(grid_times, grid_values)
        change = abs(slope) * float(grid_times[-1] - grid_times[0])
        if change == 0:
            return 0.0
        scatter = float(residuals.std())
        cap = self.preprocessor.settings.ratio_cap
        return min(change / scatter, cap) if scatter > 0 else cap

    def flat_run_share(self, grid_values: Array) -> float:
        """The longest run of equal values as a share of the grid: a stuck sensor.

        Bucket means of one repeated value can differ in the last bit (sums of 0.1),
        so values within 1e-9 relative count as equal.
        """
        if len(grid_values) == 0:
            return 0.0
        same = np.isclose(grid_values[1:], grid_values[:-1], rtol=1e-9, atol=0.0)
        # the steps that break a run; the gaps between them are the runs' lengths
        breaks = np.concatenate(([-1], np.flatnonzero(~same), [len(same)]))
        longest = int(np.diff(breaks).max()) - 1  # equal steps in the longest run
        return (longest + 1) / len(grid_values)

    def clean(self, grid: Array) -> Array:
        """The grid cleaned by the long window's own setup
        (``long_window.clean_method``), not the short model's ``preprocess`` one."""
        cfg = self.settings
        return CLEAN_REGISTRY[cfg.clean_method](
            CleanContext(grid, cfg.kalman, self.preprocessor.settings.min_samples)
        )

    def long_features(self, times: Array, raw: Array) -> tuple[dict[str, float], float]:
        """One sensor's ``lw_`` features (all but the neighbour keys) and its slope.

        Fewer filled buckets than ``min_samples``: zeros, except ``lw_grid_points``.
        """
        cfg = self.preprocessor.settings
        grid_times, grid, filled = self.time_grid(times, raw)
        # every feature and the slope read the cleaned grid, as training did (#66)
        grid = self.clean(grid)
        features = dict.fromkeys(self.own_keys, 0.0)
        features["lw_grid_points"] = float(filled)
        if filled < cfg.min_samples:
            return features, 0.0
        robust = self.preprocessor.robust_z(grid)
        values = self.preprocessor.value_features(grid_times, robust, False)
        for key in cfg.robust_keys:
            features[f"lw_{key}"] = values[key.removeprefix("rz_")]
        for key, value in self.preprocessor.shape_features(grid).items():
            features[f"lw_{key}"] = value
        decoupling = self.preprocessor.scale_free_decoupling(
            self.decompose, cfg.native_method, robust
        )
        for key, value in decoupling.items():
            features[f"lw_{key}"] = value
        features["lw_trend_scatter_ratio"] = self.trend_scatter_ratio(grid_times, grid)
        features["lw_flat_run_share"] = self.flat_run_share(grid)
        return features, self.slope(grid_times, grid)

    def unique_readings(self, window: list[Sensor]) -> list[Sensor]:
        """The window with one reading per sensor and timestamp, the first kept.

        Not ``PreProcess.deduplicate``: its packet_order checks are made for 20 s. In
        a window of hours a sensor's packet orders spread over thousands (and restart
        with the device), so it would drop the start and end of the window.
        """
        seen: set[tuple[str, float]] = set()
        unique: list[Sensor] = []
        for reading in window:
            key = (reading.sensor_id, self.preprocessor.timestamp_seconds(reading))
            if key not in seen:
                seen.add(key)
                unique.append(reading)
        if len(unique) < len(window):
            logger.info("Dropped repeated readings", dropped=len(window) - len(unique))
        return unique

    def neighbour_slopes(
        self, slopes: dict[str, float], clusters: dict[str, str]
    ) -> dict[str, dict[str, float]]:
        """Each sensor's slope minus the median slope of the others in its cluster,
        and how many others there are.

        Weather, tides or traffic move a whole cluster together; a drifting sensor
        moves alone. 0 and 0 for a sensor alone in its cluster.
        """
        members: dict[str, list[str]] = {}
        for sensor_id, cluster in clusters.items():
            members.setdefault(cluster, []).append(sensor_id)
        result: dict[str, dict[str, float]] = {}
        for sensor_ids in members.values():
            cluster_slopes = np.array([slopes[s] for s in sensor_ids], dtype=np.float64)
            for index, sensor_id in enumerate(sensor_ids):
                others = np.delete(cluster_slopes, index)
                delta = (
                    slopes[sensor_id] - float(np.median(others)) if len(others) else 0.0
                )
                result[sensor_id] = {
                    "lw_neighbour_slope_delta": delta,
                    "lw_neighbor_count": float(len(others)),
                }
        return result


def process_long_window(
    long_preprocess: LongPreProcess,
    preprocessor: PreProcess,
    decompose: Decompose,
    method: str,
    window: list[Sensor],
) -> dict[str, dict[str, float]]:
    """Map each sensor_id in one long window to its row: the long_keys in order, then
    ``sample_count`` (its readings after deduplication, for the rate thresholds).

    The long counterpart of ``windows.process_window``: bound with
    ``functools.partial(process_long_window, long_preprocess)`` it has the shape
    ``LabelledWindows.build`` calls. ``preprocessor``, ``decompose`` and ``method`` are
    that shape's; the bound ``long_preprocess`` carries its own.
    """
    pre = long_preprocess.preprocessor
    unique = long_preprocess.unique_readings(pre.finite(window))
    by_sensor: dict[str, list[Sensor]] = {}
    for reading in unique:
        by_sensor.setdefault(reading.sensor_id, []).append(reading)

    rows: dict[str, dict[str, float]] = {}
    slopes: dict[str, float] = {}
    clusters: dict[str, str] = {}
    for sensor_id, readings in by_sensor.items():
        times, raw = pre.sensor_series(readings)
        rows[sensor_id], slopes[sensor_id] = long_preprocess.long_features(times, raw)
        clusters[sensor_id] = pre.cluster_key(readings[0])

    keys = pre.settings.long_keys
    features: dict[str, dict[str, float]] = {}
    for sensor_id, neighbours in long_preprocess.neighbour_slopes(
        slopes, clusters
    ).items():
        row = rows[sensor_id] | neighbours
        features[sensor_id] = {key: row[key] for key in keys}
        features[sensor_id]["sample_count"] = float(len(by_sensor[sensor_id]))

    grid_points = [row["lw_grid_points"] for row in features.values()]
    logger.info(
        "Built long-window features",
        sensors=len(features),
        min_grid_points=min(grid_points, default=0.0),
        median_grid_points=float(np.median(grid_points)) if grid_points else 0.0,
        alone=sum(row["lw_neighbor_count"] == 0 for row in features.values()),
    )
    return features
