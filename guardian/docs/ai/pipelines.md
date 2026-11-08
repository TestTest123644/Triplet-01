# Pipelines: static checks, preprocessing, cleaning and decomposition

This page covers the steps that turn a window of readings into a verdict from rules, or into features for the model.

- `StaticCheck` answers with an anomaly label when a fixed rule is enough.
- `PreProcess` turns a 20 s window into feature rows for the short-window model.
- `LongPreProcess` does the same over 24 h for the long-window model.
- `denoise.py` cleans a series before features are computed.
- `Decompose` splits a series into frequency components.

Code: `src/guardian_layer/pipelines/`.
Config: the `preprocess`, `static_check` and `long_window` blocks of `config/settings.yaml`, read by `src/guardian_layer/utils/config.py`.

Each class is built once and reused. None of them chains the steps itself. Chaining them per window is the orchestrator's job. Until it exists, `process_window` and `process_long_window` chain them for training (see [datasets.md](datasets.md#labelledwindows)).

All of them take readings as a `list[Sensor]`, the shape `Buffer.readings` holds (`utils/schema.py`). `PreProcess` calls the same type `Reading`.

## StaticCheck

`StaticCheck(settings: StaticCheckSettings | None = None)` (`pipelines/static_check.py`). `None` uses `get_settings().static_check`.

Each rule looks at one sensor's window and returns one of these:

- its `Anomalies` label, when the rule fires
- `CheckResult.PASS`, when the rule checked and found nothing wrong
- `CheckResult.NOT_CHECKED`, when the rule couldn't run (no range, too few neighbours, no usable value)

A rule never raises on the window's content. Every value is a non-empty string, so compare with `is`, not with `if result:`. A rule that fires logs a warning with the rule, sensor, value and threshold. A rule that is skipped logs the reason at debug level.

| Rule | Signature | Fires when | NOT_CHECKED when |
|---|---|---|---|
| `out_of_range` | `out_of_range(readings: list[Sensor], value_range: tuple[float, float] \| None) -> Anomalies \| CheckResult` | a finite value lies outside `value_range` → `Anomalies.OUT_OF_RANGE` | no range, a non-finite bound, or no finite value |
| `sensor_is_down` | `sensor_is_down(readings: list[Sensor], window_end: datetime) -> Anomalies \| CheckResult` | no finite reading, or the last one is more than `down_after_seconds` before `window_end` → `Anomalies.SENSOR_IS_DOWN` | never: an empty window fires |
| `spoofed_data` | `spoofed_data(sensor_id: str, features: dict[str, float]) -> Anomalies \| CheckResult` | the sensor has a range and `abs(spatial_delta_mean) > spoof_delta` → `Anomalies.SPOOFED_DATA` | `has_range` isn't 1, fewer than `min_neighbors` neighbours, or a missing or non-finite feature |

Notes:

- `value_range` for `out_of_range` is what `PreProcess.find_range` returns.
- `sensor_is_down` ignores readings dated later than `window_end + future_tolerance_seconds`, so one wrong clock can't hide an outage.
- `features` for `spoofed_data` is the sensor's row from `PreProcess` (see [Feature keys](#feature-keys)). A fixed delta only catches gross spoofs. The model doesn't learn `spoofed_data`: fault injection gives `spoof` no label on purpose.

Helpers: `fired(label, sensor_id, value, threshold) -> Anomalies`, `skipped(rule, sensor_id, reason) -> CheckResult`, `sensor_of(readings) -> str | None`, and `feature(features, key) -> float | None` (a finite number from the row, else `None`).

## PreProcess

`PreProcess(method: str | None = None, settings: PreprocessSettings | None = None)` (`pipelines/preprocess.py`) turns one 20 s window into features per sensor.

- `method` is the decomposition method: `none`, `fft`, `haar`, `db4` or `ssa`. `None` uses `default_method`. An unknown name raises `ValueError("unknown decoupling method: <method>")`.
- `settings` of `None` uses `get_settings().preprocess`.

The steps for one window, in order:

| Step | Method | What it does |
|---|---|---|
| 1 | `finite(window) -> list[Reading]` | Drops NaN and inf values and logs the sensors. A reading with no value at all raises `ValueError` (`require_value`): that sensor isn't working. |
| 2 | `deduplicate(window) -> tuple[list[Reading], dict[str, dict[str, float]]]` | Keeps the first arrival of each `(sensor_id, packet_order)`. Without metadata, the key is `(sensor_id, timestamp, value)` (`dedup_key`). Drops packets more than `max_jump` from the sensor's median packet order, and packets more than `out_of_order_tolerance` behind the highest seen. Returns the kept readings and the [counter features](#feature-keys) per sensor. It keeps no state between windows, because overlapping windows legitimately share readings. |
| 3 | `sensor_series(readings) -> tuple[Array, Array]` | One sensor's timestamps (epoch seconds) and raw values, sorted by time, then packet order (`series_key`). |
| 4 | `find_range(readings, cluster_readings) -> tuple[float, float] \| None` | The sensor's own `(lower_range, upper_range)`, else the first one found in its cluster in this window, else `None`. A range with a non-finite bound, or with `high <= low`, is skipped. |
| 5 | `normalize(values, value_range) -> Array` | Scales to 0-1 inside the range. Without a range, z-scores within the window (zeros when flat). |
| 6 | `value_features(times, values, has_range) -> dict[str, float]` | The `value_keys` on the normalized series. |
| 7 | `resample(times, values) -> Array` | A 1 s grid over the last `max_span_seconds`, with gaps interpolated. It feeds `Decompose.caller`. |
| 8 | `short_features(decompose, times, raw) -> dict[str, float]` | The short-window model's columns (`short_keys`), from the raw series after `clean`. See below. |
| 9 | `spatial_features(sensors: list[SensorSummary]) -> dict[str, dict[str, float]]` | Per sensor, the `spatial_keys` against its k nearest neighbours in the same cluster. |

Small helpers used by the steps:

- `cluster_key(reading) -> str` is the reading's `cluster_id`.
- `timestamp_seconds(reading) -> float` gives epoch seconds, with no offset read as UTC.
- `lag_one_correlation(steps) -> float` is the correlation of each step with the next, used by `diff_autocorr`.

`process_window` in `ai/datasets/windows.py` runs these steps in this order. That's the reference to follow until the orchestrator exists.

### Short-window features

`short_features(decompose, times, raw)` describes the shape of a fault without depending on the sensor's scale or sampling rate, so the model holds up on sources it hasn't seen.

- **`clean(values) -> Array`**: runs the `clean_method` from the settings (see [Noise cleaning](#noise-cleaning)) and returns a new array.
- **`robust_z(values) -> Array`**: `(values - median) / (1.4826 × MAD)`, where MAD is the median absolute deviation. With a MAD of 0, it falls back to `normalize(values, None)`.
- **The `rz_` keys**: `value_features` run on the robust z.
- **`shape_features(values) -> dict[str, float]`**: the `shape_keys`. All zeros when the window has fewer than `min_samples` readings or is flat.
- **`native_decoupling(decompose, method, times, values) -> dict[str, float]`**: `native_method` (default `haar`) on the robust z, resampled on the sensor's own median step (`native_grid`), with keys prefixed `n_`.
- **`scale_free_decoupling(decompose, method, grid) -> dict[str, float]`**: turns the haar/db4 energies into shares of the total, and the largest level-1 detail into a multiple of the grid's spread. fft and ssa already give shares.

### Spatial features

`SensorSummary(sensor_id, cluster, lat, lon, mean, last)` is what the spatial step needs from one sensor's normalized window.

- **`location(readings) -> tuple[float | None, float | None]`**: the first valid lat/lon pair. NaN, inf and out-of-range pairs are skipped.
- **`haversine_km(lat1, lon1, lat2, lon2) -> float`**: great-circle distance using `earth_radius_km`.
- **`cluster_neighbors(members) -> list[IndexArray]`**: for each member, the indices of its k (`neighbors`) nearest located members. Ties keep input order. A member with no location, or whose peers have none, gets the whole cluster. Distances are computed in blocks of `spatial_chunk_rows`.
- **`neighbors(sensor, sensors) -> list[SensorSummary]`**: the same for one sensor, for one-off lookups.
- **`spatial_features(sensors)`**: `spatial_delta_mean` and `spatial_delta_last` are the sensor's mean and last value minus its neighbours' average. `neighbor_count` is how many neighbours were used.

### Feature keys

| Method | Returns |
|---|---|
| `feature_keys(method=None) -> tuple[str, ...]` | `value_keys` + `counter_keys` + `method_keys[method]` + `spatial_keys`, in this order. The baseline model's columns. |
| `short_keys() -> tuple[str, ...]` | `robust_keys` + `shape_keys` + `native_keys`. The short-window model's columns. |
| `known_keys() -> frozenset[str]` | every key a row can hold: all methods, short and long |

| Group | Keys | Meaning |
|---|---|---|
| `value_keys` | `sample_count`, `has_range`, `mean`, `variance`, `rolling_variance_max`, `rate_of_change_mean_abs`, `rate_of_change_max_abs`, `flatline_ratio` | Readings in the window, 1 if a range was found, level, spread, the largest variance over `rolling_samples` readings, mean and max absolute change per second, and the share of steps with no change. |
| `counter_keys` | `duplicate_count`, `out_of_order_count`, `replay_count`, `packet_gap_max` | Repeats dropped, packets accepted out of order, packets rejected for a bad order, and the largest gap in packet order. |
| `method_keys.fft` | `fft_low`, `fft_mid`, `fft_high`, `fft_peak` | Energy share in each third of the spectrum, and the peak frequency as a fraction of Nyquist. |
| `method_keys.haar` | `haar_d1_energy`, `haar_d2_energy`, `haar_d3_energy`, `haar_d1_max_abs` | Haar wavelet detail energy per level, and the largest level-1 detail. |
| `method_keys.db4` | `db4_d1_energy`, `db4_d2_energy`, `db4_d3_energy`, `db4_d1_max_abs` | The same with the Daubechies-4 wavelet. |
| `method_keys.ssa` | `ssa_trend`, `ssa_oscillation`, `ssa_noise`, `ssa_first_ratio` | Singular spectrum analysis: energy share of the first component (trend), components 2-3 (oscillation) and the rest (noise), and the first singular value's share. |
| `spatial_keys` | `spatial_delta_mean`, `spatial_delta_last`, `neighbor_count` | See [Spatial features](#spatial-features). |
| `robust_keys` | `rz_variance`, `rz_rolling_variance_max`, `rz_rate_of_change_mean_abs`, `rz_rate_of_change_max_abs`, `rz_flatline_ratio` | The value features on the robust z of the cleaned series. |
| `shape_keys` | `max_abs_step`, `max_abs_level`, `sign_flip_share`, `diff_autocorr`, `spectral_peak_share`, `diff_kurtosis`, `rolling_variance_ratio` | Largest step and level in robust z, share of steps that flip direction, lag-1 correlation of the steps, the biggest spectral peak's share of power, excess kurtosis of the steps, and the largest rolling variance over the median one (`ratio_cap` when the median is 0). |
| `native_keys` | `n_haar_d1_energy`, `n_haar_d2_energy`, `n_haar_d3_energy`, `n_haar_d1_max_abs` | `native_method` at the sensor's own rate, made scale-free. |
| `long_keys` | the 16 keys above with an `lw_` prefix (`lw_rz_*`, `lw_` shape keys, `lw_haar_*`), plus `lw_trend_scatter_ratio`, `lw_flat_run_share`, `lw_neighbour_slope_delta`, `lw_neighbor_count`, `lw_grid_points` | The long-window model's columns. See [LongPreProcess](#longpreprocess). |

The key lists are fixed in `EXPECTED_KEYS` (`utils/config.py`). `PreprocessSettings` refuses a config whose lists differ from them, because a saved model expects exactly these columns.

## LongPreProcess

`LongPreProcess(preprocessor: PreProcess, decompose: Decompose, settings: LongWindowSettings)` (`pipelines/long_preprocess.py`) turns one sensor's readings over hours into its `lw_` features.

Drift and a stuck value build up over hours, which a 20 s window can't see. `short_features` isn't reused, because its decomposition keeps only the last 20 s. The parts that don't depend on length are reused on a time grid.

| Method | Signature | Behaviour |
|---|---|---|
| `time_grid` | `time_grid(times, values) -> tuple[Array, Array, int]` | Bucket centres, bucket means and the number of filled buckets. The step is `grid_seconds`, or the sensor's median step when that's slower. Empty buckets are interpolated but not counted. Readings far from the median time are dropped, so one bad clock can't create millions of buckets. |
| `clean` | `clean(grid) -> Array` | The grid cleaned with `long_window.clean_method` and `long_window.kalman`, not the `preprocess` ones |
| `line` | `line(grid_times, grid_values) -> tuple[float, Array]` | Least-squares slope per second, and the residuals |
| `slope` | `slope(grid_times, grid_values) -> float` | The trend in robust z units per hour, so sensors of any unit compare |
| `trend_scatter_ratio` | `trend_scatter_ratio(grid_times, grid_values) -> float` | The line's change over the window divided by the spread around it. 0 when flat or under 3 points. |
| `flat_run_share` | `flat_run_share(grid_values) -> float` | The longest run of equal values (within 1e-9 relative) as a share of the grid |
| `long_features` | `long_features(times, raw) -> tuple[dict[str, float], float]` | All `lw_` keys except the neighbour ones, plus the slope. With fewer filled buckets than `min_samples`, everything is zero except `lw_grid_points`. |
| `unique_readings` | `unique_readings(window: list[Sensor]) -> list[Sensor]` | One reading per `(sensor_id, timestamp)`, the first kept. `deduplicate` isn't used here because its packet-order checks are tuned for 20 s. |
| `neighbour_slopes` | `neighbour_slopes(slopes: dict[str, float], clusters: dict[str, str]) -> dict[str, dict[str, float]]` | `lw_neighbour_slope_delta` (the sensor's slope minus the median slope of the others in its cluster) and `lw_neighbor_count`. Weather moves a whole cluster together, while a drifting sensor moves alone. |

`own_keys` holds the `long_keys` that one sensor gives on its own (all but the two neighbour keys).

`process_long_window(long_preprocess, preprocessor, decompose, method, window) -> dict[str, dict[str, float]]` maps each `sensor_id` in a long window to its `long_keys` row, plus `sample_count`. Bind it with `functools.partial(process_long_window, long_preprocess)` to pass it as `LabelledWindows.build(process=...)`.

## Noise cleaning

`pipelines/denoise.py` cleans one sensor's series before features are computed (#66). `CLEAN_REGISTRY: dict[str, CleanHandler]` maps `clean_method` to a function that takes a `CleanContext(values, kalman, min_samples)` and returns a new array.

| Name | Function | Behaviour |
|---|---|---|
| `none` | `no_clean(ctx) -> FloatArray` | a copy of the series |
| `kalman` | `kalman(ctx) -> FloatArray` | A local-level Kalman filter or smoother: the true value moves as a random walk (process noise Q), and each reading adds measurement noise R. Both come from the window, so no per-type tuning is needed. The series comes back unchanged when it's shorter than `min_samples`, flat, or has no measurement noise (`filterable`). |

How `kalman` works, step by step:

1. **Noise estimate**: `NOISE_ESTIMATORS[kalman.noise]`.
   - `fixed_noise(values, kalman)`: R is the robust std of the steps, `median(|step|) / (0.6745 × √2)`, and Q is `process_ratio × R`.
   - `estimated_noise(values, kalman)`: Q and R come from the robust variances of the one-step and two-step changes. Q is at least `process_floor × R`.

   `robust_variance(values)` is `(1.4826 × MAD)²`.
2. **Scale**: Q is multiplied by `process_scale`.
3. **Forward pass**: `forward_pass(values, process, measurement, jump_sigmas) -> KalmanPass`. A reading more than `jump_sigmas` prediction standard deviations away is taken as it is and restarts the filter, so spikes and steps pass through.
4. **Output**: `PASS_OUTPUTS[kalman.mode]`. `filtered(forward)` is the forward pass only. `backward_pass(forward)` is Rauch-Tung-Striebel smoothing, which also uses later readings and doesn't cross a jump.
5. **Repeats**: with `keep_repeats`, every run of two or more equal readings (`repeat_runs(values) -> BoolArray`) keeps its raw values, so a stuck value stays exactly flat.

`cleaning_setup(clean_method: str, kalman: KalmanSettings) -> dict[str, Any]` returns what a saved model records about its cleaning: `{"clean_method": ...}`, plus `"kalman"` settings when the method is `kalman` (see [inference.md](inference.md#baselinemodel)).

## Decompose

`Decompose(settings: PreprocessSettings | None = None)` (`pipelines/decompose.py`) turns a normalized, evenly spaced series into a fixed set of keys, so one model can take any sensor type.

`caller(fn_name: str, values: Array) -> dict[str, float]` dispatches by name:

| `fn_name` | Method | Keys |
|---|---|---|
| `none` | none | none (empty dict) |
| `fft` | `fft(values)` | `method_keys.fft` |
| `haar` | `haar(values)` | `method_keys.haar` (up to `haar_levels`, on the last 2^k samples) |
| `db4` | `db4(values)` | `method_keys.db4` (pywt, `mode="smooth"`) |
| `ssa` | `ssa(values)` | `method_keys.ssa` (trajectory window of half the series) |

A series shorter than `min_samples` gives all zeros, and so does a constant one. An unknown name raises `ValueError("unknown decoupling method: <name>")`. Which method to keep was decided in [research/decoupling/results.md](../../research/decoupling/results.md).

## Configuration

`get_settings()` (`utils/config.py`) reads `config/settings.yaml`. Every block rejects unknown keys.

### `preprocess` (`PreprocessSettings`)

| Key | Default | Effect |
|---|---|---|
| `default_method` | `none` | decomposition method when `PreProcess(method=None)` |
| `clean_method` | `kalman` | `none` or `kalman`: cleaning before `short_features` |
| `kalman` | `mode: smoother`, `jump_sigmas: 4.0`, the rest as in [`kalman`](#kalman-kalmansettings) | Kalman settings for the short window |
| `min_samples` | `4` | fewer readings give all-zero decomposition and shape features, and no cleaning |
| `haar_levels` | `3` | wavelet levels for haar and db4 |
| `method_keys` | see [Feature keys](#feature-keys) | keys per decomposition method (fixed) |
| `counter_keys` | see Feature keys | fixed |
| `out_of_order_tolerance` | `2` | packets this far behind the highest seen count as reordering. Further back is a replay. |
| `max_jump` | `1000` | packets this far from the sensor's median packet order are corrupt |
| `value_keys` | see Feature keys | fixed |
| `rolling_samples` | `5` | sub-window length for rolling variance |
| `max_span_seconds` | `20` | the window length that `resample` and `native_grid` keep |
| `spatial_keys` | see Feature keys | fixed |
| `neighbors` | `5` | k nearest neighbours |
| `earth_radius_km` | `6371.0` | for the haversine distance |
| `spatial_chunk_rows` | `256` | rows of the distance matrix per numpy call |
| `robust_keys`, `shape_keys`, `native_keys`, `long_keys` | see Feature keys | fixed |
| `native_method` | `haar` | decomposition at the sensor's own rate. `native_keys` must be `n_` + its keys. |
| `ratio_cap` | `1.0e6` | `rolling_variance_ratio` when the median variance is 0 |
| `native_grid_per_reading` | `4` | most native-grid points per reading |

### `static_check` (`StaticCheckSettings`)

| Key | Default | Effect |
|---|---|---|
| `down_after_seconds` | `10.0` | silence before the window end that counts as down |
| `spoof_delta` | `0.2` | `spatial_delta_mean` beyond this (20 % of the range) is spoofed |
| `min_neighbors` | `3` | fewer neighbours skip the spoof check |
| `future_tolerance_seconds` | `2.0` | readings later than the window end plus this are ignored |

### `kalman` (`KalmanSettings`)

This block is used under both `preprocess.kalman` and `long_window.kalman`.

| Key | Default | Effect |
|---|---|---|
| `mode` | `filter` | `filter` (forward only) or `smoother` (also uses later readings) |
| `noise` | `fixed` | `fixed` or `estimated` |
| `process_ratio` | `0.1` | Q = this × R, with `fixed` noise |
| `process_scale` | `1.0` | Q is multiplied by this. Smaller values smooth more. |
| `process_floor` | `0.001` | the smallest Q as a share of R, with `estimated` noise |
| `jump_sigmas` | `null` | a reading this many prediction standard deviations away passes unchanged. `null` smooths every reading. |
| `keep_repeats` | `true` | runs of equal readings come out unchanged |

The defaults are the class defaults. `config/settings.yaml` sets `preprocess.kalman` to `mode: smoother` and `jump_sigmas: 4.0`, and `long_window.kalman` to the same plus `process_ratio: 0.9`. The `long_window` block is documented in [inference.md](inference.md#configuration).

## How to compute features for a buffer

This runs the static checks and the short-window features on a small window, the way `process_window` does.

```python
from datetime import UTC, datetime

from guardian_layer.pipelines.decompose import Decompose
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.pipelines.static_check import StaticCheck
from guardian_layer.utils.schema import CheckResult, Metadata, Sensor

# one water level sensor, one reading a second for 20 s
window = [
    Sensor(
        sensor_id="water_level-01",
        cluster_id="water_level",
        value=1.0 + 0.01 * (second % 3),
        unit="m",
        timestamp=datetime(2026, 10, 2, 0, 0, second, tzinfo=UTC),
        metadata=Metadata(
            lat=36.79,
            lon=10.18,
            packet_order=second + 1,
            type="water_level",
            lower_range=0.0,
            upper_range=3.0,
        ),
    )
    for second in range(20)
]

pre, decompose, checks = PreProcess(), Decompose(), StaticCheck()
unique, counters = pre.deduplicate(pre.finite(window))
value_range = pre.find_range(unique, unique)

print(checks.out_of_range(unique, value_range) is CheckResult.PASS)  # True
print(checks.sensor_is_down(unique, unique[-1].timestamp))  # pass

times, raw = pre.sensor_series(unique)
features = pre.short_features(decompose, times, raw)
print(list(features) == list(pre.short_keys()))  # True
```

For a full row with counters, decomposition and spatial features, call `process_window(pre, decompose, pre.method, window)` from `guardian_layer.ai.datasets.windows`.

If you get `ValueError: ... sent no value`, a reading has no value at all, which means that sensor is down. If you get `ValueError: unknown decoupling method`, check `method` or `default_method`.

## Related

- [datasets.md](datasets.md): where training windows come from
- [inference.md](inference.md): the models that read these features
- [research/cleaning/results.md](../../research/cleaning/results.md) and [research/decoupling/results.md](../../research/decoupling/results.md): why Kalman smoothing and `haar` were picked
