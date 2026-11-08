# Inference: the model, the detectors and the saved models

This page covers the part of the AI code that turns feature rows into verdicts.

- `BaselineModel` is the classifier: XGBoost gradient-boosted trees.
- `AnomalyDetector` scores the live 20 s window of fast sensors with the short-window model.
- `LongAnomalyDetector` scores 24 h windows with the long-window model.
- `models/` holds the trained models.

Code: `src/guardian_layer/ai/inference/`.
Config: the `inference` and `long_window` blocks of `config/settings.yaml`.

The model runs last. The [static checks](pipelines.md#staticcheck) answer first for what a rule can catch (out of range, down, spoofed), and the model handles the rest.

## Verdict

Both detectors return `dict[str, Verdict]`, keyed by `sensor_id`. `Verdict` is in `utils/schema.py`.

| Field | Type | Meaning |
|---|---|---|
| `sensor_id` | `str` | the sensor |
| `score` | `float` | anomaly score, `1 - P(normal)`, from 0 to 1. It isn't calibrated, because classes are reweighted in training. |
| `label` | `Anomalies \| None` | the anomaly the model names, or `None` for normal |
| `anomalous` | `bool` (property) | `label is not None` |

## BaselineModel

`BaselineModel` (`ai/inference/model.py`) is a multiclass classifier. Class 0 is normal, and class `i` is `classes[i - 1]`.

```python
BaselineModel(
    classifier: XGBClassifier,
    method: str,                                  # decomposition method of its features
    threshold: float,                             # used when no rate threshold applies
    keys: tuple[str, ...],                        # the feature columns, in order
    classes: tuple[Anomalies, ...],               # the anomaly labels it can name
    rate_thresholds: dict[float, float] | None = None,  # readings per second -> threshold (copied)
    window_seconds: float | None = None,          # the window the rates are counted in
)
```

| Member | Signature | Behaviour |
|---|---|---|
| `fit` | `@classmethod fit(features: Array, labels: IntArray \| BoolArray, method: str, threshold: float, keys: tuple[str, ...], classes: tuple[Anomalies, ...], seed: int = 0, params: dict[str, Any] \| None = None) -> BaselineModel` | Trains on rows whose columns are `keys`. `labels` holds one class index per row. Each class is weighted to count the same in total, because faults are rare. `params` are XGBoost settings other than its defaults. Raises `ValueError` if the column count isn't `len(keys)`, or if the labels don't hold every class from 0 to `len(classes)`. |
| `predict` | `predict(features: Array) -> tuple[Array, IntArray]` | Per row, the score (`1 - P(normal)`) and the most likely anomaly class index. The index is 1-based and never 0: it says which anomaly, if it is one, and the threshold on the score decides whether it is one. |
| `threshold_for` | `threshold_for(sample_count: float) -> float` | The threshold for a window with this many readings: the rate threshold nearest its readings per second, compared on a log scale. Falls back to `threshold` without rate thresholds, without `window_seconds`, or with `sample_count <= 0`. |
| `score` | `score(features: Array) -> Array` | `predict(features)[0]` |
| `classify` | `classify(features: Array) -> IntArray` | `predict(features)[1]` |
| `save` | `save(directory: Path) -> None` | Writes `model.json` (XGBoost's own format, no pickle) and `meta.json`. Creates the folder. |
| `load` | `@classmethod load(directory: Path) -> BaselineModel` | Reads both files back and refuses a model that no longer fits the code. See below. |

`meta.json` holds:

- `method`, `threshold`, `keys`, `classes` and `window_seconds`
- `rate_thresholds`, with the rates as strings
- `cleaning`, written by `trained_cleaning`, when the model reads cleaned features

`load` raises `ValueError` and asks you to retrain in three cases:

- a key in `keys` that `PreProcess` no longer writes (`known_keys`)
- a saved `cleaning` different from what the settings apply now
- `classes` different from `inference.classes`

`trained_cleaning(keys: tuple[str, ...]) -> dict[str, Any] | None` returns the cleaning that the current settings apply to the features a model with these columns reads:

- the `preprocess` setup if `keys` holds every short-window key
- the `long_window` setup if it holds every long-window key
- `None` otherwise

It's built with [`cleaning_setup`](pipelines.md#noise-cleaning). A model saved before cleaning existed counts as `{"clean_method": "none"}`.

`model_verdicts(model: BaselineModel, features: dict[str, dict[str, float]], sensor_ids: list[str]) -> dict[str, Verdict]` (`ai/inference/detector.py`) is shared by both detectors.

- It scores the listed sensors' rows, which must hold the model's `keys` and `sample_count`. A missing key raises `KeyError`.
- A sensor is labelled when its score is above `threshold_for(sample_count)`.
- An empty `sensor_ids` returns `{}`.

## AnomalyDetector

`AnomalyDetector(short_model: BaselineModel, min_rate: float | None = None)` (`ai/inference/detector.py`) scores the live 20 s window.

- The model must have `window_seconds`, otherwise it raises `ValueError`.
- `min_rate` of `None` uses `inference.short_window_min_rate`.

| Member | Signature | Behaviour |
|---|---|---|
| `from_directory` | `@classmethod from_directory(short_dir: Path) -> AnomalyDetector` | `AnomalyDetector(BaselineModel.load(short_dir))` |
| `is_short` | `is_short(row: dict[str, float]) -> bool` | `row["sample_count"] / window_seconds >= min_rate`: the sensor is fast enough for the short-window model |
| `predict_features` | `predict_features(features: dict[str, dict[str, float]]) -> dict[str, Verdict]` | Verdicts for the fast sensors, in the rows' order. Slow sensors are left out, and the count is logged. |

A slow sensor has 0 to 2 readings in 20 s, which is too few for any model. It gets no verdict here. The long-window model judges it on hours of stored readings, and until that batch job runs, only the static checks judge it.

## LongAnomalyDetector

`LongAnomalyDetector(model: BaselineModel, min_grid_points: int)` (`ai/inference/long_detector.py`) scores long windows. It raises `ValueError` if the model has a key outside `preprocess.long_keys`, which catches a short-window model loaded from the wrong folder.

| Member | Signature | Behaviour |
|---|---|---|
| `from_directory` | `@classmethod from_directory(directory: Path) -> LongAnomalyDetector` | Loads the model with `long_window.min_grid_points` |
| `predict_features` | `predict_features(features: dict[str, dict[str, float]]) -> dict[str, Verdict]` | Verdicts from [`process_long_window`](pipelines.md#longpreprocess) rows. Sensors with `lw_grid_points < min_grid_points` are left out. Logs how many were scored, skipped and flagged. |

Nothing calls it live. A batch job will feed it stored readings: first `process_long_window` builds the rows, then this scores them.

## Saved models

Each folder holds `model.json`, `meta.json` and `metrics.json`. `metrics.json` records the training settings and the evaluation results. The numbers are discussed in `research/`, not here.

| Folder | Loaded by | Method | Columns | Window | Threshold |
|---|---|---|---|---|---|
| `models/short_window/` | `AnomalyDetector.from_directory` | `none` | 20: `counter_keys` + `short_keys` | 20 s | per rate: `1.0` and `10.0` readings/s, fallback `threshold` |
| `models/long_window/24h/` | `LongAnomalyDetector.from_directory` | `haar` | 21: `long_keys` | 86400 s | per rate (11 rates), fallback `threshold` |
| `models/anomaly_baseline/` | `BaselineModel.load` only | `none` | 15: `feature_keys("none")` | none | single, `0.5` |

`anomaly_baseline` is the first baseline model (#10, #55). It has no `window_seconds`, so `AnomalyDetector` refuses it. It reads no cleaned feature, so `load` doesn't check cleaning for it.

Results:

- Short-window model: [research/short_window/results.md](../../research/short_window/results.md)
- Long-window model: [research/long_window/results.md](../../research/long_window/results.md)
- Baseline model and evaluation method: [research/general_model/results.md](../../research/general_model/results.md) and [research/evaluation/results.md](../../research/evaluation/results.md)

## Configuration

### `inference` (`InferenceSettings`)

| Key | Default | Effect |
|---|---|---|
| `model_file` | `model.json` | the classifier file name in a model folder |
| `meta_file` | `meta.json` | the metadata file name |
| `default_threshold` | `0.5` | the single threshold `research/research.ipynb` passes to `BaselineModel.fit`. The runtime code doesn't read it. |
| `short_window_min_rate` | `0.5` | readings per second from which a sensor goes to the short-window model |
| `classes` | `[sensor_malfunction, not_sensor_data]` | anomaly classes, in order (class 0 is always normal). At least one, no repeats. A saved model must match. |

### `long_window` (`LongWindowSettings`)

| Key | Default | Effect |
|---|---|---|
| `grid_seconds` | `300.0` | bucket length of the time grid. Never finer than the sensor's own period. |
| `window_seconds` | `[86400.0]` | the long window lengths. 24 h stayed within the false-alarm budget and 6 h didn't. Each must be longer than `grid_seconds`. |
| `min_grid_points` | `6` | filled buckets a sensor needs for a verdict |
| `clean_method` | `kalman` | cleaning of the grid before the features |
| `kalman` | `mode: smoother`, `process_ratio: 0.9`, `jump_sigmas: 4.0` | see [Kalman settings](pipelines.md#kalman-kalmansettings) |

## How to run a detector on a window

This builds one fast sensor's feature row with `process_window` and scores it with the saved short-window model.

```python
from datetime import UTC, datetime
from pathlib import Path

from guardian_layer.ai.datasets.windows import process_window
from guardian_layer.ai.inference import AnomalyDetector
from guardian_layer.pipelines.decompose import Decompose
from guardian_layer.pipelines.preprocess import PreProcess
import numpy as np

from guardian_layer.utils.schema import Metadata, Sensor


def reading(sensor_id: str, second: int, value: float, lon: float) -> Sensor:
    return Sensor(
        sensor_id=sensor_id,
        cluster_id="water_level",
        value=value,
        unit="m",
        timestamp=datetime(2026, 10, 2, 0, 0, second, tzinfo=UTC),
        metadata=Metadata(
            lat=36.79,
            lon=lon,
            packet_order=second + 1,
            type="water_level",
            lower_range=0.0,
            upper_range=3.0,
        ),
    )


noise = np.random.default_rng(0).normal(0.0, 0.01, 20)
window = [
    *(
        reading("water_level-01", s, 1.0 + noise[s], 10.18) for s in range(20)
    ),  # noisy, like a real gauge
    *(
        reading("water_level-02", s, 1.0 + 0.01 * (s % 3), 10.19) for s in range(20)
    ),  # a repeated motif
]

pre = PreProcess()
features = process_window(pre, Decompose(), pre.method, window)

detector = AnomalyDetector.from_directory(Path("models/short_window"))
for sensor_id, verdict in detector.predict_features(features).items():
    print(sensor_id, round(verdict.score, 3), verdict.label)
```

Run it from the repository root, because the path is relative. It prints:

```
water_level-01 0.903 None
water_level-02 1.0 not_sensor_data
```

The noisy gauge scores 0.903 but stays normal. At 1 reading per second, its threshold is the `1.0` rate threshold, 0.942, not the fallback. The repeated motif is a fabricated pattern, so it's flagged.

If you get one of these errors:

- `ValueError: model was trained with cleaning ...`: the cleaning settings changed after training. Restore them or retrain.
- `ValueError: model was trained on different classes`: `inference.classes` changed.
- `KeyError` on a feature name: the rows weren't built by `process_window`, so they lack a model column.
- No line printed: the sensor sent fewer than `short_window_min_rate` readings per second, so it's left to the long-window model.

## Related

- [pipelines.md](pipelines.md): how the feature rows are built
- [datasets.md](datasets.md): the labelled windows the models were trained on
- [README.md](README.md): where the detectors sit in the whole flow
