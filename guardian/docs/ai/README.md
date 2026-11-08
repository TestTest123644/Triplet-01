# AI code: overview

The Data & AI squad's code decides whether a sensor's readings can be trusted. It answers in two ways.

- **Static checks** catch what a fixed rule can: a value outside its range, a silent sensor, a sensor far from its neighbours.
- **A trained model** looks at the shape of the signal in a window. It names a malfunction (stuck, spikes, drift, noise, oscillation) or a signal that isn't sensor data at all (a perfect sine, a ramp, a repeated pattern).

This folder documents that code:

| Page | Covers |
|---|---|
| [datasets.md](datasets.md) | the 20 dataset loaders (21 registered names), fault injection, and the labelled training windows |
| [pipelines.md](pipelines.md) | `StaticCheck`, `PreProcess`, `LongPreProcess`, Kalman cleaning, `Decompose`, and their settings |
| [inference.md](inference.md) | `BaselineModel`, `AnomalyDetector`, `LongAnomalyDetector`, and the saved models in `models/` |

Research results live in `research/<topic>/results.md`. These pages link to them and don't repeat their numbers.

## The live path

A buffer of readings (`Buffer.readings`, a `list[Sensor]`) goes through these steps:

```
 Buffer.readings (list[Sensor])
            |
            v
 +-----------------------------+
 | StaticCheck                 |  out_of_range, sensor_is_down, spoofed_data
 | rule fires -> its label     |  (pipelines.md#staticcheck)
 +-----------------------------+
            |
            v
 +-----------------------------+
 | PreProcess (20 s window)    |  finite -> deduplicate -> features per sensor
 | process_window              |  (pipelines.md#preprocess)
 +-----------------------------+
            |
            |  sample_count / 20 s >= short_window_min_rate (0.5 readings/s)?
            |
     yes    +---------------------------------+  no
            v                                 v
 +-----------------------------+   +-----------------------------+
 | AnomalyDetector             |   | no live verdict             |
 | models/short_window         |   | static checks only, until   |
 | (inference.md#anomalydetector)  | the long-window batch job   |
 +-----------------------------+   +-----------------------------+
            |                                 |
            v                                 v
      Verdict per sensor      stored readings, 24 h window
                                              |
                                              v
                              +-----------------------------+
                              | LongPreProcess              |
                              | process_long_window         |
                              | (pipelines.md#longpreprocess)
                              +-----------------------------+
                                              |
                                              v
                              +-----------------------------+
                              | LongAnomalyDetector         |
                              | models/long_window/24h      |
                              | (inference.md#longanomalydetector)
                              +-----------------------------+
                                              |
                                              v
                                    Verdict per sensor
```

Where to read about each step:

- [StaticCheck](pipelines.md#staticcheck)
- [PreProcess](pipelines.md#preprocess) and [LongPreProcess](pipelines.md#longpreprocess)
- [AnomalyDetector](inference.md#anomalydetector) and [LongAnomalyDetector](inference.md#longanomalydetector)
- [Verdict](inference.md#verdict)

The orchestrator that chains these steps live isn't written yet. Until it is, `process_window` (`ai/datasets/windows.py`) and `process_long_window` (`pipelines/long_preprocess.py`) are the reference for the order of the steps. Nothing runs the long-window path live yet.

## The offline path

The models are trained on real sensor data with faults injected into it:

```
 config/datasets.yaml ──> DatasetFactory.load(name) ──> Iterator[LabeledReading]
                                                              |
                                                              v
                                       LabelledWindows.read(source, readings)
                                                              |
                                                              v
                    LabelledWindows.build(data, seed)  <── FaultInjector
                    (windows -> train/test split by time -> inject -> features)
                                                              |
                                                              v
                                                       WindowDataset
                                                              |
                                                              v
                                     BaselineModel.fit(...) ──> save(models/<name>)
```

Where to read about each step:

- [DatasetFactory](datasets.md#datasetfactory)
- [Fault injection](datasets.md#fault-injection)
- [LabelledWindows](datasets.md#labelledwindows)
- [BaselineModel](inference.md#baselinemodel)

The training runs themselves are in `research/research.ipynb`.

## Design choices

**Real datasets with injected faults, not a synthetic generator.** Real sources carry real noise, sampling gaps and seasonal patterns, and very few labelled faults. So clean windows of real data get faults injected, sized in multiples of each type's normal spread, which means a fault means the same thing for any sensor type. The cost is that injected faults are only as realistic as the fault functions. Real faults flagged by a source (`real_fault`) and real-world events (`event`) are kept apart to check that. The mock telemetry generator stays a test fixture for unseen data. See [datasets.md](datasets.md#fault-injection) and [research/general_model/results.md](../../research/general_model/results.md).

**Two time horizons.** A sensor that reports once an hour has no shape inside 20 s, and drift takes hours to show. So fast sensors (from 0.5 readings/s) go to the short-window model on the live 20 s window, and slow sensors go to a 24 h model on stored readings. The cost is that a slow sensor gets no model verdict until the batch job runs, only static checks. See [research/short_window/results.md](../../research/short_window/results.md) and [research/long_window/results.md](../../research/long_window/results.md).

**Rules first, model last.** Out of range, down and spoofed are cheap, exact and explainable, so they stay rules and the model only learns what rules can't. `spoof` gets no training label on purpose: `spoofed_data` belongs to the static check. The cost is that a fixed `spoof_delta` catches only gross spoofs. See [pipelines.md](pipelines.md#staticcheck).

**Features that don't depend on scale.** The short and long models read robust z-scores, shares of energy and shape measures, not raw levels. A model trained on water levels can then judge a grid frequency sensor. The cost is that the model can't see the absolute level, which is why `out_of_range` checks it. See [pipelines.md](pipelines.md#short-window-features) and [research/decoupling/results.md](../../research/decoupling/results.md).

**Thresholds per sampling rate.** The same score means different things at 1 and 10 readings per second, so each saved model stores a threshold per rate and picks the nearest one. The cost is that every new rate needs a measured threshold. Until then, the nearest one is used. See [inference.md](inference.md#baselinemodel).

**Kalman cleaning before the features.** Smoothing measurement noise makes faults stand out, without per-type tuning, because both noise levels come from the window itself. Runs of equal readings are kept exactly, so a stuck sensor stays stuck. The cost is that a model only works under the cleaning it was trained with. `BaselineModel.load` refuses a mismatch. See [pipelines.md](pipelines.md#noise-cleaning) and [research/cleaning/results.md](../../research/cleaning/results.md).

**A registry for loaders.** A new source is one module with `@DatasetFactory.register("<name>")`, and the factory never changes. The cost is that a loader exists only once `guardian_layer.ai.datasets` is imported, which the factory does on first use. See [datasets.md](datasets.md#how-to-add-a-dataset-loader).

**XGBoost saved as JSON.** `model.json` is XGBoost's own format, so loading a model runs no pickled code. See [inference.md](inference.md#saved-models).

## Code to docs

| Source file | Section |
|---|---|
| `ai/dataset_factory.py` | [datasets.md: DatasetFactory](datasets.md#datasetfactory) |
| `ai/interface/dataset_interface.py` | [datasets.md: DatasetInterface](datasets.md#datasetinterface) |
| `ai/datasets/<loader>.py` | [datasets.md: Loaders](datasets.md#loaders) |
| `ai/datasets/common.py` | [datasets.md: Shared helpers](datasets.md#shared-helpers) |
| `ai/datasets/fault.py`, `ai/datasets/injection.py` | [datasets.md: Fault injection](datasets.md#fault-injection) |
| `ai/datasets/windows.py` | [datasets.md: LabelledWindows](datasets.md#labelledwindows) |
| `utils/dataset_config.py`, `config/datasets.yaml` | [datasets.md: Configuration](datasets.md#configuration) |
| `pipelines/static_check.py` | [pipelines.md: StaticCheck](pipelines.md#staticcheck) |
| `pipelines/preprocess.py` | [pipelines.md: PreProcess](pipelines.md#preprocess) |
| `pipelines/long_preprocess.py` | [pipelines.md: LongPreProcess](pipelines.md#longpreprocess) |
| `pipelines/denoise.py` | [pipelines.md: Noise cleaning](pipelines.md#noise-cleaning) |
| `pipelines/decompose.py` | [pipelines.md: Decompose](pipelines.md#decompose) |
| `utils/config.py` (`preprocess`, `static_check`, `kalman`) | [pipelines.md: Configuration](pipelines.md#configuration) |
| `ai/inference/model.py` | [inference.md: BaselineModel](inference.md#baselinemodel) |
| `ai/inference/detector.py` | [inference.md: AnomalyDetector](inference.md#anomalydetector) |
| `ai/inference/long_detector.py` | [inference.md: LongAnomalyDetector](inference.md#longanomalydetector) |
| `models/` | [inference.md: Saved models](inference.md#saved-models) |
| `utils/config.py` (`inference`, `long_window`) | [inference.md: Configuration](inference.md#configuration) |

All paths are under `src/guardian_layer/` except `config/` and `models/`, which are at the repository root.
