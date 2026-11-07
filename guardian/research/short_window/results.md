# Short-window model: anomalies inside one 20 s window (#96, part 1)

> **Since #66** the saved short-window model cleans each sensor's series with a
> tuned Kalman before the features, in training and live. Its numbers are in
> `research/cleaning/results.md` section 7; the tables below are from before the
> cleaning.

## 1. Question

#13 split anomaly detection into a short-window model (20-60 s, the live stream) and a
long-window model (hours to days, from the database). This is the short one: a model
for the anomalies that show up within one 20 s window, trained on the only data fast
enough for such a window, and compared with the current model (section 4 of
`research/research.ipynb`, Intel Lab only) and with the other model families of
section 3.

**Short answer.**

- **The target is met** with the model the pipeline now runs: 1.7 % false alarms on
  clean generator windows in test 2 and 1.5 % in test 3, both at the 1 reading/s
  threshold the detector applies (budget 4 %), and a held-out grid PR-AUC far above
  the current model's (0.918 vs 0.078 at 1 s, 0.984 vs 0.075 at 100 ms). The current model flags every grid window (false alarms 1.000) and 91 % of
  clean generator windows.
- **The saved model uses the features that generalize** (section 4.2, now in
  `PreProcess`): every window scaled the same way (robust z), shape features and `haar`
  at the sensor's own rate. A source it never trained on scores 0.693 (grid 1 s) and
  0.733 (grid 100 ms) PR-AUC on its test windows, against 0.10-0.21 with the earlier
  features; the generator, held out, stays low (0.201).
- **100 ms data works well:** 95 % of the faults at 0.9 % false alarms; 92 % of
  spikes, 82 % of noise, 99 % of oscillations, 91 % of stuck values.
- **1 s data is much better than before but noise stays hard:** 77 % of the faults at
  0.4 % false alarms (before: 65 % at 1.9 %), 93 % of spikes and 82 % of oscillations
  (before 25 % and 15 %), but 7 % of noise.
- **The pipeline routes by rate:** `AnomalyDetector` sends a sensor with at least 0.5
  readings/s to this model, at the threshold of its rate (1 reading/s 0.949,
  10 readings/s 0.729), and slower sensors to the section 4 model.
- **XGBoost stays the best family** (PR-AUC on grid 1 s: binary XGBoost 0.941,
  logistic regression 0.666, Isolation Forest 0.498, LOF 0.161), and its default
  settings are as good as any tuned ones.
- **Latency:** 102 ms (p50) and 123 ms (p99) for one window of 42 located sensors x 200
  readings, both models' features and the neighbour search included; well inside the
  10 s between windows.
- **Recommendation:** ready to be wired once the orchestrator exists; the open limits
  are 1 s noise, sensor types it never saw (no real fast data of another type to test
  them, section 5) and the generator's low unseen score.

## 2. Setup

- **Data** (grid frequency only among the real sources is fast enough for 20 s; no
  industrial data, no Twin data):
  - `grid_frequency`: 25 stations at 1 s, 3,589,248 readings (2 h kept in every 53 h);
    20 readings per window.
  - `grid_frequency_100ms`: 4 stations at 0.1 s, 7,718,483 readings (2 h in every 31 h);
    200 readings per window.
  - The generator (`mocks/telemetry_generator.py`): clean runs (`anomaly_rate=0`) of
    3,600 s, 20 sensors, read through `LabelledWindows` like a loader. The sensor
    without metadata is left out (`read` needs its `packet_order` and type). Runs
    100-109 train (10 runs: section 4.1); runs 0-2 (section 5's seeds) are only
    tested.
- **Windows:** 20 s of time (`injection.window_seconds = 20`), non-overlapping, with
  the readings at their real spacing (before this change every slot was one second
  apart, which made the rate-of-change features of the 100 ms data 10x too small).
- **Kinds,** injected into 10 % of the eligible sensor-windows and sized by each type's
  within-window spread, as in #95: `stuck`, `spike`, `noise`, `oscillation`
  (`sensor_malfunction`), `sine`, `ramp`, `pattern`, `uniform` (`not_sensor_data`), and
  `spoof` (no model label: scored with the static check only). `drift` is left to the
  long-window model: inside 20 s it looks like normal change.
- **Split** (injection seed 0; seeds 1-2 within 0.1 %, and within 5 % for the gaps):

  | source | training | validation | test | dropped by the gaps |
  |---|---|---|---|---|
  | grid_frequency | 99,494 | 24,952 | 53,512 | 589 |
  | grid_frequency_100ms | 20,070 | 5,160 | 11,477 | 1,564 |
  | generator (runs 100-109) | 27,152 | 6,788 | runs 0-2 | 0 |

  - Each grid source is cut once in time: its first 70 % is for training, the last
    30 % tests. The last 20 % of the training windows validate (they pick the
    thresholds). The windows in the 2 h before each later part (one sampled block) are
    dropped, so no continuous stretch of signal lies on two sides.
  - Generator training runs use all their windows; the tested runs (0-2) never train.
  - No anomaly is cut in two: every fault lies inside one window, and a window is in
    one part only. Every kind is present on both sides of every source (checked by an
    assert).
- **Model:** section 4's (`BaselineModel.fit`: XGBoost, 100 trees, depth 6, balanced
  class weights, classes `sensor_malfunction` and `not_sensor_data`) on the 20 columns
  of section 4.2's winner (`SHORT_MODEL_KEYS`: the packet counters and
  `PreProcess.short_keys()`, robust z value features, shape, `haar` at the sensor's own
  rate), trained on the training windows and not refitted after the
  thresholds are picked. Thresholds: the best F1 on each source's validation windows,
  with this model's scores (section 7 found 0.5 far too low for this model). The
  `short-window` rows use each source's own threshold; `short-window, pooled threshold`
  uses one picked on all validation windows together; `short-window @ 0.5` is the same
  model at 0.5; `short-window, rate threshold` uses the threshold of the source's
  sampling rate, as the detector would (the generator runs at 1 reading/s, so it gets
  the 1 s source's). Picked thresholds (seeds 0-2): grid 1 s 0.949-0.955, grid 100 ms
  0.71-0.77, generator 0.96-0.98. The seed-0 model is saved in `models/short_window/`
  (git-ignored) with `metrics.json` and loads with `AnomalyDetector.from_directories`
  next to the section 4 model (section 4.7). It holds a threshold per sampling rate
  (1 reading/s 0.949, 10 readings/s 0.729, from
  the real grid sources only; the generator picks none) and the 100 ms one as its
  single threshold, for a window of an unknown rate. XGBoost settings: its defaults
  (section 4.3).
- **Tests,** every cell mean ± sample sd over injection seeds 0-2:
  1. each grid source's held-out windows; for 100 ms also with the timestamps cut to
     whole seconds (`whole_second_window`), at the 100 ms threshold;
  2. section 5 (`evaluate_seed`) on generator runs 0-2 with the generator's own labels
     (value faults `out_of_range`, `spoofed`, `stuck`; packet problems kept apart), run
     with the saved seed-0 model through `AnomalyDetector`: a window is flagged when the
     detector's verdict is, so at the 1 reading/s threshold, as live (section 4.7; the
     threshold sweep still uses the raw scores);
  3. generator runs 0-2 through `LabelledWindows` with the injected kinds, every window;
     the target reads the `rate threshold` row (the 1 reading/s threshold, as live).
- **Unseen source:** each source left out of training in turn (same recipe), scored on
  the same rows as the main model. A source the model never saw has no threshold of
  its own: the unseen rows use the pooled threshold of the other sources.
- **Model families:** section 3's `EXPLORE_MODELS` unchanged (Isolation Forest and LOF,
  5 seeds each; binary XGBoost; logistic regression), anomaly or not, fitted on the same
  training rows. PR-AUC is the comparison that does not depend on a threshold; the
  other columns are at each model's own cut (the training fault share for Isolation
  Forest and LOF, 0.5 for the others).
- **Compared model:** the Intel-only model of the notebook's section 4
  (`models/anomaly_baseline`).
  The all-sources model of #95 was not kept, so it is not compared.
- **Target** (set with the design): at most 4 % false alarms on clean generator
  windows, and a higher held-out grid PR-AUC than the current model. The design first
  tied the 4 % to the rules of section 5; their 3.7 % is almost all the notebook-only
  stuck rule (`STUCK_RUN = 3`) firing on normal water-level readings (3 equal values in
  a row in 14.1 % of clean `water_level` windows: 0.01 m rounding on 0.03 m noise), so
  the rule rows of test 2 are not a baseline and the 4 % is a plain budget.
- **Metrics:** `false_alarms` is the share of the normal windows flagged;
  `recall_<kind>` the share of that kind's windows flagged; per-class precision, recall
  and F1 count a flagged window as the class the model names.
- **Reproduce:** run section 8 of `research/research.ipynb` (`short_main()`; needs the
  section 4 model). Everything is seeded. Each seed's windows are built once (about
  10 minutes) and cached in `~/.cache/guardian_layer/short_tables/`; building them and
  this output took 4,268 s (about 120 s with the cache) on a 16-CPU Windows machine (Python 3.12.12, XGBoost 3.4.1).

### 2.1 Why one pooled threshold failed

The first version picked one threshold on all validation windows together, then
refitted the model on all training windows and kept that threshold. On the 100 ms data
it caught 4 % of spikes, 3 % of noise and 36 % of oscillations. A check on injection
seed 0 (the section 8 functions, run once outside the notebook) separated two causes,
one per grid source. Test windows, recall / false alarms:

| source | thresholds: pooled / own | refitted model, pooled threshold | model the threshold was picked on, pooled | same model, own threshold |
|---|---|---|---|---|
| grid_frequency | 0.9952 / 0.9952 | 0.499 / 0.3 % | 0.675 / 2.3 % | 0.675 / 2.3 % |
| grid_frequency_100ms | 0.9952 / 0.8617 | 0.652 / 0.0 % | 0.657 / 0.0 % | 0.919 / 1.6 % |
| generator | 0.9952 / 0.9121 | 0.961 / 0.0 % | 0.956 / 0.0 % | 0.982 / 0.0 % |

- **1 s: the refit.** The pooled threshold is the 1 s source's own best one, but the
  refit moves the scores: the 90th percentile of normal 1 s test windows falls from 0.89
  to 0.63, so at 0.995 the refitted model drops from 68 % to 50 % of the faults (noise
  34 % to 5 %, oscillations 21 % to 5 %).
- **100 ms: pooling.** Its own best threshold is 0.86; the pooled one (0.995) comes
  from the 1 s and generator windows, the larger shares. Refitting changes little here.
- **One threshold for all sources, the best mean F1 over them (0.990)**, would be
  usable by the detector as is, but it does not help 100 ms enough (72 % of the
  faults) and costs 1 s false alarms (3.6 %).
- **The price of not refitting:** the refitted model ranks 1 s windows better (PR-AUC
  0.868 against 0.783 on seed 0): the last 20 % of the training windows are the closest
  in time to the test windows. A threshold that holds on the saved model is worth more
  than that ranking here.

## 3. Results

Output of `short_main()`, pasted verbatim.

```
model: models/short_window; settings: {"window_seconds": 20, "injection_seeds": [0, 1, 2], "generator_training_runs": [100, 109], "generator_evaluation_runs": [0, 1, 2], "generator_run_seconds": 3600, "gap_windows": {"grid_frequency": 360, "grid_frequency_100ms": 360}, "kinds": ["stuck", "spike", "noise", "oscillation", "spoof", "sine", "ramp", "pattern", "uniform"], "seconds": 4268}
latency, 42 sensors x 200 readings: {"sensors": 42.0, "readings": 8400.0, "p50_ms": 101.63734998786822, "p99_ms": 123.14237500366286}
target (false alarms <= 0.04, PR-AUC above): met=True
{
  "test 2 clean false alarms": 0.016959296871750084,
  "test 3 false alarms": 0.014763784429044252,
  "grid_frequency PR-AUC, short minus Intel-only": 0.8402064252522395,
  "grid_frequency_100ms PR-AUC, short minus Intel-only": 0.9094280245945777
}

## Test 1, grid_frequency

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform | precision_sensor_malfunction | recall_sensor_malfunction | f1_sensor_malfunction | precision_not_sensor_data | recall_not_sensor_data | f1_not_sensor_data |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| short-window | 0.948 ± 0.003 | 0.770 ± 0.014 | 0.850 ± 0.008 | 0.004 ± 0.000 | 0.918 ± 0.004 | 0.853 ± 0.012 | 0.928 ± 0.020 | 0.073 ± 0.023 | 0.824 ± 0.016 | 0.998 ± 0.002 | 0.999 ± 0.001 | 1.000 ± 0.000 | 0.477 ± 0.043 | 0.953 ± 0.005 | 0.647 ± 0.015 | 0.770 ± 0.009 | 0.910 ± 0.008 | 0.863 ± 0.015 | 0.886 ± 0.011 |
| short-window, pooled threshold | 0.955 ± 0.006 | 0.761 ± 0.017 | 0.847 ± 0.009 | 0.004 ± 0.001 | 0.918 ± 0.004 | 0.846 ± 0.013 | 0.920 ± 0.030 | 0.065 ± 0.023 | 0.812 ± 0.025 | 0.998 ± 0.002 | 0.999 ± 0.001 | 1.000 ± 0.000 | 0.444 ± 0.042 | 0.963 ± 0.009 | 0.640 ± 0.020 | 0.768 ± 0.012 | 0.918 ± 0.010 | 0.856 ± 0.015 | 0.886 ± 0.010 |
| short-window, rate threshold | 0.948 ± 0.003 | 0.770 ± 0.014 | 0.850 ± 0.008 | 0.004 ± 0.000 | 0.918 ± 0.004 | 0.853 ± 0.012 | 0.928 ± 0.020 | 0.073 ± 0.023 | 0.824 ± 0.016 | 0.998 ± 0.002 | 0.999 ± 0.001 | 1.000 ± 0.000 | 0.477 ± 0.043 | 0.953 ± 0.005 | 0.647 ± 0.015 | 0.770 ± 0.009 | 0.910 ± 0.008 | 0.863 ± 0.015 | 0.886 ± 0.011 |
| short-window @ 0.5 | 0.437 ± 0.006 | 0.958 ± 0.002 | 0.600 ± 0.006 | 0.122 ± 0.004 | 0.918 ± 0.004 | 0.922 ± 0.017 | 0.993 ± 0.005 | 0.805 ± 0.011 | 0.957 ± 0.005 | 0.999 ± 0.001 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.986 ± 0.001 | 0.270 ± 0.005 | 0.861 ± 0.006 | 0.411 ± 0.007 | 0.787 ± 0.004 | 0.931 ± 0.011 | 0.852 ± 0.007 |
| Intel-only (section 4) | 0.080 ± 0.000 | 0.884 ± 0.005 | 0.148 ± 0.001 | 1.000 ± 0.000 | 0.078 ± 0.001 | 0.055 ± 0.005 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.998 ± 0.001 | 0.999 ± 0.001 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.021 ± 0.005 | 0.005 ± 0.001 | 0.008 ± 0.002 | 0.043 ± 0.000 | 0.938 ± 0.004 | 0.082 ± 0.001 |

## Test 1, grid_frequency_100ms

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform | precision_sensor_malfunction | recall_sensor_malfunction | f1_sensor_malfunction | precision_not_sensor_data | recall_not_sensor_data | f1_not_sensor_data |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| short-window | 0.913 ± 0.003 | 0.953 ± 0.007 | 0.933 ± 0.004 | 0.009 ± 0.001 | 0.984 ± 0.003 | 0.911 ± 0.026 | 0.924 ± 0.013 | 0.820 ± 0.033 | 0.989 ± 0.005 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.848 ± 0.005 | 0.905 ± 0.014 | 0.875 ± 0.008 | 0.981 ± 0.002 | 1.000 ± 0.000 | 0.990 ± 0.001 |
| short-window, pooled threshold | 0.975 ± 0.004 | 0.885 ± 0.013 | 0.928 ± 0.009 | 0.002 ± 0.000 | 0.984 ± 0.003 | 0.839 ± 0.027 | 0.762 ± 0.012 | 0.578 ± 0.082 | 0.950 ± 0.045 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.953 ± 0.013 | 0.770 ± 0.026 | 0.852 ± 0.021 | 0.990 ± 0.005 | 1.000 ± 0.000 | 0.995 ± 0.003 |
| short-window, rate threshold | 0.913 ± 0.003 | 0.953 ± 0.007 | 0.933 ± 0.004 | 0.009 ± 0.001 | 0.984 ± 0.003 | 0.911 ± 0.026 | 0.924 ± 0.013 | 0.820 ± 0.033 | 0.989 ± 0.005 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.848 ± 0.005 | 0.905 ± 0.014 | 0.875 ± 0.008 | 0.981 ± 0.002 | 1.000 ± 0.000 | 0.990 ± 0.001 |
| short-window @ 0.5 | 0.854 ± 0.018 | 0.974 ± 0.009 | 0.910 ± 0.007 | 0.017 ± 0.002 | 0.984 ± 0.003 | 0.953 ± 0.021 | 0.947 ± 0.005 | 0.907 ± 0.050 | 0.995 ± 0.005 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.754 ± 0.025 | 0.946 ± 0.018 | 0.839 ± 0.010 | 0.977 ± 0.003 | 1.000 ± 0.000 | 0.988 ± 0.001 |
| Intel-only (section 4) | 0.083 ± 0.004 | 0.886 ± 0.007 | 0.151 ± 0.006 | 1.000 ± 0.000 | 0.075 ± 0.004 | 0.052 ± 0.025 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.007 ± 0.000 | 0.002 ± 0.000 | 0.003 ± 0.000 | 0.035 ± 0.002 | 0.745 ± 0.013 | 0.067 ± 0.003 |
| short-window, whole-second timestamps | 0.687 ± 0.007 | 0.833 ± 0.032 | 0.753 ± 0.011 | 0.039 ± 0.002 | 0.874 ± 0.027 | 0.776 ± 0.055 | 0.539 ± 0.104 | 0.564 ± 0.088 | 0.852 ± 0.087 | 0.997 ± 0.004 | 1.000 ± 0.000 | 0.990 ± 0.012 | 0.998 ± 0.004 | 0.470 ± 0.011 | 0.672 ± 0.059 | 0.552 ± 0.026 | 0.998 ± 0.003 | 0.989 ± 0.005 | 0.993 ± 0.002 |

## Test 3, generator

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform | precision_sensor_malfunction | recall_sensor_malfunction | f1_sensor_malfunction | precision_not_sensor_data | recall_not_sensor_data | f1_not_sensor_data |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| short-window | 0.902 ± 0.035 | 0.764 ± 0.022 | 0.827 ± 0.017 | 0.008 ± 0.003 | 0.887 ± 0.012 | 0.719 ± 0.044 | 0.750 ± 0.028 | 0.044 ± 0.023 | 0.517 ± 0.115 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.992 ± 0.013 | 0.988 ± 0.012 | 0.508 ± 0.025 | 0.670 ± 0.020 | 0.857 ± 0.052 | 0.998 ± 0.003 | 0.922 ± 0.028 |
| short-window, pooled threshold | 0.866 ± 0.011 | 0.781 ± 0.025 | 0.821 ± 0.014 | 0.012 ± 0.001 | 0.887 ± 0.012 | 0.725 ± 0.044 | 0.797 ± 0.055 | 0.075 ± 0.038 | 0.569 ± 0.091 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.963 ± 0.005 | 0.536 ± 0.017 | 0.689 ± 0.015 | 0.811 ± 0.026 | 1.000 ± 0.000 | 0.895 ± 0.016 |
| short-window, rate threshold | 0.844 ± 0.003 | 0.788 ± 0.026 | 0.815 ± 0.013 | 0.015 ± 0.001 | 0.887 ± 0.012 | 0.728 ± 0.044 | 0.803 ± 0.050 | 0.084 ± 0.039 | 0.606 ± 0.079 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.948 ± 0.007 | 0.549 ± 0.020 | 0.695 ± 0.017 | 0.785 ± 0.011 | 1.000 ± 0.000 | 0.880 ± 0.007 |
| short-window @ 0.5 | 0.337 ± 0.007 | 0.954 ± 0.005 | 0.498 ± 0.007 | 0.189 ± 0.004 | 0.887 ± 0.012 | 0.820 ± 0.028 | 0.984 ± 0.020 | 0.882 ± 0.023 | 0.925 ± 0.034 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.220 ± 0.011 | 0.852 ± 0.026 | 0.350 ± 0.014 | 0.542 ± 0.009 | 1.000 ± 0.000 | 0.703 ± 0.008 |
| Intel-only (section 4) | 0.089 ± 0.003 | 0.870 ± 0.018 | 0.162 ± 0.005 | 0.899 ± 0.000 | 0.213 ± 0.036 | 0.792 ± 0.046 | 0.843 ± 0.016 | 0.977 ± 0.012 | 0.980 ± 0.017 | 0.987 ± 0.015 | 0.445 ± 0.052 | 0.997 ± 0.005 | 1.000 ± 0.000 | 0.046 ± 0.004 | 0.898 ± 0.016 | 0.087 ± 0.007 | 0.919 ± 0.007 | 0.295 ± 0.053 | 0.445 ± 0.062 |

## Test 3, spoofed_data rule

| | recall_spoof | normal_flagged | not_checked |
|---|---|---|---|
| StaticCheck.spoofed_data | 0.000 ± 0.000 | 0.000 ± 0.000 | 0.000 ± 0.000 |

## Unseen source

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform | precision_sensor_malfunction | recall_sensor_malfunction | f1_sensor_malfunction | precision_not_sensor_data | recall_not_sensor_data | f1_not_sensor_data |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| grid_frequency: seen | 0.948 ± 0.003 | 0.770 ± 0.014 | 0.850 ± 0.008 | 0.004 ± 0.000 | 0.918 ± 0.004 | 0.853 ± 0.012 | 0.928 ± 0.020 | 0.073 ± 0.023 | 0.824 ± 0.016 | 0.998 ± 0.002 | 0.999 ± 0.001 | 1.000 ± 0.000 | 0.477 ± 0.043 | 0.953 ± 0.005 | 0.647 ± 0.015 | 0.770 ± 0.009 | 0.910 ± 0.008 | 0.863 ± 0.015 | 0.886 ± 0.011 |
| grid_frequency: unseen | 0.387 ± 0.056 | 0.687 ± 0.005 | 0.493 ± 0.044 | 0.110 ± 0.026 | 0.693 ± 0.011 | 0.731 ± 0.047 | 0.787 ± 0.043 | 0.009 ± 0.009 | 0.941 ± 0.016 | 0.999 ± 0.001 | 1.000 ± 0.000 | 0.994 ± 0.001 | 0.020 ± 0.011 | 0.239 ± 0.035 | 0.602 ± 0.018 | 0.340 ± 0.034 | 0.735 ± 0.121 | 0.734 ± 0.011 | 0.731 ± 0.062 |
| grid_frequency_100ms: seen | 0.913 ± 0.003 | 0.953 ± 0.007 | 0.933 ± 0.004 | 0.009 ± 0.001 | 0.984 ± 0.003 | 0.911 ± 0.026 | 0.924 ± 0.013 | 0.820 ± 0.033 | 0.989 ± 0.005 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.848 ± 0.005 | 0.905 ± 0.014 | 0.875 ± 0.008 | 0.981 ± 0.002 | 1.000 ± 0.000 | 0.990 ± 0.001 |
| grid_frequency_100ms: unseen | 0.720 ± 0.071 | 0.565 ± 0.076 | 0.628 ± 0.036 | 0.023 ± 0.009 | 0.733 ± 0.013 | 0.612 ± 0.014 | 0.874 ± 0.039 | 0.004 ± 0.004 | 0.159 ± 0.056 | 1.000 ± 0.000 | 0.713 ± 0.478 | 0.997 ± 0.005 | 0.156 ± 0.082 | 0.485 ± 0.105 | 0.391 ± 0.025 | 0.428 ± 0.030 | 0.963 ± 0.016 | 0.725 ± 0.135 | 0.823 ± 0.097 |
| generator: seen | 0.902 ± 0.035 | 0.764 ± 0.022 | 0.827 ± 0.017 | 0.008 ± 0.003 | 0.887 ± 0.012 | 0.719 ± 0.044 | 0.750 ± 0.028 | 0.044 ± 0.023 | 0.517 ± 0.115 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.992 ± 0.013 | 0.988 ± 0.012 | 0.508 ± 0.025 | 0.670 ± 0.020 | 0.857 ± 0.052 | 0.998 ± 0.003 | 0.922 ± 0.028 |
| generator: unseen | 0.239 ± 0.013 | 0.667 ± 0.028 | 0.351 ± 0.018 | 0.215 ± 0.005 | 0.201 ± 0.009 | 0.951 ± 0.022 | 0.432 ± 0.097 | 0.281 ± 0.123 | 0.072 ± 0.019 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.983 ± 0.029 | 0.627 ± 0.022 | 0.093 ± 0.006 | 0.373 ± 0.036 | 0.149 ± 0.011 | 0.545 ± 0.032 | 0.892 ± 0.019 | 0.677 ± 0.030 |

## Model families: Test 1, grid_frequency

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| isolation_forest | 0.522 ± 0.030 | 0.312 ± 0.012 | 0.391 ± 0.016 | 0.028 ± 0.003 | 0.498 ± 0.019 | 0.679 ± 0.049 | 0.856 ± 0.012 | 0.104 ± 0.017 | 0.016 ± 0.011 | 0.061 ± 0.051 | 0.014 ± 0.009 | 0.738 ± 0.039 | 0.049 ± 0.014 |
| lof | 0.183 ± 0.010 | 0.185 ± 0.012 | 0.184 ± 0.010 | 0.082 ± 0.003 | 0.161 ± 0.008 | 0.331 ± 0.045 | 0.405 ± 0.049 | 0.077 ± 0.009 | 0.050 ± 0.013 | 0.065 ± 0.024 | 0.149 ± 0.049 | 0.377 ± 0.043 | 0.037 ± 0.006 |
| xgboost | 0.691 ± 0.006 | 0.933 ± 0.004 | 0.794 ± 0.003 | 0.041 ± 0.001 | 0.941 ± 0.002 | 0.922 ± 0.010 | 0.984 ± 0.010 | 0.653 ± 0.030 | 0.946 ± 0.004 | 0.999 ± 0.001 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.955 ± 0.011 |
| logistic_regression | 0.469 ± 0.008 | 0.850 ± 0.011 | 0.605 ± 0.010 | 0.095 ± 0.003 | 0.666 ± 0.008 | 0.875 ± 0.016 | 0.983 ± 0.009 | 0.716 ± 0.004 | 0.609 ± 0.035 | 0.798 ± 0.052 | 0.999 ± 0.002 | 0.999 ± 0.001 | 0.838 ± 0.030 |

## Model families: Test 1, grid_frequency_100ms

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| isolation_forest | 0.249 ± 0.017 | 0.766 ± 0.030 | 0.376 ± 0.021 | 0.236 ± 0.017 | 0.453 ± 0.022 | 0.850 ± 0.041 | 1.000 ± 0.002 | 0.956 ± 0.020 | 0.621 ± 0.116 | 0.665 ± 0.158 | 0.004 ± 0.010 | 1.000 ± 0.000 | 0.999 ± 0.006 |
| lof | 0.262 ± 0.056 | 0.361 ± 0.077 | 0.303 ± 0.064 | 0.104 ± 0.009 | 0.330 ± 0.068 | 0.747 ± 0.060 | 0.646 ± 0.110 | 0.061 ± 0.054 | 0.244 ± 0.216 | 0.228 ± 0.258 | 0.174 ± 0.098 | 0.751 ± 0.240 | 0.024 ± 0.032 |
| xgboost | 0.882 ± 0.009 | 0.970 ± 0.008 | 0.924 ± 0.002 | 0.013 ± 0.001 | 0.986 ± 0.001 | 0.966 ± 0.018 | 0.929 ± 0.017 | 0.887 ± 0.052 | 0.992 ± 0.001 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 |
| logistic_regression | 0.342 ± 0.020 | 0.925 ± 0.001 | 0.499 ± 0.021 | 0.182 ± 0.018 | 0.552 ± 0.014 | 0.856 ± 0.031 | 0.840 ± 0.007 | 0.969 ± 0.015 | 0.807 ± 0.033 | 0.915 ± 0.035 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 |

## Model families: Test 3, generator

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| isolation_forest | 0.407 ± 0.034 | 0.322 ± 0.018 | 0.359 ± 0.019 | 0.048 ± 0.008 | 0.413 ± 0.018 | 0.731 ± 0.061 | 0.899 ± 0.043 | 0.092 ± 0.027 | 0.010 ± 0.011 | 0.065 ± 0.069 | 0.035 ± 0.031 | 0.695 ± 0.071 | 0.102 ± 0.053 |
| lof | 0.187 ± 0.013 | 0.177 ± 0.015 | 0.182 ± 0.013 | 0.078 ± 0.005 | 0.175 ± 0.014 | 0.369 ± 0.078 | 0.390 ± 0.073 | 0.058 ± 0.016 | 0.074 ± 0.023 | 0.036 ± 0.035 | 0.198 ± 0.070 | 0.281 ± 0.077 | 0.019 ± 0.018 |
| xgboost | 0.410 ± 0.010 | 0.929 ± 0.008 | 0.569 ± 0.011 | 0.135 ± 0.004 | 0.890 ± 0.011 | 0.804 ± 0.030 | 0.968 ± 0.033 | 0.738 ± 0.017 | 0.887 ± 0.045 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 |
| logistic_regression | 0.201 ± 0.010 | 0.877 ± 0.025 | 0.327 ± 0.014 | 0.353 ± 0.008 | 0.333 ± 0.015 | 0.930 ± 0.032 | 0.939 ± 0.009 | 0.789 ± 0.012 | 0.784 ± 0.044 | 0.767 ± 0.083 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.794 ± 0.081 |

## Test 2, Model vs rules, all sensor types

| | precision | recall | f1 | false_alarms | recall_out_of_range | recall_spoofed | recall_stuck |
|---|---|---|---|---|---|---|---|
| short-window / model | 0.831 ± 0.033 | 0.818 ± 0.006 | 0.824 ± 0.019 | 0.018 ± 0.004 | 0.993 ± 0.007 | 0.957 ± 0.012 | 0.622 ± 0.021 |
| short-window / rules | 0.658 ± 0.043 | 0.693 ± 0.034 | 0.675 ± 0.037 | 0.038 ± 0.003 | 1.000 ± 0.000 | 0.091 ± 0.030 | 0.907 ± 0.015 |
| short-window / rules + model | 0.645 ± 0.022 | 0.951 ± 0.007 | 0.769 ± 0.014 | 0.055 ± 0.003 | 1.000 ± 0.000 | 0.964 ± 0.008 | 0.915 ± 0.018 |
| Intel-only (section 4) / model | 0.069 ± 0.004 | 0.635 ± 0.022 | 0.124 ± 0.006 | 0.904 ± 0.005 | 0.722 ± 0.054 | 0.739 ± 0.050 | 0.489 ± 0.016 |
| Intel-only (section 4) / rules | 0.658 ± 0.043 | 0.693 ± 0.034 | 0.675 ± 0.037 | 0.038 ± 0.003 | 1.000 ± 0.000 | 0.091 ± 0.030 | 0.907 ± 0.015 |
| Intel-only (section 4) / rules + model | 0.096 ± 0.008 | 0.926 ± 0.013 | 0.174 ± 0.013 | 0.916 ± 0.005 | 1.000 ± 0.000 | 0.803 ± 0.035 | 0.961 ± 0.011 |

## Test 2, Model by sensor type

| | positive_share | precision | recall | f1 | false_alarms | pr_auc |
|---|---|---|---|---|---|---|
| short-window / all | 0.095 ± 0.006 | 0.831 ± 0.033 | 0.818 ± 0.006 | 0.824 ± 0.019 | 0.018 ± 0.004 | 0.857 ± 0.010 |
| short-window / water_level | 0.095 ± 0.010 | 0.910 ± 0.044 | 0.833 ± 0.009 | 0.870 ± 0.024 | 0.009 ± 0.006 | 0.892 ± 0.034 |
| short-window / rainfall | 0.083 ± 0.016 | 0.763 ± 0.050 | 0.802 ± 0.023 | 0.782 ± 0.035 | 0.023 ± 0.006 | 0.823 ± 0.028 |
| short-window / temperature | 0.092 ± 0.009 | 0.773 ± 0.056 | 0.796 ± 0.043 | 0.784 ± 0.045 | 0.024 ± 0.005 | 0.844 ± 0.048 |
| short-window / traffic_flow | 0.109 ± 0.003 | 0.872 ± 0.019 | 0.834 ± 0.023 | 0.853 ± 0.021 | 0.015 ± 0.002 | 0.871 ± 0.021 |
| Intel-only (section 4) / all | 0.095 ± 0.006 | 0.069 ± 0.004 | 0.635 ± 0.022 | 0.124 ± 0.006 | 0.904 ± 0.005 | 0.134 ± 0.012 |
| Intel-only (section 4) / water_level | 0.095 ± 0.010 | 0.046 ± 0.013 | 0.309 ± 0.051 | 0.079 ± 0.021 | 0.687 ± 0.016 | 0.079 ± 0.024 |
| Intel-only (section 4) / rainfall | 0.083 ± 0.016 | 0.061 ± 0.005 | 0.713 ± 0.081 | 0.113 ± 0.007 | 0.977 ± 0.002 | 0.124 ± 0.059 |
| Intel-only (section 4) / temperature | 0.092 ± 0.009 | 0.074 ± 0.010 | 0.761 ± 0.036 | 0.135 ± 0.017 | 0.966 ± 0.007 | 0.162 ± 0.025 |
| Intel-only (section 4) / traffic_flow | 0.109 ± 0.003 | 0.086 ± 0.004 | 0.752 ± 0.023 | 0.154 ± 0.006 | 0.985 ± 0.004 | 0.165 ± 0.008 |

## Test 2, Share flagged among windows without a value fault

| | model | rules |
|---|---|---|
| short-window / clean | 0.017 ± 0.005 | 0.037 ± 0.003 |
| short-window / duplicate | 0.016 ± 0.010 | 0.029 ± 0.018 |
| short-window / replayed | 0.016 ± 0.014 | 0.067 ± 0.040 |
| short-window / out_of_order | 0.030 ± 0.018 | 0.036 ± 0.015 |
| short-window / packet_gap | 0.036 ± 0.026 | 0.026 ± 0.024 |
| Intel-only (section 4) / clean | 0.907 ± 0.005 | 0.037 ± 0.003 |
| Intel-only (section 4) / duplicate | 0.881 ± 0.056 | 0.029 ± 0.018 |
| Intel-only (section 4) / replayed | 0.854 ± 0.029 | 0.067 ± 0.040 |
| Intel-only (section 4) / out_of_order | 0.821 ± 0.016 | 0.036 ± 0.015 |
| Intel-only (section 4) / packet_gap | 0.948 ± 0.027 | 0.026 ± 0.024 |

## Test 2, Model threshold sweep, all sensor types

| | precision | recall | f1 | false_alarms |
|---|---|---|---|---|
| short-window / 0.1 | 0.194 ± 0.011 | 0.915 ± 0.003 | 0.320 ± 0.015 | 0.398 ± 0.003 |
| short-window / 0.2 | 0.236 ± 0.012 | 0.893 ± 0.005 | 0.373 ± 0.015 | 0.304 ± 0.001 |
| short-window / 0.3 | 0.265 ± 0.013 | 0.879 ± 0.001 | 0.407 ± 0.016 | 0.255 ± 0.004 |
| short-window / 0.4 | 0.292 ± 0.011 | 0.869 ± 0.003 | 0.438 ± 0.013 | 0.220 ± 0.005 |
| short-window / 0.5 | 0.321 ± 0.011 | 0.865 ± 0.006 | 0.468 ± 0.011 | 0.192 ± 0.006 |
| short-window / 0.6 | 0.359 ± 0.010 | 0.861 ± 0.007 | 0.507 ± 0.009 | 0.161 ± 0.003 |
| short-window / 0.7 | 0.413 ± 0.012 | 0.855 ± 0.008 | 0.557 ± 0.011 | 0.127 ± 0.003 |
| short-window / 0.8 | 0.510 ± 0.006 | 0.848 ± 0.006 | 0.637 ± 0.003 | 0.085 ± 0.003 |
| short-window / 0.9 | 0.691 ± 0.012 | 0.837 ± 0.006 | 0.757 ± 0.007 | 0.039 ± 0.003 |
| Intel-only (section 4) / 0.1 | 0.083 ± 0.004 | 0.859 ± 0.021 | 0.151 ± 0.006 | 0.997 ± 0.001 |
| Intel-only (section 4) / 0.2 | 0.076 ± 0.005 | 0.767 ± 0.029 | 0.138 ± 0.008 | 0.979 ± 0.001 |
| Intel-only (section 4) / 0.3 | 0.072 ± 0.004 | 0.702 ± 0.031 | 0.130 ± 0.006 | 0.955 ± 0.001 |
| Intel-only (section 4) / 0.4 | 0.069 ± 0.004 | 0.662 ± 0.026 | 0.125 ± 0.007 | 0.931 ± 0.002 |
| Intel-only (section 4) / 0.5 | 0.069 ± 0.004 | 0.635 ± 0.022 | 0.124 ± 0.006 | 0.904 ± 0.005 |
| Intel-only (section 4) / 0.6 | 0.066 ± 0.004 | 0.589 ± 0.030 | 0.118 ± 0.007 | 0.879 ± 0.004 |
| Intel-only (section 4) / 0.7 | 0.063 ± 0.004 | 0.548 ± 0.032 | 0.113 ± 0.006 | 0.850 ± 0.005 |
| Intel-only (section 4) / 0.8 | 0.057 ± 0.002 | 0.467 ± 0.020 | 0.101 ± 0.004 | 0.811 ± 0.003 |
| Intel-only (section 4) / 0.9 | 0.039 ± 0.005 | 0.272 ± 0.030 | 0.068 ± 0.008 | 0.710 ± 0.009 |
```

## 4. Improvements

Each change was chosen on validation windows only (each source's last training windows,
after the gap); test windows only report. Every cell is the mean ± sd over injection
seeds 0-2, on the cached tables of section 8 (`experiment`, `score_variant`). The output
of each run is pasted at the end of this section.

### 4.1 Does the generator help?

The model trained without the generator, against the model with all 50 runs, on the
grid sources' validation windows: grid 1 s 0.788 to 0.804, grid 100 ms 0.964 to 0.965,
mean +0.009. The rule set before the run (drop it at +0.01 or more, keep all of it at
-0.01 or less, else keep a smaller share) gives **10 runs**, so training is faster and
the generator still covers sensors with ranges and locations. With 10 runs the 1 s test
PR-AUC is 0.817 (0.801 with 50).

### 4.2 Generalization: features that describe the fault, not the source

Two stages (and a third when both of stage B's kinds help), each kept by the rule: the
mean unseen-source PR-AUC must rise, and no real source's in-source validation PR-AUC
may drop by more than 0.02. The generator does not count in that margin: it is
synthetic and its runs are near-repeats (decided 2026-10-26, after a first run in which
it did count and the winner was base without the source columns + shape + `fft`).

| variant | in-source: generator / 1 s / 100 ms | unseen: generator / 1 s / 100 ms / **mean** |
|---|---|---|
| base (the earlier saved model's features) | 0.996 / 0.809 / 0.964 | 0.097 / 0.193 / 0.191 / **0.161** |
| A: base without the source columns | 0.991 / 0.819 / 0.951 | 0.159 / 0.155 / 0.360 / 0.224 |
| A: z-score | 0.821 / 0.862 / 0.947 | 0.190 / 0.726 / 0.339 / 0.418 |
| **A: robust z (stage A winner)** | 0.839 / 0.881 / 0.949 | 0.192 / 0.770 / 0.420 / **0.461** |
| B: robust z + shape | 0.880 / 0.893 / 0.986 | 0.196 / 0.792 / 0.677 / 0.555 |
| B: robust z + `fft` | 0.871 / 0.899 / 0.971 | 0.218 / 0.797 / 0.514 / 0.510 |
| B: robust z + `haar` | 0.859 / 0.887 / 0.973 | 0.194 / 0.798 / 0.593 / 0.528 |
| B: robust z + `ssa` | 0.873 / 0.904 / 0.975 | 0.224 / 0.782 / 0.568 / 0.525 |
| B: robust z + `db4` | 0.853 / 0.886 / 0.971 | 0.201 / 0.778 / 0.418 / 0.466 |
| **C: robust z + shape + `haar` (winner)** | 0.887 / **0.890** / **0.988** | 0.198 / 0.786 / **0.739** / **0.574** |
| first run's winner: no source columns + shape + `fft` | 0.992 / 0.858 / 0.990 | 0.134 / 0.844 / 0.569 / 0.516 |
| first run, extra row: robust z + shape + `fft` | 0.891 / 0.894 / 0.987 | 0.223 / 0.789 / 0.713 / 0.575 |

- **Robust z:** every window scaled by its own median and median absolute deviation,
  the same for every source (no range scaling for the model's value columns; ranges
  stay in `PreProcess` for the static checks). It drops the columns that name the
  source (`sample_count`, `has_range`, `mean`, the spatial columns).
- **Shape features**, per reading, the same at any scale (not at any rate: they are
  counted in readings): the largest step and level in the window's own robust spread,
  the share of sign flips of the steps, the lag-1 correlation of the steps, the
  spectral peak's share of the power, the steps' kurtosis, rolling-variance max over
  median.
- **Decoupling at the sensor's own rate:** the series resampled on its median step
  (today `PreProcess.resample` puts it on a 1 s grid, which keeps 21 of a 100 ms
  window's 200 readings), then each method; `haar` and `db4` as shares of their total
  energy. On robust z every method helps; `haar` most with shape features, `fft` and
  `ssa` close behind (robust z + shape + `fft` scored 0.575 in the first run, the same
  within the noise). On Intel Lab (#9) no method helped, but those windows held 20
  readings over 10 minutes. The bands are fractions of each sensor's own rate, so these
  columns are free of scale, not of rate.
- **Against the first run's winner,** the new one generalizes better on 100 ms and the
  generator (unseen 0.739 and 0.198 against 0.569 and 0.134) and scores higher in-source
  on 1 s (0.890 against 0.858), but lower on 1 s unseen (0.786 against 0.844) and on
  the generator's own in-source score (0.887 against 0.992).
- The winner is now `PreProcess.short_features` (keys in `settings.yaml`, checked like
  the others) and the saved model's columns; the notebook builds its columns with the
  same code. Moved code and notebook code gave equal values on 88 test series and two
  whole windows before the notebook's copies were deleted, and the re-run of this
  experiment on the rebuilt tables gave the same table (before the native-grid cap
  of 4.7, which moves section 3's PR-AUCs by 0.003 at most). Its test numbers are in
  section 3.

### 4.3 XGBoost settings

12 settings (100/300 trees, depth 4/6/8, learning rate 0.1/0.3), in-source validation
PR-AUC on the base features (not redone on the winner's columns). XGBoost's defaults
(100 trees, depth 6, rate 0.3: 0.923) are within 0.005 of the best (300 trees, depth 8,
rate 0.1: 0.924) at a quarter of its size, so they stay (`SHORT_PARAMS = {}`).

### 4.4 A threshold per sampling rate

`BaselineModel` stores a threshold per rate (readings per second) and the window length;
`AnomalyDetector` judges each sensor at the threshold of the rate nearest its window's
readings per second (on a log scale), and at the single threshold when no rate applies.
Models saved before keep loading with their single threshold. The rates' thresholds come
from the real grid sources' validation windows only. In test 3 the generator, at the
1 reading/s threshold, has 1.5 % false alarms and 79 % recall (0.8 % and 76 % at its
own, undeployable threshold).

### 4.5 Readings that share a timestamp

`PreProcess.sensor_series` sorted a sensor's readings by time, then by value; it now
breaks ties by `packet_order` (the sending order), which is right for any source; no
reading is moved in time, and nothing changes where timestamps differ (the grid sources
and the generator have no ties, so section 3's model is unaffected).

**Intel Lab changes.** The notebook puts Intel Lab on a 31 s slot grid, and two
readings of one sensor often fall into one slot, so they share a window timestamp:
390,366 such pairs, in 252,621 of 582,795 sensor-windows. Ordered by `packet_order`
(their real order) instead of by value, the features of 84,386 sensor-windows (14.5 %)
change: the level and spread do not (`mean`, `variance`), but the order-dependent ones
do, by up to 0.67 (`flatline_ratio`), 2.41 (`rate_of_change_max_abs`,
`spatial_delta_last`), 0.80 (`rate_of_change_mean_abs`) and 0.46
(`rolling_variance_max`), in range units. The section 4 model was retrained
with the new order (same seed and features): held-out PR-AUC 0.863 to 0.871,
`not_sensor_data` recall 0.866 to 0.895 and F1 0.717 to 0.739, `pattern` recall 0.978
to 0.994, every other metric within 0.01; `models/anomaly_baseline` is the retrained
one. The other Intel Lab results (sections 2, 3, 5-7 of the notebook) were not re-run.
Measured once on all Intel Lab windows (old and new
order side by side, outside the notebook). On the 100 ms windows with whole-second
timestamps it alone lifts PR-AUC from 0.569 to 0.683 and cuts false alarms from 34 %
to 4.3 % (same tables, before the generator share changed and with the earlier
features; with the winner's features, section 3: 0.874 and 3.9 %).

### 4.6 Latency

The latency window's 42 sensors have locations, so the k-nearest search runs
(5 neighbours each), and the detector routes them (all 10 readings/s: the short-window
model): 102 ms (p50) and 123 ms (p99), under the 1 s budget, so nothing was optimized
(runs of this code measured 95-123 ms p50; the machine's load varies). The
short-window features take 31 of the 181 ms of one profiled run (17 %);
`deduplicate` is still the largest step (36 %), then `find_range` (17 %: for a sensor
without a range it scans its whole cluster):

```
   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    0.011    0.011    0.181    0.181 src\guardian_layer\ai\datasets\windows.py:60(process_window)
        1    0.037    0.037    0.066    0.066 src\guardian_layer\pipelines\preprocess.py:125(deduplicate)
       42    0.000    0.000    0.031    0.001 src\guardian_layer\pipelines\preprocess.py:382(short_features)
       42    0.030    0.001    0.030    0.001 src\guardian_layer\pipelines\preprocess.py:216(find_range)
       42    0.002    0.000    0.019    0.000 src\guardian_layer\pipelines\preprocess.py:208(sensor_series)
       42    0.002    0.000    0.013    0.000 src\guardian_layer\pipelines\preprocess.py:314(shape_features)
```

### 4.7 In the pipeline: two models, routed by rate

- `PreProcess.short_features` computes the winner's columns for every sensor, and
  `process_window` adds them after today's keys, so one row serves both models;
  `feature_keys(method)` is unchanged, so the section 4 model reads the same columns.
- `BaselineModel.load` keeps the columns saved in `meta.json` (it used to rebuild
  them from the decoupling method) and refuses a model that names a key `PreProcess`
  no longer writes.
- `AnomalyDetector(model, short_model)` (or `from_directories`) sends a sensor to the
  short-window model when its window holds at least `inference.short_window_min_rate`
  readings per second (0.5: the model trained at 1/s and 10/s, and 0.5 keeps a 1/s
  sensor that lost half its packets), else to `model`; each model judges at its own
  threshold for the sensor's rate. A check on one live-sized window (the 42 sensors
  at 10/s, and the same sensors thinned to 4 readings per window) gave every fast
  sensor the short-window model's verdict and every slow one the section 4 model's.
- **Test 2 now flags what the detector flags.** It compared the raw score with the
  model's single threshold, the 10 readings/s one (0.814), also for the generator's
  1 reading/s sensors. With the new features that gave 7.7 % false alarms on clean
  windows (0.2 % with the earlier features and thresholds); judged as the detector
  judges them live, at the 1 reading/s threshold (0.950), they are 1.5 %. The change
  was made after the 7.7 % run, so both are given here; for the section 4 model, which
  has a single threshold, nothing changes. Test 3's target figure moved the same way,
  from the generator's own threshold (0.8 % in section 3) to the 1 reading/s one
  (1.5 %), after the branch review.
- **The native grid is capped** at `native_grid_per_reading` (4) points a reading, after
  the branch review: timestamps a device sends microseconds apart gave a grid of 21
  million points for 6 readings. The cap leaves regular series alone (990 grid and
  generator windows unchanged) but coarsens windows with gaps, whose median step is
  short; with it, section 3's PR-AUCs move by 0.003 at most and the picked thresholds
  by up to 0.14 (100 ms 0.76-0.91 to 0.71-0.77); test 2 is 1.7 % (1.5 % before).

### 4.8 Output of the experiments

Pasted verbatim, except the first generalization run's headings, renamed to tell the
two runs apart.

```
## Generator experiment
| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean |
|---|---|---|---|---|
| all sources | 0.999 ± 0.001 | 0.788 ± 0.018 | 0.964 ± 0.005 | 0.917 ± 0.008 |
| without the generator |  | 0.804 ± 0.008 | 0.965 ± 0.007 | 0.884 ± 0.003 |
grid validation PR-AUC without the generator: +0.0088 -> keep 10 runs

## Generalization, stage A: normalization
| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean |
|---|---|---|---|---|---|---|---|---|
| base | 0.996 ± 0.001 | 0.809 ± 0.009 | 0.964 ± 0.005 | 0.923 ± 0.003 | 0.097 ± 0.005 | 0.193 ± 0.025 | 0.191 ± 0.029 | 0.161 ± 0.015 |
| base, no source columns | 0.991 ± 0.003 | 0.819 ± 0.002 | 0.951 ± 0.003 | 0.920 ± 0.001 | 0.159 ± 0.004 | 0.155 ± 0.008 | 0.360 ± 0.043 | 0.224 ± 0.011 |
| z-score | 0.821 ± 0.006 | 0.862 ± 0.003 | 0.947 ± 0.006 | 0.877 ± 0.004 | 0.190 ± 0.013 | 0.726 ± 0.034 | 0.339 ± 0.073 | 0.418 ± 0.020 |
| robust z | 0.839 ± 0.011 | 0.881 ± 0.002 | 0.949 ± 0.007 | 0.890 ± 0.005 | 0.192 ± 0.008 | 0.770 ± 0.019 | 0.420 ± 0.025 | 0.461 ± 0.005 |

## Generalization, stage B: shape and decoupling
| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean |
|---|---|---|---|---|---|---|---|---|
| robust z | 0.839 ± 0.011 | 0.881 ± 0.002 | 0.949 ± 0.007 | 0.890 ± 0.005 | 0.192 ± 0.008 | 0.770 ± 0.019 | 0.420 ± 0.025 | 0.461 ± 0.005 |
| robust z + shape | 0.880 ± 0.007 | 0.893 ± 0.005 | 0.986 ± 0.001 | 0.920 ± 0.002 | 0.196 ± 0.024 | 0.792 ± 0.004 | 0.677 ± 0.086 | 0.555 ± 0.021 |
| robust z + fft | 0.871 ± 0.005 | 0.899 ± 0.001 | 0.971 ± 0.005 | 0.913 ± 0.003 | 0.218 ± 0.004 | 0.797 ± 0.029 | 0.514 ± 0.057 | 0.510 ± 0.016 |
| robust z + haar | 0.859 ± 0.008 | 0.887 ± 0.003 | 0.973 ± 0.002 | 0.906 ± 0.004 | 0.194 ± 0.010 | 0.798 ± 0.015 | 0.593 ± 0.063 | 0.528 ± 0.019 |
| robust z + db4 | 0.853 ± 0.013 | 0.886 ± 0.001 | 0.971 ± 0.002 | 0.903 ± 0.004 | 0.201 ± 0.010 | 0.778 ± 0.018 | 0.418 ± 0.072 | 0.466 ± 0.028 |
| robust z + ssa | 0.873 ± 0.008 | 0.904 ± 0.003 | 0.975 ± 0.004 | 0.917 ± 0.003 | 0.224 ± 0.021 | 0.782 ± 0.011 | 0.568 ± 0.088 | 0.525 ± 0.038 |

## Generalization, stage C: together
| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean |
|---|---|---|---|---|---|---|---|---|
| robust z | 0.839 ± 0.011 | 0.881 ± 0.002 | 0.949 ± 0.007 | 0.890 ± 0.005 | 0.192 ± 0.008 | 0.770 ± 0.019 | 0.420 ± 0.025 | 0.461 ± 0.005 |
| robust z + shape + haar | 0.887 ± 0.009 | 0.890 ± 0.007 | 0.988 ± 0.000 | 0.921 ± 0.004 | 0.198 ± 0.003 | 0.786 ± 0.013 | 0.739 ± 0.021 | 0.574 ± 0.011 |
stage A: robust z; stage B: shape kept True, decoupling robust z + haar
winner: robust z + shape + haar

## First run (the generator counted in the margin), stage A: normalization
| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean |
|---|---|---|---|---|---|---|---|---|
| base | 0.996 ± 0.001 | 0.809 ± 0.009 | 0.964 ± 0.005 | 0.923 ± 0.003 | 0.097 ± 0.005 | 0.193 ± 0.025 | 0.191 ± 0.029 | 0.161 ± 0.015 |
| base, no source columns | 0.991 ± 0.003 | 0.819 ± 0.002 | 0.951 ± 0.003 | 0.920 ± 0.001 | 0.159 ± 0.004 | 0.155 ± 0.008 | 0.360 ± 0.043 | 0.224 ± 0.011 |
| z-score | 0.821 ± 0.006 | 0.862 ± 0.003 | 0.947 ± 0.006 | 0.877 ± 0.004 | 0.190 ± 0.013 | 0.726 ± 0.034 | 0.339 ± 0.073 | 0.418 ± 0.020 |
| robust z | 0.839 ± 0.011 | 0.881 ± 0.002 | 0.949 ± 0.007 | 0.890 ± 0.005 | 0.192 ± 0.008 | 0.770 ± 0.019 | 0.420 ± 0.025 | 0.461 ± 0.005 |
## First run, stage B: shape and decoupling
| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean |
|---|---|---|---|---|---|---|---|---|
| base, no source columns | 0.991 ± 0.003 | 0.819 ± 0.002 | 0.951 ± 0.003 | 0.920 ± 0.001 | 0.159 ± 0.004 | 0.155 ± 0.008 | 0.360 ± 0.043 | 0.224 ± 0.011 |
| base, no source columns + shape | 0.993 ± 0.002 | 0.848 ± 0.004 | 0.988 ± 0.001 | 0.943 ± 0.002 | 0.122 ± 0.017 | 0.780 ± 0.025 | 0.538 ± 0.092 | 0.480 ± 0.037 |
| base, no source columns + fft | 0.992 ± 0.002 | 0.811 ± 0.003 | 0.973 ± 0.003 | 0.925 ± 0.002 | 0.099 ± 0.015 | 0.636 ± 0.059 | 0.491 ± 0.061 | 0.408 ± 0.031 |
| base, no source columns + haar | 0.992 ± 0.003 | 0.817 ± 0.004 | 0.975 ± 0.001 | 0.928 ± 0.001 | 0.096 ± 0.034 | 0.554 ± 0.064 | 0.271 ± 0.027 | 0.307 ± 0.040 |
| base, no source columns + db4 | 0.991 ± 0.003 | 0.805 ± 0.003 | 0.975 ± 0.002 | 0.924 ± 0.001 | 0.085 ± 0.014 | 0.425 ± 0.073 | 0.278 ± 0.121 | 0.262 ± 0.037 |
| base, no source columns + ssa | 0.991 ± 0.004 | 0.873 ± 0.007 | 0.977 ± 0.004 | 0.947 ± 0.003 | 0.138 ± 0.016 | 0.718 ± 0.043 | 0.250 ± 0.097 | 0.369 ± 0.041 |
## First run, stage C: together
| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean |
|---|---|---|---|---|---|---|---|---|
| base, no source columns | 0.991 ± 0.003 | 0.819 ± 0.002 | 0.951 ± 0.003 | 0.920 ± 0.001 | 0.159 ± 0.004 | 0.155 ± 0.008 | 0.360 ± 0.043 | 0.224 ± 0.011 |
| base, no source columns + shape + fft | 0.992 ± 0.002 | 0.858 ± 0.004 | 0.990 ± 0.001 | 0.947 ± 0.002 | 0.134 ± 0.011 | 0.844 ± 0.015 | 0.569 ± 0.020 | 0.516 ± 0.005 |
stage A: base, no source columns; stage B: shape kept True, decoupling base, no source columns + fft
winner: base, no source columns + shape + fft

## First run, extra row: robust z with shape and fft
| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean |
|---|---|---|---|---|---|---|---|---|
| robust z + shape + fft | 0.891 ± 0.007 | 0.894 ± 0.007 | 0.987 ± 0.004 | 0.924 ± 0.001 | 0.223 ± 0.025 | 0.789 ± 0.007 | 0.713 ± 0.083 | 0.575 ± 0.018 |

## Tuning
| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean |
|---|---|---|---|---|
| 100 trees, depth 4, rate 0.1 | 0.991 ± 0.002 | 0.700 ± 0.003 | 0.949 ± 0.007 | 0.880 ± 0.002 |
| 100 trees, depth 4, rate 0.3 | 0.995 ± 0.002 | 0.750 ± 0.022 | 0.963 ± 0.006 | 0.903 ± 0.008 |
| 100 trees, depth 6, rate 0.1 | 0.995 ± 0.001 | 0.781 ± 0.010 | 0.958 ± 0.006 | 0.911 ± 0.004 |
| 100 trees, depth 6, rate 0.3 | 0.996 ± 0.001 | 0.809 ± 0.009 | 0.964 ± 0.005 | 0.923 ± 0.003 |
| 100 trees, depth 8, rate 0.1 | 0.995 ± 0.001 | 0.807 ± 0.009 | 0.962 ± 0.005 | 0.921 ± 0.003 |
| 100 trees, depth 8, rate 0.3 | 0.996 ± 0.000 | 0.808 ± 0.003 | 0.963 ± 0.004 | 0.923 ± 0.000 |
| 300 trees, depth 4, rate 0.1 | 0.995 ± 0.001 | 0.747 ± 0.004 | 0.963 ± 0.006 | 0.902 ± 0.001 |
| 300 trees, depth 4, rate 0.3 | 0.996 ± 0.001 | 0.798 ± 0.005 | 0.965 ± 0.004 | 0.920 ± 0.002 |
| 300 trees, depth 6, rate 0.1 | 0.996 ± 0.001 | 0.803 ± 0.011 | 0.964 ± 0.005 | 0.921 ± 0.003 |
| 300 trees, depth 6, rate 0.3 | 0.996 ± 0.001 | 0.800 ± 0.004 | 0.964 ± 0.004 | 0.920 ± 0.001 |
| 300 trees, depth 8, rate 0.1 | 0.996 ± 0.001 | 0.813 ± 0.003 | 0.964 ± 0.004 | 0.924 ± 0.001 |
| 300 trees, depth 8, rate 0.3 | 0.996 ± 0.000 | 0.791 ± 0.002 | 0.963 ± 0.005 | 0.917 ± 0.001 |
pick: 100 trees, depth 6, rate 0.3 -> SHORT_PARAMS = {'n_estimators': 100, 'max_depth': 6, 'learning_rate': 0.3}
```

## 5. Caveats

- **New sensor types are untested on real data.** Grid frequency is the only real
  source fast enough for 20 s windows, so "unseen source" here means a grid source at
  another rate, or the synthetic generator (0.201 held out). How the model does on a
  real sensor type it never saw is not measured.
- **Sensors between the two models' rates.** Below 0.5 readings/s a sensor goes to the
  section 4 model, which trained on 31 s Intel Lab slots; nothing trained between
  0.03 and 1 reading/s.
- **The generator test is not independent.** The generator trains (10 runs), and every
  run follows the same per-type waves, so tests 2 and 3 say little about new sensors;
  the unseen-source rows are the honest ones.
- **Packet counters never train.** Every training window is free of packet problems, so
  `duplicate_count` and the other counters are always 0 there.
- **The spoof check cannot run on grid** (no ranges, no locations). On the generator it
  runs on every window and never fires: the injected spoofs (3-6 normal spreads) stay
  far below its fixed 0.2 range-delta.
- **The 100 ms data is four stations** recorded in 2019; with about 5,160 validation
  windows a seed, its threshold varies between seeds (0.71-0.77).
- **Whole-second timestamps** (as the Twin sends them) are reported for information
  only: the Twin's timestamps were raised on #13, where the decision was not to
  optimise the model for the Twin's window.
- **#66 noise cleaning** would smooth exactly the `noise`, `spike` and `oscillation`
  faults this model is for; the per-kind recall here is the baseline #66's filters are
  compared against.

## 6. What next

1. **Wire the detector** (`AnomalyDetector.from_directories`) once the orchestrator
   chains the stages; `process_window` is its stand-in until then.
2. **Re-run the generator experiment** (section 4.1): with source-free features the
   generator may add nothing to training, and the lead's direction on #13 is to train
   on real data.
3. On 1 s data, look for what separates noise from normal 1 s variation (7 % recall at
   the 1 s threshold, 81 % at 0.5 with 12 % false alarms).
4. A real fast source of another sensor type, to test unseen types honestly.
5. A 60 s window (the lead allows 20-60 s) needs the live buffer changed first.
6. #66: test the noise filters against these per-kind recalls and false alarms.
