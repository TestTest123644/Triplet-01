# Long-window model: anomalies that build up over hours (#96, part 2)

> **Since #66** the saved long-window model cleans each sensor's 5-minute grid with
> its own tuned Kalman before the features, in training and live. Its numbers are
> in `research/cleaning/results.md` section 8; the tables below are from before the
> cleaning.

## 1. Question

#13 split anomaly detection into a short-window model (20 s, the live stream; part 1,
`research/short_window/results.md`) and a long-window model (hours, read from the
database). This is the long one: a model for the 19 slow sources (one reading every
24 s or slower), whose faults (a stuck value, a drift) build up over hours and do not
show in a 20 s window. It is compared with the model those sensors got before this
change (section 4 of `research/research.ipynb`, Intel Lab only, on 20 s windows), with
the features of part 1 alone, and with the other model families of section 3.

**Short answer.**

- **The target is met at 24 h, not at 6 h.** At 24 h the model raises 0.008 false
  alarms per sensor-day on clean held-out windows (budget 0.04, one per sensor per 25
  days) at a held-out PR-AUC of 0.951. The part 1 features alone reach 0.923, and the
  Intel-only model 0.125. At 6 h it raises 0.048 false alarms per sensor-day, over
  budget, at PR-AUC 0.861.
- **The settings keep 24 h** (`long_window.window_seconds: [86400.0]`). A fault is
  then seen at most 24 h plus the job interval after it starts.
- **The new features help with stuck values and drift.** Recall on stuck values goes
  from 0.463 to 0.643 and on drift from 0.640 to 0.734 against the part 1 features
  alone. The longest flat run carries the stuck recall, and the neighbour slope the
  drift recall and part of the false-alarm rate. The trend-vs-scatter ratio adds
  nothing measurable (PR-AUC 0.953 without it).
- **On real faults it finds almost nothing (Intel batteries, Bellinge flags).** It
  flags them barely more often than clean windows (Intel 0.035 vs 0.027, Bellinge
  0.025 vs 0.018). The recall above is on injected faults.
- **Two sources are over budget on their own.** Pooled, the false alarms are within
  budget; per source, `array_of_things` (0.099 per sensor-day) and `uk_flood`
  (0.048) are not. As an unseen source, `array_of_things` does worst (PR-AUC 0.372,
  0.222 false alarms per sensor-day).
- **The model those sensors got before cannot work on a 20 s window** (test 5). A
  slow sensor has 1.000-1.069 readings in a live 20 s window and never the 4 that
  section 4's features need.
- **XGBoost stays the best family.** On the long features, binary XGBoost scores a
  PR-AUC of 0.948, against 0.510 for logistic regression, 0.199 for Isolation Forest
  and 0.132 for LOF.
- **Latency:** 4.5 ms per sensor-day (p50), features and model included: 696 ms for
  one 24 h window of 153 Intel Lab sensors (295,497 readings).
- **Recommendation:** ship `LongAnomalyDetector` with the 24 h model once a batch job
  reads 24 h of readings from the database. The open limits are real faults, the two
  sources over budget, and drift that takes weeks (section 5).

## 2. Setup

- **Data.** The 19 slow sources: `array_of_things`, `beijing`, `bellinge`,
  `chicago_beach`, `emergency_calls`, `ett`, `intel_lab`, `jena`, `melbourne`, `nab`,
  `noaa_tides`, `sensor_community`, `traffic_speeds`, `uci_air_quality`,
  `uci_household`, `uci_load`, `uk_flood`, `uk_traffic`, `usgs`. No grid frequency
  (part 1's data) and no Twin data.
  - Array of Things is read in 24 h blocks, one every 1,801 h (about 10 days of the
    source; `long_injection.sample_every_hours`). The others are read whole.
- **Windows.** 6 h and 24 h of time, non-overlapping.
  - In each window, a sensor's readings are averaged into 300 s buckets
    (`grid_seconds`). A sensor slower than that gets buckets of its own period
    instead, so a series has the same shape at any rate.
  - A sensor needs 6 filled buckets (`min_grid_points`); rows with fewer are dropped,
    as `LongAnomalyDetector` drops them.
- **Kinds.** 10 % of the eligible sensor-windows get a fault, sized by each type's
  within-window spread as in part 1:
  - `stuck` and `drift`, labelled `sensor_malfunction`;
  - `sine`, `ramp`, `pattern` and `uniform`, labelled `not_sensor_data`.
  - Spike, noise and oscillation are left to part 1: the bucket means smooth them
    away.
- **Features** (`LongPreProcess`, 21 `lw_` keys). Part 1's features run on the grid:
  robust z, shape and `haar` decoupling. Four keys are new:
  - `lw_trend_scatter_ratio`: the line's change over the window, over the spread
    around the line;
  - `lw_flat_run_share`: the longest run of equal values, as a share of the window;
  - `lw_neighbour_slope_delta` and `lw_neighbor_count`: the sensor's slope minus the
    median slope of the other sensors in its cluster, and how many there are.
    Weather moves a whole cluster; a drifting sensor moves alone.
- **Split** (per source, by time). The first 70 % of the windows train, and the last
  20 % of those validate (they pick the thresholds). One window of gap before each
  later part keeps any stretch of signal on one side only.

  | window | rows | training | validation | test |
  |---|---|---|---|---|
  | 6 h | 3,697,491 | 1,993,876 | 505,864 | 1,092,835 |
  | 24 h | 941,239 | 506,428 | 134,277 | 282,503 |

  The counts are the same for every seed; the rows dropped by the gaps make up the
  difference.
- **Model.** Section 4's `BaselineModel.fit` (XGBoost, classes `sensor_malfunction`
  and `not_sensor_data`) on the 21 `lw_` keys.
  - Threshold: the best validation F1 among the thresholds that keep clean windows
    within 0.04 false alarms per sensor-day.
  - The threshold is pooled over all sources. A per-rate threshold is added only
    where a reading rate misses the budget at the pooled one: at 24 h, none in any
    seed; at 6 h, 6 rates in each of seeds 0-2.
  - Pooled thresholds: 24 h 0.949, 0.942 and 0.947; 6 h 0.923, 0.925 and 0.923.
  - The seed-0 model is saved to `models/long_window/{6h,24h}/` with `metrics.json`.
- **Numbers** are mean ± sample sd over injection seeds 0-2.
  - A cell marked "(2 seeds)" or "(1 seed)" averages only the seeds that had test
    windows of that kind; n/a means no seed had any.
  - Run time: 3.5 h for 6 h windows and 2.7 h for 24 h, 19 sources and 3 seeds each
    (test 5, run once, not included).
- **The Intel-only model** is section 4's, retrained on the current code: the
  2026-10-11 local copy was a binary model that the current `BaselineModel` refuses
  to load. It scores section 4's columns of the same long windows (comparisons, tests
  2-3) and live 20 s windows (test 5).

## 3. Results

### 3.1 Target

| window | clean false alarms / sensor-day | PR-AUC | PR-AUC gain over reuse only | PR-AUC gain over Intel-only | met |
|---|---|---|---|---|---|
| 6 h | 0.048 | 0.861 | 0.046 | 0.756 | no |
| 24 h | 0.008 | 0.951 | 0.029 | 0.826 | yes |

The target, set before the runs, is at most 0.04 false alarms per sensor-day (one
per sensor per 25 days) on clean held-out windows, and a held-out PR-AUC above both
the reuse-only model and the Intel-only model.

### 3.2 Comparisons, all held-out windows, 24 h

- `reuse only`: part 1's features on the grid, without the four new keys.
- `without ...`: one new feature dropped.
- `Intel-only (section 4)`: the model the slow sensors got before.
- `family: ...`: section 3's families fitted on the same rows. Each uses its own
  threshold rule, not the budget.

| | precision | recall | PR-AUC | false alarms / sensor-day | stuck | drift | sine | ramp | pattern | uniform |
|---|---|---|---|---|---|---|---|---|---|---|
| long-window | 0.919 ± 0.004 | 0.869 ± 0.002 | 0.951 ± 0.002 | 0.008 ± 0.001 | 0.643 ± 0.005 | 0.734 ± 0.009 | 0.974 ± 0.001 | 0.995 ± 0.001 | 0.946 ± 0.005 | 0.905 ± 0.010 |
| reuse only | 0.889 ± 0.032 | 0.827 ± 0.013 | 0.923 ± 0.001 | 0.011 ± 0.004 | 0.463 ± 0.023 | 0.640 ± 0.031 | 0.975 ± 0.005 | 0.993 ± 0.001 | 0.951 ± 0.008 | 0.910 ± 0.011 |
| without trend vs scatter | 0.920 ± 0.002 | 0.869 ± 0.002 | 0.953 ± 0.001 | 0.008 ± 0.000 | 0.652 ± 0.010 | 0.723 ± 0.000 | 0.974 ± 0.000 | 0.995 ± 0.001 | 0.946 ± 0.005 | 0.906 ± 0.006 |
| without flat run | 0.913 ± 0.015 | 0.839 ± 0.006 | 0.934 ± 0.002 | 0.009 ± 0.002 | 0.477 ± 0.006 | 0.712 ± 0.010 | 0.971 ± 0.007 | 0.995 ± 0.000 | 0.947 ± 0.007 | 0.902 ± 0.010 |
| without neighbour slope | 0.873 ± 0.011 | 0.873 ± 0.009 | 0.939 ± 0.002 | 0.014 ± 0.001 | 0.673 ± 0.023 | 0.700 ± 0.022 | 0.980 ± 0.005 | 0.995 ± 0.001 | 0.952 ± 0.008 | 0.921 ± 0.010 |
| Intel-only (section 4) | 0.120 ± 0.001 | 0.875 ± 0.001 | 0.125 ± 0.001 | 0.697 ± 0.001 | 0.685 ± 0.006 | 0.987 ± 0.002 | 0.839 ± 0.004 | 0.757 ± 0.004 | 0.969 ± 0.001 | 1.000 ± 0.000 |
| family: isolation_forest | 0.235 ± 0.021 | 0.255 ± 0.025 | 0.199 ± 0.007 | 0.091 ± 0.002 | 0.509 ± 0.026 | 0.037 ± 0.018 | 0.103 ± 0.011 | 0.341 ± 0.166 | 0.532 ± 0.054 | 0.037 ± 0.016 |
| family: lof | 0.166 ± 0.009 | 0.304 ± 0.019 | 0.132 ± 0.009 | 0.166 ± 0.004 | 0.384 ± 0.014 | 0.345 ± 0.034 | 0.169 ± 0.017 | 0.339 ± 0.077 | 0.537 ± 0.048 | 0.059 ± 0.012 |
| family: xgboost | 0.705 ± 0.005 | 0.946 ± 0.002 | 0.948 ± 0.001 | 0.043 ± 0.001 | 0.816 ± 0.004 | 0.906 ± 0.002 | 0.992 ± 0.002 | 0.998 ± 0.000 | 0.971 ± 0.002 | 0.981 ± 0.003 |
| family: logistic_regression | 0.258 ± 0.001 | 0.702 ± 0.002 | 0.510 ± 0.004 | 0.220 ± 0.001 | 0.484 ± 0.018 | 0.278 ± 0.004 | 0.747 ± 0.009 | 0.912 ± 0.002 | 0.919 ± 0.003 | 0.863 ± 0.001 |

### 3.3 Comparisons, all held-out windows, 6 h

| | precision | recall | PR-AUC | false alarms / sensor-day | stuck | drift | sine | ramp | pattern | uniform |
|---|---|---|---|---|---|---|---|---|---|---|
| long-window | 0.865 ± 0.004 | 0.711 ± 0.006 | 0.861 ± 0.001 | 0.048 ± 0.002 | 0.575 ± 0.006 | 0.444 ± 0.012 | 0.831 ± 0.004 | 0.981 ± 0.001 | 0.891 ± 0.003 | 0.529 ± 0.019 |
| reuse only | 0.850 ± 0.002 | 0.644 ± 0.001 | 0.816 ± 0.000 | 0.049 ± 0.001 | 0.410 ± 0.006 | 0.322 ± 0.003 | 0.808 ± 0.008 | 0.976 ± 0.002 | 0.862 ± 0.003 | 0.461 ± 0.009 |
| without trend vs scatter | 0.869 ± 0.009 | 0.702 ± 0.006 | 0.859 ± 0.001 | 0.046 ± 0.004 | 0.575 ± 0.003 | 0.437 ± 0.013 | 0.823 ± 0.003 | 0.982 ± 0.002 | 0.879 ± 0.003 | 0.503 ± 0.026 |
| without flat run | 0.862 ± 0.007 | 0.690 ± 0.004 | 0.846 ± 0.000 | 0.048 ± 0.003 | 0.434 ± 0.010 | 0.443 ± 0.008 | 0.827 ± 0.004 | 0.983 ± 0.003 | 0.887 ± 0.002 | 0.541 ± 0.009 |
| without neighbour slope | 0.848 ± 0.006 | 0.669 ± 0.003 | 0.831 ± 0.001 | 0.052 ± 0.003 | 0.554 ± 0.003 | 0.333 ± 0.002 | 0.811 ± 0.006 | 0.976 ± 0.002 | 0.868 ± 0.002 | 0.463 ± 0.007 |
| Intel-only (section 4) | 0.109 ± 0.000 | 0.892 ± 0.001 | 0.105 ± 0.001 | 3.158 ± 0.000 | 0.575 ± 0.004 | 0.991 ± 0.001 | 0.918 ± 0.002 | 0.862 ± 0.002 | 0.976 ± 0.000 | 1.000 ± 0.000 |
| family: isolation_forest | 0.219 ± 0.020 | 0.239 ± 0.020 | 0.159 ± 0.005 | 0.369 ± 0.013 | 0.419 ± 0.025 | 0.077 ± 0.022 | 0.069 ± 0.008 | 0.542 ± 0.174 | 0.310 ± 0.116 | 0.039 ± 0.017 |
| family: lof | 0.167 ± 0.013 | 0.240 ± 0.013 | 0.128 ± 0.009 | 0.520 ± 0.029 | 0.391 ± 0.023 | 0.158 ± 0.021 | 0.160 ± 0.022 | 0.297 ± 0.059 | 0.413 ± 0.037 | 0.038 ± 0.003 |
| family: xgboost | 0.536 ± 0.005 | 0.884 ± 0.001 | 0.866 ± 0.000 | 0.331 ± 0.008 | 0.775 ± 0.004 | 0.686 ± 0.004 | 0.970 ± 0.001 | 0.999 ± 0.000 | 0.980 ± 0.000 | 0.885 ± 0.001 |
| family: logistic_regression | 0.192 ± 0.001 | 0.608 ± 0.001 | 0.439 ± 0.000 | 1.109 ± 0.004 | 0.435 ± 0.003 | 0.336 ± 0.004 | 0.787 ± 0.003 | 0.731 ± 0.001 | 0.913 ± 0.003 | 0.430 ± 0.003 |

### 3.4 Test 1, held-out windows per source, 24 h

| | precision | recall | PR-AUC | false alarms / sensor-day | stuck | drift | sine | ramp | pattern | uniform |
|---|---|---|---|---|---|---|---|---|---|---|
| array_of_things | 0.327 ± 0.007 | 0.602 ± 0.022 | 0.445 ± 0.021 | 0.099 ± 0.003 | 0.698 ± 0.043 | 0.553 ± 0.051 | 0.759 ± 0.024 | 0.962 ± 0.029 | 0.467 ± 0.047 | 0.196 ± 0.034 |
| beijing | 0.938 ± 0.008 | 0.822 ± 0.009 | 0.942 ± 0.004 | 0.006 ± 0.001 | 0.416 ± 0.007 | 0.639 ± 0.021 | 0.986 ± 0.004 | 1.000 ± 0.001 | 0.978 ± 0.008 | 0.870 ± 0.011 |
| bellinge | 0.807 ± 0.011 | 0.696 ± 0.016 | 0.826 ± 0.018 | 0.018 ± 0.000 | 0.431 ± 0.082 | 0.526 ± 0.067 | 0.741 ± 0.026 | 0.930 ± 0.017 | 0.756 ± 0.031 | 0.758 ± 0.023 |
| chicago_beach | 0.921 ± 0.010 | 0.771 ± 0.019 | 0.903 ± 0.004 | 0.007 ± 0.001 | 0.359 ± 0.015 | 0.575 ± 0.016 | 0.957 ± 0.003 | 0.988 ± 0.006 | 0.984 ± 0.003 | 0.644 ± 0.046 |
| emergency_calls | 1.000 ± 0.000 | 0.772 ± 0.045 | 0.978 ± 0.020 | 0.000 ± 0.000 | 0.389 ± 0.347 | 0.667 ± 0.289 | 0.833 ± 0.289 | 1.000 ± 0.000 (2 seeds) | 0.722 ± 0.255 | 1.000 ± 0.000 |
| ett | 0.898 ± 0.027 | 0.849 ± 0.030 | 0.951 ± 0.009 | 0.011 ± 0.003 | 0.561 ± 0.110 | 0.587 ± 0.038 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.994 ± 0.011 |
| intel_lab | 0.701 ± 0.060 | 0.551 ± 0.053 | 0.723 ± 0.035 | 0.027 ± 0.004 | 0.519 ± 0.085 | 0.397 ± 0.129 | 0.611 ± 0.072 | 0.958 ± 0.072 | 0.220 ± 0.102 | 0.575 ± 0.065 |
| jena | 0.941 ± 0.003 | 0.860 ± 0.018 | 0.945 ± 0.004 | 0.006 ± 0.001 | 0.510 ± 0.085 | 0.581 ± 0.083 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.939 ± 0.055 | 1.000 ± 0.000 |
| melbourne | 0.967 ± 0.003 | 0.880 ± 0.003 | 0.969 ± 0.002 | 0.003 ± 0.000 | 0.497 ± 0.025 | 0.861 ± 0.011 | 0.986 ± 0.005 | 0.997 ± 0.001 | 0.936 ± 0.004 | 0.967 ± 0.015 |
| nab | 0.667 ± 0.577 | 0.300 ± 0.265 | 0.705 ± 0.115 | 0.000 ± 0.000 | 0.167 ± 0.289 | 0.500 ± 0.707 (2 seeds) | 0.333 ± 0.577 | 1.000 (1 seed) | 0.000 (1 seed) | 0.000 ± 0.000 |
| noaa_tides | 1.000 ± 0.000 | 0.779 ± 0.035 | 0.960 ± 0.046 | 0.000 ± 0.000 | 0.833 ± 0.289 | 0.000 ± 0.000 (2 seeds) | 1.000 ± 0.000 | 1.000 (1 seed) | 1.000 ± 0.000 | 0.489 ± 0.429 |
| sensor_community | 0.700 ± 0.110 | 0.512 ± 0.073 | 0.727 ± 0.072 | 0.025 ± 0.005 | 0.556 ± 0.192 | 0.398 ± 0.131 | 0.454 ± 0.135 | 0.970 ± 0.052 | 0.294 ± 0.042 | 0.485 ± 0.079 |
| traffic_speeds | 0.986 ± 0.012 | 0.874 ± 0.008 | 0.981 ± 0.006 | 0.001 ± 0.001 | 0.446 ± 0.064 | 0.912 ± 0.040 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.930 ± 0.032 | 0.945 ± 0.049 |
| uci_air_quality | 0.967 ± 0.001 | 0.668 ± 0.025 | 0.911 ± 0.013 | 0.003 ± 0.000 | 0.287 ± 0.101 | 0.122 ± 0.031 | 0.989 ± 0.019 | 1.000 ± 0.000 | 0.978 ± 0.038 | 0.716 ± 0.094 |
| uci_household | 0.988 ± 0.014 | 0.843 ± 0.044 | 0.963 ± 0.015 | 0.001 ± 0.001 | 0.305 ± 0.059 | 0.725 ± 0.042 | 0.991 ± 0.015 | 1.000 ± 0.000 | 0.987 ± 0.022 | 1.000 ± 0.000 |
| uci_load | 0.983 ± 0.001 | 0.936 ± 0.002 | 0.988 ± 0.001 | 0.002 ± 0.000 | 0.835 ± 0.006 | 0.802 ± 0.008 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.977 ± 0.002 | 0.998 ± 0.001 |
| uk_flood | 0.673 ± 0.047 | 0.908 ± 0.018 | 0.909 ± 0.003 | 0.048 ± 0.009 | 0.606 ± 0.080 | 0.837 ± 0.024 | 0.996 ± 0.007 | 1.000 ± 0.000 | 0.974 ± 0.022 | 0.976 ± 0.010 |
| uk_traffic | 1.000 ± 0.000 | 0.874 ± 0.101 | 0.961 ± 0.045 | 0.000 ± 0.000 | 0.533 ± 0.503 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.889 ± 0.192 | 1.000 ± 0.000 |
| usgs | 0.667 ± 0.577 | 0.583 ± 0.520 | 0.833 ± 0.289 | 0.013 ± 0.023 | 0.000 ± 0.000 (2 seeds) | 1.000 (1 seed) | n/a | 1.000 ± 0.000 (2 seeds) | 1.000 (1 seed) | 0.000 (1 seed) |

### 3.5 Test 2, real faults, 24 h

`flagged` is the share of a source's windows with a real fault that are flagged.
`clean test windows flagged` is the same share over that source's clean test windows.

- Intel Lab: low battery voltage, or a value outside its physical range.
- Bellinge: the dataset's quality flags.

In tests 2 and 3, only windows with 6 filled buckets count, the ones
`LongAnomalyDetector` scores, as for the clean windows they are compared with.

| | windows | flagged | clean test windows flagged |
|---|---|---|---|
| bellinge / long-window | 3156.000 ± 0.000 | 0.025 ± 0.002 | 0.018 ± 0.000 |
| bellinge / Intel-only (section 4) | 3156.000 ± 0.000 | 0.436 ± 0.001 | 0.707 ± 0.001 |
| intel_lab / long-window | 1998.000 ± 0.000 | 0.035 ± 0.002 | 0.027 ± 0.004 |
| intel_lab / Intel-only (section 4) | 1998.000 ± 0.000 | 0.267 ± 0.000 | 0.000 ± 0.000 |

### 3.6 Test 3, events, 24 h

The windows of a real event: flood days in `emergency_calls` and NAB's labelled
events. Events are not sensor faults, so a low `flagged` is right here.

| | windows | flagged | clean test windows flagged |
|---|---|---|---|
| emergency_calls / long-window | 28.000 ± 0.000 | 0.000 ± 0.000 | 0.000 ± 0.000 |
| emergency_calls / Intel-only (section 4) | 28.000 ± 0.000 | 0.607 ± 0.000 | 0.569 ± 0.017 |
| nab / long-window | 26.000 ± 0.000 | 0.038 ± 0.038 | 0.000 ± 0.000 |
| nab / Intel-only (section 4) | 26.000 ± 0.000 | 0.885 ± 0.000 | 0.903 ± 0.051 |

### 3.7 Test 4, unseen source, 24 h

Each source is left out of training in turn and scored on its held-out windows at the
pooled threshold of the others. A new source has no reading rate of its own.

| | precision | recall | PR-AUC | false alarms / sensor-day | stuck | drift | sine | ramp | pattern | uniform |
|---|---|---|---|---|---|---|---|---|---|---|
| array_of_things | 0.195 ± 0.012 | 0.675 ± 0.027 | 0.372 ± 0.047 | 0.222 ± 0.016 | 0.688 ± 0.052 | 0.717 ± 0.012 | 0.808 ± 0.050 | 0.939 ± 0.007 | 0.077 ± 0.044 | 0.844 ± 0.031 |
| beijing | 0.921 ± 0.031 | 0.709 ± 0.032 | 0.899 ± 0.001 | 0.007 ± 0.003 | 0.414 ± 0.074 | 0.386 ± 0.055 | 0.976 ± 0.013 | 0.999 ± 0.002 | 0.974 ± 0.006 | 0.468 ± 0.044 |
| bellinge | 0.464 ± 0.038 | 0.646 ± 0.012 | 0.613 ± 0.036 | 0.083 ± 0.009 | 0.373 ± 0.026 | 0.445 ± 0.032 | 0.705 ± 0.068 | 0.925 ± 0.008 | 0.761 ± 0.048 | 0.636 ± 0.051 |
| chicago_beach | 0.745 ± 0.059 | 0.820 ± 0.005 | 0.863 ± 0.009 | 0.030 ± 0.010 | 0.329 ± 0.041 | 0.626 ± 0.004 | 0.964 ± 0.019 | 0.992 ± 0.004 | 0.988 ± 0.003 | 0.871 ± 0.015 |
| emergency_calls | 1.000 ± 0.000 | 0.737 ± 0.166 | 0.974 ± 0.028 | 0.000 ± 0.000 | 0.389 ± 0.347 | 0.500 ± 0.500 | 0.667 ± 0.577 | 1.000 ± 0.000 (2 seeds) | 0.722 ± 0.255 | 1.000 ± 0.000 |
| ett | 0.769 ± 0.040 | 0.887 ± 0.023 | 0.939 ± 0.014 | 0.031 ± 0.007 | 0.565 ± 0.093 | 0.776 ± 0.027 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 |
| intel_lab | 0.487 ± 0.028 | 0.395 ± 0.044 | 0.508 ± 0.038 | 0.048 ± 0.000 | 0.574 ± 0.116 | 0.204 ± 0.135 | 0.393 ± 0.190 | 0.784 ± 0.032 | 0.000 ± 0.000 | 0.392 ± 0.025 |
| jena | 0.907 ± 0.033 | 0.868 ± 0.024 | 0.933 ± 0.008 | 0.010 ± 0.004 | 0.558 ± 0.032 | 0.600 ± 0.088 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.939 ± 0.055 | 1.000 ± 0.000 |
| melbourne | 0.891 ± 0.009 | 0.831 ± 0.024 | 0.930 ± 0.004 | 0.011 ± 0.001 | 0.515 ± 0.049 | 0.671 ± 0.064 | 0.928 ± 0.010 | 0.984 ± 0.001 | 0.888 ± 0.014 | 0.973 ± 0.010 |
| nab | 0.644 ± 0.038 | 0.411 ± 0.084 | 0.617 ± 0.082 | 0.026 ± 0.011 | 0.333 ± 0.289 | 0.500 ± 0.707 (2 seeds) | 0.500 ± 0.500 | 1.000 (1 seed) | 0.000 (1 seed) | 0.000 ± 0.000 |
| noaa_tides | 1.000 ± 0.000 | 0.830 ± 0.087 | 0.984 ± 0.028 | 0.000 ± 0.000 | 0.917 ± 0.144 | 0.000 ± 0.000 (2 seeds) | 1.000 ± 0.000 | 1.000 (1 seed) | 1.000 ± 0.000 | 0.556 ± 0.509 |
| sensor_community | 0.700 ± 0.110 | 0.512 ± 0.073 | 0.727 ± 0.072 | 0.025 ± 0.005 | 0.556 ± 0.192 | 0.398 ± 0.131 | 0.454 ± 0.135 | 0.970 ± 0.052 | 0.294 ± 0.042 | 0.485 ± 0.079 |
| traffic_speeds | 0.777 ± 0.072 | 0.644 ± 0.040 | 0.839 ± 0.024 | 0.022 ± 0.010 | 0.351 ± 0.105 | 0.334 ± 0.154 | 0.990 ± 0.017 | 1.000 ± 0.000 | 0.416 ± 0.091 | 0.740 ± 0.142 |
| uci_air_quality | 0.992 ± 0.007 | 0.617 ± 0.030 | 0.895 ± 0.019 | 0.001 ± 0.001 | 0.160 ± 0.061 | 0.030 ± 0.052 | 0.978 ± 0.037 | 1.000 ± 0.000 | 0.978 ± 0.038 | 0.662 ± 0.129 |
| uci_household | 0.978 ± 0.015 | 0.805 ± 0.054 | 0.937 ± 0.021 | 0.002 ± 0.001 | 0.146 ± 0.029 | 0.638 ± 0.189 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.987 ± 0.022 | 1.000 ± 0.000 |
| uci_load | 0.891 ± 0.004 | 0.790 ± 0.018 | 0.930 ± 0.008 | 0.011 ± 0.000 | 0.580 ± 0.072 | 0.195 ± 0.063 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.977 ± 0.002 | 0.987 ± 0.013 |
| uk_flood | 0.643 ± 0.018 | 0.916 ± 0.009 | 0.909 ± 0.003 | 0.055 ± 0.003 | 0.635 ± 0.055 | 0.849 ± 0.018 | 0.996 ± 0.007 | 1.000 ± 0.000 | 0.978 ± 0.014 | 0.984 ± 0.017 |
| uk_traffic | 1.000 ± 0.000 | 0.860 ± 0.085 | 0.975 ± 0.023 | 0.000 ± 0.000 | 0.422 ± 0.367 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.889 ± 0.192 | 1.000 ± 0.000 |
| usgs | 0.833 ± 0.289 | 0.750 ± 0.250 | 0.864 ± 0.236 | 0.013 ± 0.023 | 0.500 ± 0.707 (2 seeds) | 1.000 (1 seed) | n/a | 1.000 ± 0.000 (2 seeds) | 1.000 (1 seed) | 0.000 (1 seed) |

### 3.8 Test 5, the Intel-only model on live 20 s windows

24 h of each slow source from its time cut, in the live 20 s windows the slow sensors
went through before this change. There is one run: no faults are injected, since a
window of one reading has no within-window spread to size a fault by.

- `false_alarms`: the share of clean windows the Intel-only model flags.
- No window holds 4 readings or more (`min_samples`), so that model's shape features
  never see a whole series. That is why the slow sensors move to the long-window
  model.

| | windows | readings / window | false_alarms |
|---|---|---|---|
| array_of_things | 9890.000 | 1.013 | 0.005 |
| beijing | 3167.000 | 1.000 | 0.000 |
| bellinge | 11148.000 | 1.000 | 0.000 |
| chicago_beach | 375.000 | 1.000 | 0.000 |
| emergency_calls | 20160.000 | 1.000 | 0.000 |
| ett | 1152.000 | 1.000 | 0.000 |
| intel_lab | 129747.000 | 1.069 | 0.000 |
| jena | 1152.000 | 1.000 | 0.000 |
| melbourne | 8331.000 | 1.012 | 0.004 |
| nab | 1072.000 | 1.004 | 0.001 |
| noaa_tides | 2400.000 | 1.000 | 0.000 |
| sensor_community | 175376.000 | 1.024 | 0.005 |
| traffic_speeds | 3765.000 | 1.039 | 0.008 |
| uci_air_quality | 262.000 | 1.000 | 0.000 |
| uci_household | 8634.000 | 1.000 | 0.000 |
| uci_load | 33696.000 | 1.000 | 0.000 |
| uk_flood | 406113.000 | 1.005 | 0.003 |
| uk_traffic | 1920.000 | 1.000 | 0.000 |
| usgs | 864.000 | 1.000 | 0.000 |

### 3.9 Latency

`process_long_window` and `LongAnomalyDetector.predict_features` on the fullest Intel
Lab window (p50):

| window | sensors | readings | p50 | p50 per sensor-day |
|---|---|---|---|---|
| 6 h | 153 | 81,873 | 245 ms | 6.4 ms |
| 24 h | 153 | 295,497 | 696 ms | 4.5 ms |

## 4. Improvements

### 4.1 The new features

At 24 h, compared with the part 1 features alone:

- PR-AUC goes from 0.923 to 0.951.
- Recall goes from 0.827 to 0.869, at a lower false-alarm rate (0.008 against 0.011
  per sensor-day).
- Each of the four new keys, dropped in turn:
  - **`lw_flat_run_share`** carries the stuck values: without it, stuck recall falls
    from 0.643 to 0.477 and PR-AUC to 0.934.
  - **`lw_neighbour_slope_delta`** carries drift: without it, drift recall falls from
    0.734 to 0.700, false alarms rise from 0.008 to 0.014 per sensor-day, and PR-AUC
    falls to 0.939.
  - **`lw_trend_scatter_ratio`** adds nothing measurable: PR-AUC 0.953 without it at
    24 h, 0.859 against 0.861 at 6 h.

### 4.2 The window length

24 h is the only length within budget, and it has the higher PR-AUC (0.951 against
0.861). Its cost is the delay: a fault shows at most 24 h plus the job interval after
it starts, against 6 h.

At 6 h, drift recall is 0.444 (against 0.734 at 24 h). In 6 h a slow drift moves the
level little more than the noise does.

### 4.3 A bug the first run found

The first run used `PreProcess.deduplicate` on each long window. That function's
packet-order checks are made for 20 s, so it dropped the start and the end of long
windows (2,010 warnings in a smoke run of 4 sources).

`LongPreProcess.unique_readings` replaced it: one reading per sensor and timestamp.
The tests `test_a_long_window_keeps_all_its_readings` and
`test_a_restarted_packet_order_keeps_its_readings` cover it.

## 5. Caveats

- **Injected drift is fast.** It builds within one window, while real drift (Intel
  Lab's batteries) takes about 2 weeks. A drift that slow moves a 24 h window far
  less than the drift injected here.
- **No evidence on real faults** (test 2). The model flags real-fault windows barely more
  often than clean ones. Two possible reasons, neither tested:
  - Intel's faults are values out of range, which the static checks catch live.
  - A 24 h window is too short for a 2-week battery decline.
- **The neighbour slope assumes one faulty sensor per cluster at a time.** A fault
  that hits a whole cluster at once (a shared power supply, a shared calibration)
  moves every slope together and is not seen as drift.
- **Array of Things gives about 10 days** (24 h blocks every 1,801 h). It is also the
  weakest source, held out (PR-AUC 0.445) and unseen (0.372), and the furthest over
  budget.
- **The budget holds pooled, not per source** (test 1): `array_of_things` 0.099 and
  `uk_flood` 0.048 per sensor-day. As unseen sources, `bellinge` (0.083), `uk_flood`
  (0.055) and `intel_lab` (0.048) are also over.
- **Live, slow sensors get only the static checks until the DB job lands.**
  `AnomalyDetector` now scores the fast sensors only. It logs the slow ones it leaves
  out, and nothing reads 24 h back from the database yet.
- **`AnomalyDetector.is_short` assumes 20 s windows** (`sample_count /
  window_seconds`). A live buffer that flushes by count rather than by time would
  route sensors on a wrong rate. Not confirmed with Dev.
- **`sensor_community` scores the same in tests 1 and 4**, to the last digit, in
  every seed. Leaving it out seems to change nothing in the fit, so it likely has no
  training rows of its own. Not checked.
- **Some cells are small:** `usgs`, `nab`, `noaa_tides` and `emergency_calls` have a
  few test windows of some kinds; see the seed counts in the tables.
- **The settings now hold one length** (24 h). A rerun of section 9 measures 24 h
  only; add 21600.0 back to `window_seconds` to compare again.

## 6. What next

1. **A batch job reading from the database.** Every job interval it reads the last
   24 h of each slow sensor, builds the rows with `process_long_window` and runs
   `LongAnomalyDetector.from_directory("models/long_window/24h")`.
2. **History features** over weeks (a sensor against its own past), for the 2-week
   drift a 24 h window cannot see.
3. **Verdicts to monitoring**, as the short model's go.
4. Drop or rework `lw_trend_scatter_ratio`, which adds nothing measurable.
5. Look at `array_of_things` and `uk_flood`, the sources over budget: a threshold per
   source, or what their clean windows share with the faults.
