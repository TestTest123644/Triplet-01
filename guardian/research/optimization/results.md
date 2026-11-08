# Model optimization: which changes improve the anomaly model? (#64)

## 1. Question

Issue #64 asks to explore ways to improve the anomaly model's performance: sensor fusion,
normalizing the data, one model per sensor type, and decomposition with a column saying
what the sensor measures (`type`), and leaves room for other ideas. Each idea is compared
here with the shipped model of section 4 of `research/research.ipynb` (the #55 multiclass
model), one change at a time, on the same data, and the variants are then scored on the
project's own sensor types (the flood-scenario generator, as in
`research/evaluation/results.md`).

**Short answer.**

- **The biggest gain costs no feature: the threshold.** At the shipped 0.5 the model
  flags 9.6 % of the normal windows (F1 0.626 ± 0.002). A threshold picked on the last
  20 % of the training windows (about 0.94) gives F1 0.753 ± 0.003 and flags 1.4 % of
  them, but recall falls from 0.935 to 0.693 and `stuck` is almost never caught any more
  (0.511 to 0.037). A threshold does not change how the windows are ranked (PR-AUC); 0.5
  is simply too low for this model.
- **On the three Intel Lab types, most ideas help a little, and they add up.** PR-AUC
  (baseline 0.860 ± 0.001): a `type` column 0.885, one model per type 0.900, neighbour
  fusion 0.877, type-noise scaling 0.876; decomposition 0.863-0.865 (no gain beyond the
  noise, as in #9); per-sensor scaling 0.832 and window z-score 0.736 are worse.
  Combined (type noise + `type` + fusion) 0.898, and with a bigger model (300 trees of
  depth 8) 0.912 ± 0.002, with F1 0.828 ± 0.003 and 1.9 % false alarms at 0.5.
- **One model per type is not needed.** Its gain (PR-AUC 0.900, F1 0.738) is matched by
  one model with a `type` column and more trees (0.899, F1 0.798): what the split adds is
  mostly capacity.
- **On a type the model never saw, the case of the project's water level, rainfall and
  traffic sensors, only the relative features help** (noise units, fusion, z-score).
  Unseen-type PR-AUC: baseline 0.475 ± 0.209; type noise + fusion 0.684 ± 0.110 (on
  light, the type that fails, 0.197 to 0.546); window z-score 0.638 ± 0.037. The `type`
  column (0.486), the bigger model (0.470) and per-type models (which cannot score a new
  type at all) do not.
- **On the generator, no variant fixes the false alarms.** At 0.5 every variant flags 95 %
  to 99 % of the windows without a value fault, and at the picked threshold the best ones
  still flag about half (window z-score 0.496, type noise + fusion 0.550 ± 0.254). Noise
  scaling and fusion do rank the faulty windows better (PR-AUC 0.148 to 0.422, chance
  about 0.095), so the noise level found in #12 is part of the gap, not all of it.
- **Spoofing.** The `spoofed_data` rule has no signal (PR-AUC 0.009, the share of spoofed
  windows). A `spoofed_data` class in the model, with the change of the spatial delta
  since the previous window, ranks spoofed windows about 20 times better than chance
  (PR-AUC 0.206 ± 0.013), but at 0.5 its precision is 0.049: a lead, not a fix.
- None of this changes the shipped model; section 5 lists what to implement first.

## 2. Setup

- **Data, faults, split**: as in section 4 of the notebook and section 7 of
  `research/general_model/results.md`: Intel Berkeley Lab data (temperature, humidity,
  light), the five malfunction kinds and the four fabricated kinds of `TRAIN_KINDS`
  injected into 10 % of the eligible sensor-windows, sized by each type's within-window
  spread; `spoof` windows and real battery faults are kept out of the model's rows; the
  split is in time at the 70 % quantile of the usable windows. Per injection seed this
  gives about 258,000 training and 114,000 test sensor-windows (seed 0: 258,132 and
  113,981, of which 104,494 normal, 5,154 `sensor_malfunction`, 4,333
  `not_sensor_data`). 3 injection seeds (0-2); every cell is mean ± sample sd over them.
- **Model**: `BaselineModel.fit`, untuned XGBoost (100 trees, depth 6) with balanced
  class weights, the shipped settings. Only the columns change between variants, except
  in the three "bigger model" rows (300 trees, depth 8) and the "one model per type" row
  (three such models, one per type).
- **Baseline features**: `feature_keys("none")`, values scaled to their type's range
  (`(value - low) / (high - low)`), as shipped.
- **Variants**, computed from one table per seed built by `window_table` (the production
  features, checked to be identical to `process_window`'s, plus the inputs below):
  - **Normalization**: the unit the values are divided by instead of the range width.
    `type noise`: the type's normal within-window spread (median std of the normal
    training windows with at least 4 readings: temperature 0.0313 C, humidity 0.0833 %,
    light 3.67 lux for seed 0). `sensor noise`: the same median per sensor (light motes
    range from 0.25 to 16.9 lux). `window z-score`: each window's own mean and std, i.e.
    what `PreProcess` does for a sensor without a range (the level and the window's own
    variation are lost; `has_range` is 0).
  - **Decomposition**: the four features of `fft`, `haar`, `db4` or `ssa` added, as in
    the #9 research.
  - **`type` column**: an integer code of what the sensor measures (temperature 0,
    humidity 1, light 2), alone (`none + type`) and with each decomposition method.
  - **One model per type**: three models with the baseline features, each trained and
    tested on its own type; their scores are pooled for the metrics.
  - **Sensor fusion**: `nbr` compares the sensor with its same-type neighbours (the k = 5
    of the spatial step): the log ratio of its within-window std to theirs, and its slope
    minus their mean slope. `col` compares it with the other sensors at the same place
    (an Intel Lab mote measures the three types at one spot): the log ratios of its std
    and of its absolute slope to theirs, each in its type's noise unit.
  - **Bigger model**: 300 trees of depth 8 instead of 100 of depth 6, chosen from two
    settings tried on seed 0 (see the caveats).
- **Metrics** on the test windows: anomaly or not (score above the threshold) precision,
  recall, F1 and PR-AUC; `false_alarms`, the share of normal windows flagged; the F1 of
  each anomaly class the model names; and the recall of `stuck`, the weakest kind. A
  random ranking has a PR-AUC equal to the share of anomalies, about 0.08 to 0.09.
- **Threshold picked on validation**: every variant is also trained on the first 80 % of
  the training windows (in time), the threshold with the best F1 is picked on the last
  20 %, and the test windows are scored with it (`... picked` columns).
- **Unseen type**: trained on two types, tested on the third, for each type (9 runs per
  cell: 3 types x 3 seeds), as in `research/decoupling/results.md`.
- **Spoofing**: `spoof` windows get a fourth class (`spoofed_data`) and the model is
  trained with them; `delta_change` adds the change of the spatial delta since the same
  sensor's previous window. The `spoofed_data` rule (`StaticCheck`) is scored on the same
  test windows (1,060 spoofed of 115,041 for seed 0, 0.9 %).
- **Generator**: the models trained on all Intel Lab types (one per injection seed) score
  the section 5 runs of `mocks/telemetry_generator.py` (seeds 0-2, 20 sensors, 3,600 s,
  anomaly rate 0.01; 9 runs per cell), at 0.5 and at the threshold picked for that
  variant on the Intel Lab validation windows (`... picked`). Label 1 = a value fault in
  the window. The spreads of the noise scalings are estimated from each run's own windows
  without labels (about 2 % of each type's range: the generator's 1 % noise plus its
  shared wave).
- **Reproduce**: run section 7 of `research/research.ipynb` (`optimize_main()`).
  Everything is seeded. This output took 1,840 s (about 31 minutes) on a 2-CPU container
  (Python 3.12.3, XGBoost 3.4.1); a second run gave the same output byte for byte.

## 3. Results

Output of `optimize_main()`, pasted verbatim.

```
## All types

| | precision | recall | f1 | false_alarms | pr_auc | f1_sensor_malfunction | f1_not_sensor_data | recall_stuck | threshold | f1 picked | recall picked | false_alarms picked | recall_stuck picked |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 0.470 ± 0.002 | 0.935 ± 0.003 | 0.626 ± 0.002 | 0.096 ± 0.001 | 0.860 ± 0.001 | 0.430 ± 0.005 | 0.717 ± 0.003 | 0.511 ± 0.020 | 0.936 ± 0.004 | 0.753 ± 0.003 | 0.693 ± 0.010 | 0.014 ± 0.001 | 0.037 ± 0.020 |
| type noise | 0.497 ± 0.004 | 0.933 ± 0.003 | 0.649 ± 0.003 | 0.086 ± 0.001 | 0.876 ± 0.002 | 0.453 ± 0.003 | 0.746 ± 0.005 | 0.513 ± 0.022 | 0.935 ± 0.009 | 0.777 ± 0.004 | 0.726 ± 0.016 | 0.013 ± 0.002 | 0.142 ± 0.012 |
| sensor noise | 0.437 ± 0.003 | 0.920 ± 0.002 | 0.593 ± 0.003 | 0.108 ± 0.001 | 0.832 ± 0.003 | 0.402 ± 0.004 | 0.683 ± 0.013 | 0.491 ± 0.019 | 0.935 ± 0.010 | 0.733 ± 0.004 | 0.678 ± 0.022 | 0.016 ± 0.003 | 0.058 ± 0.022 |
| window z-score | 0.344 ± 0.004 | 0.892 ± 0.000 | 0.497 ± 0.004 | 0.155 ± 0.003 | 0.736 ± 0.001 | 0.268 ± 0.003 | 0.598 ± 0.001 | 0.377 ± 0.003 | 0.936 ± 0.002 | 0.658 ± 0.000 | 0.605 ± 0.001 | 0.021 ± 0.000 | 0.004 ± 0.003 |
| fft | 0.474 ± 0.006 | 0.933 ± 0.002 | 0.628 ± 0.006 | 0.095 ± 0.003 | 0.863 ± 0.003 | 0.433 ± 0.006 | 0.742 ± 0.002 | 0.500 ± 0.016 | 0.934 ± 0.004 | 0.756 ± 0.003 | 0.689 ± 0.010 | 0.012 ± 0.001 | 0.048 ± 0.013 |
| haar | 0.481 ± 0.002 | 0.935 ± 0.005 | 0.636 ± 0.002 | 0.092 ± 0.001 | 0.865 ± 0.004 | 0.441 ± 0.004 | 0.736 ± 0.007 | 0.513 ± 0.027 | 0.930 ± 0.009 | 0.761 ± 0.005 | 0.708 ± 0.021 | 0.014 ± 0.002 | 0.101 ± 0.028 |
| db4 | 0.482 ± 0.004 | 0.933 ± 0.003 | 0.636 ± 0.004 | 0.092 ± 0.002 | 0.864 ± 0.003 | 0.441 ± 0.005 | 0.742 ± 0.005 | 0.508 ± 0.020 | 0.933 ± 0.003 | 0.760 ± 0.003 | 0.706 ± 0.007 | 0.014 ± 0.000 | 0.094 ± 0.021 |
| ssa | 0.487 ± 0.001 | 0.933 ± 0.003 | 0.640 ± 0.001 | 0.090 ± 0.001 | 0.863 ± 0.002 | 0.448 ± 0.003 | 0.748 ± 0.006 | 0.501 ± 0.029 | 0.928 ± 0.004 | 0.760 ± 0.004 | 0.712 ± 0.009 | 0.015 ± 0.001 | 0.097 ± 0.017 |
| none + type | 0.510 ± 0.002 | 0.935 ± 0.004 | 0.660 ± 0.001 | 0.082 ± 0.001 | 0.885 ± 0.003 | 0.463 ± 0.007 | 0.743 ± 0.006 | 0.518 ± 0.012 | 0.933 ± 0.006 | 0.784 ± 0.005 | 0.725 ± 0.012 | 0.011 ± 0.001 | 0.147 ± 0.018 |
| fft + type | 0.515 ± 0.004 | 0.933 ± 0.002 | 0.664 ± 0.004 | 0.080 ± 0.001 | 0.886 ± 0.003 | 0.469 ± 0.008 | 0.766 ± 0.003 | 0.508 ± 0.027 | 0.930 ± 0.006 | 0.780 ± 0.005 | 0.719 ± 0.013 | 0.011 ± 0.001 | 0.147 ± 0.020 |
| haar + type | 0.517 ± 0.004 | 0.935 ± 0.002 | 0.666 ± 0.004 | 0.080 ± 0.001 | 0.889 ± 0.004 | 0.471 ± 0.009 | 0.758 ± 0.001 | 0.517 ± 0.018 | 0.921 ± 0.005 | 0.792 ± 0.004 | 0.747 ± 0.009 | 0.013 ± 0.001 | 0.213 ± 0.043 |
| db4 + type | 0.511 ± 0.003 | 0.933 ± 0.002 | 0.660 ± 0.003 | 0.082 ± 0.001 | 0.884 ± 0.003 | 0.468 ± 0.008 | 0.756 ± 0.003 | 0.510 ± 0.020 | 0.929 ± 0.006 | 0.787 ± 0.004 | 0.733 ± 0.014 | 0.012 ± 0.002 | 0.195 ± 0.047 |
| ssa + type | 0.516 ± 0.001 | 0.933 ± 0.003 | 0.665 ± 0.000 | 0.080 ± 0.000 | 0.885 ± 0.001 | 0.474 ± 0.005 | 0.768 ± 0.004 | 0.507 ± 0.022 | 0.927 ± 0.007 | 0.787 ± 0.006 | 0.734 ± 0.018 | 0.012 ± 0.001 | 0.178 ± 0.032 |
| fusion nbr | 0.490 ± 0.003 | 0.935 ± 0.003 | 0.643 ± 0.003 | 0.089 ± 0.001 | 0.877 ± 0.004 | 0.454 ± 0.005 | 0.726 ± 0.006 | 0.512 ± 0.017 | 0.933 ± 0.004 | 0.778 ± 0.002 | 0.718 ± 0.010 | 0.012 ± 0.001 | 0.065 ± 0.026 |
| fusion col | 0.478 ± 0.005 | 0.936 ± 0.002 | 0.633 ± 0.005 | 0.093 ± 0.002 | 0.867 ± 0.003 | 0.442 ± 0.008 | 0.747 ± 0.005 | 0.510 ± 0.012 | 0.926 ± 0.005 | 0.769 ± 0.001 | 0.721 ± 0.010 | 0.014 ± 0.001 | 0.055 ± 0.011 |
| fusion nbr + col | 0.493 ± 0.002 | 0.935 ± 0.001 | 0.646 ± 0.002 | 0.088 ± 0.001 | 0.880 ± 0.002 | 0.454 ± 0.003 | 0.757 ± 0.001 | 0.506 ± 0.014 | 0.931 ± 0.006 | 0.782 ± 0.001 | 0.717 ± 0.008 | 0.011 ± 0.001 | 0.065 ± 0.008 |
| one model per type | 0.621 ± 0.005 | 0.911 ± 0.002 | 0.738 ± 0.004 | 0.051 ± 0.001 | 0.900 ± 0.003 | 0.535 ± 0.009 | 0.782 ± 0.003 | 0.451 ± 0.015 | 0.920 ± 0.007 | 0.809 ± 0.005 | 0.750 ± 0.014 | 0.009 ± 0.001 | 0.199 ± 0.028 |
| type noise + fusion | 0.519 ± 0.003 | 0.933 ± 0.002 | 0.667 ± 0.003 | 0.079 ± 0.001 | 0.892 ± 0.003 | 0.480 ± 0.006 | 0.774 ± 0.005 | 0.513 ± 0.020 | 0.926 ± 0.004 | 0.797 ± 0.002 | 0.747 ± 0.002 | 0.012 ± 0.000 | 0.159 ± 0.018 |
| type noise + type + fusion | 0.530 ± 0.004 | 0.932 ± 0.003 | 0.675 ± 0.003 | 0.076 ± 0.001 | 0.898 ± 0.001 | 0.488 ± 0.005 | 0.780 ± 0.002 | 0.521 ± 0.016 | 0.928 ± 0.018 | 0.804 ± 0.004 | 0.744 ± 0.028 | 0.010 ± 0.003 | 0.175 ± 0.055 |
| baseline, bigger model | 0.688 ± 0.001 | 0.861 ± 0.001 | 0.765 ± 0.001 | 0.036 ± 0.000 | 0.880 ± 0.001 | 0.554 ± 0.003 | 0.803 ± 0.004 | 0.338 ± 0.014 | 0.847 ± 0.035 | 0.789 ± 0.002 | 0.743 ± 0.022 | 0.013 ± 0.003 | 0.157 ± 0.038 |
| none + type, bigger model | 0.739 ± 0.005 | 0.866 ± 0.003 | 0.798 ± 0.004 | 0.028 ± 0.001 | 0.899 ± 0.001 | 0.594 ± 0.010 | 0.819 ± 0.002 | 0.365 ± 0.020 | 0.873 ± 0.028 | 0.810 ± 0.005 | 0.742 ± 0.019 | 0.008 ± 0.002 | 0.198 ± 0.004 |
| type noise + type + fusion, bigger model | 0.801 ± 0.005 | 0.858 ± 0.002 | 0.828 ± 0.003 | 0.019 ± 0.001 | 0.912 ± 0.002 | 0.644 ± 0.007 | 0.850 ± 0.003 | 0.350 ± 0.020 | 0.822 ± 0.018 | 0.831 ± 0.003 | 0.769 ± 0.006 | 0.008 ± 0.001 | 0.219 ± 0.020 |

## Unseen type

| | precision | recall | f1 | false_alarms | pr_auc | f1_sensor_malfunction | f1_not_sensor_data | recall_stuck |
|---|---|---|---|---|---|---|---|---|
| baseline | 0.207 ± 0.117 | 0.836 ± 0.133 | 0.321 ± 0.159 | 0.424 ± 0.273 | 0.475 ± 0.209 | 0.194 ± 0.106 | 0.483 ± 0.032 | 0.647 ± 0.200 |
| type noise | 0.278 ± 0.174 | 0.848 ± 0.054 | 0.389 ± 0.198 | 0.375 ± 0.334 | 0.553 ± 0.203 | 0.252 ± 0.142 | 0.576 ± 0.117 | 0.562 ± 0.235 |
| sensor noise | 0.239 ± 0.132 | 0.864 ± 0.067 | 0.359 ± 0.172 | 0.399 ± 0.315 | 0.525 ± 0.235 | 0.228 ± 0.122 | 0.565 ± 0.075 | 0.590 ± 0.242 |
| window z-score | 0.219 ± 0.095 | 0.872 ± 0.026 | 0.341 ± 0.129 | 0.371 ± 0.232 | 0.638 ± 0.037 | 0.179 ± 0.094 | 0.560 ± 0.084 | 0.462 ± 0.257 |
| none + type | 0.213 ± 0.122 | 0.856 ± 0.104 | 0.328 ± 0.162 | 0.426 ± 0.282 | 0.486 ± 0.212 | 0.201 ± 0.108 | 0.513 ± 0.058 | 0.694 ± 0.191 |
| fusion nbr + col | 0.232 ± 0.120 | 0.821 ± 0.144 | 0.352 ± 0.158 | 0.329 ± 0.184 | 0.548 ± 0.215 | 0.223 ± 0.119 | 0.521 ± 0.057 | 0.587 ± 0.166 |
| type noise + fusion | 0.310 ± 0.166 | 0.865 ± 0.042 | 0.432 ± 0.178 | 0.264 ± 0.195 | 0.684 ± 0.110 | 0.287 ± 0.146 | 0.625 ± 0.094 | 0.507 ± 0.181 |
| baseline, bigger model | 0.258 ± 0.172 | 0.686 ± 0.182 | 0.353 ± 0.203 | 0.342 ± 0.292 | 0.470 ± 0.213 | 0.208 ± 0.129 | 0.524 ± 0.072 | 0.480 ± 0.255 |

## Unseen type, per held-out type

| | precision | recall | f1 | false_alarms | pr_auc | f1_sensor_malfunction | f1_not_sensor_data | recall_stuck |
|---|---|---|---|---|---|---|---|---|
| baseline / temperature | 0.339 ± 0.001 | 0.885 ± 0.012 | 0.490 ± 0.003 | 0.161 ± 0.004 | 0.607 ± 0.006 | 0.301 ± 0.002 | 0.523 ± 0.008 | 0.389 ± 0.015 |
| baseline / humidity | 0.211 ± 0.010 | 0.953 ± 0.004 | 0.346 ± 0.013 | 0.337 ± 0.009 | 0.621 ± 0.011 | 0.221 ± 0.007 | 0.469 ± 0.007 | 0.734 ± 0.036 |
| baseline / light | 0.069 ± 0.007 | 0.669 ± 0.065 | 0.125 ± 0.012 | 0.773 ± 0.033 | 0.197 ± 0.004 | 0.061 ± 0.004 | 0.457 ± 0.012 | 0.818 ± 0.054 |
| type noise / temperature | 0.481 ± 0.011 | 0.798 ± 0.015 | 0.601 ± 0.011 | 0.080 ± 0.005 | 0.715 ± 0.016 | 0.398 ± 0.010 | 0.699 ± 0.037 | 0.460 ± 0.023 |
| type noise / humidity | 0.271 ± 0.002 | 0.917 ± 0.000 | 0.419 ± 0.002 | 0.234 ± 0.006 | 0.657 ± 0.005 | 0.284 ± 0.004 | 0.435 ± 0.007 | 0.360 ± 0.014 |
| type noise / light | 0.081 ± 0.001 | 0.829 ± 0.010 | 0.147 ± 0.002 | 0.811 ± 0.013 | 0.285 ± 0.045 | 0.075 ± 0.003 | 0.593 ± 0.013 | 0.867 ± 0.067 |
| sensor noise / temperature | 0.378 ± 0.004 | 0.870 ± 0.009 | 0.527 ± 0.005 | 0.134 ± 0.003 | 0.692 ± 0.015 | 0.326 ± 0.008 | 0.641 ± 0.028 | 0.307 ± 0.027 |
| sensor noise / humidity | 0.263 ± 0.013 | 0.935 ± 0.004 | 0.410 ± 0.016 | 0.249 ± 0.008 | 0.670 ± 0.011 | 0.291 ± 0.010 | 0.474 ± 0.009 | 0.601 ± 0.013 |
| sensor noise / light | 0.077 ± 0.003 | 0.787 ± 0.036 | 0.140 ± 0.006 | 0.813 ± 0.015 | 0.212 ± 0.016 | 0.067 ± 0.002 | 0.581 ± 0.010 | 0.862 ± 0.038 |
| window z-score / temperature | 0.307 ± 0.002 | 0.879 ± 0.002 | 0.454 ± 0.002 | 0.185 ± 0.005 | 0.684 ± 0.007 | 0.254 ± 0.006 | 0.546 ± 0.008 | 0.244 ± 0.004 |
| window z-score / humidity | 0.253 ± 0.006 | 0.898 ± 0.002 | 0.394 ± 0.007 | 0.252 ± 0.001 | 0.626 ± 0.010 | 0.228 ± 0.004 | 0.471 ± 0.012 | 0.345 ± 0.036 |
| window z-score / light | 0.096 ± 0.005 | 0.839 ± 0.003 | 0.173 ± 0.008 | 0.677 ± 0.041 | 0.604 ± 0.015 | 0.055 ± 0.004 | 0.663 ± 0.008 | 0.798 ± 0.037 |
| none + type / temperature | 0.354 ± 0.004 | 0.884 ± 0.013 | 0.505 ± 0.004 | 0.151 ± 0.008 | 0.629 ± 0.003 | 0.313 ± 0.007 | 0.585 ± 0.018 | 0.443 ± 0.022 |
| none + type / humidity | 0.210 ± 0.009 | 0.960 ± 0.001 | 0.345 ± 0.013 | 0.342 ± 0.011 | 0.626 ± 0.009 | 0.223 ± 0.007 | 0.497 ± 0.008 | 0.813 ± 0.019 |
| none + type / light | 0.073 ± 0.000 | 0.726 ± 0.021 | 0.133 ± 0.001 | 0.786 ± 0.022 | 0.204 ± 0.016 | 0.067 ± 0.001 | 0.457 ± 0.013 | 0.827 ± 0.053 |
| fusion nbr + col / temperature | 0.366 ± 0.002 | 0.880 ± 0.009 | 0.517 ± 0.002 | 0.142 ± 0.005 | 0.698 ± 0.007 | 0.344 ± 0.003 | 0.588 ± 0.015 | 0.369 ± 0.016 |
| fusion nbr + col / humidity | 0.239 ± 0.008 | 0.946 ± 0.003 | 0.382 ± 0.010 | 0.286 ± 0.005 | 0.685 ± 0.010 | 0.250 ± 0.005 | 0.515 ± 0.005 | 0.679 ± 0.023 |
| fusion nbr + col / light | 0.089 ± 0.008 | 0.638 ± 0.056 | 0.157 ± 0.013 | 0.559 ± 0.033 | 0.262 ± 0.023 | 0.074 ± 0.006 | 0.460 ± 0.016 | 0.713 ± 0.047 |
| type noise + fusion / temperature | 0.504 ± 0.046 | 0.830 ± 0.032 | 0.625 ± 0.027 | 0.078 ± 0.019 | 0.786 ± 0.009 | 0.441 ± 0.023 | 0.717 ± 0.032 | 0.440 ± 0.048 |
| type noise + fusion / humidity | 0.301 ± 0.006 | 0.915 ± 0.002 | 0.452 ± 0.007 | 0.202 ± 0.003 | 0.721 ± 0.002 | 0.313 ± 0.004 | 0.507 ± 0.012 | 0.342 ± 0.026 |
| type noise + fusion / light | 0.125 ± 0.007 | 0.849 ± 0.009 | 0.217 ± 0.010 | 0.513 ± 0.039 | 0.546 ± 0.040 | 0.108 ± 0.004 | 0.650 ± 0.014 | 0.738 ± 0.021 |
| baseline, bigger model / temperature | 0.449 ± 0.003 | 0.711 ± 0.028 | 0.550 ± 0.007 | 0.081 ± 0.005 | 0.619 ± 0.006 | 0.318 ± 0.014 | 0.615 ± 0.014 | 0.169 ± 0.020 |
| baseline, bigger model / humidity | 0.272 ± 0.008 | 0.880 ± 0.007 | 0.415 ± 0.010 | 0.224 ± 0.008 | 0.605 ± 0.008 | 0.267 ± 0.008 | 0.501 ± 0.012 | 0.530 ± 0.010 |
| baseline, bigger model / light | 0.053 ± 0.005 | 0.466 ± 0.041 | 0.095 ± 0.009 | 0.722 ± 0.040 | 0.186 ± 0.011 | 0.039 ± 0.003 | 0.455 ± 0.013 | 0.742 ± 0.092 |

## Generator

| | precision | recall | f1 | false_alarms | pr_auc | recall picked | false_alarms picked |
|---|---|---|---|---|---|---|---|
| baseline | 0.079 ± 0.006 | 0.807 ± 0.064 | 0.144 ± 0.011 | 0.986 ± 0.007 | 0.148 ± 0.031 | 0.352 ± 0.106 | 0.806 ± 0.178 |
| type noise | 0.059 ± 0.004 | 0.566 ± 0.040 | 0.106 ± 0.008 | 0.953 ± 0.001 | 0.319 ± 0.036 | 0.514 ± 0.071 | 0.947 ± 0.007 |
| sensor noise | 0.061 ± 0.005 | 0.597 ± 0.038 | 0.111 ± 0.008 | 0.958 ± 0.006 | 0.293 ± 0.125 | 0.556 ± 0.047 | 0.953 ± 0.001 |
| window z-score | 0.067 ± 0.004 | 0.653 ± 0.030 | 0.121 ± 0.007 | 0.954 ± 0.002 | 0.101 ± 0.018 | 0.524 ± 0.058 | 0.496 ± 0.011 |
| type noise + fusion | 0.058 ± 0.005 | 0.559 ± 0.043 | 0.105 ± 0.008 | 0.952 ± 0.001 | 0.422 ± 0.064 | 0.513 ± 0.062 | 0.550 ± 0.254 |

## Spoofing

| | precision | recall | f1 | normal_flagged | pr_auc |
|---|---|---|---|---|---|
| model, baseline | 0.016 ± 0.001 | 0.324 ± 0.009 | 0.031 ± 0.002 | 0.204 ± 0.017 | 0.099 ± 0.002 |
| model, delta_change | 0.049 ± 0.003 | 0.605 ± 0.019 | 0.090 ± 0.005 | 0.121 ± 0.001 | 0.206 ± 0.013 |
| model, type noise + delta_change | 0.052 ± 0.002 | 0.596 ± 0.023 | 0.096 ± 0.004 | 0.111 ± 0.002 | 0.201 ± 0.019 |
| spoofed_data rule | 0.009 ± 0.001 | 0.112 ± 0.013 | 0.016 ± 0.002 | 0.124 ± 0.000 | 0.009 ± 0.000 |
```

## 4. Argument

1. **At 0.5, the threshold decides most of the F1.** Every variant of the shipped size
   gains 0.12 to 0.16 F1 when its threshold is picked on validation data (baseline 0.626
   to 0.753, type noise 0.649 to 0.777, `none + type` 0.660 to 0.784), the per-type models
   0.07 and the bigger models 0.003 to 0.024; the picked thresholds are 0.82 to 0.94,
   never 0.5. The model is trained with balanced class weights, so each class
   counts the same in total although 92 % of the windows are normal; anything unusual
   then gets a high anomaly score, which is what flags 9.6 % of the normal windows
   (inference from how the weights work; other weights were not tried). The bigger model
   is less affected: its 0.5 F1 is close to its picked F1 (0.765 against 0.789, and
   0.828 against 0.831 for the combined one). The price of a higher threshold is the
   `stuck` windows, the weakest kind: at the picked threshold their recall is 0.037 for
   the baseline and at most 0.219 for any variant. Which operating point is right
   (missing a frozen sensor against quarantining a healthy one) is a decision for the
   team, not something this data can settle.
2. **Knowing the type helps on the seen types, and per-type models are mostly
   capacity.** With seed 0, the baseline flags 16.9 % of the normal humidity windows,
   9.8 % of temperature and 2.2 % of light (one-off count), so the pooled model mixes up
   the types' normal behaviour. A `type` column raises PR-AUC from 0.860 to 0.885 and F1
   from 0.626 to 0.660. Three models, one per type, reach 0.900 and 0.738, but they have
   three times the trees: one model with a `type` column and a bigger forest (300 trees of
   depth 8) reaches 0.899 and 0.798, with fewer false alarms at 0.5 (2.8 % against 5.1 %).
   One model is also one artifact to train, ship and check. More trees without the
   `type` column give 0.880.
3. **Type-noise scaling helps; per-sensor and per-window scaling hurt on the seen
   types.** A scaling that is the same for every row cannot change XGBoost's trees (a
   split on a rescaled column finds the same rows), so only scaling per group can matter.
   Dividing by the type's normal spread puts the normal within-window variation of every
   type at about 1, so one split can serve the three types: PR-AUC 0.876 against 0.860.
   Part of that may come from how the faults are made: they are sized in multiples of
   the same spreads, so in this unit a fault looks alike in every type (caveat 1). Per
   sensor (0.832), the spreads of the light motes differ by a factor of 67 (0.25 to 16.9
   lux), while a fault is sized by the type's spread, so it shrinks on a noisy mote and
   grows on a quiet one (inference). Per window (0.736), the window's own variation
   becomes 1 and its level is lost, so a `noise` fault, which only adds variation, no
   longer shows (inference; `stuck` recall also falls, to 0.377). This is how the model
   sees a sensor without a range today.
4. **Decomposition adds nothing beyond the `type` column.** Alone, the four methods are
   at 0.863-0.865 against 0.860 (sds 0.001-0.004), which matches the #9 finding for the
   binary model. With the `type` column they are at 0.884-0.889 against 0.885 for
   `none + type`: the gain is the column's. `haar + type` is the highest (0.889 ± 0.004,
   picked F1 0.792 ± 0.004), within about one sd of the others.
5. **Fusion: the neighbours help more than the co-located types.** Comparing the sensor
   with its same-type neighbours (`nbr`) gives 0.877, with the other types at its place
   (`col`) 0.867, both 0.880; on top of type noise, 0.892 instead of 0.876. A shared
   change (a door opening, the sun) moves the neighbours too, while a fault moves one
   sensor, which is what the ratios and the slope difference measure (inference; the
   gain per fault kind was not broken down beyond `stuck`). Their largest effect is on
   unseen types (next point).
6. **Unseen types need features that do not depend on the type.** The `type` column can
   only describe types seen in training (unseen PR-AUC 0.486 against 0.475), and the
   bigger model learns the seen types better, not the new one (0.470). What transfers is
   what is relative: type noise + fusion is the best on held-out temperature (0.786) and
   humidity (0.721) and second on light (0.546), against 0.607, 0.621 and 0.197 for the
   baseline; window z-score, poor on the seen types, is the best on light (0.604).
   Light is the type that fails, as in `research/decoupling/results.md`: its normal
   behaviour differs (lamps and daylight; 25 % of its normal training windows are flat,
   one-off count), and in the relative units it looks more like the other two. Even
   then, unseen light flags 51 % of its normal windows at 0.5 (type noise + fusion), so
   a new type would need its own threshold. The pooled sds (0.04-0.24) mostly reflect the
   differences between the three held-out types.
7. **The generator gap is in what normal looks like, which features do not fix.** At 0.5,
   95.2 % to 98.6 % of the generator's windows without a value fault are flagged by every
   variant. Type noise (0.319) and type noise + fusion (0.422) rank the faulty windows
   two to three times better than the baseline (0.148; chance about 0.095), and the
   picked threshold brings the flagged share of normal windows down to 0.496 (window
   z-score) and 0.550 ± 0.254 (type noise + fusion) at a recall near 0.5. A plausible
   reason why the windows still look faulty (inference, not measured): every generator
   sensor follows its type's shared wave, which moves it by up to about 4 of its spreads
   inside one 20 s window (0.2 of the range x 2π / 300 s x 20 s, about 8 % of the range,
   against a spread of about 2 %, hand arithmetic). In the Intel Lab data that is what an
   injected `drift` looks like, while normal windows rarely move that much. The neighbour
   features see that the wave is shared, which fits their giving the best ranking here;
   that the scores stay high suggests the model still relies on the sensor's own slope
   (not checked).
8. **Spoofing has a lead but no fix.** The `spoofed_data` rule flags 11.2 % of the
   spoofed windows and 12.4 % of the normal ones: no signal, as in
   `research/general_model/results.md` section 7. With a `spoofed_data` class, the model
   on the baseline features already ranks spoofed windows ten times better than chance
   (PR-AUC 0.099 against a 0.009 share), and `delta_change` doubles that (0.206 ± 0.013)
   and its recall (0.324 to 0.605). But at 0.5 it names 12.1 % of the normal windows
   spoofed, so its precision is 0.049. A reading of these numbers (not tested): a spoof
   shifts one window by 3 to 6 spreads, which `delta_change` sees as a jump from the
   previous window, while in the spatial delta itself it is hidden by the natural
   differences between neighbours.

## 5. Recommendations

In order of value for the effort; none of them is implemented here.

1. **Pick the threshold on validation data and ship it with the model.** The training
   section can hold out its last 20 % of windows and save the picked threshold in
   `meta.json`, which already has a `threshold` field. First the team has to choose the
   operating point, because of the `stuck` recall (point 1).
2. **Add type-noise scaling and the neighbour fusion features to `PreProcess`.** They
   help on the seen types (0.860 to 0.892) and on a new one (0.475 to 0.684), which is
   the project's case. This needs the per-type spread stored with the model, a way to
   estimate it for a new type from its first normal period, and two new spatial keys
   (`EXPECTED_KEYS`, `config/settings.yaml`, tests). The co-located features add little
   (+0.007) and need several types on one spot, so they can wait.
3. **Keep one model, not one per type.** Add the `type` column only once the project's
   own types are in the training data: before that it does not help the types that
   matter. A bigger model should be tuned on validation data (not done here) and its
   latency checked against the 0.4-4.1 ms measured in `research/evaluation/results.md`.
4. **Do not switch to per-sensor or per-window scaling, and keep `none`.** Both scalings
   are worse, and decomposition adds nothing beyond the `type` column.
5. **Follow up on spoofing with `delta_change`.** It is the first feature with a signal,
   for example as a model class with its own threshold, or a rule on `delta_change` in
   noise units. It needs the previous window's spatial delta per sensor, i.e. state
   across windows, which `PreProcess` avoids on purpose.
6. **The generator gap needs data, not features**: normal behaviour like that of the
   project's sensors (recordings of the deployed sensors, or a generator calibrated on
   them). None of the variants closes it.

## 6. Caveats

1. **Fault sizes favour the type-noise unit.** Faults are injected in multiples of each
   type's within-window spread, and the type-noise scaling divides by a similar spread,
   so in that unit a fault looks alike in every type by construction. Real fault sizes
   are unknown.
2. **The bigger model was chosen on test data.** Two settings were tried on seed 0 with
   the `type` column (300 trees of depth 6 and of depth 8) before the 3-seed run, and the
   deeper one kept; it was not tuned on validation data, so its rows are slightly
   optimistic.
3. **The picked threshold maximizes F1** on the validation windows. Another target (a
   false-alarm budget, a minimum recall) gives other numbers. Those models are trained on
   the first 80 % of the training windows only, so the main columns (full training set,
   0.5) and the `picked` columns come from different models.
4. **XGBoost's results depend a little on the machine.** This run (2 CPUs, Python 3.12.3,
   XGBoost 3.4.1) gives the seed-0 baseline F1 0.627 and PR-AUC 0.861 where
   `research/general_model/results.md` section 7 has 0.631 and 0.863, on the same data
   (identical test class counts); multithreaded training sums in a different order.
5. **Co-located fusion needs several types on one spot.** True in the Intel Lab (each mote
   measures three types), not in the generator, where each sensor is alone and those
   features are missing.
6. **The generator's spreads come from the scored windows themselves**, without labels
   (medians, so the 10 % of faulty windows move them little). A deployment would estimate
   them from a first period of a sensor's data.
7. **`delta_change` needs the same sensor's previous window**: state across windows,
   which the stateless `PreProcess` does not keep. Spoof is a model class only in the
   spoofing table; everywhere else spoof windows are left out, as in section 4.
8. **Unseen-type cells pool the three held-out types** (9 runs), so their sd mostly
   reflects the types; read the per-type rows. Unseen-type and spoofing rows use the 0.5
   threshold only.
9. **The ± values cover the 3 injection seeds** (9 runs for the generator and unseen-type
   cells); no significance test was run. One indoor lab, three sensor types.

## 7. Acknowledgement

Data: Intel Berkeley Research Lab, https://db.csail.mit.edu/labdata/labdata.html
