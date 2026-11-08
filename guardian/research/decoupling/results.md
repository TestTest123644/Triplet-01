# Signal decoupling: which method generalizes across sensor types? (#9)

## 1. Question

**Short answer.**

- With XGBoost, the better model here, no decoupling method stands out from the others
  beyond the noise of these runs: unseen-type PR-AUC is 0.274 (`none`), 0.287 (`fft`,
  `db4`), 0.283 (`haar`), 0.281 (`ssa`), all-types PR-AUC 0.689-0.700, and no method
  narrows the generalization gap (PR-AUC gap 0.402-0.421). With Isolation Forest the
  wavelet methods give a small lift (unseen-type PR-AUC 0.154 `db4`, 0.145 `haar`,
  against 0.123 for `none` and 0.113 for `ssa`), but Isolation Forest stays close to
  chance on these faults (all-types PR-AUC 0.107-0.141, where chance is about 0.09).
- By the ranking rule the winner is `fft`, but that is an artefact of a tie, not a
  finding: `db4` (0.2205), `haar` (0.214) and `fft` (0.2115) are within 0.01 of each other,
  and the tie is broken by a PR-AUC gap difference of 0.0005, far inside the noise (unseen
  sds 0.013-0.026 for Isolation Forest, 0.162-0.171 for XGBoost). On these faults the
  decoupling methods do not separate beyond the noise; only `none` and `ssa` fall outside
  the 0.01 band of the top three in the ranking number (0.022 and 0.0235 lower than `db4`).
- No method fixes the unseen "light" type. XGBoost's unseen-light PR-AUC is 0.059-0.064
  for every method, below the share of positives (0.083-0.084), and Isolation Forest has
  recall 0.86-0.95 at precision 0.080-0.099, so it flags most unseen-light windows (about
  73-97 % by hand arithmetic from the table, point 6). Unseen temperature and humidity are better for XGBoost
  (PR-AUC 0.365-0.377 and 0.397-0.426); Isolation Forest flags almost nothing there.
- The scores are much lower than in the previous version of this file (XGBoost all-types
  F1 0.562-0.574 against 0.854-0.861, PR-AUC 0.689-0.700 against 0.945-0.948). The
  reason (an inference, see point 1 of section 4) is that the injected faults are now
  subtle and almost always in range, sized by the normal variation inside one window.
  The earlier faults were large and 35.9 % of them were out of range (seed 0, previous
  version), so rule checks
  before the model would have caught most of them. The model is the last resort, so the
  subtle faults are the fair test.

The #9 "signal decoupling" step decomposes each sensor's raw signal into components. The
review of PR #45 asked that decoupling should not optimise for one sensor type, and that
the research use real sensor data. So the question is: **which decoupling method gives
anomaly features that still work on a sensor type the model never saw in training?**

Methods compared (each adds four numbers to the shared base features, which are
statistical, ordering and spatial-delta features):

- `none`: no decoupling, only the base features.
- `fft`: `fft_low`, `fft_mid`, `fft_high`, `fft_peak` (energy share in the low, middle and
  high third of the spectrum, and the peak frequency).
- `haar`: `haar_d1_energy`, `haar_d2_energy`, `haar_d3_energy`, `haar_d1_max_abs`
  (Haar wavelet detail energy per level, and the largest level-1 detail).
- `db4`: `db4_d1_energy`, `db4_d2_energy`, `db4_d3_energy`, `db4_d1_max_abs` (the same with
  a Daubechies-4 wavelet; smooth edge handling, so a steady trend does not look like a
  spike. The same smoothing damps a jump on the newest sample: for a 0.1 jump at the last
  of 16 readings, `db4_d1_max_abs` is 0.023 against 0.071 for `haar`; re-measured for this
  rerun with a one-off snippet).
- `ssa`: `ssa_trend`, `ssa_oscillation`, `ssa_noise`, `ssa_first_ratio` (singular
  spectrum analysis: the first three are the energy share of the first component, of
  components 2-3, and of the rest; `ssa_first_ratio` is the first singular value divided
  by the sum of all singular values, a ratio and not an energy share).

## 2. Setup

- **Data**: Intel Berkeley Research Lab, 54 motes, one reading about every 31 s with
  temperature, humidity, light and battery voltage. After dropping malformed lines, mote
  ids above 54 and the 65535 epoch artefact, 2,210,054 readings from 52 motes remain, the
  first on 2004-02-28 00:58:46 and the last on 2004-04-03 07:48:44 (counted with a
  one-off snippet that calls the loader in the data section of `research/research.ipynb`). The files are
  downloaded once to a cache outside the repo (`~/.cache/guardian_layer/intel_lab/`).
- **Three sensor types**: temperature, humidity, light. Voltage is never given to the
  model; it is only used to find real faults. A reading is a **real fault** if the voltage
  is below 2.4 V or a value is impossible (temperature outside 0-50 C, humidity outside
  0-100 %, light below 0). That is 601,491 readings, 27.2 % of the loaded readings, and
  they are concentrated late in the study when batteries die. A window counts as a real
  fault for a mote if any reading of that mote in the window is one, and then all three
  sensor types of that mote and window are marked: 69,007 of the 194,265 mote-windows,
  which is 207,021 of 582,795 sensor-windows (35.5 %). Real-fault windows are kept apart
  and are never used for training or scoring in this experiment. (All counts in this
  Setup come from a one-off snippet, not a committed script.)
- **Time from the real timestamps.** Each reading's date and time give a global sampling
  slot, `round((t - t_first) / 31 s)`, shared by all motes. The slot gives the rescaled
  timestamp (2004-02-28T00:00:00Z plus the slot number in seconds), so one real reading
  (about 31 s) becomes 1 s, and the 20 s windows hold 20 consecutive slots (about 10
  minutes of real time). Windows are non-overlapping and all sensors are together in a
  window (the spatial deltas need peers). 4,533 windows contain data. The mote's own epoch
  counter is not a clock (motes restart it); it is kept as `packet_order`, the packet
  sequence number. Mote x/y positions become lat/lon offsets.
- **Labels (binary, 1 = data wrong)**: in a seeded random 10 % of the eligible
  sensor-windows (clean, at least 4 readings: 121,488 mote-windows, 364,464
  sensor-windows), one of 6 faults is injected into that sensor's values: `stuck`, `spike`,
  `drift`, `noise`, `oscillation`, `spoof` (shapes in the data section of `research/research.ipynb`: a freeze
  from a random point; 1-3 readings jumping 5-10 spreads; an offset growing to 3-6
  spreads; extra jitter of 1-3 spreads; a sine of 2-4 spreads with a period of 4-8
  readings; the whole window shifted 3-6 spreads). An injected fault that would change no
  value (for example `stuck` on a window that is already flat) is not labelled 1: the
  seeds inject 34,998, 35,025 and 34,985 windows, while 10 % of 364,464 is 36,446 (hand
  arithmetic). Packet problems (duplicates, replays, disorder) are neither injected nor
  labelled 1: deduplication handles them before the model.
- **Fault size: subtle.** The "spread" that scales a fault is each type's median
  within-window standard deviation, taken over the clean training windows with at least 4
  readings and a non-zero std: temperature 0.0312 C, humidity 0.0836 %, light 5.58 lux
  (of 85,034 training windows with at least 4 readings, the flat ones with std 0 are
  skipped: 68 for temperature, 265 for humidity, 26,128 = 30.7 % for light). So a typical fault is a fraction of a degree, of a
  percent, or a few lux to a few tens of lux, not a jump across the sensor's range. The
  previous version scaled faults by the global std of each type (3.42 C, 6.28 %, 527 lux).
- **Out of range.** Faults that push a value outside the type's range (temperature 0-50,
  humidity 0-100, light 0-1847.36) are marked. With injection seed 0, 1,938 of 34,998
  injected windows (5.54 %) are out of range, all of them light: temperature 0 %, humidity
  0 %, light 17.93 %; seeds 1 and 2 give 5.60 % and 5.56 % overall and 17.95 % and 17.91 %
  for light. The cause is dark windows near 0 lux, where a negative offset or oscillation
  goes below 0. They stay labelled 1 in the data (the rule checks would catch them before
  the model).
- **Split: in time, at 70 % of the usable windows.** The cut is the 70 % quantile of the
  start slots of the clean windows with at least 4 readings (the windows that can carry an
  injected fault): slot 38,040, which is 2004-03-12 16:32:46 real time, 13.6 days after
  the first reading. Windows that start before it train, the rest test. ("Usable" has two meanings below: the
  cut is a quantile over the 121,488 eligible mote-windows (clean, at least 4 readings:
  85,034 before the cut and 36,454 after, 70 %), while the sensor-window counts quoted
  next cover every clean window, including those with fewer than 4 readings, so 69.4 %
  train; one-off counts.) This is not 70 %
  of the whole time range: batteries die late, in week 3 (days 21-28 after the first reading) 87.0 % of
  readings are real faults and in week 4 all of them, and a split at 70 % of raw time (2004-03-23)
  left about 60 usable test sensor-windows (20 per type) in the first rerun, which is why
  it was changed (the superseded run; one-off counts). With the quantile split there are
  260,733 usable training sensor-windows (86,911 per type) and 115,041 usable test
  sensor-windows (38,347 per type), the same for every injection seed; 9.4 % of the
  training and 9.0-9.1 % of the test sensor-windows are labelled 1 (per seed 0.0945,
  0.0941, 0.0939 and 0.0899, 0.0912, 0.0913). The test windows are from mid to late March,
  the training windows from late February to mid March, so the model is tested on a
  later period than it learned from (see the caveat on drift). The real-fault windows
  split the other way: 2,709 sensor-windows in the training period and 204,312 in the
  test period.
- **Normalization** is fixed (range scaling) for all methods. It is not part of the
  comparison.
- **Setups**: for each type T, train on the other two types (training period) and test on
  T (test period): "unseen T". Reference: train and test on all three types. The
  **generalization gap** is the all-types score minus the mean unseen-type score.
- **Models**: Isolation Forest (unsupervised; contamination set to the training positive
  share, which uses the labels) and XGBoost (`xgboost-cpu`, binary classifier,
  `scale_pos_weight` = negatives / positives, other settings default, 0.5 threshold).
  Neither is tuned.
- **Seeds**: 3 injection seeds (0-2). Isolation Forest also has 5 model seeds, so its
  all-types cells are 15 runs; XGBoost has one run per injection seed (3). "Unseen type"
  cells pool the three held-out types (45 runs for Isolation Forest, 9 for XGBoost).
  Every cell is mean ± sample sd.
- **Metrics**: precision, recall, F1 (at the model's own threshold) and PR-AUC (threshold
  free). A random ranking has a PR-AUC equal to the share of positives, here about
  0.09: 0.090-0.091 over all test sensor-windows, and per held-out type 0.092-0.098 for
  temperature and humidity and 0.083-0.084 for light (one-off counts, per seed). Recall per
  injected fault kind is in the detail tables.
- **Reproduce**: run the "Decoupling comparison" section of `research/research.ipynb`, which prints the
  tables below. Everything is seeded. This output came from the earlier script version,
  which ran for 1,372 s (about 23 minutes) with 8 parallel workers on a 16-core machine;
  the notebook runs the same tasks one after another, so it takes longer.

## 3. Results

Output of the script, pasted verbatim (cells are mean ± sample sd; a gap is the difference
of two means):

```
## isolation_forest: summary

| | all types f1 | unseen type f1 | gap f1 | all types pr_auc | unseen type pr_auc | gap pr_auc |
|---|---|---|---|---|---|---|
| none | 0.137 ± 0.010 | 0.067 ± 0.063 | 0.070 | 0.115 ± 0.004 | 0.123 ± 0.014 | -0.008 |
| fft | 0.156 ± 0.007 | 0.109 ± 0.029 | 0.046 | 0.125 ± 0.004 | 0.136 ± 0.013 | -0.011 |
| haar | 0.172 ± 0.008 | 0.065 ± 0.081 | 0.107 | 0.134 ± 0.004 | 0.145 ± 0.022 | -0.011 |
| db4 | 0.182 ± 0.010 | 0.065 ± 0.081 | 0.117 | 0.141 ± 0.005 | 0.154 ± 0.026 | -0.013 |
| ssa | 0.118 ± 0.006 | 0.067 ± 0.059 | 0.051 | 0.107 ± 0.003 | 0.113 ± 0.017 | -0.006 |

## isolation_forest: detail

| | precision | recall | f1 | pr_auc | recall_stuck | recall_spike | recall_drift | recall_noise | recall_oscillation | recall_spoof |
|---|---|---|---|---|---|---|---|---|---|---|
| none / all types | 0.132 ± 0.011 | 0.142 ± 0.010 | 0.137 ± 0.010 | 0.115 ± 0.004 | 0.045 ± 0.009 | 0.242 ± 0.022 | 0.090 ± 0.011 | 0.197 ± 0.021 | 0.159 ± 0.020 | 0.091 ± 0.005 |
| none / unseen temperature | 0.094 ± 0.013 | 0.032 ± 0.013 | 0.046 ± 0.015 | 0.106 ± 0.005 | 0.021 ± 0.014 | 0.036 ± 0.013 | 0.031 ± 0.017 | 0.036 ± 0.019 | 0.040 ± 0.015 | 0.024 ± 0.009 |
| none / unseen humidity | 0.108 ± 0.036 | 0.002 ± 0.001 | 0.004 ± 0.001 | 0.130 ± 0.008 | 0.001 ± 0.001 | 0.002 ± 0.002 | 0.001 ± 0.001 | 0.003 ± 0.002 | 0.003 ± 0.001 | 0.001 ± 0.002 |
| none / unseen light | 0.082 ± 0.001 | 0.952 ± 0.010 | 0.151 ± 0.002 | 0.133 ± 0.008 | 0.964 ± 0.020 | 1.000 ± 0.000 | 0.778 ± 0.054 | 0.994 ± 0.003 | 1.000 ± 0.000 | 0.977 ± 0.011 |
| fft / all types | 0.149 ± 0.007 | 0.163 ± 0.006 | 0.156 ± 0.007 | 0.125 ± 0.004 | 0.076 ± 0.011 | 0.317 ± 0.011 | 0.054 ± 0.011 | 0.289 ± 0.017 | 0.130 ± 0.015 | 0.088 ± 0.004 |
| fft / unseen temperature | 0.213 ± 0.052 | 0.053 ± 0.011 | 0.083 ± 0.009 | 0.137 ± 0.008 | 0.042 ± 0.015 | 0.115 ± 0.022 | 0.010 ± 0.010 | 0.114 ± 0.014 | 0.029 ± 0.013 | 0.012 ± 0.009 |
| fft / unseen humidity | 0.208 ± 0.035 | 0.065 ± 0.007 | 0.099 ± 0.010 | 0.141 ± 0.009 | 0.042 ± 0.006 | 0.141 ± 0.020 | 0.011 ± 0.003 | 0.152 ± 0.015 | 0.024 ± 0.004 | 0.022 ± 0.004 |
| fft / unseen light | 0.080 ± 0.001 | 0.885 ± 0.005 | 0.147 ± 0.002 | 0.131 ± 0.018 | 0.913 ± 0.022 | 1.000 ± 0.000 | 0.483 ± 0.028 | 0.984 ± 0.007 | 0.998 ± 0.002 | 0.930 ± 0.011 |
| haar / all types | 0.179 ± 0.011 | 0.166 ± 0.007 | 0.172 ± 0.008 | 0.134 ± 0.004 | 0.029 ± 0.004 | 0.302 ± 0.012 | 0.081 ± 0.007 | 0.245 ± 0.020 | 0.225 ± 0.021 | 0.077 ± 0.006 |
| haar / unseen temperature | 0.100 ± 0.031 | 0.009 ± 0.009 | 0.015 ± 0.013 | 0.116 ± 0.005 | 0.006 ± 0.009 | 0.013 ± 0.011 | 0.006 ± 0.007 | 0.008 ± 0.010 | 0.012 ± 0.011 | 0.008 ± 0.009 |
| haar / unseen humidity | 0.086 ± 0.009 | 0.002 ± 0.000 | 0.005 ± 0.001 | 0.164 ± 0.011 | 0.000 ± 0.000 | 0.002 ± 0.001 | 0.002 ± 0.002 | 0.004 ± 0.003 | 0.003 ± 0.001 | 0.003 ± 0.002 |
| haar / unseen light | 0.097 ± 0.017 | 0.909 ± 0.040 | 0.175 ± 0.026 | 0.154 ± 0.004 | 0.836 ± 0.105 | 1.000 ± 0.000 | 0.755 ± 0.037 | 0.994 ± 0.002 | 1.000 ± 0.001 | 0.809 ± 0.178 |
| db4 / all types | 0.186 ± 0.013 | 0.177 ± 0.009 | 0.182 ± 0.010 | 0.141 ± 0.005 | 0.030 ± 0.005 | 0.321 ± 0.014 | 0.076 ± 0.005 | 0.276 ± 0.019 | 0.241 ± 0.026 | 0.079 ± 0.005 |
| db4 / unseen temperature | 0.111 ± 0.028 | 0.009 ± 0.009 | 0.015 ± 0.014 | 0.122 ± 0.007 | 0.005 ± 0.007 | 0.015 ± 0.013 | 0.006 ± 0.009 | 0.009 ± 0.009 | 0.012 ± 0.011 | 0.007 ± 0.008 |
| db4 / unseen humidity | 0.087 ± 0.019 | 0.002 ± 0.001 | 0.004 ± 0.001 | 0.178 ± 0.014 | 0.000 ± 0.001 | 0.002 ± 0.002 | 0.001 ± 0.001 | 0.003 ± 0.002 | 0.003 ± 0.002 | 0.002 ± 0.002 |
| db4 / unseen light | 0.099 ± 0.012 | 0.863 ± 0.030 | 0.177 ± 0.019 | 0.162 ± 0.005 | 0.801 ± 0.080 | 1.000 ± 0.000 | 0.568 ± 0.020 | 0.994 ± 0.002 | 0.999 ± 0.001 | 0.759 ± 0.140 |
| ssa / all types | 0.106 ± 0.006 | 0.133 ± 0.007 | 0.118 ± 0.006 | 0.107 ± 0.003 | 0.071 ± 0.010 | 0.255 ± 0.025 | 0.053 ± 0.009 | 0.213 ± 0.022 | 0.097 ± 0.014 | 0.089 ± 0.003 |
| ssa / unseen temperature | 0.057 ± 0.017 | 0.021 ± 0.012 | 0.030 ± 0.015 | 0.115 ± 0.005 | 0.036 ± 0.017 | 0.036 ± 0.016 | 0.006 ± 0.009 | 0.026 ± 0.016 | 0.012 ± 0.012 | 0.008 ± 0.011 |
| ssa / unseen humidity | 0.090 ± 0.079 | 0.013 ± 0.005 | 0.023 ± 0.008 | 0.130 ± 0.008 | 0.003 ± 0.002 | 0.044 ± 0.016 | 0.002 ± 0.001 | 0.028 ± 0.012 | 0.002 ± 0.001 | 0.001 ± 0.001 |
| ssa / unseen light | 0.081 ± 0.001 | 0.908 ± 0.008 | 0.148 ± 0.002 | 0.094 ± 0.010 | 0.931 ± 0.019 | 1.000 ± 0.000 | 0.581 ± 0.049 | 0.990 ± 0.005 | 0.999 ± 0.001 | 0.946 ± 0.013 |
```

```
## xgboost: summary

| | all types f1 | unseen type f1 | gap f1 | all types pr_auc | unseen type pr_auc | gap pr_auc |
|---|---|---|---|---|---|---|
| none | 0.565 ± 0.005 | 0.297 ± 0.136 | 0.268 | 0.695 ± 0.007 | 0.274 ± 0.162 | 0.421 |
| fft | 0.562 ± 0.007 | 0.300 ± 0.142 | 0.262 | 0.689 ± 0.008 | 0.287 ± 0.171 | 0.402 |
| haar | 0.574 ± 0.008 | 0.299 ± 0.134 | 0.275 | 0.700 ± 0.009 | 0.283 ± 0.168 | 0.417 |
| db4 | 0.570 ± 0.003 | 0.308 ± 0.140 | 0.262 | 0.692 ± 0.006 | 0.287 ± 0.170 | 0.405 |
| ssa | 0.573 ± 0.004 | 0.309 ± 0.132 | 0.265 | 0.692 ± 0.009 | 0.281 ± 0.163 | 0.411 |

## xgboost: detail

| | precision | recall | f1 | pr_auc | recall_stuck | recall_spike | recall_drift | recall_noise | recall_oscillation | recall_spoof |
|---|---|---|---|---|---|---|---|---|---|---|
| none / all types | 0.445 ± 0.004 | 0.773 ± 0.008 | 0.565 ± 0.005 | 0.695 ± 0.007 | 0.547 ± 0.008 | 0.901 ± 0.006 | 0.961 ± 0.006 | 0.983 ± 0.001 | 0.982 ± 0.004 | 0.201 ± 0.012 |
| none / unseen temperature | 0.323 ± 0.011 | 0.703 ± 0.010 | 0.443 ± 0.013 | 0.365 ± 0.016 | 0.421 ± 0.014 | 0.750 ± 0.008 | 0.927 ± 0.005 | 0.955 ± 0.007 | 0.965 ± 0.011 | 0.170 ± 0.006 |
| none / unseen humidity | 0.195 ± 0.008 | 0.843 ± 0.012 | 0.316 ± 0.010 | 0.397 ± 0.014 | 0.771 ± 0.027 | 0.933 ± 0.026 | 0.972 ± 0.004 | 0.993 ± 0.003 | 0.986 ± 0.006 | 0.394 ± 0.052 |
| none / unseen light | 0.073 ± 0.004 | 0.696 ± 0.018 | 0.132 ± 0.007 | 0.059 ± 0.004 | 0.865 ± 0.039 | 0.520 ± 0.095 | 0.574 ± 0.123 | 0.779 ± 0.021 | 0.716 ± 0.050 | 0.839 ± 0.047 |
| fft / all types | 0.443 ± 0.006 | 0.770 ± 0.009 | 0.562 ± 0.007 | 0.689 ± 0.008 | 0.532 ± 0.008 | 0.897 ± 0.006 | 0.955 ± 0.004 | 0.982 ± 0.001 | 0.983 ± 0.004 | 0.206 ± 0.013 |
| fft / unseen temperature | 0.325 ± 0.001 | 0.693 ± 0.018 | 0.443 ± 0.003 | 0.374 ± 0.019 | 0.410 ± 0.028 | 0.750 ± 0.013 | 0.922 ± 0.012 | 0.941 ± 0.026 | 0.953 ± 0.009 | 0.157 ± 0.025 |
| fft / unseen humidity | 0.212 ± 0.008 | 0.830 ± 0.009 | 0.338 ± 0.009 | 0.426 ± 0.011 | 0.724 ± 0.048 | 0.936 ± 0.020 | 0.974 ± 0.009 | 0.993 ± 0.005 | 0.988 ± 0.002 | 0.355 ± 0.024 |
| fft / unseen light | 0.067 ± 0.006 | 0.607 ± 0.025 | 0.120 ± 0.010 | 0.061 ± 0.005 | 0.820 ± 0.061 | 0.308 ± 0.103 | 0.577 ± 0.100 | 0.665 ± 0.082 | 0.621 ± 0.083 | 0.809 ± 0.058 |
| haar / all types | 0.456 ± 0.007 | 0.775 ± 0.009 | 0.574 ± 0.008 | 0.700 ± 0.009 | 0.554 ± 0.019 | 0.902 ± 0.005 | 0.962 ± 0.003 | 0.982 ± 0.003 | 0.985 ± 0.004 | 0.204 ± 0.009 |
| haar / unseen temperature | 0.315 ± 0.008 | 0.713 ± 0.005 | 0.437 ± 0.009 | 0.368 ± 0.012 | 0.486 ± 0.026 | 0.762 ± 0.010 | 0.920 ± 0.009 | 0.952 ± 0.006 | 0.962 ± 0.011 | 0.170 ± 0.008 |
| haar / unseen humidity | 0.204 ± 0.018 | 0.843 ± 0.009 | 0.328 ± 0.023 | 0.420 ± 0.007 | 0.764 ± 0.038 | 0.938 ± 0.009 | 0.980 ± 0.011 | 0.993 ± 0.005 | 0.989 ± 0.004 | 0.386 ± 0.033 |
| haar / unseen light | 0.074 ± 0.007 | 0.680 ± 0.105 | 0.134 ± 0.013 | 0.061 ± 0.000 | 0.839 ± 0.041 | 0.452 ± 0.294 | 0.576 ± 0.029 | 0.785 ± 0.098 | 0.744 ± 0.082 | 0.802 ± 0.064 |
| db4 / all types | 0.452 ± 0.001 | 0.772 ± 0.007 | 0.570 ± 0.003 | 0.692 ± 0.006 | 0.536 ± 0.012 | 0.898 ± 0.011 | 0.961 ± 0.001 | 0.984 ± 0.001 | 0.984 ± 0.003 | 0.205 ± 0.012 |
| db4 / unseen temperature | 0.331 ± 0.006 | 0.700 ± 0.016 | 0.449 ± 0.007 | 0.376 ± 0.012 | 0.441 ± 0.016 | 0.749 ± 0.008 | 0.924 ± 0.009 | 0.943 ± 0.013 | 0.959 ± 0.008 | 0.157 ± 0.017 |
| db4 / unseen humidity | 0.215 ± 0.004 | 0.832 ± 0.004 | 0.342 ± 0.006 | 0.424 ± 0.006 | 0.741 ± 0.013 | 0.926 ± 0.035 | 0.979 ± 0.013 | 0.994 ± 0.004 | 0.985 ± 0.006 | 0.359 ± 0.012 |
| db4 / unseen light | 0.074 ± 0.004 | 0.645 ± 0.088 | 0.132 ± 0.009 | 0.062 ± 0.002 | 0.812 ± 0.044 | 0.296 ± 0.213 | 0.624 ± 0.010 | 0.750 ± 0.096 | 0.734 ± 0.090 | 0.779 ± 0.066 |
| ssa / all types | 0.457 ± 0.002 | 0.769 ± 0.006 | 0.573 ± 0.004 | 0.692 ± 0.009 | 0.532 ± 0.011 | 0.898 ± 0.008 | 0.954 ± 0.005 | 0.983 ± 0.002 | 0.982 ± 0.007 | 0.200 ± 0.010 |
| ssa / unseen temperature | 0.330 ± 0.008 | 0.698 ± 0.015 | 0.448 ± 0.009 | 0.377 ± 0.004 | 0.434 ± 0.022 | 0.742 ± 0.026 | 0.920 ± 0.017 | 0.944 ± 0.019 | 0.968 ± 0.003 | 0.155 ± 0.013 |
| ssa / unseen humidity | 0.207 ± 0.006 | 0.834 ± 0.006 | 0.332 ± 0.008 | 0.402 ± 0.005 | 0.732 ± 0.029 | 0.943 ± 0.013 | 0.965 ± 0.006 | 0.992 ± 0.003 | 0.988 ± 0.003 | 0.378 ± 0.023 |
| ssa / unseen light | 0.080 ± 0.001 | 0.789 ± 0.045 | 0.146 ± 0.002 | 0.064 ± 0.004 | 0.860 ± 0.045 | 0.812 ± 0.122 | 0.619 ± 0.068 | 0.819 ± 0.060 | 0.795 ± 0.069 | 0.872 ± 0.040 |
```

Ranking number used below (computed by hand from the two summary tables): the mean of the
two models' unseen-type PR-AUC is, for `none`, (0.123 + 0.274) / 2 = 0.1985; `fft`
(0.136 + 0.287) / 2 = 0.2115; `haar` (0.145 + 0.283) / 2 = 0.214; `db4`
(0.154 + 0.287) / 2 = 0.2205; `ssa` (0.113 + 0.281) / 2 = 0.197. Mean PR-AUC gap of the
three methods inside the tie band: `fft` (-0.011 + 0.402) / 2 = 0.1955; `db4`
(-0.013 + 0.405) / 2 = 0.196; `haar` (-0.011 + 0.417) / 2 = 0.203.

## 4. Argument

1. **The scores are much lower than in the previous version, and the likely reason is the
   size of the faults.** The previous version had XGBoost all-types F1 0.854-0.861 and
   PR-AUC 0.945-0.948, and Isolation Forest all-types F1 0.437-0.657 and PR-AUC
   0.494-0.733. Now XGBoost has F1 0.562-0.574 and PR-AUC 0.689-0.700, and Isolation Forest
   F1 0.118-0.182 and PR-AUC 0.107-0.141, about 1.2-1.6 times the chance level of 0.09.
   Two things changed together (faults sized by the within-window spread, and time built
   from the real timestamps with the split in time), and the rerun did not separate them,
   so the cause is an inference. The most plausible one: the injected faults used to be
   large (global-std spreads, 35.9 % of injected windows out of range in the previous
   version, seed 0), so most were also trivially out of range and would have been caught by the
   rule checks before the model. Now only 5.54 % are out of range (light), and a fault
   is a few within-window spreads. The model is meant to be the last resort after those
   checks, so these subtle faults are the fair test and the lower scores are the honest
   ones.
2. **XGBoost: no method stands out from the others beyond the noise.** Pooled unseen-type
   PR-AUC is 0.274 ± 0.162 (`none`), 0.287 ± 0.171 (`fft`), 0.283 ± 0.168 (`haar`),
   0.287 ± 0.170 (`db4`), 0.281 ± 0.163 (`ssa`): a spread of 0.013, against sds of
   0.162-0.171. All-types PR-AUC is 0.689-0.700 (sds 0.006-0.009) and all-types F1
   0.562-0.574 (sds 0.003-0.008). The sd of the pooled unseen column mostly reflects
   differences between the held-out types, so it is not a fair yardstick for the methods;
   per held-out type the PR-AUC ranges are temperature 0.365-0.377 (cell sds 0.004-0.019),
   humidity 0.397-0.426 (sds 0.005-0.014) and light 0.059-0.064 (sds 0.000-0.005). The
   one visible step is on humidity: `fft`, `haar` and `db4` reach 0.426, 0.420 and 0.424
   against 0.397 for `none` and 0.402 for `ssa`, a difference of 0.02-0.03 which is
   larger than those cell sds, but it rests on 3 injection seeds, appears on one held-out
   type only, and does not carry to the pooled unseen mean or to temperature and light. No
   test was run. The data shows no consistent gain from any decoupling method for
   XGBoost; it cannot rule out small per-type effects.
3. **Isolation Forest (its contamination is set from the training labels, so it is not
   label-free) is close to chance, and the wavelets lift it a little.** All-types PR-AUC
   is 0.141 `db4`, 0.134 `haar`, 0.125 `fft`, 0.115 `none`, 0.107 `ssa` (sds 0.003-0.005);
   the order is the same as in the unseen-type column (0.154, 0.145, 0.136, 0.123, 0.113),
   but there neighbouring methods are 0.009-0.013 apart against sds of 0.013-0.026. Per
   held-out type, `db4` and `haar` have a higher PR-AUC than `none` on all three:
   temperature 0.122 and 0.116 against 0.106, humidity 0.178 and 0.164 against 0.130,
   light 0.162 and 0.154 against 0.133. Those gains (0.010-0.048) are mostly larger than
   the cell sds (0.005-0.014), but in absolute terms every Isolation Forest PR-AUC is
   0.094-0.178 against a chance level of 0.083-0.098, and F1 is 0.118-0.182 for all
   types. `ssa` is the lowest on light (0.094) and equal to `none` on humidity (0.130);
   `fft` is above `none` on temperature (0.137) and humidity (0.141) and level with it on
   light (0.131 against 0.133). A possible reason for the weak result
   (not measured): the features change little for a fault of a few within-window spreads,
   and Isolation Forest, which splits on randomly chosen features, spends many splits on
   features where faults do not stand out.
4. **Winner by the rule** (rank by the mean of the two models' unseen-type PR-AUC means;
   ties within 0.01 go to the smaller mean PR-AUC gap): `db4` 0.2205 is top, `haar` 0.214
   (0.0065 lower) and `fft` 0.2115 (0.009 lower) are inside the 0.01 band, `none` 0.1985
   and `ssa` 0.197 are outside it (0.022 and 0.0235 lower). Inside the band the mean gap
   is `fft` 0.1955, `db4` 0.196, `haar` 0.203, so the rule picks **`fft`**. The margin that
   decides it is 0.0005 in gap and at most 0.009 in score, much smaller than the unseen
   sds (0.013-0.026 for Isolation Forest, 0.162-0.171 for XGBoost), and the Isolation
   Forest part of the gap is negative for every method (point 5), so it does not reward
   better transfer. The honest reading: `fft`, `haar` and `db4` are tied, and the rule's
   winner is an artefact of the tie. In the same ranking number `none` is 0.022 behind
   `db4`: the unseen column differs by 0.031 for Isolation Forest and 0.013 for XGBoost,
   which average to 0.022 (hand arithmetic from the summary tables).
5. **Generalization gap.** For XGBoost the PR-AUC gap is 0.402-0.421 (about 0.69-0.70 on
   all types falls to 0.27-0.29 on an unseen type) and the F1 gap is 0.262-0.275, with
   no method narrowing it: the methods differ by 0.019 in PR-AUC gap, below the unseen
   sds. For Isolation Forest the PR-AUC gap is -0.006 to -0.013, i.e. the unseen-type
   mean is slightly above the all-types value; since the model is close to chance on
   both, this says nothing about transfer. Possible reason for the negative sign (not
   checked): the all-types PR-AUC ranks the three types in one pool, while the unseen
   column averages three separate per-type rankings. The F1 gap for Isolation Forest is
   0.046-0.117, with large sds in the unseen column (0.029-0.081).
6. **Unseen types: the three held-out types fail differently.** XGBoost on unseen
   temperature and humidity keeps recall 0.69-0.84 but precision only 0.20-0.33, and its
   PR-AUC (0.365-0.426) is about 4 times the chance level (0.09). On unseen light it fails:
   PR-AUC 0.059-0.064 is below the share of positives (0.083-0.084), i.e. its ranking of
   light windows is no better than chance on average, with precision 0.067-0.080 and
   recall 0.61-0.79 at the 0.5 threshold. Isolation Forest fails in both directions: on
   unseen light recall is 0.863-0.952 at precision 0.080-0.099, so it flags most windows:
   flagged share = recall x positive share / precision, with a positive share of about
   0.0835, is about 97 % (`none`), 92 % (`fft`), 78 % (`haar`), 73 % (`db4`) and 94 %
   (`ssa`) (hand arithmetic from the table; `haar` and `db4` have a precision a little
   above the positive share, the others at it), while on unseen temperature and humidity it flags almost
   nothing (recall 0.002-0.065, F1 0.004-0.099). So what fails to transfer for Isolation
   Forest is mostly the threshold (inference, not tested), and for XGBoost on light also
   the ranking. Light's normal behaviour (lamps and daylight switching it between near zero
   and bright, 30.7 % of training windows with at least 4 readings are flat) differs from
   the slow temperature and humidity signals (explanation by inference; the scripts do not
   measure it). No decoupling method fixes this: unseen-light F1 is 0.120-0.146 for
   XGBoost and 0.147-0.177 for Isolation Forest.
7. **Which kinds are hard.** In the all-types setup XGBoost catches `spike` (0.897-0.902),
   `drift` (0.954-0.962), `noise` (0.982-0.984) and `oscillation` (0.982-0.985) well, `stuck`
   moderately (0.532-0.554) and `spoof` poorly (0.200-0.206). Isolation Forest is weak on
   every kind, best on `spike` (0.242-0.321) and `noise` (0.197-0.289), and at or below 0.076
   on `stuck` (0.029-0.076) and 0.091 on `spoof` (0.077-0.091). A plausible reason for
   `spoof` (inference): it shifts the whole window by 3-6 spreads (about 0.1-0.2 C for
   temperature), which leaves the within-window features unchanged and is small against
   normal differences between motes, so only the spatial deltas can show it. `stuck` also
   has the fewest labelled windows because a freeze on an already flat window is left
   unlabelled (for injection seed 0, 1,249 `stuck` test windows against 1,755-1,871 for
   the other kinds).

## 5. Caveats

- **Time rescaling**: real readings are 31 s apart; treating them as 1 s apart means a
  20 s window covers about 10 minutes of real time, so the frequency and wavelet levels
  describe real behaviour on a slow time scale, not seconds.
- **The faults are now subtle, with these numbers** (one-off counts): the spread is the
  median within-window std of the clean training windows (temperature 0.0312 C, humidity
  0.0836 %, light 5.58 lux); out of range is 5.54 % of injected windows with seed 0 (5.60 %
  and 5.56 % with seeds 1 and 2), all of it light (17.93 % of the light injections;
  temperature and humidity 0 %), counted per kind as `spike` 326, `drift` 279, `noise` 535,
  `oscillation` 518, `spoof` 280 windows, `stuck` 0. These windows would be caught by the
  rule checks before the model, yet they are in the test set. Light's spread comes from the
  non-flat windows only (30.7 % of the light training windows with at least 4 readings are
  flat), so a flat dark window gets a fault larger than its own variation, which is zero.
  Very subtle faults are not covered either: we tried one size scale (multiples of the
  median within-window std); whether a model catches faults smaller than that was not
  tested.
- **Slot rounding.** Rounding to 31 s slots puts several readings of one mote in the same
  slot: 262,279 readings (11.9 %) share a slot with another reading of the same mote
  (130,122 slot-mote pairs of 2,077,897, up to 21 readings in one pair). They keep their own
  `packet_order` (only 5 (slot, mote, epoch) triples occur twice), but the windows are not
  perfectly regular series.
- **Real-fault windows are 35.5 % of the sensor-windows** (one bad reading marks all three
  types of that mote and window), and 98.7 % of them (204,312 of 207,021) fall in the
  test period. They are set aside here, so the test windows are those of motes and days
  that still work, not a sample of the whole lab.
- **The split moves from early to late March, so drift or seasonality can enter.** Training
  windows are from 2004-02-28 to 2004-03-12 and test windows from 2004-03-12 to about
  2004-03-24 (the last clean window is on day 24.8; one-off count from the superseded run).
  Temperature, light or the lab's use may differ between the two periods; we did not
  measure it. It affects all methods the same way, so the ranking is probably unaffected
  (inference), but absolute scores include it. The split also depends on a choice (70 %
  quantile of usable windows, not 70 % of raw time, see Setup).
- **Test set size.** The test set is no longer thin: 38,347 usable test sensor-windows
  per type (115,041 in all), with about 3,200-3,800 injected positives per type and seed.
  The sds are therefore dominated by injection and model seeds, not by tiny samples.
- **The `ssa` summary cannot tell trend from oscillation.** Re-measured for this rerun
  with a one-off snippet on a 20-sample series: a linear ramp gives `ssa_trend` 0.548 and
  `ssa_oscillation` 0.452, a pure sine of period 8 gives 0.585 and 0.415. So `ssa`'s low
  place is a verdict on this 4-number summary, not on SSA itself.
- **Injected faults are still designed by us.** Their shapes and sizes (multiples of the
  type's within-window spread) are our choice, so recall per kind depends on those choices.
  Real faults are used only in the general-model check, not here.
- **One dataset, one indoor lab, three sensor types.** The results may not carry over to
  outdoor sensors, other physical quantities or other layouts.
- **Default hyperparameters** for both models, no tuning. Isolation Forest's contamination
  is set from the training labels, which an unlabelled deployment would have to estimate.
- **XGBoost's 0.5 threshold is not recalibrated for an unseen type.** F1 on an unseen
  type mixes ranking and threshold; PR-AUC is the threshold-free view.
- **The ± values cover injection and model seeds only**, not other data (the real data
  and the split are fixed). In the unseen-type column the sd also contains the differences
  between the three held-out types (45 or 9 runs pooled), so it is not a seed-noise level;
  read the per-type rows in the detail tables. XGBoost all-types cells rest on 3 runs. No
  significance test was run: "within the noise" means a difference smaller than the sd,
  nothing more.
- **Real-fault readings of one mote still enter the spatial deltas of the clean motes in
  the same window**, because the neighbour comparison uses every sensor present in the
  window.
- **The unseen-type result pools the three held-out types with seed variation.** One
  number hides large differences between types (light vs humidity, point 6).

## 6. Recommendation for `DEFAULT_METHOD`

Keep **`none`**. The ranking rule names `fft`, but the three leaders `db4` (0.2205), `haar`
(0.214) and `fft` (0.2115) are tied inside the noise and the rule's pick comes from a 0.0005
difference in gap. For XGBoost, the better model on these faults, no method stands out
(unseen PR-AUC 0.274-0.287, all-types 0.689-0.700). For Isolation Forest the wavelets give
a small lift (unseen PR-AUC 0.154 `db4` and 0.145 `haar` against 0.123), but the model
stays close to chance on subtle faults (all-types PR-AUC 0.107-0.141, chance about 0.09),
so the lift is of little practical use. `ssa` is behind `none` for Isolation Forest. None
of the methods fixes the unseen-light failure, which needs a different remedy than feature
choice (for example a recalibrated threshold, or including light in training). `none` also
adds no feature computation. If decoupling is wanted anyway, `db4` or `haar` have the
highest Isolation Forest scores here, but this is the owner's decision.

`DEFAULT_METHOD` is `none` and is **not changed by this work**. The comment above it in
`src/guardian_layer/pipelines/preprocess.py` cites these results.

## 7. Acknowledgement

Data: Intel Berkeley Research Lab, https://db.csail.mit.edu/labdata/labdata.html
