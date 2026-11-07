# One anomaly model for all sensor types: which model, and does it see real faults? (#9)

## 1. Question

**Short answer.**

- Since #55 the shipped model is multiclass and names the anomaly; see section 7. The
  numbers below describe the earlier binary model.
- One pooled model for all three sensor types, tested on subtle injected faults: XGBoost
  is best (F1 0.565 ± 0.005 and PR-AUC 0.695 ± 0.007 with `none`; 0.562 ± 0.007 and
  0.689 ± 0.008 with `fft`), then logistic regression (F1 0.275-0.290, PR-AUC
  0.303-0.348). Isolation Forest and LOF are close to chance (PR-AUC 0.105-0.125, where
  a random ranking gets about 0.09).
- The scores are much lower than in the previous version of this file (XGBoost F1
  0.854-0.857, PR-AUC 0.946-0.948). The injected faults are now subtle and almost always
  in range (sized by the normal variation inside one window), whereas the earlier faults
  were large and many were out of range, so rule checks before the model would have caught
  them. The model is the last resort, so the subtle faults are the fair test (this
  cause is an inference; see point 1 of section 4).
- Without labelled faults to train on, no model here is good: Isolation Forest (whose
  contamination setting still uses the training labels) and LOF barely beat chance on
  injected faults, so the earlier advice "Isolation Forest with `haar`" does not hold on
  subtle faults.
- Real-fault check: no model flags more than 57.2 % of the real battery-fault windows.
  LOF flags the most (52.7 % with `none`, 57.2 % with `fft`) with 12.8-13.4 % false alarms;
  XGBoost flags 39.3 % (`none`) and 32.6 % (`fft`) with 9.6-9.7 % false alarms; Isolation
  Forest flags 24.6 % (`none`) and 29.4 % (`fft`) with 9.3 %; logistic regression flags
  22.6 % (`none`) and 23.2 % (`fft`) while flagging 34.6 % and 32.0 % of normal windows.

The review of PR #45 asked for one binary model, not one model per sensor type: it detects
a single thing, a sensor giving abnormal results (label 0 = data correct, 1 = data wrong),
and it is the **last resort** after deduplication and the rule checks. Which model fits
that role on real sensor data, and does it notice real faults it was never trained on?

## 2. Setup

- **Data, labels, split, seeds**: identical to `research/decoupling/results.md`, where
  they are described in full. In short: Intel Berkeley Research Lab data (2,210,054
  readings from 52 motes, 2004-02-28 to 2004-04-03), three sensor types (temperature,
  humidity, light). Time comes from each reading's real timestamp, cut into ~31 s
  sampling slots shared by all motes and rescaled so one slot is 1 s; the 20 s windows
  hold 20 consecutive slots and the mote's own epoch counter is kept as `packet_order`.
  Faults of 6 kinds (`stuck`, `spike`, `drift`, `noise`, `oscillation`, `spoof`) are
  injected into a random 10 % of the eligible sensor-windows without a real fault (label 1;
  packet problems are never labelled 1). Fault sizes are multiples of each type's median
  within-window standard deviation over clean training windows (temperature 0.0312 C,
  humidity 0.0836 %, light 5.58 lux), so they are subtle; 5.54 % of injected windows with
  seed 0 are out of range (light only). The split is in time at the 70 % quantile of the
  usable windows, 2004-03-12 16:32:46 real time: 260,733 training and 115,041 test
  sensor-windows (9.4 % and 9.0-9.1 % labelled 1; one-off counts, see the decoupling
  file). 3 injection seeds (0-2).
- **Decoupling methods run**: `fft`, the winner of the decoupling results
  (`research/decoupling/results.md`) by their ranking rule (a tie with `haar` and `db4`;
  that file finds no method standing out for XGBoost and a small lift from the wavelets
  for Isolation Forest), and `none`,
  the current `DEFAULT_METHOD`. Normalization is fixed (range scaling).
- **Models** (all untuned, trained on all three types together):
  - Isolation Forest: unsupervised; contamination = training positive share (this uses
    the labels); 5 model seeds.
  - LOF (local outlier factor, `novelty=True`): unsupervised; features standardized;
    fitted on a random subsample of 20,000 training rows (a different subsample per
    seed), contamination = positive share of the subsample (this also uses the labels);
    5 model seeds.
  - XGBoost (`xgboost-cpu`): supervised, `scale_pos_weight` = negatives / positives,
    other settings default, 0.5 threshold; one run per injection seed.
  - Logistic regression: supervised, features standardized,
    `class_weight="balanced"`, 0.5 threshold; one run per injection seed.
- **Metrics** on the injected-fault test windows: precision, recall, F1, PR-AUC, and recall
  per fault kind. Cells are mean ± sample sd over the runs (15 for Isolation Forest and
  LOF, 3 for XGBoost and logistic regression). A random ranking has a PR-AUC equal to the
  share of positives in the test windows, 0.090-0.091.
- **Real-fault check**: the models above are trained as usual (real-fault windows are not
  in training), then score the **real-fault sensor-windows** (any period; 207,021 of the
  582,795 sensor-windows, 2,709 in the training period and 204,312 in the test period,
  counted with a one-off snippet). `real_faults_flagged` is the share of those windows
  predicted 1. `false_alarms` is the share of **normal test windows** (test period, no real
  fault, no injected fault) predicted 1.
- **Reproduce**: run the "General-model exploration" section of `research/research.ipynb`, with `METHOD` set
  to `"fft"` for the first table and left at `DEFAULT_METHOD` (`none`) for the second.
  Everything is seeded. The earlier script version took 660 s (about 11 minutes) for `fft`
  and 1,313 s (about 22 minutes) for `none` on a 16-core machine (3 parallel workers, one
  per injection seed); the notebook runs the seeds one after another, so it takes longer.

## 3. Results

Output of the two runs, pasted verbatim.

```
decoupling method: fft

## Injected faults, all types

| | precision | recall | f1 | pr_auc | recall_stuck | recall_spike | recall_drift | recall_noise | recall_oscillation | recall_spoof |
|---|---|---|---|---|---|---|---|---|---|---|
| isolation_forest | 0.149 ± 0.007 | 0.163 ± 0.006 | 0.156 ± 0.007 | 0.125 ± 0.004 | 0.076 ± 0.011 | 0.317 ± 0.011 | 0.054 ± 0.011 | 0.289 ± 0.017 | 0.130 ± 0.015 | 0.088 ± 0.004 |
| lof | 0.117 ± 0.004 | 0.177 ± 0.008 | 0.141 ± 0.005 | 0.111 ± 0.001 | 0.156 ± 0.010 | 0.205 ± 0.010 | 0.160 ± 0.009 | 0.232 ± 0.021 | 0.158 ± 0.009 | 0.143 ± 0.006 |
| xgboost | 0.443 ± 0.006 | 0.770 ± 0.009 | 0.562 ± 0.007 | 0.689 ± 0.008 | 0.532 ± 0.008 | 0.897 ± 0.006 | 0.955 ± 0.004 | 0.982 ± 0.001 | 0.983 ± 0.004 | 0.206 ± 0.013 |
| logistic_regression | 0.182 ± 0.002 | 0.713 ± 0.009 | 0.290 ± 0.001 | 0.303 ± 0.001 | 0.063 ± 0.001 | 0.794 ± 0.006 | 0.973 ± 0.002 | 0.989 ± 0.001 | 0.976 ± 0.004 | 0.309 ± 0.007 |

## Real battery faults

| | real_faults_flagged | false_alarms |
|---|---|---|
| isolation_forest | 0.294 ± 0.020 | 0.093 ± 0.002 |
| lof | 0.572 ± 0.017 | 0.134 ± 0.003 |
| xgboost | 0.326 ± 0.012 | 0.097 ± 0.002 |
| logistic_regression | 0.232 ± 0.002 | 0.320 ± 0.005 |
```

```
decoupling method: none

## Injected faults, all types

| | precision | recall | f1 | pr_auc | recall_stuck | recall_spike | recall_drift | recall_noise | recall_oscillation | recall_spoof |
|---|---|---|---|---|---|---|---|---|---|---|
| isolation_forest | 0.132 ± 0.011 | 0.142 ± 0.010 | 0.137 ± 0.010 | 0.115 ± 0.004 | 0.045 ± 0.009 | 0.242 ± 0.022 | 0.090 ± 0.011 | 0.197 ± 0.021 | 0.159 ± 0.020 | 0.091 ± 0.005 |
| lof | 0.113 ± 0.003 | 0.163 ± 0.005 | 0.134 ± 0.004 | 0.105 ± 0.001 | 0.192 ± 0.007 | 0.327 ± 0.015 | 0.125 ± 0.008 | 0.103 ± 0.008 | 0.098 ± 0.007 | 0.142 ± 0.006 |
| xgboost | 0.445 ± 0.004 | 0.773 ± 0.008 | 0.565 ± 0.005 | 0.695 ± 0.007 | 0.547 ± 0.008 | 0.901 ± 0.006 | 0.961 ± 0.006 | 0.983 ± 0.001 | 0.982 ± 0.004 | 0.201 ± 0.012 |
| logistic_regression | 0.170 ± 0.001 | 0.711 ± 0.009 | 0.275 ± 0.001 | 0.348 ± 0.004 | 0.047 ± 0.003 | 0.731 ± 0.005 | 0.986 ± 0.004 | 0.992 ± 0.003 | 0.991 ± 0.003 | 0.341 ± 0.006 |

## Real battery faults

| | real_faults_flagged | false_alarms |
|---|---|---|
| isolation_forest | 0.246 ± 0.032 | 0.093 ± 0.004 |
| lof | 0.527 ± 0.012 | 0.128 ± 0.002 |
| xgboost | 0.393 ± 0.008 | 0.096 ± 0.001 |
| logistic_regression | 0.226 ± 0.003 | 0.346 ± 0.005 |
```

Cross-check: the injected-fault rows for Isolation Forest and XGBoost reproduce the
"all types" rows of `research/decoupling/results.md` (for example `fft` Isolation Forest
F1 0.156 ± 0.007, PR-AUC 0.125 ± 0.004; `none` XGBoost F1 0.565 ± 0.005).

## 4. Argument

1. **XGBoost is the best model on injected faults, at a much lower level than before.**
   F1 0.565 ± 0.005 (`none`) and 0.562 ± 0.007 (`fft`), PR-AUC 0.695 and 0.689, precision
   0.445 and 0.443, recall 0.773 and 0.770. The next best, logistic regression, has
   F1 0.275 ± 0.001 (`none`) and 0.290 ± 0.001 (`fft`) and PR-AUC 0.348 ± 0.004 and
   0.303 ± 0.001. Isolation Forest has F1 0.137 and 0.156 and PR-AUC 0.115 and 0.125; LOF
   has F1 0.134 and 0.141 and PR-AUC 0.105 and 0.111: both are 1.2-1.4 times the chance
   level (0.09). The previous version had XGBoost at F1 0.854-0.857 and PR-AUC
   0.946-0.948, logistic regression at F1 0.746 and Isolation Forest at F1 0.521-0.657.
   Two things changed together (faults sized by within-window spread, and real
   timestamps with the split in time), and the rerun did not separate them, so the cause is
   an inference: the earlier faults were large (35.9 % of injected windows out of range in
   the previous version, seed 0), so many were trivially caught by range rules before the model;
   now a fault is a few within-window spreads (for temperature about 0.03-0.3 C), which
   is what a last-resort model has to find. LOF and Isolation Forest were not tuned and
   LOF is fitted on a subsample, so this says "not useful with default settings here",
   not "cannot work".
2. **Which kinds each model misses.** XGBoost catches `spike` (0.901 / 0.897 for `none` /
   `fft`), `drift` (0.961 / 0.955), `noise` (0.983 / 0.982) and `oscillation` (0.982 /
   0.983) well, `stuck` about half (0.547 / 0.532) and `spoof` poorly (0.201 / 0.206).
   Logistic regression, a linear model, catches `drift`, `noise` and `oscillation` (0.973
   or more) and `spike` (0.731 / 0.794), misses `stuck` (0.047 / 0.063) and is weak on
   `spoof` (0.341 / 0.309). Isolation Forest is weak on every kind: best on `spike` (0.242 /
   0.317) and `noise` (0.197 / 0.289), at or below 0.091 on `stuck` (0.045 / 0.076),
   `drift` (0.090 / 0.054) and `spoof` (0.091 / 0.088). LOF is between 0.098 and 0.327 on
   every kind. A plausible reason for `spoof` (inference): it shifts the whole window by
   3-6 spreads, which leaves the within-window features unchanged and is small against
   normal differences between motes.
3. **Decoupling (`fft` vs `none`) changes little.** XGBoost: F1 0.562 vs 0.565 and PR-AUC
   0.689 vs 0.695, within the noise of these runs (sds 0.005-0.008). The two unsupervised
   models gain slightly with `fft`: Isolation Forest F1 0.137 to 0.156 (sds 0.010 and
   0.007) and PR-AUC 0.115 to 0.125 (sd 0.004), LOF F1 0.134 to 0.141 and PR-AUC 0.105 to
   0.111 (sd 0.001); both remain close to chance. Logistic regression is mixed: F1 rises
   from 0.275 to 0.290 while PR-AUC falls from 0.348 to 0.303 (sds 0.001-0.004). No test
   was run. This matches the decoupling file, where no method stands out for XGBoost.
4. **Supervised vs unsupervised.** XGBoost and logistic regression need labelled faults,
   which real deployments rarely have, and they can only recognise fault shapes in their
   training labels. Isolation Forest and LOF need no labels for the model itself, but here
   both take their contamination from the training labels (a real deployment has to
   estimate it), and on subtle faults they are close to chance. The trade-off also shows
   on a sensor type not seen in training (decoupling file): XGBoost's F1 falls to
   0.297-0.309 (PR-AUC 0.274-0.287, with unseen light below chance), and Isolation Forest's
   to 0.065-0.109, so neither is safe on a new type without a threshold check.
5. **What the real-fault check shows.** The models never saw these windows (98.7 % of
   them are in the test period, 204,312 of 207,021), and none flags 60 % of them.

   | model (`fft` / `none`) | real faults flagged | false alarms |
   |---|---|---|
   | Isolation Forest | 0.294 ± 0.020 / 0.246 ± 0.032 | 0.093 ± 0.002 / 0.093 ± 0.004 |
   | LOF | 0.572 ± 0.017 / 0.527 ± 0.012 | 0.134 ± 0.003 / 0.128 ± 0.002 |
   | XGBoost | 0.326 ± 0.012 / 0.393 ± 0.008 | 0.097 ± 0.002 / 0.096 ± 0.001 |
   | Logistic regression | 0.232 ± 0.002 / 0.226 ± 0.003 | 0.320 ± 0.005 / 0.346 ± 0.005 |

   (restated from the output above.) A model that flagged windows at random would flag
   the same share of real-fault and normal windows. Logistic regression flags a smaller
   share of real faults (0.232, 0.226) than of normal windows (0.320, 0.346), so it is
   worse than random on this check. Isolation Forest flags 0.294 / 0.246 of real faults
   against 0.093 of normal windows, 3.2 and 2.6 times as many; its false-alarm share is
   close to its contamination setting (the training positive share, about 0.094), which
   is consistent with it flagging about that share of any windows. XGBoost flags 3.4 and
   4.1 times as many real faults as normal windows, LOF 4.3 and 4.1 times (hand ratios
   from the table). LOF, the worst model on injected faults, catches the most real faults;
   a reading consistent with this (not tested) is that real faults include impossible
   values far from normal in feature space, which a density method flags whatever the
   shape of the fault, while XGBoost learned the shapes we injected. We also did not
   split the real-fault windows by what makes them faulty (an impossible value, or only
   low voltage with plausible values), so we cannot say which of them the models miss;
   windows with only low voltage may look normal in the three values the model sees. The
   sds of this check are small because the real-fault windows are fixed and only the
   training set changes with the seed (injected training values and labels, and the
   model seed); the `fft` / `none` differences for
   XGBoost (0.326 vs 0.393) and LOF (0.572 vs 0.527) are larger than those sds, but they
   do not tell how the models would do on other data.
6. **Reading of the evidence.** For injected faults with labels available, XGBoost is the
   clear choice, but at F1 about 0.56 it misses a quarter of the faults and flags many
   normal windows (precision about 0.44). Without labelled faults, neither Isolation Forest
   nor LOF is useful on subtle faults here, and no model covers real battery faults well
   (at most 57.2 % flagged). Part of the real-fault windows are impossible values: 86,176
   of the 207,021 real-fault sensor-windows (41.6 %) contain an out-of-range value of
   their own type (counted with a one-off snippet). A range rule before the model would
   remove those, which fits the model being the last resort. The flagged share was not
   computed per group (that would need a rerun). A second stage that combines an
   unsupervised and a supervised model is a possible design; it was not tested here.

## 5. Next steps not run here

None of these is implemented or measured. The costs are rough reasoning, not benchmarks.

- **Time-series foundation models** (Chronos, TimesFM, MOMENT), as feature extractors
  (an embedding per window fed to Isolation Forest) or as zero-shot detectors (forecast
  error as the anomaly score).
  - Adds: pretrained on many series, so less per-type tuning; may catch temporal patterns
    the hand-built features miss (`stuck`, slow `drift`).
  - Costs: a deep-learning dependency and model weights, more compute per window than the
    current features (to be measured), harder to explain.
  - Worth trying when the current features plateau, especially for `stuck`, and when a GPU
    or latency budget allows.
- **Self-supervised pretraining**: an autoencoder or masked-reconstruction model trained
  on unlabelled normal windows; anomaly = high reconstruction error.
  - Adds: learns normal behaviour without labels, so it can flag fault kinds nobody
    listed; one model for all types when inputs are normalized.
  - Costs: a training pipeline, a deep-learning dependency (a small model is much lighter
    than a foundation model), tuning of model size and threshold, drift monitoring.
  - Worth trying when there is a large archive of unlabelled normal telemetry and
    Isolation Forest on the hand-built features stops improving; it is now close to
    chance on subtle faults. The Intel Lab data (more than 2 million readings) could be
    used to try it.

## 6. Caveats

- **Time rescaling**: real readings are 31 s apart, treated as 1 s apart; a 20 s window
  covers about 10 minutes of real time.
- **Injected faults are designed by us**; their shapes and sizes set what each model can
  learn, especially the supervised ones, which see the same fault shapes in training and
  test.
- **The faults are subtle, with these numbers** (one-off counts): the spread is the median
  within-window std of the clean training windows (temperature 0.0312 C, humidity 0.0836 %,
  light 5.58 lux); with seed 0, 5.54 % of injected windows are out of range, all of it
  light (17.93 % of the light injections; temperature and humidity 0 %), where dark windows
  near 0 lux go negative. Light's spread comes from the non-flat windows only (30.7 % of
  the light training windows with at least 4 readings are flat), so a flat dark window gets
  a fault larger than its own variation. We tried one size scale only; whether the models
  catch even smaller faults was not tested.
- **Slot rounding**: 11.9 % of readings (262,279) share a 31 s slot with another reading
  of the same mote; they keep their own `packet_order` (only 5 (slot, mote, epoch)
  triples occur twice), but windows are not perfectly regular series.
- **Real faults are 35.5 % of the sensor-windows** (one bad reading marks all three types
  of that mote and window), almost all in the test period (98.7 %). The real-fault check
  therefore mostly scores late-March and April windows, while the models train on
  late-February to mid-March windows.
- **The split moves from early to late March, so drift or seasonality can enter.** Training
  windows are from 2004-02-28 to 2004-03-12 and test windows from 2004-03-12 to about
  2004-03-24. We did not measure how much the distributions differ; absolute scores
  include it, and it probably affects the models alike (inference). The split also depends
  on a choice (70 % quantile of usable windows, not 70 % of raw time, which left about 60
  usable test windows; see the decoupling file).
- **One dataset, one indoor lab, three sensor types.**
- **Default hyperparameters** for every model, no tuning. LOF is fitted on a 20,000-row
  subsample, so it sees 7.7 % of the 260,733 training rows.
- **Real-fault windows are not labelled by kind.** The rule that finds them (voltage below
  2.4 V or an impossible value) uses voltage, which the model never sees, so some of them
  may look normal in the three values the model does see.
- **The ± values cover injection and model seeds only**, not other data; no significance
  test was run, so "within the noise" means a difference smaller than the sd. For the
  real-fault check the seeds change only the training set (injected training values and
  labels, and the model seed), since the real-fault windows are fixed, so those sds are
  small.
- **Test set size**: 115,041 usable test sensor-windows (38,347 per type), 10,347, 10,491
  and 10,500 of them labelled 1 for seeds 0, 1 and 2, so the test set is not thin; the
  sds come from the injection and model seeds. For the real-fault check, `false_alarms`
  rest on about 104,500-104,700 normal test sensor-windows per seed (115,041 minus the
  injected ones; hand arithmetic).
- **Real-fault readings of one mote still enter the spatial deltas of the clean motes in
  the same window.**
- **Unseen-type numbers** (quoted from the decoupling file) pool the three held-out types
  with seed variation, so their sds mostly reflect differences between types.
- The threshold of XGBoost and logistic regression is the default 0.5; false alarms and
  flagged shares depend on it and on the contamination setting of Isolation Forest and LOF.

## 7. Update for #55: the model names the anomaly

**What changed.** The shipped model is multiclass: normal, `sensor_malfunction`
(`stuck`, `spike`, `drift`, `noise`, `oscillation`) and `not_sensor_data` (four new
injected kinds that replace the window with an in-range synthetic stream: `sine`,
`ramp`, `pattern`, `uniform`). The anomaly score is 1 - P(normal), so the threshold keeps
its meaning. `spoof` windows are kept out of the model's training and test rows; the
`spoofed_data` static check is scored on them instead. Sections 2 to 6 above still
describe the binary model and were not rerun.

**Setup.** Same data, time split, features (`none`) and injection seed 0 as section 2,
one run, untuned XGBoost with balanced class weights, threshold 0.5. Injected windows
are about 10 % of the eligible sensor-windows, split over ten kinds. Numbers are the
`metrics.json` and printed output of section 4 of the notebook, run on Python 3.13.5.
The test set holds 104,494 normal, 5,154 `sensor_malfunction` and 4,333
`not_sensor_data` windows (the row sums of the confusion matrix).

### Anomaly or not (score above the threshold)

| | precision | recall | f1 | pr_auc |
|---|---|---|---|---|
| all injected kinds except spoof | 0.476 | 0.939 | 0.631 | 0.863 |

### Per class (the class the model names, after the threshold)

| | precision | recall | f1 |
|---|---|---|---|
| normal | 0.994 | 0.906 | 0.948 |
| sensor_malfunction | 0.309 | 0.755 | 0.439 |
| not_sensor_data | 0.611 | 0.866 | 0.717 |

### Share of each injected kind flagged as an anomaly

| stuck | spike | drift | noise | oscillation | sine | ramp | pattern | uniform |
|---|---|---|---|---|---|---|---|---|
| 0.528 | 0.901 | 0.964 | 0.985 | 0.992 | 0.995 | 0.994 | 0.978 | 1.000 |

### Confusion matrix (rows = truth, columns = named class)

| truth \ named | normal | sensor_malfunction | not_sensor_data |
|---|---|---|---|
| normal | 94,680 | 8,144 | 1,670 |
| sensor_malfunction | 546 | 3,890 | 718 |
| not_sensor_data | 36 | 545 | 3,752 |

### The `spoofed_data` static check, on held-out windows

| spoof windows flagged (recall) | normal windows flagged |
|---|---|
| 0.107 | 0.124 |

### Latency (section 6, 50 timed runs per row)

| | preprocess p50 | model p50 | total p50 | total p99 | 10 s budget used at p99 |
|---|---|---|---|---|---|
| 10 sensors | 3.8 ms | 1.1 ms | 4.9 ms | 6.9 ms | 0.07 % |
| 100 sensors | 42.8 ms | 1.9 ms | 44.8 ms | 47.8 ms | 0.48 % |
| 500 sensors | 476.1 ms | 5.2 ms | 486.2 ms | 905.0 ms | 9.05 % |
| 1000 sensors | 434.5 ms | 3.7 ms | 437.9 ms | 2256.5 ms | 22.56 % |

**What the numbers show.**

- **The model flags what it was trained to flag.** Recall is 0.939 for any anomaly, and
  every fabricated kind is flagged at 0.978 or more. The five malfunction kinds are flagged
  at about the rates the binary model had in section 3 (stuck 0.528 against 0.547, spike
  0.901 against 0.901, drift 0.964 against 0.961, noise 0.985 against 0.983, oscillation
  0.992 against 0.982; those are three-seed means, these one seed). `stuck` stays the
  weakest at about half.
- **Precision is poor, because 9.4 % of normal windows are flagged.** 8,144 normal windows
  are named `sensor_malfunction` and 1,670 `not_sensor_data`. That is why
  `sensor_malfunction` precision is 0.309 although its recall is 0.755: only 3,890 of the
  12,579 windows named `sensor_malfunction` really are. The false-alarm rate is about the
  9.6-9.7 % the binary model had in section 3.
- **The two anomaly classes are told apart most of the time.** 718 malfunction windows
  (13.9 %) are named `not_sensor_data` and 545 fabricated ones (12.6 %) are named
  `sensor_malfunction`. This run does not show which kinds are mixed up (for example
  whether `sine` is confused with `oscillation`); that needs a per-kind breakdown.
- **The binary PR-AUC of 0.863 cannot be compared with the 0.695 of section 3.** The
  positive set changed: the spoof windows (recall about 0.2 before) are out, and four
  easily flagged fabricated kinds are in, which raises the number by itself.
- **The `spoofed_data` rule has no signal at its first threshold.** It flags 10.7 % of the
  spoof windows and 12.4 % of the normal ones. The injected spoofs shift a window by 3 to
  6 within-window spreads, far below the rule's 0.2 of the sensor's range, so it was
  expected to miss them; that it also flags more normal windows than spoofed ones was not
  expected. The cause was not investigated.
- **Latency is not comparable with `research/evaluation/results.md`.** That table was
  measured on 24 CPUs and Python 3.12.10, this one on 16 CPUs and Python 3.13.5.
  Preprocessing, which this change does not touch, is 2.8 to 3.6 times slower here at 10,
  100 and 500 sensors (for example 100 sensors: 15.2 ms there, 42.8 ms here), so the
  machine or Python version explains those rows. The 1000-sensor row does not follow:
  preprocessing p50 is faster here (434.5 ms against 694.8 ms) while its p99 is slower
  (2,256 ms against 849 ms), and it is even faster than this run's own 500-sensor row.
  The 500 and 1000 rows are noisy, so the 22.6 % budget figure rests on a shaky p99.
  The model's own time stays under 6 ms at every size.

**Caveats.**

- One injection seed and one model seed, so no standard deviations; compare with the
  three-seed numbers above only loosely.
- The fabricated kinds are synthetic shapes defined in the notebook and the model is
  trained and tested on the same generator, so the 0.978-1.000 recall says the features
  separate these shapes, not that real fabricated streams will be caught. The real-fault
  check of section 3 was not run for the multiclass model.
- The static-check thresholds (`spoof_delta` 0.2, `down_after_seconds` 10 s,
  `min_neighbors` 3) are first values and were not tuned; the spoof result above shows the
  first one needs it. `out_of_range` and `sensor_is_down` were only unit-tested here, not
  measured on the Intel data.
- `research/evaluation/results.md` still describes the binary model.

## 8. Acknowledgement

Data: Intel Berkeley Research Lab, https://db.csail.mit.edu/labdata/labdata.html
