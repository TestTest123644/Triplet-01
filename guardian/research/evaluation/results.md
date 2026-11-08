# The baseline model on the project's own sensors, and inference latency (#12)

## 1. Question

Issue #12 asks to evaluate the baseline model and benchmark inference latency per sensor
window. Two questions:

1. Does the baseline model (XGBoost trained on the Intel Lab data by
   the "Baseline training" section of `research/research.ipynb`) detect faults on the project's own sensor types (water
   level, rainfall, temperature, traffic flow)?
2. How long does the Guardian take per 20 s window as the number of sensors grows?

**Short answer.**

- **No, not as trained.** On the generator's data the model flags 99.6 % of the windows
  that hold no fault at all, and still 92.9 % at a threshold of 0.9. Its precision
  (0.082) is below the share of faulty windows (0.095): it does no better than flagging
  every window. Deployed as is, the Guardian would quarantine almost every sensor.
- **Two simple rules** (a value outside its range; the same value 3 times in a row) flag
  no healthy window and catch 68.5 % of value faults, but only 6.4 % of spoofed readings.
- **Latency was not a constraint up to about 1,000 sensors.** At 1,000 sensors a window
  took 699 ms (p50) and 849 ms (p99), 8.5 % of the 10 s between two windows; the model
  takes 4 ms of it, preprocessing 99 %. Above that the spatial step made it miss the 1 s
  target; section 7 (#13) measures the fix: 2,000 sensors now take 0.40 s.

**Metrics.** The issue names MAE, RMSE and MAPE, which measure the error of a predicted
value. The baseline model is a binary classifier (1 = the sensor's window is faulty), so
this file uses the classification metrics of `research/general_model/results.md`
(precision, recall, F1, PR-AUC, false alarms). MAE/RMSE/MAPE would apply to a forecasting
model, which does not exist yet.

## 2. Setup

- **Model**: `models/anomaly_baseline` written by the "Baseline training" section of `research/research.ipynb` (method
  `none`, threshold 0.5). On its own Intel Lab test split it gets F1 0.571, PR-AUC 0.702
  and recall 0.215 on `spoof`, in line with `research/general_model/results.md` (XGBoost,
  `none`: F1 0.565 ± 0.005).
- **Data**: `mocks/telemetry_generator.py`, seeds 0-2, 20 sensors, 3,600 s each, anomaly
  rate 0.01 (the default 0.05 would make about 40 % of windows faulty; 0.01 gives 9.5 %,
  close to the research's 9 %). The output is cut into non-overlapping 20 s windows in
  arrival order; a line's time is the newest timestamp seen so far, so a replayed message
  lands in the window it arrives in. About 3,600 sensor-windows per seed.
- **Labels**: 1 when the sensor-window holds a value fault (`out_of_range`, `spoofed`,
  `stuck`). Packet problems (`duplicate`, `replayed`, `out_of_order`, `packet_gap`) stay
  0, as in the research; the third table reports how often they are flagged.
- **Rules**: same deduplication and range lookup as `PreProcess`; a sensor-window is
  flagged when a value is outside its range or the same value appears 3 times in a row.
- **Benchmark**: one 20 s generator run (seed 0) per sensor count; 3 warm-up runs, then
  50 timed runs, wall clock in one process. Machine: Lenovo Legion Pro 5i laptop, Windows,
  24 logical CPUs, Python 3.12.10. Power settings were not controlled.
- **Reproduce**: run the "Baseline training", "Evaluation" and "Latency benchmark"
  sections of `research/research.ipynb`, in that order (the first two write and read `models/anomaly_baseline`
  at the repo root).

## 3. Results

Output of the two scripts, pasted verbatim.

```
model: models\anomaly_baseline (method none, threshold 0.5); generator: seeds (0, 1, 2), 20 sensors, 3600 s each, anomaly rate 0.01

## Model vs rules, all sensor types

| | precision | recall | f1 | false_alarms | recall_out_of_range | recall_spoofed | recall_stuck |
|---|---|---|---|---|---|---|---|
| model | 0.082 ± 0.004 | 0.846 ± 0.030 | 0.149 ± 0.006 | 0.996 ± 0.001 | 0.924 ± 0.043 | 0.928 ± 0.035 | 0.710 ± 0.034 |
| rules | 0.997 ± 0.003 | 0.685 ± 0.034 | 0.812 ± 0.025 | 0.000 ± 0.000 | 1.000 ± 0.000 | 0.064 ± 0.034 | 0.907 ± 0.015 |
| rules + model | 0.094 ± 0.006 | 0.981 ± 0.011 | 0.171 ± 0.010 | 0.996 ± 0.001 | 1.000 ± 0.000 | 0.959 ± 0.030 | 0.982 ± 0.016 |

## Model by sensor type

| | positive_share | precision | recall | f1 | false_alarms | pr_auc |
|---|---|---|---|---|---|---|
| all | 0.095 ± 0.006 | 0.082 ± 0.004 | 0.846 ± 0.030 | 0.149 ± 0.006 | 0.996 ± 0.001 | 0.051 ± 0.003 |
| water_level | 0.095 ± 0.010 | 0.083 ± 0.010 | 0.863 ± 0.031 | 0.151 ± 0.017 | 0.998 ± 0.001 | 0.051 ± 0.006 |
| rainfall | 0.083 ± 0.016 | 0.068 ± 0.003 | 0.812 ± 0.130 | 0.125 ± 0.005 | 0.994 ± 0.002 | 0.045 ± 0.008 |
| temperature | 0.092 ± 0.009 | 0.081 ± 0.009 | 0.867 ± 0.013 | 0.149 ± 0.015 | 0.995 ± 0.003 | 0.050 ± 0.005 |
| traffic_flow | 0.109 ± 0.003 | 0.094 ± 0.004 | 0.847 ± 0.017 | 0.170 ± 0.006 | 0.996 ± 0.002 | 0.059 ± 0.002 |

## Share flagged among windows without a value fault

| | model | rules |
|---|---|---|
| clean | 0.996 ± 0.001 | 0.000 ± 0.000 |
| duplicate | 0.986 ± 0.013 | 0.000 ± 0.000 |
| replayed | 1.000 ± 0.000 | 0.000 ± 0.000 |
| out_of_order | 0.997 ± 0.005 | 0.000 ± 0.000 |
| packet_gap | 0.992 ± 0.014 | 0.000 ± 0.000 |

## Model threshold sweep, all sensor types

| | precision | recall | f1 | false_alarms |
|---|---|---|---|---|
| 0.1 | 0.093 ± 0.006 | 0.974 ± 0.008 | 0.169 ± 0.009 | 1.000 ± 0.000 |
| 0.2 | 0.089 ± 0.005 | 0.936 ± 0.009 | 0.163 ± 0.009 | 1.000 ± 0.000 |
| 0.3 | 0.087 ± 0.004 | 0.905 ± 0.027 | 0.158 ± 0.007 | 0.999 ± 0.000 |
| 0.4 | 0.084 ± 0.004 | 0.871 ± 0.027 | 0.153 ± 0.006 | 0.998 ± 0.001 |
| 0.5 | 0.082 ± 0.004 | 0.846 ± 0.030 | 0.149 ± 0.006 | 0.996 ± 0.001 |
| 0.6 | 0.078 ± 0.004 | 0.803 ± 0.029 | 0.142 ± 0.007 | 0.992 ± 0.001 |
| 0.7 | 0.074 ± 0.004 | 0.749 ± 0.025 | 0.134 ± 0.007 | 0.985 ± 0.000 |
| 0.8 | 0.066 ± 0.005 | 0.652 ± 0.033 | 0.119 ± 0.008 | 0.972 ± 0.003 |
| 0.9 | 0.034 ± 0.003 | 0.316 ± 0.009 | 0.062 ± 0.006 | 0.929 ± 0.003 |
```

```
model: models\anomaly_baseline; machine: AMD64, 24 CPUs, Python 3.12.10
model load: 24.2 ms
first call, 10 sensors (cold): 2.2 ms
timed runs per row: 50, after 3 warm-up runs

| | messages | preprocess p50 | model p50 | total p50 | total p95 | total p99 | sensors/s at p50 | 10 s budget used at p99 |
|---|---|---|---|---|---|---|---|---|
| 10 sensors | 202 | 1.3 ms | 0.4 ms | 1.7 ms | 1.8 ms | 1.8 ms | 5,841 | 0.02% |
| 100 sensors | 1987 | 15.2 ms | 0.7 ms | 15.9 ms | 16.5 ms | 17.4 ms | 6,300 | 0.17% |
| 500 sensors | 9869 | 131.3 ms | 1.7 ms | 133.1 ms | 238.2 ms | 255.4 ms | 3,757 | 2.55% |
| 1000 sensors | 19774 | 694.8 ms | 4.1 ms | 698.9 ms | 835.0 ms | 849.3 ms | 1,431 | 8.49% |
```

## 4. Argument

1. **The model flags almost every window.** It flags 99.6 % of clean windows, and every
   sensor type behaves the same (0.994-0.998). Its precision, 0.082, is below the share
   of faulty windows, 0.095, which is what flagging everything would give. Its PR-AUC,
   0.051, is below the 0.095 of a random ranking, so its scores do not even rank faulty
   windows above clean ones. The high recall (0.846, and 0.981 with the rules) comes from
   flagging everything, not from detecting faults. A threshold cannot fix it: at 0.9,
   92.9 % of healthy windows are still flagged while recall falls to 0.316. Windows with
   only packet problems are flagged as often as clean ones (0.986-1.000).
2. **A likely cause (an inference, not tested here): the two datasets disagree on what
   normal looks like.** `PreProcess` scales each value to its type's range. Inside
   one window, the Intel Lab readings the model learned from vary by a median of 0.06 % to
   0.3 % of their range (temperature 0.0312 C of 50 C, humidity 0.0836 % of 100 %, light
   5.58 lux of 1,847 lux; spreads from `research/general_model/results.md`). The
   generator's noise alone is 1 % of the range (`NOISE_FRACTION`), on top of a slow wave
   shared by each type. A normal generator window therefore looks like the `noise` and
   `drift` faults the model was trained to flag. Which side is unrealistic is open: the
   noise of the real sensors is not known yet.
3. **The rules are precise but blind to spoofing.** They flag no healthy window, catch
   every `out_of_range` fault and 90.7 % of `stuck` windows, but only 6.4 % of `spoofed`
   ones: a spoofed reading stays inside its range and is never repeated, so neither rule
   sees it. Spoofing is the gap the model is meant to fill.
4. **Latency is dominated by preprocessing, and it grows faster than the sensor count.**
   From 100 to 1,000 sensors, preprocessing goes from 15.2 ms to 694.8 ms (46 times
   longer for 10 times the sensors): the spatial step compares each sensor with every
   other sensor of its type (noted as O(n²) in `PreProcess.spatial_features`). The model takes 0.4-4.1 ms,
   loads in 24 ms, and its first call takes 2.2 ms. Even at 1,000 sensors, the worst
   timed run uses 8.5 % of the 10 s budget. The jump of p95 at 500 sensors (238 ms
   against a p50 of 133 ms) suggests runs slowed by the machine; power settings were not
   controlled.

## 5. Next steps

- **Make the model and the data agree on what normal looks like**, then re-run
  the "Evaluation" section: train on data with the project's sensor types (for example generator
  runs with other seeds than the test ones), and/or set the generator's noise from the
  expected noise of real sensors.
- **Until then, the rules are the safer Guardian on this data** (no false alarms), with
  spoofing uncovered.
- Add a flood scenario to the generator (rising water level, one sensor rising alone), to
  check that sensors correctly reporting a real flood are not flagged.

## 6. Caveats

- **Synthetic data**: the generator's faults are simple and its noise level is a choice,
  so these numbers describe this generator, not real sensors.
- **Windows** do not overlap, as in the research; the Guardian buffer slides by 10 s.
- **Latency** comes from one laptop with uncontrolled power settings, in one process;
  other machines will differ.
- **The ± values** cover the 3 generator seeds only, with one trained model; no
  significance test was run.
- **The threshold** is the model's default 0.5; the sweep shows no threshold works.

## 7. Latency after the spatial fix (#13)

**Target.** The report asks for sub-second feedback loops, and `telemetry-ingest` has 6
partitions, so each consumer must finish one window in under 1 s.

**Change.** `PreProcess.find_range` no longer copies the cluster's readings for every
sensor, and the spatial step searches neighbours once per cluster with vectorized
distances (`PreProcess.cluster_neighbors`) instead of once per sensor over every sensor.
The features are unchanged (`tests/pipelines/test_preprocess_equivalence.py` pins them
against the old code), so the model needs no retrain.

**Setup.** The "Latency benchmark" section of `research/research.ipynb`, now with 2,000
and 5,000 sensors and the static checks (`out_of_range`, `sensor_is_down`,
`spoofed_data`) timed per window. Before = the code before the fix (`b685da1`), after =
the fix, same notebook code and model. Machine: laptop, Windows, 16 logical CPUs,
Python 3.12.12; 50 timed runs per row after 3 warm-up runs, except 5,000 sensors before
the fix (5 runs: one run takes about 30 s).

| sensors | preprocess p50 before | preprocess p50 after | checks p50 after | total p50 before | total p50 after | total p99 after |
|---|---|---|---|---|---|---|
| 10 | 1.7 ms | 1.9 ms | 0.4 ms | 2.6 ms | 2.9 ms | 3.4 ms |
| 100 | 22.1 ms | 18.3 ms | 4.1 ms | 28.1 ms | 23.6 ms | 25.4 ms |
| 500 | 173.7 ms | 97.5 ms | 15.4 ms | 199.8 ms | 115.8 ms | 207.5 ms |
| 1,000 | 494.8 ms | 169.5 ms | 27.9 ms | 593.5 ms | 201.2 ms | 216.5 ms |
| 2,000 | 1,658.7 ms | 335.5 ms | 56.6 ms | 2,002.7 ms | 399.7 ms | 592.2 ms |
| 5,000 | 26,472.9 ms | 1,136.8 ms | 139.2 ms | 32,753.1 ms | 1,292.2 ms | 1,478.1 ms |

**Reading.**

1. **2,000 sensors per consumer now fit the 1 s target** (0.40 s p50, 0.59 s p99, against
   2.0 s before). 5,000 do not (1.29 s p50), but they now take 4 % of the old time.
2. **Time now grows close to linearly** (1,000 to 2,000 sensors: 2.0 times longer). The
   largest remaining cost is the per-sensor loop (series, normalization, value features),
   not the spatial step. The neighbour search is still O(cluster²) per window; a KD-tree
   is the next step if one consumer holds more than about 3,000 sensors per cluster
   (an estimate, not measured here).
3. **The static checks cost about 14 % of a window** (57 ms at 2,000 sensors). The #12
   benchmark did not time them.

**Not timed.** Kafka consumption, message validation (a Buffer already holds `Sensor`
readings), DB and Elasticsearch writes, and RSA decryption of payloads (not wired in
`src/` yet).

**Open question.** The target holds per consumer only if a consumer receives whole
clusters: `find_range` and the spatial step compare a sensor with its cluster, so if
`telemetry-ingest` is not keyed by `cluster_id`, a consumer sees part of each cluster and
both give wrong results. No message key is set yet.

**Caveats.** One laptop with uncontrolled power settings. Two earlier runs of the same
"after" code were disturbed by other load on the machine (1.1 s and 1.4 s p50 at 2,000
sensors); a direct loop over `process_window` gives 0.34 s, matching the table. The p95
and p99 of the 5,000-sensor "before" row (60 s) are not reliable.
