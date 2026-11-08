# Noise cleaning before the short- and long-window features (#66)

**Where #66 ended.**

- **Both models now clean each sensor's series with a tuned Kalman before the
  features, in training and live.** The lead decided on PR #114 that cleaning is
  needed (section 5), for both models.
- **Part 1 (sections 1-6)** tested three filters as first set (moving median, EMA,
  Kalman). None passed the keep rule agreed before the run.
- **Part 2 (section 7), short-window model:** a Kalman smoother (process noise 0.1 ×
  the measurement noise), a reading more than 4 prediction sds away passes as it is, and
  a run of repeated readings stays raw. It
  - keeps the stuck values part 1's Kalman lost (1 s stuck recall 0.853 without
    cleaning, 0.536 tested Kalman, 0.847 tuned);
  - raises false alarms at the live threshold on 1 s (0.004 to 0.008) and on the
    generator (0.015 to 0.031) and lowers them on 100 ms (0.009 to 0.007); #96's
    target is still met;
  - ranks about as well on the sources it trains on (PR-AUC 0.918 to 0.909 at 1 s,
    0.984 to 0.981 at 100 ms, 0.887 to 0.892 on the generator) and better on two of
    the three held out (1 s 0.693 to 0.781, generator 0.201 to 0.303; 100 ms 0.733 to
    0.657).
- **Part 3 (section 8), long-window model:** its own Kalman on the 5-minute grid
  (smoother, process noise 0.9 × the measurement noise, jumps at 4 sds). The tuning
  run's gain (unseen-source PR-AUC 0.804 to 0.857, seed 0, validation rows) does not
  hold on the test windows (seeds 0-2):
  - unseen-source PR-AUC 0.827 to 0.822; pooled PR-AUC 0.951 to 0.947;
  - false alarms 0.008 to 0.013 per sensor-day (budget 0.04); #96's target is still
    met;
  - Intel Lab loses most (held out 0.723 to 0.653).
- **Recall is a lower bound** for some injected faults (section 9).

## 1. Question

#66 asked for a noise-cleaning step in preprocessing, so that noise is removed before
the features "without removing the signals the model has to detect". After #13 split
anomaly detection into a short- and a long-window model (#96), it was agreed on #66 to
test two or three filters against the short-window model, comparing false alarms and
the recall of each fault kind, and to keep a cleaning step only if false alarms drop
and no kind's recall does; otherwise to close #66 with the numbers. This is that test
(section 10 of `research/research.ipynb`).

**Part 1 answer** (the filters as first set; sections 5-8 say what followed).

- **No filter passes; no cleaning step is added.** All three (moving median, EMA
  low-pass, Kalman) fail the keep rule on both grid sources.
- **On 1 s data every filter raises false alarms:** 0.004 without a filter, 0.006-0.007
  with one.
- **On 100 ms data false alarms fall (0.009 to 0.004-0.005), but the faults go with
  them:** stuck values from 91 % caught to 55-66 %, noise from 82 % to 71-73 %, and the
  median filter erases spikes (92 % to 2 %).
- **The model ranks windows worse with every filter:** held-out PR-AUC falls from 0.918
  to 0.80-0.85 (1 s) and from 0.984 to 0.87-0.97 (100 ms).
- **1 s noise recall, the model's weakest kind, does not really improve.** At each
  filter's own threshold it rises (0.073 to 0.10-0.18), but those thresholds also flag
  more clean windows. At the baseline's false-alarm rate EMA and Kalman catch the same
  noise as no filter (0.065, 0.075 vs 0.073); only the median keeps a gain (0.123),
  while its spike recall falls from 0.928 to 0.184.
- **Why:** the 1 s false alarms do look like noise (fast-alternating steps), but a
  filter smooths injected noise and healthy jitter alike, and it also makes every
  healthy window smoother, so a stuck (flat) value stands out less. On 100 ms data the
  false alarms look like bursts and spikes, not noise, so there is no noise to remove.
- **Recommendation then:** keep the short-window features on the raw series. The lead
  decided otherwise (section 5).

## 2. Setup

- **Model and data:** section 8's short-window model and data, unchanged: XGBoost on
  `SHORT_MODEL_KEYS` (packet counters, robust z value features, shape features, `haar`
  at the sensor's own rate), trained on `grid_frequency` (1 s), `grid_frequency_100ms`
  and 10 generator runs, 20 s windows, the short-window kinds injected (injection seeds
  0-2), the same time split, gaps and validation windows as
  `research/short_window/results.md` section 2.
- **Filters** (one setting each), run on each sensor's raw series before the
  short-window columns are computed; the packet counters and section 4's base columns
  stay on the raw readings:

  | name | what it does | setting |
  |---|---|---|
  | `none` | no filter (the baseline, rebuilt by the same code) | |
  | `median` | centred moving median | 5 readings |
  | `ema` | exponential moving average, forward in time (a first-order low-pass) | alpha 0.3 |
  | `kalman` | 1-D local-level Kalman filter | measurement std from the window: median(\|step\|) / (0.6745 × √2); process variance 0.1 × measurement variance |

  A series shorter than 4 readings (`preprocess.min_samples`), shorter than the
  median's 5, flat, or with a zero noise estimate is left as it is.
- **Fair comparison:** for each filter and seed the model is retrained and its
  thresholds re-picked on its own validation windows exactly as section 8 does; every
  filter is scored on the same test windows.
- **Scored at the threshold of the source's rate** (what `AnomalyDetector` applies
  live): tests 1 (each grid source's held-out windows) and 3 (generator runs 0-2). The
  unseen-source rows keep section 8's thresholds: the source's own one for "seen", the
  pooled one of the other sources for "unseen".
- **Also at the baseline's false-alarm rate** (reported, not gated): each filter's
  test 1 again, at the threshold that flags the same share of clean test windows as
  `none` does in that seed (`... at the baseline's false alarms` tables). Picked on
  the test windows themselves, so it is a comparison at one operating point, not a
  threshold the detector could use.
- **Keep rule** (fixed before the run): on both grid sources' test 1, mean over the 3
  seeds,
  - false alarms below `none`'s mean minus max(`none`'s seed sd, 0.001), and
  - for each of `stuck`, `spike`, `noise`, `oscillation`, `sine`, `ramp`, `pattern`,
    `uniform`, recall at least `none`'s mean minus max(`none`'s seed sd, 0.01).

  "Does not drop" is read as "not lower beyond seed noise" (sample sd over the seeds).
  The floors stop a single window from deciding: several kinds score 1.000 ± 0.000, and
  1 s false alarms are 0.004 ± 0.000. The generator and the unseen sources are
  reported, not gated: the generator's noise (1 % of the range) is far above real
  sensors', and the model is judged on real data.
- **What is at stake:** without a filter, 1 s noise recall is already 0.073 ± 0.023
  (the model cannot tell injected noise from normal 1 s variation at the live
  threshold), so on 1 s the noise check only protects recall down to 0.050; the 100 ms
  noise recall (0.820 ± 0.033) is the check that bites.
- **False alarms of the baseline:** on the `none` model, the clean test windows it
  flags against those it does not, and the injected noise windows, described by the
  median of five columns that measure a window's noise texture: the share of the
  finest-scale `haar` energy (`n_haar_d1_energy`), how often the steps change sign
  (`sign_flip_share`), the correlation of each step with the next (`diff_autocorr`),
  the largest rolling variance over the median one (`rolling_variance_ratio`) and the
  largest step in the window's own robust spread (`max_abs_step`). They are all
  scale-free, so the profile compares texture, not raw amplitude.
- **Reproduce:** run section 10 of `research/research.ipynb` (`clean_main()`; needs
  section 8's cells). The tables of each filter and seed are built once (about 5-22
  minutes each on a 16-CPU Windows machine, Python 3.12.12, XGBoost 3.4.1) and cached
  in `~/.cache/guardian_layer/short_tables/`; with the cache the output below takes
  about 65 s. No model is saved.

## 3. Results

Output of `clean_main()`, pasted verbatim.

```
settings: {"filters": ["none", "median", "ema", "kalman"], "clean_window": 5, "ema_alpha": 0.3, "kalman_process_ratio": 0.1, "injection_seeds": [0, 1, 2], "recall_floor": 0.01, "false_alarm_floor": 0.001, "seconds": 403}
filter latency, 42 sensors x 200 readings: {"none": {"p50_ms": 0.028049995307810605, "p99_ms": 0.03608099796110764}, "median": {"p50_ms": 8.924049994675443, "p99_ms": 11.248838003230047}, "ema": {"p50_ms": 6.196200003614649, "p99_ms": 9.585887997673124}, "kalman": {"p50_ms": 6.822450006438885, "p99_ms": 15.026306998042855}}
median: keeps=False
ema: keeps=False
kalman: keeps=False
picked: none

## Test 1, grid_frequency

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform | precision_sensor_malfunction | recall_sensor_malfunction | f1_sensor_malfunction | precision_not_sensor_data | recall_not_sensor_data | f1_not_sensor_data |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| none | 0.948 ± 0.003 | 0.770 ± 0.014 | 0.850 ± 0.008 | 0.004 ± 0.000 | 0.918 ± 0.004 | 0.853 ± 0.012 | 0.928 ± 0.020 | 0.073 ± 0.023 | 0.824 ± 0.016 | 0.998 ± 0.002 | 0.999 ± 0.001 | 1.000 ± 0.000 | 0.477 ± 0.043 | 0.953 ± 0.005 | 0.647 ± 0.015 | 0.770 ± 0.009 | 0.910 ± 0.008 | 0.863 ± 0.015 | 0.886 ± 0.011 |
| median | 0.904 ± 0.009 | 0.619 ± 0.007 | 0.735 ± 0.004 | 0.007 ± 0.001 | 0.803 ± 0.004 | 0.584 ± 0.035 | 0.195 ± 0.006 | 0.177 ± 0.027 | 0.704 ± 0.019 | 0.964 ± 0.003 | 0.999 ± 0.001 | 0.998 ± 0.002 | 0.334 ± 0.031 | 0.762 ± 0.009 | 0.313 ± 0.010 | 0.444 ± 0.012 | 0.805 ± 0.017 | 0.775 ± 0.004 | 0.789 ± 0.006 |
| ema | 0.903 ± 0.015 | 0.683 ± 0.005 | 0.778 ± 0.006 | 0.007 ± 0.001 | 0.854 ± 0.004 | 0.447 ± 0.034 | 0.818 ± 0.021 | 0.116 ± 0.018 | 0.677 ± 0.009 | 0.996 ± 0.001 | 0.999 ± 0.001 | 0.999 ± 0.001 | 0.407 ± 0.029 | 0.822 ± 0.021 | 0.451 ± 0.005 | 0.583 ± 0.009 | 0.865 ± 0.015 | 0.837 ± 0.009 | 0.851 ± 0.008 |
| kalman | 0.916 ± 0.010 | 0.664 ± 0.012 | 0.769 ± 0.009 | 0.006 ± 0.001 | 0.852 ± 0.007 | 0.536 ± 0.018 | 0.756 ± 0.027 | 0.099 ± 0.011 | 0.557 ± 0.013 | 0.996 ± 0.001 | 0.999 ± 0.001 | 1.000 ± 0.000 | 0.369 ± 0.035 | 0.840 ± 0.021 | 0.421 ± 0.009 | 0.561 ± 0.010 | 0.869 ± 0.005 | 0.826 ± 0.013 | 0.847 ± 0.009 |

## Test 1, grid_frequency_100ms

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform | precision_sensor_malfunction | recall_sensor_malfunction | f1_sensor_malfunction | precision_not_sensor_data | recall_not_sensor_data | f1_not_sensor_data |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| none | 0.913 ± 0.003 | 0.953 ± 0.007 | 0.933 ± 0.004 | 0.009 ± 0.001 | 0.984 ± 0.003 | 0.911 ± 0.026 | 0.924 ± 0.013 | 0.820 ± 0.033 | 0.989 ± 0.005 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.848 ± 0.005 | 0.905 ± 0.014 | 0.875 ± 0.008 | 0.981 ± 0.002 | 1.000 ± 0.000 | 0.990 ± 0.001 |
| median | 0.945 ± 0.016 | 0.789 ± 0.010 | 0.860 ± 0.012 | 0.005 ± 0.002 | 0.870 ± 0.012 | 0.662 ± 0.033 | 0.018 ± 0.012 | 0.714 ± 0.038 | 0.994 ± 0.010 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.992 ± 0.000 | 0.828 ± 0.035 | 0.519 ± 0.039 | 0.638 ± 0.039 | 0.920 ± 0.022 | 0.966 ± 0.004 | 0.942 ± 0.010 |
| ema | 0.955 ± 0.013 | 0.887 ± 0.014 | 0.920 ± 0.002 | 0.004 ± 0.001 | 0.965 ± 0.005 | 0.554 ± 0.062 | 0.852 ± 0.028 | 0.722 ± 0.017 | 0.992 ± 0.008 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.997 ± 0.005 | 0.908 ± 0.023 | 0.766 ± 0.031 | 0.831 ± 0.009 | 0.981 ± 0.003 | 0.996 ± 0.003 | 0.988 ± 0.003 |
| kalman | 0.946 ± 0.021 | 0.887 ± 0.025 | 0.915 ± 0.005 | 0.005 ± 0.002 | 0.961 ± 0.005 | 0.546 ± 0.070 | 0.848 ± 0.059 | 0.728 ± 0.063 | 0.991 ± 0.009 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.997 ± 0.005 | 0.882 ± 0.041 | 0.755 ± 0.054 | 0.812 ± 0.018 | 0.973 ± 0.008 | 0.992 ± 0.004 | 0.982 ± 0.003 |

## Test 3, generator

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform | precision_sensor_malfunction | recall_sensor_malfunction | f1_sensor_malfunction | precision_not_sensor_data | recall_not_sensor_data | f1_not_sensor_data |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| none | 0.844 ± 0.003 | 0.788 ± 0.026 | 0.815 ± 0.013 | 0.015 ± 0.001 | 0.887 ± 0.012 | 0.728 ± 0.044 | 0.803 ± 0.050 | 0.084 ± 0.039 | 0.606 ± 0.079 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.948 ± 0.007 | 0.549 ± 0.020 | 0.695 ± 0.017 | 0.785 ± 0.011 | 1.000 ± 0.000 | 0.880 ± 0.007 |
| median | 0.663 ± 0.041 | 0.718 ± 0.030 | 0.689 ± 0.036 | 0.037 ± 0.005 | 0.781 ± 0.022 | 0.559 ± 0.049 | 0.228 ± 0.031 | 0.154 ± 0.037 | 0.727 ± 0.027 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.992 ± 0.013 | 0.552 ± 0.060 | 0.359 ± 0.032 | 0.435 ± 0.042 | 0.668 ± 0.048 | 0.998 ± 0.003 | 0.799 ± 0.035 |
| ema | 0.762 ± 0.014 | 0.763 ± 0.033 | 0.762 ± 0.019 | 0.024 ± 0.002 | 0.845 ± 0.015 | 0.458 ± 0.033 | 0.755 ± 0.048 | 0.129 ± 0.013 | 0.674 ± 0.050 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.788 ± 0.012 | 0.472 ± 0.020 | 0.590 ± 0.014 | 0.721 ± 0.015 | 1.000 ± 0.000 | 0.838 ± 0.010 |
| kalman | 0.762 ± 0.004 | 0.729 ± 0.042 | 0.745 ± 0.023 | 0.023 ± 0.001 | 0.839 ± 0.024 | 0.544 ± 0.029 | 0.606 ± 0.061 | 0.136 ± 0.053 | 0.480 ± 0.049 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.804 ± 0.009 | 0.411 ± 0.030 | 0.544 ± 0.026 | 0.723 ± 0.006 | 1.000 ± 0.000 | 0.839 ± 0.004 |

## Unseen source

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform | precision_sensor_malfunction | recall_sensor_malfunction | f1_sensor_malfunction | precision_not_sensor_data | recall_not_sensor_data | f1_not_sensor_data |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| none / grid_frequency: seen | 0.948 ± 0.003 | 0.770 ± 0.014 | 0.850 ± 0.008 | 0.004 ± 0.000 | 0.918 ± 0.004 | 0.853 ± 0.012 | 0.928 ± 0.020 | 0.073 ± 0.023 | 0.824 ± 0.016 | 0.998 ± 0.002 | 0.999 ± 0.001 | 1.000 ± 0.000 | 0.477 ± 0.043 | 0.953 ± 0.005 | 0.647 ± 0.015 | 0.770 ± 0.009 | 0.910 ± 0.008 | 0.863 ± 0.015 | 0.886 ± 0.011 |
| none / grid_frequency: unseen | 0.387 ± 0.056 | 0.687 ± 0.005 | 0.493 ± 0.044 | 0.110 ± 0.026 | 0.693 ± 0.011 | 0.731 ± 0.047 | 0.787 ± 0.043 | 0.009 ± 0.009 | 0.941 ± 0.016 | 0.999 ± 0.001 | 1.000 ± 0.000 | 0.994 ± 0.001 | 0.020 ± 0.011 | 0.239 ± 0.035 | 0.602 ± 0.018 | 0.340 ± 0.034 | 0.735 ± 0.121 | 0.734 ± 0.011 | 0.731 ± 0.062 |
| none / grid_frequency_100ms: seen | 0.913 ± 0.003 | 0.953 ± 0.007 | 0.933 ± 0.004 | 0.009 ± 0.001 | 0.984 ± 0.003 | 0.911 ± 0.026 | 0.924 ± 0.013 | 0.820 ± 0.033 | 0.989 ± 0.005 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.848 ± 0.005 | 0.905 ± 0.014 | 0.875 ± 0.008 | 0.981 ± 0.002 | 1.000 ± 0.000 | 0.990 ± 0.001 |
| none / grid_frequency_100ms: unseen | 0.720 ± 0.071 | 0.565 ± 0.076 | 0.628 ± 0.036 | 0.023 ± 0.009 | 0.733 ± 0.013 | 0.612 ± 0.014 | 0.874 ± 0.039 | 0.004 ± 0.004 | 0.159 ± 0.056 | 1.000 ± 0.000 | 0.713 ± 0.478 | 0.997 ± 0.005 | 0.156 ± 0.082 | 0.485 ± 0.105 | 0.391 ± 0.025 | 0.428 ± 0.030 | 0.963 ± 0.016 | 0.725 ± 0.135 | 0.823 ± 0.097 |
| none / generator: seen | 0.902 ± 0.035 | 0.764 ± 0.022 | 0.827 ± 0.017 | 0.008 ± 0.003 | 0.887 ± 0.012 | 0.719 ± 0.044 | 0.750 ± 0.028 | 0.044 ± 0.023 | 0.517 ± 0.115 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.992 ± 0.013 | 0.988 ± 0.012 | 0.508 ± 0.025 | 0.670 ± 0.020 | 0.857 ± 0.052 | 0.998 ± 0.003 | 0.922 ± 0.028 |
| none / generator: unseen | 0.239 ± 0.013 | 0.667 ± 0.028 | 0.351 ± 0.018 | 0.215 ± 0.005 | 0.201 ± 0.009 | 0.951 ± 0.022 | 0.432 ± 0.097 | 0.281 ± 0.123 | 0.072 ± 0.019 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.983 ± 0.029 | 0.627 ± 0.022 | 0.093 ± 0.006 | 0.373 ± 0.036 | 0.149 ± 0.011 | 0.545 ± 0.032 | 0.892 ± 0.019 | 0.677 ± 0.030 |
| median / grid_frequency: seen | 0.904 ± 0.009 | 0.619 ± 0.007 | 0.735 ± 0.004 | 0.007 ± 0.001 | 0.803 ± 0.004 | 0.584 ± 0.035 | 0.195 ± 0.006 | 0.177 ± 0.027 | 0.704 ± 0.019 | 0.964 ± 0.003 | 0.999 ± 0.001 | 0.998 ± 0.002 | 0.334 ± 0.031 | 0.762 ± 0.009 | 0.313 ± 0.010 | 0.444 ± 0.012 | 0.805 ± 0.017 | 0.775 ± 0.004 | 0.789 ± 0.006 |
| median / grid_frequency: unseen | 0.947 ± 0.008 | 0.466 ± 0.017 | 0.624 ± 0.015 | 0.003 ± 0.000 | 0.709 ± 0.031 | 0.499 ± 0.044 | 0.130 ± 0.008 | 0.021 ± 0.007 | 0.450 ± 0.038 | 0.709 ± 0.085 | 0.998 ± 0.002 | 0.900 ± 0.002 | 0.033 ± 0.003 | 0.595 ± 0.038 | 0.256 ± 0.009 | 0.358 ± 0.006 | 0.947 ± 0.004 | 0.523 ± 0.024 | 0.674 ± 0.021 |
| median / grid_frequency_100ms: seen | 0.945 ± 0.016 | 0.789 ± 0.010 | 0.860 ± 0.012 | 0.005 ± 0.002 | 0.870 ± 0.012 | 0.662 ± 0.033 | 0.018 ± 0.012 | 0.714 ± 0.038 | 0.994 ± 0.010 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.992 ± 0.000 | 0.828 ± 0.035 | 0.519 ± 0.039 | 0.638 ± 0.039 | 0.920 ± 0.022 | 0.966 ± 0.004 | 0.942 ± 0.010 |
| median / grid_frequency_100ms: unseen | 0.631 ± 0.096 | 0.609 ± 0.082 | 0.612 ± 0.008 | 0.039 ± 0.021 | 0.642 ± 0.052 | 0.500 ± 0.066 | 0.050 ± 0.040 | 0.484 ± 0.304 | 0.806 ± 0.059 | 0.946 ± 0.039 | 0.967 ± 0.058 | 0.834 ± 0.275 | 0.351 ± 0.174 | 0.267 ± 0.064 | 0.240 ± 0.063 | 0.253 ± 0.064 | 0.652 ± 0.203 | 0.633 ± 0.157 | 0.611 ± 0.089 |
| median / generator: seen | 0.870 ± 0.019 | 0.632 ± 0.037 | 0.731 ± 0.020 | 0.010 ± 0.002 | 0.781 ± 0.022 | 0.473 ± 0.037 | 0.166 ± 0.010 | 0.049 ± 0.030 | 0.446 ± 0.074 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.857 ± 0.057 | 0.818 ± 0.067 | 0.248 ± 0.026 | 0.379 ± 0.027 | 0.851 ± 0.012 | 0.963 ± 0.017 | 0.904 ± 0.014 |
| median / generator: unseen | 0.376 ± 0.014 | 0.671 ± 0.024 | 0.481 ± 0.016 | 0.113 ± 0.005 | 0.604 ± 0.007 | 0.639 ± 0.025 | 0.248 ± 0.066 | 0.283 ± 0.042 | 0.628 ± 0.041 | 0.946 ± 0.029 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.577 ± 0.072 | 0.207 ± 0.024 | 0.321 ± 0.013 | 0.251 ± 0.017 | 0.420 ± 0.022 | 0.842 ± 0.023 | 0.560 ± 0.016 |
| ema / grid_frequency: seen | 0.903 ± 0.015 | 0.683 ± 0.005 | 0.778 ± 0.006 | 0.007 ± 0.001 | 0.854 ± 0.004 | 0.447 ± 0.034 | 0.818 ± 0.021 | 0.116 ± 0.018 | 0.677 ± 0.009 | 0.996 ± 0.001 | 0.999 ± 0.001 | 0.999 ± 0.001 | 0.407 ± 0.029 | 0.822 ± 0.021 | 0.451 ± 0.005 | 0.583 ± 0.009 | 0.865 ± 0.015 | 0.837 ± 0.009 | 0.851 ± 0.008 |
| ema / grid_frequency: unseen | 0.423 ± 0.054 | 0.636 ± 0.009 | 0.506 ± 0.038 | 0.088 ± 0.021 | 0.646 ± 0.009 | 0.550 ± 0.035 | 0.756 ± 0.015 | 0.023 ± 0.008 | 0.824 ± 0.024 | 0.987 ± 0.004 | 0.998 ± 0.003 | 0.883 ± 0.021 | 0.054 ± 0.013 | 0.229 ± 0.039 | 0.517 ± 0.017 | 0.316 ± 0.037 | 0.838 ± 0.046 | 0.608 ± 0.020 | 0.704 ± 0.016 |
| ema / grid_frequency_100ms: seen | 0.955 ± 0.013 | 0.887 ± 0.014 | 0.920 ± 0.002 | 0.004 ± 0.001 | 0.965 ± 0.005 | 0.554 ± 0.062 | 0.852 ± 0.028 | 0.722 ± 0.017 | 0.992 ± 0.008 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.997 ± 0.005 | 0.908 ± 0.023 | 0.766 ± 0.031 | 0.831 ± 0.009 | 0.981 ± 0.003 | 0.996 ± 0.003 | 0.988 ± 0.003 |
| ema / grid_frequency_100ms: unseen | 0.796 ± 0.015 | 0.665 ± 0.022 | 0.724 ± 0.010 | 0.017 ± 0.002 | 0.807 ± 0.022 | 0.564 ± 0.044 | 0.842 ± 0.008 | 0.031 ± 0.032 | 0.840 ± 0.091 | 0.976 ± 0.009 | 1.000 ± 0.000 | 0.932 ± 0.044 | 0.185 ± 0.077 | 0.563 ± 0.006 | 0.465 ± 0.009 | 0.509 ± 0.007 | 0.879 ± 0.015 | 0.742 ± 0.045 | 0.804 ± 0.020 |
| ema / generator: seen | 0.859 ± 0.034 | 0.710 ± 0.022 | 0.777 ± 0.015 | 0.012 ± 0.003 | 0.845 ± 0.015 | 0.417 ± 0.010 | 0.633 ± 0.041 | 0.054 ± 0.037 | 0.549 ± 0.074 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.947 ± 0.057 | 0.885 ± 0.022 | 0.394 ± 0.022 | 0.545 ± 0.020 | 0.827 ± 0.045 | 0.984 ± 0.018 | 0.898 ± 0.019 |
| ema / generator: unseen | 0.408 ± 0.016 | 0.646 ± 0.033 | 0.499 ± 0.014 | 0.095 ± 0.011 | 0.588 ± 0.009 | 0.397 ± 0.086 | 0.599 ± 0.045 | 0.368 ± 0.070 | 0.131 ± 0.012 | 0.946 ± 0.008 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.680 ± 0.066 | 0.251 ± 0.002 | 0.283 ± 0.019 | 0.266 ± 0.009 | 0.440 ± 0.027 | 0.889 ± 0.002 | 0.589 ± 0.025 |
| kalman / grid_frequency: seen | 0.916 ± 0.010 | 0.664 ± 0.012 | 0.769 ± 0.009 | 0.006 ± 0.001 | 0.852 ± 0.007 | 0.536 ± 0.018 | 0.756 ± 0.027 | 0.099 ± 0.011 | 0.557 ± 0.013 | 0.996 ± 0.001 | 0.999 ± 0.001 | 1.000 ± 0.000 | 0.369 ± 0.035 | 0.840 ± 0.021 | 0.421 ± 0.009 | 0.561 ± 0.010 | 0.869 ± 0.005 | 0.826 ± 0.013 | 0.847 ± 0.009 |
| kalman / grid_frequency: unseen | 0.496 ± 0.081 | 0.607 ± 0.027 | 0.543 ± 0.051 | 0.063 ± 0.019 | 0.647 ± 0.032 | 0.564 ± 0.025 | 0.636 ± 0.084 | 0.026 ± 0.018 | 0.770 ± 0.047 | 0.950 ± 0.039 | 0.998 ± 0.002 | 0.869 ± 0.056 | 0.028 ± 0.015 | 0.297 ± 0.069 | 0.482 ± 0.043 | 0.364 ± 0.057 | 0.753 ± 0.146 | 0.581 ± 0.009 | 0.651 ± 0.061 |
| kalman / grid_frequency_100ms: seen | 0.946 ± 0.021 | 0.887 ± 0.025 | 0.915 ± 0.005 | 0.005 ± 0.002 | 0.961 ± 0.005 | 0.546 ± 0.070 | 0.848 ± 0.059 | 0.728 ± 0.063 | 0.991 ± 0.009 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.997 ± 0.005 | 0.882 ± 0.041 | 0.755 ± 0.054 | 0.812 ± 0.018 | 0.973 ± 0.008 | 0.992 ± 0.004 | 0.982 ± 0.003 |
| kalman / grid_frequency_100ms: unseen | 0.808 ± 0.087 | 0.681 ± 0.106 | 0.731 ± 0.049 | 0.018 ± 0.011 | 0.809 ± 0.027 | 0.535 ± 0.054 | 0.765 ± 0.038 | 0.386 ± 0.285 | 0.906 ± 0.044 | 0.987 ± 0.012 | 0.328 ± 0.568 | 0.978 ± 0.013 | 0.629 ± 0.303 | 0.682 ± 0.061 | 0.446 ± 0.031 | 0.537 ± 0.017 | 0.645 ± 0.136 | 0.658 ± 0.119 | 0.640 ± 0.074 |
| kalman / generator: seen | 0.865 ± 0.016 | 0.684 ± 0.036 | 0.764 ± 0.028 | 0.011 ± 0.001 | 0.839 ± 0.024 | 0.528 ± 0.029 | 0.510 ± 0.002 | 0.052 ± 0.023 | 0.338 ± 0.037 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.968 ± 0.008 | 0.905 ± 0.010 | 0.334 ± 0.020 | 0.488 ± 0.021 | 0.831 ± 0.015 | 0.992 ± 0.003 | 0.904 ± 0.009 |
| kalman / generator: unseen | 0.452 ± 0.017 | 0.631 ± 0.009 | 0.527 ± 0.014 | 0.077 ± 0.004 | 0.615 ± 0.017 | 0.550 ± 0.032 | 0.575 ± 0.096 | 0.291 ± 0.056 | 0.058 ± 0.004 | 0.916 ± 0.073 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.636 ± 0.028 | 0.329 ± 0.026 | 0.297 ± 0.030 | 0.312 ± 0.026 | 0.468 ± 0.012 | 0.875 ± 0.042 | 0.609 ± 0.011 |

## Test 1, grid_frequency, at the baseline's false alarms

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform | precision_sensor_malfunction | recall_sensor_malfunction | f1_sensor_malfunction | precision_not_sensor_data | recall_not_sensor_data | f1_not_sensor_data |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| none | 0.948 ± 0.003 | 0.770 ± 0.014 | 0.850 ± 0.008 | 0.004 ± 0.000 | 0.918 ± 0.004 | 0.853 ± 0.012 | 0.928 ± 0.020 | 0.073 ± 0.023 | 0.824 ± 0.016 | 0.998 ± 0.002 | 0.999 ± 0.001 | 1.000 ± 0.000 | 0.477 ± 0.043 | 0.953 ± 0.005 | 0.647 ± 0.015 | 0.770 ± 0.009 | 0.910 ± 0.008 | 0.863 ± 0.015 | 0.886 ± 0.011 |
| median | 0.932 ± 0.004 | 0.586 ± 0.004 | 0.720 ± 0.002 | 0.004 ± 0.000 | 0.803 ± 0.004 | 0.561 ± 0.031 | 0.184 ± 0.006 | 0.123 ± 0.022 | 0.643 ± 0.016 | 0.950 ± 0.009 | 0.999 ± 0.001 | 0.998 ± 0.002 | 0.238 ± 0.015 | 0.810 ± 0.025 | 0.285 ± 0.005 | 0.422 ± 0.009 | 0.830 ± 0.014 | 0.755 ± 0.004 | 0.791 ± 0.005 |
| ema | 0.939 ± 0.003 | 0.648 ± 0.015 | 0.767 ± 0.010 | 0.004 ± 0.000 | 0.854 ± 0.004 | 0.425 ± 0.027 | 0.777 ± 0.037 | 0.065 ± 0.006 | 0.621 ± 0.033 | 0.995 ± 0.003 | 0.999 ± 0.001 | 0.997 ± 0.000 | 0.305 ± 0.021 | 0.878 ± 0.005 | 0.418 ± 0.021 | 0.566 ± 0.018 | 0.894 ± 0.006 | 0.813 ± 0.011 | 0.851 ± 0.009 |
| kalman | 0.938 ± 0.003 | 0.641 ± 0.014 | 0.761 ± 0.009 | 0.004 ± 0.000 | 0.852 ± 0.007 | 0.533 ± 0.017 | 0.728 ± 0.038 | 0.075 ± 0.007 | 0.506 ± 0.030 | 0.996 ± 0.001 | 0.999 ± 0.001 | 0.999 ± 0.001 | 0.295 ± 0.021 | 0.882 ± 0.005 | 0.399 ± 0.015 | 0.549 ± 0.014 | 0.884 ± 0.011 | 0.811 ± 0.010 | 0.846 ± 0.009 |

## Test 1, grid_frequency_100ms, at the baseline's false alarms

| | precision | recall | f1 | false_alarms | pr_auc | recall_stuck | recall_spike | recall_noise | recall_oscillation | recall_sine | recall_ramp | recall_pattern | recall_uniform | precision_sensor_malfunction | recall_sensor_malfunction | f1_sensor_malfunction | precision_not_sensor_data | recall_not_sensor_data | f1_not_sensor_data |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| none | 0.913 ± 0.003 | 0.953 ± 0.007 | 0.933 ± 0.004 | 0.009 ± 0.001 | 0.984 ± 0.003 | 0.911 ± 0.026 | 0.924 ± 0.013 | 0.820 ± 0.033 | 0.989 ± 0.005 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.848 ± 0.005 | 0.905 ± 0.014 | 0.875 ± 0.008 | 0.981 ± 0.002 | 1.000 ± 0.000 | 0.990 ± 0.001 |
| median | 0.899 ± 0.003 | 0.809 ± 0.017 | 0.852 ± 0.010 | 0.009 ± 0.001 | 0.870 ± 0.012 | 0.717 ± 0.030 | 0.021 ± 0.012 | 0.798 ± 0.040 | 0.997 ± 0.005 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.997 ± 0.005 | 0.740 ± 0.009 | 0.554 ± 0.051 | 0.633 ± 0.037 | 0.916 ± 0.018 | 0.967 ± 0.005 | 0.941 ± 0.008 |
| ema | 0.911 ± 0.003 | 0.923 ± 0.011 | 0.917 ± 0.005 | 0.009 ± 0.001 | 0.965 ± 0.005 | 0.609 ± 0.073 | 0.932 ± 0.027 | 0.856 ± 0.026 | 0.994 ± 0.005 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.829 ± 0.004 | 0.837 ± 0.025 | 0.833 ± 0.014 | 0.979 ± 0.004 | 0.997 ± 0.002 | 0.988 ± 0.003 |
| kalman | 0.909 ± 0.003 | 0.911 ± 0.013 | 0.910 ± 0.006 | 0.009 ± 0.001 | 0.961 ± 0.005 | 0.582 ± 0.079 | 0.900 ± 0.018 | 0.819 ± 0.008 | 0.997 ± 0.005 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.817 ± 0.009 | 0.801 ± 0.035 | 0.809 ± 0.020 | 0.968 ± 0.006 | 0.992 ± 0.004 | 0.980 ± 0.002 |

## False alarms of the baseline

| | windows | n_haar_d1_energy | sign_flip_share | diff_autocorr | rolling_variance_ratio | max_abs_step |
|---|---|---|---|---|---|---|
| grid_frequency: flagged clean | 204.667 ± 12.503 | 0.314 ± 0.015 | 0.715 ± 0.013 | -0.554 ± 0.010 | 1.773 ± 0.050 | 2.389 ± 0.023 |
| grid_frequency: unflagged clean | 48504.333 ± 27.502 | 0.036 ± 0.000 | 0.333 ± 0.000 | 0.379 ± 0.001 | 3.439 ± 0.003 | 0.885 ± 0.001 |
| grid_frequency: injected noise | 592.667 ± 27.154 | 0.285 ± 0.014 | 0.667 ± 0.000 | -0.473 ± 0.008 | 2.250 ± 0.050 | 2.669 ± 0.054 |
| grid_frequency_100ms: flagged clean | 96.333 ± 7.024 | 0.311 ± 0.016 | 0.662 ± 0.013 | -0.497 ± 0.017 | 42.554 ± 32.476 | 4.426 ± 0.301 |
| grid_frequency_100ms: unflagged clean | 10322.333 ± 36.364 | 0.247 ± 0.000 | 0.672 ± 0.000 | -0.529 ± 0.001 | 5.944 ± 0.008 | 1.325 ± 0.001 |
| grid_frequency_100ms: injected noise | 147.333 ± 8.737 | 0.336 ± 0.003 | 0.670 ± 0.006 | -0.501 ± 0.005 | 4.190 ± 0.132 | 3.701 ± 0.040 |
```

## 4. Argument

### 4.1 Why no filter passes

The rule fails on several counts for each filter; the first failure is enough.

| | false alarms 1 s | false alarms 100 ms | PR-AUC 1 s | PR-AUC 100 ms | largest recall drop, 1 s | largest recall drop, 100 ms |
|---|---|---|---|---|---|---|
| `none` | 0.004 ± 0.000 | 0.009 ± 0.001 | 0.918 | 0.984 | | |
| `median` | 0.007 ± 0.001 | 0.005 ± 0.002 | 0.803 | 0.870 | spike 0.928 → 0.195 | spike 0.924 → 0.018 |
| `ema` | 0.007 ± 0.001 | 0.004 ± 0.001 | 0.854 | 0.965 | stuck 0.853 → 0.447 | stuck 0.911 → 0.554 |
| `kalman` | 0.006 ± 0.001 | 0.005 ± 0.002 | 0.852 | 0.961 | stuck 0.853 → 0.536 | stuck 0.911 → 0.546 |

- **1 s:** false alarms rise with every filter, so the first condition fails outright.
  Recall also drops beyond the tolerance on stuck values, spikes, oscillation and
  `uniform`.
- **100 ms:** false alarms fall by more than the floor, but stuck values (91 % to
  55-66 %) and noise (82 % to 71-73 %) drop far beyond seed noise, and the median filter
  erases spikes.
- **Cost is not the issue:** each filter adds a few milliseconds (p50) per live-sized
  window (42 sensors × 200 readings): about 2 ms on an idle machine, 6-9 ms in the run
  pasted above, which ran on a loaded one.
- **The rows the rule does not gate say the same:** on the generator false alarms rise
  (0.015 to 0.023-0.037) and PR-AUC falls (0.887 to 0.78-0.85).

### 4.2 Noise recall, at the same false-alarm rate

Each filter re-picks its own threshold, so at the live threshold the filters flag
different shares of clean windows; their 1 s noise recall rises (0.073 to 0.099-0.177)
while their false alarms rise too (0.004 to 0.006-0.007). At the baseline's false-alarm
rate (test 1 recall, mean ± sd over the seeds):

| | noise 1 s | spike 1 s | stuck 1 s | noise 100 ms | spike 100 ms | stuck 100 ms |
|---|---|---|---|---|---|---|
| `none` | 0.073 ± 0.023 | 0.928 ± 0.020 | 0.853 ± 0.012 | 0.820 ± 0.033 | 0.924 ± 0.013 | 0.911 ± 0.026 |
| `median` | 0.123 ± 0.022 | 0.184 ± 0.006 | 0.561 ± 0.031 | 0.798 ± 0.040 | 0.021 ± 0.012 | 0.717 ± 0.030 |
| `ema` | 0.065 ± 0.006 | 0.777 ± 0.037 | 0.425 ± 0.027 | 0.856 ± 0.026 | 0.932 ± 0.027 | 0.609 ± 0.073 |
| `kalman` | 0.075 ± 0.007 | 0.728 ± 0.038 | 0.533 ± 0.017 | 0.819 ± 0.008 | 0.900 ± 0.018 | 0.582 ± 0.079 |

- **EMA and Kalman:** their 1 s noise gain was the looser threshold; at the same false
  alarms they catch what no filter catches. On 100 ms EMA's noise recall is about one
  seed sd higher (0.856 vs 0.820), within seed noise.
- **Median:** keeps a 1 s noise gain (0.073 to 0.123, about two sds), but it is the
  filter that erases spikes (0.928 to 0.184 at 1 s, 0.924 to 0.021 at 100 ms).
- **Stuck values fall with every filter at any threshold** (1 s 0.853 to 0.43-0.56,
  100 ms 0.911 to 0.58-0.72): smoothing makes every healthy window flatter (section
  4.3).
- So the comparison at one operating point says the same as the rule: no filter makes
  the model better at noise without making it worse at other kinds.

### 4.3 What the baseline's false alarms are

- **1 s: they look like noise.** The 205 clean windows a seed flags (of about 48,700)
  have the texture of the injected noise windows, not of the clean windows the model
  lets through: finest-scale energy share 0.314 (injected noise 0.285, unflagged clean
  0.036), steps changing sign 72 % of the time (67 %, 33 %), each step anti-correlated
  with the next (-0.55, -0.47, +0.38). So healthy jitter does trigger them. But a
  filter cannot separate the two: it smooths the injected noise exactly as it smooths
  the jitter, so the model loses both, and its own threshold, re-picked on the
  smoothed windows, ends up flagging more clean windows, not fewer.
- **100 ms: they look like bursts and spikes, not noise.** The 96 flagged clean windows
  have the noise texture of the unflagged ones (finest-scale energy 0.311 vs 0.247,
  sign flips 66 % vs 67 %), but a much larger rolling-variance ratio (42.6 vs 5.9) and
  largest step (4.4 vs 1.3 robust spreads). There is no extra noise for a filter to
  remove; the false-alarm drop the filters buy here comes from the model seeing less,
  and the stuck and noise faults go with it.
- **Why stuck values suffer:** a stuck value shows as a window that is flatter than the
  sensor's normal variation. A filter makes every healthy window flatter, so the
  difference the model learned shrinks (stuck recall 85-91 % to 45-66 %).

### 4.4 What the filters do help: an unseen, much noisier source

Reported only (not gated): a model trained without the generator scores the generator
better when the series are filtered (PR-AUC 0.201 without a filter, 0.588-0.615 with
one), and the same holds for the 100 ms source held out (0.733 to 0.807-0.809 with
`ema` or `kalman`; `median` 0.642). Held out, 1 s grid gets worse with `ema` and
`kalman` (0.693 to 0.646-0.647) and slightly better with `median` (0.709). Smoothing
pulls a much noisier source closer to what the model trained on. But the generator's
noise is synthetic and far above real sensors', the in-source cost above is large, and
the agreed rule judges in-source real data; this is a lead for a source the model has
never seen, not a reason to clean.

## 5. Decision

Part 1 recommended no cleaning step. The lead decided on PR #114 that the readings
must be cleaned of noise before the model, in training and live: a model whose results
depend on the noise is not one to trust, and a less performant model that can be
trusted is better than one the noise can move. Both models get the cleaning (the
short-window and the long-window one), and improvements beyond it (the fault
injection among them) wait for a later stage.

Kalman was chosen of the three: the median erases spikes (section 4.1), and Kalman
ranks as well as EMA while keeping more stuck values and doing better on the unseen
generator; it weighs each reading by the noise it estimates from the window, so it
needs no setting per sensor type. Part 1's Kalman had one setting, so sections 7 and 8
tune it, each model on its own data. The keep rule of section 2 is not applied there:
the cleaning ships, and its false alarms are reported.

## 6. Caveats

- **One setting per filter.** A gentler filter (a 3-reading median, a larger EMA alpha)
  would lose fewer faults but also remove less; with false alarms rising on 1 s for all
  three, a gentler one is unlikely to make them fall.
- **Grid frequency only.** It is the only real source fast enough for a 20 s window;
  noisier real sensor types at this rate are not available to test.
- **The generator and unseen sources do not gate** (decided before the run); section
  4.4 reports them.
- **The same-false-alarm comparison picks its threshold on the test windows** (section
  4.2): fine for comparing filters at one operating point, not a threshold to deploy.
- **The profile compares texture, not amplitude:** its five columns are scale-free, so
  it shows what the flagged windows look like, not how much raw jitter they carry.
- **The baseline was rebuilt** with the merged #96 code (the earlier cached tables were
  from before the merge); its numbers match `research/short_window/results.md`.

## 7. Part 2: Kalman tuned for the short-window model

### 7.1 Setup

- **The cleaning** (`pipelines/denoise.py`, `preprocess.clean_method: kalman`): a 1-D
  local-level Kalman over each sensor's raw series, before the short-window columns,
  in training (`wide_window`) and live (`PreProcess.short_features`). The settings
  (`preprocess.kalman`):
  - `mode`: `filter` (forward only) or `smoother` (a backward pass also uses the
    readings after each point; it does not smooth across a jump);
  - `noise`: `fixed` (measurement noise from the window, median(\|step\|) /
    (0.6745 × √2), process noise `process_ratio` × it) or `estimated` (both from the
    window's one- and two-step changes, process noise × `process_scale`, at least
    `process_floor` × the measurement noise);
  - `jump_sigmas`: a reading this many prediction sds from the forecast is taken as it
    is and the filter restarts there (null: every reading is smoothed);
  - `keep_repeats`: every reading in a run of 2 or more equal readings (within 1e-9
    relative, `flat_run_share`'s test) comes out as it is. A stuck value then stays
    exactly flat; part 1's Kalman smoothed it, which is where its stuck recall went
    (section 4.3). On for every setup below; off only for part 1's tested setup, which
    sections 3 and 10 reproduce.
- **Scored on validation windows only:** for each setup section 8's model is refitted
  on its fit windows and scored on its validation windows; the test windows are not
  used to pick. Per source: PR-AUC (`val_pr_auc_<source>`) and false alarms at the
  threshold of the source's rate (`alarms_<source>`); each of the three sources is
  also held out of training in turn and scored by a model fitted on the other two
  (`unseen_pr_auc_<source>`).
- **The pick:** the highest mean PR-AUC on the held-out sources (`unseen_mean`, the
  generator included); an exact tie goes to the simpler setup. False alarms are
  reported, not gated: the cleaning ships.
- **The search** (injection seed 0, then the best three on seeds 0-2):
  - stage A: `filter` or `smoother`, without jumps or with jumps at 3 sds;
  - stage B, on A's best: `fixed` noise at ratio 0.03, 0.1, 0.3, 1, and `estimated`
    at scale 0.5, 1, 2; an end of a grid that wins is extended, at most 2 steps;
  - stage C, if a jump setup won A: jumps at 2, 3, 4 sds, extended the same way;
  - the three best by `unseen_mean`, on seeds 0-2; the pick is the best mean.
- **No cleaning** is scored the same way, from section 10's cached tables.
- **Reproduce:** section 11 of `research/research.ipynb` (`kalman_main()`, needs
  sections 8 and 10's cells). Each setup builds its tables once (cached); the run
  below took 6842 s.

### 7.2 Results

Output of `kalman_main()`, pasted verbatim.

```
## Stage A: filter or smoother, jumps

| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean | alarms_generator | alarms_grid_frequency | alarms_grid_frequency_100ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| no cleaning | 0.890 ± 0.000 | 0.898 ± 0.000 | 0.987 ± 0.000 | 0.925 ± 0.000 | 0.205 ± 0.000 | 0.801 ± 0.000 | 0.719 ± 0.000 | 0.575 ± 0.000 | 0.015 ± 0.000 | 0.007 ± 0.000 | 0.005 ± 0.000 |
| filter, fixed ratio 0.1, no jumps | 0.864 ± 0.000 | 0.870 ± 0.000 | 0.985 ± 0.000 | 0.906 ± 0.000 | 0.215 ± 0.000 | 0.744 ± 0.000 | 0.800 ± 0.000 | 0.586 ± 0.000 | 0.020 ± 0.000 | 0.013 ± 0.000 | 0.005 ± 0.000 |
| filter, fixed ratio 0.1, jumps 3 | 0.889 ± 0.000 | 0.885 ± 0.000 | 0.985 ± 0.000 | 0.919 ± 0.000 | 0.212 ± 0.000 | 0.803 ± 0.000 | 0.468 ± 0.000 | 0.494 ± 0.000 | 0.025 ± 0.000 | 0.013 ± 0.000 | 0.004 ± 0.000 |
| smoother, fixed ratio 0.1, no jumps | 0.841 ± 0.000 | 0.862 ± 0.000 | 0.975 ± 0.000 | 0.893 ± 0.000 | 0.258 ± 0.000 | 0.827 ± 0.000 | 0.645 ± 0.000 | 0.577 ± 0.000 | 0.052 ± 0.000 | 0.028 ± 0.000 | 0.003 ± 0.000 |
| smoother, fixed ratio 0.1, jumps 3 | 0.892 ± 0.000 | 0.888 ± 0.000 | 0.972 ± 0.000 | 0.917 ± 0.000 | 0.270 ± 0.000 | 0.840 ± 0.000 | 0.719 ± 0.000 | 0.610 ± 0.000 | 0.026 ± 0.000 | 0.014 ± 0.000 | 0.005 ± 0.000 |

## Stage B: fixed noise

| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean | alarms_generator | alarms_grid_frequency | alarms_grid_frequency_100ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| smoother, fixed ratio 0.03, jumps 3 | 0.872 ± 0.000 | 0.877 ± 0.000 | 0.971 ± 0.000 | 0.907 ± 0.000 | 0.266 ± 0.000 | 0.813 ± 0.000 | 0.717 ± 0.000 | 0.599 ± 0.000 | 0.033 ± 0.000 | 0.014 ± 0.000 | 0.010 ± 0.000 |
| smoother, fixed ratio 0.1, jumps 3 | 0.892 ± 0.000 | 0.888 ± 0.000 | 0.972 ± 0.000 | 0.917 ± 0.000 | 0.270 ± 0.000 | 0.840 ± 0.000 | 0.719 ± 0.000 | 0.610 ± 0.000 | 0.026 ± 0.000 | 0.014 ± 0.000 | 0.005 ± 0.000 |
| smoother, fixed ratio 0.3, jumps 3 | 0.908 ± 0.000 | 0.901 ± 0.000 | 0.984 ± 0.000 | 0.931 ± 0.000 | 0.253 ± 0.000 | 0.852 ± 0.000 | 0.689 ± 0.000 | 0.598 ± 0.000 | 0.016 ± 0.000 | 0.008 ± 0.000 | 0.003 ± 0.000 |
| smoother, fixed ratio 1, jumps 3 | 0.907 ± 0.000 | 0.902 ± 0.000 | 0.987 ± 0.000 | 0.932 ± 0.000 | 0.220 ± 0.000 | 0.838 ± 0.000 | 0.579 ± 0.000 | 0.546 ± 0.000 | 0.012 ± 0.000 | 0.006 ± 0.000 | 0.004 ± 0.000 |

## Stage B: estimated noise

| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean | alarms_generator | alarms_grid_frequency | alarms_grid_frequency_100ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| smoother, estimated scale 0.5, jumps 3 | 0.869 ± 0.000 | 0.818 ± 0.000 | 0.940 ± 0.000 | 0.875 ± 0.000 | 0.174 ± 0.000 | 0.773 ± 0.000 | 0.622 ± 0.000 | 0.523 ± 0.000 | 0.005 ± 0.000 | 0.004 ± 0.000 | 0.006 ± 0.000 |
| smoother, estimated scale 1, jumps 3 | 0.873 ± 0.000 | 0.823 ± 0.000 | 0.945 ± 0.000 | 0.880 ± 0.000 | 0.204 ± 0.000 | 0.774 ± 0.000 | 0.613 ± 0.000 | 0.530 ± 0.000 | 0.007 ± 0.000 | 0.006 ± 0.000 | 0.004 ± 0.000 |
| smoother, estimated scale 2, jumps 3 | 0.866 ± 0.000 | 0.832 ± 0.000 | 0.951 ± 0.000 | 0.883 ± 0.000 | 0.176 ± 0.000 | 0.762 ± 0.000 | 0.628 ± 0.000 | 0.522 ± 0.000 | 0.006 ± 0.000 | 0.005 ± 0.000 | 0.009 ± 0.000 |

## Stage C: jump threshold

| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean | alarms_generator | alarms_grid_frequency | alarms_grid_frequency_100ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| smoother, fixed ratio 0.1, jumps 2 | 0.862 ± 0.000 | 0.861 ± 0.000 | 0.968 ± 0.000 | 0.897 ± 0.000 | 0.250 ± 0.000 | 0.796 ± 0.000 | 0.639 ± 0.000 | 0.562 ± 0.000 | 0.017 ± 0.000 | 0.011 ± 0.000 | 0.005 ± 0.000 |
| smoother, fixed ratio 0.1, jumps 3 | 0.892 ± 0.000 | 0.888 ± 0.000 | 0.972 ± 0.000 | 0.917 ± 0.000 | 0.270 ± 0.000 | 0.840 ± 0.000 | 0.719 ± 0.000 | 0.610 ± 0.000 | 0.026 ± 0.000 | 0.014 ± 0.000 | 0.005 ± 0.000 |
| smoother, fixed ratio 0.1, jumps 4 | 0.899 ± 0.000 | 0.898 ± 0.000 | 0.977 ± 0.000 | 0.925 ± 0.000 | 0.289 ± 0.000 | 0.844 ± 0.000 | 0.724 ± 0.000 | 0.619 ± 0.000 | 0.028 ± 0.000 | 0.012 ± 0.000 | 0.006 ± 0.000 |

## Stage C: jump threshold, past the edge

| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean | alarms_generator | alarms_grid_frequency | alarms_grid_frequency_100ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| smoother, fixed ratio 0.1, jumps 5 | 0.899 ± 0.000 | 0.904 ± 0.000 | 0.977 ± 0.000 | 0.926 ± 0.000 | 0.287 ± 0.000 | 0.848 ± 0.000 | 0.689 ± 0.000 | 0.608 ± 0.000 | 0.026 ± 0.000 | 0.010 ± 0.000 | 0.003 ± 0.000 |

## Confirmed on all seeds

| | val_pr_auc_generator | val_pr_auc_grid_frequency | val_pr_auc_grid_frequency_100ms | val_mean | unseen_pr_auc_generator | unseen_pr_auc_grid_frequency | unseen_pr_auc_grid_frequency_100ms | unseen_mean | alarms_generator | alarms_grid_frequency | alarms_grid_frequency_100ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| no cleaning | 0.885 ± 0.006 | 0.892 ± 0.008 | 0.988 ± 0.001 | 0.921 ± 0.003 | 0.201 ± 0.006 | 0.786 ± 0.013 | 0.739 ± 0.019 | 0.575 ± 0.003 | 0.015 ± 0.000 | 0.007 ± 0.001 | 0.005 ± 0.001 |
| smoother, fixed ratio 0.1, jumps 4 | 0.892 ± 0.011 | 0.895 ± 0.003 | 0.982 ± 0.004 | 0.923 ± 0.004 | 0.299 ± 0.032 | 0.813 ± 0.027 | 0.664 ± 0.053 | 0.592 ± 0.025 | 0.031 ± 0.003 | 0.013 ± 0.001 | 0.005 ± 0.001 |
| smoother, fixed ratio 0.1, jumps 3 | 0.883 ± 0.008 | 0.884 ± 0.004 | 0.979 ± 0.006 | 0.915 ± 0.003 | 0.289 ± 0.037 | 0.806 ± 0.029 | 0.655 ± 0.056 | 0.583 ± 0.026 | 0.028 ± 0.003 | 0.013 ± 0.001 | 0.004 ± 0.001 |
| smoother, fixed ratio 0.1, jumps 5 | 0.893 ± 0.007 | 0.901 ± 0.002 | 0.982 ± 0.004 | 0.925 ± 0.002 | 0.280 ± 0.013 | 0.814 ± 0.031 | 0.667 ± 0.020 | 0.587 ± 0.019 | 0.028 ± 0.002 | 0.011 ± 0.001 | 0.003 ± 0.001 |

seconds: 6842
picked: smoother, fixed ratio 0.1, jumps 4
settings: {"mode": "smoother", "noise": "fixed", "process_ratio": 0.1, "process_scale": 1.0, "process_floor": 0.001, "jump_sigmas": 4.0, "keep_repeats": true}
```

**Picked: smoother, fixed ratio 0.1, jumps at 4 sds** (`preprocess.kalman`, with
`keep_repeats: true`).

- **Jumps matter most.** Without them the smoother scores 0.577 and the filter 0.586
  (no cleaning 0.575); with jumps at 3 sds the smoother reaches 0.610 on seed 0.
- **Estimated noise loses** (0.522-0.530, below no cleaning); the fixed ratio is best
  at 0.1 (0.03 and 0.3 next, 1 is worst).
- **The pick is not clearly better than its neighbours.** On seeds 0-2 jumps at 4, 5
  and 3 sds score 0.592 ± 0.025, 0.587 ± 0.019 and 0.583 ± 0.026: within one seed sd.
  No cleaning scores 0.575 ± 0.003; the gain is about one seed sd too.
- **Validation PR-AUC on the trained sources barely moves** (0.921 no cleaning, 0.923
  the pick); validation false alarms rise on the generator (0.015 to 0.031) and on 1 s
  (0.007 to 0.013).

### 7.3 Before and after

Section 10's `clean_main()` with the shipped setup as a fifth row (`kalman, tuned`,
the default preprocess tables), on the test windows, mean ± sd over injection seeds
0-2. The shipped model (`models/short_window`) is section 8's `short_main()` under
the same default; its test rows are these.

At the threshold of the source's rate (what the detector applies):

Test 1, grid_frequency (1 s)

| | PR-AUC | false alarms | recall | stuck | spike | noise | oscillation |
|---|---|---|---|---|---|---|---|
| no filter | 0.918 ± 0.004 | 0.004 ± 0.000 | 0.770 ± 0.014 | 0.853 ± 0.012 | 0.928 ± 0.020 | 0.073 ± 0.023 | 0.824 ± 0.016 |
| Kalman, tested (part 1) | 0.852 ± 0.007 | 0.006 ± 0.001 | 0.664 ± 0.012 | 0.536 ± 0.018 | 0.756 ± 0.027 | 0.099 ± 0.011 | 0.557 ± 0.013 |
| Kalman, tuned (shipped) | 0.909 ± 0.002 | 0.008 ± 0.001 | 0.739 ± 0.007 | 0.847 ± 0.019 | 0.888 ± 0.018 | 0.210 ± 0.018 | 0.521 ± 0.021 |

Test 1, grid_frequency_100ms

| | PR-AUC | false alarms | recall | stuck | spike | noise | oscillation |
|---|---|---|---|---|---|---|---|
| no filter | 0.984 ± 0.003 | 0.009 ± 0.001 | 0.953 ± 0.007 | 0.911 ± 0.026 | 0.924 ± 0.013 | 0.820 ± 0.033 | 0.989 ± 0.005 |
| Kalman, tested (part 1) | 0.961 ± 0.005 | 0.005 ± 0.002 | 0.887 ± 0.025 | 0.546 ± 0.070 | 0.848 ± 0.059 | 0.728 ± 0.063 | 0.991 ± 0.009 |
| Kalman, tuned (shipped) | 0.981 ± 0.003 | 0.007 ± 0.001 | 0.937 ± 0.010 | 0.927 ± 0.030 | 0.891 ± 0.028 | 0.745 ± 0.077 | 0.965 ± 0.031 |

Test 3, generator

| | PR-AUC | false alarms | recall | stuck | spike | noise | oscillation |
|---|---|---|---|---|---|---|---|
| no filter | 0.887 ± 0.012 | 0.015 ± 0.001 | 0.788 ± 0.026 | 0.728 ± 0.044 | 0.803 ± 0.050 | 0.084 ± 0.039 | 0.606 ± 0.079 |
| Kalman, tested (part 1) | 0.839 ± 0.024 | 0.023 ± 0.001 | 0.729 ± 0.042 | 0.544 ± 0.029 | 0.606 ± 0.061 | 0.136 ± 0.053 | 0.480 ± 0.049 |
| Kalman, tuned (shipped) | 0.892 ± 0.012 | 0.031 ± 0.003 | 0.831 ± 0.011 | 0.804 ± 0.041 | 0.918 ± 0.066 | 0.203 ± 0.060 | 0.646 ± 0.062 |

At the baseline's false alarms (the threshold that flags as many clean test windows as
no filter does; section 2):

Test 1, grid_frequency (1 s)

| | PR-AUC | false alarms | recall | stuck | spike | noise | oscillation |
|---|---|---|---|---|---|---|---|
| no filter | 0.918 ± 0.004 | 0.004 ± 0.000 | 0.770 ± 0.014 | 0.853 ± 0.012 | 0.928 ± 0.020 | 0.073 ± 0.023 | 0.824 ± 0.016 |
| Kalman, tested (part 1) | 0.852 ± 0.007 | 0.004 ± 0.000 | 0.641 ± 0.014 | 0.533 ± 0.017 | 0.728 ± 0.038 | 0.075 ± 0.007 | 0.506 ± 0.030 |
| Kalman, tuned (shipped) | 0.909 ± 0.002 | 0.004 ± 0.000 | 0.676 ± 0.012 | 0.827 ± 0.013 | 0.862 ± 0.014 | 0.093 ± 0.004 | 0.409 ± 0.044 |

Test 1, grid_frequency_100ms

| | PR-AUC | false alarms | recall | stuck | spike | noise | oscillation |
|---|---|---|---|---|---|---|---|
| no filter | 0.984 ± 0.003 | 0.009 ± 0.001 | 0.953 ± 0.007 | 0.911 ± 0.026 | 0.924 ± 0.013 | 0.820 ± 0.033 | 0.989 ± 0.005 |
| Kalman, tested (part 1) | 0.961 ± 0.005 | 0.009 ± 0.001 | 0.911 ± 0.013 | 0.582 ± 0.079 | 0.900 ± 0.018 | 0.819 ± 0.008 | 0.997 ± 0.005 |
| Kalman, tuned (shipped) | 0.981 ± 0.003 | 0.009 ± 0.001 | 0.947 ± 0.007 | 0.932 ± 0.034 | 0.906 ± 0.035 | 0.791 ± 0.045 | 0.973 ± 0.026 |

Each source held out of training (the pooled threshold of the other two):

| | 1 s PR-AUC | 1 s false alarms | 100 ms PR-AUC | 100 ms false alarms | generator PR-AUC | generator false alarms |
|---|---|---|---|---|---|---|
| no filter | 0.693 ± 0.011 | 0.110 ± 0.026 | 0.733 ± 0.013 | 0.023 ± 0.009 | 0.201 ± 0.009 | 0.215 ± 0.005 |
| Kalman, tested (part 1) | 0.647 ± 0.032 | 0.063 ± 0.019 | 0.809 ± 0.027 | 0.018 ± 0.011 | 0.615 ± 0.017 | 0.077 ± 0.004 |
| Kalman, tuned (shipped) | 0.781 ± 0.009 | 0.010 ± 0.003 | 0.657 ± 0.051 | 0.023 ± 0.022 | 0.303 ± 0.031 | 0.226 ± 0.011 |

#96's target (section 8's `short_main()`; before: `research/short_window/results.md`):

| | test 2 clean false alarms | test 3 false alarms | 1 s PR-AUC, short minus Intel-only | 100 ms PR-AUC, short minus Intel-only | met |
|---|---|---|---|---|---|
| before (no cleaning) | 0.017 | 0.015 | 0.840 | 0.909 | yes |
| after (tuned Kalman) | 0.028 | 0.031 | 0.831 | 0.906 | yes |

The target: at most 0.04 false alarms on test 2's clean windows and on test 3, and a
PR-AUC above the Intel-only model's on both grid sources.

Latency, the filter alone on a live-sized window (`clean_main()`):

```
filter latency, 42 sensors x 200 readings: {"none": {"p50_ms": 0.010899995686486363, "p99_ms": 0.014313006540760386}, "median": {"p50_ms": 2.251600002637133, "p99_ms": 3.5126539986231364}, "ema": {"p50_ms": 2.7275999964331277, "p99_ms": 2.931265995866852}, "kalman": {"p50_ms": 3.5344999996596016, "p99_ms": 4.03215999569511}, "kalman, tuned": {"p50_ms": 8.878550004737917, "p99_ms": 9.39868500092416}}
```

The tuned Kalman takes 8.9 ms (p50) on 42 sensors × 200 readings, against 3.5 ms for
part 1's filter-only Kalman: the smoother's backward pass. The whole short-window
path (`short_main()`'s latency line) takes 116.6 ms p50, against 101.6 ms in
`research/short_window/results.md`.

The `kalman, tuned` rows of `clean_main()`, verbatim:

```
| kalman, tuned | 0.903 ± 0.007 | 0.739 ± 0.007 | 0.813 ± 0.002 | 0.008 ± 0.001 | 0.909 ± 0.002 | 0.847 ± 0.019 | 0.888 ± 0.018 | 0.210 ± 0.018 | 0.521 ± 0.021 | 0.996 ± 0.001 | 0.997 ± 0.004 | 0.999 ± 0.001 | 0.463 ± 0.014 | 0.869 ± 0.012 | 0.537 ± 0.005 | 0.664 ± 0.001 | 0.821 ± 0.016 | 0.838 ± 0.002 | 0.829 ± 0.009 |
| kalman, tuned | 0.931 ± 0.009 | 0.937 ± 0.010 | 0.934 ± 0.004 | 0.007 ± 0.001 | 0.981 ± 0.003 | 0.927 ± 0.030 | 0.891 ± 0.028 | 0.745 ± 0.077 | 0.965 ± 0.031 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.997 ± 0.004 | 1.000 ± 0.000 | 0.859 ± 0.014 | 0.834 ± 0.024 | 0.846 ± 0.006 | 0.944 ± 0.010 | 0.984 ± 0.009 | 0.964 ± 0.001 |
| kalman, tuned | 0.729 ± 0.023 | 0.831 ± 0.011 | 0.777 ± 0.018 | 0.031 ± 0.003 | 0.892 ± 0.012 | 0.804 ± 0.041 | 0.918 ± 0.066 | 0.203 ± 0.060 | 0.646 ± 0.062 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.747 ± 0.009 | 0.605 ± 0.014 | 0.668 ± 0.008 | 0.688 ± 0.039 | 1.000 ± 0.000 | 0.814 ± 0.028 |
| kalman, tuned / grid_frequency: seen | 0.903 ± 0.007 | 0.739 ± 0.007 | 0.813 ± 0.002 | 0.008 ± 0.001 | 0.909 ± 0.002 | 0.847 ± 0.019 | 0.888 ± 0.018 | 0.210 ± 0.018 | 0.521 ± 0.021 | 0.996 ± 0.001 | 0.997 ± 0.004 | 0.999 ± 0.001 | 0.463 ± 0.014 | 0.869 ± 0.012 | 0.537 ± 0.005 | 0.664 ± 0.001 | 0.821 ± 0.016 | 0.838 ± 0.002 | 0.829 ± 0.009 |
| kalman, tuned / grid_frequency: unseen | 0.866 ± 0.031 | 0.623 ± 0.008 | 0.725 ± 0.007 | 0.010 ± 0.003 | 0.781 ± 0.009 | 0.736 ± 0.021 | 0.818 ± 0.017 | 0.030 ± 0.009 | 0.470 ± 0.035 | 0.981 ± 0.010 | 0.991 ± 0.003 | 0.927 ± 0.008 | 0.045 ± 0.006 | 0.674 ± 0.083 | 0.497 ± 0.017 | 0.571 ± 0.029 | 0.917 ± 0.027 | 0.636 ± 0.012 | 0.751 ± 0.001 |
| kalman, tuned / grid_frequency_100ms: seen | 0.931 ± 0.009 | 0.937 ± 0.010 | 0.934 ± 0.004 | 0.007 ± 0.001 | 0.981 ± 0.003 | 0.927 ± 0.030 | 0.891 ± 0.028 | 0.745 ± 0.077 | 0.965 ± 0.031 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.997 ± 0.004 | 1.000 ± 0.000 | 0.859 ± 0.014 | 0.834 ± 0.024 | 0.846 ± 0.006 | 0.944 ± 0.010 | 0.984 ± 0.009 | 0.964 ± 0.001 |
| kalman, tuned / grid_frequency_100ms: unseen | 0.718 ± 0.181 | 0.490 ± 0.033 | 0.573 ± 0.047 | 0.023 ± 0.022 | 0.657 ± 0.051 | 0.771 ± 0.024 | 0.630 ± 0.245 | 0.119 ± 0.071 | 0.465 ± 0.043 | 0.914 ± 0.095 | 0.000 ± 0.000 | 0.984 ± 0.021 | 0.086 ± 0.064 | 0.585 ± 0.220 | 0.447 ± 0.058 | 0.484 ± 0.065 | 0.880 ± 0.033 | 0.487 ± 0.024 | 0.627 ± 0.011 |
| kalman, tuned / generator: seen | 0.872 ± 0.018 | 0.771 ± 0.034 | 0.818 ± 0.023 | 0.011 ± 0.002 | 0.892 ± 0.012 | 0.783 ± 0.054 | 0.885 ± 0.075 | 0.043 ± 0.021 | 0.440 ± 0.116 | 1.000 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.927 ± 0.047 | 0.875 ± 0.022 | 0.520 ± 0.020 | 0.652 ± 0.014 | 0.847 ± 0.022 | 0.983 ± 0.010 | 0.910 ± 0.012 |
| kalman, tuned / generator: unseen | 0.261 ± 0.009 | 0.789 ± 0.027 | 0.392 ± 0.012 | 0.226 ± 0.011 | 0.303 ± 0.031 | 0.960 ± 0.004 | 0.842 ± 0.076 | 0.357 ± 0.137 | 0.504 ± 0.112 | 0.990 ± 0.017 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.627 ± 0.081 | 0.132 ± 0.003 | 0.578 ± 0.014 | 0.216 ± 0.004 | 0.497 ± 0.027 | 0.875 ± 0.017 | 0.634 ± 0.021 |
| kalman, tuned | 0.941 ± 0.003 | 0.676 ± 0.012 | 0.787 ± 0.008 | 0.004 ± 0.000 | 0.909 ± 0.002 | 0.827 ± 0.013 | 0.862 ± 0.014 | 0.093 ± 0.004 | 0.409 ± 0.044 | 0.995 ± 0.000 | 0.995 ± 0.003 | 0.999 ± 0.001 | 0.245 ± 0.011 | 0.924 ± 0.007 | 0.490 ± 0.015 | 0.640 ± 0.011 | 0.875 ± 0.008 | 0.795 ± 0.008 | 0.833 ± 0.008 |
| kalman, tuned | 0.913 ± 0.003 | 0.947 ± 0.007 | 0.929 ± 0.004 | 0.009 ± 0.001 | 0.981 ± 0.003 | 0.932 ± 0.034 | 0.906 ± 0.035 | 0.791 ± 0.045 | 0.973 ± 0.026 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.997 ± 0.004 | 1.000 ± 0.000 | 0.829 ± 0.008 | 0.853 ± 0.022 | 0.841 ± 0.008 | 0.941 ± 0.010 | 0.984 ± 0.009 | 0.962 ± 0.002 |
```

### 7.4 What the tuning changed

- **Stuck values are kept.** 1 s stuck recall: 0.853 without cleaning, 0.536 with
  part 1's Kalman, 0.847 tuned; 100 ms 0.911, 0.546, 0.927. Repeated readings stay
  raw, so a stuck value stays flat.
- **Spikes mostly survive** (1 s 0.928, 0.756, 0.888): a spike is a jump past 4 sds
  and passes as it is.
- **1 s noise recall rises (0.073 to 0.210), mostly through the looser threshold.** At
  the baseline's false alarms it is 0.093 (part 1's Kalman 0.075). Oscillation falls
  (1 s 0.824 to 0.521; 0.409 at the baseline's false alarms).
- **False alarms at the live threshold:** 1 s 0.004 to 0.008, 100 ms 0.009 to 0.007,
  generator 0.015 to 0.031; all within #96's budget.
- **Held-out sources:** 1 s grid 0.693 to 0.781, and its false alarms 0.110 to 0.010;
  the generator 0.201 to 0.303; 100 ms 0.733 to 0.657 (± 0.051). Part 1's Kalman did
  better on the generator (0.615) and 100 ms (0.809) and worse on 1 s (0.647).

## 8. Part 3: Kalman tuned for the long-window model

### 8.1 Setup

- **The cleaning** (`LongPreProcess.clean`, `long_window.clean_method: kalman`): the
  same Kalman (section 7.1) over each sensor's 5-minute bucket grid, before every
  long-window feature, in training (section 9's tables) and live. It has its own
  setup (`long_window.kalman`): a bucket mean holds far less noise than one reading,
  so the short model's pick need not be the best here.
- **Scored on validation rows only,** like section 7: section 9's late rows of the fit
  part, injection seed 0, 24 h windows. Per setup: the pooled validation PR-AUC, false
  alarms per sensor-day and stuck and drift recall of the model fitted on every
  source; and, for each source with both classes in its validation rows, the PR-AUC of
  a model fitted on the others (`unseen_<source>`, `unseen_mean` over them).
- **The pick:** the highest `unseen_mean`; false alarms reported, not gated.
- **The search** (one table build takes about 50 minutes, so it is small): the short
  model's pick (smoother, ratio 0.1, jumps 4), its ratio one step down (0.0333) and one
  step up (0.3), the pick as a filter, and one more step past an end that wins (0.9).
- **Reproduce:** section 12 of `research/research.ipynb` (`long_kalman_main()`, needs
  section 9's cells); the run below took 22647 s.

### 8.2 Results

Output of `long_kalman_main()`, pasted verbatim.

```
## Long-window Kalman tuning, seed 0, validation rows

| | val_pr_auc | val_false_alarms_per_sensor_day | val_recall_stuck | val_recall_drift | unseen_array_of_things | unseen_beijing | unseen_bellinge | unseen_chicago_beach | unseen_emergency_calls | unseen_ett | unseen_intel_lab | unseen_jena | unseen_melbourne | unseen_nab | unseen_noaa_tides | unseen_sensor_community | unseen_traffic_speeds | unseen_uci_air_quality | unseen_uci_household | unseen_uci_load | unseen_uk_flood | unseen_uk_traffic | unseen_usgs | unseen_mean |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| no cleaning | 0.959 ± 0.000 | 0.005 ± 0.000 | 0.651 ± 0.000 | 0.729 ± 0.000 | 0.420 ± 0.000 | 0.894 ± 0.000 | 0.554 ± 0.000 | 0.873 ± 0.000 | 1.000 ± 0.000 | 0.952 ± 0.000 | 0.647 ± 0.000 | 0.944 ± 0.000 | 0.920 ± 0.000 | 0.228 ± 0.000 | 1.000 ± 0.000 | 0.833 ± 0.000 | 0.838 ± 0.000 | 0.898 ± 0.000 | 0.954 ± 0.000 | 0.925 ± 0.000 | 0.906 ± 0.000 | 1.000 ± 0.000 | 0.500 ± 0.000 | 0.804 ± 0.000 |
| smoother, fixed ratio 0.1, jumps 4 | 0.949 ± 0.000 | 0.014 ± 0.000 | 0.696 ± 0.000 | 0.719 ± 0.000 | 0.405 ± 0.000 | 0.868 ± 0.000 | 0.625 ± 0.000 | 0.835 ± 0.000 | 1.000 ± 0.000 | 0.954 ± 0.000 | 0.557 ± 0.000 | 0.949 ± 0.000 | 0.902 ± 0.000 | 0.738 ± 0.000 | 0.962 ± 0.000 | 0.801 ± 0.000 | 0.821 ± 0.000 | 0.897 ± 0.000 | 0.949 ± 0.000 | 0.893 ± 0.000 | 0.899 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.845 ± 0.000 |
| smoother, fixed ratio 0.0333333, jumps 4 | 0.944 ± 0.000 | 0.014 ± 0.000 | 0.688 ± 0.000 | 0.699 ± 0.000 | 0.308 ± 0.000 | 0.860 ± 0.000 | 0.497 ± 0.000 | 0.835 ± 0.000 | 1.000 ± 0.000 | 0.956 ± 0.000 | 0.695 ± 0.000 | 0.944 ± 0.000 | 0.903 ± 0.000 | 0.767 ± 0.000 | 0.962 ± 0.000 | 0.788 ± 0.000 | 0.838 ± 0.000 | 0.877 ± 0.000 | 0.919 ± 0.000 | 0.882 ± 0.000 | 0.905 ± 0.000 | 0.982 ± 0.000 | 1.000 ± 0.000 | 0.838 ± 0.000 |
| smoother, fixed ratio 0.3, jumps 4 | 0.949 ± 0.000 | 0.014 ± 0.000 | 0.700 ± 0.000 | 0.742 ± 0.000 | 0.439 ± 0.000 | 0.877 ± 0.000 | 0.629 ± 0.000 | 0.837 ± 0.000 | 1.000 ± 0.000 | 0.959 ± 0.000 | 0.392 ± 0.000 | 0.940 ± 0.000 | 0.919 ± 0.000 | 0.758 ± 0.000 | 1.000 ± 0.000 | 0.878 ± 0.000 | 0.878 ± 0.000 | 0.873 ± 0.000 | 0.954 ± 0.000 | 0.912 ± 0.000 | 0.899 ± 0.000 | 1.000 ± 0.000 | 1.000 ± 0.000 | 0.850 ± 0.000 |
| filter, fixed ratio 0.1, jumps 4 | 0.949 ± 0.000 | 0.011 ± 0.000 | 0.700 ± 0.000 | 0.756 ± 0.000 | 0.450 ± 0.000 | 0.877 ± 0.000 | 0.554 ± 0.000 | 0.831 ± 0.000 | 1.000 ± 0.000 | 0.949 ± 0.000 | 0.704 ± 0.000 | 0.929 ± 0.000 | 0.907 ± 0.000 | 0.733 ± 0.000 | 1.000 ± 0.000 | 0.858 ± 0.000 | 0.811 ± 0.000 | 0.882 ± 0.000 | 0.955 ± 0.000 | 0.913 ± 0.000 | 0.906 ± 0.000 | 0.995 ± 0.000 | 1.000 ± 0.000 | 0.855 ± 0.000 |
| smoother, fixed ratio 0.9, jumps 4 | 0.953 ± 0.000 | 0.009 ± 0.000 | 0.695 ± 0.000 | 0.754 ± 0.000 | 0.450 ± 0.000 | 0.884 ± 0.000 | 0.599 ± 0.000 | 0.853 ± 0.000 | 1.000 ± 0.000 | 0.954 ± 0.000 | 0.582 ± 0.000 | 0.938 ± 0.000 | 0.903 ± 0.000 | 0.758 ± 0.000 | 1.000 ± 0.000 | 0.865 ± 0.000 | 0.867 ± 0.000 | 0.871 ± 0.000 | 0.956 ± 0.000 | 0.915 ± 0.000 | 0.902 ± 0.000 | 0.990 ± 0.000 | 1.000 ± 0.000 | 0.857 ± 0.000 |

seconds: 22647
picked: smoother, fixed ratio 0.9, jumps 4
settings: {"mode": "smoother", "noise": "fixed", "process_ratio": 0.9, "process_scale": 1.0, "process_floor": 0.001, "jump_sigmas": 4.0, "keep_repeats": true}
```

**Picked: smoother, fixed ratio 0.9, jumps at 4 sds** (`long_window.kalman`, with
`keep_repeats: true`).

- **On one seed, every cleaned setup beats no cleaning** on `unseen_mean` (0.838-0.857
  against 0.804), and they are too close to rank: 0.857, 0.855 and 0.850 for the top
  three.
- **Most of that gain is two sources:** nab 0.228 to 0.758 and usgs 0.500 to 1.000
  (both have few test windows: section 5 of `research/long_window/results.md`).
- **The pooled validation PR-AUC falls** (0.959 to 0.953) and false alarms rise (0.005
  to 0.009 per sensor-day).
- **0.9 is the edge step** the search allows past the grid; a weaker smoothing (a
  higher ratio) is untried.

### 8.3 Before and after

Section 9's `long_main()` under the new default, on the test windows, mean ± sd over
injection seeds 0-2, against the numbers in `research/long_window/results.md` (no
cleaning). The `Intel-only` rows, which read no cleaned grid, are equal in both runs
to the last digit, so the splits and the injected faults are the same.

All held-out windows, 24 h:

| | precision | recall | PR-AUC | false alarms / sensor-day | stuck | drift | sine | ramp | pattern | uniform |
|---|---|---|---|---|---|---|---|---|---|---|
| long-window, before | 0.919 ± 0.004 | 0.869 ± 0.002 | 0.951 ± 0.002 | 0.008 ± 0.001 | 0.643 ± 0.005 | 0.734 ± 0.009 | 0.974 ± 0.001 | 0.995 ± 0.001 | 0.946 ± 0.005 | 0.905 ± 0.010 |
| long-window, after | 0.883 ± 0.008 | 0.879 ± 0.005 | 0.947 ± 0.001 | 0.013 ± 0.001 | 0.675 ± 0.011 | 0.751 ± 0.011 | 0.968 ± 0.003 | 0.995 ± 0.000 | 0.952 ± 0.005 | 0.915 ± 0.009 |
| reuse only, before | 0.889 ± 0.032 | 0.827 ± 0.013 | 0.923 ± 0.001 | 0.011 ± 0.004 | 0.463 ± 0.023 | 0.640 ± 0.031 | 0.975 ± 0.005 | 0.993 ± 0.001 | 0.951 ± 0.008 | 0.910 ± 0.011 |
| reuse only, after | 0.868 ± 0.006 | 0.832 ± 0.006 | 0.918 ± 0.002 | 0.014 ± 0.001 | 0.472 ± 0.008 | 0.667 ± 0.017 | 0.969 ± 0.001 | 0.992 ± 0.001 | 0.948 ± 0.003 | 0.914 ± 0.004 |

#96's target (`metrics.json`):

| | clean false alarms / sensor-day | PR-AUC | PR-AUC gain over reuse only | PR-AUC gain over Intel-only | met |
|---|---|---|---|---|---|
| before (no cleaning) | 0.008 | 0.951 | 0.029 | 0.826 | yes |
| after (tuned Kalman) | 0.013 | 0.947 | 0.028 | 0.821 | yes |

```
target (<= 0.04 per sensor-day, PR-AUC above): met=True
{
  "clean false alarms per sensor-day": 0.01265697990823423,
  "PR-AUC minus reuse only": 0.02820571295996044,
  "PR-AUC minus Intel-only": 0.8213662841331965
}
```

Real faults (test 2) and events (test 3):

| | flagged, before | flagged, after | clean test windows flagged, before | clean test windows flagged, after |
|---|---|---|---|---|
| bellinge | 0.025 ± 0.002 | 0.043 ± 0.006 | 0.018 ± 0.000 | 0.026 ± 0.010 |
| intel_lab | 0.035 ± 0.002 | 0.055 ± 0.005 | 0.027 ± 0.004 | 0.061 ± 0.009 |
| emergency_calls | 0.000 ± 0.000 | 0.000 ± 0.000 | 0.000 ± 0.000 | 0.000 ± 0.000 |
| nab | 0.038 ± 0.038 | 0.077 ± 0.067 | 0.000 ± 0.000 | 0.000 ± 0.000 |

Per source: held out of the test windows (test 1) and unseen in training (test 4):

| source | held out PR-AUC, before | after | false alarms / sensor-day, before | after | unseen PR-AUC, before | after |
|---|---|---|---|---|---|---|
| array_of_things | 0.445 ± 0.021 | 0.440 ± 0.024 | 0.099 ± 0.003 | 0.147 ± 0.011 | 0.372 ± 0.047 | 0.408 ± 0.043 |
| beijing | 0.942 ± 0.004 | 0.935 ± 0.004 | 0.006 ± 0.001 | 0.014 ± 0.002 | 0.899 ± 0.001 | 0.883 ± 0.009 |
| bellinge | 0.826 ± 0.018 | 0.823 ± 0.010 | 0.018 ± 0.000 | 0.026 ± 0.010 | 0.613 ± 0.036 | 0.613 ± 0.007 |
| chicago_beach | 0.903 ± 0.004 | 0.888 ± 0.004 | 0.007 ± 0.001 | 0.013 ± 0.002 | 0.863 ± 0.009 | 0.842 ± 0.013 |
| emergency_calls | 0.978 ± 0.020 | 0.970 ± 0.026 | 0.000 ± 0.000 | 0.000 ± 0.000 | 0.974 ± 0.028 | 0.958 ± 0.042 |
| ett | 0.951 ± 0.009 | 0.953 ± 0.007 | 0.011 ± 0.003 | 0.009 ± 0.000 | 0.939 ± 0.014 | 0.940 ± 0.014 |
| intel_lab | 0.723 ± 0.035 | 0.653 ± 0.095 | 0.027 ± 0.004 | 0.061 ± 0.009 | 0.508 ± 0.038 | 0.398 ± 0.005 |
| jena | 0.945 ± 0.004 | 0.939 ± 0.006 | 0.006 ± 0.001 | 0.004 ± 0.004 | 0.933 ± 0.008 | 0.920 ± 0.013 |
| melbourne | 0.969 ± 0.002 | 0.963 ± 0.003 | 0.003 ± 0.000 | 0.005 ± 0.000 | 0.930 ± 0.004 | 0.916 ± 0.003 |
| nab | 0.705 ± 0.115 | 0.602 ± 0.108 | 0.000 ± 0.000 | 0.000 ± 0.000 | 0.617 ± 0.082 | 0.537 ± 0.108 |
| noaa_tides | 0.960 ± 0.046 | 0.974 ± 0.033 | 0.000 ± 0.000 | 0.000 ± 0.000 | 0.984 ± 0.028 | 0.992 ± 0.014 |
| sensor_community | 0.727 ± 0.072 | 0.733 ± 0.077 | 0.025 ± 0.005 | 0.028 ± 0.006 | 0.727 ± 0.072 | 0.733 ± 0.077 |
| traffic_speeds | 0.981 ± 0.006 | 0.981 ± 0.005 | 0.001 ± 0.001 | 0.004 ± 0.002 | 0.839 ± 0.024 | 0.879 ± 0.032 |
| uci_air_quality | 0.911 ± 0.013 | 0.906 ± 0.019 | 0.003 ± 0.000 | 0.003 ± 0.003 | 0.895 ± 0.019 | 0.884 ± 0.023 |
| uci_household | 0.963 ± 0.015 | 0.962 ± 0.018 | 0.001 ± 0.001 | 0.004 ± 0.002 | 0.937 ± 0.021 | 0.931 ± 0.031 |
| uci_load | 0.988 ± 0.001 | 0.987 ± 0.001 | 0.002 ± 0.000 | 0.002 ± 0.000 | 0.930 ± 0.008 | 0.916 ± 0.012 |
| uk_flood | 0.909 ± 0.003 | 0.911 ± 0.013 | 0.048 ± 0.009 | 0.040 ± 0.003 | 0.909 ± 0.003 | 0.911 ± 0.013 |
| uk_traffic | 0.961 ± 0.045 | 0.971 ± 0.019 | 0.000 ± 0.000 | 0.000 ± 0.000 | 0.975 ± 0.023 | 0.966 ± 0.025 |
| usgs | 0.833 ± 0.289 | 1.000 ± 0.000 | 0.013 ± 0.023 | 0.000 ± 0.000 | 0.864 ± 0.236 | 1.000 ± 0.000 |
| mean | 0.875 | 0.873 | 0.014 | 0.019 | 0.827 | 0.822 |

Latency, `process_long_window` and `LongAnomalyDetector.predict_features` on the
fullest Intel Lab window (24 h, 153 sensors): 746.6 ms p50 after, against 696 ms
before.

```
latency: {"sensors": 153.0, "readings": 295497.0, "p50_ms": 746.627050000825, "p50_ms_per_sensor_day": 4.879915359482516}
```

### 8.4 What the cleaning changed

- **The validation gain does not hold on the test windows.** On seed 0's validation
  rows the unseen mean rose 0.804 to 0.857; on the test windows over seeds 0-2 it is
  0.827 before and 0.822 after (7 sources better, 11 worse).
  The tuning's nab gain reverses (unseen 0.617 to 0.537, held out 0.705 to 0.602).
- **Pooled, the model is about as good, with more false alarms:** PR-AUC 0.951 to
  0.947, false alarms 0.008 to 0.013 per sensor-day (the budget is 0.04), stuck recall
  0.643 to 0.675, drift 0.734 to 0.751.
- **Intel Lab loses most:** held out PR-AUC 0.723 to 0.653 and false alarms 0.027 to
  0.061 per sensor-day; unseen 0.508 to 0.398. Array of Things' false alarms rise too
  (0.099 to 0.147), the source already furthest over budget.
- **usgs gains** (unseen 0.864 to 1.000), on a few windows.
- **The model still ships cleaned,** as decided (section 5: both models cleaned, no
  gate). A no-cleaning long model is one setting away
  (`long_window.clean_method: none`, then section 9's `long_main()`); a wider long
  search (more seeds, weaker smoothing) is the place to look for a setup that holds.

## 9. Recall is a lower bound

Some injected faults are hard to see in any window, cleaned or not, and they still
count as faults in the recall:

- **Short stuck faults.** `apply_stuck` freezes a sensor from a start drawn uniformly
  from the window's first n - 1 readings to its end. A 20 s window at 1 s holds 20
  readings, so 4 of the 19 starts (15-18) freeze 5 readings or fewer: about 21 % of
  1 s stuck faults. A short flat end is close to what a quiet sensor does.
- **Small noise faults.** The noise fault adds a noise of sd U(1, 3) × the type's
  median within-window spread (`noise_spreads` in `config/datasets.yaml`). In a window
  busier than the median, that can be smaller than the window's own variation. How
  many injected noise faults this hits was not measured.

So recall here is a lower bound on what a fault large enough to see would give.
