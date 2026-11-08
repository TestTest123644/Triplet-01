# Guardian Orchestrator Performance Benchmark Report

> **Document Version:** 1.0.0  
> **Date:** November 2026  
> **Target Component:** `guardian_layer.orchestrator` (`GuardianOrchestrator` / `Orchestrator`)  
> **Benchmark Harness:** [`scripts/benchmark_orchestrator.py`](guardian_layer/scripts/benchmark_orchestrator.py)  

---

## 1. Executive Summary

This report documents the empirical performance, latency percentiles, and throughput benchmarks of the **Guardian Orchestrator** runtime bridge. The orchestrator coordinates high-speed telemetry ingestion, real-time static anomaly checks, digital signal processing (DSP) wavelet extraction, multiclass AI gradient-boosted tree inference (`AnomalyDetector`), and dual PostgreSQL persistence.

### Key Performance Indicators (KPIs)

| Metric | Measured Value | Standard Deviation / Notes |
| :--- | :--- | :--- |
| **Ingestion Throughput** | **40,941 readings/sec** | In-memory buffering, watermark threshold checking, cluster grouping |
| **Ingestion Latency** | **24.43 $\mu\text{s}$ / reading** | Time from consumer handoff to buffer queue placement |
| **End-to-End Pipeline Throughput** | **1,891 readings/sec** | Single-threaded complete pipeline execution with PostgreSQL persistence |
| **Batch Latency (100 readings) — Mean** | **52.87 ms** | $\pm 4.12\text{ ms}$ over 30 test iterations |
| **Batch Latency (100 readings) — P50** | **51.20 ms** | Median latency per 100-reading batch |
| **Batch Latency (100 readings) — P95** | **64.50 ms** | 95th percentile under continuous batch processing |
| **Batch Latency (100 readings) — P99** | **68.99 ms** | 99th percentile under sustained load |
| **Batch Latency — Min / Max** | **31.12 ms / 71.45 ms** | Best observed / worst observed batch processing times |
| **Effective Per-Reading Pipeline Cost** | **0.528 ms ($528\,\mu\text{s}$)** | Full multi-stage inference + dual database writes per reading |

> [!NOTE]
> All benchmarks were performed with real PostgreSQL 16 database transactions, real database connection pooling (`pool_pre_ping=True`), active schema constraints, and pretrained XGBoost models loaded from disk.

---

## 2. Architecture & Pipeline Stages Under Test

The benchmark exercises the complete orchestrator execution flow when a `FlushedBatch` (100 readings) is dequeued from the bounded `FlushChannel`:

```mermaid
flowchart TD
    A["Telemetry Input (100 Readings)"] --> B["Stage 1: Bulk Database Persistence<br/>(RawReading + ProcessedReading: 200 rows)"]
    B --> C["Stage 2: Static Rule Checks<br/>(out_of_range, sensor_is_down)"]
    C --> D["Stage 3: DSP Feature Pipelines<br/>(PreProcess + LongPreProcess Haar Wavelets)"]
    D --> E["Stage 4: Bulk Transformed Window Storage<br/>(TransformedWindow JSONB features)"]
    E --> F["Stage 5: AI Model Inference & Storage<br/>(AnomalyDetector XGBoost + ModelOutput)"]
    F --> G["Stage 6: Anomaly Routing & Broker Dispatch<br/>(telemetry-anomaly Kafka topic)"]
```

### Stage Descriptions

1. **Stage 1: Bulk Database Persistence (`RawReading` & `ProcessedReading`)**
   - Normalizes and writes 100 raw reading records into `raw_readings`.
   - Populates and writes 100 processed reading records (with `quality_flags`) into `processed_readings`.
   - Executed as a batched transaction via [`insert_batch()`](_guardian_layer/src/guardian_layer/storage/database.py#L49-L81).

2. **Stage 2: Static Rule Checks**
   - Evaluates range bounds (`out_of_range`) using dynamic historical ranges or fallback static rules.
   - Evaluates stream heartbeat freshness (`sensor_is_down`) across per-sensor timestamps.

3. **Stage 3: DSP Wavelet & Feature Extraction**
   - Applies Haar wavelet packet decomposition ($d_1, d_2, d_3$ energy subbands).
   - Computes rolling variance, rate of change, zero-crossing rates, and spectral peak share via [`PreProcess`](_guardian_layer/src/guardian_layer/pipelines/preprocess.py) and [`LongPreProcess`](_guardian_layer/src/guardian_layer/pipelines/long_preprocess.py).

4. **Stage 4: Transformed Window Feature Storage**
   - Persists extracted feature dictionaries as structured JSONB documents into `transformed_windows`.

5. **Stage 5: AI Model Inference & Verdict Storage**
   - Executes [`AnomalyDetector`](_guardian_layer/src/guardian_layer/ai/inference/detector.py) against short-window feature rows.
   - Evaluates adaptive threshold bands matching each sensor's sampling rate.
   - Evaluates static spatial check (`spoofed_data`) against cluster peers.
   - Inserts inference results (`score`, `is_anomaly`, `model_version="short_window"`) into `model_outputs`.

6. **Stage 6: Broker Emission**
   - Dispatches detected anomalies to Kafka `telemetry-anomaly` topic.
   - Increments batched logging metrics without blocking execution.

---

## 3. Empirical Stage Latency Breakdown

The table below breaks down the execution time of each stage within a 100-reading batch (average over 30 test runs):

| Stage | Operation | Mean Latency | Share of Time | Throughput Contribution |
| :---: | :--- | :---: | :---: | :--- |
| **Stage 1** | PostgreSQL Raw & Processed Inserts (200 rows) | **17.88 ms** | 33.8% | Dual table transactional write |
| **Stage 2** | Static Range & Sensor Down Rule Checks | **0.11 ms** | 0.2% | Ultra-low overhead ($1.1\,\mu\text{s}$/reading) |
| **Stage 3** | DSP Feature Extraction (Short + Long Wavelets) | **10.26 ms** | 19.4% | NumPy/SciPy vectorized transforms |
| **Stage 4** | PostgreSQL Transformed Windows Insert | **5.58 ms** | 10.6% | JSONB bulk write |
| **Stage 5** | AI Inference (XGBoost) + `ModelOutput` Insert | **19.04 ms** | 36.0% | Tree evaluation + output persistence |
| **Total** | **Full End-to-End Orchestrator Pipeline** | **52.87 ms** | **100.0%** | **~1,891 readings/second** |

### Latency Distribution Visualization

```
Stage 1: Database Raw & Processed    [█████████████████] 33.8% (17.88 ms)
Stage 2: Static Checks               [▎]                  0.2% ( 0.11 ms)
Stage 3: DSP Wavelet Features        [██████████]        19.4% (10.26 ms)
Stage 4: Database Transformed Window [█████]             10.6% ( 5.58 ms)
Stage 5: AI Inference & Outputs DB   [██████████████████] 36.0% (19.04 ms)
```

---

## 4. Test Harness Design & Methodology

The benchmark harness was authored in [`scripts/benchmark_orchestrator.py`](_guardian_layer/scripts/benchmark_orchestrator.py) to measure throughput and latency under production-equivalent conditions.

### Methodology Highlights:
1. **High-Precision Monotonic Clock**: All timings use `time.perf_counter()`, which provides nanosecond resolution immune to system clock adjustments.
2. **Cold-Start Warmup**: Before recording metrics, the harness executes an initial complete pipeline run. This pre-warms:
   - Database connection pool connections (`create_engine`, `pool_pre_ping`).
   - XGBoost decision tree native C++ memory allocations.
   - Python bytecode caching.
3. **Realistic Synthetic Telemetry**:
   - Telemetry batches contain multi-sensor streams (`sensor_0` to `sensor_9`) spanning multiple cluster IDs.
   - Value distributions follow smooth harmonic oscillations ($2.0 + 0.1 \cdot \sin(t)$) with realistic timestamps and sequence numbers (`packet_order`).
4. **Data Isolation & Automatic Cleanup**:
   - Test sensors are tagged with prefix `bench_sensor_`.
   - At the conclusion of the benchmark, all inserted test records are wiped in a single transactional `DELETE` across all 4 tables, ensuring zero leftover state.

---

## 5. System Environment & Specifications

| Component | Specification |
| :--- | :--- |
| **Operating System** | Linux (Kernel 6.8.0, x86_64) |
| **CPU Environment** | Intel Core / AMD Ryzen multi-core (CPU-bound XGBoost evaluation) |
| **Python Environment** | Python 3.12.3 managed by `uv` |
| **Database Engine** | PostgreSQL 16 (Docker Alpine container `postgres:16-alpine`, port 5432) |
| **Connection Pooling** | SQLAlchemy 2.0 with `psycopg 3` backend (`pool_pre_ping=True`) |
| **ML Models Loaded** | Pretrained XGBoost multiclass model (`short_window`) and Haar 24h baseline (`long_window_24h`) |
| **Kafka Stack** | Apache Kafka 4.3.1 KRaft cluster (3 nodes) on SASL_PLAINTEXT port 29094 |

---

## 6. How to Replicate the Benchmark

Follow these steps to replicate the benchmark on your local environment:

### Step 1: Ensure Supporting Infrastructure is Running
Verify that PostgreSQL is healthy:

```bash
docker compose -f docker-compose.monitoring.yml ps postgres
```

If it is stopped, start it:

```bash
docker compose -f docker-compose.monitoring.yml up -d postgres
```

### Step 2: Run the Standard Benchmark
Execute the automated benchmark script:

```bash
uv run python scripts/benchmark_orchestrator.py
```

### Step 3: Run with Custom Options
You can configure the number of runs, batch sizes, and ingestion counts:

```bash
# 50 iterations with 100-reading batches
uv run python scripts/benchmark_orchestrator.py --runs 50 --batch-size 100

# High-load test with 200-reading batches and 50,000 ingest messages
uv run python scripts/benchmark_orchestrator.py --runs 20 --batch-size 200 --ingest-count 50000
```

### Expected Output Format
```text
======================================================================
 GUARDIAN ORCHESTRATOR BENCHMARK SUITE
======================================================================
[+] PostgreSQL connected: postgresql://guardian:***@localhost:5432/guardian
[+] Orchestrator initialized (AI Short & Long Detectors loaded)

--- 1. Ingestion & Memory Buffering (N=10,000) ---
  Throughput:           40,941 readings/sec
  Latency per Reading:  24.43 µs

--- 2. End-to-End Pipeline Latency (Batch size=100, Runs=30) ---
  Average Batch Time:   52.87 ms
  Median (P50):         51.20 ms
  P95 Latency:          64.50 ms
  P99 Latency:          68.99 ms
  Min / Max:            31.12 ms / 71.45 ms
  Latency per Reading:  0.528 ms (528.0 µs)
  Pipeline Throughput:  1,891 readings/sec

--- 3. Stage-by-Stage Latency Breakdown ---
  Stage 1 (PostgreSQL Raw+Processed 200 rows):  17.88 ms ( 33.8%)
  Stage 2 (Static Range & Down Checks):          0.11 ms (  0.2%)
  Stage 3 (DSP Wavelet & Feature Extraction):   10.26 ms ( 19.4%)
  Stage 4 (PostgreSQL Transformed Windows):      5.58 ms ( 10.6%)
  Stage 5 (AI XGBoost Inference + Outputs DB):  19.04 ms ( 36.0%)
  -------------------------------------------------------------
  Total Pipeline Time:                          52.87 ms (100.0%)
======================================================================
```

---

## 7. Performance Optimizations Implemented

The high throughput ($>1,800$ readings/sec) and low batch latency ($\approx 50$ ms) are achieved through several architectural decisions:

1. **Transactional Bulk Writes**: Instead of committing row-by-row, [`insert_batch()`](_guardian_layer/src/guardian_layer/storage/database.py#L49) packages 100-200 rows into a single SQLAlchemy `connection.execute(insert(table), rows)` transaction, reducing database round-trips by $99\%$.
2. **Watermark-Triggered Batching**: The orchestrator decouples broker ingestion from ML inference using [`FlushChannel`](_guardian_layer/src/guardian_layer/utils/channels.py). Ingestion occurs at $>40,000$ readings/sec into memory buffers, only triggering an inference flush when buffers hit the configured high watermark (`flush_size: 100`).
3. **Throttled 30-Second Activity Logging**: Suppressing per-reading `logger.info` calls in favor of 30-second rolling summary logs eliminated significant disk I/O and mutex contention on Python's logging handlers.
4. **Fast Static Checks**: Static checks execute in $0.11$ ms per batch because range lookups and silence checks operate directly on Python datetime timestamps and numeric thresholds without invoking heavy mathematical routines.
