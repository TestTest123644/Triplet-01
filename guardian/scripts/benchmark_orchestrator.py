#!/usr/bin/env python3
"""Benchmark suite for measuring Guardian Orchestrator throughput and latency."""

import argparse
import time
import urllib.parse
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np

from guardian_layer.ai.datasets.windows import process_window
from guardian_layer.orchestrator import Orchestrator
from guardian_layer.pipelines.long_preprocess import process_long_window
from guardian_layer.storage import create_schema, make_engine
from guardian_layer.storage.database import insert_batch
from guardian_layer.storage.models import (
    ModelOutput,
    ProcessedReading,
    RawReading,
    TransformedWindow,
)
from guardian_layer.utils.schema import FlushedBatch, Metadata, Sensor


def get_db_url() -> str:
    """Resolve database URL from environment or secrets."""
    import os

    for env_var in ("GUARDIAN_DATABASE_URL", "DATABASE_URL"):
        val = os.getenv(env_var)
        if val:
            return val

    password = ""
    for default_path in (
        Path("secrets/postgres_password.txt"),
        Path("/run/secrets/postgres_password"),
    ):
        if default_path.exists():
            password = default_path.read_text().strip()
            break

    if not password:
        return ""

    user = os.getenv("POSTGRES_USER", "guardian")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "guardian")
    quoted_pwd = urllib.parse.quote_plus(password)
    return f"postgresql://{user}:{quoted_pwd}@{host}:{port}/{db}"


def generate_batch(count: int = 100, sensors: int = 10) -> FlushedBatch:
    """Generate a realistic test batch of sensor telemetry readings."""
    now = datetime.now(UTC)
    readings: list[Sensor] = []
    for i in range(count):
        sid = f"bench_sensor_{i % sensors}"
        cid = f"cluster_{i % 2}"
        readings.append(
            Sensor(
                sensor_id=sid,
                cluster_id=cid,
                timestamp=now - timedelta(seconds=count - i),
                value=2.0 + 0.1 * np.sin(i / 5.0),
                unit="m",
                metadata=Metadata(
                    packet_order=i + 1,
                    type="water_level",
                    lower_range=0.0,
                    upper_range=5.0,
                ),
            )
        )
    return FlushedBatch(
        id="bench-batch",
        sensor_id="bench_sensor_0",
        readings=tuple(readings),
        flushed_at=now,
    )


def run_benchmark(
    runs: int = 30,
    batch_size: int = 100,
    ingest_count: int = 10000,
) -> None:
    print("=" * 70)
    print(" GUARDIAN ORCHESTRATOR BENCHMARK SUITE")
    print("=" * 70)

    # 1. Initialize Engine
    db_url = get_db_url()
    engine = None
    if db_url:
        try:
            engine = make_engine(db_url)
            create_schema(engine)
            print(
                "[+] PostgreSQL connected: "
                + f"{engine.url.render_as_string(hide_password=True)}"
            )
        except Exception as e:
            print(f"[-] PostgreSQL connection failed: {e}")
            engine = None
    else:
        print("[!] Running in memory-only mode (no PostgreSQL configured)")

    orch = Orchestrator(engine=engine)
    print("[+] Orchestrator initialized (AI Short & Long Detectors loaded)")

    # 2. Ingestion & Buffering Throughput
    print(f"\n--- 1. Ingestion & Memory Buffering (N={ingest_count:,}) ---")
    ingest_orch = Orchestrator()
    t0 = time.perf_counter()
    now = datetime.now(UTC)
    for i in range(ingest_count):
        s = Sensor(
            sensor_id=f"sensor_{i % 20}",
            cluster_id=f"cluster_{i % 4}",
            timestamp=now,
            value=1.5,
            unit="m",
            metadata=Metadata(
                packet_order=i + 1,
                type="water_level",
                lower_range=0.0,
                upper_range=5.0,
            ),
        )
        ingest_orch.ingest_reading(s)
    t_ingest = time.perf_counter() - t0
    ingest_rate = ingest_count / t_ingest
    us_per_reading = (t_ingest * 1_000_000) / ingest_count
    print(f"  Throughput:           {ingest_rate:,.0f} readings/sec")
    print(f"  Latency per Reading:  {us_per_reading:.2f} µs")

    # 3. Full Pipeline Latency (Warmup + Measurement)
    print(
        "\n--- 2. End-to-End Pipeline Latency"
        + f"(Batch size={batch_size}, Runs={runs}) ---"
    )
    test_batch = generate_batch(count=batch_size)
    orch.process_pipeline(test_batch)  # warmup

    latencies: list[float] = []
    for _ in range(runs):
        t_start = time.perf_counter()
        orch.process_pipeline(test_batch)
        latencies.append((time.perf_counter() - t_start) * 1000.0)

    mean_ms = float(np.mean(latencies))
    p50_ms = float(np.median(latencies))
    p95_ms = float(np.percentile(latencies, 95))
    p99_ms = float(np.percentile(latencies, 99))
    min_ms = float(np.min(latencies))
    max_ms = float(np.max(latencies))
    throughput = batch_size / (mean_ms / 1000.0)

    print(f"  Average Batch Time:   {mean_ms:.2f} ms")
    print(f"  Median (P50):         {p50_ms:.2f} ms")
    print(f"  P95 Latency:          {p95_ms:.2f} ms")
    print(f"  P99 Latency:          {p99_ms:.2f} ms")
    print(f"  Min / Max:            {min_ms:.2f} ms / {max_ms:.2f} ms")
    print(
        f"  Latency per Reading:  {mean_ms / batch_size:.3f} ms "
        + f"({(mean_ms / batch_size) * 1000:.1f} µs)"
    )
    print(f"  Pipeline Throughput:  {throughput:,.0f} readings/sec")

    # 4. Stage-by-Stage Breakdown
    if engine is not None:
        print("\n--- 3. Stage-by-Stage Latency Breakdown ---")
        t_db_raw: list[float] = []
        t_static: list[float] = []
        t_dsp: list[float] = []
        t_db_windows: list[float] = []
        t_ai_inference: list[float] = []

        readings = list(test_batch.readings)
        now_utc = datetime.now(UTC)

        for _ in range(runs):
            # Stage 1: DB Raw & Processed
            t = time.perf_counter()
            raw_rows = [
                {
                    "sensor_id": r.sensor_id,
                    "cluster_id": r.cluster_id,
                    "timestamp": r.timestamp,
                    "value": r.value,
                    "unit": r.unit,
                    "metadata": None,
                }
                for r in readings
            ]
            proc_rows = [
                {
                    "sensor_id": r.sensor_id,
                    "cluster_id": r.cluster_id,
                    "timestamp": r.timestamp,
                    "value": r.value,
                    "unit": r.unit,
                    "metadata": None,
                    "quality_flags": [],
                }
                for r in readings
            ]
            insert_batch(engine, RawReading, raw_rows)
            insert_batch(engine, ProcessedReading, proc_rows)
            t_db_raw.append((time.perf_counter() - t) * 1000)

            # Stage 2: Static Checks
            t = time.perf_counter()
            by_sensor: dict[str, list[Sensor]] = {}
            for r in readings:
                by_sensor.setdefault(r.sensor_id, []).append(r)
            for _, s_readings in by_sensor.items():
                v_range = orch.preprocessor.find_range(s_readings, readings) or (
                    0.0,
                    5.0,
                )
                orch.static_checker.out_of_range(s_readings, v_range)
                orch.static_checker.sensor_is_down(s_readings, now_utc)
            t_static.append((time.perf_counter() - t) * 1000)

            # Stage 3: DSP Feature Extraction (Short + Long)
            t = time.perf_counter()
            short_f = process_window(
                orch.preprocessor, orch.decompose, orch.method, readings
            )
            long_f = process_long_window(
                orch.long_preprocess,
                orch.preprocessor,
                orch.decompose,
                orch.method,
                readings,
            )
            t_dsp.append((time.perf_counter() - t) * 1000)

            # Stage 4: DB Transformed Windows
            t = time.perf_counter()
            all_sids = set(short_f) | set(long_f)
            w_rows = [
                {
                    "sensor_id": sid,
                    "window_start": now_utc,
                    "window_end": now_utc,
                    "features": short_f.get(sid, {}),
                }
                for sid in all_sids
            ]
            insert_batch(engine, TransformedWindow, w_rows)
            t_db_windows.append((time.perf_counter() - t) * 1000)

            # Stage 5: AI Inference & Model Output DB
            t = time.perf_counter()
            verdicts = orch._evaluate_detector(orch.detector, short_f)
            m_rows = [
                {
                    "sensor_id": sid,
                    "window_end": now_utc,
                    "score": float(v.score),
                    "is_anomaly": bool(v.anomalous),
                    "model_version": "short_window",
                }
                for sid, v in verdicts.items()
            ]
            insert_batch(engine, ModelOutput, m_rows)
            t_ai_inference.append((time.perf_counter() - t) * 1000)

        mean_db_raw = float(np.mean(t_db_raw))
        mean_static = float(np.mean(t_static))
        mean_dsp = float(np.mean(t_dsp))
        mean_db_win = float(np.mean(t_db_windows))
        mean_ai = float(np.mean(t_ai_inference))
        total_staged = mean_db_raw + mean_static + mean_dsp + mean_db_win + mean_ai

        print(
            f"  Stage 1 (PostgreSQL Raw+Processed 200 rows): {mean_db_raw:6.2f} ms ("
            + f"{mean_db_raw / total_staged * 100:4.1f}%)"
        )
        print(
            f"  Stage 2 (Static Range & Down Checks):        {mean_static:6.2f} ms"
            + f" ({mean_static / total_staged * 100:4.1f}%)"
        )
        print(
            f"  Stage 3 (DSP Wavelet & Feature Extraction): {mean_dsp:6.2f} ms "
            + f"({mean_dsp / total_staged * 100:4.1f}%)"
        )
        print(
            f"  Stage 4 (PostgreSQL Transformed Windows):   {mean_db_win:6.2f} ms "
            + f"({mean_db_win / total_staged * 100:4.1f}%)"
        )
        print(
            f"  Stage 5 (AI XGBoost Inference + Outputs DB): {mean_ai:6.2f} ms "
            + f"({mean_ai / total_staged * 100:4.1f}%)"
        )
        print("  -------------------------------------------------------------")
        print(
            f"  Total Pipeline Time:                    {total_staged:6.2f} ms (100.0%)"
        )

        # Cleanup test data
        try:
            from sqlalchemy import text

            with engine.begin() as conn:
                conn.execute(
                    text(
                        """DELETE FROM raw_readings WHERE sensor_id
                        LIKE 'bench_sensor_%';"""
                    )
                )
                conn.execute(
                    text(
                        """DELETE FROM processed_readings
                        WHERE sensor_id LIKE 'bench_sensor_%';"""
                    )
                )
                conn.execute(
                    text(
                        """DELETE FROM transformed_windows
                         WHERE sensor_id LIKE 'bench_sensor_%';"""
                    )
                )
                conn.execute(
                    text(
                        """DELETE FROM model_outputs
                        WHERE sensor_id LIKE 'bench_sensor_%';"""
                    )
                )
        except Exception:
            pass

    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Guardian Orchestrator Benchmark")
    parser.add_argument(
        "--runs", type=int, default=20, help="Number of benchmark iterations"
    )
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Readings per flushed batch"
    )
    parser.add_argument(
        "--ingest-count",
        type=int,
        default=10000,
        help="Readings for buffer ingestion test",
    )
    args = parser.parse_args()

    run_benchmark(
        runs=args.runs, batch_size=args.batch_size, ingest_count=args.ingest_count
    )
