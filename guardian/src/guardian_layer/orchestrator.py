"""Guardian runtime orchestrator bridge.

Coordinates the end-to-end data pipeline:
1. Manages a ClusterManager backed by a bounded FlushChannel sink.
2. Ingests and buffers validated sensor readings consumed from the message broker.
3. Concurrently watches the FlushChannel queue for flushed sensor batches.
4. Executes the processing pipeline on each flushed batch:
   - Persists raw sensor readings into the database (RawReading).
   - Evaluates static rule checks (out_of_range, sensor_is_down).
   - Computes short-window and long-window features (PreProcess and LongPreProcess).
   - Persists transformed features to the database (TransformedWindow).
   - Evaluates static spatial check (spoofed_data) and executes inference detectors.
   - Persists model verdicts to the database (ModelOutput).
5. Emits detected anomalies (from static checks and ML inference) to the Kafka broker.
"""

from __future__ import annotations

import queue
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, Message, Producer
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from guardian_layer.ai.datasets.windows import process_window
from guardian_layer.ai.inference.detector import AnomalyDetector, model_verdicts
from guardian_layer.ai.inference.long_detector import LongAnomalyDetector
from guardian_layer.ai.inference.model import BaselineModel
from guardian_layer.consumer_producer.action_taker.schema import AnomalyRecord
from guardian_layer.managers.cluster_manger import ClusterManager
from guardian_layer.pipelines.decompose import Decompose
from guardian_layer.pipelines.long_preprocess import (
    LongPreProcess,
    process_long_window,
)
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.pipelines.static_check import StaticCheck
from guardian_layer.storage.database import insert_batch
from guardian_layer.storage.models import (
    ModelOutput,
    ProcessedReading,
    RawReading,
    TransformedWindow,
)
from guardian_layer.utils.channels import FlushChannel
from guardian_layer.utils.config import Settings, get_settings
from guardian_layer.utils.logger import enable, get_logger
from guardian_layer.utils.schema import (
    SENSOR_RULES,
    Ack,
    Anomalies,
    FlushedBatch,
    Sensor,
    SensorType,
    Verdict,
)

enable("INFO")
# 2. Use the explicit module namespace:
logger = get_logger("guardian_layer.orchestrator")

DEFAULT_SHORT_MODEL_DIR = (
    Path(__file__).parent
    / "ai"
    / "models_bin"
    / "short_window-20261107T175856Z-1-001"
    / "short_window"
)
DEFAULT_LONG_MODEL_DIR = (
    Path(__file__).parent
    / "ai"
    / "models_bin"
    / "long_window-20261107T175846Z-1-001"
    / "long_window"
    / "24h"
)


class Orchestrator:
    """Core runtime orchestrator bridging stream brokers, in-memory buffers,

    feature pipelines, AI inference, and database persistence.
    """

    def __init__(
        self,
        cluster_manager: ClusterManager | None = None,
        flush_channel: FlushChannel | None = None,
        consumer: Consumer | None = None,
        producer: Producer | None = None,
        engine: Engine | None = None,
        static_checker: StaticCheck | None = None,
        preprocessor: PreProcess | None = None,
        long_preprocess: LongPreProcess | None = None,
        decompose: Decompose | None = None,
        detector: AnomalyDetector | BaselineModel | Any = None,
        long_detector: LongAnomalyDetector | BaselineModel | Any = None,
        settings: Settings | None = None,
        input_topic: str = "telemetry-validated",
        anomaly_topic: str = "telemetry-anomaly",
        confirmation_topic: str = "telemetry-confirmation",
        kafka_bootstrap_servers: str | None = None,
        kafka_group_id: str = "guardian-processor",
        kafka_config: dict[str, Any] | None = None,
        hourly_interval_seconds: float = 3600.0,
        log_batch_interval_seconds: float = 30.0,
    ) -> None:
        self.settings = settings or get_settings()

        # 1. Bounded hand-off queue and ClusterManager
        self.flush_channel = flush_channel or FlushChannel()
        self.cluster_manager = cluster_manager or ClusterManager(
            sink=self.flush_channel
        )

        # 2. Database storage engine
        self.engine = engine
        self.hourly_interval_seconds = hourly_interval_seconds

        # 3. Pipelines & AI inference
        self.static_checker = static_checker or StaticCheck(
            settings=self.settings.static_check
        )
        self.preprocessor = preprocessor or PreProcess(
            settings=self.settings.preprocess
        )
        self.decompose = decompose or Decompose()
        self.long_preprocess = long_preprocess or LongPreProcess(
            preprocessor=self.preprocessor,
            decompose=self.decompose,
            settings=self.settings.long_window,
        )

        # Load short-window model: user-provided detector > default artifact
        if detector is None and DEFAULT_SHORT_MODEL_DIR.exists():
            try:
                self.detector = AnomalyDetector.from_directory(DEFAULT_SHORT_MODEL_DIR)
                logger.info("Loaded default short-window anomaly detector")
            except Exception:
                logger.exception("Failed to load default short-window detector")
                self.detector = None
        else:
            self.detector = detector

        # Load long-window model: user-provided long_detector > default artifact
        if long_detector is None and DEFAULT_LONG_MODEL_DIR.exists():
            try:
                self.long_detector = LongAnomalyDetector(
                    BaselineModel.load(DEFAULT_LONG_MODEL_DIR), min_grid_points=1
                )
                logger.info("Loaded default long-window anomaly detector")
            except Exception:
                logger.exception("Failed to load default long-window detector")
                self.long_detector = None
        else:
            self.long_detector = long_detector

        self.method = self.preprocessor.method

        # 4. Message Broker topics and clients
        self.input_topic = input_topic
        self.consumer = consumer
        self.producer = producer
        self.anomaly_topic = anomaly_topic
        self.confirmation_topic = confirmation_topic

        # Lazily instantiate Kafka consumer/producer if servers provided
        # and instances not injected
        if self.consumer is None and kafka_bootstrap_servers:
            self.consumer = self._init_kafka_consumer(
                kafka_bootstrap_servers, kafka_group_id, kafka_config
            )

        if self.producer is None and kafka_bootstrap_servers:
            self.producer = self._init_kafka_producer(
                kafka_bootstrap_servers, kafka_config
            )

        # Concurrency & monitoring state
        self._stop_event = threading.Event()
        self._watcher_thread: threading.Thread | None = None
        self._consumer_thread: threading.Thread | None = None
        self._hourly_thread: threading.Thread | None = None
        self._metrics_thread: threading.Thread | None = None
        self._emitted_anomalies: list[AnomalyRecord] = []
        self._emitted_confirmations: list[dict[str, Any]] = []
        self._processed_batches: list[FlushedBatch] = []
        self._lock = threading.Lock()

        # Batched / rate-limited periodic logging (every 30s)
        self.log_batch_interval_seconds = log_batch_interval_seconds
        self._metrics_lock = threading.Lock()
        self._last_metrics_log_time = time.monotonic()
        self._metrics: dict[str, int] = {
            "anomalies_published": 0,
            "messages_consumed": 0,
            "batches_processed": 0,
            "readings_processed": 0,
        }

    def _get_model_version(self, detector: Any, default: str) -> str:
        """Resolve the model name / version of the detector that performed inference."""
        if detector is None:
            return default
        for attr in ("model_name", "model_version", "name"):
            val = getattr(detector, attr, None)
            if val and isinstance(val, str):
                return val
        for inner in ("short_model", "model"):
            m = getattr(detector, inner, None)
            if m is not None:
                for attr in ("model_name", "model_version", "name"):
                    val = getattr(m, attr, None)
                    if val and isinstance(val, str):
                        return val
        cls_name = getattr(detector, "__class__", type(detector)).__name__
        if cls_name == "AnomalyDetector":
            return "short_window"
        if cls_name == "LongAnomalyDetector":
            return "long_window_24h"
        return default

    # -------------------------------------------------------------------------
    # Kafka Initialization Helpers
    # -------------------------------------------------------------------------

    def _init_kafka_consumer(
        self,
        bootstrap_servers: str,
        group_id: str,
        extra_config: dict[str, Any] | None,
    ) -> Consumer:
        try:
            from confluent_kafka import Consumer

            conf: dict[str, Any] = {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
            }
            if extra_config:
                conf.update(extra_config)
            consumer = Consumer(conf)
            consumer.subscribe([self.input_topic])
            logger.info(
                "Initialized Kafka consumer",
                topic=self.input_topic,
                bootstrap=bootstrap_servers,
            )
            return consumer
        except Exception as e:
            logger.exception("Failed to initialize Kafka consumer")
            raise e

    def _init_kafka_producer(
        self,
        bootstrap_servers: str,
        extra_config: dict[str, Any] | None,
    ) -> Producer:
        try:
            from confluent_kafka import Producer

            conf: dict[str, Any] = {"bootstrap.servers": bootstrap_servers}
            if extra_config:
                conf.update(extra_config)
            producer = Producer(conf)
            logger.info(
                "Initialized Kafka producer",
                topic=self.anomaly_topic,
                bootstrap=bootstrap_servers,
            )
            return producer
        except Exception as e:
            logger.exception("Failed to initialize Kafka producer")
            raise e

    # -------------------------------------------------------------------------
    # Ingestion & Broker Consumer
    # -------------------------------------------------------------------------

    def ingest_reading(self, sensor: Sensor) -> Ack:
        """Store sensor reading in its cluster buffer, creating cluster if missing."""
        with self._lock:
            if not self.cluster_manager.validate(sensor.cluster_id).ok:
                self.cluster_manager.create(sensor)
            return self.cluster_manager.add(sensor)

    def consume_broker_message(self, message: Message) -> Sensor | None:
        """Parse raw broker message into a Sensor reading and ingest it."""
        try:
            if isinstance(message, Sensor):
                self.ingest_reading(message)
                return message

            # Handle confluent_kafka.Message or object with error()/value()
            if hasattr(message, "error") and callable(message.error):
                err = message.error()
                if err is not None:
                    logger.error(
                        f"Consumer code : {err.code()}\nmessage : "
                        + f"{message.value()}, error : {str(err)}",
                        error=str(err),
                    )
                    return None

            if hasattr(message, "value") and callable(message.value):
                payload: Any = message.value()
            else:
                payload = message

            if payload is None:
                return None

            if isinstance(payload, Sensor):
                sensor = payload
            elif isinstance(payload, (str, bytes)):
                sensor = Sensor.model_validate_json(payload)
            elif isinstance(payload, Mapping):
                sensor = Sensor.model_validate(payload)
            else:
                raise TypeError(f"Unsupported broker message payload: {type(payload)}")

            self.ingest_reading(sensor)
            with self._metrics_lock:
                self._metrics["messages_consumed"] += 1
                should_flush = (
                    time.monotonic() - self._last_metrics_log_time
                    >= self.log_batch_interval_seconds
                )
            if should_flush:
                self.flush_batch_logs()
            return sensor
        except Exception:
            logger.exception("Failed to consume broker message")
            return None

    # -------------------------------------------------------------------------
    # Pipeline Execution
    # -------------------------------------------------------------------------

    def process_pipeline(
        self, batch: FlushedBatch, window_end: datetime | None = None
    ) -> dict[str, Any]:
        """Execute the process pipeline on a flushed batch of sensor readings.

        Pipeline stages:
        1. Store raw data to database.
        2. Pass to static check (out_of_range, sensor_is_down).
        3. Pass to feature pipelines (PreProcess & LongPreProcess).
        4. Store output of both pipelines to database (TransformedWindow).
        5. Run static spatial check (spoofed_data) & AI detector(s),
           and store output to database (ModelOutput).
        6. Publish any detected anomalies to the Kafka broker.
        """
        readings: list[Sensor] = list(batch.readings)
        if not readings:
            return {"batch_id": batch.id, "readings_count": 0, "anomalies": []}

        with self._metrics_lock:
            self._metrics["batches_processed"] += 1
            self._metrics["readings_processed"] += len(readings)
            should_flush = (
                time.monotonic() - self._last_metrics_log_time
                >= self.log_batch_interval_seconds
            )
        if should_flush:
            self.flush_batch_logs()

        now = datetime.now(UTC)
        eval_window_end = window_end or now
        detected_anomalies: list[tuple[str, Anomalies]] = []

        # ---------------------------------------------------------------------
        # Stage 1: Store raw and processed data to database
        # ---------------------------------------------------------------------
        if self.engine is not None:
            raw_rows: list[dict[str, Any]] = [
                {
                    "sensor_id": r.sensor_id,
                    "cluster_id": r.cluster_id,
                    "timestamp": r.timestamp,
                    "value": r.value,
                    "unit": r.unit,
                    "metadata": (
                        r.metadata.model_dump() if r.metadata is not None else None
                    ),
                }
                for r in readings
            ]
            processed_rows: list[dict[str, Any]] = [
                {
                    "sensor_id": r.sensor_id,
                    "cluster_id": r.cluster_id,
                    "timestamp": r.timestamp,
                    "value": r.value,
                    "unit": r.unit,
                    "metadata": (
                        r.metadata.model_dump() if r.metadata is not None else None
                    ),
                    "quality_flags": list(r.quality_flags) if r.quality_flags else [],
                }
                for r in readings
            ]
            try:
                insert_batch(self.engine, RawReading, raw_rows)
                insert_batch(self.engine, ProcessedReading, processed_rows)
            except Exception:
                logger.exception("Failed to persist readings to database")

        # ---------------------------------------------------------------------
        # Stage 2: Pass to static check
        # ---------------------------------------------------------------------
        by_sensor: dict[str, list[Sensor]] = {}
        for r in readings:
            by_sensor.setdefault(r.sensor_id, []).append(r)

        for sensor_id, s_readings in by_sensor.items():
            value_range = self.preprocessor.find_range(s_readings, readings)
            if value_range is None and s_readings[0].metadata is not None:
                try:
                    s_type = SensorType(s_readings[0].metadata.type)
                    rule = SENSOR_RULES.get(s_type)
                    if rule is not None:
                        value_range = (rule.lower, rule.upper)
                except (ValueError, TypeError):
                    pass

            range_res = self.static_checker.out_of_range(s_readings, value_range)
            if isinstance(range_res, Anomalies):
                detected_anomalies.append((sensor_id, range_res))

            down_res = self.static_checker.sensor_is_down(s_readings, eval_window_end)
            if isinstance(down_res, Anomalies):
                detected_anomalies.append((sensor_id, down_res))

        # ---------------------------------------------------------------------
        # Stage 3: Feature pipelines (PreProcess & LongPreProcess)
        # ---------------------------------------------------------------------
        short_features: dict[str, dict[str, float]] = process_window(
            self.preprocessor, self.decompose, self.method, readings
        )

        # Static check: spoofed_data on spatial features
        for sensor_id, feats in short_features.items():
            spoofed_res = self.static_checker.spoofed_data(sensor_id, feats)
            if isinstance(spoofed_res, Anomalies):
                detected_anomalies.append((sensor_id, spoofed_res))

        long_features: dict[str, dict[str, float]] = process_long_window(
            self.long_preprocess,
            self.preprocessor,
            self.decompose,
            self.method,
            readings,
        )

        # ---------------------------------------------------------------------
        # Stage 4: Store data (output of both pipelines) to database
        # ---------------------------------------------------------------------
        all_sensor_ids = set(by_sensor.keys()) | set(short_features.keys())
        transformed_rows: list[dict[str, Any]] = []

        for sensor_id in sorted(all_sensor_ids):
            s_readings = by_sensor.get(sensor_id, readings)
            w_start = min(r.timestamp for r in s_readings)
            w_end = max(r.timestamp for r in s_readings)

            combined_feats: dict[str, float] = {
                **short_features.get(sensor_id, {}),
                **long_features.get(sensor_id, {}),
            }

            transformed_rows.append(
                {
                    "sensor_id": sensor_id,
                    "window_start": w_start,
                    "window_end": w_end,
                    "features": combined_feats,
                }
            )

        if self.engine is not None and transformed_rows:
            try:
                insert_batch(self.engine, TransformedWindow, transformed_rows)
            except Exception:
                logger.exception("Failed to persist transformed features to database")

        # ---------------------------------------------------------------------
        # Stage 5: Run short-window detector on buffer features & store to DB
        # ---------------------------------------------------------------------
        verdicts: dict[str, Verdict] = {}
        model_output_rows: list[dict[str, Any]] = []

        if self.detector is not None:
            v_short = self._evaluate_detector(self.detector, short_features)
            verdicts.update(v_short)
            model_name = self._get_model_version(self.detector, default="short_window")
            for sid, verdict in v_short.items():
                if verdict.anomalous and verdict.label is not None:
                    detected_anomalies.append((sid, verdict.label))

                model_output_rows.append(
                    {
                        "sensor_id": verdict.sensor_id,
                        "window_end": eval_window_end,
                        "score": float(verdict.score),
                        "is_anomaly": bool(verdict.anomalous),
                        "model_version": model_name,
                    }
                )

        if self.engine is not None and model_output_rows:
            try:
                insert_batch(self.engine, ModelOutput, model_output_rows)
            except Exception:
                logger.exception("Failed to persist model outputs to database")

        # ---------------------------------------------------------------------
        # Stage 6: Publish detected anomalies & telemetry trust confirmations
        # ---------------------------------------------------------------------
        unique_anomalies = dict.fromkeys(detected_anomalies)
        emitted_records: list[AnomalyRecord] = []
        sensor_anomaly_reasons: dict[str, list[str]] = {}

        for sensor_id, anomaly_label in unique_anomalies:
            record = AnomalyRecord(
                device_id=sensor_id,
                reading_id=None,
                label=anomaly_label,
                detected_at=now,
            )
            self.publish_anomaly(record)
            emitted_records.append(record)
            label_str = (
                anomaly_label.value
                if hasattr(anomaly_label, "value")
                else str(anomaly_label)
            )
            sensor_anomaly_reasons.setdefault(sensor_id, []).append(label_str)

        # Emit trust confirmation (telemetry-confirmation topic)
        # for all sensors in this batch
        all_eval_sensors = set(by_sensor.keys())
        if batch.sensor_id:
            all_eval_sensors.add(batch.sensor_id)

        emitted_confirmations: list[dict[str, Any]] = []
        for sensor_id in sorted(all_eval_sensors):
            is_anomalous = sensor_id in sensor_anomaly_reasons
            reasons = sensor_anomaly_reasons.get(sensor_id, [])
            v_obj = verdicts.get(sensor_id)
            trust_val = (
                (1.0 - float(v_obj.score))
                if (v_obj is not None and v_obj.score is not None)
                else (0.02 if is_anomalous else 0.97)
            )
            s_readings = by_sensor.get(sensor_id, [])
            r_id = (
                f"r-{sensor_id}-{s_readings[-1].metadata.packet_order}"
                if (s_readings and s_readings[-1].metadata)
                else None
            )

            conf = self.publish_confirmation(
                device_id=sensor_id,
                trusted=not is_anomalous,
                reading_id=r_id,
                reasons=reasons,
                trust_score=trust_val,
            )
            emitted_confirmations.append(conf)

        with self._lock:
            self._processed_batches.append(batch)

        return {
            "batch_id": batch.id,
            "sensor_id": batch.sensor_id,
            "readings_count": len(readings),
            "short_features": short_features,
            "long_features": long_features,
            "verdicts": verdicts,
            "anomalies": emitted_records,
            "confirmations": emitted_confirmations,
        }

    def _evaluate_detector(
        self,
        detector: Any,
        features: dict[str, dict[str, float]],
    ) -> dict[str, Verdict]:
        """Safely score features using an AnomalyDetector or BaselineModel."""
        if not features:
            return {}
        try:
            if hasattr(detector, "predict_features"):
                return detector.predict_features(features)
            if isinstance(detector, BaselineModel):
                return model_verdicts(detector, features, list(features.keys()))
            if hasattr(detector, "predict"):
                return detector.predict(features)
        except Exception:
            logger.exception("Inference detector evaluation failed")
        return {}

    # -------------------------------------------------------------------------
    # Anomaly Publishing
    # -------------------------------------------------------------------------

    def publish_anomaly(self, record: AnomalyRecord) -> None:
        """Publish an anomaly detection event to the Kafka broker."""
        with self._lock:
            self._emitted_anomalies.append(record)

        if self.producer is None:
            logger.warning(
                "Anomaly detected but no broker producer configured",
                device_id=record.device_id,
                label=record.label,
            )
            return

        try:
            payload = record.model_dump_json().encode("utf-8")
            key = record.device_id.encode("utf-8")

            self.producer.produce(topic=self.anomaly_topic, key=key, value=payload)
            if hasattr(self.producer, "poll"):
                self.producer.poll(0)

            with self._metrics_lock:
                self._metrics["anomalies_published"] += 1
                should_flush = (
                    time.monotonic() - self._last_metrics_log_time
                    >= self.log_batch_interval_seconds
                )
            if should_flush:
                self.flush_batch_logs()
        except Exception:
            logger.exception(
                "Failed to publish anomaly to broker",
                device_id=record.device_id,
                topic=self.anomaly_topic,
            )

    def publish_confirmation(
        self,
        device_id: str,
        trusted: bool,
        reading_id: str | None = None,
        reasons: list[str] | None = None,
        trust_score: float | None = None,
        ttl_seconds: int = 30,
    ) -> dict[str, Any]:
        """Publish an Ed25519-signed telemetry trust
        confirmation to the Kafka broker."""
        import base64
        import hashlib
        import json
        import uuid

        reasons = reasons or []
        state = "TRUSTED" if trusted else "UNTRUSTED"
        if trust_score is None:
            trust_score = 0.97 if trusted else 0.02

        now = datetime.now(UTC)
        expires = now + timedelta(seconds=ttl_seconds)
        verdict_id = f"v-{uuid.uuid4().hex[:6]}"
        r_id = reading_id or f"r-{device_id}-{uuid.uuid4().hex[:4]}"

        canonical_body = json.dumps(
            {
                "device_id": device_id,
                "expires_at": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "issued_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "reading_id": r_id,
                "reasons": reasons,
                "state": state,
                "trust": round(float(trust_score), 2),
                "verdict_id": verdict_id,
            },
            sort_keys=True,
        )

        sig_hash = hashlib.sha256(canonical_body.encode("utf-8")).hexdigest()[:16]

        try:
            from cryptography.hazmat.primitives.asymmetric import ed25519

            if not hasattr(self, "_signing_key"):
                self._signing_key = ed25519.Ed25519PrivateKey.generate()
            sig_bytes = self._signing_key.sign(canonical_body.encode("utf-8"))
            sig_b64 = base64.b64encode(sig_bytes).decode("ascii")
        except Exception:
            sig_b64 = base64.b64encode(
                hashlib.sha256(canonical_body.encode("utf-8")).digest()
            ).decode("ascii")

        verdict_msg = {
            "verdict_id": verdict_id,
            "reading_id": r_id,
            "sig_hash": sig_hash,
            "device_id": device_id,
            "state": state,
            "trusted": trusted,
            "trust": round(float(trust_score), 2),
            "reasons": reasons,
            "issued_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "expires_at": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sig": sig_b64,
        }

        with self._lock:
            self._emitted_confirmations.append(verdict_msg)

        if self.producer is not None:
            try:
                payload = json.dumps(verdict_msg).encode("utf-8")
                key = device_id.encode("utf-8")
                self.producer.produce(
                    topic=self.confirmation_topic,
                    key=key,
                    value=payload,
                )
                if hasattr(self.producer, "poll"):
                    self.producer.poll(0)
            except Exception:
                logger.exception(
                    "Failed to publish telemetry confirmation to broker",
                    device_id=device_id,
                    topic=self.confirmation_topic,
                )

        return verdict_msg

    # -------------------------------------------------------------------------
    # Channel Watcher & Background Execution
    # -------------------------------------------------------------------------

    def check_channel_and_process(self, timeout: float = 0.1) -> FlushedBatch | None:
        """Get the next flushed batch from queue channel if not empty and process it."""
        try:
            batch = self.flush_channel.get(timeout=timeout)
        except queue.Empty:
            return None

        try:
            self.process_pipeline(batch)
        finally:
            self.flush_channel.task_done()

        return batch

    def run_watcher(self, stop_event: threading.Event | None = None) -> None:
        """Continuously check whether the queue channel has items to process."""
        event = stop_event or self._stop_event
        logger.info("Flush channel watcher started")
        while not event.is_set():
            self.check_channel_and_process(timeout=0.1)
        logger.info("Flush channel watcher stopped")

    def run_consumer(self, stop_event: threading.Event | None = None) -> None:
        """Continuous consumer loop consuming readings from broker and buffering."""
        if self.consumer is None:
            logger.warning("No broker consumer configured, consumer loop exiting")
            return

        try:
            self.consumer.subscribe([self.input_topic])
            logger.info("Subscribed to broker topic", topic=self.input_topic)
        except Exception:
            logger.exception(
                "Failed to subscribe consumer to topic", topic=self.input_topic
            )

        event = stop_event or self._stop_event
        logger.info("Broker consumer started", topic=self.input_topic)
        while not event.is_set():
            try:
                msg = self.consumer.poll(0.5)
                if msg is not None:
                    if hasattr(msg, "error") and callable(msg.error) and msg.error():
                        err = msg.error()
                        if err is not None:
                            logger.error(
                                "Kafka consumer error: code=%s message=%s",
                                err.code(),
                                str(err),
                            )
                            continue
                    self.consume_broker_message(msg)
            except Exception:
                logger.exception("Error in broker consumer polling loop")
        logger.info("Broker consumer stopped")

    def start(self) -> None:
        """Start watcher, broker consumer, and hourly scheduler background threads."""
        self._stop_event.clear()

        self._watcher_thread = threading.Thread(
            target=self.run_watcher,
            name="orchestrator-watcher",
            daemon=True,
        )
        self._watcher_thread.start()

        if self.consumer is not None:
            self._consumer_thread = threading.Thread(
                target=self.run_consumer,
                name="orchestrator-consumer",
                daemon=True,
            )
            self._consumer_thread.start()

        if self.engine is not None:
            self._hourly_thread = threading.Thread(
                target=self.run_hourly_scheduler,
                args=(self.hourly_interval_seconds,),
                name="orchestrator-hourly-scheduler",
                daemon=True,
            )
            self._hourly_thread.start()

        self._metrics_thread = threading.Thread(
            target=self.run_metrics_logger,
            name="orchestrator-metrics-logger",
            daemon=True,
        )
        self._metrics_thread.start()

        logger.info("Guardian orchestrator started")

    def stop(self, timeout: float = 5.0) -> None:
        """Stop all background threads and flush producer."""
        self._stop_event.set()

        if self._watcher_thread is not None and self._watcher_thread.is_alive():
            self._watcher_thread.join(timeout=timeout)
            self._watcher_thread = None

        if self._consumer_thread is not None and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=timeout)
            self._consumer_thread = None

        if self._hourly_thread is not None and self._hourly_thread.is_alive():
            self._hourly_thread.join(timeout=timeout)
            self._hourly_thread = None

        if self._metrics_thread is not None and self._metrics_thread.is_alive():
            self._metrics_thread.join(timeout=timeout)
            self._metrics_thread = None

        if self.producer is not None and hasattr(self.producer, "flush"):
            try:
                self.producer.flush(timeout=timeout)
            except Exception:
                logger.exception("Error flushing broker producer")

        if self.consumer is not None and hasattr(self.consumer, "close"):
            try:
                self.consumer.close()
            except Exception:
                logger.exception("Error closing broker consumer")

        self.flush_batch_logs()

        logger.info("Guardian orchestrator stopped")

    def flush_batch_logs(self) -> None:
        """Log periodic batch summary of orchestrator events (every 30 seconds)."""
        with self._metrics_lock:
            now = time.monotonic()
            elapsed = now - self._last_metrics_log_time
            if elapsed <= 0:
                elapsed = 1e-6

            pub = self._metrics["anomalies_published"]
            consumed = self._metrics["messages_consumed"]
            batches = self._metrics["batches_processed"]
            readings = self._metrics["readings_processed"]

            if pub == 0 and consumed == 0 and batches == 0:
                self._last_metrics_log_time = now
                return

            self._metrics["anomalies_published"] = 0
            self._metrics["messages_consumed"] = 0
            self._metrics["batches_processed"] = 0
            self._metrics["readings_processed"] = 0
            self._last_metrics_log_time = now

        if pub > 0:
            logger.info(
                "Published %d anomalies to broker topic '%s' in the last %.1fs",
                pub,
                self.anomaly_topic,
                elapsed,
                anomalies_published=pub,
                topic=self.anomaly_topic,
                period_seconds=round(elapsed, 1),
            )
        if consumed > 0:
            logger.info(
                "Consumed %d messages from broker topic '%s' in the last %.1fs",
                consumed,
                self.input_topic,
                elapsed,
                messages_consumed=consumed,
                topic=self.input_topic,
                period_seconds=round(elapsed, 1),
            )
        if batches > 0:
            logger.info(
                "Processed %d flushed batches (%d readings) in the last %.1fs",
                batches,
                readings,
                elapsed,
                batches_processed=batches,
                readings_processed=readings,
                period_seconds=round(elapsed, 1),
            )

    def run_metrics_logger(self, stop_event: threading.Event | None = None) -> None:
        """Periodically flush batched logs every 30 seconds."""
        event = stop_event or self._stop_event
        while not event.wait(timeout=self.log_batch_interval_seconds):
            self.flush_batch_logs()

    # -------------------------------------------------------------------------
    # Hourly Database Window Evaluation
    # -------------------------------------------------------------------------

    def evaluate_hourly_database_window(
        self,
        lookback_seconds: float = 3600.0,
        end_time: datetime | None = None,
    ) -> dict[str, Any]:
        """Pull the last lookback_seconds of raw readings from the database,

        extract long-window features, and run long-window ML predictions.
        """
        if self.engine is None:
            logger.debug("Database engine not configured, skipping hourly evaluation")
            return {"readings_count": 0, "verdicts": {}, "anomalies": []}

        eval_end = end_time or datetime.now(UTC)
        eval_start = eval_end - timedelta(seconds=lookback_seconds)

        try:
            with Session(self.engine) as session:
                stmt = (
                    select(RawReading)
                    .where(
                        RawReading.timestamp >= eval_start,
                        RawReading.timestamp <= eval_end,
                    )
                    .order_by(RawReading.timestamp.asc())
                )
                rows = session.scalars(stmt).all()
        except Exception:
            logger.exception("Failed to query raw readings for hourly evaluation")
            return {"readings_count": 0, "verdicts": {}, "anomalies": []}

        if not rows:
            logger.info(
                "Hourly evaluation: no readings found in database for [%s, %s]",
                eval_start,
                eval_end,
            )
            return {"readings_count": 0, "verdicts": {}, "anomalies": []}

        from guardian_layer.utils.schema import Metadata

        readings: list[Sensor] = []
        for row in rows:
            meta = None
            if row.sensor_metadata:
                try:
                    meta = Metadata.model_validate(row.sensor_metadata)
                except Exception:
                    pass
            readings.append(
                Sensor(
                    sensor_id=row.sensor_id,
                    cluster_id=row.cluster_id or "default",
                    value=row.value,
                    unit=row.unit,
                    timestamp=row.timestamp,
                    metadata=meta,
                )
            )

        long_features = process_long_window(
            self.long_preprocess,
            self.preprocessor,
            self.decompose,
            self.method,
            readings,
        )

        detector_to_use = self.long_detector or self.detector
        verdicts: dict[str, Verdict] = {}
        model_output_rows: list[dict[str, Any]] = []
        detected_anomalies: list[tuple[str, Anomalies]] = []

        if detector_to_use is not None:
            model_name = self._get_model_version(
                detector_to_use, default="long_window_24h"
            )
            verdicts = self._evaluate_detector(detector_to_use, long_features)
            for sid, verdict in verdicts.items():
                if verdict.anomalous and verdict.label is not None:
                    detected_anomalies.append((sid, verdict.label))

                model_output_rows.append(
                    {
                        "sensor_id": verdict.sensor_id,
                        "window_end": eval_end,
                        "score": float(verdict.score),
                        "is_anomaly": bool(verdict.anomalous),
                        "model_version": model_name,
                    }
                )

        if self.engine is not None and model_output_rows:
            try:
                insert_batch(self.engine, ModelOutput, model_output_rows)
            except Exception:
                logger.exception("Failed to persist hourly model outputs to database")

        unique_anomalies = dict.fromkeys(detected_anomalies)
        emitted_records: list[AnomalyRecord] = []
        for sensor_id, anomaly_label in unique_anomalies:
            record = AnomalyRecord(
                device_id=sensor_id,
                reading_id=None,
                label=anomaly_label,
                detected_at=eval_end,
            )
            self.publish_anomaly(record)
            emitted_records.append(record)

        logger.info(
            "Hourly database evaluation completed",
            readings_count=len(readings),
            sensors_count=len(long_features),
            flagged=len(emitted_records),
        )

        return {
            "readings_count": len(readings),
            "long_features": long_features,
            "verdicts": verdicts,
            "anomalies": emitted_records,
        }

    def run_hourly_scheduler(
        self,
        interval_seconds: float = 3600.0,
        stop_event: threading.Event | None = None,
    ) -> None:
        """Periodic background worker pulling readings from database and predicting."""
        event = stop_event or self._stop_event
        logger.info(
            "Hourly database evaluation scheduler started",
            interval_seconds=interval_seconds,
        )
        while not event.is_set():
            elapsed = 0.0
            while elapsed < interval_seconds and not event.is_set():
                time.sleep(min(1.0, interval_seconds - elapsed))
                elapsed += 1.0
            if event.is_set():
                break
            try:
                self.evaluate_hourly_database_window(lookback_seconds=interval_seconds)
            except Exception:
                logger.exception("Error in periodic database evaluation")
        logger.info("Hourly database evaluation scheduler stopped")

    def join(self) -> None:
        """Block until all published batches in the channel have been processed."""
        self.flush_channel.join()

    def __enter__(self) -> Orchestrator:
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop()

    # -------------------------------------------------------------------------
    # Properties & Inspection
    # -------------------------------------------------------------------------

    @property
    def emitted_anomalies(self) -> list[AnomalyRecord]:
        """All anomaly records emitted by the orchestrator."""
        with self._lock:
            return list(self._emitted_anomalies)

    @property
    def processed_batches(self) -> list[FlushedBatch]:
        """All flushed batches processed by the orchestrator."""
        with self._lock:
            return list(self._processed_batches)

    @property
    def is_running(self) -> bool:
        """Whether any background orchestrator worker threads are active."""
        watcher_alive = (
            self._watcher_thread is not None and self._watcher_thread.is_alive()
        )
        consumer_alive = (
            self._consumer_thread is not None and self._consumer_thread.is_alive()
        )
        hourly_alive = (
            self._hourly_thread is not None and self._hourly_thread.is_alive()
        )
        return watcher_alive or consumer_alive or hourly_alive


# Architectural Alias
GuardianOrchestrator = Orchestrator


def main() -> None:
    """Run the Guardian Orchestrator as a standalone background service."""
    import os
    import signal

    settings = get_settings()

    # 1. Database engine (auto-detect url or secrets)
    def _get_database_url() -> str:
        for env_var in ("GUARDIAN_DATABASE_URL", "DATABASE_URL"):
            val = os.getenv(env_var)
            if val:
                return val

        # Check for postgres password file or env vars
        password = ""
        for env_var in ("POSTGRES_PASSWORD_PATH", "POSTGRES_PASSWORD_FILE"):
            p = os.getenv(env_var)
            if p and os.path.exists(p):
                password = Path(p).read_text().strip()
                break

        if not password:
            pwd = os.getenv("POSTGRES_PASSWORD", "")
            if pwd:
                password = pwd
            else:
                for default_path in (
                    Path("secrets/postgres_password.txt"),
                    Path("/run/secrets/postgres_password"),
                ):
                    if default_path.exists():
                        password = default_path.read_text().strip()
                        break

        if not password:
            return ""

        import urllib.parse

        user = os.getenv("POSTGRES_USER", "guardian")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "guardian")
        quoted_pwd = urllib.parse.quote_plus(password)
        return f"postgresql://{user}:{quoted_pwd}@{host}:{port}/{db}"

    engine = None
    db_url = _get_database_url()
    if db_url:
        try:
            from guardian_layer.storage import create_schema, make_engine

            engine = make_engine(db_url)
            create_schema(engine)
            logger.info(
                "Database engine initialized for orchestrator",
                database_configured=True,
            )
        except Exception:
            logger.exception("Failed to connect to database engine")
    else:
        logger.warning(
            "No database configured or postgres password found. "
            "Running orchestrator WITHOUT database persistence! "
            "Set GUARDIAN_DATABASE_URL to enable persistence."
        )

    # 2. Broker configuration
    def _get_kafka_password() -> str:
        for env_var in ("KAFKA_PASSWORD_PATH", "KAFKA_PASSWORD_FILE"):
            p = os.getenv(env_var)
            if p and os.path.exists(p):
                return Path(p).read_text().strip()
        pwd = os.getenv("KAFKA_PASSWORD", "")
        if pwd:
            return pwd
        # Automatically detect local host secrets file or docker secret
        for default_path in (
            Path("secrets/kafka_client_password.txt"),
            Path("/run/secrets/kafka_client_password"),
        ):
            if default_path.exists():
                return default_path.read_text().strip()
        return ""

    bootstrap = os.getenv("KAFKA_BOOTSTRAP") or settings.kafka.bootstrap_servers
    username = os.getenv("KAFKA_USERNAME") or os.getenv("KAFKA_USER", "guardian")
    password = _get_kafka_password()
    group_id = os.getenv("KAFKA_GROUP_ID") or settings.kafka.group_id
    input_topic = os.getenv("KAFKA_INPUT_TOPIC") or settings.kafka.topic_input
    anomaly_topic = os.getenv("KAFKA_ANOMALY_TOPIC", "telemetry-anomaly")

    kafka_common_conf: dict[str, Any] = {
        "bootstrap.servers": bootstrap,
    }
    if username and password:
        kafka_common_conf.update(
            {
                "security.protocol": os.getenv(
                    "KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"
                ),
                "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
                "sasl.username": username,
                "sasl.password": password,
            }
        )
    else:
        logger.warning(
            "No Kafka password found. If connecting to SASL_PLAINTEXT broker, "
            "Kafka will close the connection."
        )

    consumer_conf = {
        **kafka_common_conf,
        "group.id": group_id,
        "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    }

    producer_conf = {
        **kafka_common_conf,
    }

    producer = Producer(producer_conf)
    consumer = Consumer(consumer_conf)
    consumer.subscribe([input_topic])
    logger.info(
        "Kafka consumer and producer initialized",
        bootstrap=bootstrap,
        input_topic=input_topic,
        anomaly_topic=anomaly_topic,
    )

    orchestrator = Orchestrator(
        engine=engine,
        settings=settings,
        consumer=consumer,
        producer=producer,
        input_topic=input_topic,
        anomaly_topic=anomaly_topic,
    )

    # 3. Graceful shutdown handler
    stop_event = threading.Event()

    def _shutdown(signum: int, frame: Any) -> None:
        logger.info("Shutdown signal received (%s), stopping...", signum)
        stop_event.set()
        orchestrator.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # 4. Start orchestrator
    logger.info(
        "Starting Guardian Orchestrator [brokers=%s, input=%s, anomaly=%s]",
        bootstrap,
        input_topic,
        anomaly_topic,
    )
    orchestrator.start()

    try:
        while not stop_event.is_set():
            stop_event.wait(timeout=1.0)
    finally:
        orchestrator.stop()


if __name__ == "__main__":
    logger.info("Running Guardian Orchestrator as standalone service")
    main()
