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
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine

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
    RawReading,
    TransformedWindow,
)
from guardian_layer.utils.channels import FlushChannel
from guardian_layer.utils.config import Settings, get_settings
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import (
    SENSOR_RULES,
    Ack,
    Anomalies,
    FlushedBatch,
    Sensor,
    SensorType,
    Verdict,
)

logger = get_logger(__name__)


class Orchestrator:
    """Core runtime orchestrator bridging stream brokers, in-memory buffers,

    feature pipelines, AI inference, and database persistence.
    """

    def __init__(
        self,
        cluster_manager: ClusterManager | None = None,
        flush_channel: FlushChannel | None = None,
        consumer: Any = None,
        producer: Any = None,
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
        kafka_bootstrap_servers: str | None = None,
        kafka_group_id: str = "guardian-processor",
        kafka_config: dict[str, Any] | None = None,
    ) -> None:
        self.settings = settings or get_settings()

        # 1. Bounded hand-off queue and ClusterManager
        self.flush_channel = flush_channel or FlushChannel()
        self.cluster_manager = cluster_manager or ClusterManager(
            sink=self.flush_channel
        )

        # 2. Database storage engine
        self.engine = engine

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
        self.detector = detector
        self.long_detector = long_detector
        self.method = self.preprocessor.method

        # 4. Message Broker topics and clients
        self.input_topic = input_topic
        self.anomaly_topic = anomaly_topic
        self.consumer = consumer
        self.producer = producer

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
        self._emitted_anomalies: list[AnomalyRecord] = []
        self._processed_batches: list[FlushedBatch] = []
        self._lock = threading.Lock()

    # -------------------------------------------------------------------------
    # Kafka Initialization Helpers
    # -------------------------------------------------------------------------

    def _init_kafka_consumer(
        self,
        bootstrap_servers: str,
        group_id: str,
        extra_config: dict[str, Any] | None,
    ) -> Any:
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
        except Exception:
            logger.exception("Failed to initialize Kafka consumer")
            return None

    def _init_kafka_producer(
        self,
        bootstrap_servers: str,
        extra_config: dict[str, Any] | None,
    ) -> Any:
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
        except Exception:
            logger.exception("Failed to initialize Kafka producer")
            return None

    # -------------------------------------------------------------------------
    # Ingestion & Broker Consumer
    # -------------------------------------------------------------------------

    def ingest_reading(self, sensor: Sensor) -> Ack:
        """Store sensor reading in its cluster buffer, creating cluster if missing."""
        with self._lock:
            if not self.cluster_manager.validate(sensor.cluster_id).ok:
                self.cluster_manager.create(sensor)
            return self.cluster_manager.add(sensor)

    def consume_broker_message(self, message: Any) -> Sensor | None:
        """Parse raw broker message into a Sensor reading and ingest it."""
        try:
            if isinstance(message, Sensor):
                self.ingest_reading(message)
                return message

            # Handle confluent_kafka.Message or object with error()/value()
            if hasattr(message, "error") and callable(message.error):
                err = message.error()
                if err is not None:
                    logger.error("Consumer message error", error=str(err))
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

        now = datetime.now(UTC)
        eval_window_end = window_end or now
        detected_anomalies: list[tuple[str, Anomalies]] = []

        # ---------------------------------------------------------------------
        # Stage 1: Store raw data to database
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
            try:
                insert_batch(self.engine, RawReading, raw_rows)
            except Exception:
                logger.exception("Failed to persist raw readings to database")

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
        # Stage 5: Run detector(s) and store output to database
        # ---------------------------------------------------------------------
        verdicts: dict[str, Verdict] = {}
        model_output_rows: list[dict[str, Any]] = []

        if self.detector is not None:
            v_short = self._evaluate_detector(self.detector, short_features)
            verdicts.update(v_short)
            for sid, verdict in v_short.items():
                if verdict.anomalous and verdict.label is not None:
                    detected_anomalies.append((sid, verdict.label))

                model_output_rows.append(
                    {
                        "sensor_id": verdict.sensor_id,
                        "window_end": eval_window_end,
                        "score": float(verdict.score),
                        "is_anomaly": bool(verdict.anomalous),
                        "model_version": getattr(
                            self.detector, "model_version", "short-baseline"
                        ),
                    }
                )

        if self.long_detector is not None:
            v_long = self._evaluate_detector(self.long_detector, long_features)
            for sid, verdict in v_long.items():
                if verdict.anomalous and verdict.label is not None:
                    detected_anomalies.append((sid, verdict.label))

                model_output_rows.append(
                    {
                        "sensor_id": verdict.sensor_id,
                        "window_end": eval_window_end,
                        "score": float(verdict.score),
                        "is_anomaly": bool(verdict.anomalous),
                        "model_version": getattr(
                            self.long_detector, "model_version", "long-baseline"
                        ),
                    }
                )

        if self.engine is not None and model_output_rows:
            try:
                insert_batch(self.engine, ModelOutput, model_output_rows)
            except Exception:
                logger.exception("Failed to persist model outputs to database")

        # ---------------------------------------------------------------------
        # Stage 6: Publish any detected anomaly to Kafka broker
        # ---------------------------------------------------------------------
        unique_anomalies = dict.fromkeys(detected_anomalies)
        emitted_records: list[AnomalyRecord] = []

        for sensor_id, anomaly_label in unique_anomalies:
            record = AnomalyRecord(
                device_id=sensor_id,
                reading_id=None,
                label=anomaly_label,
                detected_at=now,
            )
            self.publish_anomaly(record)
            emitted_records.append(record)

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

            if hasattr(self.producer, "produce"):
                self.producer.produce(topic=self.anomaly_topic, key=key, value=payload)
                if hasattr(self.producer, "poll"):
                    self.producer.poll(0)
            elif hasattr(self.producer, "send"):
                self.producer.send(self.anomaly_topic, value=payload)
            elif hasattr(self.producer, "append"):
                self.producer.append(record)
            logger.info(
                "Published anomaly to broker",
                topic=self.anomaly_topic,
                device_id=record.device_id,
                label=record.label,
            )
        except Exception:
            logger.exception(
                "Failed to publish anomaly to broker",
                device_id=record.device_id,
                topic=self.anomaly_topic,
            )

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

        event = stop_event or self._stop_event
        logger.info("Broker consumer started", topic=self.input_topic)
        while not event.is_set():
            try:
                msg = self.consumer.poll(0.1)
                if msg is not None:
                    self.consume_broker_message(msg)
            except Exception:
                logger.exception("Error in broker consumer polling loop")
        logger.info("Broker consumer stopped")

    def start(self) -> None:
        """Start watcher and broker consumer background threads."""
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

        logger.info("Guardian orchestrator stopped")

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
        return watcher_alive or consumer_alive


# Architectural Alias
GuardianOrchestrator = Orchestrator


def main() -> None:
    """Run the Guardian Orchestrator as a standalone background service."""
    import os
    import signal

    settings = get_settings()

    # 1. Database engine (if configured)
    engine = None
    db_url = os.getenv("GUARDIAN_DATABASE_URL")
    if db_url:
        try:
            from guardian_layer.storage import make_engine

            engine = make_engine(db_url)
            logger.info("Database engine initialized for orchestrator")
        except Exception:
            logger.exception("Failed to connect to GUARDIAN_DATABASE_URL")

    # 2. Broker configuration
    kafka_bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP",
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29094"),
    )
    input_topic = os.getenv("KAFKA_VALIDATED_TOPIC", "telemetry-validated")
    anomaly_topic = os.getenv("KAFKA_ANOMALY_TOPIC", "telemetry-anomaly")
    group_id = os.getenv("KAFKA_GROUP_ID", "guardian-processor")

    orchestrator = Orchestrator(
        engine=engine,
        settings=settings,
        kafka_bootstrap_servers=kafka_bootstrap,
        kafka_group_id=group_id,
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
        kafka_bootstrap,
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
    main()
