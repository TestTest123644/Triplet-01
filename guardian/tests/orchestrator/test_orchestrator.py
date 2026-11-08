"""Tests for the Guardian Orchestrator."""

from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from guardian_layer.ai.inference.detector import AnomalyDetector
from guardian_layer.ai.inference.model import BaselineModel
from guardian_layer.managers.cluster_manger import ClusterManager
from guardian_layer.orchestrator import GuardianOrchestrator, Orchestrator
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.storage import (
    ModelOutput,
    RawReading,
    TransformedWindow,
    create_schema,
)
from guardian_layer.utils.channels import FlushChannel
from guardian_layer.utils.config import get_settings
from guardian_layer.utils.schema import (
    Anomalies,
    Buffer,
    Cluster,
    FlushedBatch,
    Metadata,
    Sensor,
)


class MockKafkaProducer:
    """Mock Kafka producer that records produce calls."""

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def produce(
        self,
        topic: str,
        value: bytes | str,
        key: bytes | str | None = None,
        **kwargs: Any,
    ) -> None:
        self.messages.append({"topic": topic, "key": key, "value": value})

    def poll(self, timeout: float = 0.0) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        return 0


class MockKafkaConsumer:
    """Mock Kafka consumer delivering a queued list of messages."""

    def __init__(self, messages: list[Any]) -> None:
        self._messages = list(messages)
        self.subscribed: list[str] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def poll(self, timeout: float = 0.1) -> Any:
        if self._messages:
            return self._messages.pop(0)
        return None

    def close(self) -> None:
        self.closed = True


def make_sensor(
    sensor_id: str = "water-01",
    cluster_id: str = "water_level",
    value: float = 1.5,
    packet_order: int = 1,
    timestamp: datetime | None = None,
    lower_range: float = 0.0,
    upper_range: float = 3.0,
) -> Sensor:
    ts = timestamp or datetime.now(UTC)
    return Sensor(
        sensor_id=sensor_id,
        cluster_id=cluster_id,
        value=value,
        unit="m",
        timestamp=ts,
        metadata=Metadata(
            lat=36.8,
            lon=10.2,
            packet_order=packet_order,
            type=cluster_id,
            lower_range=lower_range,
            upper_range=upper_range,
        ),
    )


def make_batch(
    sensor_id: str = "water-01",
    cluster_id: str = "water_level",
    count: int = 20,
    value: float = 1.5,
    start_time: datetime | None = None,
    step_seconds: float = 1.0,
    lower_range: float = 0.0,
    upper_range: float = 3.0,
) -> FlushedBatch:
    start = start_time or (datetime.now(UTC) - timedelta(seconds=count))
    readings = [
        make_sensor(
            sensor_id=sensor_id,
            cluster_id=cluster_id,
            value=value,
            packet_order=i + 1,
            timestamp=start + timedelta(seconds=i * step_seconds),
            lower_range=lower_range,
            upper_range=upper_range,
        )
        for i in range(count)
    ]
    return FlushedBatch(
        id="batch-001",
        sensor_id=sensor_id,
        readings=tuple(readings),
        flushed_at=start + timedelta(seconds=count * step_seconds),
    )


@pytest.fixture
def sqlite_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    create_schema(engine)
    return engine


@pytest.fixture
def trained_detector():
    """Build a trained AnomalyDetector fixture."""
    keys = PreProcess().feature_keys("none")
    classes = get_settings().inference.classes
    variance_idx = keys.index("variance")

    rng = np.random.default_rng(42)
    features = rng.normal(0.0, 0.1, (600, len(keys)))
    labels = rng.choice(3, size=600, p=(0.7, 0.2, 0.1))
    features[labels == 1, variance_idx] += 10.0

    fitted = BaselineModel.fit(features, labels, "none", 0.5, keys, classes)
    model = BaselineModel(
        fitted.classifier, "none", 0.5, keys, classes, window_seconds=20.0
    )
    return AnomalyDetector(model, min_rate=0.0)


# =============================================================================
# Unit Tests
# =============================================================================


def test_orchestrator_initialization():
    orch = Orchestrator()
    assert isinstance(orch.cluster_manager, ClusterManager)
    assert isinstance(orch.flush_channel, FlushChannel)
    assert orch.cluster_manager._sink is orch.flush_channel
    assert orch.input_topic == "telemetry-validated"
    assert orch.anomaly_topic == "telemetry-anomaly"
    assert GuardianOrchestrator is Orchestrator


def test_ingest_reading_creates_cluster_and_buffers():
    orch = Orchestrator()
    reading = make_sensor(sensor_id="s1", cluster_id="c1")

    # Ingest should auto-create cluster and add reading
    ack = orch.ingest_reading(reading)
    assert ack.ok
    assert orch.cluster_manager.validate("c1").ok

    cluster = orch.cluster_manager.get("c1")
    assert isinstance(cluster, Cluster)
    assert "s1" in cluster.buffers

    buf = cluster.buffers["s1"].get("s1")
    assert isinstance(buf, Buffer)
    assert len(buf.readings) == 1
    assert buf.readings[0].value == 1.5


def test_consume_broker_message_formats():
    orch = Orchestrator()
    reading = make_sensor(sensor_id="s1", cluster_id="c1")

    # 1. Direct Sensor instance
    s1 = orch.consume_broker_message(reading)
    assert s1 is reading

    # 2. JSON string
    json_str = reading.model_dump_json()
    s2 = orch.consume_broker_message(json_str)
    assert s2 is not None
    assert s2.sensor_id == "s1"

    # 3. JSON bytes
    json_bytes = json_str.encode("utf-8")
    s3 = orch.consume_broker_message(json_bytes)
    assert s3 is not None
    assert s3.sensor_id == "s1"

    # 4. Dictionary
    payload_dict = reading.model_dump()
    s4 = orch.consume_broker_message(payload_dict)
    assert s4 is not None
    assert s4.sensor_id == "s1"

    # 5. Invalid payload returns None
    assert orch.consume_broker_message(12345) is None


def test_process_pipeline_stores_raw_data_and_features_to_db(sqlite_engine):
    orch = Orchestrator(engine=sqlite_engine)
    batch = make_batch(count=15)

    result = orch.process_pipeline(batch)

    assert result["batch_id"] == "batch-001"
    assert result["readings_count"] == 15
    assert "water-01" in result["short_features"]
    assert "water-01" in result["long_features"]

    # Verify Database Persistence
    with Session(sqlite_engine) as session:
        raw_rows = session.scalars(select(RawReading)).all()
        assert len(raw_rows) == 15
        assert raw_rows[0].sensor_id == "water-01"
        assert raw_rows[0].value == 1.5

        transformed_rows = session.scalars(select(TransformedWindow)).all()
        assert len(transformed_rows) == 1
        window = transformed_rows[0]
        assert window.sensor_id == "water-01"
        # Features contains both short and long keys
        assert "mean" in window.features
        assert "lw_trend_scatter_ratio" in window.features


def test_process_pipeline_static_check_out_of_range():
    producer = MockKafkaProducer()
    orch = Orchestrator(producer=producer)

    # Upper range is 3.0, reading value is 10.0 -> out_of_range
    batch = make_batch(count=10, value=10.0, lower_range=0.0, upper_range=3.0)

    result = orch.process_pipeline(batch)

    # Anomaly detected and published
    anomalies = result["anomalies"]
    assert len(anomalies) >= 1
    assert any(a.label == Anomalies.OUT_OF_RANGE for a in anomalies)

    # Producer received the anomaly
    assert len(producer.messages) >= 1
    assert producer.messages[0]["topic"] == "telemetry-anomaly"
    assert b"out_of_range" in producer.messages[0]["value"]


def test_process_pipeline_static_check_sensor_is_down():
    producer = MockKafkaProducer()
    orch = Orchestrator(producer=producer)

    # Readings ended 60 seconds ago -> sensor_is_down (threshold default is 10s)
    old_start = datetime.now(UTC) - timedelta(seconds=120)
    batch = make_batch(count=10, value=1.5, start_time=old_start)

    result = orch.process_pipeline(batch)

    anomalies = result["anomalies"]
    assert any(a.label == Anomalies.SENSOR_IS_DOWN for a in anomalies)
    assert any(a.label == Anomalies.SENSOR_IS_DOWN for a in orch.emitted_anomalies)


def test_process_pipeline_with_detector_and_model_output_storage(
    sqlite_engine, trained_detector
):
    producer = MockKafkaProducer()
    orch = Orchestrator(
        engine=sqlite_engine,
        producer=producer,
        detector=trained_detector,
    )

    # Normal batch
    normal_batch = make_batch(count=20, value=1.0)
    res_normal = orch.process_pipeline(normal_batch)
    assert "water-01" in res_normal["verdicts"]

    # Verify ModelOutput stored in DB
    with Session(sqlite_engine) as session:
        outputs = session.scalars(select(ModelOutput)).all()
        assert len(outputs) == 1
        assert outputs[0].sensor_id == "water-01"
        assert outputs[0].score >= 0.0


def test_check_channel_and_process():
    orch = Orchestrator()
    batch = make_batch(count=10)

    # Publish batch directly to flush_channel
    orch.flush_channel.publish(batch)
    assert orch.flush_channel.qsize() == 1

    # Check and process takes it and runs pipeline
    processed = orch.check_channel_and_process(timeout=0.1)
    assert processed is batch
    assert orch.flush_channel.qsize() == 0
    assert len(orch.processed_batches) == 1


def test_end_to_end_buffer_watermark_triggers_flush_to_channel():
    orch = Orchestrator()

    # Buffer watermark is 200 by default. Ingest 201 readings to trigger flush
    for i in range(201):
        orch.ingest_reading(make_sensor(packet_order=i + 1))

    # A batch of 100 readings should have been flushed to flush_channel!
    assert orch.flush_channel.qsize() == 1

    # Watcher processes the batch
    batch = orch.check_channel_and_process(timeout=0.1)
    assert batch is not None
    assert len(batch.readings) == 100
    assert len(orch.processed_batches) == 1


def test_orchestrator_background_watcher_and_consumer():
    producer = MockKafkaProducer()
    sensor_msg = make_sensor(sensor_id="stream-1", value=1.2).model_dump_json()
    consumer = MockKafkaConsumer([sensor_msg])

    orch = Orchestrator(
        consumer=consumer,
        producer=producer,
    )

    with orch:
        assert orch.is_running
        # Let background threads poll
        batch = make_batch(count=10)
        orch.flush_channel.publish(batch)

        # Wait for join
        orch.join()

    assert not orch.is_running
    assert len(orch.processed_batches) == 1
    assert orch.processed_batches[0].id == "batch-001"
    assert consumer.closed
