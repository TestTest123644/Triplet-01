"""Unit tests for Phase 3: Crypto, Schema Enforcement, and Kafka Streaming."""

import json
import pytest
from pydantic import ValidationError

from twin.crypto_utils import verify_payload_signature
from twin.kafka_streamer import KafkaStreamer, TwinSensorReading, serialize_reading
from twin.simulation import CitySimulator


def test_schema_valid_payload():
    sim = CitySimulator(seed=42)
    streamer = KafkaStreamer(mock_mode=True)

    readings = sim.tick()
    assert len(readings) == 42

    for raw in readings:
        dev = sim.devices[raw["device_id"]]
        reading, raw_bytes = streamer.stream_reading(raw, dev.private_key)

        # 1. Payload size MUST be strictly <= 300 bytes
        assert len(raw_bytes) <= 300, f"Payload {len(raw_bytes)} bytes exceeds 300 bytes: {raw_bytes}"

        # 2. Signature must verify against device's public key
        payload_dict = json.loads(raw_bytes.decode("utf-8"))
        assert verify_payload_signature(payload_dict, dev.public_key) is True

        # 3. Required fields present
        assert reading.reading_id.startswith("r-")
        assert reading.device_id == dev.device_id
        assert reading.status == "ok"
        assert reading.ts.endswith("Z")


def test_schema_extra_fields_forbidden():
    raw = {
        "reading_id": "r-dev-01-1",
        "device_id": "dev-01",
        "device_type": "traffic_camera",
        "ts": "2026-10-17T12:00:00.000000Z",
        "seq": 1,
        "value": 45.0,
        "unit": "vehicles/min",
        "location": "zone-1",
        "status": "ok",
        "sig": "fake_sig",
        "unauthorized_extra_field": "illegal",
    }
    with pytest.raises(ValidationError):
        TwinSensorReading(**raw)


def test_schema_invalid_status_forbidden():
    raw = {
        "reading_id": "r-dev-01-1",
        "device_id": "dev-01",
        "device_type": "traffic_camera",
        "ts": "2026-10-17T12:00:00.000000Z",
        "seq": 1,
        "value": 45.0,
        "unit": "vehicles/min",
        "location": "zone-1",
        "status": "unknown_status",
        "sig": "fake_sig",
    }
    with pytest.raises(ValidationError):
        TwinSensorReading(**raw)


def test_schema_invalid_timestamp_forbidden():
    raw = {
        "reading_id": "r-dev-01-1",
        "device_id": "dev-01",
        "device_type": "traffic_camera",
        "ts": "2026-10-17 12:00:00",  # missing UTC Z
        "seq": 1,
        "value": 45.0,
        "unit": "vehicles/min",
        "location": "zone-1",
        "status": "ok",
        "sig": "fake_sig",
    }
    with pytest.raises(ValidationError):
        TwinSensorReading(**raw)


def test_payload_size_benchmark_across_all_42_devices():
    sim = CitySimulator(seed=101)
    streamer = KafkaStreamer(mock_mode=True)

    readings = sim.tick()
    sizes = []
    for raw in readings:
        dev = sim.devices[raw["device_id"]]
        _, raw_bytes = streamer.stream_reading(raw, dev.private_key)
        sizes.append(len(raw_bytes))

    max_size = max(sizes)
    min_size = min(sizes)
    avg_size = sum(sizes) / len(sizes)

    # Validate that maximum size is under 300 bytes
    assert max_size <= 300
    print(f"\nPayload size stats: min={min_size}B, max={max_size}B, avg={avg_size:.1f}B")


@pytest.mark.asyncio
async def test_wait_for_kafka_ready_import_and_failure_handling():
    """Verify wait_for_kafka_ready correctly imports confluent_kafka.admin and handles unreachable brokers."""
    from twin.main import wait_for_kafka_ready

    # Test with non-existent port to verify AdminClient doesn't crash on import/attribute access
    ready = await wait_for_kafka_ready("127.0.0.1:59998", max_retries=1, delay=0.01)
    assert ready is False

