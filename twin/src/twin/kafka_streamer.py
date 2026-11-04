"""Kafka Streaming Engine for Twin Layer (Layer 1).

Assigned to: Agent_Kafka
Handles outbound data streaming only. Enforces the strict JSON schema
from context.md, cryptographic Ed25519 signing, and the 300-byte payload limit.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
from pydantic import BaseModel, Field, field_validator

from twin.crypto_utils import canonicalize_for_signing, sign_payload, verify_payload_signature

logger = logging.getLogger("twin.kafka_streamer")

DeviceTypeEnum = Literal[
    "traffic_camera",
    "water_level",
    "power_load",
    "air_quality",
    "emergency_call",
]

DeviceStatusEnum = Literal[
    "ok",
    "isolated",
    "quarantined",
    "rolled_back",
    "modelled",
]


class ZoneLocation(BaseModel):
    zone_id: str
    lat: Optional[float] = None
    lon: Optional[float] = None

    model_config = {"extra": "forbid"}


class TwinSensorReading(BaseModel):
    reading_id: str
    device_id: str
    device_type: DeviceTypeEnum
    ts: str  # RFC 3339 date-time format
    seq: int = Field(ge=0)
    value: float
    unit: str
    location: Union[str, ZoneLocation]
    status: DeviceStatusEnum
    sig: str  # Ed25519 signature

    model_config = {"extra": "forbid"}

    @field_validator("ts")
    def validate_rfc3339(cls, v: str) -> str:
        if not v.endswith("Z"):
            raise ValueError("Timestamp must end with 'Z' indicating UTC RFC 3339 format")
        return v


def serialize_reading(reading: TwinSensorReading) -> bytes:
    """Serialize TwinSensorReading to compact UTF-8 JSON bytes."""
    data = reading.model_dump()
    json_str = json.dumps(data, separators=(",", ":"), ensure_ascii=True)
    return json_str.encode("utf-8")


class KafkaStreamer:
    """Outbound Kafka producer streaming signed telemetry envelopes under 300 bytes."""

    MAX_PAYLOAD_BYTES = 300

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "twin.sensors",
        mock_mode: bool = False,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.mock_mode = mock_mode
        self.producer = None
        self.sent_count = 0
        self.sink_buffer: List[bytes] = []

        if not mock_mode:
            try:
                import confluent_kafka

                conf = {
                    "bootstrap.servers": self.bootstrap_servers,
                    "linger.ms": 5,
                    "batch.num.messages": 1000,
                    "queue.buffering.max.messages": 100000,
                    "acks": 1,
                }
                self.producer = confluent_kafka.Producer(conf)
            except Exception as e:
                logger.warning(
                    f"Could not connect to Kafka broker at {self.bootstrap_servers} ({e}). "
                    "Falling back to sink buffer mode."
                )
                self.producer = None

    def sign_and_build_reading(
        self, raw_reading: Dict[str, Any], private_key_b64: str
    ) -> TwinSensorReading:
        """Sign payload using Ed25519 and construct validated TwinSensorReading model."""
        payload_to_sign = {
            "reading_id": raw_reading["reading_id"],
            "device_id": raw_reading["device_id"],
            "device_type": raw_reading["device_type"],
            "ts": raw_reading["ts"],
            "seq": raw_reading["seq"],
            "value": round(float(raw_reading["value"]), 2),
            "unit": raw_reading["unit"],
            "location": raw_reading["location"],
            "status": raw_reading["status"],
        }
        sig = sign_payload(payload_to_sign, private_key_b64)
        if raw_reading.get("_corrupt_sig"):
            # Corrupt signature string to simulate link-level byte mutation
            corrupted = "AAAA" if sig.endswith("==") else "BB=="
            sig = sig[:-4] + corrupted
        payload_to_sign["sig"] = sig
        reading = TwinSensorReading(**payload_to_sign)
        return reading

    def stream_reading(
        self, raw_reading: Dict[str, Any], private_key_b64: str
    ) -> Tuple[TwinSensorReading, bytes]:
        """Validate, sign, serialize, verify size limit (<300 bytes), and emit to Kafka."""
        reading = self.sign_and_build_reading(raw_reading, private_key_b64)
        serialized_bytes = serialize_reading(reading)

        # Enforce the strict 300-byte constraint
        payload_size = len(serialized_bytes)
        if payload_size > self.MAX_PAYLOAD_BYTES:
            raise ValueError(
                f"Payload size {payload_size} bytes exceeds the strict {self.MAX_PAYLOAD_BYTES}-byte limit! "
                f"Payload: {serialized_bytes.decode('utf-8')}"
            )

        if self.producer is not None:
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=reading.device_id.encode("utf-8"),
                    value=serialized_bytes,
                )
                # Poll to serve delivery callbacks without blocking
                self.producer.poll(0)
            except Exception as e:
                logger.error(f"Error producing to Kafka: {e}")
                self.sink_buffer.append(serialized_bytes)
        else:
            self.sink_buffer.append(serialized_bytes)

        self.sent_count += 1
        return reading, serialized_bytes

    def flush(self, timeout: float = 1.0) -> None:
        """Flush remaining buffered messages in producer."""
        if self.producer is not None:
            self.producer.flush(timeout=timeout)
