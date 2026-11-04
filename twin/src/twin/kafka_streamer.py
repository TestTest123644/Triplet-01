"""Kafka Streaming Engine for Twin Layer (Layer 1).

Assigned to: Agent_Kafka
Handles outbound data streaming only. Enforces the strict JSON schema
from context.md, cryptographic Ed25519 signing, and the 300-byte payload limit.
"""

from __future__ import annotations

import collections
import json
import logging
import math
import os
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
from pydantic import BaseModel, Field, ValidationError, field_validator

from twin.crypto_utils import canonicalize_for_signing, sign_payload, verify_payload_signature
from twin.wire_limits import WIRE_VALUE_MAX, WIRE_VALUE_MIN

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

    @field_validator("value")
    def validate_wire_value(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError(f"Sensor value must be finite, got {v}")
        if v < WIRE_VALUE_MIN or v > WIRE_VALUE_MAX:
            raise ValueError(
                f"Sensor value {v} outside wire bounds [{WIRE_VALUE_MIN}, {WIRE_VALUE_MAX}]"
            )
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
        producer_config: Optional[Dict[str, Any]] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.mock_mode = mock_mode
        self.producer = None
        self.sent_count = 0

        self.sink_max = int(os.getenv("SINK_MAX", "10000"))
        self.sink_buffer: collections.deque[bytes] = collections.deque(maxlen=self.sink_max)

        self.produced = 0
        self.delivered = 0
        self.delivery_failed = 0
        self.sunk = 0
        self.dropped = 0
        self.rejected_oversize = 0
        self.rejected_invalid = 0

        self._oversize_alerted_devices: set[str] = set()
        self._rate_history: collections.deque[Tuple[float, int]] = collections.deque(maxlen=1000)

        # Operational health state tracking
        self._error_history: collections.deque[float] = collections.deque(maxlen=1000)
        self._kafka_state: str = "disabled" if mock_mode else "ok"

        if not mock_mode:
            try:
                import confluent_kafka

                message_timeout_ms = int(os.getenv("KAFKA_MESSAGE_TIMEOUT_MS", "10000"))
                conf = {
                    "bootstrap.servers": self.bootstrap_servers,
                    "linger.ms": 5,
                    "batch.num.messages": 1000,
                    "queue.buffering.max.messages": 100000,
                    "message.timeout.ms": message_timeout_ms,
                    "acks": 1,
                }
                if producer_config:
                    conf.update(producer_config)

                self.producer = confluent_kafka.Producer(conf)
                self._kafka_state = "ok"
            except Exception as e:
                logger.warning(
                    f"Could not connect to Kafka broker at {self.bootstrap_servers} ({e}). "
                    "Falling back to sink buffer mode."
                )
                self.producer = None
                self._kafka_state = "disabled"

    def _record_error(self) -> None:
        """Record error timestamp and handle transition to degraded state."""
        now = time.monotonic()
        self._error_history.append(now)
        if self._kafka_state != "degraded":
            self._kafka_state = "degraded"
            logger.warning("[ALERT] Kafka producer degraded: buffer error or delivery failure encountered")

    def _record_delivery_success(self) -> None:
        """Record successful delivery and transition back to nominal if no recent errors."""
        now = time.monotonic()
        while self._error_history and now - self._error_history[0] > 10.0:
            self._error_history.popleft()
        if not self._error_history and self._kafka_state == "degraded":
            self._kafka_state = "ok"
            logger.info("[NOMINAL] Kafka producer recovered to nominal operational state")

    def _on_delivery(self, err: Any, msg: Any) -> None:
        """Delivery report callback for confluent-kafka producer."""
        if err is not None:
            self.delivery_failed += 1
            self._record_error()
        else:
            self.delivered += 1
            self._record_delivery_success()

    def _sink_payload(self, payload_bytes: bytes) -> None:
        """Append to bounded sink buffer and track dropped packets."""
        if len(self.sink_buffer) >= self.sink_max:
            self.dropped += 1
        self.sink_buffer.append(payload_bytes)
        self.sunk += 1

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
        try:
            reading = TwinSensorReading(**payload_to_sign)
        except (ValueError, ValidationError) as e:
            self.rejected_invalid += 1
            raise ValueError(f"Invalid reading payload: {e}") from e
        return reading

    def stream_reading(
        self, raw_reading: Dict[str, Any], private_key_b64: str
    ) -> Optional[Tuple[TwinSensorReading, bytes]]:
        """Validate, sign, serialize, verify size limit (<300 bytes), and emit to Kafka."""
        reading = self.sign_and_build_reading(raw_reading, private_key_b64)
        serialized_bytes = serialize_reading(reading)

        # Enforce the strict 300-byte constraint
        payload_size = len(serialized_bytes)
        if payload_size > self.MAX_PAYLOAD_BYTES:
            self.rejected_oversize += 1
            dev_id = reading.device_id
            if dev_id not in self._oversize_alerted_devices:
                self._oversize_alerted_devices.add(dev_id)
                logger.warning(
                    f"[ALERT] Payload size {payload_size} bytes for device {dev_id} "
                    f"exceeds strict {self.MAX_PAYLOAD_BYTES}-byte limit"
                )
            return None

        if self.producer is not None:
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=reading.device_id.encode("utf-8"),
                    value=serialized_bytes,
                    on_delivery=self._on_delivery,
                )
                self.produced += 1
                # Poll to serve delivery callbacks without blocking
                self.producer.poll(0)
            except BufferError:
                self._record_error()
                self.producer.poll(0)
                try:
                    self.producer.produce(
                        topic=self.topic,
                        key=reading.device_id.encode("utf-8"),
                        value=serialized_bytes,
                        on_delivery=self._on_delivery,
                    )
                    self.produced += 1
                    self.producer.poll(0)
                except Exception:
                    self._sink_payload(serialized_bytes)
            except Exception:
                self._record_error()
                self._sink_payload(serialized_bytes)
        else:
            self._sink_payload(serialized_bytes)

        self.sent_count += 1
        now = time.monotonic()
        if not self._rate_history or now - self._rate_history[-1][0] >= 0.2:
            self._rate_history.append((now, self.produced + self.sunk))

        return reading, serialized_bytes

    def get_kafka_state(self) -> str:
        """Evaluate current operational state of Kafka producer pipeline."""
        if self.producer is None:
            return "disabled"
        now = time.monotonic()
        while self._error_history and now - self._error_history[0] > 10.0:
            self._error_history.popleft()
        if self._error_history:
            return "degraded"
        return self._kafka_state

    def stats(self) -> Dict[str, Any]:
        """Return streaming metrics and error counters."""
        return {
            "produced": self.produced,
            "delivered": self.delivered,
            "delivery_failed": self.delivery_failed,
            "sunk": self.sunk,
            "dropped": self.dropped,
            "rejected_oversize": self.rejected_oversize,
            "rejected_invalid": self.rejected_invalid,
            "kafka_state": self.get_kafka_state(),
        }

    def get_msgs_per_sec(self) -> float:
        """Calculate throughput averaged over the last 10 seconds of wall-clock time."""
        now = time.monotonic()
        total = self.produced + self.sunk
        while self._rate_history and now - self._rate_history[0][0] > 10.0:
            self._rate_history.popleft()
        if not self._rate_history:
            return 0.0
        oldest_time, oldest_count = self._rate_history[0]
        duration = now - oldest_time
        if duration <= 0:
            return 0.0
        return round((total - oldest_count) / duration, 2)

    def flush(self, timeout: float = 1.0) -> None:
        """Flush remaining buffered messages in producer."""
        if self.producer is not None:
            self.producer.flush(timeout=timeout)
