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
import re
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
from pydantic import BaseModel, Field, ValidationError, field_validator

from twin.crypto_utils import canonicalize_for_signing, sign_payload, verify_payload_signature
from twin.kafka_config import get_broker_address_family, kafka_client_config
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


def tamper_value_bytes(serialized_bytes: bytes) -> bytes:
    """Tamper one digit of 'value' field in serialized bytes while keeping JSON valid and identical length."""
    m = re.search(rb'"value":(-?[0-9]+(?:\.[0-9]+)?)', serialized_bytes)
    if not m:
        return serialized_bytes
    val_start, val_end = m.span(1)
    val_bytes = bytearray(serialized_bytes[val_start:val_end])
    # Find last digit in value span and change it (digit + 1 mod 10)
    for i in range(len(val_bytes) - 1, -1, -1):
        b = val_bytes[i]
        if ord(b"0") <= b <= ord(b"9"):
            new_digit = ord(b"0") + ((b - ord(b"0") + 1) % 10)
            val_bytes[i] = new_digit
            break
    return serialized_bytes[:val_start] + bytes(val_bytes) + serialized_bytes[val_end:]


def get_producer_acks_config() -> Tuple[Union[str, int], bool]:
    """Parse and validate KAFKA_ACKS environment variable.

    Returns:
        (acks, enable_idempotence) tuple.
        Allowed KAFKA_ACKS values: 'all' (default), '1', '0'.
        enable.idempotence is True only when acks is 'all'.
    """
    raw = os.getenv("KAFKA_ACKS", "all").strip().lower()
    allowed = ("all", "1", "0")
    if raw not in allowed:
        raise ValueError(
            f"Invalid KAFKA_ACKS: '{raw}'. Allowed: {', '.join(allowed)}"
        )
    if raw == "all":
        return "all", True
    return int(raw), False


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

        # 2.d: Frame history for replay (bounded at 50 frames per device)
        self._frame_history: Dict[str, collections.deque[bytes]] = collections.defaultdict(
            lambda: collections.deque(maxlen=50)
        )
        # 2.e: Delay buffers for held frames (bounded at 600 frames per device)
        self._delay_buffers: Dict[str, collections.deque[Tuple[int, TwinSensorReading, bytes]]] = (
            collections.defaultdict(lambda: collections.deque(maxlen=600))
        )
        self.last_emitted_backlog: List[Tuple[TwinSensorReading, bytes]] = []

        # Operational health state tracking
        self._error_history: collections.deque[float] = collections.deque(maxlen=1000)
        self._kafka_state: str = "disabled" if mock_mode else "ok"
        self._producer_conf: Dict[str, Any] = {}
        self._cached_password: Optional[str] = None

        # Validate KAFKA_ACKS and KAFKA_BROKER_ADDRESS_FAMILY env vars on startup (raises ValueError if invalid)
        get_producer_acks_config()
        get_broker_address_family()

        if not mock_mode:
            try:
                import confluent_kafka

                conf = self.build_producer_config(self.bootstrap_servers, producer_config)
                self._producer_conf = dict(conf)
                self._cached_password = conf.get("sasl.password")
                self.producer = confluent_kafka.Producer(conf)
                self._kafka_state = "ok"
            except ValueError:
                raise
            except Exception as e:
                logger.warning(
                    f"Could not connect to Kafka broker at {self.bootstrap_servers} ({e}). "
                    "Falling back to sink buffer mode."
                )
                self.producer = None
                self._kafka_state = "disabled"

    @classmethod
    def build_producer_config(
        cls,
        bootstrap_servers: str = "localhost:19092",
        producer_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build producer configuration dictionary with security settings and durable acks/idempotence."""
        acks_val, enable_idempotence = get_producer_acks_config()
        sasl_conf = kafka_client_config()
        message_timeout_ms = int(os.getenv("KAFKA_MESSAGE_TIMEOUT_MS", "10000"))
        conf: Dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 5,
            "batch.num.messages": 1000,
            "queue.buffering.max.messages": 100000,
            "message.timeout.ms": message_timeout_ms,
            "acks": acks_val,
            "enable.idempotence": enable_idempotence,
            "broker.address.family": get_broker_address_family(),
        }
        conf.update(sasl_conf)
        if producer_config:
            conf.update(producer_config)
        return conf

    def reload_kafka_password(self) -> str:
        """Hot-swap Kafka producer with re-resolved SASL password without losing messages.

        Returns 'changed', 'unchanged', or 'error: <short reason>'.
        """
        from twin.secrets import get_secret

        protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip()
        if protocol == "PLAINTEXT":
            return "unchanged"

        new_password = get_secret("KAFKA_PASSWORD")
        if not new_password:
            return "error: missing or empty KAFKA_PASSWORD"

        if self._cached_password == new_password:
            return "unchanged"

        if self.mock_mode and self.producer is None:
            self._cached_password = new_password
            return "changed"

        # Build new producer configuration with updated password
        new_conf = dict(self._producer_conf) if self._producer_conf else self.build_producer_config(self.bootstrap_servers)
        new_conf["sasl.password"] = new_password

        try:
            import confluent_kafka

            new_producer = confluent_kafka.Producer(new_conf)

            # Probe broker metadata to verify authentication credentials before swapping
            try:
                new_producer.list_topics(timeout=5)
            except Exception as probe_err:
                logger.warning(
                    f"New Kafka credentials rejected or broker unreachable during metadata probe: {type(probe_err).__name__}"
                )
                return "error: new credentials rejected by broker"

            # Flush old producer before swapping (deadline 5 s)
            old_producer = self.producer
            if old_producer is not None:
                try:
                    old_producer.flush(timeout=5.0)
                except Exception as flush_err:
                    logger.warning(f"Error flushing previous Kafka producer before swap: {flush_err}")

            # Atomically swap producer reference
            self.producer = new_producer
            self._producer_conf = new_conf
            self._cached_password = new_password
            self._kafka_state = "ok"

            # Flush old producer once more after swap before dropping it
            if old_producer is not None:
                try:
                    old_producer.flush(timeout=5.0)
                except Exception as flush_err2:
                    logger.warning(f"Error flushing previous Kafka producer after swap: {flush_err2}")

            logger.info("Kafka SASL producer swapped successfully with rotated credentials")
            return "changed"
        except Exception as e:
            logger.error(f"Failed to hot-swap Kafka producer: {type(e).__name__}")
            return "error: new credentials rejected by broker"

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

    def _emit_to_wire(self, key_bytes: bytes, payload_bytes: bytes) -> None:
        """Emit payload to real Kafka producer or fallback sink."""
        if self.producer is not None:
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=key_bytes,
                    value=payload_bytes,
                    on_delivery=self._on_delivery,
                )
                self.produced += 1
                self.producer.poll(0)
            except BufferError:
                self._record_error()
                self.producer.poll(0)
                try:
                    self.producer.produce(
                        topic=self.topic,
                        key=key_bytes,
                        value=payload_bytes,
                        on_delivery=self._on_delivery,
                    )
                    self.produced += 1
                    self.producer.poll(0)
                except Exception:
                    self._sink_payload(payload_bytes)
            except Exception:
                self._record_error()
                self._sink_payload(payload_bytes)
        else:
            self._sink_payload(payload_bytes)

        self.sent_count += 1
        now = time.monotonic()
        if not self._rate_history or now - self._rate_history[-1][0] >= 0.2:
            self._rate_history.append((now, self.produced + self.sunk))

    def get_delayed_frames_pending(self) -> int:
        """Return total number of delayed frames currently held in backlog queues."""
        return sum(len(q) for q in self._delay_buffers.values())

    def flush_delayed(
        self, device_id: Optional[str] = None
    ) -> List[Tuple[TwinSensorReading, bytes]]:
        """Flush held delayed frames in FIFO order to the wire. Bounded."""
        flushed: List[Tuple[TwinSensorReading, bytes]] = []
        dev_ids = [device_id] if device_id else list(self._delay_buffers.keys())
        for did in dev_ids:
            q = self._delay_buffers.get(did)
            if not q:
                continue
            while len(q) > 0:
                _, reading, raw_bytes = q.popleft()
                self._emit_to_wire(did.encode("utf-8"), raw_bytes)
                flushed.append((reading, raw_bytes))
        if self.producer is not None:
            try:
                self.producer.poll(0)
            except Exception:
                pass
        return flushed

    def stream_reading(
        self, raw_reading: Dict[str, Any], private_key_b64: str
    ) -> Optional[Tuple[TwinSensorReading, bytes]]:
        """Validate, sign, serialize, verify size limit (<300 bytes), and emit to Kafka."""
        dev_id = str(raw_reading.get("device_id", ""))
        link_mode = raw_reading.get("_link_mode")
        is_replay = bool(raw_reading.get("_replay") or link_mode == "replay")
        is_corrupt_bytes = bool(raw_reading.get("_corrupt_bytes") or link_mode == "corrupt_bytes")
        is_delay = bool(raw_reading.get("_delay") or link_mode == "delay")

        # 2.d: Replay mode: re-emit the exact bytes previously sent for that device
        if is_replay:
            offset = int(raw_reading.get("_replay_offset", 10))
            history = self._frame_history[dev_id]
            if len(history) > 0:
                idx = max(0, len(history) - 1 - offset)
                replayed_bytes = history[idx]
                replayed_reading = TwinSensorReading.model_validate_json(replayed_bytes)
                self._emit_to_wire(dev_id.encode("utf-8"), replayed_bytes)
                return replayed_reading, replayed_bytes

        # Standard sign and build
        reading = self.sign_and_build_reading(raw_reading, private_key_b64)
        serialized_bytes = serialize_reading(reading)

        # 2.c: Corrupt bytes mode: tamper one digit of serialized value in-transit
        if is_corrupt_bytes:
            serialized_bytes = tamper_value_bytes(serialized_bytes)

        # Enforce the strict 300-byte constraint
        payload_size = len(serialized_bytes)
        if payload_size > self.MAX_PAYLOAD_BYTES:
            self.rejected_oversize += 1
            if dev_id not in self._oversize_alerted_devices:
                self._oversize_alerted_devices.add(dev_id)
                logger.warning(
                    f"[ALERT] Payload size {payload_size} bytes for device {dev_id} "
                    f"exceeds strict {self.MAX_PAYLOAD_BYTES}-byte limit"
                )
            return None

        # 2.e: Delay mode: hold frames for delay_ticks and release later
        if is_delay:
            delay_ticks = min(600, max(1, int(raw_reading.get("_delay_ticks", 50))))
            queue = self._delay_buffers[dev_id]

            # Decrement ticks for all existing items and pick the oldest due frame if ready
            due_frame = None
            if len(queue) > 0:
                new_q: collections.deque[Tuple[int, TwinSensorReading, bytes]] = collections.deque(maxlen=600)
                for ticks_left, r_model, r_bytes in queue:
                    t = ticks_left - 1
                    if t <= 0 and due_frame is None:
                        due_frame = (r_model, r_bytes)
                    else:
                        new_q.append((max(0, t), r_model, r_bytes))
                self._delay_buffers[dev_id] = new_q

            # Hold current frame
            self._delay_buffers[dev_id].append((delay_ticks, reading, serialized_bytes))
            self._frame_history[dev_id].append(serialized_bytes)

            if due_frame is not None:
                due_reading, due_bytes = due_frame
                self._emit_to_wire(dev_id.encode("utf-8"), due_bytes)
                return due_reading, due_bytes
            return None

        # Store in history for future replay
        self._frame_history[dev_id].append(serialized_bytes)

        # Emit the fresh non-delayed frame first
        self._emit_to_wire(reading.device_id.encode("utf-8"), serialized_bytes)

        # If device has a backlog of delayed frames, release all held frames in order
        self.last_emitted_backlog = []
        if dev_id in self._delay_buffers and len(self._delay_buffers[dev_id]) > 0:
            while len(self._delay_buffers[dev_id]) > 0:
                _, backlog_reading, backlog_bytes = self._delay_buffers[dev_id].popleft()
                self._emit_to_wire(dev_id.encode("utf-8"), backlog_bytes)
                self.last_emitted_backlog.append((backlog_reading, backlog_bytes))

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
