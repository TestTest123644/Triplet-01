from __future__ import annotations

import threading
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING
from uuid import uuid4

from guardian_layer.models.sensor import Sensor

if TYPE_CHECKING:
    from guardian_layer.managers.buffer_manger import BufferManager

FLUSH_QUEUE_MAXSIZE = 1000


class Status(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"


class Component(StrEnum):
    CONSUMER = "consumer"
    CLUSTER = "cluster"
    BUFFER = "buffer"


@dataclass(slots=True)
class Ack:  # renamed from Message to avoid clashing with the telemetry dict type
    id: str  # unique message id
    detail: str  # text note
    status: Status
    sender: Component
    receiver: Component

    @property
    def ok(self) -> bool:  # derived, cannot disagree with status
        return self.status is Status.SUCCESS


@dataclass(frozen=True, slots=True)
class FlushedBatch:  # plain data, no lock, safe to queue
    id: str
    sensor_id: str
    readings: tuple[Sensor, ...]
    flushed_at: datetime


@dataclass(slots=True)
class Buffer:  # what workers receive
    id: str
    sensor_id: str
    readings: list[Sensor]
    last_update: datetime
    last_flush: datetime = field(default_factory=lambda: datetime.now(UTC))
    high_watermark: int = 200
    flush_size: int = field(default=100)  # how many oldest readings a flush takes
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False, compare=False
    )  # guards readings, last_flush and last_update

    def __len__(self) -> int:
        return len(self.readings)

    def __post_init__(self) -> None:
        if self.flush_size > self.high_watermark:
            raise ValueError(
                f"flush_size {self.flush_size} cannot exceed "
                + f"high_watermark {self.high_watermark}"
            )
        if not self.sensor_id.strip():
            raise ValueError("sensor_id must not be empty or whitespace")
        if not self.id.strip():
            raise ValueError("id must not be empty or whitespace")

    @property
    def is_full(self) -> bool:
        """Check if readings have hit the high watermark."""
        return len(self) >= self.high_watermark

    def add_reading(self, reading: Sensor) -> FlushedBatch | None:
        """Append a reading, flushing the oldest ``flush_size`` first when full.

        Returns the flushed batch, or None when no flush ran.
        """
        flushed: list[Sensor] = []
        with self._lock:
            now = datetime.now(UTC)
            if self.is_full:
                flushed = self.readings[: self.flush_size]
                del self.readings[: self.flush_size]
                self.last_flush = now
            self.readings.append(reading)
            self.last_update = now

        if not flushed:
            return None

        try:
            import sys

            batch_cls = getattr(
                sys.modules.get("guardian_layer.utils.schema"),
                "FlushedBatch",
                FlushedBatch,
            )
            return batch_cls(
                id=str(uuid4()),
                sensor_id=self.sensor_id,
                readings=tuple(flushed),
                flushed_at=now,
            )
        except Exception as error:
            raise RuntimeError(
                f"Flush failed for sensor {self.sensor_id}: "
                f"{len(flushed)} readings dropped"
            ) from error

    def restore(self, readings: Sequence[Sensor]) -> None:
        """Put the readings of a failed flush back at the front, oldest first."""
        with self._lock:
            self.readings[:0] = readings


@dataclass(slots=True)
class Cluster:
    id: str
    sensor_type: str
    buffers: dict[str, BufferManager] = field(
        default_factory=dict
    )  # keyed by sensor_id
    last_update: datetime | None = None

    @property
    def length(self) -> int:
        return len(self.buffers)
