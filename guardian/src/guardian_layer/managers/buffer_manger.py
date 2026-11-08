import queue
import threading
import time
from datetime import UTC, datetime
from uuid import uuid4

from guardian_layer.managers.interface.manager_interface import ManagerInterface
from guardian_layer.utils.channels import FlushSink
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import (
    Ack,
    Buffer,
    Component,
    FlushedBatch,
    Sensor,
    Status,
)

logger = get_logger(__name__)


class BufferManager(ManagerInterface):
    """Manage sensor reading buffers."""

    def __init__(self, sink: FlushSink) -> None:
        self.buffers: dict[str, Buffer] = {}
        self._sink = sink
        self._create_lock = threading.Lock()

    def validate(self, value: str) -> Ack:
        """Check whether a sensor buffer exists."""

        if value not in self.buffers:
            return Ack(
                id=str(uuid4()),
                detail=f"Buffer {value} not found",
                status=Status.FAILED,
                sender=Component.BUFFER,
                receiver=Component.CLUSTER,
            )

        return Ack(
            id=str(uuid4()),
            detail=f"Buffer {value} exists",
            status=Status.SUCCESS,
            sender=Component.BUFFER,
            receiver=Component.CLUSTER,
        )

    def create(self, value: str) -> Ack:
        """Create an empty buffer for a sensor if it does not already exist."""
        try:
            with self._create_lock:
                if value in self.buffers:
                    return Ack(
                        id=str(uuid4()),
                        detail=f"Buffer {value} already exists",
                        status=Status.FAILED,
                        sender=Component.BUFFER,
                        receiver=Component.CLUSTER,
                    )
                self.buffers[value] = Buffer(
                    id=value,
                    sensor_id=value,
                    readings=[],
                    last_update=datetime.now(UTC),
                )
            return Ack(
                id=str(uuid4()),
                detail=f"Buffer {value} created",
                status=Status.SUCCESS,
                sender=Component.BUFFER,
                receiver=Component.CLUSTER,
            )
        except Exception as e:
            logger.exception("Failed to create buffer", buffer_id=value)
            return Ack(
                id=str(uuid4()),
                detail=f"Failed to create buffer {value}: {e}",
                status=Status.FAILED,
                sender=Component.BUFFER,
                receiver=Component.CLUSTER,
            )

    def _sink_to_queue(self, batch: FlushedBatch, retry_count: int = 3):
        """Publish a flushed batch to the sink, handling exceptions."""
        for i in range(retry_count):
            try:
                self._sink.publish(batch)
                return
            except Exception as e:
                logger.exception(
                    f"Failed to publish flushed batch retrying {i + 1}/{retry_count}",
                    batch_id=batch.id,
                    sensor_id=batch.sensor_id,
                )
                if i == retry_count - 1:
                    raise e

    def add(self, sensor: Sensor) -> Ack:
        """Add a sensor reading to its buffer, publishing any flushed batch."""

        buffer = self.buffers.get(sensor.sensor_id)
        if buffer is None:
            return Ack(
                id=str(uuid4()),
                detail=f"Buffer {sensor.sensor_id} not found",
                status=Status.FAILED,
                sender=Component.BUFFER,
                receiver=Component.CLUSTER,
            )
        started = time.perf_counter_ns()
        try:
            batch = buffer.add_reading(sensor)
        except Exception:
            logger.exception(
                "Buffer flush failed, flushed readings dropped",
                buffer_id=buffer.id,
                sensor_id=buffer.sensor_id,
            )
            return self._added(
                sensor, f"Sensor {sensor.sensor_id} failed to add reading"
            )

        if batch is not None:
            try:
                self._sink_to_queue(batch)
            except Exception as e:
                buffer.restore(batch.readings)
                if isinstance(e, queue.Full):
                    msg = "Flush channel full, readings restored"
                else:
                    msg = f"Flush channel failed, readings restored: {e}"
                logger.error(
                    msg,
                    buffer_id=buffer.id,
                    sensor_id=batch.sensor_id,
                    restored=len(batch.readings),
                )
                return self._added(
                    sensor, f"Sensor {sensor.sensor_id} failed to add restored readings"
                )
            logger.info(
                "Buffer flushed",
                buffer_id=buffer.id,
                batch_id=batch.id,
                sensor_id=batch.sensor_id,
                flushed=len(batch.readings),
                flushed_at=batch.flushed_at.isoformat(),
                duration_us=(time.perf_counter_ns() - started) / 1000,
            )

        return self._added(sensor, f"Sensor {sensor.sensor_id} added successfully")

    def _added(self, sensor: Sensor, detail: str) -> Ack:
        return Ack(
            id=str(uuid4()),
            detail=detail,
            status=Status.SUCCESS,
            sender=Component.BUFFER,
            receiver=Component.CLUSTER,
        )

    def delete(self, id: str) -> Ack:
        """Delete one sensor buffer."""

        validation = self.validate(id)
        if not validation.ok:
            return validation

        del self.buffers[id]
        return Ack(
            id=str(uuid4()),
            detail=f"Buffer {id} deleted successfully",
            status=Status.SUCCESS,
            sender=Component.BUFFER,
            receiver=Component.CLUSTER,
        )

    def get(self, value: str) -> Buffer | Ack:
        """Return a sensor buffer or an acknowledgment if it does not exist."""

        validation = self.validate(value)
        if not validation.ok:
            return validation
        return self.buffers[value]
