"""Reading sources for external mock clients (Layer 2 Brain and Layer 3 Guardian).

Assigned to: Agent_QA
Provides ReadingSource abstract interface, KafkaSource, and QueueSource.
Strict isolation: zero imports from Layer 1 internal packages.
"""

from __future__ import annotations

import json
import logging
import queue
from typing import Any, Dict, Optional, Union

logger = logging.getLogger("mocks.reading_source")


class ReadingSource:
    """Abstract interface for consuming twin sensor reading envelopes."""

    def get_reading(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def close(self) -> None:
        pass


class QueueSource(ReadingSource):
    """In-memory thread-safe queue source for testing and standalone mocks."""

    def __init__(self, maxsize: int = 10000):
        self.q: queue.Queue[Union[bytes, Dict[str, Any]]] = queue.Queue(maxsize=maxsize)

    def put(self, item: Union[bytes, Dict[str, Any]]) -> None:
        self.q.put(item)

    def get_reading(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        try:
            item = self.q.get(timeout=timeout)
        except queue.Empty:
            return None
        if isinstance(item, bytes):
            try:
                return json.loads(item.decode("utf-8"))
            except Exception:
                return None
        return item


class KafkaSource(ReadingSource):
    """Real confluent-kafka consumer connecting to twin.sensors."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "mock-client-group",
        topic: str = "twin.sensors",
    ):
        try:
            import confluent_kafka
            self.consumer = confluent_kafka.Consumer({
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            })
            self.consumer.subscribe([topic])
        except Exception as e:
            logger.warning(f"Could not initialize Kafka consumer: {e}")
            self.consumer = None

    def get_reading(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        if self.consumer is None:
            return None
        msg = self.consumer.poll(timeout)
        if msg is None or msg.error():
            return None
        val_bytes = msg.value()
        if not val_bytes:
            return None
        try:
            return json.loads(val_bytes.decode("utf-8"))
        except Exception:
            return None

    def close(self) -> None:
        if self.consumer is not None:
            try:
                self.consumer.close()
            except Exception:
                pass
