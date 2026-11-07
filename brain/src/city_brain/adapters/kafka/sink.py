"""The triplet's output: every `brain.events` record, keyed by what it is about (spec 5.4).

The sink writes the same bytes the run log writes, so a reader can check one against the
other. Delivery is fire-and-forget with a flush on close: the run log, not Kafka, is the
record of what the Brain did, so a broker that is down must never stall the control loop.
"""

import json
from typing import Any

DEFAULT_TOPIC = "brain.events"


def canonical(event: dict) -> bytes:
    return json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


class KafkaSink:
    """Publishes brain events; the producer is injected so it can be exercised without a broker."""

    def __init__(
        self,
        bootstrap: str,
        topic: str = DEFAULT_TOPIC,
        producer: Any | None = None,
        queue_size: int = 100_000,
    ):
        self._bootstrap = bootstrap
        self._topic = topic
        self._producer = producer
        self._queue_size = queue_size
        self.dropped = 0

    @property
    def config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self._bootstrap,
            "queue.buffering.max.messages": self._queue_size,
            "enable.idempotence": True,  # a retried publish must not duplicate a record
            "linger.ms": 5,
        }

    @property
    def topic(self) -> str:
        return self._topic

    def connect(self) -> None:
        if self._producer is None:
            from confluent_kafka import Producer  # imported here so the import is optional

            self._producer = Producer(self.config)

    def publish(self, event: dict) -> bool:
        """Queue one record. A full or missing queue is counted, never raised at the core."""
        if self._producer is None:
            self.dropped += 1
            return False
        try:
            self._producer.produce(
                self._topic, key=str(event.get("subject", "city")).encode("utf-8"),
                value=canonical(event))
        except BufferError:  # the broker is behind: the run log still has it
            self.dropped += 1
            return False
        poll = getattr(self._producer, "poll", None)
        if poll is not None:
            poll(0)
        return True

    def flush(self, timeout_s: float = 2.0) -> int:
        if self._producer is None:
            return 0
        return int(self._producer.flush(timeout_s) or 0)

    def close(self) -> None:
        self.flush()
