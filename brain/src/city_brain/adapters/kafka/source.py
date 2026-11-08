"""The triplet's input: `twin.sensors`, `guardian.verdicts` and `guardian.events` (spec 5.1, 5.3).

Offsets are committed only after the Brain has taken responsibility for a message — it has
been journalled, or at least stepped — so a crash replays the tail rather than losing it.
When the Brain falls behind, the source pauses its partitions instead of growing a queue
without bound, and resumes once the backlog has drained.

The consumer is injected so the adapter can be exercised without a broker; `connect()`
builds the real one.
"""

import logging
from collections.abc import Mapping, Sequence
from typing import Any

logger = logging.getLogger("city_brain.kafka_source")

PAUSE_AT = 10_000
RESUME_AT = 5_000


class KafkaSource:
    """Reads the partner topics and hands out `(stream, raw bytes)` pairs."""

    def __init__(
        self,
        bootstrap: str,
        group: str,
        topics: Mapping[str, str] | Sequence[str],
        offset_reset: str = "latest",
        consumer: Any | None = None,
        pause_at: int = PAUSE_AT,
        resume_at: int = RESUME_AT,
        security_config: Mapping[str, Any] | None = None,
    ):
        self._bootstrap = bootstrap
        self._group = group
        # stream name -> topic. A bare list of topics names streams after the topics.
        self._topics = dict(topics) if isinstance(topics, Mapping) else {t: t for t in topics}
        self._by_topic = {topic: stream for stream, topic in self._topics.items()}
        self._offset_reset = offset_reset
        self._consumer = consumer
        self._pause_at, self._resume_at = int(pause_at), int(resume_at)
        self._security_config = dict(security_config) if security_config else {}
        self._paused = False
        self._uncommitted = 0

    # ----- the connection -----

    @property
    def config(self) -> dict[str, Any]:
        cfg = {
            "bootstrap.servers": self._bootstrap,
            "group.id": self._group,
            "auto.offset.reset": self._offset_reset,
            "enable.auto.commit": False,  # the Brain commits what it has taken on
            "enable.partition.eof": False,
        }
        if self._security_config:
            cfg.update(self._security_config)
        return cfg

    @property
    def topics(self) -> list[str]:
        return sorted(self._by_topic)

    @property
    def paused(self) -> bool:
        return self._paused

    def connect(self) -> None:
        if self._consumer is None:
            from confluent_kafka import Consumer  # imported here so the import is optional

            self._consumer = Consumer(self.config)
        self._consumer.subscribe(self.topics)

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()

    # ----- reading -----

    def poll(self, timeout_s: float = 0.1, limit: int = 500) -> list[tuple[str, bytes]]:
        """Up to `limit` messages, as `(stream, raw)`. Unknown topics are dropped, not guessed."""
        if self._consumer is None:
            raise RuntimeError("connect() first")
        out: list[tuple[str, bytes]] = []
        for message in self._consume(timeout_s, limit):
            if message is None:
                break
            err = message.error()
            if err:
                code_fn = getattr(err, "code", None)
                code_str = str(code_fn()) if code_fn else ""
                if code_str not in ("_PARTITION_EOF", "-191"):
                    logger.warning("KafkaSource poll error on topic %s: %s", message.topic(), err)
                continue  # a broker-side error is not a reading; the next poll tries again
            stream = self._by_topic.get(message.topic())
            if stream is None:
                continue
            out.append((stream, message.value()))
        self._uncommitted += len(out)
        return out

    def _consume(self, timeout_s: float, limit: int):
        batch = getattr(self._consumer, "consume", None)
        if batch is not None:
            return batch(num_messages=limit, timeout=timeout_s) or []
        return [self._consumer.poll(timeout=timeout_s)]

    def commit(self) -> None:
        """Only after the Brain has the messages: this is the at-least-once boundary."""
        if self._consumer is None or self._uncommitted == 0:
            return
        self._consumer.commit(asynchronous=False)
        self._uncommitted = 0

    @property
    def uncommitted(self) -> int:
        return self._uncommitted

    # ----- backpressure -----

    def apply_backpressure(self, depth: int) -> bool:
        """Pause or resume the partitions for the Brain's queue depth. Returns `paused`."""
        if self._consumer is None:
            return self._paused
        assignment = self._consumer.assignment() or []
        if not self._paused and depth >= self._pause_at:
            self._consumer.pause(assignment)
            self._paused = True
        elif self._paused and depth < self._resume_at:
            self._consumer.resume(assignment)
            self._paused = False
        return self._paused
