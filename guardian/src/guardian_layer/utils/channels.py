from __future__ import annotations

import queue
from typing import Protocol

from guardian_layer.utils.schema import FLUSH_QUEUE_MAXSIZE, FlushedBatch


class FlushSink(Protocol):  # what writers can do
    def publish(self, batch: FlushedBatch) -> None:
        """Hand a batch over; raises queue.Full when it cannot be taken."""
        ...


class FlushSource(Protocol):  # what readers can do
    def get(self, timeout: float | None = None) -> FlushedBatch:
        """Next batch, oldest first; raises queue.Empty after ``timeout``."""
        ...

    def task_done(self) -> None:
        """Mark the last batch from ``get`` as processed."""
        ...


class FlushChannel:
    """Bounded hand-off between buffering and workers. Knows neither side."""

    def __init__(
        self, maxsize: int = FLUSH_QUEUE_MAXSIZE, publish_timeout: float = 0.0
    ) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be > 0: an unbounded channel hides backlog")
        self._queue: queue.Queue[FlushedBatch] = queue.Queue(maxsize=maxsize)
        # 0: a writer never waits on a full channel, it restores and retries later
        self._publish_timeout = publish_timeout

    def publish(self, batch: FlushedBatch) -> None:
        if self._publish_timeout > 0:
            self._queue.put(batch, timeout=self._publish_timeout)  # raises queue.Full
        else:
            self._queue.put_nowait(batch)  # raises queue.Full

    def get(self, timeout: float | None = None) -> FlushedBatch:
        return self._queue.get(timeout=timeout)  # raises queue.Empty

    def task_done(self) -> None:
        self._queue.task_done()

    def join(self) -> None:
        """Block until every published batch has been marked done."""
        self._queue.join()

    def qsize(self) -> int:
        return self._queue.qsize()  # approximate, for metrics
