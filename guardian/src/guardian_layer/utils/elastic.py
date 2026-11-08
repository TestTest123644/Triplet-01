"""Asynchronous, authenticated Elasticsearch log handler."""

import json
import logging
import os
import queue
import sys
import threading
import time
import traceback
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx

from guardian_layer.utils.config import ElasticsearchSettings, get_settings


class AsyncElasticSearchHandler(logging.Handler):
    """Serialize and queue log events; send bulk requests from a worker thread."""

    def __init__(
        self,
        settings: ElasticsearchSettings | None = None,
        endpoint: str | None = None,
    ) -> None:
        super().__init__()
        self.settings = settings or get_settings().elasticsearch
        self.endpoint = (
            endpoint or os.getenv("GUARDIAN_ELASTICSEARCH_URL", self.settings.url)
        ).rstrip("/")
        self.username = os.getenv("ELASTICSEARCH_USERNAME", self.settings.username)
        self.password = os.getenv("ELASTICSEARCH_PASSWORD", "")
        if self.endpoint and not self.password:
            raise ValueError(
                "ELASTICSEARCH_PASSWORD is required when Elasticsearch "
                "logging is enabled"
            )

        self._queue: queue.Queue[tuple[str, str]] = queue.Queue(
            maxsize=self.settings.queue_size
        )
        self._state_lock = threading.RLock()
        self._error_lock = threading.Lock()
        self._drop_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None
        self._pid = os.getpid()
        self._closed = False
        self._dropped_records = 0
        self._last_error_report = 0.0

    @property
    def dropped_records(self) -> int:
        with self._drop_lock:
            return self._dropped_records

    def _count_dropped(self, count: int = 1) -> None:
        with self._drop_lock:
            self._dropped_records += count

    def _after_fork_child(self) -> None:
        self._state_lock = threading.RLock()
        self._error_lock = threading.Lock()
        self._drop_lock = threading.Lock()
        self._queue = queue.Queue(maxsize=self.settings.queue_size)
        self._stop_event = threading.Event()
        self._worker = None
        self._pid = os.getpid()
        self._closed = False

    def _ensure_worker(self) -> bool:
        with self._state_lock:
            if self._pid != os.getpid():
                self._after_fork_child()
            if self._closed:
                return False
            if self._worker is None or not self._worker.is_alive():
                self._stop_event.clear()
                self._worker = threading.Thread(
                    target=self._drain_queue,
                    name="guardian-elasticsearch-logger",
                    daemon=True,
                )
                self._worker.start()
            return True

    @staticmethod
    def _serialize_record(record: logging.LogRecord) -> tuple[str, str]:
        reserved_keys = logging.makeLogRecord({}).__dict__
        context = {
            key: value
            for key, value in record.__dict__.items()
            if key not in reserved_keys and not key.startswith("_")
        }
        document: dict[str, Any] = {
            "@timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if context:
            # Keep a stable mapping type regardless of caller-provided value types.
            document["context"] = json.dumps(context, default=str, sort_keys=True)
        if record.exc_info:
            document["exception"] = "".join(
                traceback.format_exception(*record.exc_info)
            )
        event_id = uuid4().hex
        serialized = json.dumps(
            document, default=str, sort_keys=True, separators=(",", ":")
        )
        return event_id, serialized

    def emit(self, record: logging.LogRecord) -> None:
        try:
            event = self._serialize_record(record)
            if not self._ensure_worker():
                self._count_dropped()
                return
            self._queue.put_nowait(event)
        except queue.Full:
            self._count_dropped()
        except Exception as exc:
            self._count_dropped()
            self._report_error(f"Could not queue a guardian log record: {exc}")

    def _drain_queue(self) -> None:
        while True:
            if self._stop_event.is_set() and self._queue.empty():
                return
            try:
                first = self._queue.get(timeout=0.25)
            except queue.Empty:
                continue

            batch = [first]
            deadline = time.monotonic() + self.settings.batch_window_seconds
            while len(batch) < self.settings.batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    batch.append(self._queue.get(timeout=remaining))
                except queue.Empty:
                    break

            try:
                self._flush(batch)
            except Exception as exc:
                self._count_dropped(len(batch))
                self._report_error(f"Unexpected Elasticsearch logging failure: {exc}")
            finally:
                for _ in batch:
                    self._queue.task_done()

    def _flush(self, batch: list[tuple[str, str]]) -> None:
        pending = batch
        for attempt in range(self.settings.retry_attempts + 1):
            try:
                response = httpx.post(
                    f"{self.endpoint}/_bulk",
                    content=self._encode_bulk(pending),
                    headers={"Content-Type": "application/x-ndjson"},
                    auth=(self.username, self.password),
                    timeout=self.settings.request_timeout_seconds,
                )
                if response.status_code == 429 or response.status_code >= 500:
                    reason = f"Elasticsearch returned HTTP {response.status_code}"
                    retry_batch = pending
                else:
                    response.raise_for_status()
                    result = response.json()
                    failures = self._bulk_failures(result, pending)
                    retry_batch = [
                        event
                        for event, status, _ in failures
                        if status == 429 or status >= 500
                    ]
                    permanent_failures = [
                        failure
                        for failure in failures
                        if failure[1] != 429 and failure[1] < 500
                    ]
                    if permanent_failures:
                        self._count_dropped(len(permanent_failures))
                        self._report_error(
                            "Elasticsearch rejected log records: "
                            + "; ".join(reason for _, _, reason in permanent_failures)
                        )
                    if not retry_batch:
                        return
                    reason = "; ".join(
                        failure_reason
                        for _, _, failure_reason in failures
                        if failure_reason
                    )
            except httpx.TransportError as exc:
                retry_batch = pending
                reason = f"Elasticsearch transport error: {exc}"
            except httpx.HTTPStatusError as exc:
                self._count_dropped(len(pending))
                self._report_error(f"Elasticsearch request failed: {exc}")
                return
            except (ValueError, TypeError, AttributeError) as exc:
                self._count_dropped(len(pending))
                self._report_error(f"Invalid Elasticsearch response: {exc}")
                return

            if attempt < self.settings.retry_attempts:
                time.sleep(self.settings.retry_backoff_seconds * (2**attempt))
                pending = retry_batch
            else:
                self._count_dropped(len(retry_batch))
                self._report_error(
                    f"Elasticsearch batch failed after {attempt + 1} attempts: {reason}"
                )

    @staticmethod
    def _encode_bulk(batch: list[tuple[str, str]]) -> str:
        lines: list[str] = []
        for event_id, document in batch:
            lines.append(
                json.dumps(
                    {"index": {"_index": "guardian-logs", "_id": event_id}},
                    separators=(",", ":"),
                )
            )
            lines.append(document)
        return "\n".join(lines) + "\n"

    @staticmethod
    def _bulk_failures(
        response: dict[str, Any],
        batch: list[tuple[str, str]],
    ) -> list[tuple[tuple[str, str], int, str]]:
        failures: list[tuple[tuple[str, str], int, str]] = []
        items = response.get("items", [])
        for event, item in zip(batch, items, strict=False):
            operation = next(iter(item.values()), {})
            status = int(operation.get("status", 0))
            if status < 300:
                continue
            error = operation.get("error", {})
            detail = error.get("reason") or error.get("type") or "unknown error"
            failures.append(
                (event, status, f"record {event[0]} HTTP {status}: {detail}")
            )
        if response.get("errors") and not failures:
            failures.append(
                (batch[0], 500, "bulk response reported errors without details")
            )
        return failures

    def _report_error(self, message: str) -> None:
        now = time.monotonic()
        with self._error_lock:
            if now - self._last_error_report < self.settings.error_log_interval_seconds:
                return
            self._last_error_report = now
        sys.stderr.write(f"{message}\n")

    def flush(self) -> None:
        if self._queue.empty() or not self._ensure_worker():
            return
        deadline = time.monotonic() + self.settings.shutdown_timeout_seconds
        with self._queue.all_tasks_done:
            while self._queue.unfinished_tasks:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._report_error(
                        "Timed out waiting for queued guardian logs to flush"
                    )
                    return
                self._queue.all_tasks_done.wait(remaining)

    def close(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            worker = self._worker
        self.flush()
        with self._state_lock:
            self._closed = True
            self._stop_event.set()
            worker = self._worker
        if worker is not None and worker is not threading.current_thread():
            worker.join(timeout=self.settings.shutdown_timeout_seconds)
            if worker.is_alive():
                remaining = self._queue.qsize()
                if remaining:
                    self._count_dropped(remaining)
                self._report_error("Elasticsearch logging worker did not stop in time")
        super().close()


def _reset_active_handlers_after_fork() -> None:
    logger = logging.getLogger("guardian_layer")
    for handler in logger.handlers:
        if isinstance(handler, AsyncElasticSearchHandler):
            handler._after_fork_child()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_active_handlers_after_fork)


__all__ = ["AsyncElasticSearchHandler"]
