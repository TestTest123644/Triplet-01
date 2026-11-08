import json
import logging
import threading
from typing import Any

from guardian_layer.utils import (
    configure,
    get_logger,
    is_enabled,
)
from guardian_layer.utils.config import ElasticsearchSettings
from guardian_layer.utils.elastic import AsyncElasticSearchHandler

MOCK_PASSWORD = "x"


def test_logger_module_exports_and_can_be_configured(monkeypatch: Any) -> None:
    monkeypatch.delenv("GUARDIAN_ELASTICSEARCH_URL", raising=False)
    monkeypatch.delenv("ELASTICSEARCH_PASSWORD", raising=False)
    configure("debug")
    logger = get_logger("tests.logger")

    assert logger is not None
    assert is_enabled() is True

    configure("off")
    assert is_enabled() is False


def test_application_logs_are_sent_to_elasticsearch_async(
    monkeypatch: Any,
) -> None:
    sent = threading.Event()
    captured: dict[str, Any] = {}

    class Response:
        status_code = 200

        def json(self) -> dict[str, bool]:
            return {"errors": False}

        def raise_for_status(self) -> None:
            return None

    def fake_post(url: str, **kwargs: Any) -> Response:
        captured["url"] = url
        captured["kwargs"] = kwargs
        captured["body"] = kwargs["content"]
        sent.set()
        return Response()

    settings = ElasticsearchSettings(
        url="http://localhost:9200",
        batch_window_seconds=0,
        retry_backoff_seconds=0,
    )
    monkeypatch.setenv("ELASTICSEARCH_PASSWORD", MOCK_PASSWORD)
    monkeypatch.setattr("guardian_layer.utils.elastic.httpx.post", fake_post)
    handler = AsyncElasticSearchHandler(settings)
    assert handler._worker is None
    record = logging.LogRecord(
        "guardian_layer.test_service",
        logging.INFO,
        __file__,
        1,
        "reading batch persisted",
        (),
        None,
    )
    record.table = "raw_readings"
    record.row_count = 3
    handler.emit(record)

    try:
        assert sent.wait(timeout=2)
        handler.flush()
        action, document = captured["body"].splitlines()
        assert json.loads(action)["index"]["_index"] == "guardian-logs"
        assert json.loads(action)["index"]["_id"]
        payload = json.loads(document)
        assert payload["message"] == "reading batch persisted"
        assert json.loads(payload["context"]) == {
            "row_count": 3,
            "table": "raw_readings",
        }
        assert payload["@timestamp"]
        assert captured["url"] == "http://localhost:9200/_bulk"
        assert captured["kwargs"]["auth"] == ("elastic", MOCK_PASSWORD)
        assert captured["kwargs"]["timeout"] == settings.request_timeout_seconds
    finally:
        handler.close()


def test_elasticsearch_handler_retries_transient_responses(monkeypatch: Any) -> None:
    attempts: list[str] = []

    class Response:
        def __init__(self, status_code: int) -> None:
            self.status_code = status_code

        def json(self) -> dict[str, bool]:
            return {"errors": False}

        def raise_for_status(self) -> None:
            return None

    def fake_post(url: str, **kwargs: Any) -> Response:
        attempts.append(url)
        return Response(503 if len(attempts) == 1 else 200)

    monkeypatch.setenv("ELASTICSEARCH_PASSWORD", MOCK_PASSWORD)
    monkeypatch.setattr("guardian_layer.utils.elastic.httpx.post", fake_post)
    handler = AsyncElasticSearchHandler(
        ElasticsearchSettings(
            url="http://localhost:9200",
            retry_backoff_seconds=0,
            batch_window_seconds=0,
        )
    )
    record = logging.LogRecord(
        "guardian_layer.retry", logging.ERROR, __file__, 1, "x", (), None
    )
    handler.emit(record)

    try:
        handler.flush()
        assert len(attempts) == 2
        assert handler.dropped_records == 0
    finally:
        handler.close()


def test_elasticsearch_handler_requires_authentication(monkeypatch: Any) -> None:
    monkeypatch.delenv("ELASTICSEARCH_PASSWORD", raising=False)
    try:
        AsyncElasticSearchHandler(ElasticsearchSettings(url="http://localhost:9200"))
    except ValueError as exc:
        assert "ELASTICSEARCH_PASSWORD" in str(exc)
    else:
        raise AssertionError("Expected missing Elasticsearch password to fail")


def test_close_flushes_queued_records_and_joins_worker(monkeypatch: Any) -> None:
    sent = threading.Event()

    class Response:
        status_code = 200

        def json(self) -> dict[str, bool]:
            return {"errors": False}

        def raise_for_status(self) -> None:
            return None

    def fake_post(url: str, **kwargs: Any) -> Response:
        sent.set()
        return Response()

    monkeypatch.setenv("ELASTICSEARCH_PASSWORD", MOCK_PASSWORD)
    monkeypatch.setattr("guardian_layer.utils.elastic.httpx.post", fake_post)
    handler = AsyncElasticSearchHandler(
        ElasticsearchSettings(url="http://localhost:9200", batch_window_seconds=0)
    )
    record = logging.LogRecord(
        "guardian_layer.shutdown", logging.INFO, __file__, 1, "final", (), None
    )
    handler.emit(record)
    worker = handler._worker

    handler.close()

    assert sent.is_set()
    assert worker is not None and not worker.is_alive()
    assert handler._queue.unfinished_tasks == 0
