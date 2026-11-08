"""Entry point: wire the settings, Kafka consumer, MCP client and rules, then run."""

import signal
import threading
from datetime import timedelta

from confluent_kafka import Consumer

from guardian_layer.consumer_producer.action_taker.consumer import ActionTaker
from guardian_layer.consumer_producer.action_taker.mcp_client import ActionExecutor
from guardian_layer.consumer_producer.action_taker.rules import RepeatCounter, Rules
from guardian_layer.consumer_producer.action_taker.schema import (
    AnomalyRecord,
    DlqRecord,
)
from guardian_layer.consumer_producer.action_taker.utils.settings import get_settings
from guardian_layer.mcp_client.client import McpToolClient
from guardian_layer.utils.logger import get_logger

# not __name__: run with "python -m" it is "__main__", outside the guardian_layer
# logger tree, and every line logged here would be dropped
logger = get_logger("guardian_layer.consumer_producer.action_taker.main")


def main() -> None:
    settings = get_settings()
    stop_event = threading.Event()
    client = McpToolClient(url=settings.mcp.url, timeout_s=settings.mcp.timeout)
    rules = Rules(
        RepeatCounter(
            threshold=settings.rules.repeat_threshold,
            window=timedelta(seconds=settings.rules.repeat_window_seconds),
        )
    )
    executor = ActionExecutor(
        client,
        retry_attempts=settings.mcp.retry_attempts,
        backoff_seconds=settings.mcp.retry_backoff_seconds,
        dry_run=settings.dry_run,
        stop_event=stop_event,
    )
    taker = ActionTaker(
        Consumer(settings.kafka.client_config()),
        rules,
        executor,
        models={
            settings.kafka.dlq_topic: DlqRecord,
            settings.kafka.anomaly_topic: AnomalyRecord,
        },
        group_id=settings.kafka.group_id,
        pause_seconds=settings.kafka.pause_seconds,
        stop_event=stop_event,
    )
    signal.signal(signal.SIGTERM, taker.stop)
    signal.signal(signal.SIGINT, taker.stop)
    logger.info(
        "action_taker starting: mcp=%s dry_run=%s", settings.mcp.url, settings.dry_run
    )
    try:
        taker.run()
    finally:
        client.close()


if __name__ == "__main__":
    main()
