import json
import logging
import signal
import threading
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from guardian_layer.consumer_producer.action_taker.consumer import ActionTaker
from guardian_layer.consumer_producer.action_taker.exceptions import McpUnavailable
from guardian_layer.consumer_producer.action_taker.schema import (
    Action,
    ActionOutcome,
    AnomalyRecord,
    DlqRecord,
    OutcomeStatus,
    ToolName,
)

LOGGER = "guardian_layer.consumer_producer.action_taker.consumer"
DLQ = "telemetry-dlq"
ANOMALY = "telemetry-anomaly"
GROUP = "action-taker-processor"
DONE = ActionOutcome(OutcomeStatus.DONE, "ok")


def dlq_value(code: str = "E200_BAD_SIGNATURE") -> bytes:
    return json.dumps(
        {
            "error_code": code,
            "detail": "",
            "raw": {"device_id": "z1-tc1", "reading_id": "r-1"},
            "failed_at": "2026-11-04T10:00:00+00:00",
        }
    ).encode()


class FakeMessage:
    def __init__(
        self, value: bytes | None, offset: int = 0, topic: str = DLQ, error: Any = None
    ):
        self._value, self._offset, self._topic, self._error = (
            value,
            offset,
            topic,
            error,
        )

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> Any:
        return self._error


class FakeConsumer:
    """Serves its messages, then sets the stop event so run() returns."""

    def __init__(self, stop_event: threading.Event, *messages: FakeMessage):
        self.stop_event = stop_event
        self.queue = list(messages)
        self.served: list[FakeMessage] = []
        self.commits: list[FakeMessage] = []
        self.seeks: list[tuple[str, int, int]] = []
        self.paused: list[list[TopicPartition]] = []
        self.resumed: list[list[TopicPartition]] = []
        self.subscribed: list[str] = []
        self.closed = False
        self.commit_errors: list[Exception] = []
        self.partitions = [TopicPartition(DLQ, 0), TopicPartition(ANOMALY, 0)]

    def subscribe(self, topics: list[str], on_assign: Any = None) -> None:
        self.subscribed = topics

    def poll(self, timeout: float) -> FakeMessage | None:
        if not self.queue:
            self.stop_event.set()
            return None
        message = self.queue.pop(0)
        self.served.append(message)
        return message

    def commit(self, message: FakeMessage, asynchronous: bool) -> None:
        assert asynchronous is False
        if self.commit_errors:
            raise self.commit_errors.pop(0)
        self.commits.append(message)

    def seek(self, partition: TopicPartition) -> None:
        self.seeks.append((partition.topic, partition.partition, partition.offset))
        # like Kafka: the message at that offset is delivered again
        rewound = [m for m in self.served if m.offset() == partition.offset]
        self.queue[:0] = rewound[-1:]

    def assignment(self) -> list[TopicPartition]:
        return self.partitions

    def pause(self, partitions: list[TopicPartition]) -> None:
        self.paused.append(partitions)

    def resume(self, partitions: list[TopicPartition]) -> None:
        self.resumed.append(partitions)

    def close(self) -> None:
        self.closed = True


class FakeRules:
    def __init__(self, *actions: Action):
        self.actions = list(actions)
        self.records: list[DlqRecord | AnomalyRecord] = []

    def decide(self, record: DlqRecord | AnomalyRecord) -> list[Action]:
        self.records.append(record)
        return self.actions


class FakeExecutor:
    """Returns DONE; raises the scripted exceptions first, in order."""

    def __init__(self, *failures: Exception, on_fail: Any = None):
        self.failures = list(failures)
        self.on_fail = on_fail
        self.executed: list[Action] = []

    def execute(self, action: Action) -> ActionOutcome:
        self.executed.append(action)
        if self.failures:
            if self.on_fail:
                self.on_fail()
            raise self.failures.pop(0)
        return DONE


ISOLATE = Action(ToolName.ISOLATE_DEVICE, "z1-tc1")
ROLLBACK = Action(ToolName.ROLLBACK_READING, "r-1")


def make_taker(
    *messages: FakeMessage,
    rules: FakeRules | None = None,
    executor: FakeExecutor | None = None,
    stop_event: threading.Event | None = None,
) -> tuple[ActionTaker, FakeConsumer]:
    stop_event = stop_event or threading.Event()
    consumer = FakeConsumer(stop_event, *messages)
    taker = ActionTaker(
        consumer,  # pyright: ignore[reportArgumentType]
        rules or FakeRules(ISOLATE, ROLLBACK),  # pyright: ignore[reportArgumentType]
        executor or FakeExecutor(),  # pyright: ignore[reportArgumentType]
        models={DLQ: DlqRecord, ANOMALY: AnomalyRecord},
        group_id=GROUP,
        pause_seconds=0.01,
        stop_event=stop_event,
    )
    return taker, consumer


@pytest.fixture
def consumer_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    module_logger = logging.getLogger(LOGGER)
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    yield caplog
    module_logger.removeHandler(caplog.handler)


def test_subscribes_to_both_topics():
    taker, consumer = make_taker()
    taker.run()
    assert sorted(consumer.subscribed) == [ANOMALY, DLQ]


def test_valid_message_acts_then_commits(consumer_log):
    message = FakeMessage(dlq_value())
    executor = FakeExecutor()
    taker, consumer = make_taker(message, executor=executor)
    taker.run()
    assert executor.executed == [ISOLATE, ROLLBACK]
    assert consumer.commits == [message]
    assert "tool=isolate_device target=z1-tc1" in consumer_log.text
    assert "offset=0" in consumer_log.text


def test_message_without_actions_is_committed():
    message = FakeMessage(dlq_value("E500_ENRICHMENT"))
    executor = FakeExecutor()
    taker, consumer = make_taker(message, rules=FakeRules(), executor=executor)
    taker.run()
    assert executor.executed == []
    assert consumer.commits == [message]


def test_invalid_json_is_skipped_and_committed(consumer_log):
    message = FakeMessage(b"{not json", offset=7)
    executor = FakeExecutor()
    taker, consumer = make_taker(message, executor=executor)
    taker.run()
    assert consumer.commits == [message]
    assert executor.executed == []
    assert f"topic={DLQ} partition=0 offset=7" in consumer_log.text


def test_tombstone_and_non_utf8_are_skipped():
    tombstone, binary = FakeMessage(None, 1), FakeMessage(b"\xff\xfe", 2)
    good = FakeMessage(dlq_value(), 3)
    taker, consumer = make_taker(tombstone, binary, good)
    taker.run()
    assert consumer.commits == [tombstone, binary, good]


def test_message_with_error_is_not_committed():
    message = FakeMessage(None, error="broker error")
    executor = FakeExecutor()
    taker, consumer = make_taker(message, executor=executor)
    taker.run()
    assert consumer.commits == []
    assert executor.executed == []


def test_mcp_outage_rewinds_pauses_and_does_not_commit():
    message = FakeMessage(dlq_value(), offset=5)
    stop_event = threading.Event()
    # MCP stays down for the only attempt; then the loop is stopped
    executor = FakeExecutor(McpUnavailable("down"), McpUnavailable("down"))
    taker, consumer = make_taker(message, executor=executor, stop_event=stop_event)
    taker.handle(message)  # pyright: ignore[reportArgumentType]
    assert consumer.seeks == [(DLQ, 0, 5)]
    assert consumer.paused == [consumer.partitions]
    assert consumer.resumed == [consumer.partitions]
    assert consumer.commits == []


def test_message_after_outage_is_processed_and_committed():
    message = FakeMessage(dlq_value(), offset=5)
    executor = FakeExecutor(McpUnavailable("down"))
    taker, consumer = make_taker(message, executor=executor)
    taker.run()
    assert consumer.commits == [message]
    assert executor.executed == [ISOLATE, ISOLATE, ROLLBACK]


def test_rewound_message_is_decided_once():
    # a rewind after an MCP outage must not add repeat hits: one spoofed record
    # would otherwise reach the isolate threshold on its own
    message = FakeMessage(dlq_value(), offset=5)
    rules = FakeRules(ISOLATE)
    executor = FakeExecutor(McpUnavailable("down"), McpUnavailable("down"))
    taker, consumer = make_taker(message, rules=rules, executor=executor)
    taker.run()
    assert len(rules.records) == 1
    assert executor.executed == [ISOLATE, ISOLATE, ISOLATE]
    assert consumer.commits == [message]


def test_failed_commit_does_not_stop_the_consumer(consumer_log):
    first, second = FakeMessage(dlq_value(), 1), FakeMessage(dlq_value(), 2)
    taker, consumer = make_taker(first, second)
    consumer.commit_errors = [
        KafkaException(KafkaError(KafkaError.REBALANCE_IN_PROGRESS))
    ]
    taker.run()
    assert consumer.commits == [second]
    assert "commit failed" in consumer_log.text


def test_redelivered_message_runs_again_without_error():
    first, again = FakeMessage(dlq_value(), 4), FakeMessage(dlq_value(), 4)
    taker, consumer = make_taker(first, again)
    taker.run()
    assert consumer.commits == [first, again]


def test_stop_during_outage_skips_pause_and_commit():
    message = FakeMessage(dlq_value())
    stop_event = threading.Event()
    executor = FakeExecutor(McpUnavailable("down"), on_fail=stop_event.set)
    taker, consumer = make_taker(message, executor=executor, stop_event=stop_event)
    taker.run()
    assert consumer.seeks == []
    assert consumer.paused == []
    assert consumer.commits == []
    assert consumer.closed


def test_stop_handler_ends_the_loop_and_closes():
    message = FakeMessage(dlq_value())
    taker, consumer = make_taker(message)
    taker.stop(signal.SIGTERM, None)
    taker.run()
    assert consumer.served == []
    assert consumer.closed


def test_assignment_is_logged(consumer_log):
    taker, consumer = make_taker()
    taker.log_assignment(
        consumer,  # pyright: ignore[reportArgumentType]
        [TopicPartition(DLQ, 0), TopicPartition(ANOMALY, 2)],
    )
    assert GROUP in consumer_log.text
    assert f"{DLQ}[0]" in consumer_log.text
    assert f"{ANOMALY}[2]" in consumer_log.text
