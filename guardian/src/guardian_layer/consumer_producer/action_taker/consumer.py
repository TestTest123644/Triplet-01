"""The Kafka loop: poll, parse, decide, act through MCP, then commit.

At-least-once: an offset is committed only after every action of its message got
an answer from MCP, or after the message was deliberately skipped (unparseable,
or no rule). While MCP is unreachable the message is rewound and consumption is
paused, so nothing is lost; repeated actions are harmless (see mcp_client).
"""

import threading
from types import FrameType

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from pydantic import ValidationError

from guardian_layer.consumer_producer.action_taker.exceptions import (
    InvalidMessage,
    McpUnavailable,
)
from guardian_layer.consumer_producer.action_taker.mcp_client import ActionExecutor
from guardian_layer.consumer_producer.action_taker.rules import Rules
from guardian_layer.consumer_producer.action_taker.schema import (
    Action,
    AnomalyRecord,
    DlqRecord,
)
from guardian_layer.utils.logger import get_logger

logger = get_logger(__name__)

POLL_TIMEOUT_SECONDS = 1.0  # how long one poll waits, and so how fast SIGTERM is seen


class ActionTaker:
    def __init__(
        self,
        consumer: Consumer,
        rules: Rules,
        executor: ActionExecutor,
        models: dict[str, type[DlqRecord] | type[AnomalyRecord]],
        group_id: str,
        pause_seconds: float,
        stop_event: threading.Event,
    ):
        self._consumer = consumer
        self._rules = rules
        self._executor = executor
        self._models = models  # topic -> the message model it carries
        self._group_id = group_id
        self._pause_seconds = pause_seconds
        self._stop_event = stop_event
        # (topic, partition, offset) of messages rewound during an MCP outage
        self._rewound: dict[
            tuple[str | None, int | None, int | None], list[Action]
        ] = {}

    def run(self) -> None:
        """Consume until stop() is called, then close the consumer."""
        self._consumer.subscribe(list(self._models), on_assign=self.log_assignment)
        try:
            while not self._stop_event.is_set():
                message = self._consumer.poll(POLL_TIMEOUT_SECONDS)
                if message is not None:
                    self.handle(message)
        finally:
            self._consumer.close()
            logger.info("consumer closed")

    def handle(self, message: Message) -> None:
        """Process one message; commits it unless MCP was unreachable."""
        if message.error() is not None:
            logger.warning("kafka error: %s", message.error())
            return
        try:
            record = self.parse(message)
        except InvalidMessage as error:
            logger.warning("skipping invalid message %s: %s", position(message), error)
            self._commit(message)
            return
        where = (message.topic(), message.partition(), message.offset())
        # a message rewound after an outage reuses its first decision: deciding again
        # would add repeat hits and could reach a threshold on a single message
        actions = self._rewound.pop(where, None)
        if actions is None:
            actions = self._rules.decide(record)
        try:
            for action in actions:
                outcome = self._executor.execute(action)
                logger.info(
                    "action tool=%s target=%s %s result=%s detail=%s",
                    action.tool,
                    action.target,
                    position(message),
                    outcome.status,
                    outcome.detail,
                )
        except McpUnavailable as error:
            self._rewound[where] = actions
            self._wait_for_mcp(message, error)
            return
        self._commit(message)

    def parse(self, message: Message) -> DlqRecord | AnomalyRecord:
        topic = message.topic()
        model = self._models.get(topic) if topic is not None else None
        value = message.value()
        if model is None or value is None:
            raise InvalidMessage(f"no model for topic {topic!r} or empty value")
        try:
            return model.model_validate_json(value)
        except ValidationError as error:
            raise InvalidMessage(str(error)) from error

    def stop(self, signum: int, frame: FrameType | None) -> None:
        """SIGTERM/SIGINT handler: finish the current message, then stop."""
        logger.info("signal %d received, stopping", signum)
        self._stop_event.set()

    def log_assignment(
        self, consumer: Consumer, partitions: list[TopicPartition]
    ) -> None:
        assigned = ", ".join(f"{tp.topic}[{tp.partition}]" for tp in partitions)
        logger.info("joined group %s, assigned %s", self._group_id, assigned)

    def _wait_for_mcp(self, message: Message, error: McpUnavailable) -> None:
        if self._stop_event.is_set():
            # shutting down: leave it uncommitted, it is redelivered on restart
            logger.warning("stopping with %s uncommitted: %s", position(message), error)
            return
        logger.warning(
            "MCP unavailable at %s, pausing %.1f s: %s",
            position(message),
            self._pause_seconds,
            error,
        )
        topic, partition, offset = (
            message.topic(),
            message.partition(),
            message.offset(),
        )
        if topic is None or partition is None or offset is None:
            return  # only error messages lack a position, and handle() drops those
        # rewind so the same message comes back after the pause
        self._consumer.seek(TopicPartition(topic, partition, offset))
        assigned = self._consumer.assignment()
        self._consumer.pause(assigned)
        # librdkafka keeps the group heartbeat in its own thread, so waiting
        # without polling does not cost the assignment
        self._stop_event.wait(self._pause_seconds)
        self._consumer.resume(assigned)

    def _commit(self, message: Message) -> None:
        try:
            self._consumer.commit(message=message, asynchronous=False)
        except KafkaException as error:
            # e.g. a rebalance: the message is redelivered later, and its actions
            # are safe to repeat, so this is not worth stopping the service for
            logger.warning(
                "commit failed at %s, it will be redelivered: %s",
                position(message),
                error,
            )


def position(message: Message) -> str:
    """Where a message sits, for log lines."""
    return (
        f"topic={message.topic()} partition={message.partition()} "
        f"offset={message.offset()}"
    )
