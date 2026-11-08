"""The Kafka source and sink (spec 5.1, 5.4, 10.4).

The adapters are exercised against a stand-in broker; the `@pytest.mark.kafka` tests at the
end run against a real one when `BRAIN_KAFKA_BOOTSTRAP` is reachable.
"""

import json
import uuid

import pytest

from city_brain.adapters.kafka import KafkaSink, KafkaSource
from city_brain.adapters.kafka.sink import canonical
from city_brain.adapters.kafka.source import PAUSE_AT, RESUME_AT

TOPICS = {"readings": "twin.sensors", "verdicts": "guardian.verdicts", "events": "guardian.events"}


class FakeMessage:
    def __init__(self, topic, value, error=None):
        self._topic, self._value, self._error = topic, value, error

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def error(self):
        return self._error


class FakeConsumer:
    def __init__(self, batches=()):
        self.batches = list(batches)
        self.subscribed: list[str] = []
        self.commits = 0
        self.paused: list = []
        self.resumed: list = []
        self.closed = False
        self._assignment = [("twin.sensors", 0)]

    def subscribe(self, topics):
        self.subscribed = list(topics)

    def consume(self, num_messages=1, timeout=0.0):
        return self.batches.pop(0) if self.batches else []

    def commit(self, asynchronous=False):
        self.commits += 1

    def assignment(self):
        return self._assignment

    def pause(self, parts):
        self.paused.append(list(parts))

    def resume(self, parts):
        self.resumed.append(list(parts))

    def close(self):
        self.closed = True


class FakeProducer:
    def __init__(self, fail=False):
        self.records: list[tuple[str, bytes, bytes]] = []
        self.flushed = 0
        self.fail = fail

    def produce(self, topic, key=None, value=None):
        if self.fail:
            raise BufferError("full")
        self.records.append((topic, key, value))

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        self.flushed += 1
        return 0


def source(batches=(), **kw):
    consumer = FakeConsumer(batches)
    src = KafkaSource("broker:9092", "city-brain", TOPICS, consumer=consumer, **kw)
    src.connect()
    return src, consumer


# ----- the source -----


def test_it_subscribes_to_every_partner_topic():
    src, consumer = source()
    assert consumer.subscribed == sorted(TOPICS.values())


def test_the_consumer_never_commits_on_its_own():
    src, _ = source()
    assert src.config["enable.auto.commit"] is False
    assert src.config["group.id"] == "city-brain"
    assert src.config["auto.offset.reset"] == "latest"


def test_a_message_is_tagged_with_the_stream_it_came_from():
    src, _ = source([[FakeMessage("twin.sensors", b'{"a":1}'),
                      FakeMessage("guardian.verdicts", b'{"b":2}')]])
    assert src.poll() == [("readings", b'{"a":1}'), ("verdicts", b'{"b":2}')]


def test_a_topic_nobody_asked_for_is_dropped():
    src, _ = source([[FakeMessage("someone.elses.topic", b"{}")]])
    assert src.poll() == []


def test_a_broker_error_is_not_a_reading():
    src, _ = source([[FakeMessage("twin.sensors", None, error="partition gone"),
                      FakeMessage("twin.sensors", b"{}")]])
    assert src.poll() == [("readings", b"{}")]


def test_an_empty_poll_is_not_an_error():
    src, _ = source()
    assert src.poll() == []


def test_committing_happens_only_after_something_was_taken_on():
    src, consumer = source([[FakeMessage("twin.sensors", b"{}")]])
    src.commit()
    assert consumer.commits == 0, "nothing had been read yet"
    src.poll()
    assert src.uncommitted == 1
    src.commit()
    assert consumer.commits == 1 and src.uncommitted == 0


def test_a_bare_topic_list_names_its_own_streams():
    src = KafkaSource("b:9092", "g", ["twin.sensors"], consumer=FakeConsumer())
    src.connect()
    assert src.topics == ["twin.sensors"]


def test_polling_before_connecting_is_a_mistake_not_a_crash():
    with pytest.raises(RuntimeError):
        KafkaSource("b:9092", "g", TOPICS).poll()


def test_closing_closes_the_consumer():
    src, consumer = source()
    src.close()
    assert consumer.closed


# ----- backpressure -----


def test_a_deep_queue_pauses_the_partitions():
    src, consumer = source()
    assert src.apply_backpressure(0) is False and consumer.paused == []
    assert src.apply_backpressure(PAUSE_AT) is True
    assert consumer.paused == [[("twin.sensors", 0)]]


def test_the_partitions_resume_once_the_backlog_drains():
    src, consumer = source()
    src.apply_backpressure(PAUSE_AT)
    assert src.apply_backpressure(RESUME_AT + 1) is True, "still above the resume mark"
    assert src.apply_backpressure(RESUME_AT - 1) is False
    assert consumer.resumed == [[("twin.sensors", 0)]]


def test_pausing_twice_pauses_once():
    src, consumer = source()
    src.apply_backpressure(PAUSE_AT)
    src.apply_backpressure(PAUSE_AT + 1000)
    assert len(consumer.paused) == 1


# ----- the sink -----


EVENT = {"specversion": "1.0", "id": "0-1", "type": "tn.tsyp.brain.command.v1",
         "subject": "z1-wl1", "data": {"status": "DISPATCHED"}}


def test_an_event_is_published_keyed_by_its_subject():
    producer = FakeProducer()
    sink = KafkaSink("broker:9092", producer=producer)
    assert sink.publish(EVENT) is True
    topic, key, value = producer.records[0]
    assert topic == "brain.events" and key == b"z1-wl1"
    assert json.loads(value) == EVENT


def test_the_bytes_are_the_same_canonical_json_the_log_writes():
    assert canonical(EVENT) == json.dumps(
        EVENT, sort_keys=True, separators=(",", ":")).encode("utf-8")


def test_the_producer_is_idempotent():
    assert KafkaSink("broker:9092").config["enable.idempotence"] is True


def test_sink_and_source_incorporate_sasl_config():
    sec = {
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "broadcast",
        "sasl.password": "secret123",
    }
    sink = KafkaSink("broker:9092", security_config=sec, acks=1)
    cfg = sink.config
    assert cfg["security.protocol"] == "SASL_PLAINTEXT"
    assert cfg["sasl.username"] == "broadcast"
    assert cfg["sasl.password"] == "secret123"
    assert cfg["acks"] == 1
    assert cfg["batch.num.messages"] == 1000
    assert cfg["enable.idempotence"] is False

    src = KafkaSource("broker:9092", "test-grp", ["topic1"], security_config=sec)
    src_cfg = src.config
    assert src_cfg["security.protocol"] == "SASL_PLAINTEXT"
    assert src_cfg["sasl.username"] == "broadcast"
    assert src_cfg["sasl.password"] == "secret123"


def test_a_full_queue_is_counted_not_raised():
    sink = KafkaSink("broker:9092", producer=FakeProducer(fail=True))
    assert sink.publish(EVENT) is False and sink.dropped == 1


def test_a_sink_without_a_broker_counts_what_it_could_not_send():
    sink = KafkaSink("broker:9092")
    assert sink.publish(EVENT) is False and sink.dropped == 1
    assert sink.flush() == 0


def test_closing_flushes():
    producer = FakeProducer()
    sink = KafkaSink("broker:9092", producer=producer)
    sink.publish(EVENT)
    sink.close()
    assert producer.flushed == 1


def test_an_event_with_no_subject_still_has_a_key():
    producer = FakeProducer()
    KafkaSink("broker:9092", producer=producer).publish({"id": "0-2"})
    assert producer.records[0][1] == b"city"


# ----- against a real broker -----


@pytest.mark.kafka
def test_a_published_event_comes_back_off_the_broker():
    from confluent_kafka import Consumer

    from tests.partners import ENDPOINTS

    bootstrap = ENDPOINTS["kafka"].value()
    topic = f"brain.events.test.{uuid.uuid4().hex[:8]}"
    sink = KafkaSink(bootstrap, topic=topic)
    sink.connect()
    assert sink.publish(EVENT) is True
    assert sink.flush(10.0) == 0
    consumer = Consumer({
        "bootstrap.servers": bootstrap, "group.id": f"test-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic])
    try:
        for _ in range(100):
            message = consumer.poll(timeout=1.0)
            if message is not None and not message.error():
                assert json.loads(message.value()) == EVENT
                return
        pytest.fail("the event never came back")
    finally:
        consumer.close()


@pytest.mark.kafka
def test_the_source_reads_what_the_sink_wrote():
    bootstrap = __import__("tests.partners", fromlist=["ENDPOINTS"]).ENDPOINTS["kafka"].value()
    topic = f"twin.sensors.test.{uuid.uuid4().hex[:8]}"
    sink = KafkaSink(bootstrap, topic=topic)
    sink.connect()
    sink.publish({"subject": "z1-wl1", "id": "0-1"})
    sink.flush(10.0)
    src = KafkaSource(bootstrap, f"test-{uuid.uuid4().hex[:8]}", {"readings": topic},
                      offset_reset="earliest")
    src.connect()
    try:
        for _ in range(100):
            batch = src.poll(timeout_s=1.0)
            if batch:
                assert batch[0][0] == "readings"
                src.commit()
                return
        pytest.fail("the source read nothing back")
    finally:
        src.close()
