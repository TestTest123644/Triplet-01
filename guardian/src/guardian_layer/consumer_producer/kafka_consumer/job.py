import json
import os

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from guardian_layer.consumer_producer.kafka_consumer.jobs.functions import (
    DEVICE_METADATA,
    REJECTED,
    DeviceEnrichment,
    EnrichAndTransform,
    ParseJson,
    TransformTwin,
    VerifySignature,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:9092")

# Fail closed: signature verification is skipped ONLY when ENVIRONMENT=dev
ENVIRONMENT = os.getenv("ENVIRONMENT", "prod").lower()
PARALLELISM = int(os.getenv("PARALLELISM", "6"))  # <= partitions of sensors.raw
INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "telemetry-ingest")
METADATA_TOPIC = os.getenv("KAFKA_METADATA_TOPIC", "devices-metadata")
VALIDATED_TOPIC = os.getenv("KAFKA_VALIDATED_TOPIC", "telemetry-validated")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "telemetry-dlq")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "guardian-processor")


def _read_password() -> str | None:
    password_file = os.getenv("KAFKA_PASSWORD_FILE")
    if password_file:
        try:
            with open(password_file, encoding="utf-8") as f:
                return f.read().strip()
        except OSError as e:
            raise RuntimeError(
                f"Failed to read Kafka password from {password_file}: {e}"
            ) from e
    return os.getenv("KAFKA_PASSWORD")


def get_kafka_properties() -> dict[str, str]:
    properties: dict[str, str] = {}
    username = os.getenv("KAFKA_USERNAME") or os.getenv("KAFKA_USER")
    password = _read_password()

    if username and password:
        login_module = os.getenv(
            "KAFKA_LOGIN_MODULE",
            "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule",
        )
        properties["security.protocol"] = os.getenv(
            "KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"
        )
        properties["sasl.mechanism"] = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
        properties["sasl.jaas.config"] = (
            f'{login_module} required username="{username}" password="{password}";'
        )
    return properties


def kafka_sink(topic: str) -> KafkaSink:
    builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    )
    for key, value in get_kafka_properties().items():
        builder.set_property(key, value)
    return builder.build()


def metadata_source() -> KafkaSource:
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(METADATA_TOPIC)
        .set_group_id(f"{GROUP_ID}-metadata")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
    )
    for key, value in get_kafka_properties().items():
        builder.set_property(key, value)
    return builder.build()


def raw_source() -> KafkaSource:
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(INPUT_TOPIC)
        .set_group_id(f"{GROUP_ID}-raw")
        .set_starting_offsets(
            KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
        )
        .set_value_only_deserializer(SimpleStringSchema())
    )
    for key, value in get_kafka_properties().items():
        builder.set_property(key, value)
    return builder.build()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(30_000)

    raw = env.from_source(
        source=raw_source(),
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="sensors-raw",
    )

    metadata_raw = env.from_source(
        source=metadata_source(),
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="device-metadata",
    )

    # Every stream that can emit REJECTED gets collected here
    rejected_streams = []

    # 1. Parse (always): bad JSON / missing device_id -> DLQ, never a crash
    parsed = raw.process(ParseJson(), output_type=Types.STRING())
    rejected_streams.append(parsed.get_side_output(REJECTED))

    # 2. Verify signatures (everything except dev)
    if ENVIRONMENT == "dev":
        verified = parsed
    else:
        verified = parsed.process(VerifySignature(), output_type=Types.STRING())
        rejected_streams.append(verified.get_side_output(REJECTED))

    # 3. Broadcast device metadata and attach it to events
    metadata_broadcast = metadata_raw.broadcast(DEVICE_METADATA)

    keyed_events = verified.key_by(
        lambda value: json.loads(value)["device_id"],
        key_type=Types.STRING(),
    )

    enriched = keyed_events.connect(metadata_broadcast).process(
        DeviceEnrichment(),
        output_type=Types.STRING(),
    )

    # 4. Build + validate the Sensor model
    validated = enriched.process(EnrichAndTransform(), output_type=Types.STRING())
    rejected_streams.append(validated.get_side_output(REJECTED))

    # 5. Transform to the twin format
    sensors = validated.process(
        TransformTwin(os.getenv("TRANSFORMER_NAME", "twin_v1")),
        output_type=Types.STRING(),
    )
    rejected_streams.append(sensors.get_side_output(REJECTED))  # was missing before

    rejected = rejected_streams[0].union(*rejected_streams[1:])

    sensors.sink_to(kafka_sink(VALIDATED_TOPIC))
    rejected.sink_to(kafka_sink(DLQ_TOPIC))

    env.execute("sensor-validation-pipeline")


if __name__ == "__main__":
    main()
