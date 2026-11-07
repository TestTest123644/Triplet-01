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
    TransformTwin,
    VerifySignature,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:9092")


def kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def metadata_source() -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics("devices.metadata")
        .set_group_id("device-metadata-v1")
        .set_starting_offsets(
            KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def raw_source() -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics("sensors.raw")
        .set_group_id("sensor-pipeline-v1")
        .set_starting_offsets(
            KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        os.environ["KAFKA_CONNECTOR_JAR"]
    )  # file:///.../flink-sql-connector-kafka-*.jar
    env.set_parallelism(6)  # <= number of partitions of sensors.raw
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

    verified = (
        raw.process(VerifySignature())
        if os.getenv("enviorment", "dev") == "prod"
        else raw
    )
    metadata_broadcast = metadata_raw.broadcast(DEVICE_METADATA)

    # 3. Enrich events
    keyed_events = verified.key_by(
        lambda value: json.loads(value)["device_id"],
        key_type=Types.STRING(),
    )

    enriched = keyed_events.connect(metadata_broadcast).process(
        DeviceEnrichment(),
        output_type=Types.STRING(),
    )
    transformed = enriched.process(EnrichAndTransform(), output_type=Types.STRING())

    rejected = verified.get_side_output(REJECTED).union(
        transformed.get_side_output(REJECTED)
    )
    sensors = transformed.process(
        TransformTwin(os.getenv("TRANSFORMER_NAME", "twin_v1")),
        output_type=Types.STRING(),
    )

    sensors.sink_to(kafka_sink("sensors.validated"))
    rejected.sink_to(kafka_sink("sensors.dlq"))

    env.execute("sensor-validation-pipeline")


# create a seprate setting file for the consumer job
# create the docker.flink file for the consumer job
if __name__ == "__main__":
    main()
