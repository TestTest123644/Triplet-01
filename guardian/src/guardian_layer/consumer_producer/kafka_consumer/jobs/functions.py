import json
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError
from pyflink.common import Types
from pyflink.datastream.functions import KeyedBroadcastProcessFunction, ProcessFunction
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import MapStateDescriptor

from guardian_layer.consumer_producer.kafka_consumer.mcp_client.client import (
    McpToolClient,
)
from guardian_layer.consumer_producer.kafka_consumer.utils.schema import (
    Metadata,
    Sensor,
)
from guardian_layer.data_transformer.factory_transformer import TransformerFactory
from guardian_layer.data_transformer.transformer.twin_v1 import (
    TransformDropped,
    TransformRejected,
)

from .enrichment import CachedMetadataProvider, fetch_metadata
from .signature import DeviceKeyProvider, Ed25519SignatureVerifier

DEVICE_METADATA = MapStateDescriptor(
    "device_metadata",
    Types.STRING(),  # device_id
    Types.STRING(),  # JSON metadata
)

REJECTED = OutputTag("rejected", Types.STRING())


def reject(code: str, detail: str, raw) -> str:
    return json.dumps(
        {
            "error_code": code,
            "detail": detail,
            "raw": raw,
            "failed_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
        },
        default=str,
    )


class VerifySignature(ProcessFunction):
    def __init__(self, skip_verification: bool = False):
        super().__init__()
        self._skip = skip_verification

    def open(self, runtime_context):
        # created here, not in __init__: the client holds threads and
        # sockets and can't be pickled
        self._client = None
        self._verifier = None
        if self._skip:
            return
        self._client = McpToolClient(url="http://twin_layer:8000/sse", timeout_s=15.0)
        self._verifier = Ed25519SignatureVerifier(DeviceKeyProvider(self._client))

    def close(self):
        if self._client is not None:
            self._client.close()

    def process_element(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, value: str, ctx: ProcessFunction.Context
    ) -> Iterator[tuple[OutputTag, str] | dict[str, Any]]:
        try:
            payload = json.loads(value)
        except json.JSONDecodeError as e:
            yield REJECTED, reject("E100_DESERIALIZE", str(e), value)
            return

        if not isinstance(payload, dict):
            yield (
                REJECTED,
                reject("E100_DESERIALIZE", "payload is not an object", value),
            )
            return
        if not self._skip and self._verifier is not None:
            if not self._verifier.has_key(payload):
                yield (
                    REJECTED,
                    reject("E201_UNKNOWN_DEVICE", "no public key for device", payload),
                )
                return

            if not self._verifier.verify(payload):
                yield (
                    REJECTED,
                    reject("E200_BAD_SIGNATURE", "signature mismatch", payload),
                )
                return

            payload.pop("sig", None)
        yield payload


class EnrichAndTransform(ProcessFunction):
    def open(self, runtime_context):
        self._meta = CachedMetadataProvider(fetch_metadata, ttl_s=300)

    def process_element(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, payload: dict, ctx: ProcessFunction.Context
    ) -> Iterator[tuple[OutputTag, str] | Sensor]:
        try:
            meta = self._meta.get(payload.get("sensor_id", ""))
            sensor = Sensor(
                **payload,
                metadata=Metadata(**meta) if meta else None,
            )
        except (
            ValidationError
        ) as e:  # schema errors (subclass of ValueError, so catch first)
            yield REJECTED, reject("E300_SCHEMA", e.json(), payload)
            return
        except ValueError as e:  # raised by SensorValidator in model_post_init
            yield REJECTED, reject("E400_SENSOR_VALIDATION", str(e), payload)
            return
        except Exception as e:  # enrichment backend failure, etc.
            yield REJECTED, reject("E500_ENRICHMENT", repr(e), payload)
            return

        yield sensor  # main output: JSON string


class TransformTwin(ProcessFunction):
    def __init__(self, transformer_name: str = "twin_v1"):
        super().__init__()
        self._name = transformer_name

    def open(self, runtime_context):
        self._transformer = TransformerFactory.create(self._name)

    def process_element(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, payload: dict, ctx: ProcessFunction.Context
    ) -> Iterator[tuple[OutputTag, str] | str]:
        try:
            sensor = self._transformer.transform(payload)
        except TransformDropped:
            return  # expected; skip, optionally count a metric
        except TransformRejected as e:
            yield REJECTED, reject(e.code, e.detail, payload)
            return
        except Exception as e:
            yield REJECTED, reject("E400_SENSOR_VALIDATION", repr(e), payload)
            return

        yield sensor.model_dump_json()


class DeviceEnrichment(KeyedBroadcastProcessFunction):
    def process_broadcast_element(self, value, ctx) -> None:
        """
        Receive metadata updates and update the broadcast state.
        """
        metadata = json.loads(value)

        state = ctx.get_broadcast_state(DEVICE_METADATA)

        device_id = metadata["device_id"]

        if metadata.get("operation") == "delete":
            state.remove(device_id)
            return

        state.put(device_id, value)

    def process_element(self, value, ctx) -> Iterator[str]:  # type: ignore[override]
        """
        Enrich sensor events using device_id.
        """
        event = json.loads(value)

        device_id = event["device_id"]

        state = ctx.get_broadcast_state(DEVICE_METADATA)

        metadata = state.get(device_id)

        if metadata is None:
            # Metadata not available yet.
            # Handle this through a retry mechanism or side output.
            yield value
            return

        metadata = json.loads(metadata)

        enriched = {
            **event,
            "device_metadata": metadata,
        }

        yield json.dumps(enriched)
