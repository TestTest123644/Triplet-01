import json
from collections.abc import Iterator
from datetime import datetime, timezone

from pydantic import ValidationError
from pyflink.common import Types
from pyflink.datastream.functions import KeyedBroadcastProcessFunction, ProcessFunction
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import MapStateDescriptor

from guardian_layer.consumer_producer.kafka_consumer.mcp_client.client import (
    McpToolClient,
)
from guardian_layer.data_transformer.factory_transformer import TransformerFactory
from guardian_layer.data_transformer.transformer.twin_v1 import (
    TransformDropped,
    TransformRejected,
)

from .signature import DeviceKeyProvider, Ed25519SignatureVerifier

# ----------------------------------------------------------------------
# Convention: every operator receives and emits a JSON *string*
# (Types.STRING()). Each function json.loads() on entry and
# json.dumps() / model_dump_json() on exit. Never pass dicts or
# Sensor objects between operators.
# ----------------------------------------------------------------------

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


class ParseJson(ProcessFunction):
    """
    First operator, always runs (dev and prod).
    Guarantees that everything downstream is a JSON object with a
    string device_id, so key_by can never crash on a bad record.
    """

    def process_element(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, value: str, ctx: ProcessFunction.Context
    ) -> Iterator[tuple[OutputTag, str] | str]:
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

        device_id = payload.get("device_id")
        if not isinstance(device_id, str) or not device_id:
            yield (
                REJECTED,
                reject(
                    "E101_MISSING_DEVICE_ID",
                    "device_id missing or not a string",
                    payload,
                ),
            )
            return

        yield value


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
    ) -> Iterator[tuple[OutputTag, str] | str]:
        if self._skip or self._verifier is None:
            yield value
            return

        payload = json.loads(value)  # already validated by ParseJson

        # NOTE: McpError from the key lookup is deliberately NOT caught.
        # An MCP outage fails the task and Flink restarts from the last
        # checkpoint instead of sending valid messages to the DLQ.
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
        yield json.dumps(payload)


class DeviceEnrichment(KeyedBroadcastProcessFunction):
    def process_broadcast_element(self, value, ctx) -> None:
        """Receive metadata updates and update the broadcast state."""
        try:
            metadata = json.loads(value)
            device_id = metadata["device_id"]
        except (json.JSONDecodeError, KeyError, TypeError):
            # A bad record on the metadata topic must not kill the job.
            return

        state = ctx.get_broadcast_state(DEVICE_METADATA)

        if metadata.get("operation") == "delete":
            state.remove(device_id)
            return

        state.put(device_id, value)

    def process_element(self, value, ctx) -> Iterator[str]:  # type: ignore[override]
        """Attach broadcast metadata (if any) under 'device_metadata'."""
        event = json.loads(value)  # validated by ParseJson
        device_id = event["device_id"]

        state = ctx.get_broadcast_state(DEVICE_METADATA)
        metadata = state.get(device_id)

        if metadata is None:
            # Metadata not available yet: pass the event through unenriched.
            yield value
            return

        yield json.dumps({**event, "device_metadata": json.loads(metadata)})


class EnrichAndTransform(ProcessFunction):
    """Build and validate the Sensor model. Emits sensor JSON."""

    def __init__(self, transformer_name: str = "twin_v1"):
        super().__init__()
        self._name = transformer_name

    def open(self, runtime_context):
        self._transformer = TransformerFactory.create(self._name)

    def process_element(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, value: str, ctx: ProcessFunction.Context
    ) -> Iterator[tuple[OutputTag, str] | str]:
        try:
            payload = json.loads(value)
        except json.JSONDecodeError as e:
            yield REJECTED, reject("E100_DESERIALIZE", str(e), value)
            return

        try:
            sensor = self._transformer.transform(payload)
        except TransformDropped:
            return
        except TransformRejected as e:
            yield REJECTED, reject(e.code, e.detail, payload)
            return
        except ValidationError as e:
            yield REJECTED, reject("E300_SCHEMA", e.json(), payload)
            return
        except ValueError as e:
            yield REJECTED, reject("E400_SENSOR_VALIDATION", str(e), payload)
            return
        except Exception as e:
            yield REJECTED, reject("E500_ENRICHMENT", repr(e), payload)
            return

        yield sensor.model_dump_json()


class TransformTwin(ProcessFunction):
    def __init__(self, transformer_name: str = "twin_v1"):
        super().__init__()
        self._name = transformer_name

    def open(self, runtime_context):
        self._transformer = TransformerFactory.create(self._name)

    def process_element(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, value: str, ctx: ProcessFunction.Context
    ) -> Iterator[tuple[OutputTag, str] | str]:
        try:
            payload = json.loads(value)
        except json.JSONDecodeError as e:
            yield REJECTED, reject("E100_DESERIALIZE", str(e), value)
            return

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
