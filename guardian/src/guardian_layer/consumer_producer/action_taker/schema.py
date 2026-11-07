"""Messages action_taker reads and the actions it takes."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import AwareDatetime, BaseModel

from guardian_layer.utils.schema import Anomalies


class ToolName(StrEnum):
    """Twin MCP tools action_taker calls (names from twin/mcp_server.py)."""

    ISOLATE_DEVICE = "isolate_device"
    QUARANTINE_DEVICE = "quarantine_device"
    ROLLBACK_READING = "rollback_reading"
    REINSTATE_DEVICE = "reinstate_device"
    GET_DEVICE_STATE = "get_device_state"


@dataclass(frozen=True, slots=True)
class Action:
    """One MCP call to make: the tool and the device or reading it targets."""

    tool: ToolName
    target: str

    @property
    def arguments(self) -> dict[str, str]:
        # rollback_reading is the only tool keyed by reading; the rest take a device
        if self.tool is ToolName.ROLLBACK_READING:
            return {"reading_id": self.target}
        return {"device_id": self.target}


class OutcomeStatus(StrEnum):
    DONE = "done"  # the twin applied the action
    REFUSED = "refused"  # the twin answered with an error (device/reading not found)
    SKIPPED = "skipped"  # nothing to do (reinstate on a device that is not contained)
    DRY_RUN = "dry_run"  # dry run: logged, not sent


@dataclass(frozen=True, slots=True)
class ActionOutcome:
    status: OutcomeStatus
    detail: str


def _text_field(raw: dict[str, Any] | str, key: str) -> str | None:
    """A non-empty string field of a DLQ ``raw`` payload, or None."""
    if not isinstance(raw, dict):
        return None
    value = raw.get(key)
    return value if isinstance(value, str) and value else None


class DlqRecord(BaseModel):
    """A rejected record from Flink (format: ``reject`` in kafka_consumer/jobs)."""

    error_code: str
    detail: str
    raw: dict[str, Any] | str  # the original reading; a string when it was not JSON
    failed_at: AwareDatetime

    @property
    def device_id(self) -> str | None:
        return _text_field(self.raw, "device_id")

    @property
    def reading_id(self) -> str | None:
        return _text_field(self.raw, "reading_id")


class AnomalyRecord(BaseModel):
    """An anomaly detection from Guardian; ``label`` None means back to normal."""

    device_id: str
    reading_id: str | None = None
    label: Anomalies | None  # required key: null must be explicit, not missing
    detected_at: AwareDatetime
