from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import AwareDatetime, BaseModel


class TwinStatus(StrEnum):
    OK = "ok"
    ISOLATED = "isolated"
    QUARANTINED = "quarantined"
    ROLLED_BACK = "rolled_back"
    MODELLED = "modelled"


class TwinSensorReading(BaseModel):
    device_id: str
    device_type: str
    value: float
    unit: str
    ts: AwareDatetime  # RFC 3339: the offset is required
    seq: int
    status: str | None = None
    location: dict[str, Any] | str | None = None
    reading_id: str | None = None
