from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, Self
from uuid import uuid4

import numpy as np
import numpy.typing as npt
from pydantic import AwareDatetime, BaseModel, Field

if TYPE_CHECKING:
    from guardian_layer.managers.buffer_manger import BufferManager
    from guardian_layer.utils.config import KalmanSettings
    from guardian_layer.utils.dataset_config import FaultSettings

FLUSH_QUEUE_MAXSIZE = 1000

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]
BoolArray = npt.NDArray[np.bool_]
StrArray = npt.NDArray[np.str_]


class Status(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"


class Component(StrEnum):
    CONSUMER = "consumer"
    CLUSTER = "cluster"
    BUFFER = "buffer"


class Anomalies(StrEnum):
    OUT_OF_RANGE = "out_of_range"
    SPOOFED_DATA = "spoofed_data"
    NOT_SENSOR_DATA = "not_sensor_data"
    SENSOR_MALFUNCTION = "sensor_malfunction"
    SENSOR_IS_DOWN = "sensor_is_down"


class SensorType(StrEnum):
    WATER_LEVEL = "water_level"
    POWER_LOAD = "power_load"
    AIR_QUALITY = "air_quality"


@dataclass(frozen=True, slots=True)
class SensorRule:
    unit: str
    lower: float
    upper: float


@dataclass(frozen=True, slots=True)
class FaultContext:
    """What a fault function needs for one sensor window."""

    values: FloatArray
    rng: np.random.Generator
    settings: FaultSettings
    spread: float  # the type's normal within-window std
    sign: float  # drawn before the handler runs: +1 or -1
    value_range: tuple[float, float]


@dataclass(frozen=True, slots=True)
class CleanContext:
    """What a noise-cleaning function needs for one sensor's series (#66)."""

    values: FloatArray  # the raw readings, in time order
    kalman: KalmanSettings
    min_samples: int  # shorter series are returned unchanged


@dataclass(frozen=True, slots=True)
class KalmanPass:
    """The Kalman filter's forward pass over one series, per reading."""

    means: FloatArray  # the filtered level
    variances: FloatArray  # its variance after the update
    predicted: FloatArray  # its variance before the update (index 0 unused)
    jumps: BoolArray  # taken as they are, not smoothed


class CheckResult(StrEnum):  # a static check that names no anomaly, instead of None
    PASS = "pass"  # checked, nothing wrong
    NOT_CHECKED = "not_checked"  # could not check (no range, bad input): not "clean"


class Metadata(BaseModel):
    lat: float | None = None
    lon: float | None = None
    packet_order: int
    type: str
    upper_range: float | None = None
    lower_range: float | None = None


SENSOR_RULES: dict[SensorType, SensorRule | None] = {
    SensorType.WATER_LEVEL: SensorRule("m", 0.0, 3.000),
    SensorType.POWER_LOAD: None,
    SensorType.AIR_QUALITY: None,
}


class Sensor(BaseModel):
    sensor_id: str
    cluster_id: str
    value: float
    unit: str
    timestamp: datetime
    metadata: Metadata | None = None
    quality_flags: list[Anomalies] = Field(default_factory=list)

    def model_post_init(self, context: Any) -> None:
        from guardian_layer.utils.sensor_verification import SensorValidator

        result = SensorValidator(self).execute()
        if not result.valid:
            raise ValueError("; ".join(result.errors))
        self.quality_flags = list(
            dict.fromkeys((*self.quality_flags, *result.anomalies))
        )


class DatasetRow(NamedTuple):  # one parsed dataset value, before it becomes a reading
    timestamp: datetime
    station: str
    kind: str  # the measured quantity: metadata.type
    unit: str
    value: float
    lat: float | None = None
    lon: float | None = None
    label: Anomalies | None = None
    event: bool = False  # inside a real-world event window; not a fault


@dataclass(frozen=True, slots=True)
class VerificationResult:
    errors: tuple[str, ...] = ()
    anomalies: tuple[Anomalies, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.errors

    @property
    def flagged(self) -> bool:
        return bool(self.anomalies)


@dataclass(frozen=True, slots=True)
class LabeledReading:  # one loaded dataset reading and what the source says about it
    sensor: Sensor
    label: Anomalies | None = None  # from a source fault flag; None = no flag (normal)
    event: bool = False  # inside a real-world event window (NAB); not a fault


@dataclass(slots=True)
class Ack:  # renamed from Message to avoid clashing with the telemetry dict type
    id: str  # unique message id
    detail: str  # text note
    status: Status
    sender: Component
    receiver: Component

    @property
    def ok(self) -> bool:  # derived, cannot disagree with status
        return self.status is Status.SUCCESS


@dataclass(frozen=True, slots=True)
class FlushedBatch:  # plain data, no lock, safe to queue
    id: str
    sensor_id: str
    readings: tuple[Sensor, ...]
    flushed_at: datetime


@dataclass(slots=True)
class Buffer:  # what workers receive
    id: str
    sensor_id: str
    readings: list[Sensor]
    last_update: datetime
    last_flush: datetime = field(default_factory=lambda: datetime.now(UTC))
    high_watermark: int = 200
    flush_size: int = field(default=100)  # how many oldest readings a flush takes
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False, compare=False
    )  # guards readings, last_flush and last_update

    def __len__(self) -> int:
        return len(self.readings)

    def __post_init__(self) -> None:
        if self.flush_size > self.high_watermark:
            raise ValueError(
                f"flush_size {self.flush_size} cannot exceed "
                + f"high_watermark {self.high_watermark}"
            )
        if not self.sensor_id.strip():
            raise ValueError("sensor_id must not be empty or whitespace")
        if not self.id.strip():
            raise ValueError("id must not be empty or whitespace")

    @property
    def is_full(self) -> bool:
        """Check if readings have hit the high watermark."""
        return len(self) >= self.high_watermark

    def add_reading(self, reading: Sensor) -> FlushedBatch | None:
        """Append a reading, flushing the oldest ``flush_size`` first when full.

        Returns the flushed batch, or None when no flush ran.
        """
        flushed: list[Sensor] = []
        with self._lock:
            now = datetime.now(UTC)
            if self.is_full:
                flushed = self.readings[: self.flush_size]
                del self.readings[: self.flush_size]
                self.last_flush = now
            self.readings.append(reading)
            self.last_update = now

        if not flushed:
            return None

        try:
            return FlushedBatch(
                id=str(uuid4()),
                sensor_id=self.sensor_id,
                readings=tuple(flushed),
                flushed_at=now,
            )
        except Exception as error:
            raise RuntimeError(
                f"Flush failed for sensor {self.sensor_id}: "
                f"{len(flushed)} readings dropped"
            ) from error

    def restore(self, readings: Sequence[Sensor]) -> None:
        """Put the readings of a failed flush back at the front, oldest first."""
        with self._lock:
            self.readings[:0] = readings


@dataclass(slots=True)
class Cluster:
    id: str
    sensor_type: str
    buffers: dict[str, BufferManager] = field(
        default_factory=dict
    )  # keyed by sensor_id
    last_update: datetime | None = None

    @property
    def length(self) -> int:
        return len(self.buffers)


@dataclass(frozen=True)
class Verdict:  # what the anomaly model says about one sensor's window
    sensor_id: str
    score: float  # anomaly score, 1 - P(normal), 0-1
    label: Anomalies | None  # the anomaly the model names; None = normal

    @property
    def anomalous(self) -> bool:  # derived, cannot disagree with label
        return self.label is not None


@dataclass(frozen=True)
class SensorSummary:
    """What the spatial step needs from one sensor's normalized window."""

    sensor_id: str
    cluster: str
    lat: float | None
    lon: float | None
    mean: float
    last: float


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
    status: TwinStatus
    location: dict[str, Any] | str | None = None


class _Flat:
    """Arrays by field name: the flat layout of the ``.npz`` read cache."""

    __slots__ = ()
    __dataclass_fields__: ClassVar[dict[str, Any]]  # set by @dataclass

    @classmethod
    def from_arrays(cls, arrays: Mapping[str, np.ndarray]) -> Self:
        return cls(**{f.name: arrays[f.name] for f in fields(cls)})

    def to_arrays(self) -> dict[str, np.ndarray]:
        return {f.name: getattr(self, f.name) for f in fields(self)}


@dataclass(frozen=True, slots=True)
class ReadingColumns(_Flat):
    """Per reading, sorted by (slot, station in natural order, time, loader order)."""

    slot: IntArray  # rescaled time: n-th sample period since the first reading
    value: FloatArray
    packet_order: IntArray
    lat: FloatArray  # NaN = none
    lon: FloatArray
    label: npt.NDArray[np.int8]  # index into ReadInfo.label_names, -1 = no source label
    event: BoolArray
    sensor: IntArray  # index into the SensorTable arrays


@dataclass(frozen=True, slots=True)
class SensorTable(_Flat):
    """Per sensor, in (station, type) order: the sensor index is its target rank."""

    sensor_id: StrArray
    cluster_id: StrArray
    unit: StrArray
    sensor_type: StrArray
    station: StrArray
    lower_range: FloatArray  # NaN = none
    upper_range: FloatArray


@dataclass(frozen=True, slots=True)
class ReadInfo(_Flat):
    """How the source was read, for a cache to compare with settings (0-d arrays)."""

    source: StrArray
    start: FloatArray  # epoch seconds of the first kept reading
    sample_seconds: FloatArray
    every_hours: IntArray
    block_hours: IntArray
    label_names: StrArray  # 1-d: the Anomalies values the label column indexes


@dataclass(frozen=True, slots=True)
class RawReadings:
    """What a loader's stream gives, before sampling drops and ranking.

    Per reading in loader order, and per sensor in first-seen order (the ``sensor``
    column indexes the per-sensor arrays).
    """

    time: FloatArray  # epoch seconds
    value: FloatArray
    lat: FloatArray  # NaN = none
    lon: FloatArray
    sensor: IntArray
    packet_order: IntArray
    label: npt.NDArray[np.int8]  # index into the Anomalies values, -1 = none
    event: BoolArray
    sensor_id: StrArray
    cluster_id: StrArray
    unit: StrArray
    sensor_type: StrArray
    lower_range: FloatArray  # NaN = none
    upper_range: FloatArray


@dataclass(frozen=True, slots=True)
class WindowGroups:
    """The sensor-windows of one source: the rows of each (window, sensor), in time
    order, and what each one is.

    ``window`` is per reading; the rest is per sensor-window, in target order (the
    sensor index is the target rank).
    """

    window_slots: int  # slots per window, the one place build reads it
    window: IntArray  # the window of each reading
    groups: list[IntArray]  # the rows of each sensor-window
    group_window: IntArray
    group_sensor: IntArray
    group_type: StrArray
    real: BoolArray  # the source labels a reading of it
    event: BoolArray  # inside an event window, and not real
    clean: BoolArray  # neither real nor event
    eligible: BoolArray  # clean, with enough readings to take features from


@dataclass(frozen=True, slots=True)
class SourceReadings:
    """One source's readings as numpy columns, ready to cut into windows.

    Only numpy arrays (scalars as 0-d arrays), so ``np.savez(path, **to_arrays())``
    caches it and ``from_arrays(np.load(path))`` reads it back: read once, build per
    seed. Each part loads alone, for the code that needs only its columns.
    """

    readings: ReadingColumns
    sensors: SensorTable
    info: ReadInfo

    @classmethod
    def from_arrays(cls, arrays: Mapping[str, np.ndarray]) -> SourceReadings:
        return cls(
            ReadingColumns.from_arrays(arrays),
            SensorTable.from_arrays(arrays),
            ReadInfo.from_arrays(arrays),
        )

    def to_arrays(self) -> dict[str, np.ndarray]:
        return {
            **self.readings.to_arrays(),
            **self.sensors.to_arrays(),
            **self.info.to_arrays(),
        }


@dataclass(frozen=True)
class WindowDataset:
    """One row per sensor-window (one sensor in one window)."""

    features: FloatArray  # columns: feature_keys(method), or the keys asked for
    label: BoolArray  # injected with one of the asked kinds
    kind: StrArray  # "", an injected kind, "real_fault" or "event"
    sensor_type: StrArray
    real_fault: BoolArray  # the source labels a reading of it (kept apart)
    event: BoolArray  # inside a real-world event window, not a fault (kept apart)
    source_label: StrArray  # the source's Anomalies value, or ""
    train: BoolArray
    out_of_range: BoolArray  # injected, with a value outside its type's range
    sensor_id: StrArray
    window: IntArray  # window number: consecutive windows differ by 1
    dataset: StrArray  # the source name
    spreads: dict[str, float]  # per type, the spread faults were sized with
