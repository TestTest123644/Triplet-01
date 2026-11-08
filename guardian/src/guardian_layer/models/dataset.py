from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, ClassVar, NamedTuple, Self

import numpy as np
import numpy.typing as npt

from guardian_layer.models.sensor import Anomalies, Sensor

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]
BoolArray = npt.NDArray[np.bool_]
StrArray = npt.NDArray[np.str_]


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
class LabeledReading:  # one loaded dataset reading and what the source says about it
    sensor: Sensor
    label: Anomalies | None = None  # from a source fault flag; None = no flag (normal)
    event: bool = False  # inside a real-world event window (NAB); not a fault


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
