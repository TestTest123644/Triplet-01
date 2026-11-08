from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

import numpy as np
import numpy.typing as npt

from guardian_layer.models.sensor import Anomalies

if TYPE_CHECKING:
    from guardian_layer.utils.config import KalmanSettings
    from guardian_layer.utils.dataset_config import FaultSettings

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]
BoolArray = npt.NDArray[np.bool_]
StrArray = npt.NDArray[np.str_]


class CheckResult(StrEnum):  # a static check that names no anomaly, instead of None
    PASS = "pass"  # checked, nothing wrong
    NOT_CHECKED = "not_checked"  # could not check (no range, bad input): not "clean"


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
