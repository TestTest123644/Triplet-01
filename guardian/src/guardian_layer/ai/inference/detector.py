"""Inference service: each sensor's window features in, one verdict per sensor out."""

from pathlib import Path

import numpy as np

from guardian_layer.ai.inference.model import BaselineModel
from guardian_layer.utils.config import get_settings
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import Verdict

logger = get_logger(__name__)


def model_verdicts(
    model: BaselineModel,
    features: dict[str, dict[str, float]],
    sensor_ids: list[str],
) -> dict[str, Verdict]:
    """One model's verdicts for these sensors (shared by the live and long-window
    detectors). Each row needs the model's keys and ``sample_count``."""
    if not sensor_ids:
        return {}
    rows = np.array(
        [[features[s][key] for key in model.keys] for s in sensor_ids],
        dtype=np.float64,
    )
    scores, classes = model.predict(rows)
    return {
        sensor_id: Verdict(
            sensor_id,
            float(score),
            # the score decides whether it is an anomaly, at the threshold of the
            # sensor's sampling rate; the class says which
            model.classes[int(index) - 1]
            if score > model.threshold_for(features[sensor_id]["sample_count"])
            else None,
        )
        for sensor_id, score, index in zip(sensor_ids, scores, classes, strict=True)
    }


class AnomalyDetector:
    """Score the live 20 s window of every fast sensor with the short-window model.

    A slow sensor has 0-2 readings in 20 s, too few for any model, so it gets no
    verdict here: the long-window model judges it on hours of stored readings
    (``LongAnomalyDetector``, run as a batch job; until then only the static checks).
    """

    model_name: str = "short_window"
    model_version: str = "short_window"

    def __init__(
        self, short_model: BaselineModel, min_rate: float | None = None
    ) -> None:
        if short_model.window_seconds is None:
            raise ValueError(
                "the short-window model needs its window_seconds to count rates"
            )
        self.short_model = short_model
        self.window_seconds = short_model.window_seconds
        # readings per second from which a sensor goes to the short-window model
        self.min_rate = (
            get_settings().inference.short_window_min_rate
            if min_rate is None
            else min_rate
        )

    @classmethod
    def from_directory(cls, short_dir: Path) -> "AnomalyDetector":
        return cls(BaselineModel.load(short_dir))

    def is_short(self, row: dict[str, float]) -> bool:
        """Whether the sensor's window is fast enough for the short-window model."""
        return row["sample_count"] / self.window_seconds >= self.min_rate

    def predict_features(
        self, features: dict[str, dict[str, float]]
    ) -> dict[str, Verdict]:
        """Verdicts of the fast sensors from per-sensor feature rows (a missing feature
        raises), in the rows' order; slow sensors are left out."""
        fast = [
            sensor_id for sensor_id in features if self.is_short(features[sensor_id])
        ]
        slow = len(features) - len(fast)
        if slow:
            logger.info("Left slow sensors to the long-window model", slow=slow)
        return model_verdicts(self.short_model, features, fast)
