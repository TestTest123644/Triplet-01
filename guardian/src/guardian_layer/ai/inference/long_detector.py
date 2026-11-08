"""Long-window inference (#96): feature rows of hours of readings in, one verdict per
sensor out.

The caller chains the stages, as the live orchestrator chains ``process_window`` and
``AnomalyDetector``: ``process_long_window`` builds the rows, then this scores them.
The follow-up batch job feeds it stored readings; nothing calls it live.
"""

from pathlib import Path

from guardian_layer.ai.inference.detector import model_verdicts
from guardian_layer.ai.inference.model import BaselineModel
from guardian_layer.utils.config import get_settings
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import Verdict

logger = get_logger(__name__)


class LongAnomalyDetector:
    """Score each sensor's long window with the long-window model."""

    model_name: str = "long_window_24h"
    model_version: str = "long_window_24h"

    def __init__(self, model: BaselineModel, min_grid_points: int) -> None:
        # a short-window or Intel model loaded from the wrong folder would score the
        # long rows on columns they do not hold
        unknown = sorted(set(model.keys) - set(get_settings().preprocess.long_keys))
        if unknown:
            raise ValueError(f"model keys not in long_keys: {unknown}")
        self.model = model
        # filled grid buckets a sensor needs; fewer give all-zero features
        self.min_grid_points = min_grid_points

    @classmethod
    def from_directory(cls, directory: Path) -> "LongAnomalyDetector":
        return cls(
            BaselineModel.load(directory), get_settings().long_window.min_grid_points
        )

    def predict_features(
        self, features: dict[str, dict[str, float]]
    ) -> dict[str, Verdict]:
        """Verdicts from ``process_long_window``'s rows (a missing feature raises), in
        the rows' order; sensors with too few grid points are left out."""
        kept = [
            sensor_id
            for sensor_id, row in features.items()
            if row["lw_grid_points"] >= self.min_grid_points
        ]
        verdicts = model_verdicts(self.model, features, kept)
        logger.info(
            "Scored long windows",
            scored=len(kept),
            skipped=len(features) - len(kept),
            flagged=sum(verdict.anomalous for verdict in verdicts.values()),
        )
        return verdicts
