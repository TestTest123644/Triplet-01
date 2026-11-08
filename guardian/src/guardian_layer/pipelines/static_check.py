"""Static checks for #55: the anomaly labels that rules can give without the model.

One class, ``StaticCheck``: one method per label, each answering for one sensor's
window with its ``Anomalies`` label, ``CheckResult.PASS`` (checked, nothing wrong) or
``CheckResult.NOT_CHECKED`` (could not check). Chaining them with the model is left to
the orchestrator. Compare results with ``is``: every value is a non-empty string, so
``if check(...)`` is always true.
"""

import math
from datetime import datetime
from typing import Any

from guardian_layer.utils.config import StaticCheckSettings, get_settings
from guardian_layer.utils.helpers import epoch_seconds
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import Anomalies, CheckResult, Sensor

logger = get_logger(__name__)


class StaticCheck:
    """Rule checks on one sensor's window; a check never raises on its content."""

    def __init__(self, settings: StaticCheckSettings | None = None) -> None:
        self.settings = settings or get_settings().static_check

    # every label is logged with what triggered it, so a false alarm can be traced
    def fired(
        self, label: Anomalies, sensor_id: str | None, value: Any, threshold: Any
    ) -> Anomalies:
        """Log the rule, sensor, value and threshold that raised ``label``."""
        logger.warning(
            "Static check fired",
            rule=label.value,
            sensor_id=sensor_id,
            value=value,
            threshold=threshold,
        )
        return label

    def skipped(self, rule: str, sensor_id: str | None, reason: str) -> CheckResult:
        """Log why ``rule`` could not run; NOT_CHECKED."""
        logger.debug(
            "Static check skipped", rule=rule, sensor_id=sensor_id, reason=reason
        )
        return CheckResult.NOT_CHECKED

    def sensor_of(self, readings: list[Sensor]) -> str | None:
        """The sensor_id of a window's readings; None for an empty window."""
        return readings[0].sensor_id if readings else None

    def out_of_range(
        self, readings: list[Sensor], value_range: tuple[float, float] | None
    ) -> Anomalies | CheckResult:
        """Anomalies.OUT_OF_RANGE: a finite value lies outside the sensor's range.

        ``value_range`` is what PreProcess.find_range returns. NOT_CHECKED without a
        finite range or without a finite value to judge.
        """
        rule, sensor_id = Anomalies.OUT_OF_RANGE.value, self.sensor_of(readings)
        if value_range is None:
            return self.skipped(rule, sensor_id, "no range")
        low, high = value_range
        # a NaN bound makes every comparison False: the check would pass anything
        if not (math.isfinite(low) and math.isfinite(high)):
            return self.skipped(rule, sensor_id, "non-finite range")
        values = [r.value for r in readings if math.isfinite(r.value)]
        if not values:
            return self.skipped(rule, sensor_id, "no finite value")
        outside = [value for value in values if value < low or value > high]
        if outside:
            return self.fired(
                Anomalies.OUT_OF_RANGE, sensor_id, outside[0], value_range
            )
        return CheckResult.PASS

    def sensor_is_down(
        self, readings: list[Sensor], window_end: datetime
    ) -> Anomalies | CheckResult:
        """Anomalies.SENSOR_IS_DOWN: no finite reading lately (down_after_seconds).

        Readings dated after the window end (plus future_tolerance_seconds) are
        ignored, so one wrong clock cannot hide an outage.
        """
        end = epoch_seconds(window_end)
        latest_allowed = end + self.settings.future_tolerance_seconds
        times = [
            epoch_seconds(reading.timestamp)
            for reading in readings
            if math.isfinite(reading.value)
        ]
        sensor_id, limit = self.sensor_of(readings), self.settings.down_after_seconds
        usable = [t for t in times if t <= latest_allowed]
        if len(usable) < len(times):
            logger.warning(
                "Ignored future-dated readings",
                sensor_id=sensor_id,
                ignored=len(times) - len(usable),
            )
        if not usable:  # nothing usable in the whole window
            return self.fired(Anomalies.SENSOR_IS_DOWN, sensor_id, None, limit)
        silence = end - max(usable)
        if silence > limit:
            return self.fired(Anomalies.SENSOR_IS_DOWN, sensor_id, silence, limit)
        return CheckResult.PASS

    def feature(self, features: dict[str, float], key: str) -> float | None:
        """A finite number from the row; None if missing, NaN or not a number."""
        value = features.get(key)
        if isinstance(value, int | float) and math.isfinite(value):
            return float(value)
        return None

    def spoofed_data(
        self, sensor_id: str, features: dict[str, float]
    ) -> Anomalies | CheckResult:
        """Anomalies.SPOOFED_DATA: the sensor's mean sits far from its neighbours'.

        ``features`` is the sensor's row from PreProcess. NOT_CHECKED without a range,
        with too few neighbours, or when a feature it reads is missing or not finite.
        """
        # without a range each sensor is z-scored inside its own window, so every
        # mean is about 0 and the delta says nothing
        rule = Anomalies.SPOOFED_DATA.value
        if self.feature(features, "has_range") != 1.0:
            return self.skipped(rule, sensor_id, "no range")
        neighbors = self.feature(features, "neighbor_count")
        if neighbors is None or neighbors < self.settings.min_neighbors:
            return self.skipped(rule, sensor_id, "too few neighbours")
        delta = self.feature(features, "spatial_delta_mean")
        if delta is None:  # a NaN delta would compare False and look clean
            return self.skipped(rule, sensor_id, "no finite spatial delta")
        # shortcut: a fixed delta catches gross spoofs only, move spoofed_data into the
        # model's classes if subtle ones matter
        if abs(delta) > self.settings.spoof_delta:
            return self.fired(
                Anomalies.SPOOFED_DATA, sensor_id, delta, self.settings.spoof_delta
            )
        return CheckResult.PASS
