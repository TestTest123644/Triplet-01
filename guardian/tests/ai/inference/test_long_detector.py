from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from guardian_layer.ai.inference import BaselineModel, LongAnomalyDetector
from guardian_layer.pipelines.decompose import Decompose
from guardian_layer.pipelines.long_preprocess import (
    LongPreProcess,
    process_long_window,
)
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.utils.config import LongWindowSettings, get_settings
from guardian_layer.utils.schema import Sensor

LONG_KEYS = get_settings().preprocess.long_keys
CLASSES = get_settings().inference.classes


def fit_on(keys: tuple[str, ...], seed: int) -> BaselineModel:
    rng = np.random.default_rng(seed)
    features = rng.normal(0.0, 0.1, (600, len(keys)))
    labels = rng.choice(3, size=600, p=(0.6, 0.2, 0.2))
    features[labels == 1, 0] += 5.0
    features[labels == 2, 1] += 5.0
    fitted = BaselineModel.fit(features, labels, "haar", 0.5, keys, CLASSES)
    return BaselineModel(
        fitted.classifier, "haar", 0.5, keys, CLASSES, window_seconds=21600.0
    )


@pytest.fixture(scope="module")
def long() -> BaselineModel:
    return fit_on(LONG_KEYS, 0)


@pytest.fixture(scope="module")
def short_keys_model() -> BaselineModel:
    return fit_on(PreProcess().short_keys(), 1)


def long_row(grid_points: float = 72.0) -> dict[str, float]:
    row = dict.fromkeys(LONG_KEYS, 0.0)
    row["lw_grid_points"] = grid_points
    row["sample_count"] = 72.0
    return row


def test_one_verdict_per_sensor_with_enough_points(long):
    rows = {"a": long_row(), "b": long_row()}
    assert set(LongAnomalyDetector(long, 6).predict_features(rows)) == {"a", "b"}


def test_a_sensor_below_min_grid_points_gets_no_verdict(long):
    rows = {"a": long_row(), "b": long_row(), "few": long_row(grid_points=5.0)}
    assert set(LongAnomalyDetector(long, 6).predict_features(rows)) == {"a", "b"}


def test_nothing_scoreable_gives_an_empty_result(long):
    detector = LongAnomalyDetector(long, 6)
    assert detector.predict_features({}) == {}
    assert detector.predict_features({"few": long_row(grid_points=1.0)}) == {}


def test_a_model_on_other_keys_is_refused(short_keys_model):
    with pytest.raises(ValueError, match="long_keys"):
        LongAnomalyDetector(short_keys_model, 6)


def test_from_directory_matches_in_memory(long, tmp_path):
    long.save(tmp_path)
    rows = {"a": long_row(), "b": long_row(grid_points=72.0)}
    rows["b"]["lw_rz_variance"] = 5.0
    assert LongAnomalyDetector.from_directory(tmp_path).predict_features(
        rows
    ) == LongAnomalyDetector(long, 6).predict_features(rows)


START = datetime(2026, 10, 2, tzinfo=UTC)


def reading_at(sensor_id: str, value: float, seconds: float, order: int) -> Sensor:
    """One reading ``seconds`` after 2026-10-02T00:00Z, sent ``order``-th."""
    return Sensor.model_validate(
        {
            "sensor_id": sensor_id,
            "cluster_id": "temp",
            "value": value,
            "unit": "C",
            "timestamp": (START + timedelta(seconds=seconds)).isoformat(),
            "metadata": {"packet_order": order, "type": "temp"},
        }
    )


def test_readings_to_verdicts(long):
    rng = np.random.default_rng(0)
    window = [
        reading_at(sensor_id, float(rng.normal()), minute * 60.0, minute + 1)
        for sensor_id in ("a", "b")
        for minute in range(360)
    ]
    window.append(reading_at("one", 1.0, 0.0, 1))  # one grid point: no verdict
    long_preprocess = LongPreProcess(PreProcess(), Decompose(), LongWindowSettings())
    rows = process_long_window(
        long_preprocess, PreProcess(), Decompose(), "none", window
    )
    verdicts = LongAnomalyDetector(long, 6).predict_features(rows)
    assert set(verdicts) == {"a", "b"}
