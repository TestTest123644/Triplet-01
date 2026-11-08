import numpy as np
import pytest

from guardian_layer.ai.inference import AnomalyDetector, BaselineModel
from guardian_layer.ai.inference.detector import model_verdicts
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.utils.config import get_settings
from guardian_layer.utils.schema import Anomalies, Verdict

KEYS = PreProcess().feature_keys("none")
CLASSES = get_settings().inference.classes
VARIANCE = KEYS.index("variance")
FLATLINE = KEYS.index("flatline_ratio")


@pytest.fixture(scope="module")
def model():
    rng = np.random.default_rng(0)
    features = rng.normal(0.0, 0.1, (600, len(KEYS)))
    labels = rng.choice(3, size=600, p=(0.6, 0.2, 0.2))
    features[labels == 1, VARIANCE] += 5.0
    features[labels == 2, FLATLINE] += 5.0
    return BaselineModel.fit(features, labels, "none", 0.5, KEYS, CLASSES)


@pytest.fixture(scope="module")
def short():
    """A model blind to variance (it learned flatline only), on 20 s windows: it and
    ``model`` disagree on a high-variance row."""
    rng = np.random.default_rng(1)
    features = rng.normal(0.0, 0.1, (600, len(KEYS)))
    labels = rng.choice(3, size=600, p=(0.6, 0.2, 0.2))
    features[labels == 1, FLATLINE] += 5.0
    features[labels == 2, FLATLINE] += 10.0
    fitted = BaselineModel.fit(features, labels, "none", 0.5, KEYS, CLASSES)
    return BaselineModel(
        fitted.classifier, "none", 0.5, KEYS, CLASSES, window_seconds=20.0
    )


def feature_row(
    variance: float = 0.0, flatline: float = 0.0, sample_count: float = 0.0
) -> dict[str, float]:
    row = dict.fromkeys(KEYS, 0.0)
    row["variance"] = variance
    row["flatline_ratio"] = flatline
    row["sample_count"] = sample_count
    return row


def one_model(model, rows):
    """Every row scored by this one model, whatever its rate."""
    return model_verdicts(model, rows, list(rows))


def test_model_verdicts_name_each_label(model):
    verdicts = one_model(
        model,
        {
            "fine": feature_row(),
            "broken": feature_row(variance=5.0),
            "fake": feature_row(flatline=5.0),
        },
    )
    assert verdicts["fine"].label is None
    assert verdicts["broken"].label == Anomalies.SENSOR_MALFUNCTION
    assert verdicts["fake"].label == Anomalies.NOT_SENSOR_DATA
    assert verdicts["broken"].score > 0.5 > verdicts["fine"].score


def test_anomalous_follows_the_label():
    assert not Verdict("a", 0.1, None).anomalous
    assert Verdict("a", 0.9, Anomalies.SENSOR_MALFUNCTION).anomalous


def test_a_score_above_the_threshold_always_gets_a_label(model):
    # with a threshold of 0 every row is anomalous, also the ones whose single most
    # likely class is "normal": they still get their most likely anomaly class
    lenient = BaselineModel(
        model.classifier, model.method, 0.0, model.keys, model.classes
    )
    verdict = one_model(lenient, {"fine": feature_row()})["fine"]
    assert verdict.anomalous
    assert verdict.label in lenient.classes


def test_predict_features_empty_window(short):
    assert AnomalyDetector(short).predict_features({}) == {}
    assert one_model(short, {}) == {}


def test_model_verdicts_reject_a_missing_feature(model):
    row = feature_row(0.0)
    del row["mean"]
    with pytest.raises(KeyError):
        one_model(model, {"a": row})


def test_each_sensor_is_judged_at_its_rate_threshold(model):
    by_rate = BaselineModel(
        model.classifier,
        "none",
        0.5,
        KEYS,
        CLASSES,
        rate_thresholds={1.0: 1.0, 10.0: -1.0},
        window_seconds=20.0,
    )
    verdicts = AnomalyDetector(by_rate).predict_features(
        {"slow": feature_row(sample_count=20), "fast": feature_row(sample_count=200)}
    )
    assert verdicts["slow"].label is None  # no score is above 1.0
    assert verdicts["fast"].label is not None  # every score is above -1.0


def score_of(model, row):
    return one_model(model, {"s": row})["s"].score


def test_fast_sensor_uses_the_short_model(model, short):
    fast = feature_row(variance=5.0, sample_count=20.0)  # 1 reading per second
    verdict = AnomalyDetector(short).predict_features({"s": fast})["s"]
    assert verdict.score == pytest.approx(score_of(short, fast))
    assert verdict.score != pytest.approx(score_of(model, fast))


def test_slow_sensors_get_no_verdict(short):
    rows = {
        "fast": feature_row(sample_count=20.0),
        "slow": feature_row(sample_count=2.0),  # 0.1 per second
    }
    assert set(AnomalyDetector(short).predict_features(rows)) == {"fast"}


def test_the_minimum_rate_itself_goes_short(short):
    row = feature_row(variance=5.0, sample_count=10.0)  # exactly 0.5 per second
    detector = AnomalyDetector(short, min_rate=0.5)
    assert detector.predict_features({"s": row})["s"].score == pytest.approx(
        score_of(short, row)
    )


def test_mixed_window_keeps_the_fast_sensors_in_order(short):
    rows = {
        "fast-1": feature_row(variance=5.0, sample_count=200.0),
        "slow": feature_row(variance=5.0, sample_count=1.0),
        "fast-2": feature_row(flatline=5.0, sample_count=20.0),
    }
    verdicts = AnomalyDetector(short).predict_features(rows)
    assert list(verdicts) == ["fast-1", "fast-2"]
    for sensor_id in verdicts:
        assert verdicts[sensor_id].score == pytest.approx(
            score_of(short, rows[sensor_id])
        )


def test_a_model_without_window_seconds_is_rejected(model):
    with pytest.raises(ValueError, match="window_seconds"):
        AnomalyDetector(model)


def test_from_directory_loads_the_short_model(short, tmp_path):
    short.save(tmp_path)
    rows = {"s": feature_row(flatline=5.0, sample_count=20.0)}
    assert AnomalyDetector.from_directory(tmp_path).predict_features(
        rows
    ) == AnomalyDetector(short).predict_features(rows)
