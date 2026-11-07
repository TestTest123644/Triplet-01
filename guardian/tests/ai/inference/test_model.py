import json

import numpy as np
import pytest

from guardian_layer.ai.inference.model import BaselineModel
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.utils.config import get_settings
from guardian_layer.utils.schema import Anomalies

KEYS = PreProcess().feature_keys("none")
CLASSES = get_settings().inference.classes
VARIANCE = KEYS.index("variance")
FLATLINE = KEYS.index("flatline_ratio")


def make_data(rows: int = 600, seed: int = 0):
    """Class 1 rows have a high variance feature, class 2 rows a high flatline one."""
    rng = np.random.default_rng(seed)
    features = rng.normal(0.0, 0.1, (rows, len(KEYS)))
    labels = rng.choice(3, size=rows, p=(0.6, 0.2, 0.2))
    features[labels == 1, VARIANCE] += 5.0
    features[labels == 2, FLATLINE] += 5.0
    return features, labels


def fit(features, labels, method="none", threshold=0.5, keys=KEYS, classes=CLASSES):
    return BaselineModel.fit(features, labels, method, threshold, keys, classes)


def test_fit_keeps_the_classes_it_is_given():
    features, labels = make_data()
    classes = (Anomalies.NOT_SENSOR_DATA, Anomalies.SENSOR_MALFUNCTION)
    assert fit(features, labels, classes=classes).classes == classes


def test_fit_separates_anomalies():
    features, labels = make_data()
    model = fit(features, labels)
    scores = model.score(features)
    assert scores[labels > 0].min() > model.threshold > scores[labels == 0].max()


def test_classify_names_the_anomaly_class():
    features, labels = make_data()
    model = fit(features, labels)
    faulty = labels > 0
    np.testing.assert_array_equal(model.classify(features)[faulty], labels[faulty])


def test_classify_never_answers_normal():
    features, labels = make_data()
    model = fit(features, labels)
    assert set(model.classify(features)) <= {1, 2}


def test_predict_gives_score_and_class_together():
    features, labels = make_data()
    model = fit(features, labels)
    scores, classes = model.predict(features)
    np.testing.assert_allclose(scores, model.score(features))
    np.testing.assert_array_equal(classes, model.classify(features))


def test_save_load_round_trip_gives_same_answers(tmp_path):
    features, labels = make_data()
    model = fit(features, labels, threshold=0.3)
    model.save(tmp_path)
    loaded = BaselineModel.load(tmp_path)
    assert loaded.threshold == 0.3
    assert loaded.method == "none"
    assert loaded.classes == model.classes
    np.testing.assert_allclose(loaded.score(features), model.score(features))
    np.testing.assert_array_equal(loaded.classify(features), model.classify(features))


@pytest.mark.parametrize(
    "labels",
    [
        np.zeros(60, int),  # normal only
        np.arange(60) % 2,  # class 2 missing
        np.arange(60) % 3 + 1,  # normal missing, and 3 is not a class
        np.arange(60) % 3 - 1,  # negative index
        np.arange(60) % 2 == 0,  # the old boolean labels
    ],
)
def test_fit_needs_every_class(labels):
    with pytest.raises(ValueError, match="every class"):
        fit(np.zeros((60, len(KEYS))), labels)


def test_fit_rejects_wrong_column_count():
    _, labels = make_data(60)
    with pytest.raises(ValueError, match="column"):
        fit(np.zeros((60, 5)), labels)


def test_load_rejects_changed_feature_schema(tmp_path):
    features, labels = make_data()
    fit(features, labels).save(tmp_path)
    meta = tmp_path / "meta.json"
    meta.write_text(meta.read_text().replace('"variance"', '"renamed"'))
    with pytest.raises(ValueError, match="feature"):
        BaselineModel.load(tmp_path)


def test_load_rejects_a_changed_class_list(tmp_path):
    features, labels = make_data()
    fit(features, labels).save(tmp_path)
    meta = tmp_path / "meta.json"
    meta.write_text(meta.read_text().replace('"not_sensor_data"', '"spoofed_data"'))
    with pytest.raises(ValueError, match="classes"):
        BaselineModel.load(tmp_path)


def test_load_rejects_a_model_saved_before_classes_existed(tmp_path):
    features, labels = make_data()
    fit(features, labels).save(tmp_path)
    meta = tmp_path / "meta.json"
    content = json.loads(meta.read_text())
    del content["classes"]
    meta.write_text(json.dumps(content))
    with pytest.raises(ValueError, match="classes"):
        BaselineModel.load(tmp_path)


def test_loaded_keys_match_preprocess_feature_keys(tmp_path):
    keys = PreProcess().feature_keys("haar")
    features = np.random.default_rng(0).normal(0.0, 0.1, (60, len(keys)))
    labels = np.arange(60) % 3
    fit(features, labels, "haar", keys=keys).save(tmp_path)
    loaded = BaselineModel.load(tmp_path)
    assert loaded.keys == PreProcess().feature_keys("haar")


SHORT_KEYS = (*get_settings().preprocess.counter_keys, *PreProcess().short_keys())


@pytest.fixture(autouse=True)
def fresh_settings():
    """A test that switches the settings must not leave them cached for the next."""
    yield
    get_settings.cache_clear()


def use_cleaning(tmp_path, monkeypatch, method: str, long_method: str = "none") -> None:
    """Switch the short and long clean_method by pointing GUARDIAN_CONFIG at a small
    YAML."""
    path = tmp_path / f"settings_{method}_{long_method}.yaml"
    path.write_text(
        f"preprocess:\n  clean_method: {method}\n"
        f"long_window:\n  clean_method: {long_method}\n"
    )
    monkeypatch.setenv("GUARDIAN_CONFIG", str(path))
    get_settings.cache_clear()


LONG_KEYS = get_settings().preprocess.long_keys


def long_model():
    rng = np.random.default_rng(0)
    features = rng.normal(0.0, 0.1, (60, len(LONG_KEYS)))
    return fit(features, np.arange(60) % 3, keys=LONG_KEYS)


def short_model():
    rng = np.random.default_rng(0)
    features = rng.normal(0.0, 0.1, (60, len(SHORT_KEYS)))
    return fit(features, np.arange(60) % 3, keys=SHORT_KEYS)


def test_short_window_model_records_its_cleaning(tmp_path, monkeypatch):
    use_cleaning(tmp_path, monkeypatch, "kalman")
    short_model().save(tmp_path / "model")
    meta = json.loads((tmp_path / "model" / "meta.json").read_text())
    assert meta["cleaning"]["clean_method"] == "kalman"
    # the shipped setup (research section 11's pick), recorded whole
    assert meta["cleaning"]["kalman"] == get_settings().preprocess.kalman.model_dump()
    assert meta["cleaning"]["kalman"]["mode"] == "smoother"


def test_load_rejects_a_model_trained_with_other_cleaning(tmp_path, monkeypatch):
    use_cleaning(tmp_path, monkeypatch, "kalman")
    short_model().save(tmp_path / "model")
    use_cleaning(tmp_path, monkeypatch, "none")
    with pytest.raises(ValueError, match="cleaning"):
        BaselineModel.load(tmp_path / "model")


def test_meta_without_cleaning_counts_as_none(tmp_path, monkeypatch):
    use_cleaning(tmp_path, monkeypatch, "none")
    short_model().save(tmp_path / "model")
    meta = tmp_path / "model" / "meta.json"
    content = json.loads(meta.read_text())
    del content["cleaning"]  # a short-window model saved before #66
    meta.write_text(json.dumps(content))
    assert BaselineModel.load(tmp_path / "model").keys == SHORT_KEYS
    use_cleaning(tmp_path, monkeypatch, "kalman")
    with pytest.raises(ValueError, match="retrain"):
        BaselineModel.load(tmp_path / "model")


def test_section_4_model_loads_under_any_cleaning(tmp_path, monkeypatch):
    use_cleaning(tmp_path, monkeypatch, "none")
    fit(*make_data()).save(tmp_path / "model")
    assert "cleaning" not in json.loads((tmp_path / "model" / "meta.json").read_text())
    use_cleaning(tmp_path, monkeypatch, "kalman")
    assert BaselineModel.load(tmp_path / "model").keys == KEYS


def by_rate(threshold=0.9, rates=None, window_seconds=20.0):
    """A fitted model with a threshold per sampling rate (readings per second)."""
    rates = {1.0: 0.99, 10.0: 0.86} if rates is None else rates
    base = fit(*make_data())
    return BaselineModel(
        base.classifier,
        "none",
        threshold,
        KEYS,
        CLASSES,
        rate_thresholds=rates,
        window_seconds=window_seconds,
    )


def test_threshold_for_picks_the_nearest_rate():
    model = by_rate()
    assert model.threshold_for(20) == 0.99  # 1 reading/s in a 20 s window
    assert model.threshold_for(200) == 0.86  # 10 readings/s
    assert model.threshold_for(150) == 0.86  # 7.5/s: nearer 10 than 1 on a log scale
    assert model.threshold_for(0) == 0.9  # no reading: the single threshold


def test_without_rate_thresholds_the_single_threshold_is_used():
    assert fit(*make_data()).threshold_for(200) == 0.5
    assert by_rate(window_seconds=None).threshold_for(200) == 0.9


def test_rate_thresholds_survive_save_and_load(tmp_path):
    by_rate().save(tmp_path)
    loaded = BaselineModel.load(tmp_path)
    assert loaded.rate_thresholds == {1.0: 0.99, 10.0: 0.86}
    assert loaded.window_seconds == 20.0


def test_a_model_saved_without_rate_thresholds_still_loads(tmp_path):
    by_rate().save(tmp_path)
    meta = tmp_path / "meta.json"
    content = json.loads(meta.read_text())
    del content["rate_thresholds"], content["window_seconds"]
    meta.write_text(json.dumps(content))
    loaded = BaselineModel.load(tmp_path)
    assert loaded.threshold_for(200) == loaded.threshold == 0.9


def test_fit_passes_xgboost_settings():
    features, labels = make_data()
    model = BaselineModel.fit(
        features, labels, "none", 0.5, KEYS, CLASSES, params={"n_estimators": 7}
    )
    assert model.classifier.n_estimators == 7


def test_load_keeps_a_mixed_key_list(tmp_path):
    keys = (*get_settings().preprocess.counter_keys, *PreProcess().short_keys())
    features = np.random.default_rng(0).normal(0.0, 0.1, (60, len(keys)))
    fit(features, np.arange(60) % 3, keys=keys).save(tmp_path)
    assert BaselineModel.load(tmp_path).keys == keys


def test_load_rejects_an_unknown_key(tmp_path):
    fit(*make_data()).save(tmp_path)
    meta = tmp_path / get_settings().inference.meta_file
    content = json.loads(meta.read_text())
    content["keys"][0] = "gone"
    meta.write_text(json.dumps(content))
    with pytest.raises(ValueError, match="retrain"):
        BaselineModel.load(tmp_path)


def test_long_window_model_records_its_cleaning(tmp_path, monkeypatch):
    use_cleaning(tmp_path, monkeypatch, "none", "kalman")
    long_model().save(tmp_path / "model")
    meta = json.loads((tmp_path / "model" / "meta.json").read_text())
    assert meta["cleaning"]["clean_method"] == "kalman"
    assert "kalman" in meta["cleaning"]


def test_load_rejects_a_long_model_trained_with_other_cleaning(tmp_path, monkeypatch):
    use_cleaning(tmp_path, monkeypatch, "none", "kalman")
    long_model().save(tmp_path / "model")
    use_cleaning(tmp_path, monkeypatch, "none", "none")
    with pytest.raises(ValueError, match="retrain"):
        BaselineModel.load(tmp_path / "model")


def test_long_meta_without_cleaning_counts_as_none(tmp_path, monkeypatch):
    use_cleaning(tmp_path, monkeypatch, "none", "none")
    long_model().save(tmp_path / "model")
    meta = tmp_path / "model" / "meta.json"
    content = json.loads(meta.read_text())
    del content["cleaning"]  # a long-window model saved before #66
    meta.write_text(json.dumps(content))
    assert BaselineModel.load(tmp_path / "model").keys == LONG_KEYS
    use_cleaning(tmp_path, monkeypatch, "none", "kalman")
    with pytest.raises(ValueError, match="retrain"):
        BaselineModel.load(tmp_path / "model")


def test_short_cleaning_does_not_affect_a_long_model(tmp_path, monkeypatch):
    use_cleaning(tmp_path, monkeypatch, "none", "none")
    long_model().save(tmp_path / "model")
    use_cleaning(tmp_path, monkeypatch, "kalman", "none")
    assert BaselineModel.load(tmp_path / "model").keys == LONG_KEYS
