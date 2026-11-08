"""Baseline anomaly model: gradient-boosted trees on one sensor's window features."""

import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
from xgboost import XGBClassifier

from guardian_layer.pipelines.denoise import cleaning_setup
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.utils.config import get_settings
from guardian_layer.utils.schema import Anomalies


def trained_cleaning(keys: tuple[str, ...]) -> dict[str, Any] | None:
    """The cleaning (pipelines/denoise.py) the settings apply now to the features a
    model with these columns reads: the short-window setup, the long-window one, or
    None for a model that reads no cleaned feature (section 4's)."""
    settings = get_settings()
    if set(PreProcess().short_keys()) <= set(keys):
        cfg = settings.preprocess
        return cleaning_setup(cfg.clean_method, cfg.kalman)
    if set(settings.preprocess.long_keys) <= set(keys):
        long = settings.long_window
        return cleaning_setup(long.clean_method, long.kalman)
    return None


Array = npt.NDArray[np.float64]
BoolArray = npt.NDArray[np.bool_]
IntArray = npt.NDArray[np.int64]


class BaselineModel:
    """Multiclass classifier: class 0 = normal, class i = classes[i - 1]."""

    def __init__(
        self,
        classifier: XGBClassifier,
        method: str,
        threshold: float,
        keys: tuple[str, ...],
        classes: tuple[Anomalies, ...],
        rate_thresholds: dict[float, float] | None = None,
        window_seconds: float | None = None,
    ) -> None:
        self.classifier = classifier
        self.method = method
        self.threshold = threshold  # used when no rate threshold applies
        self.keys = keys  # the feature columns, keys PreProcess writes
        self.classes = classes  # the anomaly labels the model can name, in order
        # readings per second -> threshold; copied so callers can't change it
        self.rate_thresholds = dict(rate_thresholds or {})
        self.window_seconds = window_seconds  # the window the rates are counted in

    @classmethod
    def fit(
        cls,
        features: Array,
        labels: IntArray | BoolArray,
        method: str,
        threshold: float,
        keys: tuple[str, ...],
        classes: tuple[Anomalies, ...],
        seed: int = 0,
        params: dict[str, Any] | None = None,
    ) -> "BaselineModel":
        """Train on rows whose columns are ``keys`` (keys PreProcess writes).

        ``labels`` holds one class index per row: 0 for normal, i for classes[i - 1].
        ``params`` are XGBoost settings other than its defaults.
        """
        if features.shape[1] != len(keys):
            raise ValueError("features must have one column per key")
        class_count = len(classes) + 1  # plus normal
        indices = labels.astype(np.int64)
        # checked here: XGBoost would train on fewer classes without complaining, and
        # every later class index would then point at the wrong label
        if set(np.unique(indices).tolist()) != set(range(class_count)):
            raise ValueError(
                f"training labels must hold every class from 0 to {class_count - 1}"
            )
        # balanced weights: each class counts the same in total (faults are rare)
        counts = np.bincount(indices, minlength=class_count)
        weights = len(indices) / (class_count * counts[indices])
        classifier = XGBClassifier(random_state=seed, **(params or {})).fit(
            features, indices, sample_weight=weights
        )
        return cls(classifier, method, threshold, keys, classes)

    def predict(self, features: Array) -> tuple[Array, IntArray]:
        """Per row: the anomaly score and the most likely anomaly class index.

        The score is 1 - P(normal) (0-1; not calibrated, classes are reweighted). The
        class index is 1-based (classes[i - 1]) and never 0: it answers "which anomaly,
        if it is one", and the threshold on the score decides whether it is one.
        """
        probabilities = np.asarray(
            self.classifier.predict_proba(features), dtype=np.float64
        )
        scores = 1.0 - probabilities[:, 0]
        classes = probabilities[:, 1:].argmax(axis=1).astype(np.int64) + 1
        return scores, classes

    def threshold_for(self, sample_count: float) -> float:
        """The threshold of a window with this many readings: the rate threshold
        nearest its readings per second, else the single threshold."""
        if not self.rate_thresholds or not self.window_seconds or sample_count <= 0:
            return self.threshold
        rate = sample_count / self.window_seconds
        # rates are compared as ratios: 7.5/s is nearer 10/s than 1/s
        distance = {key: abs(math.log(rate / key)) for key in self.rate_thresholds}
        return self.rate_thresholds[min(distance, key=distance.__getitem__)]

    def score(self, features: Array) -> Array:
        """Anomaly score per row, 1 - P(normal)."""
        return self.predict(features)[0]

    def classify(self, features: Array) -> IntArray:
        """Most likely anomaly class index per row (1-based: classes[i - 1])."""
        return self.predict(features)[1]

    def save(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        files = get_settings().inference
        self.classifier.save_model(directory / files.model_file)
        meta = {
            "method": self.method,
            "threshold": self.threshold,
            "keys": list(self.keys),
            "classes": list(self.classes),
            "rate_thresholds": {
                str(rate): t for rate, t in self.rate_thresholds.items()
            },
            "window_seconds": self.window_seconds,
        }
        # the cleaning in force when saved: the model must be trained under it (#66)
        cleaning = trained_cleaning(self.keys)
        if cleaning is not None:
            meta["cleaning"] = cleaning
        (directory / files.meta_file).write_text(json.dumps(meta, indent=2))

    @classmethod
    def load(cls, directory: Path) -> "BaselineModel":
        files = get_settings().inference
        meta = json.loads((directory / files.meta_file).read_text())
        # the saved columns, as long as PreProcess still writes each of them: the
        # short-window model mixes feature_keys with the short-window keys
        keys = tuple(meta["keys"])
        unknown = sorted(set(keys) - PreProcess().known_keys())
        if unknown:
            raise ValueError(
                f"model was trained on features PreProcess no longer writes: "
                f"{unknown}; retrain it"
            )
        # a short- or long-window model scores cleaned features; a meta from before
        # #66 has no cleaning and was trained on the raw series
        current = trained_cleaning(keys)
        if current is not None:
            saved = meta.get("cleaning", {"clean_method": "none"})
            if saved != current:
                raise ValueError(
                    f"model was trained with cleaning {saved}, the settings now apply "
                    f"{current}; retrain it"
                )
        # .get: a model saved before #55 has no classes and is binary
        if meta.get("classes") != list(files.classes):
            raise ValueError(
                "model was trained on different classes than settings.yaml lists "
                "now; retrain it"
            )
        classifier = XGBClassifier()
        classifier.load_model(directory / files.model_file)
        # .get: a model saved before the rate thresholds has a single threshold
        rates = {float(rate): t for rate, t in meta.get("rate_thresholds", {}).items()}
        return cls(
            classifier,
            meta["method"],
            meta["threshold"],
            keys,
            files.classes,
            rates,
            meta.get("window_seconds"),
        )
