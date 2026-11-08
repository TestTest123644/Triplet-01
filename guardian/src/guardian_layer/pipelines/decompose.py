"""Signal decoupling for #9: split a prepared series into frequency components.

One class, ``Decompose``: each method turns a normalized, 1-second-grid series into
fixed keys (so one model can take any sensor type), and ``caller`` picks one by name.
Which method to keep was decided by the research notebook.
"""

import warnings

import numpy as np
import numpy.typing as npt
import pywt  # type: ignore[import-untyped]

from guardian_layer.utils.config import PreprocessSettings, get_settings

Array = npt.NDArray[np.float64]


class Decompose:
    """Decoupling features of one normalized, resampled series."""

    def __init__(self, settings: PreprocessSettings | None = None) -> None:
        self.settings = settings or get_settings().preprocess

    def caller(self, fn_name: str, values: Array) -> dict[str, float]:
        """Features of the series with the named method (zeros when it is too short)."""
        match fn_name:
            case "none":
                return dict.fromkeys(self.settings.method_keys["none"], 0.0)
            case "fft" | "haar" | "db4" | "ssa":
                if len(values) < self.settings.min_samples:
                    return dict.fromkeys(self.settings.method_keys[fn_name], 0.0)
                return getattr(self, fn_name)(values)  # type: ignore[no-any-return]
            case _:
                raise ValueError(f"unknown decoupling method: {fn_name}")

    def fft(self, values: Array) -> dict[str, float]:
        """Energy share in low/mid/high thirds of the spectrum, and the peak."""
        features = dict.fromkeys(self.settings.method_keys["fft"], 0.0)
        if np.ptp(values) == 0:  # constant (mean removal can leave ~1e-17 noise)
            return features
        energy = np.abs(np.fft.rfft(values - values.mean()))[1:] ** 2  # drop DC
        total = energy.sum()
        low, mid, high = np.array_split(energy, 3)
        features["fft_low"] = float(low.sum() / total)
        features["fft_mid"] = float(mid.sum() / total)
        features["fft_high"] = float(high.sum() / total)
        # bin i+1 of n/2 is the fraction of the Nyquist frequency
        features["fft_peak"] = float((energy.argmax() + 1) / (len(values) / 2))
        return features

    def haar(self, values: Array) -> dict[str, float]:
        """Haar wavelet detail energy per level, and the largest level-1 detail."""
        features = dict.fromkeys(self.settings.method_keys["haar"], 0.0)
        approx = values[-(2 ** int(np.log2(len(values)))) :]  # last 2^k samples
        for level in range(1, self.settings.haar_levels + 1):
            if len(approx) < 2:
                break
            pairs = approx.reshape(-1, 2)
            detail = (pairs[:, 0] - pairs[:, 1]) / np.sqrt(2)
            approx = (pairs[:, 0] + pairs[:, 1]) / np.sqrt(2)
            features[f"haar_d{level}_energy"] = float(np.mean(detail**2))
            if level == 1:
                features["haar_d1_max_abs"] = float(np.abs(detail).max())
        return features

    def db4(self, values: Array) -> dict[str, float]:
        """Daubechies-4 wavelet detail energy per level, and the level-1 maximum."""
        features = dict.fromkeys(self.settings.method_keys["db4"], 0.0)
        if np.ptp(values) == 0:  # constant: all details are zero
            return features
        window = values[-(2 ** int(np.log2(len(values)))) :]  # last 2^k samples
        levels = min(self.settings.haar_levels, int(np.log2(len(window))))
        # db4's filter is 8 long, so short windows exceed pywt's "safe" level; smooth
        # mode extrapolates the edges linearly, so a steady trend gives no edge
        # artefact
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message="Level value", category=UserWarning
            )
            coefficients = pywt.wavedec(window, "db4", mode="smooth", level=levels)
        details = coefficients[:0:-1]  # cD1 (finest) first
        for level, detail in enumerate(details, start=1):
            features[f"db4_d{level}_energy"] = float(np.mean(detail**2))
        features["db4_d1_max_abs"] = float(np.abs(details[0]).max())
        return features

    def ssa(self, values: Array) -> dict[str, float]:
        """Singular spectrum analysis: energy share of trend, oscillation and noise."""
        features = dict.fromkeys(self.settings.method_keys["ssa"], 0.0)
        if np.ptp(values) == 0:  # constant: nothing to decompose
            return features
        centered = values - values.mean()  # otherwise component 1 is just the level
        trajectory = np.lib.stride_tricks.sliding_window_view(
            centered, len(values) // 2
        )
        sigma = np.linalg.svd(trajectory, compute_uv=False)
        energy = sigma**2
        total = energy.sum()
        features["ssa_trend"] = float(energy[0] / total)
        features["ssa_oscillation"] = float(energy[1:3].sum() / total)
        features["ssa_noise"] = float(energy[3:].sum() / total)
        features["ssa_first_ratio"] = float(sigma[0] / sigma.sum())
        return features
