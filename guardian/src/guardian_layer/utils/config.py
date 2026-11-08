"""Settings for the AI pipeline, read only from config/settings.yaml.

The defaults below equal the shipped file, so tests need no file. Set
GUARDIAN_CONFIG to read another file (it must exist); environment variables
for the fields themselves are not read.
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from guardian_layer.models.sensor import Anomalies
from guardian_layer.utils.helpers import find_project_root

# the keys PreProcess actually writes; the YAML lists must equal these exactly (order
# too), or the model's feature columns would change silently
EXPECTED_KEYS: dict[str, Any] = {
    "method_keys": {
        "none": (),
        "fft": ("fft_low", "fft_mid", "fft_high", "fft_peak"),
        "haar": (
            "haar_d1_energy",
            "haar_d2_energy",
            "haar_d3_energy",
            "haar_d1_max_abs",
        ),
        "db4": ("db4_d1_energy", "db4_d2_energy", "db4_d3_energy", "db4_d1_max_abs"),
        "ssa": ("ssa_trend", "ssa_oscillation", "ssa_noise", "ssa_first_ratio"),
    },
    "counter_keys": (
        "duplicate_count",
        "out_of_order_count",
        "replay_count",
        "packet_gap_max",
    ),
    "value_keys": (
        "sample_count",
        "has_range",
        "mean",
        "variance",
        "rolling_variance_max",
        "rate_of_change_mean_abs",
        "rate_of_change_max_abs",
        "flatline_ratio",
    ),
    "spatial_keys": ("spatial_delta_mean", "spatial_delta_last", "neighbor_count"),
    # the short-window model's own columns (research/short_window/results.md 4.2)
    "robust_keys": (
        "rz_variance",
        "rz_rolling_variance_max",
        "rz_rate_of_change_mean_abs",
        "rz_rate_of_change_max_abs",
        "rz_flatline_ratio",
    ),
    "shape_keys": (
        "max_abs_step",
        "max_abs_level",
        "sign_flip_share",
        "diff_autocorr",
        "spectral_peak_share",
        "diff_kurtosis",
        "rolling_variance_ratio",
    ),
    "native_keys": (
        "n_haar_d1_energy",
        "n_haar_d2_energy",
        "n_haar_d3_energy",
        "n_haar_d1_max_abs",
    ),
    # the long-window model's columns, on the time grid (pipelines/long_preprocess.py):
    # the robust z value features, shape, decoupling, the 3 long-only features and the
    # filled grid points
    "long_keys": (
        "lw_rz_variance",
        "lw_rz_rolling_variance_max",
        "lw_rz_rate_of_change_mean_abs",
        "lw_rz_rate_of_change_max_abs",
        "lw_rz_flatline_ratio",
        "lw_max_abs_step",
        "lw_max_abs_level",
        "lw_sign_flip_share",
        "lw_diff_autocorr",
        "lw_spectral_peak_share",
        "lw_diff_kurtosis",
        "lw_rolling_variance_ratio",
        "lw_haar_d1_energy",
        "lw_haar_d2_energy",
        "lw_haar_d3_energy",
        "lw_haar_d1_max_abs",
        "lw_trend_scatter_ratio",
        "lw_flat_run_share",
        "lw_neighbour_slope_delta",
        "lw_neighbor_count",
        "lw_grid_points",
    ),
}


# the noise-cleaning choices (#66); pipelines/denoise.py's registry holds the same
# method names (a test checks it)
CLEAN_METHODS = ("none", "kalman")
KALMAN_MODES = ("filter", "smoother")
KALMAN_NOISES = ("fixed", "estimated")


class KalmanSettings(BaseModel):
    """The Kalman cleaning of one sensor's series (pipelines/denoise.py)."""

    model_config = ConfigDict(extra="forbid")

    # filter: forward only; smoother: also uses the readings after each point
    mode: str = "filter"
    # fixed: measurement noise from the window, process noise = process_ratio x it;
    # estimated: both noises from the window's one- and two-step changes
    noise: str = "fixed"
    process_ratio: FiniteFloat = Field(default=0.1, gt=0)
    # process noise multiplied by this: smaller smooths more
    process_scale: FiniteFloat = Field(default=1.0, gt=0)
    # least process noise, as a share of the measurement noise (estimated noise):
    # white noise gives a process noise of about 0, which would flatten the window
    process_floor: FiniteFloat = Field(default=1e-3, gt=0)
    # a reading this many prediction sds away is taken as it is (spikes and steps pass
    # through); None: every reading is smoothed
    jump_sigmas: FiniteFloat | None = Field(default=None, gt=0)
    # a run of equal readings is a stuck value or a quantised sensor, never noise:
    # it comes out as it is, so a stuck stretch does not become a slow curve
    keep_repeats: bool = True

    @model_validator(mode="after")
    def check_names(self) -> Self:
        if self.mode not in KALMAN_MODES:
            raise ValueError(f"mode {self.mode!r} is unknown")
        if self.noise not in KALMAN_NOISES:
            raise ValueError(f"noise {self.noise!r} is unknown")
        return self


class PreprocessSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")  # a misspelled key must not be ignored

    default_method: str = "none"

    # noise cleaning of each sensor's series before the short-window features (#66):
    # the setup research section 11 picked (best mean PR-AUC on unseen sources)
    clean_method: str = "kalman"
    kalman: KalmanSettings = KalmanSettings(mode="smoother", jump_sigmas=4.0)

    # decoupling
    min_samples: int = 4  # fewer readings than this give all-zero decoupling features
    haar_levels: int = 3  # also used for db4
    method_keys: dict[str, tuple[str, ...]] = EXPECTED_KEYS["method_keys"].copy()

    # deduplication
    counter_keys: tuple[str, ...] = EXPECTED_KEYS["counter_keys"]
    # packet_order this far behind the highest seen is reordering; further is a replay
    out_of_order_tolerance: int = 2
    # packet_order this far from the sensor's median in the window is corrupt, not a
    # reference; 1 packet/s gives ~20 per 20 s window, so 1000 is generous
    max_jump: int = 1000

    # value features
    value_keys: tuple[str, ...] = EXPECTED_KEYS["value_keys"]
    rolling_samples: int = 5  # sub-window length for rolling variance
    # the window length: a replay without packet_order (or a bad clock) can carry a
    # timestamp years old, and a 1 s grid back to it would need gigabytes
    max_span_seconds: int = 20

    # spatial deltas
    spatial_keys: tuple[str, ...] = EXPECTED_KEYS["spatial_keys"]
    neighbors: int = Field(default=5, gt=0)  # k nearest, from the report's k-NN (k = 5)
    earth_radius_km: float = 6371.0
    # rows of the distance matrix per numpy call; 256 x 10,000 floats is ~20 MB per
    # array, ~100 MB peak with the temporaries of one block
    spatial_chunk_rows: int = Field(default=256, gt=0)

    # short-window features: robust z value features, shape, decoupling at the
    # sensor's own rate
    robust_keys: tuple[str, ...] = EXPECTED_KEYS["robust_keys"]
    shape_keys: tuple[str, ...] = EXPECTED_KEYS["shape_keys"]
    native_method: str = "haar"  # the generalization winner (results.md 4.2)
    native_keys: tuple[str, ...] = EXPECTED_KEYS["native_keys"]
    ratio_cap: float = 1e6  # rolling_variance_ratio when the median variance is 0
    # most native-grid points per reading: timestamps a burst sends microseconds apart
    # must not give a grid of millions of points (a regular series needs about 1)
    native_grid_per_reading: int = Field(default=4, gt=0)
    # the long-window model's columns (#96)
    long_keys: tuple[str, ...] = EXPECTED_KEYS["long_keys"]

    @model_validator(mode="after")
    def check_methods(self) -> Self:
        for field, expected in EXPECTED_KEYS.items():
            if getattr(self, field) != expected:
                raise ValueError(f"{field} must equal the keys PreProcess writes")
        if self.default_method not in self.method_keys:
            raise ValueError(f"default_method {self.default_method!r} is unknown")
        if self.clean_method not in CLEAN_METHODS:
            raise ValueError(f"clean_method {self.clean_method!r} is unknown")
        native = self.method_keys.get(self.native_method)
        if native is None or self.native_keys != tuple(f"n_{k}" for k in native):
            raise ValueError("native_keys must be n_ + the native_method's keys")
        return self


class StaticCheckSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # silence before the window end that counts as down (half the 20 s window)
    down_after_seconds: float = 60.0
    # distance from the neighbours' mean in range-scaled units (0.2 = 20 % of the range)
    spoof_delta: float = 0.2
    min_neighbors: int = 3  # fewer neighbours than this skip the spoof check
    # readings dated later than the window end plus this are ignored: one wrong or
    # spoofed clock must not make a silent sensor look alive (small skew is normal)
    future_tolerance_seconds: float = 2.0


class InferenceSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_file: str = "model.json"  # XGBoost's own format: no pickle, safe to load
    meta_file: str = "meta.json"
    default_threshold: float = 0.5  # same as the research (research/general_model)
    # readings per second from which a sensor goes to the short-window model
    short_window_min_rate: float = Field(default=0.5, gt=0)
    # anomaly classes the model names, in order; class index 0 is always "normal"
    classes: tuple[Anomalies, ...] = (
        Anomalies.SENSOR_MALFUNCTION,
        Anomalies.NOT_SENSOR_DATA,
    )

    @model_validator(mode="after")
    def check_classes(self) -> Self:
        if not self.classes or len(set(self.classes)) != len(self.classes):
            raise ValueError("classes must list at least one label, each once")
        return self


class LongWindowSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # readings are averaged into buckets this long (never finer than the sensor's own
    # period), so a series has the same shape at any sampling rate
    grid_seconds: float = Field(default=300.0, gt=0)
    # the window length: 24 h kept within the false-alarm budget, 6 h did not
    # (research/long_window/results.md); add a length here to measure it again
    window_seconds: tuple[float, ...] = (86400.0,)
    # filled buckets a sensor needs for a verdict; interpolated gaps do not count
    min_grid_points: int = 6
    # the cleaning of each sensor's grid before the features (#66): the long model's
    # own setup, as a 5-minute mean holds far less noise than one reading; the one
    # research section 12 picked (best mean PR-AUC on unseen sources)
    clean_method: str = "kalman"
    kalman: KalmanSettings = KalmanSettings(
        mode="smoother", process_ratio=0.9, jump_sigmas=4.0
    )

    @model_validator(mode="after")
    def check_windows(self) -> Self:
        if self.clean_method not in CLEAN_METHODS:
            raise ValueError(f"clean_method {self.clean_method!r} is unknown")
        if not self.window_seconds:
            raise ValueError("window_seconds must list at least one length")
        if any(window <= self.grid_seconds for window in self.window_seconds):
            raise ValueError("every window_seconds must be longer than grid_seconds")
        return self


class ElasticsearchSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = ""
    username: str = "elastic"
    request_timeout_seconds: float = Field(default=5.0, gt=0)
    retry_attempts: int = Field(default=3, ge=0)
    retry_backoff_seconds: float = Field(default=0.5, ge=0)
    batch_size: int = Field(default=50, gt=0)
    batch_window_seconds: float = Field(default=0.05, ge=0)
    queue_size: int = Field(default=1000, gt=0)
    shutdown_timeout_seconds: float = Field(default=5.0, gt=0)
    error_log_interval_seconds: float = Field(default=30.0, ge=0)


class MCPSettings(BaseModel):
    url: str = Field(default="http://localhost:8000/sse", description="MCP server URL")
    timeout: float = Field(
        default=3.0, description="Timeout for MCP requests in seconds"
    )


class KafkaSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    bootstrap_servers: str = Field(
        default="localhost:29094", description="Kafka bootstrap servers"
    )
    topic_input: str = Field(
        default="telemetry-validated", description="Kafka topic for sensor data"
    )
    output_topic: str = Field(
        default="telemetry-dlq", description="Kafka topic for sensor data"
    )
    group_id: str = Field(
        default="guardian-processor", description="Kafka consumer group ID"
    )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="forbid")

    preprocess: PreprocessSettings = PreprocessSettings()
    static_check: StaticCheckSettings = StaticCheckSettings()
    inference: InferenceSettings = InferenceSettings()
    elasticsearch: ElasticsearchSettings = ElasticsearchSettings()
    long_window: LongWindowSettings = LongWindowSettings()
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    mcp: MCPSettings = Field(default_factory=MCPSettings)

    @model_validator(mode="after")
    def check_long_window(self) -> Self:
        # fewer grid points than the decoupling minimum give all-zero haar features
        if self.long_window.min_grid_points < self.preprocess.min_samples:
            raise ValueError("long_window.min_grid_points must be >= min_samples")
        return self

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        *args: PydanticBaseSettingsSource,
        **kwargs: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # YAML only: init arguments, env vars and dotenv are left out on purpose
        override = os.environ.get("GUARDIAN_CONFIG")
        if override:
            path = Path(override)
            # the YAML source skips a missing file; an explicitly named one is a mistake
            if not path.is_file():
                raise FileNotFoundError(f"GUARDIAN_CONFIG file not found: {path}")
        else:
            root = find_project_root()
            if root is None:  # no repository around (installed package): use defaults
                return ()
            path = root / "config" / "settings.yaml"
        # explicit: Windows would read the file in its own code page ("°" -> "Â°")
        return (
            YamlConfigSettingsSource(
                settings_cls, yaml_file=path, yaml_file_encoding="utf-8"
            ),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
