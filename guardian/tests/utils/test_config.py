import pytest
from pydantic import ValidationError

from guardian_layer.utils.config import (
    EXPECTED_KEYS,
    ElasticsearchSettings,
    InferenceSettings,
    KalmanSettings,
    LongWindowSettings,
    PreprocessSettings,
    Settings,
    StaticCheckSettings,
)
from guardian_layer.utils.schema import Anomalies


@pytest.fixture(autouse=True)
def no_config_override(monkeypatch):
    monkeypatch.delenv("GUARDIAN_CONFIG", raising=False)


def test_defaults_match_the_former_constants():
    settings = Settings()
    assert settings.preprocess.default_method == "none"
    assert settings.preprocess.min_samples == 4
    assert settings.preprocess.neighbors == 5
    assert settings.preprocess.spatial_chunk_rows == 256
    assert settings.preprocess.method_keys["fft"][0] == "fft_low"
    assert settings.inference.model_file == "model.json"
    assert settings.inference.default_threshold == 0.5


def test_the_shipped_yaml_equals_the_class_defaults():
    # Settings() reads config/settings.yaml; the classes alone are the defaults
    assert Settings().preprocess == PreprocessSettings()
    assert Settings().inference == InferenceSettings()
    assert Settings().static_check == StaticCheckSettings()
    assert Settings().elasticsearch == ElasticsearchSettings()
    assert Settings().long_window == LongWindowSettings()


def test_yaml_overrides_a_value(tmp_path, monkeypatch):
    path = tmp_path / "settings.yaml"
    path.write_text("preprocess:\n  neighbors: 3\n")
    monkeypatch.setenv("GUARDIAN_CONFIG", str(path))
    settings = Settings()
    assert settings.preprocess.neighbors == 3
    assert settings.preprocess.min_samples == 4  # untouched keys keep the default


def test_missing_file_named_by_env_var_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("GUARDIAN_CONFIG", str(tmp_path / "nope.yaml"))
    with pytest.raises(FileNotFoundError):
        Settings()


def test_env_vars_for_fields_are_ignored(monkeypatch):
    monkeypatch.setenv("PREPROCESS__NEIGHBORS", "9")
    monkeypatch.setenv("PREPROCESS", '{"neighbors": 9}')
    monkeypatch.setenv("NEIGHBORS", "9")
    assert Settings().preprocess.neighbors == 5


def test_method_keys_must_include_none_and_fft_haar_db4_ssa():
    with pytest.raises(ValidationError, match="method_keys"):
        PreprocessSettings(method_keys={"none": (), "fft": ("fft_low",)})


def test_default_method_must_be_a_known_method():
    with pytest.raises(ValidationError, match="default_method"):
        PreprocessSettings(default_method="wavelet")


def renamed(keys):
    return (*keys[:-1], "renamed")


@pytest.mark.parametrize("field", ["value_keys", "counter_keys", "spatial_keys"])
@pytest.mark.parametrize("change", [renamed, lambda k: k[:-1], lambda k: k[::-1]])
def test_key_lists_must_equal_the_keys_the_code_writes(field, change):
    default = getattr(PreprocessSettings(), field)
    with pytest.raises(ValidationError, match=field):
        PreprocessSettings(**{field: change(default)})


@pytest.mark.parametrize("change", [renamed, lambda k: k[:-1], lambda k: k[::-1]])
def test_method_keys_lists_must_equal_the_keys_the_code_writes(change):
    method_keys = dict(PreprocessSettings().method_keys)
    method_keys["fft"] = change(method_keys["fft"])
    with pytest.raises(ValidationError, match="method_keys"):
        PreprocessSettings(method_keys=method_keys)


def test_a_misspelled_nested_key_raises(tmp_path, monkeypatch):
    path = tmp_path / "settings.yaml"
    path.write_text("preprocess:\n  neighbours: 3\n")
    monkeypatch.setenv("GUARDIAN_CONFIG", str(path))
    with pytest.raises(ValidationError, match="neighbours"):
        Settings()
    with pytest.raises(ValidationError, match="threshold"):
        InferenceSettings(threshold=0.1)  # type: ignore[call-arg]


def test_without_a_project_root_the_defaults_apply(monkeypatch):
    monkeypatch.setattr("guardian_layer.utils.config.find_project_root", lambda: None)
    settings = Settings()
    assert settings.preprocess == PreprocessSettings()
    assert settings.inference == InferenceSettings()


def test_changing_an_instances_method_keys_leaves_the_defaults_alone():
    PreprocessSettings().method_keys["fft"] = ("changed",)
    assert PreprocessSettings().method_keys["fft"][0] == "fft_low"
    assert EXPECTED_KEYS["method_keys"]["fft"][0] == "fft_low"


def test_static_check_and_class_defaults():
    settings = Settings()
    assert settings.static_check.down_after_seconds == 60.0
    assert settings.static_check.spoof_delta == 0.2
    assert settings.static_check.min_neighbors == 3
    assert settings.static_check.future_tolerance_seconds == 2.0
    assert settings.inference.classes == (
        Anomalies.SENSOR_MALFUNCTION,
        Anomalies.NOT_SENSOR_DATA,
    )


@pytest.mark.parametrize(
    "classes",
    [
        ("not_a_label",),  # not an Anomalies value
        (),  # the model needs at least one anomaly class
        ("sensor_malfunction", "sensor_malfunction"),  # the same class twice
    ],
)
def test_classes_must_be_distinct_anomaly_labels(classes):
    with pytest.raises(ValidationError, match="classes"):
        InferenceSettings(classes=classes)


def test_max_jump_default():
    assert Settings().preprocess.max_jump == 1000


@pytest.mark.parametrize("field", ["neighbors", "spatial_chunk_rows"])
def test_spatial_counts_must_be_positive(field):
    # 0 neighbours would silently fall back to the whole cluster, 0 rows never ends
    with pytest.raises(ValidationError, match=field):
        PreprocessSettings(**{field: 0})


def test_yaml_is_read_as_utf8(tmp_path, monkeypatch):
    # Windows reads files in its own code page by default: "é" would become "Ã©"
    path = tmp_path / "settings.yaml"
    path.write_bytes('elasticsearch:\n  username: "é"\n'.encode())
    monkeypatch.setenv("GUARDIAN_CONFIG", str(path))
    assert Settings().elasticsearch.username == "é"


def test_short_window_keys_are_the_expected_ones():
    s = Settings().preprocess
    assert s.robust_keys == EXPECTED_KEYS["robust_keys"]
    assert s.shape_keys[0] == "max_abs_step" and len(s.shape_keys) == 7
    assert s.native_keys == tuple(f"n_{k}" for k in s.method_keys[s.native_method])


def test_native_keys_must_follow_the_native_method():
    assert PreprocessSettings(native_method="haar").native_keys[0] == "n_haar_d1_energy"
    with pytest.raises(ValidationError):
        PreprocessSettings(native_method="fft")  # the keys still say haar


def test_unknown_clean_method_raises():
    with pytest.raises(ValidationError, match="clean_method"):
        PreprocessSettings(clean_method="wiener")


def test_unknown_long_clean_method_raises():
    with pytest.raises(ValidationError, match="clean_method"):
        LongWindowSettings(clean_method="wiener")


@pytest.mark.parametrize("field", ["mode", "noise"])
def test_unknown_kalman_mode_or_noise_raises(field):
    with pytest.raises(ValidationError, match=field):
        KalmanSettings(**{field: "unknown"})


def test_short_window_min_rate_must_be_positive():
    assert InferenceSettings(short_window_min_rate=0.5).short_window_min_rate == 0.5
    with pytest.raises(ValidationError):
        InferenceSettings(short_window_min_rate=0.0)


def test_long_keys_are_checked_like_the_other_groups():
    assert PreprocessSettings().long_keys == EXPECTED_KEYS["long_keys"]
    with pytest.raises(ValidationError):
        PreprocessSettings(long_keys=("lw_grid_points",))


def test_every_long_window_is_longer_than_the_grid_step():
    with pytest.raises(ValidationError):
        LongWindowSettings(grid_seconds=300.0, window_seconds=(300.0,))


def test_long_window_lengths_are_required():
    with pytest.raises(ValidationError):
        LongWindowSettings(window_seconds=())


def test_min_grid_points_is_at_least_the_decoupling_minimum(tmp_path, monkeypatch):
    path = tmp_path / "settings.yaml"
    # preprocess.min_samples is 4: fewer grid points give all-zero decoupling
    path.write_text("long_window:\n  min_grid_points: 3\n")
    monkeypatch.setenv("GUARDIAN_CONFIG", str(path))
    with pytest.raises(ValidationError, match="min_grid_points"):
        Settings()
