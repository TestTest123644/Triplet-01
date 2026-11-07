import math

import pytest
from pydantic import ValidationError

from guardian_layer.utils import dataset_config
from guardian_layer.utils.dataset_config import (
    DatasetSettings,
    InjectionSettings,
    get_dataset_settings,
    read_dataset_settings,
)


@pytest.fixture(autouse=True)
def no_override(monkeypatch):
    monkeypatch.delenv("GUARDIAN_DATASETS_CONFIG", raising=False)


def test_the_shipped_yaml_equals_the_class_defaults():
    # this reads config/datasets.yaml; the field defaults are the classes
    loaded = read_dataset_settings()
    for name, field in DatasetSettings.model_fields.items():
        assert getattr(loaded, name) == field.default, name


def test_a_sampling_period_of_zero_is_rejected():
    # 0 would divide by zero in common.in_sampled_block
    with pytest.raises(ValidationError):
        InjectionSettings(sample_every_hours={"x": 0})


@pytest.mark.parametrize("bad", [0.0, -1.0, math.inf, math.nan])
def test_window_seconds_must_be_positive_and_finite(bad):
    with pytest.raises(ValidationError):
        InjectionSettings(window_seconds=bad)


def test_fault_sizes_live_in_their_own_group():
    faults = InjectionSettings().faults
    assert faults.spike_spreads == (5.0, 10.0)
    assert faults.pattern_values == (2, 4)
    assert not hasattr(InjectionSettings(), "spike_spreads")


def test_an_unknown_fault_size_key_is_rejected():
    with pytest.raises(ValidationError):
        InjectionSettings(faults={"spike_spread": (1.0, 2.0)})


def test_building_settings_reads_no_file(tmp_path, monkeypatch):
    # loaders' tests build their own settings: sections they don't pass are defaults
    project(tmp_path, monkeypatch, "nab:\n  folder: elsewhere\n")
    assert read_dataset_settings().nab.folder == "elsewhere"
    assert DatasetSettings(root="/x").nab.folder == "nab"


def project(tmp_path, monkeypatch, text):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "datasets.yaml").write_bytes(text.encode())
    monkeypatch.setattr(dataset_config, "find_project_root", lambda: tmp_path)


def test_unknown_key_is_rejected(tmp_path, monkeypatch):
    project(tmp_path, monkeypatch, "nab:\n  foldr: x\n")
    with pytest.raises(ValidationError):
        read_dataset_settings()


def test_yaml_is_read_as_utf8(tmp_path, monkeypatch):
    # Windows reads files in its own code page by default: "°" would become "Â°"
    project(tmp_path, monkeypatch, 'usgs:\n  parameters:\n    "1": [t, "°C"]\n')
    assert read_dataset_settings().usgs.parameters["1"] == ("t", "°C")


def test_an_empty_or_missing_file_gives_the_defaults(tmp_path, monkeypatch):
    project(tmp_path, monkeypatch, "")
    assert read_dataset_settings() == DatasetSettings()
    (tmp_path / "config" / "datasets.yaml").unlink()
    assert read_dataset_settings() == DatasetSettings()


def test_env_var_names_another_file(tmp_path, monkeypatch):
    path = tmp_path / "mine.yaml"
    path.write_text("root: /other/datasets\n", encoding="utf-8")
    monkeypatch.setenv("GUARDIAN_DATASETS_CONFIG", str(path))
    assert read_dataset_settings().root == "/other/datasets"


def test_env_var_naming_a_missing_file_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("GUARDIAN_DATASETS_CONFIG", str(tmp_path / "nope.yaml"))
    with pytest.raises(FileNotFoundError, match="GUARDIAN_DATASETS_CONFIG"):
        read_dataset_settings()


def test_get_dataset_settings_is_cached():
    assert get_dataset_settings() is get_dataset_settings()


def test_long_window_applies_the_override():
    long = DatasetSettings().long_window(21600.0)
    assert long.injection.window_seconds == 21600.0
    assert long.injection.fault_kinds == ("stuck", "drift")
    assert long.injection.fabricated_kinds == ("sine", "ramp", "pattern", "uniform")
    assert long.injection.block_hours == 24
    assert long.injection.sample_every_hours["array_of_things"] == 1801
    assert long.injection.sample_every_hours["grid_frequency"] == 53  # kept


def test_long_window_leaves_the_short_settings_alone():
    base = DatasetSettings()
    base.long_window(86400.0)
    assert base.injection == InjectionSettings()


def test_long_window_validates_the_window():
    with pytest.raises(ValidationError):
        DatasetSettings().long_window(0.0)
