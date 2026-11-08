import importlib
import subprocess
import sys

import pytest

import guardian_layer.ai.datasets as datasets
from guardian_layer.ai.dataset_factory import DatasetFactory
from guardian_layer.ai.datasets import nab
from guardian_layer.ai.interface.dataset_interface import DatasetInterface
from guardian_layer.utils.dataset_config import DatasetSettings, get_dataset_settings

LOADERS = {
    "array_of_things": datasets.ArrayOfThings,
    "beijing": datasets.Beijing,
    "bellinge": datasets.Bellinge,
    "chicago_beach": datasets.ChicagoBeach,
    "ett": datasets.Ett,
    "grid_frequency": datasets.GridFrequency,
    "grid_frequency_100ms": datasets.GridFrequency100ms,
    "intel_lab": datasets.IntelLab,
    "jena": datasets.Jena,
    "melbourne": datasets.Melbourne,
    "emergency_calls": datasets.EmergencyCalls,
    "nab": datasets.Nab,
    "noaa_tides": datasets.NoaaTides,
    "sensor_community": datasets.SensorCommunity,
    "traffic_speeds": datasets.TrafficSpeeds,
    "uci_air_quality": datasets.UciAirQuality,
    "uci_household": datasets.UciHousehold,
    "uci_load": datasets.UciLoad,
    "uk_flood": datasets.UkFlood,
    "uk_traffic": datasets.UkTraffic,
    "usgs": datasets.Usgs,
}


def test_names_lists_every_loader():
    assert DatasetFactory.names() == sorted(LOADERS)


@pytest.mark.parametrize(("name", "loader"), LOADERS.items())
def test_create_builds_the_named_loader(name, loader):
    settings = DatasetSettings(root="/somewhere")
    built = DatasetFactory(settings).create(name)
    assert isinstance(built, loader)
    assert built.settings is settings


def test_load_reads_the_named_loader(monkeypatch):
    monkeypatch.setattr(datasets.Nab, "load", lambda self: iter(["nab"]))
    assert list(DatasetFactory().load("nab")) == ["nab"]


def test_unknown_name_raises_at_once():
    # no list(): the error must not wait for iteration
    with pytest.raises(ValueError, match="unknown dataset: skab"):
        DatasetFactory().create("skab")


def test_a_new_loader_registers_itself(monkeypatch):
    # the extension the lead asked for: a new source needs no edit to the factory
    monkeypatch.setattr(DatasetFactory, "registry", dict(DatasetFactory.registry))

    @DatasetFactory.register("demo")
    class Demo(DatasetInterface):
        def load(self):
            return iter([])

    assert isinstance(DatasetFactory().create("demo"), Demo)
    assert "demo" in DatasetFactory.names()


def test_duplicate_name_is_rejected(monkeypatch):
    monkeypatch.setattr(DatasetFactory, "registry", dict(DatasetFactory.registry))

    class Other(DatasetInterface):
        def load(self):
            return iter([])

    with pytest.raises(ValueError, match="already registered: nab"):
        DatasetFactory.register("nab")(Other)


def test_reloading_a_loader_module_replaces_its_class(monkeypatch):
    # Jupyter autoreload re-runs the module: the new class takes the old one's name
    monkeypatch.setattr(DatasetFactory, "registry", dict(DatasetFactory.registry))
    monkeypatch.setattr(nab, "Nab", nab.Nab)  # restored after the test
    importlib.reload(nab)
    assert DatasetFactory.registry["nab"] is nab.Nab is not datasets.Nab


def test_no_settings_gives_the_loader_the_shared_settings():
    assert DatasetFactory().create("nab").settings is get_dataset_settings()


@pytest.mark.parametrize(
    "code",
    [
        "import guardian_layer.ai.datasets\n"
        "from guardian_layer.ai.dataset_factory import DatasetFactory\n",
        "from guardian_layer.ai.dataset_factory import DatasetFactory\n",
    ],
)
def test_imports_in_a_fresh_interpreter(code):
    # in-process the import order of other tests can mask a circular import
    check = code + "assert len(DatasetFactory.names()) == 21\n"
    subprocess.run([sys.executable, "-I", "-c", check], check=True)
