import pytest

import guardian_layer.ai.datasets as datasets
from guardian_layer.ai.interface.dataset_interface import DatasetInterface

LOADERS = [
    datasets.ArrayOfThings,
    datasets.Beijing,
    datasets.Bellinge,
    datasets.ChicagoBeach,
    datasets.Ett,
    datasets.GridFrequency,
    datasets.IntelLab,
    datasets.Jena,
    datasets.Melbourne,
    datasets.Nab,
    datasets.NoaaTides,
    datasets.SensorCommunity,
    datasets.TrafficSpeeds,
    datasets.UciAirQuality,
    datasets.UciHousehold,
    datasets.UciLoad,
    datasets.UkFlood,
    datasets.UkTraffic,
    datasets.Usgs,
]


@pytest.mark.parametrize("loader", LOADERS)
def test_every_loader_implements_the_interface(loader):
    assert issubclass(loader, DatasetInterface)


def test_the_interface_cannot_be_built():
    with pytest.raises(TypeError):
        DatasetInterface()  # type: ignore[abstract]


def test_a_loader_without_load_cannot_be_built():
    class Broken(DatasetInterface):
        pass

    with pytest.raises(TypeError):
        Broken()  # type: ignore[abstract]
