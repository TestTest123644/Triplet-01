"""Loaders that read the real sensor datasets (#69) into LabeledReading.

Importing this package registers every loader with ``DatasetFactory``
(``guardian_layer.ai.dataset_factory``), which builds them by name.
"""

from guardian_layer.ai.datasets.array_of_things import ArrayOfThings
from guardian_layer.ai.datasets.beijing import Beijing
from guardian_layer.ai.datasets.bellinge import Bellinge
from guardian_layer.ai.datasets.chicago_beach import ChicagoBeach
from guardian_layer.ai.datasets.emergency_calls import EmergencyCalls
from guardian_layer.ai.datasets.ett import Ett
from guardian_layer.ai.datasets.grid_frequency import GridFrequency, GridFrequency100ms
from guardian_layer.ai.datasets.intel_lab import IntelLab
from guardian_layer.ai.datasets.jena import Jena
from guardian_layer.ai.datasets.melbourne import Melbourne
from guardian_layer.ai.datasets.nab import Nab
from guardian_layer.ai.datasets.noaa import NoaaTides
from guardian_layer.ai.datasets.sensor_community import SensorCommunity
from guardian_layer.ai.datasets.traffic_speeds import TrafficSpeeds
from guardian_layer.ai.datasets.uci_air_quality import UciAirQuality
from guardian_layer.ai.datasets.uci_household import UciHousehold
from guardian_layer.ai.datasets.uci_load import UciLoad
from guardian_layer.ai.datasets.uk_flood import UkFlood
from guardian_layer.ai.datasets.uk_traffic import UkTraffic
from guardian_layer.ai.datasets.usgs import Usgs

__all__ = [
    "ArrayOfThings",
    "Beijing",
    "Bellinge",
    "ChicagoBeach",
    "EmergencyCalls",
    "Ett",
    "GridFrequency",
    "GridFrequency100ms",
    "IntelLab",
    "Jena",
    "Melbourne",
    "Nab",
    "NoaaTides",
    "SensorCommunity",
    "TrafficSpeeds",
    "UciAirQuality",
    "UciHousehold",
    "UciLoad",
    "UkFlood",
    "UkTraffic",
    "Usgs",
]
