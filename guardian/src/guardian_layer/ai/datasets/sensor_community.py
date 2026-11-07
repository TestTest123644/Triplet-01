"""Sensor.Community citizen sensors: one CSV per sensor and day, 25 sensor models.

The model is part of the type (``bme280_temperature``), so two models measuring the
same quantity never share a cluster. The files are small: sorted in memory.
"""

import csv
from collections.abc import Iterator

from guardian_layer.ai.dataset_factory import DatasetFactory
from guardian_layer.ai.datasets.common import (
    dataset_path,
    open_text,
    parse_float,
    to_readings,
    to_utc,
)
from guardian_layer.ai.interface.dataset_interface import DatasetInterface
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import DatasetRow, LabeledReading

logger = get_logger(__name__)


@DatasetFactory.register("sensor_community")
class SensorCommunity(DatasetInterface):
    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.sensor_community
        folder = dataset_path(self.settings, config.folder)
        for path in sorted(folder.glob(config.pattern)):
            skipped = 0
            with open_text(path) as file:
                for row in csv.DictReader(file, delimiter=";"):
                    station = row.get("sensor_id") or ""
                    model = (row.get("sensor_type") or "").lower()
                    try:
                        timestamp = to_utc(row.get("timestamp") or "")
                    except ValueError:
                        timestamp = None
                    lat = parse_float(row.get("lat") or "")
                    lon = parse_float(row.get("lon") or "")
                    if lat is None or lon is None:  # one without the other is no place
                        lat = lon = None
                    for column, quantity in config.columns.items():
                        if column not in row:
                            continue  # this model has no such column
                        value = parse_float(row.get(column) or "")
                        if (
                            not station
                            or not model
                            or timestamp is None
                            or value is None
                        ):
                            skipped += 1
                            continue
                        kind = f"{model}_{quantity}"
                        yield DatasetRow(
                            timestamp, station, kind, config.unit, value, lat, lon
                        )
            if skipped:
                logger.warning(
                    "Skipped Sensor.Community values", path=str(path), skipped=skipped
                )

    def load(self) -> Iterator[LabeledReading]:
        """Every listed value of every sensor, each sensor oldest-first."""
        return to_readings("sensor_community", self.rows(), sort=True)
