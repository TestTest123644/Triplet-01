"""Traffic speeds: Chicago bus-GPS region estimates and NYC DOT link speeds.

Both files are stored newest-first, so their rows are sorted in memory.
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


@DatasetFactory.register("traffic_speeds")
class TrafficSpeeds(DatasetInterface):
    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.traffic_speeds
        # file, station prefix, station column, time column, type
        files = (
            (config.chicago_file, "chicago", "region_id", "time", "bus_region_speed"),
            (config.nyc_file, "nyc", "link_id", "data_as_of", "link_speed"),
        )
        for name, prefix, station, time, kind in files:
            path = dataset_path(self.settings, config.folder, name)
            skipped = 0
            with open_text(path) as file:
                for row in csv.DictReader(file):
                    value = parse_float(row.get("speed") or "")
                    try:
                        timestamp = to_utc(row.get(time) or "")
                    except ValueError:
                        timestamp = None
                    # rows with no measurement, speed 0 in both: NYC travel time 0,
                    # Chicago no bus seen in the region
                    if (
                        value is None
                        or timestamp is None
                        or row.get("travel_time") == "0"
                        or row.get("bus_count") == "0"
                    ):
                        skipped += 1
                        continue
                    yield DatasetRow(
                        timestamp, f"{prefix}_{row[station]}", kind, config.unit, value
                    )
            if skipped:
                logger.warning(
                    "Skipped traffic speed rows", path=str(path), skipped=skipped
                )

    def load(self) -> Iterator[LabeledReading]:
        """Both cities' speeds, each sensor oldest-first."""
        return to_readings("traffic_speeds", self.rows(), sort=True)
