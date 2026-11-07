"""Intel Berkeley Lab: temperature, humidity and light from 54 motes (2004).

Moved from ``research/research.ipynb`` (which stays as it is). Times are real, not
rescaled to one slot per second.
"""

import math
from collections.abc import Iterator
from datetime import datetime

from guardian_layer.ai.dataset_factory import DatasetFactory
from guardian_layer.ai.datasets.common import (
    dataset_path,
    open_text,
    parse_float,
    to_utc,
)
from guardian_layer.ai.interface.dataset_interface import DatasetInterface
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import Anomalies, LabeledReading, Metadata, Sensor

logger = get_logger(__name__)


@DatasetFactory.register("intel_lab")
class IntelLab(DatasetInterface):
    def locations(self) -> dict[int, tuple[float, float]]:
        """mote -> (lat, lon) from 'mote x y' lines (metres in the lab)."""
        config = self.settings.intel_lab
        path = dataset_path(self.settings, config.folder, config.locations_file)
        metres_per_lon = config.metres_per_degree * math.cos(
            math.radians(config.origin_lat)
        )
        locations: dict[int, tuple[float, float]] = {}
        with open_text(path) as file:
            for line in file:
                parts = line.split()
                if len(parts) == 3:
                    x, y = float(parts[1]), float(parts[2])
                    locations[int(parts[0])] = (
                        config.origin_lat + y / config.metres_per_degree,
                        config.origin_lon + x / metres_per_lon,
                    )
        return locations

    def rows(self) -> list[tuple[datetime, int, int, list[float], bool]]:
        """Good rows as (time, mote, epoch, values, faulty), in time order.

        data.txt is sorted by epoch, and a mote's epoch restarts when it reboots, so
        file order jumps between months; the file is small enough to sort in memory.
        """
        config = self.settings.intel_lab
        path = dataset_path(self.settings, config.folder, config.data_file)
        rows: list[tuple[datetime, int, int, list[float], bool]] = []
        skipped = 0
        with open_text(path) as file:
            for line in file:
                parts = line.split()
                # date, time, epoch, mote, one value per type, voltage
                numbers = [parse_float(p) for p in parts[2:]]
                if len(parts) != 5 + len(config.types) or None in numbers:
                    skipped += 1
                    continue
                try:
                    epoch, mote = int(parts[2]), int(parts[3])
                    timestamp = to_utc(f"{parts[0]} {parts[1]}")
                except ValueError:  # an epoch like 3.5, or a date like Feb 30
                    skipped += 1
                    continue
                if epoch == config.bad_epoch or not (
                    config.first_mote <= mote <= config.last_mote
                ):
                    skipped += 1
                    continue
                values = [float(p) for p in parts[4:-1]]
                # a battery fault: low voltage or a physically impossible value; it
                # spoils the whole row, as in the notebook's real_fault
                faulty = float(parts[-1]) < config.voltage_min or any(
                    not low <= value <= high
                    for value, (_, low, high) in zip(
                        values, config.types.values(), strict=True
                    )
                )
                rows.append((timestamp, mote, epoch, values, faulty))
        if skipped:
            logger.warning("Skipped Intel Lab rows", path=str(path), skipped=skipped)
        rows.sort(key=lambda row: (row[0], row[1]))  # as the notebook's lexsort
        return rows

    def load(self) -> Iterator[LabeledReading]:
        """One reading per type for each good row, in time order.

        packet_order is the mote's epoch, so a reboot shows up as a replay, as in the
        notebook.
        """
        config = self.settings.intel_lab
        locations = self.locations()
        for timestamp, mote, epoch, values, faulty in self.rows():
            lat, lon = locations.get(mote, (None, None))
            label = Anomalies.SENSOR_MALFUNCTION if faulty else None
            for value, (name, (unit, low, high)) in zip(
                values, config.types.items(), strict=True
            ):
                sensor = Sensor(
                    sensor_id=f"intel_lab:{mote}:{name}",
                    cluster_id=f"intel_lab:{name}",
                    value=value,
                    unit=unit,
                    timestamp=timestamp,
                    metadata=Metadata(
                        lat=lat,
                        lon=lon,
                        packet_order=epoch,
                        type=name,
                        lower_range=low,
                        upper_range=high,
                    ),
                )
                yield LabeledReading(sensor=sensor, label=label)
