"""Array of Things, Chicago 2018-2019: street-pole nodes with many small sensors.

The file is 33 GB of gzip, in time order: it is streamed, never sorted or collected.
"""

import csv
from collections import defaultdict
from collections.abc import Iterator
from itertools import count

from guardian_layer.ai.dataset_factory import DatasetFactory
from guardian_layer.ai.datasets.common import (
    dataset_path,
    open_text,
    parse_float,
    sampled_lines,
    to_utc,
)
from guardian_layer.ai.interface.dataset_interface import DatasetInterface
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import LabeledReading, Metadata, Sensor

logger = get_logger(__name__)


@DatasetFactory.register("array_of_things")
class ArrayOfThings(DatasetInterface):
    def load(self) -> Iterator[LabeledReading]:
        """One reading per row; type is ``<subsystem>_<sensor>_<parameter>``."""
        config = self.settings.array_of_things
        path = dataset_path(self.settings, config.folder, config.data_file)
        orders: defaultdict[str, count[int]] = defaultdict(lambda: count(1))
        skipped = 0
        injection = self.settings.injection
        # `read` applies the same rule to the readings: this is only a speed-up
        every_hours = injection.sample_every_hours.get("array_of_things", 1)
        with open_text(path) as file:
            header = next(file, "")
            lines = sampled_lines(
                file, every_hours, injection.block_hours, "%Y/%m/%d %H"
            )
            fieldnames = next(csv.reader([header]), None)
            for row in csv.DictReader(lines, fieldnames=fieldnames):
                if (
                    None in row.values()
                    or row["parameter"] in config.skipped_parameters
                    or row["sensor"] in config.skipped_sensors
                ):
                    skipped += 1  # a short row leaves None in its missing fields
                    continue
                value = parse_float(row["value_hrf"])  # "NA" and NaN give None
                try:
                    timestamp = to_utc(row["timestamp"], config.time_format)
                except ValueError:
                    timestamp = None
                if value is None or timestamp is None:
                    skipped += 1
                    continue
                # parameter alone would put CO, NO2, O3... in one cluster, and one chip
                # model sits on two boards of a node (tsl250rd: lightsense, metsense)
                kind = f"{row['subsystem']}_{row['sensor']}_{row['parameter']}"
                sensor_id = f"array_of_things:{row['node_id']}:{kind}"
                sensor = Sensor(
                    sensor_id=sensor_id,
                    cluster_id=f"array_of_things:{kind}",
                    value=value,
                    unit=config.unit,
                    timestamp=timestamp,
                    metadata=Metadata(packet_order=next(orders[sensor_id]), type=kind),
                )
                yield LabeledReading(sensor=sensor)
        if skipped:
            logger.warning(
                "Skipped Array of Things rows", path=str(path), skipped=skipped
            )
