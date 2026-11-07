"""Jena weather station (Max Planck Institute roof), every 10 minutes, 2023-2024.

Each zip holds one CSV named like it; the files are read in name order (2023a,
2023b, 2024), which is time order.
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


@DatasetFactory.register("jena")
class Jena(DatasetInterface):
    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.jena
        folder = dataset_path(self.settings, config.folder)
        for path in sorted(folder.glob(config.pattern)):
            skipped = 0
            member = f"{path.stem}.csv"
            with open_text(path, member, encoding=config.encoding) as file:
                for row in csv.DictReader(file):
                    try:
                        timestamp = to_utc(
                            (row.get("Date Time") or "").strip(), config.time_format
                        )
                    except ValueError:
                        skipped += len(config.columns)
                        continue
                    for column, (kind, unit) in config.columns.items():
                        if column not in row:
                            continue  # an older file without this sensor
                        value = parse_float(row.get(column) or "", config.missing)
                        if value is None:
                            skipped += 1
                            continue
                        yield DatasetRow(timestamp, config.station, kind, unit, value)
            if skipped:
                logger.warning(
                    "Skipped Jena values", path=f"{path}:{member}", skipped=skipped
                )

    def load(self) -> Iterator[LabeledReading]:
        """Twelve series of one station; the files are in time order."""
        return to_readings("jena", self.rows())
