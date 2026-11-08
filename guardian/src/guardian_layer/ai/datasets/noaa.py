"""NOAA CO-OPS tide stations: water level and meteorological products, 6-minute data.

Each file is one product of one station; the source's quality flags become labels.
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
from guardian_layer.utils.schema import Anomalies, DatasetRow, LabeledReading

logger = get_logger(__name__)


@DatasetFactory.register("noaa_tides")
class NoaaTides(DatasetInterface):
    def label(self, row: dict[str, str]) -> Anomalies | None:
        """The first flag set to 1, in the configured order; None when none is."""
        for flag, label in self.settings.noaa_tides.flag_labels.items():
            if row.get(flag) == "1":
                return label
        return None

    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.noaa_tides
        folder = dataset_path(self.settings, config.folder)
        for path in sorted(folder.glob(config.pattern)):
            station, _, product = path.stem.partition("_")
            columns = config.products.get(product)
            if columns is None:
                continue
            skipped = 0
            with open_text(path) as file:
                reader = csv.reader(file)
                header = [name.strip() for name in next(reader, [])]
                for cells in reader:
                    if cells and cells[0].startswith("Error"):  # "No data was found"
                        skipped += 1
                        continue
                    # a repeated name keeps its first column: wind lists Direction
                    # twice, degrees first and compass text second
                    row = dict(reversed(list(zip(header, cells, strict=False))))
                    try:
                        timestamp = to_utc(row.get("Date Time", ""))
                    except ValueError:
                        skipped += len(columns)
                        continue
                    label = self.label(row)
                    for column, (kind, unit) in columns.items():
                        value = parse_float(row.get(column, ""))
                        if value is None:
                            skipped += 1
                            continue
                        yield DatasetRow(
                            timestamp, station, kind, unit, value, label=label
                        )
            if skipped:
                logger.warning("Skipped NOAA values", path=str(path), skipped=skipped)

    def load(self) -> Iterator[LabeledReading]:
        """Every product of every station; the files are in time order."""
        return to_readings("noaa_tides", self.rows())
