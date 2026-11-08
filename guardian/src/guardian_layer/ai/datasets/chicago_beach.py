"""Chicago Park District beach sensors: lake water quality and weather stations.

Both files are unsorted, so their rows are sorted in memory.
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


@DatasetFactory.register("chicago_beach")
class ChicagoBeach(DatasetInterface):
    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.chicago_beach
        for name, (station_column, columns) in config.files.items():
            path = dataset_path(self.settings, config.folder, name)
            skipped = 0
            with open_text(path) as file:
                for row in csv.DictReader(file):
                    station = row.get(station_column) or ""
                    try:
                        timestamp = to_utc(
                            row.get(config.time_column) or "", config.time_format
                        )
                    except ValueError:
                        timestamp = None
                    for column, (kind, unit) in columns.items():
                        sentinels = (
                            *config.missing,
                            *config.column_missing.get(column, ()),
                        )
                        value = parse_float(row.get(column) or "", sentinels)
                        if not station or timestamp is None or value is None:
                            skipped += 1
                            continue
                        yield DatasetRow(timestamp, station, kind, unit, value)
            if skipped:
                logger.warning(
                    "Skipped Chicago beach values", path=str(path), skipped=skipped
                )

    def load(self) -> Iterator[LabeledReading]:
        """Water and weather measures per beach or station, each sensor oldest-first."""
        return to_readings("chicago_beach", self.rows(), sort=True)
