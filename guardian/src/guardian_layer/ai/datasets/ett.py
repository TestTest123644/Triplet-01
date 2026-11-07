"""ETT: electricity transformer loads and oil temperature, every 15 minutes."""

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


@DatasetFactory.register("ett")
class Ett(DatasetInterface):
    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.ett
        for name, station in config.files.items():
            path = dataset_path(self.settings, config.folder, name)
            skipped = 0
            with open_text(path) as file:
                for row in csv.DictReader(file):
                    try:
                        timestamp = to_utc(row.get("date") or "")
                    except ValueError:
                        skipped += len(config.columns)
                        continue
                    for column in config.columns:
                        value = parse_float(row.get(column) or "")
                        if value is None:
                            skipped += 1
                            continue
                        yield DatasetRow(
                            timestamp, station, column.lower(), config.unit, value
                        )
            if skipped:
                logger.warning("Skipped ETT values", path=str(path), skipped=skipped)

    def load(self) -> Iterator[LabeledReading]:
        """Seven series per transformer; the files are in time order."""
        return to_readings("ett", self.rows())
