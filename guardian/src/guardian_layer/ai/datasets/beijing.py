"""Beijing multi-site air quality (UCI): 12 stations, hourly, 2013-2017.

The station files sit in a zip inside ``data.zip``; the archive's top-level CSVs are
stock prices and are not read.
"""

import csv
from collections.abc import Iterator
from datetime import UTC, datetime

from guardian_layer.ai.dataset_factory import DatasetFactory
from guardian_layer.ai.datasets.common import (
    dataset_path,
    open_text,
    parse_float,
    to_readings,
    zip_names,
)
from guardian_layer.ai.interface.dataset_interface import DatasetInterface
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import DatasetRow, LabeledReading

logger = get_logger(__name__)


@DatasetFactory.register("beijing")
class Beijing(DatasetInterface):
    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.beijing
        path = dataset_path(self.settings, config.folder, config.zip_file)
        for name in zip_names(path, config.inner_zip):
            skipped = 0
            with open_text(path, config.inner_zip, name) as file:
                for row in csv.DictReader(file):
                    try:  # local time without a zone: read as UTC
                        timestamp = datetime(
                            int(row["year"]),
                            int(row["month"]),
                            int(row["day"]),
                            int(row["hour"]),
                            tzinfo=UTC,
                        )
                    except (KeyError, TypeError, ValueError):
                        skipped += len(config.columns)
                        continue
                    station = row.get("station") or ""
                    for column, (kind, unit) in config.columns.items():
                        value = parse_float(row.get(column) or "", config.missing)
                        if not station or value is None:
                            skipped += 1
                            continue
                        yield DatasetRow(timestamp, station, kind, unit, value)
            if skipped:
                logger.warning(
                    "Skipped Beijing values", path=f"{path}:{name}", skipped=skipped
                )

    def load(self) -> Iterator[LabeledReading]:
        """Eleven hourly series per station; each file is in time order."""
        return to_readings("beijing", self.rows())
