"""UCI Air Quality: an Italian roadside multisensor device beside reference analysers,
hourly, 2004-2005. Semicolon CSV with decimal commas; -200 marks a missing value.
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


@DatasetFactory.register("uci_air_quality")
class UciAirQuality(DatasetInterface):
    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.uci_air_quality
        path = dataset_path(self.settings, config.folder, config.zip_file)
        skipped = 0
        with open_text(path, config.member) as file:
            for row in csv.DictReader(file, delimiter=";"):
                try:
                    timestamp = to_utc(
                        f"{row.get('Date')} {row.get('Time')}", config.time_format
                    )
                except ValueError:
                    skipped += len(config.columns)
                    continue
                for column, (kind, unit) in config.columns.items():
                    text = (row.get(column) or "").replace(",", ".")  # decimal comma
                    value = parse_float(text, config.missing)
                    if value is None:
                        skipped += 1
                        continue
                    yield DatasetRow(timestamp, config.station, kind, unit, value)
        if skipped:
            logger.warning(
                "Skipped UCI air quality values", path=str(path), skipped=skipped
            )

    def load(self) -> Iterator[LabeledReading]:
        """Twelve hourly series of one device; the file is in time order."""
        return to_readings("uci_air_quality", self.rows())
