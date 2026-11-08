"""UK motorway traffic counts (National Highways WebTRIS), one JSON file per site."""

import json
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


@DatasetFactory.register("uk_traffic")
class UkTraffic(DatasetInterface):
    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.uk_traffic
        folder = dataset_path(self.settings, config.folder)
        for path in sorted(folder.glob(config.pattern)):
            skipped = 0
            with open_text(path) as file:
                records = json.load(file).get("Rows", [])
            for record in records:
                try:  # the row's time is its 15-minute period's end
                    timestamp = to_utc(
                        f"{record['Report Date'][:10]} {record['Time Period Ending']}"
                    )
                except (KeyError, TypeError, ValueError):
                    skipped += len(config.columns)
                    continue
                for column, (kind, unit) in config.columns.items():
                    value = parse_float(str(record.get(column) or ""))
                    if value is None:
                        skipped += 1
                        continue
                    yield DatasetRow(timestamp, path.stem, kind, unit, value)
            if skipped:
                logger.warning(
                    "Skipped UK traffic values", path=str(path), skipped=skipped
                )

    def load(self) -> Iterator[LabeledReading]:
        """Average speed and volume per site, each sensor oldest-first."""
        return to_readings("uk_traffic", self.rows(), sort=True)
