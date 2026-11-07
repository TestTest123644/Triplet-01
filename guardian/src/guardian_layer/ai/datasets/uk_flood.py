"""UK Environment Agency flood monitoring: river level, flow and rainfall readings.

The daily files are not in time order per measure, so all rows are sorted in memory.
"""

import csv
import re
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


@DatasetFactory.register("uk_flood")
class UkFlood(DatasetInterface):
    def kind(self, parameter: str, qualifier: str, unit: str) -> str:
        """``level``, ``Stage``, ``mAOD`` -> ``level_stage_maod``.

        The unit names the datum: a height above sea level (mAOD) and a local depth
        (mASD) of the same kind are not comparable neighbours.
        """
        text = f"{parameter} {qualifier} {unit}".lower()
        return re.sub(r"[^a-z0-9]+", "_", text).strip("_")

    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.uk_flood
        folder = dataset_path(self.settings, config.folder)
        for path in sorted(folder.glob(config.pattern)):
            skipped = 0
            with open_text(path) as file:
                for row in csv.DictReader(file):
                    # one station can carry two measures of a kind (mASD and mAOD):
                    # the measure id is the station
                    measure = (row.get("measure") or "").rsplit("/", 1)[-1]
                    if row["parameter"] in config.skipped_parameters:
                        continue
                    value = parse_float(row.get("value") or "")  # "a|b" gives None
                    try:
                        timestamp = to_utc(row.get("dateTime") or "")
                    except ValueError:
                        timestamp = None
                    if not measure or value is None or timestamp is None:
                        skipped += 1
                        continue
                    kind = self.kind(
                        row["parameter"], row["qualifier"], row["unitName"]
                    )
                    yield DatasetRow(timestamp, measure, kind, row["unitName"], value)
            if skipped:
                logger.warning("Skipped UK flood rows", path=str(path), skipped=skipped)

    def load(self) -> Iterator[LabeledReading]:
        """Every measure of every file, each sensor oldest-first."""
        return to_readings("uk_flood", self.rows(), sort=True)
