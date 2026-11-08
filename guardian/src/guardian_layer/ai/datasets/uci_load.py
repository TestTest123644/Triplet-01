"""UCI ElectricityLoadDiagrams 2011-2014: 370 Portuguese clients, 15-minute load.

One row per time, one column per meter, decimal commas. About 52 million readings:
streamed. Known limit of the source (UCI page): each March 1:00-2:00 is zero for
every meter and each October 1:00-2:00 holds two hours (daylight saving); kept as is.
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


@DatasetFactory.register("uci_load")
class UciLoad(DatasetInterface):
    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.uci_load
        path = dataset_path(self.settings, config.folder, config.zip_file)
        skipped = 0
        with open_text(path, config.member) as file:
            reader = csv.reader(file, delimiter=";")
            meters = next(reader, [])[1:]
            started: set[str] = set()  # meters that have reported a non-zero load
            for cells in reader:
                try:
                    timestamp = to_utc(cells[0])
                except (IndexError, ValueError):
                    skipped += len(meters)
                    continue
                for meter, cell in zip(meters, cells[1:], strict=False):
                    value = parse_float(cell.replace(",", "."))  # decimal comma
                    if value is None:
                        skipped += 1
                        continue
                    if meter not in started:
                        if value == 0:
                            continue  # not installed yet: expected, not counted
                        started.add(meter)
                    yield DatasetRow(timestamp, meter, config.kind, config.unit, value)
        if skipped:
            logger.warning("Skipped UCI load values", path=str(path), skipped=skipped)

    def load(self) -> Iterator[LabeledReading]:
        """One load series per meter; the file is in time order."""
        return to_readings("uci_load", self.rows())
