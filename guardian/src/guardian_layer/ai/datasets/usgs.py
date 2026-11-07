"""USGS river sites: water quality sondes and gage height, tab-separated ``.rdb``."""

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


@DatasetFactory.register("usgs")
class Usgs(DatasetInterface):
    def rows(self) -> Iterator[DatasetRow]:
        config = self.settings.usgs
        folder = dataset_path(self.settings, config.folder)
        for path in sorted(folder.glob(config.pattern)):
            skipped = 0
            with open_text(path) as file:
                lines = (line for line in file if not line.startswith("#"))
                reader = csv.reader(lines, delimiter="\t")
                header = next(reader, [])
                next(reader, None)  # the column-format line: 5s 15s 20d ...
                # value columns are <ts_id>_<pcode>; keep the listed codes
                columns = [
                    (index, *config.parameters[name.rsplit("_", 1)[-1]])
                    for index, name in enumerate(header)
                    if "_" in name and name.rsplit("_", 1)[-1] in config.parameters
                ]
                for row in reader:
                    offset = config.time_zones.get(row[3] if len(row) > 3 else "")
                    try:
                        timestamp = to_utc(f"{row[2]}{offset}") if offset else None
                    except ValueError:
                        timestamp = None
                    for index, kind, unit in columns:
                        cell = row[index] if index < len(row) else ""
                        value = parse_float(cell)
                        if timestamp is None or value is None:
                            skipped += 1
                            continue
                        yield DatasetRow(timestamp, row[1], kind, unit, value)
            if skipped:
                logger.warning("Skipped USGS values", path=str(path), skipped=skipped)

    def load(self) -> Iterator[LabeledReading]:
        """Each site's listed parameters; the files are in time order."""
        return to_readings("usgs", self.rows())
