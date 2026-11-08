"""City of Melbourne: microclimate sensors.

The file starts with a BOM, uses semicolons and is unsorted: it is sorted in memory.
A microclimate device sometimes sends two packets in the same second with different
values (2,786 rows): the first one after sorting is kept, the other is dropped.
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


@DatasetFactory.register("melbourne")
class Melbourne(DatasetInterface):
    def place(self, text: str) -> tuple[float | None, float | None]:
        """``"-37.81, 144.96"`` -> (lat, lon); (None, None) when it is not a pair."""
        parts = text.split(",")
        if len(parts) != 2:
            return None, None
        lat, lon = parse_float(parts[0]), parse_float(parts[1])
        return (lat, lon) if lat is not None and lon is not None else (None, None)

    def microclimate_rows(self) -> Iterator[DatasetRow]:
        config = self.settings.melbourne
        path = dataset_path(self.settings, config.folder, config.microclimate_file)
        columns = config.microclimate_columns
        skipped = 0
        # parsed rows are sorted before they become 12 values each: sorting the
        # ~13M values themselves took 3.5 GB
        parsed = []
        with open_text(path) as file:
            for row in csv.DictReader(file, delimiter=";"):
                station = row.get("device_id") or ""
                try:
                    timestamp = to_utc(row.get("received_at") or "")
                except ValueError:
                    timestamp = None
                if not station or timestamp is None:
                    skipped += len(columns)
                    continue
                values = tuple(parse_float(row.get(column) or "") for column in columns)
                parsed.append(
                    (timestamp, station, self.place(row.get("latlong") or ""), values)
                )
        parsed.sort(key=lambda entry: (entry[0], entry[1]))
        for timestamp, station, (lat, lon), values in parsed:
            for kind, value in zip(columns.values(), values, strict=True):
                if value is None:
                    skipped += 1
                    continue
                yield DatasetRow(
                    timestamp,
                    station,
                    kind,
                    config.microclimate_unit,
                    value,
                    lat,
                    lon,
                )
        if skipped:
            logger.warning("Skipped Melbourne values", path=str(path), skipped=skipped)

    def rows(self) -> Iterator[DatasetRow]:
        yield from self.microclimate_rows()

    def load(self) -> Iterator[LabeledReading]:
        """Microclimate series, each sensor oldest-first."""
        return to_readings("melbourne", self.rows())  # sorted above
