"""Power grid frequency recordings (Jumar et al., Database of Power Grid Frequency
Measurements): deviation from the nominal 50/60 Hz, one file per campaign.

Each file holds the station's own series in columns 1-2; columns after that repeat
the Karlsruhe reference and are ignored. Files go up to 1.3 GB: they are streamed.
The four 100 ms recordings are their own dataset, ``grid_frequency_100ms``, so they
keep their own 0.1 s sample period next to the other stations' 1 s.
"""

import csv
from collections.abc import Iterator
from pathlib import Path
from typing import ClassVar

from guardian_layer.ai.dataset_factory import DatasetFactory
from guardian_layer.ai.datasets.common import (
    dataset_path,
    open_text,
    parse_float,
    sampled_lines,
    to_readings,
    to_utc,
    zip_names,
)
from guardian_layer.ai.interface.dataset_interface import DatasetInterface
from guardian_layer.utils.dataset_config import GridFrequencySettings
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import DatasetRow, LabeledReading

logger = get_logger(__name__)


@DatasetFactory.register("grid_frequency")
class GridFrequency(DatasetInterface):
    name: ClassVar[str] = "grid_frequency"

    def config(self) -> GridFrequencySettings:
        return self.settings.grid_frequency

    def files(self) -> list[tuple[str, Path]]:
        """(station, file) for every recording, the skipped keys left out."""
        config = self.config()
        folder = dataset_path(self.settings, config.folder)
        found = []
        for pattern in config.patterns:
            for path in sorted(folder.glob(pattern)):
                station = path.name.split("__")[1]
                if not any(key in station.lower() for key in config.skipped_keys):
                    found.append((station, path))
        return found

    def rows(self) -> Iterator[DatasetRow]:
        config = self.config()
        injection = self.settings.injection
        # `read` applies the same rule to the readings: this is only a speed-up
        every_hours = injection.sample_every_hours.get(self.name, 1)
        for station, path in self.files():
            members = zip_names(path)[:1] if path.suffix == ".zip" else []
            skipped = 0
            with open_text(path, *members) as file:
                next(file, None)  # header; column 0 is the time whatever its name
                lines = sampled_lines(
                    file, every_hours, injection.block_hours, "%Y-%m-%d %H"
                )
                reader = csv.reader(lines, delimiter=";")
                for cells in reader:
                    if len(cells) < 3 or cells[2] in config.bad_qi:
                        skipped += 1
                        continue
                    value = parse_float(cells[1])
                    try:
                        timestamp = to_utc(cells[0])
                    except ValueError:
                        timestamp = None
                    if value is None or timestamp is None:
                        skipped += 1
                        continue
                    yield DatasetRow(
                        timestamp, station, config.kind, config.unit, value
                    )
            if skipped:
                logger.warning(
                    "Skipped grid frequency rows", path=str(path), skipped=skipped
                )

    def load(self) -> Iterator[LabeledReading]:
        """One frequency series per station; the files are in time order."""
        return to_readings(self.name, self.rows())


@DatasetFactory.register("grid_frequency_100ms")
class GridFrequency100ms(GridFrequency):
    name: ClassVar[str] = "grid_frequency_100ms"

    def config(self) -> GridFrequencySettings:
        return self.settings.grid_frequency_100ms
