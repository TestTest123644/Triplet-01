"""Emergency calls of two real urban floods: NYC EMS (Ida, 1 Sept 2021) and New Orleans
calls for service (Hurricane Ida, 29 Aug 2021).

A source row is one call, not a measurement: the loader counts calls per zone per
minute, and writes the empty minutes between a zone's first and last call as zeros.
The flood days are real events, not sensor faults: their readings keep label Normal
(None) and get ``event=True`` as a marker only, as NAB's windows do.

Licence: the New Orleans calls are police calls only and not for commercial use.
"""

import csv
from collections import Counter
from collections.abc import Iterator
from datetime import datetime, timedelta

from guardian_layer.ai.dataset_factory import DatasetFactory
from guardian_layer.ai.datasets.common import (
    dataset_path,
    open_text,
    to_readings,
    to_utc,
)
from guardian_layer.ai.interface.dataset_interface import DatasetInterface
from guardian_layer.utils.dataset_config import CallSourceSettings
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import DatasetRow, LabeledReading

logger = get_logger(__name__)

MINUTE = timedelta(minutes=1)


@DatasetFactory.register("emergency_calls")
class EmergencyCalls(DatasetInterface):
    def counts(self, config: CallSourceSettings) -> Counter[tuple[str, datetime]]:
        """(zone, minute) -> number of calls, without the excluded and bad rows."""
        path = dataset_path(
            self.settings, self.settings.emergency_calls.folder, config.file
        )
        counts: Counter[tuple[str, datetime]] = Counter()
        skipped = 0
        with open_text(path) as file:
            for row in csv.DictReader(file):
                if any(
                    row.get(column) == value for column, value in config.exclude.items()
                ):
                    continue  # not a call from the public: left out, not a bad row
                zone = (row.get(config.zone_column) or "").strip()
                try:
                    timestamp = to_utc(row.get(config.time_column) or "")
                except ValueError:
                    timestamp = None
                if not zone or timestamp is None:
                    skipped += 1
                    continue
                counts[zone, timestamp.replace(second=0, microsecond=0)] += 1
        if skipped:
            logger.warning("Skipped call rows", path=str(path), skipped=skipped)
        return counts

    def rows(self, config: CallSourceSettings) -> Iterator[DatasetRow]:
        """Every minute of each zone from its first call to its last, zones in turn."""
        settings = self.settings.emergency_calls
        counts = self.counts(config)
        start, end = to_utc(config.event_start), to_utc(config.event_end)
        for zone in sorted({zone for zone, _ in counts}):
            minutes = sorted(minute for z, minute in counts if z == zone)
            minute = minutes[0]
            while minute <= minutes[-1]:
                yield DatasetRow(
                    minute,
                    zone,
                    settings.kind,
                    settings.unit,
                    float(counts[zone, minute]),
                    event=start <= minute < end,
                )
                minute += MINUTE

    def load(self) -> Iterator[LabeledReading]:
        """The call counts of each city in turn, each zone oldest-first."""
        for config in self.settings.emergency_calls.sources:
            yield from to_readings(config.source, self.rows(config))
