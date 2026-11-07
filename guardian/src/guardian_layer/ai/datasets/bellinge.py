"""Bellinge (Odense) urban drainage: cleaned water depth series and rain gauges.

Licence: the sensor data are CC BY 4.0, the rain gauges CC BY-NC 4.0 (research use).
"""

import csv
import re
from bisect import bisect_right
from collections import defaultdict
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from guardian_layer.ai.dataset_factory import DatasetFactory
from guardian_layer.ai.datasets.common import (
    dataset_path,
    open_text,
    parse_float,
    to_readings,
    to_utc,
    zip_names,
)
from guardian_layer.ai.interface.dataset_interface import DatasetInterface
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import Anomalies, DatasetRow, LabeledReading

logger = get_logger(__name__)


@DatasetFactory.register("bellinge")
class Bellinge(DatasetInterface):
    def first_time(self, path: Path, name: str) -> datetime:
        """The time of a cleaned file's first row; the far future when it has none."""
        with open_text(path, name) as file:
            for row in csv.DictReader(file):
                try:
                    return to_utc(row.get("time") or "")
                except ValueError:
                    continue
        return datetime.max.replace(tzinfo=UTC)

    def level_files(self, path: Path) -> dict[tuple[str, str], list[str]]:
        """(Station, type) -> its cleaned files, oldest first; other files are left out.

        The period number restarts per SCADA system (System2000 p1 and iFix p1 of
        one sensor), so the files are ordered by their first time, then by period.
        """
        config = self.settings.bellinge
        pattern = re.compile(config.file_pattern)
        found: dict[tuple[str, str], list[tuple[datetime, int, str]]] = defaultdict(
            list
        )
        for name in zip_names(path):
            match = pattern.fullmatch(name.rsplit("/", 1)[-1])
            if match is None:
                continue
            measurement = match["measurement"]
            if match["pump"]:
                station = f"{match['location']}_{match['pump']}"
                kind = config.pump_kind
            else:
                if measurement is None and match["system"] == "Danova":
                    measurement = config.danova_measurement
                elif measurement not in config.measurements:
                    continue  # position (an actuator)
                station = f"{match['location']}_{measurement}"
                kind = config.level_kind
            first = self.first_time(path, name)
            found[station, kind].append((first, int(match["period"]), name))
        return {key: [n for _, _, n in sorted(files)] for key, files in found.items()}

    def skip(self, row: dict[str, str]) -> bool:
        """Filled-in, poor-quality or manually removed: not a reading to keep."""
        return any(
            row.get(flag) == "True" for flag in self.settings.bellinge.skip_flags
        )

    def label(self, row: dict[str, str]) -> Anomalies | None:
        """The first set flag's label, in the configured order; None when none is."""
        for flag, label in self.settings.bellinge.flag_labels.items():
            if row.get(flag) == "True":
                return label
        return None

    def level_rows(self) -> Iterator[DatasetRow]:
        config = self.settings.bellinge
        path = dataset_path(self.settings, config.folder, config.cleaned_zip)
        for (station, kind), names in self.level_files(path).items():
            unit = config.level_unit if kind == config.level_kind else config.pump_unit
            for name in names:
                skipped = 0
                with open_text(path, name) as file:
                    for row in csv.DictReader(file):
                        value = parse_float(row.get("raw_value") or "")
                        try:
                            timestamp = to_utc(row.get("time") or "")
                        except ValueError:
                            timestamp = None
                        if self.skip(row) or value is None or timestamp is None:
                            skipped += 1
                            continue
                        yield DatasetRow(
                            timestamp,
                            station,
                            kind,
                            unit,
                            value,
                            label=self.label(row)
                            if kind == config.level_kind
                            else None,
                        )
                if skipped:
                    logger.warning(
                        "Skipped Bellinge rows", path=f"{path}:{name}", skipped=skipped
                    )

    def periods(
        self, path: Path, gauge: str, names: list[str]
    ) -> list[tuple[datetime, datetime]]:
        """The gauge's no-data periods, merged and sorted."""
        config = self.settings.bellinge
        period = re.compile(config.period_line)
        spans = []
        for suffix in config.period_files:
            if gauge + suffix not in names:
                continue
            with open_text(path, gauge + suffix) as file:
                for line in file:
                    match = period.match(line)
                    if match:
                        start, stop = (
                            to_utc(text, config.period_time_format)
                            for text in match.groups()
                        )
                        spans.append((start, stop))
        merged: list[tuple[datetime, datetime]] = []
        for start, stop in sorted(spans):
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], stop))
            else:
                merged.append((start, stop))
        return merged

    def gauge_rows(self) -> Iterator[DatasetRow]:
        config = self.settings.bellinge
        path = dataset_path(self.settings, config.folder, config.gauges_zip)
        names = zip_names(path)
        for member, (station, time_format) in config.gauges.items():
            spans = self.periods(path, member.removesuffix("_ts.txt"), names)
            starts = [start for start, _ in spans]
            skipped = 0
            with open_text(path, member) as file:
                lines = iter(file)
                next(lines, None)  # Rainfall Intensity[mu-m/sec]:...
                for row in csv.DictReader(lines, delimiter=";"):
                    cells = list(row.values())
                    value = parse_float(cells[1] if len(cells) > 1 else "")
                    try:
                        timestamp = to_utc(row.get("Time") or "", time_format)
                    except ValueError:
                        timestamp = None
                    if value is None or timestamp is None:
                        skipped += 1
                        continue
                    index = bisect_right(starts, timestamp) - 1
                    if index >= 0 and timestamp <= spans[index][1]:
                        skipped += 1  # inside a no-data period
                        continue
                    yield DatasetRow(
                        timestamp, station, config.gauge_kind, config.gauge_unit, value
                    )
            if skipped:
                logger.warning(
                    "Skipped Bellinge rows", path=f"{path}:{member}", skipped=skipped
                )

    def rows(self) -> Iterator[DatasetRow]:
        yield from self.level_rows()
        yield from self.gauge_rows()

    def load(self) -> Iterator[LabeledReading]:
        """Water depth per level sensor and the pump, then rain intensity per gauge."""
        return to_readings("bellinge", self.rows())
