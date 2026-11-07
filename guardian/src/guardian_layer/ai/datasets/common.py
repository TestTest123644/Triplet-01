"""Helpers every dataset loader shares: paths, file opening, times and numbers."""

import gzip
import io
import math
import zipfile
from collections import defaultdict
from collections.abc import Collection, Iterable, Iterator
from datetime import UTC, datetime
from itertools import count
from pathlib import Path
from typing import TextIO

from guardian_layer.utils.dataset_config import DatasetSettings
from guardian_layer.utils.helpers import find_project_root
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import DatasetRow, LabeledReading, Metadata, Sensor

logger = get_logger(__name__)


def dataset_path(settings: DatasetSettings, *parts: str) -> Path:
    """A file under ``datasets.root``; raises if it is not there."""
    # an absolute root replaces the project root (pathlib's join rule)
    path = (find_project_root() or Path.cwd()) / settings.root / Path(*parts)
    if not path.exists():
        raise FileNotFoundError(
            f"dataset file not found: {path} "
            f"(set datasets.root in config/settings.yaml, now {settings.root!r})"
        )
    return path


def open_archive(path: Path, *members: str) -> zipfile.ZipFile:
    """The zip at ``path``, or the one reached through nested zip ``members``."""
    archive = zipfile.ZipFile(path)
    for member in members:  # a zip inside a zip is small (Beijing): held in memory
        inner = zipfile.ZipFile(io.BytesIO(archive.read(member)))
        archive.close()
        archive = inner
    return archive


def zip_names(path: Path, *members: str) -> list[str]:
    """Member files of a (nested) zip, without folders and macOS metadata, sorted."""
    with open_archive(path, *members) as archive:
        return sorted(
            name
            for name in archive.namelist()
            if not name.endswith("/") and not name.startswith("__MACOSX/")
        )


def open_text(path: Path, *members: str, encoding: str = "utf-8-sig") -> TextIO:
    """A text stream of a plain, ``.gz`` or zipped file (bad bytes replaced).

    ``members`` is the path inside zips: ``("x.csv",)`` for a member,
    ``("inner.zip", "x.csv")`` for a member of a zip inside the zip. ``utf-8-sig``
    drops a leading BOM and reads anything else as plain UTF-8.
    """
    if members:
        archive = open_archive(path, *members[:-1])
        try:
            stream = archive.open(members[-1])
        finally:  # also on a missing member; an open member keeps the file itself
            archive.close()
        return io.TextIOWrapper(stream, encoding=encoding, errors="replace", newline="")
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding=encoding, errors="replace", newline="")
    return path.open(encoding=encoding, errors="replace", newline="")  # "" for csv


def to_readings(
    source: str, rows: Iterable[DatasetRow], sort: bool = False
) -> Iterator[LabeledReading]:
    """Readings with their ids and a per-sensor packet_order counted in yield order.

    ``sort`` orders all rows by time first, for sources stored unsorted or
    newest-first (they are small enough to hold in memory).
    """
    if sort:
        rows = sorted(rows, key=lambda row: (row.timestamp, row.station, row.kind))
    orders: defaultdict[str, count[int]] = defaultdict(lambda: count(1))
    last: dict[str, datetime] = {}
    dropped = 0
    for row in rows:
        sensor_id = f"{source}:{row.station}:{row.kind}"
        # a repeated row or block (Jena, Sensor.Community) would step back in time
        if sensor_id in last and row.timestamp <= last[sensor_id]:
            dropped += 1
            continue
        last[sensor_id] = row.timestamp
        sensor = Sensor(
            sensor_id=sensor_id,
            cluster_id=f"{source}:{row.kind}",
            value=row.value,
            unit=row.unit,
            timestamp=row.timestamp,
            metadata=Metadata(
                lat=row.lat,
                lon=row.lon,
                packet_order=next(orders[sensor_id]),
                type=row.kind,
            ),
        )
        yield LabeledReading(sensor=sensor, label=row.label, event=row.event)
    if dropped:
        logger.warning(
            "Dropped readings not later than the sensor's last",
            source=source,
            dropped=dropped,
        )


def to_utc(text: str, fmt: str | None = None) -> datetime:
    """An aware UTC time; ISO when ``fmt`` is None, naive times read as UTC."""
    moment = (
        datetime.fromisoformat(text) if fmt is None else datetime.strptime(text, fmt)
    )
    if moment.tzinfo is None:  # the sources give local lab/city time: only gaps matter
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def in_sampled_block(epoch_seconds: float, every_hours: int, block_hours: int) -> bool:
    """True in the first ``block_hours`` hours of each ``every_hours`` cycle (UTC)."""
    return epoch_seconds // 3600 % every_hours < block_hours


def sampled_lines(
    lines: Iterable[str], every_hours: int, block_hours: int, hour_format: str
) -> Iterator[str]:
    """The lines whose first 13 characters (an hour) fall in a sampled block.

    Filtering the raw text skips the CSV parsing of the readings that are dropped.
    A prefix that does not parse is kept: the loader's own parsing counts and logs it.
    """
    keep: dict[str, bool] = {}  # one parse per hour, not per line
    for line in lines:
        prefix = line[:13]
        if prefix not in keep:
            try:
                epoch = to_utc(prefix, hour_format).timestamp()
                keep[prefix] = in_sampled_block(epoch, every_hours, block_hours)
            except ValueError:
                keep[prefix] = True
        if keep[prefix]:
            yield line


def parse_float(text: str, missing: Collection[str] = ()) -> float | None:
    """The number, or None for empty, non-numeric, NaN/inf or a listed sentinel.

    A numeric sentinel matches by value: ``-200`` also catches ``-200.0``.
    """
    text = text.strip()
    if text in missing:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if not math.isfinite(value):
        return None
    for sentinel in missing:
        try:
            if float(sentinel) == value:
                return None
        except ValueError:  # a text sentinel such as "?" was compared above
            pass
    return value
