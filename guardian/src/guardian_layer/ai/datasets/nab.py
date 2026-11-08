"""NAB (Numenta Anomaly Benchmark): Minneapolis traffic detectors and an office
thermometer.

NAB's windows mark real-world events (traffic incidents, an AC failure), not sensor
faults: readings inside one keep label Normal (None) and get ``event=True`` as a marker
only (lead on #69).
"""

import csv
import json
from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime
from itertools import count

from guardian_layer.ai.dataset_factory import DatasetFactory
from guardian_layer.ai.datasets.common import (
    dataset_path,
    open_text,
    parse_float,
    to_utc,
)
from guardian_layer.ai.interface.dataset_interface import DatasetInterface
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import LabeledReading, Metadata, Sensor

logger = get_logger(__name__)


@DatasetFactory.register("nab")
class Nab(DatasetInterface):
    def windows(self) -> dict[str, list[tuple[datetime, datetime]]]:
        """NAB name -> its (start, end) event windows."""
        config = self.settings.nab
        path = dataset_path(self.settings, config.folder, config.windows_file)
        with open_text(path) as file:
            raw: dict[str, list[list[str]]] = json.load(file)
        return {
            name: [(to_utc(start), to_utc(end)) for start, end in spans]
            for name, spans in raw.items()
        }

    def load(self) -> Iterator[LabeledReading]:
        """Each listed series in turn, in file (time) order."""
        config = self.settings.nab
        windows = self.windows()
        orders: defaultdict[str, count[int]] = defaultdict(lambda: count(1))
        for name, (station, kind, unit) in config.series.items():
            local = config.local_prefix + name.replace("/", "__")
            path = dataset_path(self.settings, config.folder, local)
            spans = windows.get(name, [])
            skipped = 0
            with open_text(path) as file:
                for row in csv.DictReader(file):
                    value = parse_float(row.get("value") or "")
                    try:
                        timestamp = to_utc(row.get("timestamp") or "")
                    except ValueError:
                        timestamp = None
                    if value is None or timestamp is None:
                        skipped += 1
                        continue
                    sensor_id = f"nab:{station}:{kind}"
                    sensor = Sensor(
                        sensor_id=sensor_id,
                        cluster_id=f"nab:{kind}",
                        value=value,
                        unit=unit,
                        timestamp=timestamp,
                        metadata=Metadata(
                            packet_order=next(orders[sensor_id]), type=kind
                        ),
                    )
                    event = any(start <= timestamp <= end for start, end in spans)
                    yield LabeledReading(sensor=sensor, event=event)
            if skipped:
                logger.warning("Skipped NAB rows", path=str(path), skipped=skipped)
