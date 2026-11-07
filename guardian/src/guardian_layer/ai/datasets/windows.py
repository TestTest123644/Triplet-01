"""Labelled sensor-windows with injected faults, from any loader's readings.

``read`` turns a loader's stream into numpy columns once (slow); ``build`` cuts them
into sensor-windows, injects faults with one seed and builds the features (fast).
The targets and the random draws come in the research notebook's order, so Intel Lab
with a seed gives the notebook's dataset.
"""

import math
import re
from array import array
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta

import numpy as np

from guardian_layer.ai.datasets.common import in_sampled_block
from guardian_layer.ai.datasets.injection import FaultInjector
from guardian_layer.pipelines.decompose import Decompose
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.utils.dataset_config import DatasetSettings, get_dataset_settings
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import (
    Anomalies,
    BoolArray,
    FloatArray,
    IntArray,
    LabeledReading,
    Metadata,
    RawReadings,
    ReadInfo,
    ReadingColumns,
    Sensor,
    SensorSummary,
    SensorTable,
    SourceReadings,
    WindowDataset,
    WindowGroups,
)

logger = get_logger(__name__)

# the feature builder of one window: sensor_id -> feature key -> value
Process = Callable[
    [PreProcess, Decompose, str, list[Sensor]], dict[str, dict[str, float]]
]


def natural_key(text: str) -> tuple[str | int, ...]:
    """Sort key comparing digit runs as numbers: "2" before "10" (Intel Lab motes)."""
    # a split on a captured group alternates text and digit runs: odd parts are digits
    return tuple(
        int(part) if index % 2 else part
        for index, part in enumerate(re.split(r"(\d+)", text))
    )


# the former PreProcess.process, kept here until the orchestrator chains the
# stages; drop this and call the orchestrator once it exists
def process_window(
    preprocessor: PreProcess, decompose: Decompose, method: str, window: list[Sensor]
) -> dict[str, dict[str, float]]:
    """Map each sensor_id in the window to its features (keys: feature_keys, then
    short_keys for the short-window model)."""
    unique, counters = preprocessor.deduplicate(preprocessor.finite(window))
    by_sensor: dict[str, list[Sensor]] = {}
    by_cluster: dict[str, list[Sensor]] = {}
    for reading in unique:
        by_sensor.setdefault(reading.sensor_id, []).append(reading)
        by_cluster.setdefault(preprocessor.cluster_key(reading), []).append(reading)

    features: dict[str, dict[str, float]] = {}
    short: dict[str, dict[str, float]] = {}
    summaries = []
    for sensor_id, readings in by_sensor.items():
        cluster = preprocessor.cluster_key(readings[0])
        value_range = preprocessor.find_range(readings, by_cluster[cluster])
        times, raw = preprocessor.sensor_series(readings)
        values = preprocessor.normalize(raw, value_range)
        features[sensor_id] = {
            **preprocessor.value_features(times, values, value_range is not None),
            **counters[sensor_id],
            **decompose.caller(method, preprocessor.resample(times, values)),
        }
        short[sensor_id] = preprocessor.short_features(decompose, times, raw)
        lat, lon = preprocessor.location(readings)
        summaries.append(
            SensorSummary(
                sensor_id,
                cluster,
                lat,
                lon,
                float(values.mean()),
                float(values[-1]),
            )
        )
    for sensor_id, spatial in preprocessor.spatial_features(summaries).items():
        features[sensor_id].update(spatial)
    for sensor_id, row in short.items():  # last: feature_keys stay a prefix
        features[sensor_id].update(row)
    return features


def optional(number: float) -> float | None:
    """NaN (how the columns store "none") back to None."""
    return None if math.isnan(number) else number


def station_of(sensor_id: str, sensor_type: str) -> str:
    """The station inside a "source:station:type" sensor id."""
    return sensor_id.partition(":")[2].removesuffix(":" + sensor_type)


def kept_rows(column: np.ndarray, keep: BoolArray, rows: IntArray) -> np.ndarray:
    """One per-reading column: the kept readings, in the sorted row order."""
    return column[keep][rows]


class LabelledWindows:
    def __init__(
        self,
        settings: DatasetSettings | None = None,
        method: str | None = None,
        injector: FaultInjector | None = None,
        preprocessor: PreProcess | None = None,
        decompose: Decompose | None = None,
        min_samples: int | None = None,
    ):
        self.settings = settings or get_dataset_settings()
        self.preprocessor = preprocessor or PreProcess(method)
        self.method = self.preprocessor.method
        self.decompose = decompose or Decompose()
        self.injector = injector or FaultInjector(self.settings.injection)
        self.min_samples = (
            self.decompose.settings.min_samples if min_samples is None else min_samples
        )

    def read(self, source: str, readings: Iterable[LabeledReading]) -> SourceReadings:
        """The source's readings as columns, on its sample-period time grid."""
        period, every_hours = self.check_source(source)
        raw = self.collect(source, readings, every_hours)
        keep = self.drop_fast(source, raw, period)
        ranked = self.rank_sensors(raw, keep)
        data = self.assemble(source, raw, keep, ranked, period, every_hours)
        logger.info(
            "Read source",
            source=source,
            readings=len(data.readings.slot),
            sensors=len(data.sensors.sensor_id),
            sample_seconds=period,
            every_hours=every_hours,
        )
        return data

    def check_source(self, source: str) -> tuple[float, int]:
        """The source's (period, every n hours); warns on short blocks."""
        s = self.settings.injection
        if source not in s.sample_seconds:
            raise ValueError(
                f"no sample period for source {source!r}: measure it and add it to "
                "datasets.injection.sample_seconds"
            )
        period = s.sample_seconds[source]
        every_hours = s.sample_every_hours.get(source, 1)
        window_seconds = self.slots_per_window(period) * period
        if every_hours > 1 and s.block_hours * 3600 < window_seconds:
            logger.warning(
                "Sampled blocks are shorter than a window: the block edges cut "
                "every window",
                source=source,
                block_hours=s.block_hours,
                window_seconds=window_seconds,
            )
        return period, every_hours

    def slots_per_window(self, period: float) -> int:
        """Time slots per window: window_slots, or window_seconds cut into periods."""
        s = self.settings.injection
        if s.window_seconds is None:
            return s.window_slots
        # at least one: a window shorter than one period still holds a reading
        return max(1, round(s.window_seconds / period))

    def collect(
        self, source: str, readings: Iterable[LabeledReading], every_hours: int
    ) -> RawReadings:
        """The sampled readings as columns, in loader order."""
        s = self.settings.injection
        label_index = {label.value: index for index, label in enumerate(Anomalies)}

        # array.array: 8 bytes a value, where a list of Python floats costs ~32
        time, value, lat, lon = array("d"), array("d"), array("d"), array("d")
        sensor, packet_order = array("q"), array("q")
        label, event = array("b"), array("b")
        ids: dict[str, int] = {}  # sensor_id -> index, first-seen
        # one value per sensor, first-seen, filled in the same loop as ids
        clusters: list[str] = []
        units: list[str] = []
        types: list[str] = []
        lowers: list[float] = []
        uppers: list[float] = []
        kept = 0
        for reading in readings:
            moment = reading.sensor.timestamp.timestamp()
            # the rule the biggest loaders already apply to raw lines
            if not in_sampled_block(moment, every_hours, s.block_hours):
                continue
            if kept == s.max_readings:  # the safety net, counted after sampling
                logger.warning(
                    "Stopped reading at max_readings",
                    source=source,
                    max_readings=s.max_readings,
                )
                break
            kept += 1
            meta = reading.sensor.metadata
            if meta is None:  # every loader sets it: packet_order and type live there
                raise ValueError(
                    f"{source}: reading without metadata: {reading.sensor.sensor_id}"
                )
            index = ids.setdefault(reading.sensor.sensor_id, len(ids))
            if index == len(clusters):
                clusters.append(reading.sensor.cluster_id)
                units.append(reading.sensor.unit)
                types.append(meta.type)
                lowers.append(
                    math.nan if meta.lower_range is None else meta.lower_range
                )
                uppers.append(
                    math.nan if meta.upper_range is None else meta.upper_range
                )
            time.append(moment)
            value.append(reading.sensor.value)
            lat.append(math.nan if meta.lat is None else meta.lat)
            lon.append(math.nan if meta.lon is None else meta.lon)
            sensor.append(index)
            packet_order.append(meta.packet_order)
            label.append(-1 if reading.label is None else label_index[reading.label])
            event.append(reading.event)
        if not time:
            raise ValueError(
                f"{source}: no reading (empty source, or none in the sampled hours)"
            )
        return RawReadings(
            time=np.asarray(time),
            value=np.asarray(value),
            lat=np.asarray(lat),
            lon=np.asarray(lon),
            sensor=np.asarray(sensor),
            packet_order=np.asarray(packet_order),
            label=np.asarray(label),
            event=np.asarray(event).astype(np.bool_),
            sensor_id=np.array(list(ids), dtype=np.str_),
            cluster_id=np.array(clusters, dtype=np.str_),
            unit=np.array(units, dtype=np.str_),
            sensor_type=np.array(types, dtype=np.str_),
            lower_range=np.array(lowers, dtype=np.float64),
            upper_range=np.array(uppers, dtype=np.float64),
        )

    def drop_fast(self, source: str, raw: RawReadings, period: float) -> BoolArray:
        """Which readings to keep: those of the sensors not faster than the period."""
        fast = self.fast_sensors(
            raw.time, raw.sensor, self.settings.injection.fast_sensor_share * period
        )
        names = [str(raw.sensor_id[index]) for index in fast]
        keep = ~np.isin(raw.sensor, fast)
        if not keep.any():  # raised, not logged: one event is never both
            raise ValueError(
                f"{source}: no reading left after dropping fast sensors: {names}"
            )
        if fast:
            logger.warning(
                "Dropped sensors faster than their source's sample period",
                source=source,
                sensors=names,
            )
        return keep

    def rank_sensors(self, raw: RawReadings, keep: BoolArray) -> IntArray:
        """The kept sensors' first-seen indices, in the notebook's (mote, type) target
        order: station in natural order, then type first-seen."""
        alive = np.unique(raw.sensor[keep]).tolist()
        sensor_ids, types = raw.sensor_id.tolist(), raw.sensor_type.tolist()
        type_rank = {
            kind: rank
            for rank, kind in enumerate(dict.fromkeys(types[index] for index in alive))
        }
        # the index last keeps equal (station, type) sensors in first-seen order
        order = sorted(
            (
                natural_key(station_of(sensor_ids[index], types[index])),
                type_rank[types[index]],
                index,
            )
            for index in alive
        )
        return np.array([index for *_, index in order], dtype=np.int64)

    def assemble(
        self,
        source: str,
        raw: RawReadings,
        keep: BoolArray,
        ranked: IntArray,
        period: float,
        every_hours: int,
    ) -> SourceReadings:
        """The kept readings on the slot grid, sorted, with the sensors renumbered by
        rank so the sensor index is the target rank."""
        sensor_ids, types = raw.sensor_id.tolist(), raw.sensor_type.tolist()
        stations = [
            station_of(sensor_ids[index], types[index]) for index in ranked.tolist()
        ]
        rank = np.full(len(sensor_ids), -1, dtype=np.int64)
        rank[ranked] = np.arange(len(ranked))

        # stations numbered in the same natural order, for the row sort below
        station_rank = {name: n for n, name in enumerate(dict.fromkeys(stations))}
        row_station = np.array([station_rank[name] for name in stations])

        times, sensors = raw.time[keep], rank[raw.sensor[keep]]
        start = float(times.min())
        slot = np.round((times - start) / period).astype(np.int64)
        # rows by (slot, station, time, loader order): the notebook's (slot, mote)
        # order, so a station reporting twice in one slot gives all the types of its
        # first row, then of its second, as to_messages did. The target and group
        # order come from the sensor index in build, not from this sort.
        rows = np.lexsort((np.arange(len(times)), times, row_station[sensors], slot))

        return SourceReadings(
            readings=ReadingColumns(
                slot=slot[rows],
                value=kept_rows(raw.value, keep, rows),
                packet_order=kept_rows(raw.packet_order, keep, rows),
                lat=kept_rows(raw.lat, keep, rows),
                lon=kept_rows(raw.lon, keep, rows),
                label=kept_rows(raw.label, keep, rows),
                event=kept_rows(raw.event, keep, rows),
                sensor=sensors[rows],
            ),
            sensors=SensorTable(
                sensor_id=raw.sensor_id[ranked],
                cluster_id=raw.cluster_id[ranked],
                unit=raw.unit[ranked],
                sensor_type=raw.sensor_type[ranked],
                station=np.array(stations, dtype=np.str_),
                lower_range=raw.lower_range[ranked],
                upper_range=raw.upper_range[ranked],
            ),
            info=ReadInfo(
                source=np.array(source, dtype=np.str_),
                start=np.array(start, dtype=np.float64),
                sample_seconds=np.array(period, dtype=np.float64),
                every_hours=np.array(every_hours, dtype=np.int64),
                block_hours=np.array(self.settings.injection.block_hours, np.int64),
                label_names=np.array(
                    [label.value for label in Anomalies], dtype=np.str_
                ),
            ),
        )

    def fast_sensors(
        self, times: FloatArray, sensors: IntArray, min_gap: float
    ) -> list[int]:
        """Sensors whose median gap between readings is under ``min_gap``: at one slot
        per period, several of their readings would share a time."""
        order = np.lexsort((times, sensors))
        bounds = np.flatnonzero(np.diff(sensors[order])) + 1
        return [
            int(sensors[rows[0]])
            for rows in np.split(order, bounds)
            if len(rows) > 1 and np.median(np.diff(times[rows])) < min_gap
        ]

    def build(
        self,
        data: SourceReadings,
        seed: int,
        kinds: tuple[str, ...] | None = None,
        process: Process = process_window,
        feature_keys: tuple[str, ...] | None = None,
    ) -> WindowDataset:
        """One feature row per sensor-window, after injecting faults with this seed."""
        s = self.settings.injection
        if kinds is None:
            kinds = (*s.fault_kinds, *s.fabricated_kinds)
        if feature_keys is None:
            feature_keys = self.preprocessor.feature_keys()

        groups = self.window_groups(data, self.min_samples)
        train = self.split_by_time(data, groups)
        spreads, ranges = self.training_stats(data, groups, train, self.min_samples)
        values, kind, out_of_range = self.inject_groups(
            data, groups, spreads, ranges, seed, kinds
        )
        rows, out = self.window_features(data, groups, values, process, feature_keys)

        kind_column = np.array([kind[g] for g in out], dtype=np.str_)
        dataset = WindowDataset(
            features=np.array(rows, dtype=np.float64).reshape(
                len(out), len(feature_keys)
            ),
            label=np.isin(kind_column, kinds),
            kind=kind_column,
            sensor_type=groups.group_type[out],
            real_fault=groups.real[out],
            event=groups.event[out],
            source_label=np.array(
                [self.source_label(data, groups.groups[g]) for g in out],
                dtype=np.str_,
            ),
            train=train[out],
            out_of_range=out_of_range[out],
            sensor_id=data.sensors.sensor_id[groups.group_sensor[out]],
            window=groups.group_window[out],
            dataset=np.full(len(out), str(data.info.source)),
            spreads=spreads,
        )
        logger.info(
            "Built windows",
            source=str(data.info.source),
            seed=seed,
            windows=len(out),
            injected=int(dataset.label.sum()),
            real_faults=int(dataset.real_fault.sum()),
            events=int(dataset.event.sum()),
        )
        return dataset

    def window_groups(self, data: SourceReadings, min_samples: int) -> WindowGroups:
        """The sensor-windows: the rows of each (window, sensor), in time order, and
        which are real faults, events, clean, or clean with min_samples+ readings."""
        # the sensor index is its target rank, so groups come in target order
        window_slots = self.slots_per_window(float(data.info.sample_seconds))
        window = data.readings.slot // window_slots
        keys = window * len(data.sensors.sensor_id) + data.readings.sensor
        order = np.argsort(keys, kind="stable")
        starts = np.concatenate(([0], np.flatnonzero(np.diff(keys[order])) + 1))
        group_window = window[order[starts]]
        group_sensor = data.readings.sensor[order[starts]]
        real = np.logical_or.reduceat(data.readings.label[order] >= 0, starts)
        event = (
            np.logical_or.reduceat(data.readings.event[order], starts) & ~real
        )  # real beats event
        clean = ~real & ~event
        sizes = np.diff(np.concatenate((starts, [len(order)])))
        eligible = clean & (sizes >= min_samples)
        if not eligible.any():  # np.quantile of nothing fails unclearly
            raise ValueError(
                f"{data.info.source}: no clean sensor-window with "
                f"{min_samples}+ readings"
            )
        return WindowGroups(
            window_slots=window_slots,
            window=window,
            groups=np.split(order, starts[1:]),
            group_window=group_window,
            group_sensor=group_sensor,
            group_type=data.sensors.sensor_type[group_sensor],
            real=real,
            event=event,
            clean=clean,
            eligible=eligible,
        )

    def split_by_time(self, data: SourceReadings, groups: WindowGroups) -> BoolArray:
        """Which sensor-windows train: those before a quantile of the start slots of
        the eligible (window, station) pairs, each counted once, as the notebook
        counts a mote."""
        train_share = self.settings.injection.train_share
        _, station = np.unique(data.sensors.station, return_inverse=True)
        pair_count = int(station.max()) + 1
        pairs = np.unique(
            groups.group_window[groups.eligible] * pair_count
            + station[groups.group_sensor[groups.eligible]]
        )
        split = float(
            np.quantile(pairs // pair_count * groups.window_slots, train_share)
        )
        return groups.group_window * groups.window_slots < split

    def training_stats(
        self,
        data: SourceReadings,
        groups: WindowGroups,
        train: BoolArray,
        min_samples: int,
    ) -> tuple[dict[str, float], dict[str, tuple[float, float]]]:
        """Per type, from the clean training sensor-windows only: the spread of its
        values, and the fallback range."""
        training: dict[str, list[FloatArray]] = {}
        for g in np.flatnonzero(groups.clean & train):
            training.setdefault(str(groups.group_type[g]), []).append(
                data.readings.value[groups.groups[g]]
            )
        spreads = self.injector.within_window_spreads(training, min_samples)
        logger.debug("Training statistics", spreads=spreads)
        ranges = {
            name: self.value_range(data, name, arrays)
            for name, arrays in training.items()
        }
        return spreads, ranges

    def inject_groups(
        self,
        data: SourceReadings,
        groups: WindowGroups,
        spreads: dict[str, float],
        ranges: dict[str, tuple[float, float]],
        seed: int,
        kinds: tuple[str, ...],
    ) -> tuple[FloatArray, list[str], BoolArray]:
        """The faulty values, the kind of each sensor-window ("", an injected kind,
        "real_fault" or "event") and whether it went out of its range. The only step
        that draws random numbers."""
        # a type with no clean training window has no spread or range;
        # dropped before any random draw so the draws of the others stay the same
        types = groups.group_type
        missing = sorted(set(types[groups.eligible].tolist()) - ranges.keys())
        if missing:
            logger.warning(
                "Not injecting types with no clean training window",
                source=str(data.info.source),
                types=missing,
            )
        targets: list[int] = []
        target_rows: list[IntArray] = []
        target_types: list[str] = []
        for g in np.flatnonzero(groups.eligible):
            if types[g] in ranges:
                targets.append(int(g))
                target_rows.append(groups.groups[g])
                target_types.append(str(types[g]))
        values, applied = self.injector.inject(
            data.readings.value,
            target_rows,
            target_types,
            spreads,
            ranges,
            np.random.default_rng(seed),
            kinds,
        )
        kind = [
            "real_fault" if r else "event" if e else ""
            for r, e in zip(groups.real, groups.event, strict=True)
        ]
        out_of_range = np.zeros(len(groups.groups), dtype=np.bool_)
        for g, applied_kind in zip(targets, applied, strict=True):
            if applied_kind:
                kind[g] = applied_kind
                low, high = ranges[str(types[g])]
                faulted = values[groups.groups[g]]
                out_of_range[g] = bool(((faulted < low) | (faulted > high)).any())
        return values, kind, out_of_range

    def window_features(
        self,
        data: SourceReadings,
        groups: WindowGroups,
        values: FloatArray,
        process: Process,
        feature_keys: tuple[str, ...],
    ) -> tuple[list[list[float]], IntArray]:
        """The feature rows of each window's sensors, and the sensor-window of each
        row."""
        base = datetime.fromtimestamp(float(data.info.start), UTC)
        # windows set in seconds keep the readings' real spacing, as live; else one
        # second per slot, as the notebook did (#95)
        slot_seconds = (
            float(data.info.sample_seconds)
            if self.settings.injection.window_seconds is not None
            else 1.0
        )
        sensor_ids = data.sensors.sensor_id.tolist()
        rows: list[list[float]] = []
        kept: list[int] = []
        # rows are sorted by slot and groups by window: one window is contiguous in both
        row_windows = np.split(
            np.arange(len(groups.window)), np.flatnonzero(np.diff(groups.window)) + 1
        )
        group_windows = np.split(
            np.arange(len(groups.groups)),
            np.flatnonzero(np.diff(groups.group_window)) + 1,
        )
        for window_rows, members in zip(row_windows, group_windows, strict=True):
            by_id = {sensor_ids[groups.group_sensor[g]]: int(g) for g in members}
            sensors = self.window_sensors(data, window_rows, values, base, slot_seconds)
            for sensor_id, features in process(
                self.preprocessor, self.decompose, self.method, sensors
            ).items():
                kept.append(by_id[sensor_id])
                rows.append([features[key] for key in feature_keys])
        return rows, np.array(kept, dtype=np.int64)

    def value_range(
        self, data: SourceReadings, name: str, training: list[FloatArray]
    ) -> tuple[float, float]:
        """The type's metadata range, else the min/max of its clean training values:
        ``uniform`` and the fabricated clip need one."""
        sensors = data.sensors
        known = (
            (sensors.sensor_type == name)
            & np.isfinite(sensors.lower_range)
            & np.isfinite(sensors.upper_range)
        )
        if known.any():
            first = int(np.flatnonzero(known)[0])
            return float(sensors.lower_range[first]), float(sensors.upper_range[first])
        joined = np.concatenate(training)
        return float(joined.min()), float(joined.max())

    def source_label(self, data: SourceReadings, rows: IntArray) -> str:
        """The first source label among these rows, or ""."""
        labels = data.readings.label[rows]
        labelled = labels[labels >= 0]
        return str(data.info.label_names[labelled[0]]) if len(labelled) else ""

    def window_sensors(
        self,
        data: SourceReadings,
        rows: IntArray,
        values: FloatArray,
        base: datetime,
        slot_seconds: float,
    ) -> list[Sensor]:
        """The window's readings as Sensors (faulted values), slot n at
        base + n * slot_seconds."""
        sensors: list[Sensor] = []
        for slot, value, order, lat, lon, index in zip(
            data.readings.slot[rows].tolist(),
            values[rows].tolist(),
            data.readings.packet_order[rows].tolist(),
            data.readings.lat[rows].tolist(),
            data.readings.lon[rows].tolist(),
            data.readings.sensor[rows].tolist(),
            strict=True,
        ):
            sensors.append(
                Sensor(
                    sensor_id=str(data.sensors.sensor_id[index]),
                    cluster_id=str(data.sensors.cluster_id[index]),
                    value=value,
                    unit=str(data.sensors.unit[index]),
                    timestamp=base + timedelta(seconds=slot * slot_seconds),
                    metadata=Metadata(
                        lat=optional(lat),
                        lon=optional(lon),
                        packet_order=order,
                        type=str(data.sensors.sensor_type[index]),
                        lower_range=optional(float(data.sensors.lower_range[index])),
                        upper_range=optional(float(data.sensors.upper_range[index])),
                    ),
                )
            )
        return sensors
