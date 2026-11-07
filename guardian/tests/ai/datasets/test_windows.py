import dataclasses
import functools
import gzip
import logging
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from guardian_layer.ai.datasets.array_of_things import ArrayOfThings
from guardian_layer.ai.datasets.common import in_sampled_block
from guardian_layer.ai.datasets.windows import (
    LabelledWindows,
    natural_key,
    process_window,
)
from guardian_layer.pipelines.decompose import Decompose
from guardian_layer.pipelines.long_preprocess import (
    LongPreProcess,
    process_long_window,
)
from guardian_layer.pipelines.preprocess import PreProcess
from guardian_layer.utils.config import LongWindowSettings, get_settings
from guardian_layer.utils.dataset_config import (
    ArrayOfThingsSettings,
    DatasetSettings,
    InjectionSettings,
)
from guardian_layer.utils.schema import (
    Anomalies,
    LabeledReading,
    Metadata,
    ReadInfo,
    Sensor,
    SourceReadings,
)

START = datetime(2004, 1, 1, tzinfo=UTC)
STATIONS = ("1", "2", "10")
TYPES = ("temperature", "humidity")
ALL_KINDS = (
    "stuck",
    "spike",
    "drift",
    "noise",
    "oscillation",
    "spoof",
    "sine",
    "ramp",
    "pattern",
    "uniform",
)


@pytest.fixture
def windows_log(caplog):
    # the guardian_layer logger does not propagate: caplog's root handler sees nothing
    name = "guardian_layer.ai.datasets.windows"
    module_logger = logging.getLogger(name)
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG, logger=name)
    yield caplog
    module_logger.removeHandler(caplog.handler)


def stream(
    stations=STATIONS,
    types=TYPES,
    slots=200,
    period=31.0,
    jitter=3.0,
    labels=None,
    events=None,
    ranges=None,
    first_slot=0,
    seed=0,
):
    """Readings shaped as to_readings yields them; labels/events keyed by
    (station, type, slot), ranges by type."""
    labels, events, ranges = labels or {}, events or set(), ranges or {}
    rng = np.random.default_rng(seed)
    orders: defaultdict[str, int] = defaultdict(int)
    readings = []
    for slot in range(first_slot, first_slot + slots):
        for s, station in enumerate(stations):
            for k, kind in enumerate(types):
                sensor_id = f"src:{station}:{kind}"
                orders[sensor_id] += 1
                low, high = ranges.get(kind, (None, None))
                sensor = Sensor(
                    sensor_id=sensor_id,
                    cluster_id=f"src:{kind}",
                    value=float(10 * k + s + rng.normal(0.0, 1.0)),
                    unit="u",
                    timestamp=START
                    + timedelta(seconds=slot * period + rng.uniform(0, jitter)),
                    metadata=Metadata(
                        lat=40.0 + s,
                        lon=-3.0,
                        packet_order=orders[sensor_id],
                        type=kind,
                        lower_range=low,
                        upper_range=high,
                    ),
                )
                key = (station, kind, slot)
                readings.append(
                    LabeledReading(
                        sensor=sensor, label=labels.get(key), event=key in events
                    )
                )
    return readings


def settings(**injection):
    injection = {"sample_seconds": {"src": 31.0}, "inject_share": 0.5, **injection}
    return DatasetSettings(injection=InjectionSettings(**injection))


def recorder():
    windows = []

    def process(preprocessor, decompose, method, window):
        windows.append(window)
        return process_window(preprocessor, decompose, method, window)

    return windows, process


def run(readings, seed=0, config=None, **build):
    builder = LabelledWindows(config or settings())
    return builder.build(builder.read("src", readings), seed, **build)


def assert_same(a, b):
    for field in dataclasses.fields(a):
        x, y = getattr(a, field.name), getattr(b, field.name)
        if isinstance(x, np.ndarray):
            np.testing.assert_array_equal(x, y, err_msg=field.name)
        elif dataclasses.is_dataclass(x):
            assert_same(x, y)
        else:
            assert x == y, field.name


def window_of(sensor: Sensor, base: datetime) -> int:
    return int((sensor.timestamp - base).total_seconds()) // 20


def test_time_is_rescaled_to_one_slot_per_second():
    readings = stream()
    start = min(r.sensor.timestamp for r in readings)
    expected = {
        (r.sensor.sensor_id, r.sensor.metadata.packet_order): (
            start
            + timedelta(
                seconds=float(
                    np.round((r.sensor.timestamp - start).total_seconds() / 31)
                )
            ),
            r.sensor.value,
        )
        for r in readings
        if r.sensor.metadata
    }
    windows, process = recorder()
    run(readings, config=settings(inject_share=0.0), process=process)
    seen = [s for window in windows for s in window]
    assert len(seen) == len(readings)
    for s in seen:
        assert s.metadata is not None
        assert (s.timestamp, s.value) == expected[
            (s.sensor_id, s.metadata.packet_order)
        ]


def test_a_window_holds_every_sensor_of_its_20_slots():
    readings = stream()
    expected: defaultdict[int, set] = defaultdict(set)
    for r in readings:
        slot = round((r.sensor.timestamp - START).total_seconds() / 31)
        expected[slot // 20].add((r.sensor.sensor_id, r.sensor.metadata.packet_order))
    windows, process = recorder()
    run(readings, process=process)
    assert len(windows) == len(expected)
    base = min(r.sensor.timestamp for r in readings)
    for window in windows:
        numbers = {window_of(s, base) for s in window}
        assert len(numbers) == 1
        got = {(s.sensor_id, s.metadata.packet_order) for s in window if s.metadata}
        assert got == expected[numbers.pop()]


def test_every_row_keeps_the_full_dataset_name():
    # a <U1 array would cut "src" to "s"
    dataset = run(stream()).dataset
    assert dataset[0] == "src"
    assert set(dataset) == {"src"}


def test_unsorted_input_gives_the_same_windows():
    readings = stream()
    config = settings(inject_share=0.0)
    a, b = run(readings, config=config), run(readings[::-1], config=config)
    order_a = np.lexsort((a.sensor_id, a.window))
    order_b = np.lexsort((b.sensor_id, b.window))
    for field in ("sensor_id", "window", "kind", "train", "features"):
        np.testing.assert_array_equal(
            getattr(a, field)[order_a], getattr(b, field)[order_b], err_msg=field
        )


@pytest.mark.parametrize("seed", range(5))
def test_a_source_fault_window_is_never_injected(seed):
    labels = {("1", "temperature", 25): Anomalies.SENSOR_MALFUNCTION}
    data = run(stream(labels=labels), seed)
    row = (data.sensor_id == "src:1:temperature") & (data.window == 1)
    assert row.sum() == 1
    assert data.kind[row][0] == "real_fault"
    assert data.real_fault[row][0]
    assert data.source_label[row][0] == "sensor_malfunction"
    assert not data.label[row][0]
    assert data.real_fault.sum() == 1


def test_event_windows_are_kept_apart():
    events = {("2", "humidity", slot) for slot in range(40, 46)}
    for seed in range(5):
        data = run(stream(events=events), seed, config=settings(inject_share=1.0))
        row = (data.sensor_id == "src:2:humidity") & (data.window == 2)
        assert data.kind[row][0] == "event"
        assert data.event[row][0]
        assert not data.label[row][0]
        assert data.event.sum() == 1


def test_a_window_with_a_fault_and_an_event_is_a_real_fault():
    labels = {("2", "humidity", 41): Anomalies.SENSOR_MALFUNCTION}
    events = {("2", "humidity", slot) for slot in range(40, 46)}
    data = run(stream(labels=labels, events=events))
    row = (data.sensor_id == "src:2:humidity") & (data.window == 2)
    assert data.kind[row][0] == "real_fault"
    assert data.real_fault[row][0]
    assert not data.event[row][0]


def test_every_asked_kind_is_injected():
    readings = stream(stations=tuple(str(i) for i in range(6)), slots=400)
    data = run(readings, config=settings(inject_share=1.0))
    assert set(ALL_KINDS) <= set(data.kind)


def test_same_seed_same_dataset():
    readings = stream()
    assert_same(run(readings, 1), run(readings, 1))
    assert not np.array_equal(run(readings, 1).kind, run(readings, 2).kind)


def test_read_once_build_twice():
    readings = stream()
    builder = LabelledWindows(settings())
    data = builder.read("src", readings)
    assert_same(builder.build(data, 1), run(readings, 1))
    assert_same(builder.build(data, 2), run(readings, 2))


FLAT_KEYS = {
    "slot", "value", "packet_order", "lat", "lon", "label", "event", "sensor",
    "sensor_id", "cluster_id", "unit", "sensor_type", "station", "lower_range",
    "upper_range", "label_names", "source", "start", "sample_seconds",
    "every_hours", "block_hours",
}  # fmt: skip


def test_the_cache_keys_are_unchanged():
    data = LabelledWindows(settings()).read("src", stream())
    assert set(data.to_arrays()) == FLAT_KEYS


def test_source_readings_round_trip_through_npz(tmp_path):
    builder = LabelledWindows(settings())
    data = builder.read("src", stream())
    np.savez(tmp_path / "src.npz", **data.to_arrays())
    with np.load(tmp_path / "src.npz") as loaded:
        back = SourceReadings.from_arrays(loaded)
    assert_same(back, data)
    assert_same(builder.build(back, 3), builder.build(data, 3))


def test_the_info_loads_without_the_readings(tmp_path):
    data = LabelledWindows(settings()).read("src", stream())
    np.savez(tmp_path / "src.npz", **data.to_arrays())
    # only ReadInfo's keys: a stray access to a readings or sensors key raises KeyError
    columns = {*data.readings.to_arrays(), *data.sensors.to_arrays()}
    with np.load(tmp_path / "src.npz") as loaded:
        only_info = {k: loaded[k] for k in loaded.files if k not in columns}
    info = ReadInfo.from_arrays(only_info)
    assert str(info.source) == "src" and float(info.sample_seconds) == 31.0


def test_split_is_by_time():
    data = run(stream(slots=400))
    assert data.window[data.train].max() < data.window[~data.train].min()
    eligible = data.kind != "real_fault"
    stations = np.array([s.split(":")[1] for s in data.sensor_id])
    pairs = set(zip(data.window[eligible], stations[eligible], strict=True))
    trained = {pair for pair in pairs if pair[0] <= data.window[data.train].max()}
    assert abs(len(trained) / len(pairs) - 0.7) <= 0.05


def test_spreads_use_training_windows_only():
    readings = stream(slots=400)
    first = run(readings)
    cut = START + timedelta(seconds=int(first.window[~first.train].min()) * 20 * 31)
    scaled = [
        dataclasses.replace(
            r, sensor=r.sensor.model_copy(update={"value": r.sensor.value * 1000})
        )
        if r.sensor.timestamp >= cut
        else r
        for r in readings
    ]
    second = run(scaled)
    assert second.spreads == first.spreads
    assert not np.array_equal(second.features, first.features)


def test_double_reading_in_one_slot_stays_finite():
    readings = stream(slots=60)
    twin = readings[6 * 30]  # station 1's temperature at slot 30
    assert twin.sensor.metadata is not None
    # a new packet_order (orders run 1-60), 5 s later: the same rescaled slot
    extra = dataclasses.replace(
        twin,
        sensor=twin.sensor.model_copy(
            update={
                "value": twin.sensor.value + 0.5,
                "timestamp": twin.sensor.timestamp + timedelta(seconds=5),
                "metadata": twin.sensor.metadata.model_copy(
                    update={"packet_order": 61}
                ),
            }
        ),
    )
    windows, process = recorder()
    data = run([*readings, extra], process=process)
    got = [
        s
        for window in windows
        for s in window
        if s.sensor_id == twin.sensor.sensor_id
        and s.metadata
        and s.metadata.packet_order in (twin.sensor.metadata.packet_order, 61)
    ]
    assert len(got) == 2
    assert got[0].timestamp == got[1].timestamp
    assert np.isfinite(data.features).all()


def test_a_double_report_keeps_each_rows_types_together():
    readings = stream(slots=60, jitter=0.0)  # a row's types share its time
    first = readings[6 * 30 : 6 * 30 + 2]  # station 1 at slot 30
    second = [
        dataclasses.replace(
            r,
            sensor=r.sensor.model_copy(
                update={
                    "timestamp": r.sensor.timestamp + timedelta(seconds=5),
                    "metadata": r.sensor.metadata.model_copy(
                        update={"packet_order": 61}
                    ),
                }
            ),
        )
        for r in first
        if r.sensor.metadata
    ]
    windows, process = recorder()
    run([*readings, *second], process=process)
    base = min(r.sensor.timestamp for r in readings)
    got = [
        (s.metadata.type, s.metadata.packet_order)
        for window in windows
        for s in window
        if s.sensor_id.startswith("src:1:")
        and s.metadata
        and s.timestamp == base + timedelta(seconds=30)
    ]
    # the first row's two types, then the second row's, as the notebook's messages
    assert got == [
        ("temperature", 31),
        ("humidity", 31),
        ("temperature", 61),
        ("humidity", 61),
    ]


def test_a_type_seen_only_in_test_windows_gets_no_fault(windows_log):
    readings = [
        *stream(slots=200),
        *stream(types=("late",), slots=40, first_slot=160, seed=1),
    ]
    data = run(readings, config=settings(inject_share=1.0))  # all 10 kinds
    late = data.sensor_type == "late"
    assert late.any()
    assert not data.train[late].any()
    assert (data.kind[late] == "").all()
    assert data.label[~late].any()
    warnings = [r for r in windows_log.records if r.levelno == logging.WARNING]
    assert any(
        r.getMessage() == "Not injecting types with no clean training window"
        and r.types == ["late"]
        for r in warnings
    )


def test_uniform_without_a_range_uses_the_training_range():
    readings = stream(slots=200)
    windows, process = recorder()
    data = run(
        readings,
        config=settings(inject_share=1.0),
        kinds=("uniform",),
        process=process,
    )
    assert (data.kind == "uniform").any()
    train_windows = set(data.window[data.train].tolist())
    base = min(r.sensor.timestamp for r in readings)
    for kind in TYPES:
        values = [
            r.sensor.value
            for r in readings
            if r.sensor.metadata
            and r.sensor.metadata.type == kind
            and window_of_slot(r, base) in train_windows
        ]
        low, high = min(values), max(values)
        injected = {
            (str(sid), int(w))
            for sid, w, k, t in zip(
                data.sensor_id, data.window, data.kind, data.sensor_type, strict=True
            )
            if k == "uniform" and t == kind
        }
        faulted = [
            s.value
            for window in windows
            for s in window
            if (s.sensor_id, window_of(s, base)) in injected
        ]
        assert faulted
        assert low <= min(faulted) and max(faulted) <= high
    assert not data.out_of_range.any()


def window_of_slot(reading: LabeledReading, base: datetime) -> int:
    slot = round((reading.sensor.timestamp - base).total_seconds() / 31)
    return slot // 20


def test_natural_key_orders_numbers_by_value():
    assert sorted(["10", "2", "1"], key=natural_key) == ["1", "2", "10"]
    assert natural_key("a10") > natural_key("a9")


def test_sensors_rank_by_natural_station_then_first_seen_type():
    readings = stream(stations=("10", "2"), types=("humidity", "temperature"))
    data = LabelledWindows(settings()).read("src", readings)
    assert data.sensors.sensor_id.tolist() == [
        "src:2:humidity", "src:2:temperature", "src:10:humidity", "src:10:temperature",
    ]  # fmt: skip


def test_read_keeps_each_sensors_cluster_unit_type_and_range():
    units = {"temperature": "C", "humidity": "%"}
    readings = []
    for r in stream(stations=("1",), slots=5, ranges={"temperature": (-10.0, 50.0)}):
        metadata = r.sensor.metadata
        assert metadata
        sensor = r.sensor.model_copy(update={"unit": units[metadata.type]})
        readings.append(dataclasses.replace(r, sensor=sensor))
    sensors = LabelledWindows(settings()).read("src", readings).sensors
    rows = {
        sensor_id: (cluster, unit, kind, low, high)
        for sensor_id, cluster, unit, kind, low, high in zip(
            sensors.sensor_id.tolist(),
            sensors.cluster_id.tolist(),
            sensors.unit.tolist(),
            sensors.sensor_type.tolist(),
            sensors.lower_range.tolist(),
            sensors.upper_range.tolist(),
            strict=True,
        )
    }
    assert rows["src:1:temperature"] == (
        "src:temperature", "C", "temperature", -10.0, 50.0,
    )  # fmt: skip
    humidity = rows["src:1:humidity"]
    assert humidity[:3] == ("src:humidity", "%", "humidity")
    assert np.isnan(humidity[3:]).all()


def test_read_keeps_only_sampled_hours():
    readings = stream(stations=("1",), types=("temperature",), slots=3 * 86400 // 31)
    config = settings(sample_every_hours={"src": 29}, block_hours=2)
    data = LabelledWindows(config).read("src", readings)
    kept = [
        r.sensor.timestamp.timestamp()
        for r in readings
        if in_sampled_block(r.sensor.timestamp.timestamp(), 29, 2)
    ]
    assert 0 < len(kept) < len(readings)
    start = min(kept)
    expected = np.round((np.array(kept) - start) / 31).astype(np.int64)
    np.testing.assert_array_equal(data.readings.slot, np.sort(expected))
    assert float(data.info.start) == start
    assert int(data.info.every_hours) == 29
    assert int(data.info.block_hours) == 2


def test_loader_and_read_filters_agree(tmp_path):
    lines = ["timestamp,node_id,subsystem,sensor,parameter,value_raw,value_hrf"]
    for i in range(2 * 144):  # every 10 minutes over two days
        moment = datetime(2018, 1, 1, tzinfo=UTC) + timedelta(minutes=10 * i)
        lines.append(
            f"{moment:%Y/%m/%d %H:%M:%S},001e0610e532,chemsense,co,concentration,"
            f"10,{i % 7}.5"
        )
    folder = tmp_path / "array_of_things"
    folder.mkdir()
    (folder / ArrayOfThingsSettings().data_file).write_bytes(
        gzip.compress(("\n".join(lines) + "\n").encode())
    )
    injection = InjectionSettings(
        sample_seconds={"array_of_things": 600.0},
        sample_every_hours={"array_of_things": 5},
        block_hours=2,
    )
    config = DatasetSettings(root=str(tmp_path), injection=injection)
    readings = list(ArrayOfThings(config).load())
    assert 0 < len(readings) < 2 * 144
    data = LabelledWindows(config).read("array_of_things", readings)
    assert len(data.readings.slot) == len(readings)


def test_fast_sensors_are_dropped(windows_log):
    readings = stream()
    fast = [
        LabeledReading(
            sensor=Sensor(
                sensor_id="src:fast:temperature",
                cluster_id="src:temperature",
                value=1.0,
                unit="u",
                timestamp=START + timedelta(seconds=10 + 3 * i),
                metadata=Metadata(packet_order=i + 1, type="temperature"),
            )
        )
        for i in range(500)
    ]
    builder = LabelledWindows(settings())
    with_fast = builder.read("src", [*readings, *fast])
    assert "src:fast:temperature" not in with_fast.sensors.sensor_id
    assert_same(with_fast, builder.read("src", readings))
    warnings = [r for r in windows_log.records if r.levelno == logging.WARNING]
    assert any(
        r.getMessage() == "Dropped sensors faster than their source's sample period"
        and r.source == "src"
        and r.sensors == ["src:fast:temperature"]
        for r in warnings
    )


def test_all_sensors_fast_raises_with_the_sensors_and_logs_nothing(windows_log):
    names = ["src:fast:temperature", "src:fast:humidity"]
    readings = [
        LabeledReading(
            sensor=Sensor(
                sensor_id=name,
                cluster_id="src:temperature",
                value=1.0,
                unit="u",
                timestamp=START + timedelta(seconds=10 + 3 * i),
                metadata=Metadata(packet_order=i + 1, type=name.split(":")[2]),
            )
        )
        for name in names
        for i in range(500)
    ]
    with pytest.raises(ValueError, match="no reading left") as error:
        LabelledWindows(settings()).read("src", readings)
    assert all(name in str(error.value) for name in names)
    assert not [r for r in windows_log.records if r.levelno == logging.WARNING]


def test_read_logs_what_it_kept(windows_log):
    data = LabelledWindows(settings()).read("src", stream())
    record = next(
        r
        for r in windows_log.records
        if r.levelno == logging.INFO and r.getMessage() == "Read source"
    )
    assert record.source == "src"
    assert record.readings == len(data.readings.slot)
    assert record.sensors == len(data.sensors.sensor_id)
    assert record.sample_seconds == 31.0
    assert record.every_hours == 1


def test_build_logs_the_window_counts(windows_log):
    # 3 stations x 2 types x 10 windows = 60 sensor-windows; 2 hold a source label
    # and 3 an event, so 55 are eligible and 40% of them (22) are chosen
    labels = {
        ("1", "temperature", 25): Anomalies.SENSOR_MALFUNCTION,
        ("2", "humidity", 65): Anomalies.SENSOR_IS_DOWN,
    }
    events = {("2", "temperature", slot) for slot in range(40, 46)}
    events |= {("10", "humidity", 100), ("1", "humidity", 140)}
    builder = LabelledWindows(settings(inject_share=0.4))
    dataset = builder.build(
        builder.read("src", stream(labels=labels, events=events)), 7
    )
    record = next(
        r
        for r in windows_log.records
        if r.levelno == logging.INFO and r.getMessage() == "Built windows"
    )
    assert record.source == "src"
    assert record.seed == 7
    assert (record.windows, record.injected, record.real_faults, record.events) == (
        60,
        22,
        2,
        3,
    )
    assert record.windows == len(dataset.label)
    assert record.injected == int(dataset.label.sum())
    for count in (record.windows, record.injected, record.real_faults, record.events):
        assert type(count) is int


def test_build_logs_the_training_spreads_at_debug(windows_log):
    dataset = run(stream())
    record = next(
        r
        for r in windows_log.records
        if r.levelno == logging.DEBUG and r.getMessage() == "Training statistics"
    )
    assert record.spreads == dataset.spreads


def test_max_readings_stops_reading(windows_log):
    calls = 0

    def counted():
        nonlocal calls
        for reading in stream(slots=100):  # 600 readings
            calls += 1
            yield reading

    data = LabelledWindows(settings(max_readings=100)).read("src", counted())
    assert len(data.readings.slot) == 100
    assert calls <= 101
    warnings = [r for r in windows_log.records if r.levelno == logging.WARNING]
    assert any(
        r.getMessage() == "Stopped reading at max_readings"
        and r.source == "src"
        and r.max_readings == 100
        for r in warnings
    )


def test_missing_sample_period_raises():
    with pytest.raises(ValueError, match="other"):
        LabelledWindows(settings()).read("other", stream(slots=5))


def test_empty_input_raises():
    with pytest.raises(ValueError, match=r"src: no reading \(empty source"):
        LabelledWindows(settings()).read("src", [])


def test_a_source_without_an_eligible_window_raises():
    readings = stream(slots=2)  # 2 readings per sensor-window, under min_samples
    builder = LabelledWindows(settings())
    with pytest.raises(ValueError, match=r"no clean sensor-window with 4\+ readings"):
        builder.build(builder.read("src", readings), 0)


def test_empty_kinds_inject_nothing():
    data = run(stream(), config=settings(inject_share=1.0), kinds=())
    assert not data.label.any()
    assert set(data.kind) <= {"", "real_fault", "event"}


def sampled_stream():
    # 12 hours at 31 s: hours 0, 3, 6 and 9 are sampled (block_hours=1, every 3)
    return stream(stations=("1",), types=("temperature",), slots=12 * 3600 // 31)


def test_max_readings_counts_kept_readings_after_sampling(windows_log):
    readings = sampled_stream()
    config = settings(sample_every_hours={"src": 3}, block_hours=1, max_readings=500)
    data = LabelledWindows(config).read("src", readings)
    in_block = [
        r for r in readings if in_sampled_block(r.sensor.timestamp.timestamp(), 3, 1)
    ]
    assert 0 < len(in_block) < 500 < len(readings)  # the cap needs skipped ones
    assert len(data.readings.slot) == len(in_block)
    assert not [r for r in windows_log.records if r.levelno == logging.WARNING]


def test_max_readings_warns_when_the_kept_readings_exceed_it(windows_log):
    config = settings(sample_every_hours={"src": 3}, block_hours=1, max_readings=300)
    data = LabelledWindows(config).read("src", sampled_stream())
    assert len(data.readings.slot) == 300
    assert any(
        r.getMessage() == "Stopped reading at max_readings" for r in windows_log.records
    )


def test_blocks_shorter_than_a_window_warn(windows_log):
    config = settings(sample_every_hours={"src": 3}, block_hours=1, window_slots=200)
    LabelledWindows(config).read("src", sampled_stream())
    warnings = [
        r
        for r in windows_log.records
        if r.levelno == logging.WARNING and r.getMessage().startswith("Sampled blocks")
    ]
    assert warnings  # caplog may hold the record twice (module and root handler)
    assert warnings[0].source == "src"
    assert warnings[0].block_hours == 1
    assert warnings[0].window_seconds == 200 * 31.0


def test_blocks_longer_than_a_window_do_not_warn(windows_log):
    # 20 slots of 31 s are 620 s: a 1 h block holds many windows
    config = settings(sample_every_hours={"src": 3}, block_hours=1)
    LabelledWindows(config).read("src", sampled_stream())
    assert not [r for r in windows_log.records if r.levelno == logging.WARNING]


def test_build_uses_the_injected_pipeline():
    preprocessor, decompose = PreProcess("fft"), Decompose()
    seen = []

    def process(p, d, method, window):  # test-only recorder
        seen.append((p, d, method))
        return process_window(p, d, method, window)

    builder = LabelledWindows(
        settings(), preprocessor=preprocessor, decompose=decompose
    )
    builder.build(builder.read("src", stream()), 0, process=process)
    assert {(id(p), id(d), m) for p, d, m in seen} == {
        (id(preprocessor), id(decompose), "fft")
    }


def test_method_alone_still_picks_the_features():
    builder = LabelledWindows(settings(), method="fft")
    data = builder.build(builder.read("src", stream()), 0)
    assert data.features.shape[1] == len(PreProcess("fft").feature_keys())


def test_an_injected_preprocessor_wins_over_method():
    builder = LabelledWindows(settings(), method="haar", preprocessor=PreProcess("fft"))
    data = builder.build(builder.read("src", stream()), 0)
    assert builder.method == "fft"
    assert data.features.shape[1] == len(PreProcess("fft").feature_keys())


def test_min_samples_can_be_set_on_the_builder():
    readings = stream(slots=3)  # 3 readings per sensor-window
    default = LabelledWindows(settings())
    with pytest.raises(ValueError, match=r"no clean sensor-window with 4\+ readings"):
        default.build(default.read("src", readings), 0)
    builder = LabelledWindows(settings(), min_samples=3)
    assert len(builder.build(builder.read("src", readings), 0).features) > 0


@pytest.mark.parametrize(
    ("period", "window_seconds", "slots"),
    [(1.0, 20.0, 20), (0.1, 20.0, 200), (0.3, 20.0, 67), (0.1, 0.04, 1)],
)
def test_window_seconds_sets_the_slots_per_window(period, window_seconds, slots):
    config = settings(sample_seconds={"src": period}, window_seconds=window_seconds)
    builder = LabelledWindows(config)
    data = builder.read("src", stream(period=period, jitter=0.0, slots=400))
    assert builder.window_groups(data, 1).window_slots == slots


def test_window_seconds_keeps_the_real_spacing():
    # 0.1 s readings: a 20 s window is 200 readings over 19.9 s, as live
    windows, process = recorder()
    config = settings(
        sample_seconds={"src": 0.1}, window_seconds=20.0, inject_share=0.0
    )
    builder = LabelledWindows(config)
    data = builder.read("src", stream(period=0.1, jitter=0.0, slots=400))
    builder.build(data, 0, process=process)
    assert windows
    for window in windows:
        times = defaultdict(list)
        for s in window:
            times[s.sensor_id].append(s.timestamp)
        for stamps in times.values():
            assert len(stamps) == 200
            span = (max(stamps) - min(stamps)).total_seconds()
            assert span == pytest.approx(19.9)


def test_window_seconds_unset_matches_the_same_slot_count():
    # 20 slots of 31 s either way: same windows, split, draws and labels
    a = run(stream())
    b = run(stream(), config=settings(window_seconds=620.0))
    for name in ("window", "train", "kind", "label", "sensor_id"):
        np.testing.assert_array_equal(getattr(a, name), getattr(b, name), err_msg=name)


def test_blocks_shorter_than_window_seconds_warn(windows_log):
    config = settings(
        sample_every_hours={"src": 3}, block_hours=1, window_seconds=7200.0
    )
    LabelledWindows(config).read("src", sampled_stream())
    warnings = [
        r for r in windows_log.records if r.getMessage().startswith("Sampled blocks")
    ]
    assert warnings
    assert warnings[0].window_seconds == 232 * 31.0  # round(7200 / 31) slots


def test_process_window_rows_carry_the_short_window_keys():
    windows, process = recorder()
    config = settings(sample_seconds={"src": 1.0}, window_seconds=20.0)
    builder = LabelledWindows(config)
    builder.build(
        builder.read("src", stream(period=1.0, jitter=0.0)), 0, process=process
    )
    preprocessor = PreProcess()
    rows = process_window(preprocessor, Decompose(), "none", windows[0])
    expected = (*preprocessor.feature_keys("none"), *preprocessor.short_keys())
    assert len(rows) > 1
    for row in rows.values():
        assert tuple(row) == expected
        assert all(np.isfinite(value) for value in row.values())


def test_build_takes_the_long_pipeline():
    keys = get_settings().preprocess.long_keys
    long_preprocess = LongPreProcess(PreProcess(), Decompose(), LongWindowSettings())
    process = functools.partial(process_long_window, long_preprocess)
    builder = LabelledWindows(settings())
    data = builder.build(
        builder.read("src", stream()), 0, process=process, feature_keys=keys
    )
    assert data.features.shape[1] == len(keys)
