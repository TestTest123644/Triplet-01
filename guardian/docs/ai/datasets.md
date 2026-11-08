# Datasets, fault injection and labelled windows

This page covers the offline side of the AI code: reading real sensor datasets into one common shape, injecting labelled faults into them, and cutting them into feature rows a model can train on.

Code: `src/guardian_layer/ai/dataset_factory.py`, `src/guardian_layer/ai/interface/dataset_interface.py`, `src/guardian_layer/ai/datasets/`.
Config: `config/datasets.yaml`, read by `src/guardian_layer/utils/dataset_config.py`.

The datasets themselves are not in the repository. They live in a shared folder next to it (`../datasets` by default, see [Configuration](#configuration)).

## LabeledReading

Every loader yields `LabeledReading` objects (`utils/schema.py`):

| Field | Type | Meaning |
|---|---|---|
| `sensor` | `Sensor` | the reading, in the same shape as live telemetry |
| `label` | `Anomalies \| None` | the fault the source itself flags for this reading. `None` means the source flags nothing (normal) |
| `event` | `bool` | `True` inside a real-world event window (a NAB incident, a flood day). Not a fault: the label stays `None` |

`to_readings` (below) builds every loader's `Sensor` the same way:

- `sensor_id` is `"<source>:<station>:<kind>"`, for example `intel_lab:1:temperature`.
- `cluster_id` is `"<source>:<kind>"`, so one source's sensors of one kind share a cluster.
- `metadata.type` is the kind, `metadata.packet_order` counts from 1 per sensor, and `metadata.lat` / `metadata.lon` are set when the source has coordinates.

Building a `Sensor` runs `SensorValidator`. A row that live telemetry would reject raises `ValueError` in the loader too, so loaders filter bad values (with `parse_float`) before building readings.

## DatasetInterface

`DatasetInterface` (`ai/interface/dataset_interface.py`) is the abstract base of every loader.

| Member | Signature | Behaviour |
|---|---|---|
| constructor | `__init__(settings: DatasetSettings \| None = None)` | Keeps `settings`. With `None`, it reads `config/datasets.yaml` once through `get_dataset_settings()`. It opens no dataset file. |
| `load` | `load() -> Iterator[LabeledReading]` | Abstract. Reads the source and yields its readings in time order. |

## DatasetFactory

`DatasetFactory` (`ai/dataset_factory.py`) is a registry of loaders by name. Adding a source is one new module. The factory itself doesn't change.

| Member | Signature | Behaviour |
|---|---|---|
| `registry` | `ClassVar[dict[str, type[DatasetInterface]]]` | name → loader class |
| constructor | `__init__(settings: DatasetSettings \| None = None)` | Keeps `settings` and passes it to every loader it builds. `None` lets each loader read the shared settings. |
| `register` | `@classmethod register(name: str)`, a class decorator | Adds the class under `name`. Registering a different class under a taken name raises `ValueError("dataset already registered: <name>")`. The same class again (a module reload, for example Jupyter autoreload) replaces it. |
| `names` | `@classmethod names() -> list[str]` | Every registered name, sorted. |
| `create` | `create(name: str) -> DatasetInterface` | The named loader, built with this factory's settings. An unknown name raises `ValueError("unknown dataset: <name>")`. |
| `load` | `load(name: str) -> Iterator[LabeledReading]` | Same as `create(name).load()`. |

The loader modules import `dataset_factory` to register themselves. The factory imports `guardian_layer.ai.datasets` only when it's first needed (in `names` and `create`), so there's no import cycle.

```python
from guardian_layer.ai.dataset_factory import DatasetFactory

print(len(DatasetFactory.names()))  # 21
```

## Loaders

There are 20 loader modules and 21 registered names: `grid_frequency.py` registers a second name for its 100 ms recordings. Each class subclasses `DatasetInterface` and reads its own block of `config/datasets.yaml` through `self.settings.<name>`.

"Source labels" says what the source itself marks, before any fault is injected.

| Name | Class | Module | Domain | What it reads | Source labels |
|---|---|---|---|---|---|
| `array_of_things` | `ArrayOfThings` | `array_of_things.py` | air and weather | Array of Things, Chicago 2018-2019: street-pole nodes with many small sensors. 33 GB of gzip, streamed in time order. | none |
| `beijing` | `Beijing` | `beijing.py` | air and weather | Beijing multi-site air quality (UCI): 12 stations, hourly, 2013-2017. Pollutants and weather. | none |
| `bellinge` | `Bellinge` | `bellinge.py` | flooding | Bellinge (Odense) urban drainage: cleaned water depth, pump flow and rain gauges. | `flag_labels`: `outbound` → `out_of_range`, `frozen` / `frozen_high` / `outlier` → `sensor_malfunction` |
| `chicago_beach` | `ChicagoBeach` | `chicago_beach.py` | air and weather | Chicago Park District beach weather stations. Sorted in memory. | none |
| `emergency_calls` | `EmergencyCalls` | `emergency_calls.py` | flooding | Emergency calls during two real floods (NYC EMS and New Orleans, Hurricane Ida 2021), counted per zone per minute. | `event=True` on the flood days |
| `ett` | `Ett` | `ett.py` | power | ETT electricity transformer loads, every 15 minutes. | none |
| `grid_frequency` | `GridFrequency` | `grid_frequency.py` | power | Power grid frequency deviation from 50/60 Hz, one file per campaign, 1 s. Streamed. | none |
| `grid_frequency_100ms` | `GridFrequency100ms` | `grid_frequency.py` | power | The four 100 ms grid frequency recordings. A subclass of `GridFrequency` with its own settings block, so it keeps its 0.1 s period. | none |
| `intel_lab` | `IntelLab` | `intel_lab.py` | baseline | Intel Berkeley Lab: temperature, humidity and light from 54 motes (2004). Carries `lower_range` / `upper_range` in metadata. | `sensor_malfunction` when the mote's voltage is below `voltage_min` |
| `jena` | `Jena` | `jena.py` | air and weather | Jena weather station (Max Planck Institute roof), every 10 minutes, 2023-2024. | none |
| `melbourne` | `Melbourne` | `melbourne.py` | air and weather | City of Melbourne microclimate sensors. Sorted in memory. A second packet in the same second is dropped. | none |
| `nab` | `Nab` | `nab.py` | traffic | NAB: Minneapolis traffic detectors and an office thermometer. | `event=True` inside NAB's windows |
| `noaa_tides` | `NoaaTides` | `noaa.py` | flooding | NOAA CO-OPS tide stations: water level and weather products, 6-minute data. | `flag_labels`: `X` / `N` / `L` → `out_of_range`, `R` / `F` → `sensor_malfunction` |
| `sensor_community` | `SensorCommunity` | `sensor_community.py` | air and weather | Sensor.Community citizen sensors. The sensor model is part of the kind (`bme280_temperature`). | none |
| `traffic_speeds` | `TrafficSpeeds` | `traffic_speeds.py` | traffic | Chicago bus-GPS region speeds and NYC DOT link speeds. Sorted in memory. | none |
| `uci_air_quality` | `UciAirQuality` | `uci_air_quality.py` | air and weather | UCI Air Quality roadside multisensor, hourly, 2004-2005. `-200` is a missing value. | none |
| `uci_household` | `UciHousehold` | `uci_household.py` | power | UCI household electric power, one house, every minute, 2006-2010. `?` is a missing value. | none |
| `uci_load` | `UciLoad` | `uci_load.py` | power | UCI ElectricityLoadDiagrams: 370 Portuguese meters, 15-minute load. About 52 million readings, streamed. | none |
| `uk_flood` | `UkFlood` | `uk_flood.py` | flooding | UK Environment Agency river level and rainfall. Sorted in memory. | none |
| `uk_traffic` | `UkTraffic` | `uk_traffic.py` | traffic | UK motorway traffic counts and speeds (WebTRIS), one JSON file per site. | none |
| `usgs` | `Usgs` | `usgs.py` | flooding | USGS river sites: water quality sondes and gage height (`.rdb`). | none |

Each loader's docstring and its block in `config/datasets.yaml` list the columns it reads and the ones it skips, with the reason (`skipped:`).

## Shared helpers

`ai/datasets/common.py` holds the helpers every loader uses.

| Function | Signature | Behaviour |
|---|---|---|
| `dataset_path` | `dataset_path(settings: DatasetSettings, *parts: str) -> Path` | `<project root>/<settings.root>/<parts>`. An absolute `root` replaces the project root. Raises `FileNotFoundError` if the file isn't there. |
| `open_archive` | `open_archive(path: Path, *members: str) -> zipfile.ZipFile` | The zip at `path`, or a zip nested inside it through `members`. A nested zip is held in memory. |
| `zip_names` | `zip_names(path: Path, *members: str) -> list[str]` | Member files of a (nested) zip, sorted, without folders or `__MACOSX/` entries. |
| `open_text` | `open_text(path: Path, *members: str, encoding: str = "utf-8-sig") -> TextIO` | A text stream of a plain, `.gz` or zipped file. Bad bytes are replaced. `members` is the path inside the zips. |
| `to_readings` | `to_readings(source: str, rows: Iterable[DatasetRow], sort: bool = False) -> Iterator[LabeledReading]` | Turns parsed `DatasetRow`s into readings (ids as described in [LabeledReading](#labeledreading)). `sort=True` orders rows by `(timestamp, station, kind)` first. A row not later than its sensor's last one is dropped, and the count is logged as a warning. |
| `to_utc` | `to_utc(text: str, fmt: str \| None = None) -> datetime` | An aware UTC time. ISO 8601 when `fmt` is `None`. A naive time is read as UTC. |
| `in_sampled_block` | `in_sampled_block(epoch_seconds: float, every_hours: int, block_hours: int) -> bool` | `True` in the first `block_hours` of each `every_hours` cycle. |
| `sampled_lines` | `sampled_lines(lines: Iterable[str], every_hours: int, block_hours: int, hour_format: str) -> Iterator[str]` | Keeps the lines whose first 13 characters (an hour) fall in a sampled block, so dropped lines are never parsed as CSV. A prefix that doesn't parse is kept. |
| `parse_float` | `parse_float(text: str, missing: Collection[str] = ()) -> float \| None` | The number, or `None` for empty, non-numeric, NaN, inf or a listed sentinel. A numeric sentinel matches by value (`"-200"` also catches `-200.0`). |

`DatasetRow` (`utils/schema.py`) is one parsed value before it becomes a reading: `timestamp`, `station`, `kind`, `unit`, `value`, and the optional `lat`, `lon`, `label` (default `None`) and `event` (default `False`).

## Fault injection

The real datasets have few labelled faults, so the training data gets faults injected into clean windows. The sizes are multiples of each sensor type's normal spread (the median standard deviation inside one clean training window of that type), so a fault means the same thing for any type.

### Fault kinds

`ai/datasets/fault.py` has one function per kind. Each takes a `FaultContext` and returns a faulty copy of the window. `FAULT_REGISTRY: dict[str, FaultHandler]` maps the kind name to its function.

`FaultContext` (`utils/schema.py`) holds `values` (the window), `rng` (a `numpy.random.Generator`), `settings` (`FaultSettings`), `spread` (the type's normal spread), `sign` (+1 or -1, drawn before the function runs) and `value_range` (`(low, high)`).

| Kind | Function | What it does | Settings it reads |
|---|---|---|---|
| `stuck` | `apply_stuck` | Frozen at one value from a random point to the end | none |
| `spike` | `apply_spike` | 1 to `spike_count_max` readings jump by `spike_spreads` × spread | `spike_count_max`, `spike_spreads` |
| `drift` | `apply_drift` | An offset that grows linearly to `drift_spreads` × spread at the window end | `drift_spreads` |
| `noise` | `apply_noise` | Extra Gaussian jitter of `noise_spreads` × spread | `noise_spreads` |
| `oscillation` | `apply_oscillation` | A sine added on top, period `period_readings` readings | `oscillation_spreads`, `period_readings` |
| `spoof` | `apply_spoof` | The whole window shifted by `spoof_spreads` × spread. Neighbours stay unchanged. | `spoof_spreads` |
| `sine` | `apply_sine` | Replaced by a noise-free sine around the window mean | `sine_spreads`, `period_readings` |
| `ramp` | `apply_ramp` | Replaced by a perfect straight line around the mean | `ramp_spreads` |
| `pattern` | `apply_pattern` | Replaced by a short random motif (`pattern_values` long), repeated | `pattern_values`, `pattern_spreads` |
| `uniform` | `apply_uniform` | Replaced by random values over the type's whole `value_range` | none |

The first six are sensor faults (`fault_kinds`). The last four are fabricated signals that aren't sensor data at all (`fabricated_kinds`).

### FaultInjector

`FaultInjector` (`ai/datasets/injection.py`):

| Member | Signature | Behaviour |
|---|---|---|
| constructor | `__init__(settings: InjectionSettings \| None = None)` | `None` uses `get_dataset_settings().injection`. |
| `apply_fault` | `apply_fault(values: FloatArray, kind: str, spread: float, rng: np.random.Generator, value_range: tuple[float, float]) -> FloatArray` | A faulty copy of one window (needs 2 or more readings). Draws the sign, then calls the kind's function. An unknown kind raises `ValueError("unknown fault kind: <kind>")`. Fabricated kinds are clipped to `value_range`. |
| `inject` | `inject(values: FloatArray, targets: list[IntArray], types: list[str], spreads: dict[str, float], ranges: dict[str, tuple[float, float]], rng: np.random.Generator, kinds: tuple[str, ...]) -> tuple[FloatArray, list[str]]` | Faults a random `inject_share` of the targets (row indices of each window) in a copy of `values`. Returns the copy and one kind per target, with `""` for a target left clean. A fault that changes nothing (stuck on a flat window) counts as clean. With empty `kinds` it draws nothing. |
| `within_window_spreads` | `within_window_spreads(windows: dict[str, list[FloatArray]], min_samples: int) -> dict[str, float]` | Per type, the median standard deviation inside one window. Windows with fewer than `min_samples` readings or a std of 0 are skipped. A type with no usable window gets 0.0 and a warning, and its spread-sized faults then change nothing. |

The random draws follow the research notebook's order, so a seeded run gives the notebook's injected values.

```python
import numpy as np
from guardian_layer.ai.datasets.injection import FaultInjector

values = np.array([20.0, 20.1, 19.9, 20.2, 20.0, 19.8])
faulty = FaultInjector().apply_fault(
    values, "spike", spread=0.15, rng=np.random.default_rng(0), value_range=(0.0, 50.0)
)
print(faulty - values)  # zeros except the spiked readings
```

## LabelledWindows

`LabelledWindows` (`ai/datasets/windows.py`) turns any loader's readings into a `WindowDataset`: one feature row per sensor-window, with injected faults and their labels. It's the bridge between the loaders above and the model in [inference.md](inference.md#baselinemodel).

```python
LabelledWindows(
    settings: DatasetSettings | None = None,
    method: str | None = None,          # decomposition method, passed to PreProcess
    injector: FaultInjector | None = None,
    preprocessor: PreProcess | None = None,
    decompose: Decompose | None = None,
    min_samples: int | None = None,      # default: Decompose settings' min_samples
)
```

It has two public steps:

1. **`read(source: str, readings: Iterable[LabeledReading]) -> SourceReadings`**: slow, run once per source. It reads the stream into numpy columns on the source's sample-period time grid.
   - `check_source` takes the period from `injection.sample_seconds`. A source missing there raises `ValueError`.
   - `collect` gathers the readings. A source listed in `sample_every_hours` keeps only one `block_hours` block in each `every_hours`.
   - `drop_fast` drops sensors faster than `fast_sensor_share` × the period.
   - `rank_sensors` and `assemble` order the sensors the way the notebook ordered its targets.
2. **`build(data: SourceReadings, seed: int, kinds: tuple[str, ...] | None = None, process: Process = process_window, feature_keys: tuple[str, ...] | None = None) -> WindowDataset`**: fast, run once per seed.
   - `window_groups` cuts the readings into sensor-windows and marks each one as a real fault, an event, clean, or clean with enough readings.
   - `split_by_time` sends the earliest `train_share` of windows to training, with one time cut for the whole source.
   - `training_stats` computes the spreads and fallback ranges from clean training windows only.
   - `inject_groups` injects the faults. It's the only step that draws random numbers.
   - `window_features` runs `process` on each window and keeps `feature_keys`.

   `kinds` defaults to `fault_kinds` + `fabricated_kinds`. `feature_keys` defaults to `PreProcess.feature_keys()`.

`WindowDataset` (`utils/schema.py`) has one entry per row:

| Field | Meaning |
|---|---|
| `features` | the feature matrix |
| `label` | `True` if an injected kind |
| `kind` | `""`, the injected kind, `"real_fault"` or `"event"` |
| `sensor_type` | the sensor-window's type |
| `real_fault` | the source flagged a fault |
| `event` | inside a real-world event window |
| `source_label` | the source's own label, or `""` |
| `train` | in the training split |
| `out_of_range` | the window went out of its range |
| `sensor_id` | the sensor |
| `window` | the window index |
| `dataset` | the source name |
| `spreads` | the per-type spreads used (one dict for the whole dataset) |

Module helpers:

- `process_window(preprocessor, decompose, method, window) -> dict[str, dict[str, float]]` is the default `process`. It maps each `sensor_id` in a window to its features: the `feature_keys` first, then the short-window keys. It stands in for the orchestrator until that exists. See [pipelines.md](pipelines.md#preprocess).
- `natural_key(text) -> tuple[str | int, ...]` sorts digit runs as numbers (`"2"` before `"10"`).

## Configuration

`DatasetSettings` (`utils/dataset_config.py`) is a pydantic `BaseSettings` with one field per loader, plus `root`, `injection` and `long_injection`. It reads `config/datasets.yaml`, or the file named by the `GUARDIAN_DATASETS_CONFIG` environment variable. It ignores other environment variables, so settings built in a test keep the defaults. `get_dataset_settings()` caches the result. Unknown keys are rejected (`extra="forbid"`).

| Key | Default | Effect |
|---|---|---|
| `root` | `../datasets` | the datasets folder, relative to the project root or absolute |
| `<loader name>` | per loader | that loader's files, columns, units, missing-value markers, flag labels and skipped columns |

`DatasetSettings.long_window(window_seconds: float) -> DatasetSettings` returns a copy with `long_injection` merged into `injection` and windows of `window_seconds`, for the long-window model.

### `injection` (`InjectionSettings`)

| Key | Default | Effect |
|---|---|---|
| `window_slots` | `20` | time slots per window when `window_seconds` is unset |
| `window_seconds` | `null` | window length in seconds. When set, it replaces `window_slots` with `max(1, round(window_seconds / period))` slots, and readings keep their real spacing. |
| `train_share` | `0.7` | earliest share of eligible windows that train |
| `inject_share` | `0.1` | share of windows that get a fault |
| `fault_kinds` | `[stuck, spike, drift, noise, oscillation, spoof]` | sensor fault kinds, in draw order |
| `fabricated_kinds` | `[sine, ramp, pattern, uniform]` | fabricated-signal kinds |
| `labels` | the five sensor kinds → `sensor_malfunction`, the four fabricated kinds → `not_sensor_data` | kind → `Anomalies` label. `spoof` has no label on purpose: the static check owns `spoofed_data`. |
| `faults` | see below | fault sizes (`FaultSettings`) |
| `sample_seconds` | one entry per source, `0.1` to `3600.0` | measured seconds between one sensor's readings. Every source used by `LabelledWindows` must be listed. |
| `max_readings` | `50000000` | memory cap: only sources above it are sampled in blocks |
| `block_hours` | `2` | hours kept in each sampling block |
| `sample_every_hours` | `array_of_things: 745`, `grid_frequency: 53`, `grid_frequency_100ms: 31` | keep one block in this many hours |
| `fast_sensor_share` | `0.5` | drop a sensor reporting faster than this share of the period |

### `injection.faults` (`FaultSettings`)

| Key | Default | Effect |
|---|---|---|
| `spike_count_max` | `3` | most spikes in one window |
| `spike_spreads` | `[5.0, 10.0]` | spike size, in spreads |
| `drift_spreads` | `[3.0, 6.0]` | drift size |
| `noise_spreads` | `[1.0, 3.0]` | added noise size |
| `oscillation_spreads` | `[2.0, 4.0]` | oscillation size |
| `period_readings` | `[4.0, 8.0]` | oscillation and sine period, in readings |
| `spoof_spreads` | `[3.0, 6.0]` | spoof offset size |
| `sine_spreads` | `[2.0, 4.0]` | sine size |
| `ramp_spreads` | `[3.0, 6.0]` | ramp size |
| `pattern_values` | `[2, 4]` | motif length, both ends included |
| `pattern_spreads` | `3.0` | motif values lie within this many spreads |

Each `(low, high)` pair is a range: one value is drawn from it per fault.

### `long_injection` (`LongInjectionSettings`)

This block holds only what the long-window model changes. Everything else comes from `injection`.

| Key | Default | Effect |
|---|---|---|
| `fault_kinds` | `[stuck, drift]` | Hour-long windows are averaged into buckets, which smooth out spikes, noise and oscillations. Drift and stuck build up over hours. |
| `fabricated_kinds` | `[sine, ramp, pattern, uniform]` | as in `injection` |
| `block_hours` | `24` | a block must hold a whole 6 h or 24 h window |
| `sample_every_hours` | `array_of_things: 1801` | merged into `injection.sample_every_hours` |

## How to add a dataset loader

This adds a new source `my_source` that `DatasetFactory` can build by name.

1. In `src/guardian_layer/utils/dataset_config.py`, add a settings class for its files and columns, and a field for it on `DatasetSettings`:

   ```python
   class MySourceSettings(BaseModel):
       model_config = ConfigDict(extra="forbid")
       folder: str = "my_source"
       data_file: str = "data.csv"
       unit: str = "m"


   # inside DatasetSettings
   my_source: MySourceSettings = MySourceSettings()
   ```

2. Add the same values under `my_source:` in `config/datasets.yaml`. The defaults in the module and the YAML stay equal.
3. Add its measured period to `injection.sample_seconds` (YAML and `InjectionSettings`). `LabelledWindows.read` refuses a source without one.
4. Create `src/guardian_layer/ai/datasets/my_source.py`. Parse rows into `DatasetRow`s and hand them to `to_readings`:

   ```python
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
   from guardian_layer.utils.schema import DatasetRow, LabeledReading


   @DatasetFactory.register("my_source")
   class MySource(DatasetInterface):
       def rows(self) -> Iterator[DatasetRow]:
           config = self.settings.my_source
           path = dataset_path(self.settings, config.folder, config.data_file)
           with open_text(path) as stream:
               for row in csv.DictReader(stream):
                   value = parse_float(row["value"])
                   if value is not None:
                       yield DatasetRow(
                           to_utc(row["time"]), row["station"], "level", config.unit, value
                       )

       def load(self) -> Iterator[LabeledReading]:
           return to_readings("my_source", self.rows())
   ```

   Pass `sort=True` if the file isn't in time order. Stream big files instead of sorting them.
5. Import the class in `src/guardian_layer/ai/datasets/__init__.py` and add it to `__all__`. That import is what registers it.
6. Add `tests/ai/datasets/test_my_source.py`. As `test_intel_lab.py` does, write a few rows into `tmp_path` and build the loader with `DatasetSettings(root=str(tmp_path))`.

To check it, run:

```bash
uv run pytest tests/ai/datasets/test_my_source.py -q
```

```bash
uv run python -c "from guardian_layer.ai.dataset_factory import DatasetFactory; print('my_source' in DatasetFactory.names())"
```

If you get `ValueError: dataset already registered`, another loader already uses that name. If you get `FileNotFoundError: dataset file not found`, check `root` and the file names in your YAML block.

## Related

- [pipelines.md](pipelines.md): the features `LabelledWindows` computes through `PreProcess` and `Decompose`
- [inference.md](inference.md): the model trained on `WindowDataset`
- [README.md](README.md): how this offline path fits with the live one
