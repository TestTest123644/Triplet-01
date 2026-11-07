"""Synthetic telemetry generator with labelled anomalies (issue #40).

Writes ``telemetry.jsonl`` in the agreed TelemetryData format and a line-aligned
``labels.jsonl`` holding the ground-truth label of each line.

Run: uv run python mocks/telemetry_generator.py --seed 42 --out mocks/out/
"""

import argparse
import json
import math
import random
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# type -> (unit, lower_range, upper_range); picked for the Bab Alioua flooding scenario
SENSOR_TYPES: dict[str, tuple[str, float, float]] = {
    "water_level": ("m", 0.0, 3.0),
    "rainfall": ("mm/h", 0.0, 100.0),
    "temperature": ("C", -5.0, 50.0),
    "traffic_flow": ("vehicles/min", 0.0, 120.0),
}
CENTER_LAT, CENTER_LON = 36.79, 10.18
NOISE_FRACTION = 0.01  # noise std as a fraction of the type's range width
SPOOF_SIGMAS = 5  # spoofed values sit at least this many noise stds off the signal
MIN_SENSORS = 7  # last 3 sensors are edge cases; each type keeps a normal sensor
REPLAY_MIN_AGE = 30  # replayed messages are at least this many readings old
VALUE_ANOMALIES = ("out_of_range", "spoofed", "stuck")
PACKET_ANOMALIES = ("duplicate", "replayed", "out_of_order", "packet_gap")
LABELS = ("normal", *VALUE_ANOMALIES, *PACKET_ANOMALIES)

Message = dict[str, Any]


@dataclass(frozen=True)
class Sensor:
    sensor_id: str
    sensor_type: str
    offset: float
    lat: float | None
    lon: float | None
    has_ranges: bool = True
    has_metadata: bool = True


def type_signal(sensor_type: str, tick: int) -> float:
    """Shared slow wave of a sensor type; every sensor of that type follows it."""
    _, low, high = SENSOR_TYPES[sensor_type]
    phase = list(SENSOR_TYPES).index(sensor_type)
    wave = math.sin(2 * math.pi * tick / 300 + phase)
    return low + (high - low) * (0.5 + 0.2 * wave)


def noise_std(sensor_type: str) -> float:
    _, low, high = SENSOR_TYPES[sensor_type]
    return NOISE_FRACTION * (high - low)


def make_sensors(count: int, rng: random.Random) -> list[Sensor]:
    """Round-robin over types; the last three sensors are the edge cases."""
    types = list(SENSOR_TYPES)
    sensors = []
    for i in range(count):
        sensor_type = types[i % len(types)]
        sensors.append(
            Sensor(
                sensor_id=f"{sensor_type}-{i // len(types) + 1:02d}",
                sensor_type=sensor_type,
                offset=rng.uniform(-2, 2) * noise_std(sensor_type),
                lat=round(CENTER_LAT + rng.uniform(-0.005, 0.005), 6),
                lon=round(CENTER_LON + rng.uniform(-0.005, 0.005), 6),
            )
        )
    sensors[-3] = replace(sensors[-3], has_ranges=False)
    sensors[-2] = replace(sensors[-2], lat=None, lon=None)
    sensors[-1] = replace(sensors[-1], has_metadata=False)
    return sensors


def make_message(
    sensor: Sensor, value: float, timestamp: str, packet_order: int
) -> Message:
    unit, low, high = SENSOR_TYPES[sensor.sensor_type]
    metadata = None
    if sensor.has_metadata:
        metadata = {
            "lat": sensor.lat,
            "lon": sensor.lon,
            "packet_order": packet_order,
            "type": sensor.sensor_type,
            "upper_range": high if sensor.has_ranges else None,
            "lower_range": low if sensor.has_ranges else None,
        }
    return {
        "sensor_id": sensor.sensor_id,
        "cluster_id": sensor.sensor_type,
        "value": round(value, 2),
        "unit": unit,
        "timestamp": timestamp,
        "metadata": metadata,
    }


def eligible_kinds(
    sensor: Sensor,
    tick: int,
    duration: int,
    last_value: float | None,
    history_len: int,
) -> list[str]:
    kinds = ["out_of_range", "spoofed"]
    if last_value is not None:
        kinds.append("stuck")
    if sensor.has_metadata:  # packet anomalies need packet_order
        kinds.append("duplicate")
        if history_len > REPLAY_MIN_AGE:
            kinds.append("replayed")
        if tick < duration - 1:
            kinds.append("out_of_order")
        # needs an earlier message to detect the gap; up to 5 drops, then a labelled one
        if history_len and tick + 5 < duration:
            kinds.append("packet_gap")
    return kinds


def sensor_stream(
    sensor: Sensor,
    duration: int,
    anomaly_rate: float,
    start: datetime,
    rng: random.Random,
) -> list[tuple[float, Message, str]]:
    """One sensor's emitted lines as (arrival_time, message, label)."""
    _, low, high = SENSOR_TYPES[sensor.sensor_type]
    width = high - low
    std = noise_std(sensor.sensor_type)
    lines: list[tuple[float, Message, str]] = []
    history: list[Message] = []  # messages sent so far, for replays
    last_value: float | None = None  # value of the last non-replayed line
    stuck_left, stuck_value = 0, 0.0
    drop_left, gap_next = 0, False
    held: Message | None = None  # delayed by out_of_order

    def emit(arrival: float, message: Message, label: str) -> None:
        nonlocal last_value
        lines.append((arrival, message, label))
        if label != "replayed":
            last_value = message["value"]

    for tick in range(duration):
        signal = type_signal(sensor.sensor_type, tick)
        value = min(max(signal + sensor.offset + rng.gauss(0, std), low), high)
        label = "normal"

        if drop_left:  # message lost in a gap
            drop_left -= 1
            gap_next = drop_left == 0
            continue
        if gap_next:
            label, gap_next = "packet_gap", False
        elif stuck_left:
            label, value = "stuck", stuck_value
            stuck_left -= 1
        elif held is None and rng.random() < anomaly_rate:
            label = rng.choice(
                eligible_kinds(sensor, tick, duration, last_value, len(history))
            )
            if label == "packet_gap":  # drop this message and 1-4 more
                drop_left = rng.randint(2, 5) - 1
                continue
            if label == "out_of_range":
                excess = rng.uniform(0.1, 0.5) * width
                value = high + excess if rng.random() < 0.5 else low - excess
            elif label == "spoofed":
                shift = 3 * SPOOF_SIGMAS * std
                value = signal + shift if signal < low + width / 2 else signal - shift
            elif label == "stuck" and last_value is not None:
                stuck_value, stuck_left = last_value, rng.randint(5, 15) - 1
                value = stuck_value

        timestamp = (start + timedelta(seconds=tick)).strftime("%Y-%m-%dT%H:%M:%SZ")
        message = make_message(sensor, value, timestamp, packet_order=tick + 1)
        if label == "out_of_order":
            held = message  # sent right after the next message
        else:
            copy_label = label if label in ("duplicate", "replayed") else None
            emit(float(tick), message, "normal" if copy_label else label)
            if held is not None:
                emit(tick + 0.1, held, "out_of_order")
                held = None
            if copy_label == "duplicate":
                emit(tick + 0.1, message, "duplicate")
            elif copy_label == "replayed":
                old = history[rng.randrange(len(history) - REPLAY_MIN_AGE)]
                emit(tick + 0.2, old, "replayed")
        history.append(message)
    return lines


def generate(
    seed: int = 42,
    sensors: int = 20,
    duration: int = 600,
    anomaly_rate: float = 0.05,
    start: str = "2026-10-02T00:00:00Z",
) -> tuple[list[Message], list[dict[str, Any]]]:
    """Return (telemetry, labels), line-aligned and ordered by arrival."""
    if sensors < MIN_SENSORS:
        raise ValueError(f"need at least {MIN_SENSORS} sensors, got {sensors}")
    rng = random.Random(seed)
    start_at = datetime.fromisoformat(start)
    if start_at.utcoffset():
        raise ValueError("start must be UTC")
    entries = []
    for index, sensor in enumerate(make_sensors(sensors, rng)):
        stream = sensor_stream(sensor, duration, anomaly_rate, start_at, rng)
        for seq, (arrival, message, label) in enumerate(stream):
            entries.append((arrival, index, seq, message, label))
    entries.sort(key=lambda entry: entry[:3])
    telemetry = [message for *_, message, _ in entries]
    labels = [
        {"line": line, "sensor_id": message["sensor_id"], "label": label}
        for line, (*_, message, label) in enumerate(entries)
    ]
    return telemetry, labels


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row) + "\n")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Synthetic telemetry generator (#40)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sensors", type=int, default=20)
    parser.add_argument("--duration", type=int, default=600, help="seconds")
    parser.add_argument("--anomaly-rate", type=float, default=0.05)
    parser.add_argument("--start", default="2026-10-02T00:00:00Z")
    parser.add_argument("--out", type=Path, default=Path("mocks/out"))
    args = parser.parse_args(argv)
    try:
        telemetry, labels = generate(
            args.seed, args.sensors, args.duration, args.anomaly_rate, args.start
        )
    except ValueError as error:
        parser.error(str(error))
    args.out.mkdir(parents=True, exist_ok=True)
    write_jsonl(args.out / "telemetry.jsonl", telemetry)
    write_jsonl(args.out / "labels.jsonl", labels)
    print(f"wrote {len(telemetry)} lines to {args.out}")


if __name__ == "__main__":
    main()
