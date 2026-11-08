import sys
from datetime import datetime
from pathlib import Path

import pytest

# mocks/ isn't an installed package; put the repo root on sys.path to import it
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from mocks.telemetry_generator import (  # noqa: E402
    LABELS,
    SENSOR_TYPES,
    SPOOF_SIGMAS,
    VALUE_ANOMALIES,
    generate,
    main,
    noise_std,
    type_signal,
)

START = "2026-10-02T00:00:00Z"
MESSAGE_KEYS = {"sensor_id", "cluster_id", "value", "unit", "timestamp", "metadata"}
METADATA_KEYS = {"lat", "lon", "packet_order", "type", "upper_range", "lower_range"}

TELEMETRY, LABEL_ROWS = generate()  # defaults: seed 42, 20 sensors, 600 s, rate 0.05


def lines_with(label):
    return [
        (i, TELEMETRY[i]) for i, row in enumerate(LABEL_ROWS) if row["label"] == label
    ]


def type_range(message):
    _, low, high = SENSOR_TYPES[message["cluster_id"]]
    return low, high


def test_same_seed_same_output_and_different_seed_differs():
    assert generate(seed=7) == generate(seed=7)
    assert generate(seed=7) != generate(seed=8)


def test_labels_are_line_aligned():
    assert len(TELEMETRY) == len(LABEL_ROWS)
    for i, (message, row) in enumerate(zip(TELEMETRY, LABEL_ROWS, strict=True)):
        assert row["line"] == i
        assert row["sensor_id"] == message["sensor_id"]
        assert row["label"] in LABELS


def test_every_line_has_the_agreed_schema():
    for message in TELEMETRY:
        assert set(message) == MESSAGE_KEYS
        assert message["cluster_id"] in SENSOR_TYPES
        assert message["timestamp"].endswith("Z")
        if message["metadata"] is not None:
            assert set(message["metadata"]) == METADATA_KEYS
            assert message["metadata"]["type"] == message["cluster_id"]


def test_edge_case_sensors_are_present():
    metadatas = [m["metadata"] for m in TELEMETRY]
    assert any(md is None for md in metadatas)
    assert any(
        md and md["upper_range"] is None and md["lower_range"] is None
        for md in metadatas
    )
    assert any(md and md["lat"] is None and md["lon"] is None for md in metadatas)


def test_too_few_sensors_is_rejected():
    with pytest.raises(ValueError):
        generate(sensors=6)


def test_normal_values_are_inside_the_type_range():
    for _, message in lines_with("normal"):
        low, high = type_range(message)
        assert low <= message["value"] <= high


def test_cli_writes_both_files(tmp_path: Path):
    main(["--sensors", "8", "--duration", "60", "--out", str(tmp_path)])
    telemetry = (tmp_path / "telemetry.jsonl").read_text().splitlines()
    labels = (tmp_path / "labels.jsonl").read_text().splitlines()
    assert len(telemetry) == len(labels) > 0


def previous_line(index, skip=("replayed",)):
    """The closest earlier line of the same sensor, ignoring labels in `skip`."""
    sensor_id = TELEMETRY[index]["sensor_id"]
    for j in range(index - 1, -1, -1):
        same_sensor = TELEMETRY[j]["sensor_id"] == sensor_id
        if same_sensor and LABEL_ROWS[j]["label"] not in skip:
            return TELEMETRY[j]
    raise AssertionError(f"no earlier line for {sensor_id}")


def tick_of(message):
    at = datetime.fromisoformat(message["timestamp"])
    return int((at - datetime.fromisoformat(START)).total_seconds())


def test_value_anomalies_appear_with_defaults():
    assert set(VALUE_ANOMALIES) <= {row["label"] for row in LABEL_ROWS}


def test_out_of_range_values_are_outside_the_type_range():
    for _, message in lines_with("out_of_range"):
        low, high = type_range(message)
        assert not low <= message["value"] <= high


def test_spoofed_values_are_in_range_but_far_from_the_type_signal():
    for _, message in lines_with("spoofed"):
        low, high = type_range(message)
        sensor_type = message["cluster_id"]
        signal = type_signal(sensor_type, tick_of(message))
        assert low <= message["value"] <= high
        assert abs(message["value"] - signal) >= SPOOF_SIGMAS * noise_std(sensor_type)


def test_stuck_values_repeat_the_previous_reading():
    for i, message in lines_with("stuck"):
        assert message["value"] == previous_line(i)["value"]


def max_order_before(index):
    """Highest packet_order the sensor of line `index` sent before that line."""
    sensor_id = TELEMETRY[index]["sensor_id"]
    return max(
        m["metadata"]["packet_order"]
        for m in TELEMETRY[:index]
        if m["sensor_id"] == sensor_id
    )


def test_every_label_appears_with_defaults():
    assert {row["label"] for row in LABEL_ROWS} == set(LABELS)


def test_duplicate_equals_the_previous_message():
    for i, message in lines_with("duplicate"):
        assert message == previous_line(i, skip=())


def test_replayed_message_is_old():
    for i, message in lines_with("replayed"):
        assert message["metadata"]["packet_order"] < max_order_before(i) - 1


def test_out_of_order_message_arrives_after_a_newer_one():
    for i, message in lines_with("out_of_order"):
        assert message["metadata"]["packet_order"] < max_order_before(i)


def test_packet_gap_skips_packet_orders():
    for i, message in lines_with("packet_gap"):
        assert message["metadata"]["packet_order"] - max_order_before(i) >= 3


def test_no_sensor_starts_with_a_packet_gap():
    for seed in (2, 6, 8, 9, 42):  # 2, 6, 8, 9 used to open a sensor with a gap
        telemetry, labels = generate(seed=seed)
        seen = set()
        for message, row in zip(telemetry, labels, strict=True):
            if message["sensor_id"] not in seen:
                seen.add(message["sensor_id"])
                assert row["label"] != "packet_gap"


def test_non_utc_start_is_rejected():
    with pytest.raises(ValueError):
        generate(start="2026-10-02T00:00:00+01:00")
