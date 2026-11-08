from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from guardian_layer.consumer_producer.action_taker.rules import RepeatCounter, Rules
from guardian_layer.consumer_producer.action_taker.schema import (
    Action,
    AnomalyRecord,
    DlqRecord,
    ToolName,
)
from guardian_layer.utils.schema import Anomalies

T0 = datetime(2026, 11, 4, 10, 0, tzinfo=UTC)
DEVICE = "z1-tc1"


@pytest.fixture
def rules() -> Rules:
    return Rules(RepeatCounter(threshold=3, window=timedelta(seconds=60)))


def dlq(
    code: str,
    device: Any = DEVICE,
    reading: str | None = None,
    at: datetime = T0,
) -> DlqRecord:
    raw: dict[str, Any] = {"device_id": device}
    if reading is not None:
        raw["reading_id"] = reading
    return DlqRecord(error_code=code, detail="", raw=raw, failed_at=at)


def anomaly(
    label: Anomalies | None,
    reading: str | None = None,
    device: str = DEVICE,
    at: datetime = T0,
) -> AnomalyRecord:
    return AnomalyRecord(
        device_id=device, reading_id=reading, label=label, detected_at=at
    )


def isolate(device: str = DEVICE) -> Action:
    return Action(ToolName.ISOLATE_DEVICE, device)


def quarantine(device: str = DEVICE) -> Action:
    return Action(ToolName.QUARANTINE_DEVICE, device)


def rollback(reading: str) -> Action:
    return Action(ToolName.ROLLBACK_READING, reading)


def test_bad_signature_isolates_and_rolls_back(rules):
    assert rules.decide(dlq("E200_BAD_SIGNATURE", reading="r-1")) == [
        isolate(),
        rollback("r-1"),
    ]


def test_unknown_device_without_reading_id_only_isolates(rules):
    assert rules.decide(dlq("E201_UNKNOWN_DEVICE")) == [isolate()]


def test_validation_errors_quarantine_at_threshold(rules):
    assert rules.decide(dlq("E300_SCHEMA", at=T0)) == []
    assert rules.decide(dlq("E300_SCHEMA", at=T0 + timedelta(seconds=10))) == []
    assert rules.decide(dlq("E300_SCHEMA", at=T0 + timedelta(seconds=20))) == [
        quarantine()
    ]


def test_other_e3_and_e4_codes_count_together(rules):
    assert rules.decide(dlq("E320_UNKNOWN_STATUS", at=T0)) == []
    later = T0 + timedelta(seconds=1)
    assert rules.decide(dlq("E400_SENSOR_VALIDATION", at=later)) == []
    assert rules.decide(dlq("E300_SCHEMA", at=T0 + timedelta(seconds=2))) == [
        quarantine()
    ]


def test_validation_errors_outside_window_do_not_count(rules):
    for seconds in (0, 30, 100):
        at = T0 + timedelta(seconds=seconds)
        assert rules.decide(dlq("E300_SCHEMA", at=at)) == []


def test_out_of_order_old_event_does_not_count(rules):
    assert rules.decide(dlq("E300_SCHEMA", at=T0 + timedelta(seconds=100))) == []
    assert rules.decide(dlq("E300_SCHEMA", at=T0 + timedelta(seconds=110))) == []
    # 100 s before the newest hit: outside the window, so no third hit
    assert rules.decide(dlq("E300_SCHEMA", at=T0)) == []


def test_counts_are_per_device(rules):
    assert rules.decide(dlq("E300_SCHEMA", at=T0)) == []
    assert rules.decide(dlq("E300_SCHEMA", at=T0)) == []
    assert rules.decide(dlq("E300_SCHEMA", device="z1-tc2", at=T0)) == []


def test_dlq_without_device_id_does_nothing(rules):
    no_json = DlqRecord(
        error_code="E200_BAD_SIGNATURE", detail="", raw="not json", failed_at=T0
    )
    assert rules.decide(no_json) == []
    for _ in range(3):
        assert rules.decide(dlq("E300_SCHEMA", device=42)) == []


@pytest.mark.parametrize("code", ["E100_DESERIALIZE", "E500_ENRICHMENT", "X999"])
def test_unmatched_codes_do_nothing(rules, code):
    assert rules.decide(dlq(code, reading="r-1")) == []


def test_spoofing_rolls_back_then_isolates_when_repeated(rules):
    assert rules.decide(anomaly(Anomalies.SPOOFED_DATA, "r-1")) == [rollback("r-1")]
    assert rules.decide(anomaly(Anomalies.SPOOFED_DATA, "r-2")) == [rollback("r-2")]
    assert rules.decide(anomaly(Anomalies.SPOOFED_DATA, "r-3")) == [
        isolate(),
        rollback("r-3"),
    ]


def test_not_sensor_data_is_spoofing(rules):
    assert rules.decide(anomaly(Anomalies.NOT_SENSOR_DATA, "r-1")) == [rollback("r-1")]


def test_spoofing_without_reading_id_has_no_rollback(rules):
    assert rules.decide(anomaly(Anomalies.SPOOFED_DATA)) == []


@pytest.mark.parametrize(
    "label",
    [Anomalies.OUT_OF_RANGE, Anomalies.SENSOR_MALFUNCTION, Anomalies.SENSOR_IS_DOWN],
)
def test_fault_labels_quarantine(rules, label):
    assert rules.decide(anomaly(label, "r-1")) == [quarantine()]


def test_back_to_normal_reinstates(rules):
    assert rules.decide(anomaly(None)) == [Action(ToolName.REINSTATE_DEVICE, DEVICE)]
