import json
from typing import Any

import pytest
from pydantic import ValidationError

from guardian_layer.consumer_producer.action_taker.schema import (
    Action,
    AnomalyRecord,
    DlqRecord,
    ToolName,
)
from guardian_layer.utils.schema import Anomalies

FAILED_AT = "2026-11-04T10:00:00+00:00"


def dlq_json(raw: Any, error_code: str = "E200_BAD_SIGNATURE") -> str:
    return json.dumps(
        {
            "error_code": error_code,
            "detail": "signature mismatch",
            "raw": raw,
            "failed_at": FAILED_AT,
        }
    )


def anomaly_json(**fields: Any) -> str:
    record: dict[str, Any] = {
        "device_id": "z1-tc1",
        "label": "spoofed_data",
        "detected_at": FAILED_AT,
    }
    record.update(fields)
    return json.dumps(record)


def test_dlq_record_reads_ids_from_raw():
    record = DlqRecord.model_validate_json(
        dlq_json({"device_id": "z1-tc1", "reading_id": "r-z1-tc1-105"})
    )
    assert record.device_id == "z1-tc1"
    assert record.reading_id == "r-z1-tc1-105"


def test_dlq_record_with_string_raw_has_no_ids():
    # E100 keeps the undecodable payload as a string
    record = DlqRecord.model_validate_json(dlq_json("not json", "E100_DESERIALIZE"))
    assert record.device_id is None
    assert record.reading_id is None


def test_dlq_record_non_string_device_id_is_none():
    record = DlqRecord.model_validate_json(dlq_json({"device_id": 42}))
    assert record.device_id is None


def test_dlq_record_empty_device_id_is_none():
    record = DlqRecord.model_validate_json(dlq_json({"device_id": ""}))
    assert record.device_id is None


def test_dlq_record_missing_field_is_rejected():
    payload = json.loads(dlq_json({"device_id": "z1-tc1"}))
    del payload["error_code"]
    with pytest.raises(ValidationError):
        DlqRecord.model_validate_json(json.dumps(payload))


def test_dlq_record_naive_timestamp_is_rejected():
    payload = json.loads(dlq_json({"device_id": "z1-tc1"}))
    payload["failed_at"] = "2026-11-04T10:00:00"
    with pytest.raises(ValidationError):
        DlqRecord.model_validate_json(json.dumps(payload))


def test_anomaly_record_parses_label():
    record = AnomalyRecord.model_validate_json(anomaly_json())
    assert record.label is Anomalies.SPOOFED_DATA
    assert record.reading_id is None


def test_anomaly_record_null_label_means_normal():
    record = AnomalyRecord.model_validate_json(anomaly_json(label=None))
    assert record.label is None


def test_anomaly_record_unknown_label_is_rejected():
    with pytest.raises(ValidationError):
        AnomalyRecord.model_validate_json(anomaly_json(label="on_fire"))


def test_anomaly_record_missing_label_key_is_rejected():
    payload = json.loads(anomaly_json())
    del payload["label"]
    with pytest.raises(ValidationError):
        AnomalyRecord.model_validate_json(json.dumps(payload))


def test_action_arguments_follow_the_tool():
    assert Action(ToolName.ROLLBACK_READING, "r-1").arguments == {"reading_id": "r-1"}
    assert Action(ToolName.ISOLATE_DEVICE, "z1-tc1").arguments == {
        "device_id": "z1-tc1"
    }
