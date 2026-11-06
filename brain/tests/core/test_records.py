import json
from datetime import timedelta
from pathlib import Path

import jsonschema
import pytest

from city_brain.config import load_config
from city_brain.core.events import Publish, ReadingReceived, RegistryLoaded
from city_brain.core.records import (
    alert_event,
    command_event,
    decision_event,
    status_event,
    verification_event,
)
from city_brain.core.shield import CheckOutcome, ShieldResult
from city_brain.core.state import BrainState
from city_brain.core.step import step
from city_brain.domain.types import (
    ActionIntent,
    ActionStatus,
    Alternative,
    EvidenceRef,
    ExpectedEffect,
    RiskClass,
    StateKind,
    TrustState,
)
from tests.factories import T0, make_observation

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "brain-event.v1.schema.json"
BRAIN_EVENT_SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
PARAMS = load_config("config/standalone.yaml").params
POLICY = {"epochs": {"seq_drop": 100, "ts_back_s": 60}}
NINE = ["incident", "evidence", "rule_or_plan", "alternatives", "rationale"]
EXPECTED = ExpectedEffect(channel="z1-wl1", metric="rate", comparator="<=", threshold=-0.01, window_s=3.0)


def fresh_state():
    return BrainState.new(POLICY, PARAMS)


def running_state():
    s = fresh_state()
    s, _ = step(s, ReadingReceived(make_observation(seq=1)))
    return s


def evidence():
    return (
        EvidenceRef(
            reading_id="r-1", sig_hash="0123456789abcdef", device_id="z1-wl1", value=6.123456789,
            sim_time=T0, state_kind=StateKind.OBSERVED, verdict_id="v-1",
            trust_state=TrustState.TRUSTED, weight=1.0,
        ),
    )


def alternatives():
    return (Alternative(option="wait", rejected_because="level above high band"),)


def sample_intent():
    return ActionIntent(
        action_id="a-1", incident_id="i-1", plan_version=1, capability="valve", target="z1-v1",
        state="open", risk_class=RiskClass.R1, expected_effect=EXPECTED, evidence=evidence(),
        rationale="drain", alternatives=alternatives(),
    )


def sample_shield(passed=True):
    checks = (CheckOutcome("NO_OP", passed, None if passed else "NO_OP"),)
    return ShieldResult(passed=passed, refusals=() if passed else ("NO_OP",), checks=checks)


def sample_decision(state=None, intent=None, outcome="act"):
    return decision_event(
        state or running_state(), intent, outcome,
        {"kind": "FLOOD_RISK", "zone": "zone-1", "belief": 0.93, "status": "OPEN"},
        "water.t0", evidence(), alternatives(), "level above high band",
        ["level falls below low band"],
    )


def sample_events():
    s = running_state()
    i = sample_intent()
    return [
        status_event(s, "mode_changed", {"mode": "NORMAL", "ingest_rate": 10.5}),
        status_event(fresh_state(), "startup", {}),
        sample_decision(s, i),
        sample_decision(s, None, "hold"),
        command_event(s, i, sample_shield(), ActionStatus.DISPATCHED,
                      {"name": "set_valve", "args": {"state": "open"}}, {"ok": True}, ["R1_OK"]),
        command_event(s, i, sample_shield(False), ActionStatus.REFUSED, None, None, ["NO_OP"]),
        verification_event(s, "a-1", EXPECTED, {"rate": -0.02}, "verified"),
        alert_event(s, "operator", "high", "ACTUATOR_SUSPECT", "valve did not move",
                    {"action_id": "a-1", "incident_id": "i-1", "target": "z1-v1"}),
        alert_event(s, "guardian", "low", "X", "m", {}),
    ]


def test_every_builder_validates_against_schema():
    jsonschema.Draft202012Validator.check_schema(BRAIN_EVENT_SCHEMA)
    for ev in sample_events():
        jsonschema.validate(ev, BRAIN_EVENT_SCHEMA)
        assert "time" not in ev
        json.dumps(ev, sort_keys=True, allow_nan=False)


def test_schema_allows_log_fields_and_rejects_bad_type():
    ev = sample_events()[0]
    jsonschema.validate({**ev, "time": "2026-10-17T12:00:00Z", "hash": "a", "prev_hash": "b"}, BRAIN_EVENT_SCHEMA)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({**ev, "type": "tn.tsyp.brain.nope.v1"}, BRAIN_EVENT_SCHEMA)
    bad = sample_events()[4]
    bad = {**bad, "data": {k: v for k, v in bad["data"].items() if k != "status"}}
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(bad, BRAIN_EVENT_SCHEMA)


def test_decision_answers_explainability_questions():
    d = sample_decision()["data"]
    for f in NINE:
        assert d[f] not in (None, "", [], {})
    for e in d["evidence"]:
        assert {"reading_id", "sig_hash", "state_kind", "verdict_id", "trust_state", "weight"} <= e.keys()
    for a in d["alternatives"]:
        assert a["rejected_because"]


def test_hold_decision_fills_fields_without_intent():
    ev = sample_decision(intent=None, outcome="hold")
    assert ev["data"]["outcome"] == "hold"
    assert ev["data"]["incident"]["kind"] == "FLOOD_RISK"
    assert ev["data"]["rule_or_plan"] == "water.t0"
    assert ev["actionid"] is None and ev["subject"] == "zone-1"


def test_data_is_json_native_and_canonical():
    s = running_state()
    d = sample_decision(s, sample_intent())
    e0 = d["data"]["evidence"][0]
    assert e0["trust_state"] == "TRUSTED" and e0["state_kind"] == "OBSERVED"
    assert e0["value"] == 6.123457
    assert e0["sim_time"] == "2026-10-17T12:00:00.000000Z"
    assert d["simtime"] == "2026-10-17T12:00:00.000000Z"
    assert d["actionid"] == "a-1" and d["correlationid"] == "i-1"
    c = command_event(s, sample_intent(), sample_shield(), ActionStatus.DISPATCHED, None, None, ["R1_OK"])
    cd = c["data"]
    assert cd["status"] == "DISPATCHED" and cd["risk_class"] == "R1"
    assert cd["capability"] == "valve" and cd["target"] == "z1-v1" and cd["state"] == "open"
    assert cd["shield"] == {"passed": True, "refusals": [], "checks": [{"name": "NO_OP", "passed": True, "code": None}]}
    assert cd["expected"]["comparator"] == "<=" and cd["reasons"] == ["R1_OK"]
    assert c["subject"] == "z1-v1" and c["actionid"] == "a-1"


def test_simtime_is_null_before_first_reading():
    ev = status_event(fresh_state(), "startup", {})
    assert ev["simtime"] is None and ev["subject"] == "city"
    assert ev["correlationid"] is None and ev["actionid"] is None
    assert ev["data"]["reason"] == "startup"


def test_event_ids_are_sequential_per_epoch():
    s = fresh_state()
    a = status_event(s, "startup", {})
    b = status_event(s, "x", {})
    assert (a["id"], b["id"]) == ("0-0", "0-1")


def test_epoch_change_resets_sequence_and_carries_new_epoch():
    s = fresh_state()
    s, _ = step(s, RegistryLoaded(devices=()))
    s, _ = step(s, ReadingReceived(make_observation(seq=5000, sim_time=T0 + timedelta(seconds=500))))
    s, fx = step(s, ReadingReceived(make_observation(seq=3, sim_time=T0, reading_id="r-n", sig_hash="new")))
    [ev] = [e.event for e in fx if isinstance(e, Publish)]
    assert ev["data"]["reason"] == "epoch_changed"
    assert ev["epoch"] == 1 and ev["id"] == "1-0"
    jsonschema.validate(ev, BRAIN_EVENT_SCHEMA)
