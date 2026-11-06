import json
from pathlib import Path

import jsonschema
import pytest
from jsonschema import FormatChecker

from city_brain.adapters.files.jsonl_log import verify_chain
from city_brain.runtime.main import main
from scenarios.runner import load_scenario, run
from tests.scenarios.helpers import (
    ALERT,
    COMMAND,
    VERIFICATION,
    commands,
    count,
    first,
    matching,
    max_level_before,
    verdicts_for,
)

BRAIN = Path(__file__).resolve().parents[2]
SCHEMA = json.loads((BRAIN / "schemas" / "brain-event.v1.schema.json").read_text(encoding="utf-8"))


def test_t02_valve_opened_before_6m_and_verified(t02_events):
    expect = load_scenario("T02").expect
    valve = expect["valve"]
    cmd = first(t02_events, type=COMMAND, target=valve, state="open", status="DISPATCHED")
    assert max_level_before(t02_events, valve, cmd["simtime"]) < expect["level_limit"]
    assert first(t02_events, type=VERIFICATION, actionid=cmd["actionid"])["data"]["verdict"] == expect["verdict"]


def test_t02_refuses_nothing_on_a_commanded_channel(t02_events):
    commanded = {e["data"]["target"] for e in matching(t02_events, type=COMMAND, status="DISPATCHED")}
    assert load_scenario("T02").expect["valve"] in commanded
    assert [e for e in matching(t02_events, type=COMMAND, status="REFUSED") if e["data"]["target"] in commanded] == []


def test_t02_channel_returns_into_band(t02_events):
    expect = load_scenario("T02").expect
    opened = first(t02_events, type=COMMAND, target=expect["valve"], state="open", status="DISPATCHED")
    closed = first(t02_events, type=COMMAND, target=expect["valve"], state="closed", status="DISPATCHED")
    assert closed["simtime"] > opened["simtime"]
    assert first(t02_events, type=VERIFICATION, actionid=closed["actionid"])["data"]["verdict"] == expect["verdict"]


def test_t02_dispatch_is_logged_before_its_acceptance_and_names_the_tool(t02_events):
    cmd = first(t02_events, type=COMMAND, status="DISPATCHED")
    trail = [e["data"]["status"] for e in commands(t02_events) if e["actionid"] == cmd["actionid"]]
    assert trail == ["AWAITING_TRUST", "DISPATCHED", "VERIFYING"]
    assert cmd["data"]["tool"] == {
        "name": "set_valve", "args": {"device_id": cmd["data"]["target"], "target_state": "open"}}


def test_t05_faulty_valve_unverified_alerted_not_resent(t05_events):
    expect = load_scenario("T05").expect
    valve = expect["valve"]
    assert verdicts_for(t05_events, valve) == expect["verdicts"]
    assert count(t05_events, type=ALERT, code="ACTUATOR_NO_EFFECT", subject=valve) == expect["no_effect_alerts"]
    assert count(t05_events, type=COMMAND, target=valve, status="DISPATCHED") == expect["dispatches"]


def test_twin_restart_reloads_keys_and_continues(restart_events):
    reasons = [e["data"]["reason"] for e in restart_events if e["type"].endswith("status.v1")]
    assert reasons.index("registry_reloaded") < reasons.index("epoch_changed")
    later = [e for e in restart_events if e["epoch"] == 1]
    assert any(e["type"].endswith("command.v1") and e["data"]["status"] == "DISPATCHED" for e in later)
    assert "verified" in [e["data"]["verdict"] for e in matching(later, type=VERIFICATION)]


def test_twin_restart_settles_the_action_it_interrupted(restart_events):
    [alert] = matching(restart_events, type=ALERT, code="EPOCH_CHANGED_ACTION_UNKNOWN", subject="z1-wl1")
    trail = [e["data"]["status"] for e in commands(restart_events) if e["actionid"] == alert["actionid"]]
    assert trail[-2:] == ["VERIFYING", "UNKNOWN"]
    assert count(restart_events, type=VERIFICATION, actionid=alert["actionid"]) == 0


def test_every_logged_event_matches_the_schema_and_the_chain_verifies(
    t02_log, t02_events, t05_log, t05_events, restart_log, restart_events
):
    for path, events in ((t02_log, t02_events), (t05_log, t05_events), (restart_log, restart_events)):
        assert verify_chain(path)
        for event in events:
            jsonschema.validate(event, SCHEMA, format_checker=FormatChecker())


def test_log_name_carries_scenario_and_epoch(t02_log):
    assert t02_log.name.startswith("T02-e0-") and t02_log.suffix == ".jsonl"


def test_triplet_profile_is_a_clear_stub():
    with pytest.raises(NotImplementedError, match="P4"):
        run("T02", "triplet")


def test_triplet_config_exits_2(capsys):
    assert main(["--config", str(BRAIN / "config" / "triplet.yaml"), "--scenario", "T02"]) == 2
    assert "triplet mode lands in P4" in capsys.readouterr().err


def test_scenario_rejects_unknown_keys(tmp_path, monkeypatch):
    monkeypatch.setattr("scenarios.runner.SCENARIOS_DIR", tmp_path)
    (tmp_path / "X.yaml").write_text(
        "duration_s: 1\nguardian_policy: trust_all\ninject: []\nexpect: {}\nsurprise: 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="surprise"):
        load_scenario("X")


def test_scenario_rejects_an_unknown_guardian_policy(tmp_path, monkeypatch):
    monkeypatch.setattr("scenarios.runner.SCENARIOS_DIR", tmp_path)
    (tmp_path / "X.yaml").write_text(
        "duration_s: 1\nguardian_policy: nonsense\ninject: []\nexpect: {}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="nonsense"):
        load_scenario("X")


def test_t05_alert_reaches_operator_and_guardian(t05_events):
    alerts = matching(t05_events, type=ALERT, code="ACTUATOR_NO_EFFECT", subject="z4-wl1")
    audiences = sorted(e["data"]["audience"] for e in alerts)
    assert audiences == ["guardian", "operator"]
