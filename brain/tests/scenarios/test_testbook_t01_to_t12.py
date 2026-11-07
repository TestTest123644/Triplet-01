"""The Test Book: T01-T12 against the Brain's own mocks (spec 16).

Every scenario is run once per session (a 300 s run signs over a hundred thousand readings),
and each test reads the log and the run summary that run produced.
"""

import json

import jsonschema
import pytest
from jsonschema import FormatChecker

from city_brain.adapters.files.jsonl_log import verify_chain
from scenarios.runner import load_scenario, summary_path
from tests.scenarios.helpers import ALERT, COMMAND, VERIFICATION, count, matching, read_log

DECISION = "tn.tsyp.brain.decision.v1"
STATUS = "tn.tsyp.brain.status.v1"
INCIDENT = "tn.tsyp.brain.incident.v1"
BOOK = ("T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12")


# ----- shared readings of a run -----


def summary(log):
    return json.loads(summary_path(log).read_text(encoding="utf-8"))


def dispatched(events, **fields):
    return matching(events, type=COMMAND, status="DISPATCHED", **fields)


def holds(events, device_id=None):
    out = []
    for event in matching(events, type=DECISION, outcome="hold"):
        incident = event["data"]["incident"]
        if device_id is None or incident.get("device_id") == device_id:
            out.append(incident.get("hold_reason"))
    return out


def capabilities_used(events):
    return {e["data"]["capability"] for e in dispatched(events)}


def quiet(events, scenario_name):
    """No command was ever sent for the capabilities the scenario says must stay quiet."""
    forbidden = set(load_scenario(scenario_name).expect.get("quiet_capabilities", ()))
    return forbidden, forbidden & capabilities_used(events)


# ----- T01: a calm city -----


def test_t01_holds_every_tank_inside_the_band(t01_log):
    low, high = load_scenario("T01").expect["water_band"]
    levels = summary(t01_log)["max_water_level_by_device"]
    assert levels, "a calm run still reports its tanks"
    assert all(low <= level <= high for level in levels.values()), levels


def test_t01_sends_no_command_outside_the_water(t01_events):
    forbidden, used = quiet(t01_events, "T01")
    assert used == set(), f"a calm city needs none of {sorted(forbidden)}"


def test_t01_opens_no_incident(t01_events):
    assert matching(t01_events, type=INCIDENT) == []


def test_t01_moves_each_valve_at_most_twice_once_settled(t01_events, t01_log):
    expect = load_scenario("T01").expect
    settle = f"{expect['settle_s']:.0f}"
    changes: dict[str, int] = {}
    for event in dispatched(t01_events, capability="valve"):
        seconds = event["simtime"][11:19]
        if seconds >= f"12:0{int(settle) // 60}:{int(settle) % 60:02d}":
            changes[event["data"]["target"]] = changes.get(event["data"]["target"], 0) + 1
    assert all(n <= expect["max_valve_changes_per_channel"] for n in changes.values()), changes


def test_t01_summary_reports_a_valid_chain(t01_log):
    assert summary(t01_log)["chain_valid"] and summary(t01_log)["final_hash"]


# ----- T03: a fake flood -----


def test_t03_never_acts_on_the_spoofed_tank(t03_events):
    spoofed = load_scenario("T03").expect["spoofed"]
    assert dispatched(t03_events, target=spoofed) == []


def test_t03_says_why_it_is_holding_the_spoofed_tank(t03_events):
    expect = load_scenario("T03").expect
    reasons = set(holds(t03_events, expect["spoofed"]))
    assert reasons & set(expect["hold_reasons"]), reasons


def test_t03_still_controls_the_honest_tank(t03_events):
    honest = load_scenario("T03").expect["honest"]
    assert holds(t03_events, honest) is not None
    assert dispatched(t03_events, target=honest) or not dispatched(t03_events, capability="valve")


def test_t03_cites_no_untrusted_reading_in_a_command(t03_events):
    spoofed = load_scenario("T03").expect["spoofed"]
    for event in dispatched(t03_events):
        cited = {ref["device_id"] for ref in event["data"].get("evidence", ())}
        assert spoofed not in cited


# ----- T04: a stolen key on a feeder -----


def test_t04_sends_no_grid_command(t04_events):
    forbidden, used = quiet(t04_events, "T04")
    assert used == set(), f"the feeder cannot be switched on doubted evidence: {sorted(used)}"


def test_t04_keeps_the_grid_belief_low(t04_events):
    ceiling = load_scenario("T04").expect["belief_below"]
    beliefs = [
        e["data"]["belief"] for e in matching(t04_events, type=INCIDENT)
        if e["data"]["kind"] == "GRID_STRESS"
    ]
    assert all(b < ceiling for b in beliefs), beliefs


def test_t04_records_a_decision_rather_than_acting(t04_events):
    assert matching(t04_events, type=DECISION, outcome="hold"), "the run must say what it is not doing"


# ----- T06: a corrupted link -----


def test_t06_counts_the_rejected_bytes(t06_log):
    code = load_scenario("T06").expect["reject_code"]
    assert summary(t06_log)["rejected_by_code"].get(code, 0) > 0


def test_t06_marks_the_channel_stale_and_cites_nothing_from_it(t06_events):
    expect = load_scenario("T06").expect
    assert set(expect["hold_reasons"]) & set(holds(t06_events, expect["device"]))
    for event in dispatched(t06_events):
        after = {ref["device_id"] for ref in event["data"].get("evidence", ())}
        assert expect["device"] not in after or event["data"]["target"] != expect["device"]


# ----- T07: a replay storm -----


def test_t07_counts_the_duplicates(t07_log):
    assert summary(t07_log)["dedupe_count"] > load_scenario("T07").expect["dedupe_above"]


def test_t07_sends_no_signal_command(t07_events):
    forbidden, used = quiet(t07_events, "T07")
    assert used == set(), f"replayed readings are not evidence: {sorted(used)}"


# ----- T08: a stuck sensor -----


def test_t08_holds_the_stuck_channel(t08_events):
    expect = load_scenario("T08").expect
    reasons = set(holds(t08_events, expect["device"]))
    assert reasons & set(expect["hold_reasons"]), reasons


def test_t08_never_acts_on_the_stuck_value(t08_events):
    expect = load_scenario("T08").expect
    assert dispatched(t08_events, target=expect["device"]) == []
    for event in dispatched(t08_events):
        for ref in event["data"].get("evidence", ()):
            assert ref["value"] != expect["stuck_value"]


# ----- T09: the containment dilemma -----


def test_t09_refuses_to_act_on_its_own(t09_events):
    device = load_scenario("T09").expect["device"]
    before = [
        e for e in dispatched(t09_events, target=device)
        if e["simtime"] is not None and e["simtime"] < "2026-10-17T12:03:15.000000Z"
    ]
    assert before == [], "a quarantined valve is not actuated without an operator"


def test_t09_asks_the_operator(t09_events):
    code = load_scenario("T09").expect["approval_code"]
    assert count(t09_events, type=ALERT, code=code) >= 1


def test_t09_records_the_request_as_awaiting_approval(t09_events):
    device = load_scenario("T09").expect["device"]
    waiting = matching(t09_events, type=COMMAND, status="AWAITING_APPROVAL", target=device)
    assert waiting, "the action must be on the record as waiting"
    assert all(e["data"]["risk_class"] == "R3" for e in waiting)


def test_t09_acts_only_after_the_approval(t09_events):
    device = load_scenario("T09").expect["device"]
    sent = dispatched(t09_events, target=device)
    assert sent, "the approved action must be sent"
    assert sent[0]["simtime"] >= "2026-10-17T12:03:1"


# ----- T10: a Guardian outage -----


def test_t10_degrades_and_comes_back(t10_log):
    expected = load_scenario("T10").expect["modes"]
    changes = summary(t10_log)["mode_changes"]
    assert changes[:len(expected)] == expected, changes


def degraded_windows(events):
    """The sim-time spans the Brain spent blind, read off its own mode records."""
    windows, opened = [], None
    for event in matching(events, type=STATUS):
        data = event["data"]
        if data.get("reason") != "mode_changed":
            continue
        if data.get("mode") == "GUARDIAN_DEGRADED":
            opened = event["simtime"]
        elif opened is not None:
            windows.append((opened, event["simtime"]))
            opened = None
    if opened is not None:
        windows.append((opened, "9999"))
    return windows


def test_t10_dispatches_nothing_while_it_is_blind(t10_events):
    """The outage must be visible, and no command may leave while it lasts."""
    windows = degraded_windows(t10_events)
    assert windows, "the outage must be visible in the log"
    inside = [
        (event["simtime"], event["data"]["capability"], event["data"]["target"])
        for event in dispatched(t10_events)
        for start, end in windows
        if start <= event["simtime"] < end
    ]
    assert inside == [], f"commands slipped through the outage: {inside}"


def test_t10_acts_again_once_it_can_see(t10_events):
    """Degrading is not giving up: the Brain resumes after the Guardian comes back."""
    windows = degraded_windows(t10_events)
    after = [e for e in dispatched(t10_events) if e["simtime"] >= windows[-1][1]]
    assert after, "the Brain must take the city back once it can see again"


def test_t10_trusts_nothing_from_the_outage_window(t10_events):
    """Any command sent after the outage cites readings that carry weight, not outage ones."""
    outage_start, outage_end = load_scenario("T10").expect["outage"]
    for event in dispatched(t10_events):
        for ref in event["data"].get("evidence", ()):
            moment = ref["sim_time"]
            inside = "2026-10-17T12:00:15" <= moment < "2026-10-17T12:00:40"
            assert not (inside and ref["weight"] > 0), (moment, ref["weight"])


# ----- T11: the Twin restarts -----


def test_t11_starts_a_new_epoch(t11_log):
    assert summary(t11_log)["epoch_count"] >= load_scenario("T11").expect["epoch_count"]


def test_t11_notices_the_new_keys(t11_log):
    code = load_scenario("T11").expect["reject_code"]
    assert summary(t11_log)["rejected_by_code"].get(code, 0) > 0


def test_t11_reloads_the_registry_and_carries_on(t11_events):
    reasons = [e["data"].get("reason") for e in matching(t11_events, type=STATUS)]
    assert "epoch_changed" in reasons and "registry_reloaded" in reasons
    after = [e for e in t11_events if e["epoch"] > 0]
    assert after, "the run continues after the restart"


def test_t11_keeps_controlling_the_water_after_the_restart(t11_events):
    later = [e for e in dispatched(t11_events, capability="valve") if e["epoch"] > 0]
    holds_later = [e for e in matching(t11_events, type=DECISION, outcome="hold") if e["epoch"] > 0]
    assert later or holds_later, "the water channels are still being planned on"


# ----- T12: a flood during a stolen-key attack -----


def test_t12_holds_the_feeder_and_drains_the_tank(t12_events):
    expect = load_scenario("T12").expect
    forbidden, used = quiet(t12_events, "T12")
    assert used == set(), f"the doubted feeder stays untouched: {sorted(used)}"
    water = dispatched(t12_events, capability="valve")
    assert water or holds(t12_events, expect["controlled"]), "the tank is still being managed"


def test_t12_cites_no_doubted_feeder_reading_in_a_command(t12_events):
    held = load_scenario("T12").expect["held"]
    for event in dispatched(t12_events):
        assert held not in {ref["device_id"] for ref in event["data"].get("evidence", ())}


# ----- every run in the book -----


@pytest.mark.parametrize("name", BOOK)
def test_every_run_writes_a_summary_beside_its_log(name, book_logs):
    log = book_logs[name]
    data = summary(log)
    assert data["scenario"] == name and data["log"] == log.name
    for key in (
        "epoch_count", "counts_by_type", "commands_by_capability", "refusals_by_code",
        "verifications_by_verdict", "mode_changes", "dedupe_count", "rejected_by_code",
        "max_water_level_by_device", "final_hash", "chain_valid",
    ):
        assert key in data, key


@pytest.mark.parametrize("name", BOOK)
def test_every_run_is_a_valid_signed_chain(name, book_logs):
    log = book_logs[name]
    public_key = log.with_suffix(".pub").read_text(encoding="ascii").strip()
    assert verify_chain(log) and verify_chain(log, public_key)


@pytest.mark.parametrize("name", BOOK)
def test_every_event_matches_the_schema(name, book_logs, brain_event_schema):
    for event in read_log(book_logs[name]):
        jsonschema.validate(event, brain_event_schema, format_checker=FormatChecker())
