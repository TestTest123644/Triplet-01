"""The response catalog, end to end through `step` (spec 8.5)."""

from datetime import timedelta

import pytest

from city_brain.core.catalog import plan_catalog
from city_brain.core.events import GuardianEventReceived, SendCommand
from city_brain.domain.types import GuardianContainment, IncidentKind, RiskClass, TrustState
from tests.helpers_city import (
    POLICY,
    ZONE,
    apply,
    city_state,
    commands,
    device_id,
    dispatched,
    published,
    tick,
    ticks,
)

CALM_TICKS = 700  # long enough for every channel's rolling baseline to settle


def calm_city(**kw):
    return ticks(city_state(**kw), CALM_TICKS)


def incident(state, kind):
    found = [i for i in state.incidents.incidents.values() if i.kind is kind]
    assert len(found) == 1, [(i.kind, i.status, round(i.belief, 3))
                             for i in state.incidents.incidents.values()]
    return found[0]


def sends(state, capability=None):
    out = [e for e in state.last_effects if isinstance(e, SendCommand)]
    return [e for e in out if capability is None or e.capability == capability]


# ----- a calm city asks for nothing (T01) -----


def test_a_calm_city_opens_no_incident():
    state = calm_city()
    assert state.incidents.incidents == {}


def test_a_calm_city_sends_no_non_water_command():
    state = calm_city()
    assert [e for e in sends(state) if e.capability != "valve"] == []


# ----- congestion -----


def congested(state, flow=160.0, n=80, **kw):
    both = {device_id("tc1"): flow, device_id("tc2"): flow - 5.0}
    return ticks(state, n, both, **kw)


def test_congestion_confirms_and_boosts_both_signals():
    state = congested(calm_city())
    assert incident(state, IncidentKind.CONGESTION).status == "mitigating"
    boosts = [c for c in dispatched(state, "signal")]
    assert {c["target"] for c in boosts} == {device_id("tc1"), device_id("tc2")}
    assert {c["state"] for c in boosts} == {"boost"}
    assert {c["risk_class"] for c in boosts} == {RiskClass.R1}


def test_the_boost_expects_the_flow_to_rise_on_a_camera():
    state = congested(calm_city())
    expected = dispatched(state, "signal")[0]["expected"]
    step = POLICY["catalog"]["CONGESTION"]["steps"][0]
    assert expected["comparator"] == ">=" and expected["window_s"] == step["window_s"]
    assert expected["channel"] in (device_id("tc1"), device_id("tc2"))


def test_one_spoofed_camera_is_contradicted_and_nothing_is_sent():
    state = ticks(calm_city(), 80, {device_id("tc1"): 170.0})
    assert state.incidents.incidents == {}
    assert sends(state, "signal") == []


def test_an_untrusted_camera_pair_moves_nothing():
    state = congested(calm_city(), verdict=TrustState.UNTRUSTED)
    assert state.incidents.incidents == {}
    assert sends(state, "signal") == []


def test_congestion_asks_for_the_signal_first_and_the_dispatch_once_calls_surge_too():
    state = congested(calm_city())
    congestion = incident(state, IncidentKind.CONGESTION)
    first = plan_catalog(congestion, state, POLICY)
    assert {i.capability for i in first} == {"signal"}
    both = {device_id("tc1"): 160.0, device_id("tc2"): 155.0, device_id("ec1"): 9.0}
    state = ticks(state, 120, both)
    assert incident(state, IncidentKind.CALL_SURGE)
    escalated = plan_catalog(incident(state, IncidentKind.CONGESTION), state, POLICY)
    assert [(i.capability, i.target, i.state, i.risk_class) for i in escalated] == [
        ("dispatch", ZONE, "traffic_accident", RiskClass.R2)]


def test_one_zone_takes_one_dispatch_at_a_time():
    state = calm_city()
    both = {device_id("tc1"): 160.0, device_id("tc2"): 155.0, device_id("ec1"): 9.0}
    state = ticks(state, 300, both)
    assert len(dispatched(state, "dispatch")) == 1


def test_the_catalog_step_advances_when_a_step_cannot_be_verified():
    state = congested(calm_city())
    congestion = incident(state, IncidentKind.CONGESTION)
    assert state.plan_step.get(congestion.incident_id, 0) == 0
    # The signal boost cannot take effect: the cameras keep reading the congested flow.
    state = congested(state, n=400)
    unverified = [e["data"] for e in published(state, "verification")
                  if e["data"]["verdict"] == "unverified"]
    assert unverified, "the boost should not have verified"
    assert state.plan_step.get(congestion.incident_id, 0) >= 1


# ----- grid stress -----


def stressed(state, load=100.0, n=120, **kw):
    return ticks(state, n, {device_id("pl1"): load}, **kw)


def test_grid_stress_confirms_only_after_the_sustain_and_then_switches_feeders():
    state = calm_city()
    short = stressed(state, n=20)
    assert incident(short, IncidentKind.GRID_STRESS).status == "suspected"
    assert sends(short, "grid") == []
    long_run = stressed(short, n=100)
    assert incident(long_run, IncidentKind.GRID_STRESS).status == "mitigating"
    switch = dispatched(long_run, "grid")
    assert [c["state"] for c in switch] == ["secondary"]
    assert switch[0]["target"] == device_id("pl1") and switch[0]["risk_class"] == RiskClass.R2


def test_grid_stress_escalates_to_shedding_when_the_feeder_change_does_nothing():
    state = stressed(calm_city(), n=600)
    assert [c["state"] for c in dispatched(state, "grid")] == ["secondary"]
    proposed = [c["state"] for c in commands(state, "grid")]
    assert "shed" in proposed, "the catalog must move to its next step"
    # The feeder measurably ignored the switch, so the Shield stops driving it and the
    # operator is told instead (spec 8.8); escalation plans the step, it does not force it.
    shed = [c for c in commands(state, "grid") if c["state"] == "shed"]
    assert shed[0]["shield"]["refusals"] == ["ACTUATOR_SUSPECT"]
    codes = [a["data"]["code"] for a in published(state, "alert")]
    assert "ACTUATOR_NO_EFFECT" in codes


def test_a_stolen_key_power_spike_sends_nothing():
    # The Guardian has already called the jump: the Brain must not count it a second time.
    state = calm_city()
    state = ticks(state, 200, {device_id("pl1"): 118.0}, verdict=TrustState.DOUBTFUL,
                  reasons=("RATE_OF_CHANGE",))
    assert [i for i in state.incidents.incidents.values()
            if i.kind is IncidentKind.GRID_STRESS] == []
    assert sends(state, "grid") == []


# ----- air quality -----


def test_high_air_quality_with_rising_traffic_restricts_the_signals():
    state = calm_city()
    rising = {device_id("tc1"): 120.0, device_id("tc2"): 118.0, device_id("aq1"): 170.0}
    state = ticks(state, 150, rising)
    assert incident(state, IncidentKind.AIR_QUALITY).status == "mitigating"
    restrict = dispatched(state, "signal")
    assert [c["state"] for c in restrict][:1] == ["restrict"]


def test_high_air_quality_with_calm_traffic_is_contradicted():
    state = ticks(calm_city(), 150, {device_id("aq1"): 170.0})
    air = incident(state, IncidentKind.AIR_QUALITY)
    assert air.status == "suspected" and sends(state, "signal") == []


# ----- emergency calls -----


def test_a_call_surge_dispatches_a_medical_surge():
    state = ticks(calm_city(), 150, {device_id("ec1"): 9.0})
    assert incident(state, IncidentKind.CALL_SURGE).status == "mitigating"
    medical = [c for c in dispatched(state, "dispatch") if c["state"] == "medical_surge"]
    assert medical and medical[0]["target"] == ZONE


def test_a_dispatch_is_rate_limited_per_zone():
    state = ticks(calm_city(), 150, {device_id("ec1"): 9.0})
    first = len([c for c in dispatched(state, "dispatch")])
    cooldown = POLICY["shield"]["cooldown_s"]["dispatch"]
    assert cooldown == 120.0
    state = ticks(state, 300, {device_id("ec1"): 9.0})  # 30 s: inside the cooldown
    assert len([c for c in dispatched(state, "dispatch")]) == first


# ----- flood -----


def test_a_critical_tank_with_an_open_valve_calls_for_a_flood_dispatch():
    state = city_state(**{"wl1": {"state": "open"}})
    state = ticks(state, 400, {device_id("wl1"): 9.6})
    assert incident(state, IncidentKind.FLOOD_RISK).status == "mitigating"
    floods = [c for c in dispatched(state, "dispatch") if c["state"] == "flood"]
    assert floods and floods[0]["target"] == ZONE


def test_a_critical_tank_whose_valve_has_not_opened_yet_asks_for_no_dispatch():
    # No Twin reply, so the valve never reads as open: the callout waits for the drain.
    state = ticks(city_state(), 400, {device_id("wl1"): 9.6}, reply=None)
    assert incident(state, IncidentKind.FLOOD_RISK).status == "confirmed"
    assert [c for c in dispatched(state, "dispatch") if c["state"] == "flood"] == []


def test_a_high_but_not_critical_tank_asks_for_no_dispatch():
    state = city_state(**{"wl1": {"state": "open"}})
    state = ticks(state, 400, {device_id("wl1"): 6.5})
    assert dispatched(state, "dispatch") == []


# ----- the planner on its own -----


def test_a_suspected_incident_asks_for_nothing():
    state = ticks(calm_city(), 20, {device_id("pl1"): 100.0})
    assert plan_catalog(incident(state, IncidentKind.GRID_STRESS), state, POLICY) == []


def test_an_incident_kind_without_an_entry_asks_for_nothing():
    state = calm_city()
    policy = {**POLICY, "catalog": {}}
    state = ticks(state, 120, {device_id("pl1"): 100.0})
    assert plan_catalog(incident(state, IncidentKind.GRID_STRESS), state, policy) == []


def test_a_contained_actuator_is_not_targeted():
    state = calm_city()
    state = apply(state, GuardianEventReceived(GuardianContainment(
        device_id=device_id("pl1"), action="quarantined", effective_at=state.sim_now,
        reasons=frozenset({"SCRIPTED"}), issued_at=state.sim_now)))
    state = stressed(state, n=200)
    assert sends(state, "grid") == []


def test_every_intent_cites_the_evidence_behind_its_incident():
    state = congested(calm_city())
    boost = dispatched(state, "signal")[0]
    cited = {ref["device_id"] for ref in boost["evidence"]}
    assert cited == {device_id("tc1"), device_id("tc2")}


def test_incidents_are_published_as_they_change():
    state = congested(calm_city())
    kinds = [e["data"]["status"] for e in published(state, "incident")]
    assert "suspected" in kinds or "confirmed" in kinds
    assert "mitigating" in kinds


def test_the_same_readings_give_the_same_commands():
    first = congested(calm_city())
    second = congested(calm_city())
    assert [(c.capability, c.target, c.state) for c in sends(first)] == [
        (c.capability, c.target, c.state) for c in sends(second)]
