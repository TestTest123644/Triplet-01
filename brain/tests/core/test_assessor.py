"""The log-odds assessor, labelled correlations and the incident lifecycle (spec 8.3.2-8.3.4)."""

from datetime import timedelta

import pytest

from city_brain.config import load_config
from city_brain.core.assessor import IncidentBook, assess, belief_of
from city_brain.core.detectors import Finding
from city_brain.domain.types import (
    CanonicalUnit,
    CorrelationLabel,
    DeviceProfile,
    DeviceStatus,
    IncidentKind,
    SensorKind,
)
from tests.factories import T0

POLICY = load_config("config/standalone.yaml").policy
ASSESSOR = POLICY["assessor"]
ZONE = "zone-1"

UNITS = {
    SensorKind.WATER_LEVEL: CanonicalUnit.M,
    SensorKind.TRAFFIC_FLOW: CanonicalUnit.VEH_PER_MIN,
    SensorKind.POWER_LOAD: CanonicalUnit.MW,
    SensorKind.AIR_QUALITY: CanonicalUnit.AQI,
    SensorKind.CALL_RATE: CanonicalUnit.CALLS_PER_MIN,
}


def device(device_id, kind):
    return DeviceProfile(
        device_id=device_id, zone_id=ZONE, kind=kind, unit=UNITS[kind],
        min_val=0.0, max_val=400.0, status=DeviceStatus.OK, actuator=None,
    )


REGISTRY = {
    "w1": device("w1", SensorKind.WATER_LEVEL),
    "t1": device("t1", SensorKind.TRAFFIC_FLOW),
    "t2": device("t2", SensorKind.TRAFFIC_FLOW),
    "p1": device("p1", SensorKind.POWER_LOAD),
    "a1": device("a1", SensorKind.AIR_QUALITY),
    "c1": device("c1", SensorKind.CALL_RATE),
}


def finding(device_id, kind, direction=1, score=1.0, value=1.0, now=T0):
    return Finding(device_id=device_id, zone_id=ZONE, kind=kind, direction=direction,
                   value=value, score=score, sim_time=now)


def run(book, findings, trust=None, reasons=None, now=T0, zone_id=ZONE):
    trust = {d: 1.0 for d in REGISTRY} if trust is None else trust
    return assess(book, zone_id, findings, trust, reasons or {}, REGISTRY, POLICY, now)


def tracked(book, kind):
    found = [i for i in book.incidents.values() if i.kind is kind]
    assert len(found) == 1, [(i.kind, i.status) for i in book.incidents.values()]
    return found[0]


def kinds(book):
    return sorted(str(i.kind) for i in book.incidents.values())


# ----- the log-odds update -----


def test_belief_of_a_bare_prior():
    assert belief_of(-2.0) == pytest.approx(0.1192, abs=1e-4)
    assert belief_of(0.0) == pytest.approx(0.5)
    assert belief_of(3.2) == pytest.approx(0.9608, abs=1e-4)


def test_no_findings_open_nothing():
    book = IncidentBook()
    assert run(book, []) == [] and book.incidents == {}


def test_a_single_anomalous_camera_is_suspected_not_confirmed():
    book = IncidentBook()
    run(book, [finding("t1", "FLOW_ANOMALY")])
    incident = tracked(book, IncidentKind.CONGESTION)
    assert incident.status == "suspected" and incident.belief < ASSESSOR["confirm_belief"]


def test_a_contradicting_sibling_keeps_the_belief_down():
    book = IncidentBook()
    run(book, [finding("t1", "FLOW_ANOMALY"), finding("t2", "FLOW_NORMAL", direction=-1)])
    assert book.incidents == {}, "a contradicted anomaly is not an incident"


def test_both_cameras_high_confirms_congestion():
    book = IncidentBook()
    changed = run(book, [finding("t1", "FLOW_ANOMALY"), finding("t2", "FLOW_ANOMALY")])
    incident = tracked(book, IncidentKind.CONGESTION)
    assert incident.status == "confirmed" and incident.belief >= ASSESSOR["confirm_belief"]
    assert changed == [incident]


def test_a_doubtful_single_sensor_cannot_raise_an_incident_on_its_own():
    book = IncidentBook()
    run(book, [finding("p1", "LOAD_HIGH")], trust={"p1": 0.5})
    assert book.incidents == {}


def test_an_untrusted_sensor_moves_nothing():
    book = IncidentBook()
    run(book, [finding("t1", "FLOW_ANOMALY"), finding("t2", "FLOW_ANOMALY")], trust={"t1": 0.0, "t2": 0.0})
    assert book.incidents == {}


def test_a_doubtful_sensor_moves_less_than_a_trusted_one():
    doubtful, trusted = IncidentBook(), IncidentBook()
    run(doubtful, [finding("p1", "LOAD_HIGH")], trust={"p1": 0.9})
    run(trusted, [finding("p1", "LOAD_HIGH")], trust={"p1": 1.0})
    assert tracked(doubtful, IncidentKind.GRID_STRESS).belief < tracked(
        trusted, IncidentKind.GRID_STRESS).belief


def test_a_weaker_score_moves_less():
    weak, strong = IncidentBook(), IncidentBook()
    run(weak, [finding("p1", "LOAD_HIGH", score=0.7)])
    run(strong, [finding("p1", "LOAD_HIGH", score=1.0)])
    assert tracked(weak, IncidentKind.GRID_STRESS).belief < tracked(
        strong, IncidentKind.GRID_STRESS).belief


def test_findings_from_one_device_count_once_per_direction():
    stacked, single = IncidentBook(), IncidentBook()
    run(stacked, [finding("w1", "LEVEL_HIGH"), finding("w1", "RISING")])
    run(single, [finding("w1", "LEVEL_HIGH")])
    assert tracked(stacked, IncidentKind.FLOOD_RISK).belief == pytest.approx(
        tracked(single, IncidentKind.FLOOD_RISK).belief)


def test_the_strongest_finding_of_a_device_is_the_one_that_counts():
    book = IncidentBook()
    run(book, [finding("w1", "LEVEL_HIGH"), finding("w1", "LEVEL_CRITICAL")])
    incident = tracked(book, IncidentKind.FLOOD_RISK)
    assert incident.belief == pytest.approx(belief_of(-2.0 + ASSESSOR["log_lr"]["LEVEL_CRITICAL"]))


def test_a_contradiction_from_the_same_device_still_counts():
    book = IncidentBook()
    run(book, [finding("a1", "AQI_HIGH"), finding("a1", "AQ_TRAFFIC_COUPLED", direction=-1)])
    incident = tracked(book, IncidentKind.AIR_QUALITY)
    expected = -2.0 + ASSESSOR["log_lr"]["AQI_HIGH"] - ASSESSOR["log_lr"]["AQ_TRAFFIC_COUPLED"]
    assert incident.belief == pytest.approx(belief_of(expected))
    assert incident.status == "suspected"


def test_a_device_outside_the_registry_is_ignored():
    book = IncidentBook()
    run(book, [finding("nope", "FLOW_ANOMALY")], trust={"nope": 1.0})
    assert book.incidents == {}


# ----- the Guardian has already judged it (no double counting) -----


def test_a_rate_of_change_verdict_suppresses_the_brains_jump_penalty():
    book = IncidentBook()
    run(book, [finding("p1", "LOAD_JUMP")], trust={"p1": 0.5},
        reasons={"p1": frozenset({"RATE_OF_CHANGE"})})
    assert book.incidents == {}


def test_a_stolen_key_power_spike_stays_below_the_close_belief():
    book = IncidentBook()
    run(book, [finding("p1", "LOAD_JUMP", score=1.0)], trust={"p1": 0.5},
        reasons={"p1": frozenset({"RATE_OF_CHANGE", "SCRIPTED"})})
    assert belief_of(ASSESSOR["prior"]["GRID_STRESS"]) < ASSESSOR["close_belief"]
    assert book.incidents == {}


def test_a_cross_sensor_verdict_suppresses_the_brains_contradiction():
    book = IncidentBook()
    run(book, [finding("t1", "FLOW_ANOMALY"), finding("t2", "FLOW_NORMAL", direction=-1)],
        reasons={"t1": frozenset({"CROSS_SENSOR_CONFLICT"})})
    assert tracked(book, IncidentKind.CONGESTION).status == "suspected"


def test_an_unrelated_verdict_reason_suppresses_nothing():
    book = IncidentBook()
    run(book, [finding("t1", "FLOW_ANOMALY"), finding("t2", "FLOW_NORMAL", direction=-1)],
        reasons={"t1": frozenset({"SCRIPTED"})})
    assert book.incidents == {}, "the contradiction still stands"


def test_a_jump_alone_does_not_open_an_incident():
    book = IncidentBook()
    run(book, [finding("p1", "LOAD_JUMP")])
    assert book.incidents == {}


# ----- labelled correlations -----


def links(incident):
    return {(c.source_id, c.target_id): c.label for c in incident.correlations}


def test_coupled_siblings_on_validated_data_are_confirmed():
    book = IncidentBook()
    run(book, [finding("t1", "FLOW_ANOMALY"), finding("t2", "FLOW_ANOMALY")])
    assert links(tracked(book, IncidentKind.CONGESTION)) == {
        ("t1", "t2"): CorrelationLabel.CONFIRMED}


def test_coupled_siblings_on_doubtful_data_are_only_probable():
    book = IncidentBook()
    run(book, [finding("t1", "FLOW_ANOMALY"), finding("t2", "FLOW_ANOMALY")],
        trust={"t1": 0.8, "t2": 0.8})
    assert links(tracked(book, IncidentKind.CONGESTION)) == {
        ("t1", "t2"): CorrelationLabel.PROBABLE}


def test_only_a_confirmed_correlation_adds_evidence():
    confirmed, probable = IncidentBook(), IncidentBook()
    run(confirmed, [finding("t1", "FLOW_ANOMALY", score=0.7), finding("t2", "FLOW_ANOMALY", score=0.7)])
    run(probable, [finding("t1", "FLOW_ANOMALY", score=0.7), finding("t2", "FLOW_ANOMALY", score=0.7)],
        trust={"t1": 0.9, "t2": 0.85})
    bonus = ASSESSOR["correlation_bonus"]
    assert tracked(confirmed, IncidentKind.CONGESTION).belief > tracked(
        probable, IncidentKind.CONGESTION).belief
    assert bonus > 0.0


def test_an_uncoupled_pair_is_hypothetical():
    book = IncidentBook()
    run(book, [finding("c1", "CALL_SURGE"), finding("c1", "CALL_SURGE")])
    assert links(tracked(book, IncidentKind.CALL_SURGE)) == {}


def test_a_correlation_needs_two_devices():
    book = IncidentBook()
    run(book, [finding("t1", "FLOW_ANOMALY")])
    assert links(tracked(book, IncidentKind.CONGESTION)) == {}


def test_a_contradicting_pair_is_not_correlated():
    book = IncidentBook()
    run(book, [finding("t1", "FLOW_ANOMALY"), finding("t2", "FLOW_NORMAL", direction=-1)],
        reasons={"t1": frozenset({"CROSS_SENSOR_CONFLICT"})})
    assert links(tracked(book, IncidentKind.CONGESTION)) == {}


# ----- the lifecycle -----


def congest(book, now, score=1.0):
    return run(book, [finding("t1", "FLOW_ANOMALY", score=score),
                      finding("t2", "FLOW_ANOMALY", score=score)], now=now)


def test_an_incident_opens_suspected_and_then_confirms():
    book = IncidentBook()
    run(book, [finding("t1", "FLOW_ANOMALY")])
    assert tracked(book, IncidentKind.CONGESTION).status == "suspected"
    congest(book, T0 + timedelta(seconds=1))
    assert tracked(book, IncidentKind.CONGESTION).status == "confirmed"


def test_the_incident_id_and_opening_time_survive_a_status_change():
    book = IncidentBook()
    run(book, [finding("t1", "FLOW_ANOMALY")])
    before = tracked(book, IncidentKind.CONGESTION)
    congest(book, T0 + timedelta(seconds=1))
    after = tracked(book, IncidentKind.CONGESTION)
    assert after.incident_id == before.incident_id and after.opened_at == before.opened_at
    assert after.updated_at == T0 + timedelta(seconds=1)


def test_below_the_open_belief_nothing_is_tracked():
    book = IncidentBook()
    run(book, [finding("w1", "RISING", score=0.7)])
    assert book.incidents == {}


def test_a_resolved_incident_leaves_the_book():
    book = IncidentBook()
    congest(book, T0)
    changed = run(book, [], now=T0 + timedelta(seconds=1))
    assert book.incidents == {}
    assert [i.status for i in changed] == ["dismissed"]


def test_a_mitigated_incident_resolves_rather_than_being_dismissed():
    book = IncidentBook()
    congest(book, T0)
    book.mitigating(tracked(book, IncidentKind.CONGESTION).incident_id, T0)
    changed = run(book, [], now=T0 + timedelta(seconds=1))
    assert [i.status for i in changed] == ["resolved"]


def test_a_mitigating_incident_stays_mitigating_while_the_belief_holds():
    book = IncidentBook()
    congest(book, T0)
    book.mitigating(tracked(book, IncidentKind.CONGESTION).incident_id, T0)
    congest(book, T0 + timedelta(seconds=1))
    assert tracked(book, IncidentKind.CONGESTION).status == "mitigating"


def test_an_incident_does_not_reopen_inside_the_cooldown():
    book = IncidentBook()
    congest(book, T0)
    run(book, [], now=T0 + timedelta(seconds=1))
    congest(book, T0 + timedelta(seconds=2))
    assert book.incidents == {}


def test_an_incident_reopens_with_a_new_id_after_the_cooldown():
    book = IncidentBook()
    congest(book, T0)
    first = tracked(book, IncidentKind.CONGESTION).incident_id
    run(book, [], now=T0 + timedelta(seconds=1))
    later = T0 + timedelta(seconds=1 + ASSESSOR["reopen_cooldown_s"])
    congest(book, later)
    assert tracked(book, IncidentKind.CONGESTION).incident_id != first


def test_a_single_sensor_domain_needs_the_belief_sustained_to_confirm():
    book = IncidentBook()
    run(book, [finding("p1", "LOAD_HIGH")], now=T0)
    assert tracked(book, IncidentKind.GRID_STRESS).status == "suspected"
    half = T0 + timedelta(seconds=ASSESSOR["confirm_sustain_s"] / 2)
    run(book, [finding("p1", "LOAD_HIGH")], now=half)
    assert tracked(book, IncidentKind.GRID_STRESS).status == "suspected"
    later = T0 + timedelta(seconds=ASSESSOR["confirm_sustain_s"])
    run(book, [finding("p1", "LOAD_HIGH")], now=later)
    assert tracked(book, IncidentKind.GRID_STRESS).status == "confirmed"


def test_a_lapse_in_belief_restarts_the_sustain_clock():
    book = IncidentBook()
    run(book, [finding("p1", "LOAD_HIGH")], now=T0)
    run(book, [finding("p1", "LOAD_HIGH", score=0.6)], now=T0 + timedelta(seconds=1))
    later = T0 + timedelta(seconds=ASSESSOR["confirm_sustain_s"] + 1)
    run(book, [finding("p1", "LOAD_HIGH")], now=later)
    assert tracked(book, IncidentKind.GRID_STRESS).status == "suspected"


def test_severity_follows_the_belief():
    weak, middling, strong = IncidentBook(), IncidentBook(), IncidentBook()
    run(weak, [finding("t1", "FLOW_ANOMALY", score=0.7)])
    run(middling, [finding("t1", "FLOW_ANOMALY", score=0.9)])
    congest(strong, T0)
    assert tracked(weak, IncidentKind.CONGESTION).severity == "low"
    assert tracked(middling, IncidentKind.CONGESTION).severity == "medium"
    assert tracked(strong, IncidentKind.CONGESTION).severity == "critical"


def test_the_incident_lists_the_findings_behind_it():
    book = IncidentBook()
    congest(book, T0)
    assert tracked(book, IncidentKind.CONGESTION).findings == ("t1:FLOW_ANOMALY", "t2:FLOW_ANOMALY")


def test_only_changed_incidents_are_returned():
    book = IncidentBook()
    congest(book, T0)
    assert congest(book, T0 + timedelta(seconds=1)) == []


def test_zones_keep_their_own_incidents():
    book = IncidentBook()
    congest(book, T0)
    assert run(book, [], now=T0 + timedelta(seconds=1), zone_id="zone-2") == []
    assert kinds(book) == ["CONGESTION"]


def test_every_incident_kind_can_open():
    book = IncidentBook()
    run(book, [
        finding("w1", "LEVEL_CRITICAL"),
        finding("t1", "FLOW_ANOMALY"), finding("t2", "FLOW_ANOMALY"),
        finding("p1", "LOAD_HIGH"),
        finding("a1", "AQI_HIGH"),
        finding("c1", "CALL_SURGE"),
    ])
    assert kinds(book) == ["AIR_QUALITY", "CALL_SURGE", "CONGESTION", "FLOOD_RISK", "GRID_STRESS"]
