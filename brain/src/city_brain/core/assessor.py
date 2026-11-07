"""Findings to belief, and belief to incidents (spec 8.3.2-8.3.4).

Belief is a log-odds sum: `L = prior(kind) + sum_i w_i * s_i * LR_i`, where `w_i` is how far
the Guardian trusts the device the finding came from, `s_i` how strongly the detector holds
it, and `LR_i` the finding's weight from policy. Belief is the logistic of `L`.

Three rules keep one event from being counted twice:

* one contribution per device per direction, the strongest, so three findings about the same
  tank are one piece of evidence for it and a contradiction is never swallowed by support;
* a finding the Guardian has already acted on is dropped: if its verdict cites
  RATE_OF_CHANGE the Brain adds no jump penalty of its own, and if it cites
  CROSS_SENSOR_CONFLICT the Brain adds no contradiction of its own;
* only a CONFIRMED correlation adds evidence; PROBABLE and HYPOTHETICAL are recorded for the
  operator and weigh nothing.
"""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime

from city_brain.core.detectors import Finding
from city_brain.domain.types import (
    CorrelationLabel,
    CorrelationLink,
    DeviceProfile,
    Incident,
    IncidentKind,
    SensorKind,
)

# Which incident each finding speaks to.
INCIDENT_OF = {
    "LEVEL_HIGH": IncidentKind.FLOOD_RISK,
    "LEVEL_CRITICAL": IncidentKind.FLOOD_RISK,
    "RISING": IncidentKind.FLOOD_RISK,
    "FLOW_ANOMALY": IncidentKind.CONGESTION,
    "FLOW_NORMAL": IncidentKind.CONGESTION,
    "LOAD_HIGH": IncidentKind.GRID_STRESS,
    "LOAD_JUMP": IncidentKind.GRID_STRESS,
    "AQI_HIGH": IncidentKind.AIR_QUALITY,
    "AQ_TRAFFIC_COUPLED": IncidentKind.AIR_QUALITY,
    "CALL_SURGE": IncidentKind.CALL_SURGE,
}
# A Guardian verdict reason, and the findings it means the Brain must not count again.
GUARDIAN_COVERS = {
    "RATE_OF_CHANGE": frozenset({"LOAD_JUMP"}),
    "CROSS_SENSOR_CONFLICT": frozenset({"FLOW_NORMAL", "AQ_TRAFFIC_COUPLED"}),
}
OPEN_STATUSES = ("suspected", "confirmed", "mitigating")


def belief_of(log_odds: float) -> float:
    """The logistic of a log-odds sum, without overflowing on a confident one."""
    if log_odds >= 0.0:
        return 1.0 / (1.0 + math.exp(-min(log_odds, 60.0)))
    return math.exp(max(log_odds, -60.0)) / (1.0 + math.exp(max(log_odds, -60.0)))


@dataclass
class IncidentBook:
    """Every incident the Brain is tracking, and what it remembers about the closed ones."""

    incidents: dict[str, Incident] = field(default_factory=dict)
    closed_at: dict[tuple[str, str], datetime] = field(default_factory=dict)
    opened: dict[tuple[str, str], int] = field(default_factory=dict)  # (zone, kind) -> count
    confirm_since: dict[str, datetime] = field(default_factory=dict)
    mitigated: set[str] = field(default_factory=set)

    def of_kind(self, zone_id: str, kind: IncidentKind) -> Incident | None:
        for incident_id in sorted(self.incidents):
            incident = self.incidents[incident_id]
            if incident.zone_id == zone_id and incident.kind is kind:
                return incident
        return None

    def mitigating(self, incident_id: str, now: datetime) -> Incident | None:
        """Record that a response to this incident has been dispatched."""
        incident = self.incidents.get(incident_id)
        if incident is None:
            return None
        self.mitigated.add(incident_id)
        updated = incident.model_copy(update={"status": "mitigating", "updated_at": now})
        self.incidents[incident_id] = updated
        return updated


# ----- the update -----


@dataclass(frozen=True)
class _Contribution:
    finding: Finding
    log_odds: float


def assess(
    book: IncidentBook,
    zone_id: str,
    findings: Sequence[Finding],
    trust: Mapping[str, float],
    verdict_reasons: Mapping[str, frozenset[str]],
    registry: Mapping[str, DeviceProfile],
    policy: Mapping,
    now: datetime,
) -> list[Incident]:
    """Fold the zone's findings into its incidents. Returns the incidents that changed."""
    assessor = policy["assessor"]
    kept = _admissible(findings, zone_id, registry, verdict_reasons)
    changed: list[Incident] = []
    for kind in IncidentKind:
        mine = [f for f in kept if INCIDENT_OF[f.kind] is kind]
        contributions = _strongest(mine, trust, assessor)
        links = _correlations(contributions, registry, trust, assessor)
        log_odds = float(assessor["prior"][str(kind)])
        log_odds += sum(c.log_odds for c in contributions.values())
        log_odds += float(assessor["correlation_bonus"]) * sum(
            1 for link in links if link.label is CorrelationLabel.CONFIRMED)
        updated = _settle(book, zone_id, kind, belief_of(log_odds), contributions, links, assessor, now)
        if updated is not None:
            changed.append(updated)
    return changed


def _admissible(
    findings: Sequence[Finding],
    zone_id: str,
    registry: Mapping[str, DeviceProfile],
    verdict_reasons: Mapping[str, frozenset[str]],
) -> list[Finding]:
    """Drop findings from unknown devices, other zones, and ones the Guardian already judged."""
    covered: set[str] = set()
    for reason, kinds in sorted(GUARDIAN_COVERS.items()):
        if any(reason in reasons for reasons in verdict_reasons.values()):
            covered |= kinds
    kept = []
    for finding in findings:
        if finding.zone_id != zone_id or finding.device_id not in registry:
            continue
        if finding.kind not in INCIDENT_OF:
            continue
        own = verdict_reasons.get(finding.device_id, frozenset())
        suppressed = finding.kind in covered and (
            finding.direction == -1 or finding.kind in GUARDIAN_COVERS.get("RATE_OF_CHANGE", ()))
        if suppressed and (own or finding.direction == -1):
            continue
        kept.append(finding)
    return kept


def _strongest(
    findings: Sequence[Finding], trust: Mapping[str, float], assessor: Mapping
) -> dict[tuple[str, int], _Contribution]:
    """One contribution per device per direction: the strongest it has to offer."""
    best: dict[tuple[str, int], _Contribution] = {}
    for finding in sorted(findings, key=lambda f: f.sort_key):
        # Trust enters convexly: half-trusted evidence is worth much less than half.
        weight = float(trust.get(finding.device_id, 0.0)) ** float(assessor["trust_exponent"])
        magnitude = weight * finding.score * float(assessor["log_lr"][finding.kind])
        if magnitude <= 0.0:
            continue
        key = (finding.device_id, finding.direction)
        contribution = _Contribution(finding, finding.direction * magnitude)
        current = best.get(key)
        if current is None or abs(contribution.log_odds) > abs(current.log_odds):
            best[key] = contribution
    return best


def _coupled(a: DeviceProfile, b: DeviceProfile, assessor: Mapping) -> str | None:
    """How the city's physics links two devices, or None."""
    for first, second in assessor["couplings"]:
        kinds = {SensorKind(first), SensorKind(second)}
        if {a.kind, b.kind} == kinds or (a.kind is b.kind and kinds == {a.kind}):
            if a.kind is b.kind:
                return "same kind in one zone"
            return f"{min(first, second)} follows {max(first, second)} in one zone"
    return None


def _correlations(
    contributions: Mapping[tuple[str, int], _Contribution],
    registry: Mapping[str, DeviceProfile],
    trust: Mapping[str, float],
    assessor: Mapping,
) -> tuple[CorrelationLink, ...]:
    """Label the pairs of corroborating devices behind one incident (spec 8.3.4)."""
    supporting = sorted(
        {device_id for (device_id, direction) in contributions if direction == 1})
    validated = float(assessor["validated_trust"])
    links = []
    for i, source in enumerate(supporting):
        for target in supporting[i + 1:]:
            reason = _coupled(registry[source], registry[target], assessor)
            if reason is None:
                continue  # co-occurrence alone is not a link worth recording
            both_validated = min(trust.get(source, 0.0), trust.get(target, 0.0)) >= validated
            label = CorrelationLabel.CONFIRMED if both_validated else CorrelationLabel.PROBABLE
            links.append(CorrelationLink(
                source_id=source, target_id=target, label=label, reason=reason))
    return tuple(links)


def _severity(belief: float, assessor: Mapping) -> str:
    bounds = assessor["severity"]
    for name in ("critical", "high", "medium"):
        if belief >= float(bounds[name]):
            return name
    return "low"


def _settle(
    book: IncidentBook,
    zone_id: str,
    kind: IncidentKind,
    belief: float,
    contributions: Mapping[tuple[str, int], _Contribution],
    links: tuple[CorrelationLink, ...],
    assessor: Mapping,
    now: datetime,
) -> Incident | None:
    """Open, update or close the zone's incident of this kind. Returns it if it changed."""
    current = book.of_kind(zone_id, kind)
    names = tuple(sorted(
        f"{c.finding.device_id}:{c.finding.kind}" for c in contributions.values()
        if c.finding.direction == 1))
    if current is None:
        if belief < float(assessor["open_belief"]):
            return None
        closed = book.closed_at.get((zone_id, str(kind)))
        if closed is not None and (now - closed).total_seconds() < float(assessor["reopen_cooldown_s"]):
            return None
        return _open(book, zone_id, kind, belief, names, links, assessor, now)
    if belief <= float(assessor["close_belief"]):
        return _close(book, current, now)
    status = _status(book, current, belief, kind, assessor, now)
    updated = current.model_copy(update={
        "belief": belief, "severity": _severity(belief, assessor), "status": status,
        "findings": names, "correlations": links, "updated_at": now,
    })
    book.incidents[current.incident_id] = updated
    if (current.belief, current.status, current.findings, current.correlations) == (
            updated.belief, updated.status, updated.findings, updated.correlations):
        return None
    return updated


def _open(
    book: IncidentBook,
    zone_id: str,
    kind: IncidentKind,
    belief: float,
    names: tuple[str, ...],
    links: tuple[CorrelationLink, ...],
    assessor: Mapping,
    now: datetime,
) -> Incident:
    key = (zone_id, str(kind))
    book.opened[key] = book.opened.get(key, 0) + 1
    incident_id = f"{str(kind).lower()}:{zone_id}:{book.opened[key]}"
    incident = Incident(
        incident_id=incident_id, zone_id=zone_id, kind=kind, belief=belief,
        severity=_severity(belief, assessor), status="suspected", findings=names,
        correlations=links, opened_at=now, updated_at=now,
    )
    book.incidents[incident_id] = incident
    if belief >= float(assessor["confirm_belief"]):
        book.confirm_since[incident_id] = now
        if str(kind) not in assessor["single_sensor_kinds"]:
            incident = incident.model_copy(update={"status": "confirmed"})
            book.incidents[incident_id] = incident
    return incident


def _status(
    book: IncidentBook,
    current: Incident,
    belief: float,
    kind: IncidentKind,
    assessor: Mapping,
    now: datetime,
) -> str:
    if current.status == "mitigating":
        return "mitigating"
    if belief < float(assessor["confirm_belief"]):
        book.confirm_since.pop(current.incident_id, None)
        return "suspected" if current.status == "confirmed" else current.status
    since = book.confirm_since.setdefault(current.incident_id, now)
    if str(kind) in assessor["single_sensor_kinds"]:
        held = (now - since).total_seconds() >= float(assessor["confirm_sustain_s"])
        return "confirmed" if held else current.status
    return "confirmed"


def _close(book: IncidentBook, current: Incident, now: datetime) -> Incident:
    status = "resolved" if current.incident_id in book.mitigated else "dismissed"
    closed = current.model_copy(update={"status": status, "updated_at": now})
    del book.incidents[current.incident_id]
    book.confirm_since.pop(current.incident_id, None)
    book.mitigated.discard(current.incident_id)
    book.closed_at[(current.zone_id, str(current.kind))] = now
    return closed
