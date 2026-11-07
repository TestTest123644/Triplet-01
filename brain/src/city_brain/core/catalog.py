"""The response catalog: what an incident asks for, step by step (spec 8.5).

The table itself lives in `policy.catalog`, so a response can be retuned without touching
code. Each entry is a list of steps; the Brain is on one step at a time per incident, and
moves to the next only when the step it tried could not be verified, or when another live
incident says the first step will not be enough.

Nothing here decides whether an action may run: the Shield and the trust gate do that.
"""

from collections.abc import Mapping, Sequence

from city_brain.core.shield import targets_a_zone
from city_brain.domain.ids import action_id
from city_brain.domain.types import (
    ActionIntent,
    ActionStatus,
    Alternative,
    DeviceProfile,
    DeviceStatus,
    EvidenceRef,
    ExpectedEffect,
    Incident,
    IncidentKind,
    RiskClass,
    SensorKind,
)

ACTIONABLE = ("confirmed", "mitigating")
# An action that is on its way: the incident is being mitigated.
UNDERWAY = frozenset({ActionStatus.AWAITING_TRUST, ActionStatus.DISPATCHED, ActionStatus.VERIFYING,
                      ActionStatus.VERIFIED, ActionStatus.UNVERIFIED})
# The channel each capability's effect shows up on.
EFFECT_KIND = {
    "signal": SensorKind.TRAFFIC_FLOW,
    "grid": SensorKind.POWER_LOAD,
    "dispatch": SensorKind.CALL_RATE,
}
OPEN_VALVE_STATES = ("open",)


def plan_catalog(incident: Incident, state, policy: Mapping) -> list[ActionIntent]:
    """The intents the incident's current step asks for, or none.

    `state` is the `BrainState`: the catalog reads the registry, the evidence the channels
    have collected and which step this incident is on, and writes nothing.
    """
    entry = policy["catalog"].get(str(incident.kind))
    if entry is None or incident.status not in ACTIONABLE:
        return []
    if not _preconditions_met(incident, state, entry):
        return []
    steps = entry["steps"]
    index = min(_step_index(incident, state, entry), len(steps) - 1)
    step = steps[index]
    targets = _targets(incident, state, step["capability"], policy)
    evidence = _evidence(incident, state)
    if not targets or not evidence:
        return []
    channel = _effect_channel(incident, state, step["capability"])
    if channel is None:
        return []
    return [
        _intent(incident, state, step, target, channel, evidence, index)
        for target in targets
    ]


def note_step_failed(state, incident_id: str) -> None:
    """A step that could not be verified: the next one is next (spec 8.5)."""
    state.plan_step[incident_id] = state.plan_step.get(incident_id, 0) + 1


# ----- the entry -----


def _device_of(finding_name: str) -> str:
    """A finding is named `<device_id>:<kind>`; a device id may itself contain colons."""
    return finding_name.rsplit(":", 1)[0]


def _preconditions_met(incident: Incident, state, entry: Mapping) -> bool:
    needed = entry.get("requires_finding")
    if needed is not None and not any(f.endswith(f":{needed}") for f in incident.findings):
        return False
    if not entry.get("requires_valve_open"):
        return True
    return any(_valve_committed(state, _device_of(f)) for f in incident.findings)


def _valve_committed(state, device_id: str) -> bool:
    """Whether this channel's valve is already open or no longer trusted to act (spec 8.5)."""
    device = state.registry.get(device_id)
    if device is None or device.actuator is None:
        return False
    if device.actuator.target in state.suspect:
        return True
    return device.actuator.current_state in OPEN_VALVE_STATES


def _step_index(incident: Incident, state, entry: Mapping) -> int:
    index = int(state.plan_step.get(incident.incident_id, 0))
    if index == 0 and _escalating(incident, state, entry):
        return 1
    return index


def _escalating(incident: Incident, state, entry: Mapping) -> bool:
    """Another live incident in the zone that means the first step will not be enough."""
    others = entry.get("escalate_with") or ()
    for name in others:
        other = state.incidents.of_kind(incident.zone_id, IncidentKind(name))
        if other is not None and other.status in ACTIONABLE:
            return True
    return False


# ----- targets, channels and evidence -----


def _zone_devices(state, zone_id: str, kind: SensorKind) -> list[DeviceProfile]:
    return [
        state.registry[device_id] for device_id in sorted(state.registry)
        if state.registry[device_id].zone_id == zone_id
        and state.registry[device_id].kind is kind
        and state.registry[device_id].status is DeviceStatus.OK
    ]


def _targets(incident: Incident, state, capability: str, policy: Mapping) -> list[str]:
    if targets_a_zone(capability, policy):
        return [incident.zone_id]
    return [
        device.actuator.target for device in _zone_devices(
            state, incident.zone_id, EFFECT_KIND[capability])
        if device.actuator is not None and device.actuator.capability == capability
    ]


def _effect_channel(incident: Incident, state, capability: str) -> str | None:
    """The device whose readings will show the effect: the first of its kind in the zone."""
    devices = _zone_devices(state, incident.zone_id, EFFECT_KIND[capability])
    return devices[0].device_id if devices else None


def _evidence(incident: Incident, state) -> tuple[EvidenceRef, ...]:
    """The readings behind the incident's findings, newest last."""
    refs: list[EvidenceRef] = []
    for name in incident.findings:
        refs.extend(state.recent.get(_device_of(name), ()))
    seen: dict[tuple[str, str], EvidenceRef] = {}
    for ref in refs:
        seen[(ref.reading_id, ref.sig_hash)] = ref
    return tuple(seen[key] for key in sorted(seen))


# ----- the intent -----


def _intent(
    incident: Incident,
    state,
    step: Mapping,
    target: str,
    channel: str,
    evidence: Sequence[EvidenceRef],
    index: int,
) -> ActionIntent:
    capability, wanted = step["capability"], step["state"]
    plan_version = int(state.plan_version.get(target, 0)) + 1
    effect = ExpectedEffect(
        channel=channel, metric="rate", comparator=step["comparator"],
        threshold=float(step["per_s"]), window_s=float(step["window_s"]),
    )
    rationale = (
        f"{incident.kind} in {incident.zone_id} is {incident.status} at belief "
        f"{incident.belief:.2f}; step {index} of the catalog is {capability} {wanted}."
    )
    alternatives = (
        Alternative(option="hold", rejected_because=f"{incident.kind} is {incident.status}"),
    )
    if index > 0:
        alternatives = (
            Alternative(option=f"step {index - 1}", rejected_because="it did not take effect"),
            *alternatives,
        )
    return ActionIntent(
        action_id=action_id(incident.incident_id, capability, target, wanted, plan_version),
        incident_id=incident.incident_id,
        plan_version=plan_version,
        capability=capability,
        target=target,
        state=wanted,
        risk_class=RiskClass(step["risk"]),
        expected_effect=effect,
        evidence=tuple(evidence),
        rationale=rationale,
        alternatives=alternatives,
    )
