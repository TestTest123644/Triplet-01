"""The Brain's HTTP face: what it believes, why it acted, and the operator's two levers.

Everything under `/v1` is read-only except the approval and control endpoints, which carry
the operator's bearer token and turn into `OperatorCommand`s for the core (spec 5.6, 12.4).
The API reads `BrainApp.state` directly: it never computes anything the core has not already
decided, so a query can never change what the Brain would do.
"""

import json
from collections.abc import Iterable, Mapping
from typing import Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from pydantic import BaseModel

from city_brain.core.events import OperatorCommand
from city_brain.core.records import format_time
from city_brain.core.water_mpc import planner_tier
from city_brain.domain.types import ActionStatus, SensorKind

EXPLAIN_QUESTIONS = (
    "what_happened", "how_sure", "why_this_action", "what_was_rejected", "what_it_rests_on",
    "what_would_change_it", "what_the_effect_was", "who_may_undo_it", "what_the_risk_was",
)


class ApprovalDecision(BaseModel):
    decision: Literal["approve", "deny"]


def _json(value: Any) -> Any:
    """Domain objects as JSON, through the same conversion the run log uses."""
    from city_brain.core.records import _native

    return _native(value)


def _device_state(state, device_id: str) -> dict[str, Any]:
    device = state.registry[device_id]
    channel = state.channel_filter(device_id)
    verdict = state.device_verdicts.get(device_id)
    estimate = None
    if channel is not None and channel.initialised:
        estimate = {
            "value": round(channel.level, 6),
            "rate": round(getattr(channel, "rate", getattr(channel, "drift", 0.0)), 6),
            "flags": sorted(channel.flags),
            "stale": channel.is_stale(state.global_tick),
        }
    recent = list(state.recent.get(device_id, ()))
    return {
        "device_id": device_id,
        "kind": str(device.kind),
        "unit": str(device.unit),
        "status": str(device.status),
        "state_kind": str(recent[-1].state_kind) if recent else "UNKNOWN",
        "trust_state": str(verdict.state) if verdict is not None else "UNKNOWN",
        "estimate": estimate,
        "actuator": _json(device.actuator) if device.actuator is not None else None,
    }


def _action_view(state, action_id: str) -> dict[str, Any]:
    record = state.actions[action_id]
    return {
        "action_id": action_id,
        "status": str(record.status),
        "reasons": list(record.reasons),
        "capability": record.intent.capability,
        "target": record.intent.target,
        "state": record.intent.state,
        "risk_class": str(record.intent.risk_class),
        "incident_id": record.intent.incident_id,
        "shield": {"passed": record.shield.passed, "refusals": list(record.shield.refusals)},
        "expected": _json(record.intent.expected_effect),
        "evidence": _json(record.intent.evidence),
        "rationale": record.intent.rationale,
        "dispatched_at": None if record.dispatched_at is None else format_time(record.dispatched_at),
    }


def _metrics(state) -> str:
    lines = [
        "# HELP city_brain_events_total Brain events published, by kind.",
        "# TYPE city_brain_events_total counter",
        f"city_brain_events_total {state.event_seq}",
        "# HELP city_brain_mode The mode the Brain is in (1 for the current one).",
        "# TYPE city_brain_mode gauge",
    ]
    for mode in sorted({str(state.mode), "WARMUP", "NORMAL", "GUARDIAN_DEGRADED",
                        "TWIN_DEGRADED", "PAUSED"}):
        lines.append(f'city_brain_mode{{mode="{mode}"}} {1 if str(state.mode) == mode else 0}')
    lines += [
        "# HELP city_brain_counter_total Edge and ingest counters.",
        "# TYPE city_brain_counter_total counter",
    ]
    for name in sorted(state.counters):
        lines.append(f'city_brain_counter_total{{code="{name}"}} {state.counters[name]}')
    lines += [
        "# HELP city_brain_actions Actions by status.",
        "# TYPE city_brain_actions gauge",
    ]
    by_status: dict[str, int] = {}
    for record in state.actions.values():
        by_status[str(record.status)] = by_status.get(str(record.status), 0) + 1
    for status in sorted(by_status):
        lines.append(f'city_brain_actions{{status="{status}"}} {by_status[status]}')
    lines += [
        "# HELP city_brain_incidents Open incidents by kind.",
        "# TYPE city_brain_incidents gauge",
    ]
    by_kind: dict[str, int] = {}
    for incident in state.incidents.incidents.values():
        by_kind[str(incident.kind)] = by_kind.get(str(incident.kind), 0) + 1
    for kind in sorted(by_kind):
        lines.append(f'city_brain_incidents{{kind="{kind}"}} {by_kind[kind]}')
    return "\n".join(lines) + "\n"


def build_rest_app(brain_app, operator_token: str, decisions: Iterable[Mapping] = ()) -> FastAPI:
    """The query and operator API over a running `BrainApp` (spec 5.6).

    `decisions` is anything that yields the decision records the Brain has published, newest
    last; the runtime passes its own log so `/v1/explain` can answer from what was written.
    """
    app = FastAPI(title="City Brain", version="1", docs_url="/docs")
    app.state.brain = brain_app
    app.state.decisions = decisions

    def operator(authorization: str = Header(default="")) -> str:
        """Only the operator may approve an action or stop the Brain."""
        if authorization != f"Bearer {operator_token}":
            raise HTTPException(status_code=401, detail="operator token required")
        return "operator"

    def state():
        return app.state.brain.state

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/ready")
    def ready() -> dict:
        brain = state()
        ready_now = bool(brain.registry) and brain.epoch_origin is not None
        return {"ready": ready_now, "devices": len(brain.registry), "mode": str(brain.mode)}

    @app.get("/v1/status")
    def status() -> dict:
        brain = state()
        return {
            "mode": str(brain.mode),
            "capability_tier": str(planner_tier(brain.policy)),
            "epoch": brain.epoch,
            "sim_time": None if brain.epoch_origin is None else format_time(brain.sim_now),
            "global_tick": brain.global_tick,
            "devices": len(brain.registry),
            "zones": sorted(brain.zones),
            "counters": dict(sorted(brain.counters.items())),
            "open_incidents": len(brain.incidents.incidents),
            "pending_approvals": sorted(brain.pending_approvals),
            "versions": {"schema": "1", "api": "1"},
        }

    @app.get("/v1/zones/{zone}")
    def zone(zone: str) -> dict:
        brain = state()
        if zone not in brain.zones:
            raise HTTPException(status_code=404, detail=f"unknown zone '{zone}'")
        devices = [d for d in sorted(brain.registry) if brain.registry[d].zone_id == zone]
        return {
            "zone_id": zone,
            "channels": [_device_state(brain, device_id) for device_id in devices],
            "incidents": [
                _json(incident) for incident in brain.incidents.incidents.values()
                if incident.zone_id == zone
            ],
            "actuators": [
                _json(brain.registry[d].actuator) for d in devices
                if brain.registry[d].actuator is not None
            ],
            "water_bands": {
                "low": brain.policy["water"]["low_band"],
                "high": brain.policy["water"]["high_band"],
                "critical": brain.policy["water"]["critical_band"],
            },
        }

    @app.get("/v1/incidents")
    def incidents() -> dict:
        brain = state()
        return {"incidents": [_json(i) for i in brain.incidents.incidents.values()]}

    @app.get("/v1/incidents/{incident_id}")
    def incident(incident_id: str) -> dict:
        brain = state()
        found = brain.incidents.incidents.get(incident_id)
        if found is None:
            raise HTTPException(status_code=404, detail=f"unknown incident '{incident_id}'")
        actions = [
            _action_view(brain, action_id) for action_id in sorted(brain.actions)
            if brain.actions[action_id].intent.incident_id == incident_id
        ]
        return {"incident": _json(found), "actions": actions}

    @app.get("/v1/actions/{action_id}")
    def action(action_id: str) -> dict:
        brain = state()
        if action_id not in brain.actions:
            raise HTTPException(status_code=404, detail=f"unknown action '{action_id}'")
        return _action_view(brain, action_id)

    @app.get("/v1/explain/{decision_id}")
    def explain(decision_id: str) -> dict:
        """The nine questions a decision has to be able to answer (spec 5.6)."""
        for record in app.state.decisions:
            if record.get("id") != decision_id:
                continue
            data = record.get("data", {})
            return {
                "decision_id": decision_id,
                "simtime": record.get("simtime"),
                "answers": {
                    "what_happened": data.get("incident"),
                    "how_sure": [
                        {"reading_id": ref.get("reading_id"), "state_kind": ref.get("state_kind"),
                         "trust_state": ref.get("trust_state"), "weight": ref.get("weight")}
                        for ref in data.get("evidence", ())
                    ],
                    "why_this_action": data.get("rationale"),
                    "what_was_rejected": data.get("alternatives"),
                    "what_it_rests_on": data.get("evidence"),
                    "what_would_change_it": data.get("would_change_if"),
                    "what_the_effect_was": record.get("actionid"),
                    "who_may_undo_it": "operator",
                    "what_the_risk_was": data.get("rule_or_plan"),
                },
            }
        raise HTTPException(status_code=404, detail=f"unknown decision '{decision_id}'")

    @app.get("/v1/approvals")
    def approvals() -> dict:
        brain = state()
        return {
            "approvals": [
                {**_action_view(brain, action_id), "asked_at": format_time(asked)}
                for action_id, asked in sorted(brain.pending_approvals.items())
                if action_id in brain.actions
            ]
        }

    @app.post("/v1/approvals/{action_id}")
    async def decide(action_id: str, body: ApprovalDecision, who: str = Depends(operator)) -> dict:
        brain = state()
        record = brain.actions.get(action_id)
        if record is None or record.status is not ActionStatus.AWAITING_APPROVAL:
            raise HTTPException(status_code=404, detail=f"no action '{action_id}' is waiting")
        await app.state.brain.feed(
            OperatorCommand(action=body.decision, action_id=action_id, operator_id=who))
        return {"action_id": action_id, "status": str(brain.actions[action_id].status)}

    @app.post("/v1/control/pause")
    async def pause(who: str = Depends(operator)) -> dict:
        await app.state.brain.feed(OperatorCommand(action="pause", operator_id=who))
        return {"mode": str(state().mode)}

    @app.post("/v1/control/resume")
    async def resume(who: str = Depends(operator)) -> dict:
        await app.state.brain.feed(OperatorCommand(action="resume", operator_id=who))
        return {"mode": str(state().mode)}

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(content=_metrics(state()), media_type="text/plain; version=0.0.4")

    return app


def water_devices(state) -> list[str]:
    """The zone's tanks, for a caller that wants only the water channels."""
    return [d for d in sorted(state.registry) if state.registry[d].kind is SensorKind.WATER_LEVEL]


def openapi_json(app: FastAPI) -> str:
    return json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n"
