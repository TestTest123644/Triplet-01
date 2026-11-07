"""The Brain's own MCP server: read-only, so a partner can ask but never steer (spec 5.6).

Three questions, the same three the REST API answers: what a zone looks like, what incidents
are open, and why a decision was made. Nothing here can change the Brain's state — there is
no tool for it, and the operator's levers stay behind the authenticated REST endpoints.
"""

from collections.abc import Iterable, Mapping
from typing import Any

from mcp.server.fastmcp import FastMCP

DEFAULT_PORT = 8110


def build_brain_mcp_server(brain_app, decisions: Iterable[Mapping] = (), name: str = "city-brain") -> FastMCP:
    """A read-only MCP face over a running `BrainApp`."""
    server = FastMCP(name)

    @server.tool()
    def get_zone_state(zone_id: str) -> dict[str, Any]:
        """Every channel in a zone, with its estimate, trust and actuator."""
        from city_brain.adapters.rest.api import _device_state, _json

        state = brain_app.state
        if zone_id not in state.zones:
            return {"error": f"unknown zone '{zone_id}'"}
        devices = [d for d in sorted(state.registry) if state.registry[d].zone_id == zone_id]
        return {
            "zone_id": zone_id,
            "mode": str(state.mode),
            "channels": [_device_state(state, device_id) for device_id in devices],
            "incidents": [
                _json(incident) for incident in state.incidents.incidents.values()
                if incident.zone_id == zone_id
            ],
        }

    @server.tool()
    def list_incidents() -> list[dict[str, Any]]:
        """The incidents the Brain currently believes in."""
        from city_brain.adapters.rest.api import _json

        return [_json(i) for i in brain_app.state.incidents.incidents.values()]

    @server.tool()
    def explain_decision(decision_id: str) -> dict[str, Any]:
        """The nine answers behind one decision record."""
        for record in decisions:
            if record.get("id") != decision_id:
                continue
            data = record.get("data", {})
            return {
                "decision_id": decision_id,
                "simtime": record.get("simtime"),
                "outcome": data.get("outcome"),
                "incident": data.get("incident"),
                "rule_or_plan": data.get("rule_or_plan"),
                "evidence": data.get("evidence"),
                "alternatives": data.get("alternatives"),
                "rationale": data.get("rationale"),
                "would_change_if": data.get("would_change_if"),
            }
        return {"error": f"unknown decision '{decision_id}'"}

    return server


TOOL_NAMES = ("get_zone_state", "list_incidents", "explain_decision")
