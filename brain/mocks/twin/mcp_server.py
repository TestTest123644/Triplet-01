"""The mock Twin behind an MCP server, so the Brain's MCP client can be tested without the partner.

The tool list mirrors the Twin's own `twin/mcp_server.py` exactly: the same names, the same
arguments and the same replies, because the point of this server is to prove that the Brain
can drive the real one.
"""

import argparse
from typing import Any

from mcp.server.fastmcp import FastMCP

from mocks.twin import MockTwin

DEFAULT_PORT = 8000


def build_mock_mcp_server(twin: MockTwin, name: str = "mock-twin") -> FastMCP:
    server = FastMCP(name)

    @server.tool()
    def list_devices() -> list[dict[str, Any]]:
        """Every device in the city, with its range and actuator state."""
        return twin.list_devices()

    @server.tool()
    def get_device_key(device_id: str) -> str | None:
        """The base64 Ed25519 public key a device signs its readings with."""
        return twin.get_device_key(device_id)

    @server.tool()
    def get_device_state(device_id: str) -> dict[str, Any] | None:
        """One device's current value, unit and status."""
        device = twin.devices.get(device_id)
        if device is None:
            return None
        return {
            "device_id": device.device_id, "zone_id": device.zone_id,
            "device_type": device.device_type, "value": device.value, "unit": device.unit,
            "status": device.status, "actuator_state": dict(device.actuator_state),
        }

    @server.tool()
    def get_city_state() -> dict[str, Any]:
        """The whole city, zone by zone."""
        return twin.get_city_state()

    @server.tool()
    def set_valve(device_id: str, target_state: str, idempotency_key: str = "") -> dict[str, Any]:
        """Open, close or part-open a tank's drain valve."""
        return twin.handle_tool(
            "set_valve",
            {"device_id": device_id, "target_state": target_state, "idempotency_key": idempotency_key})

    @server.tool()
    def set_signal_plan(device_id: str, target_state: str, idempotency_key: str = "") -> dict[str, Any]:
        """Change a junction's signal plan."""
        return twin.handle_tool(
            "set_signal_plan",
            {"device_id": device_id, "target_state": target_state, "idempotency_key": idempotency_key})

    @server.tool()
    def switch_grid(device_id: str, target_state: str, idempotency_key: str = "") -> dict[str, Any]:
        """Move a feeder between grids, or shed it."""
        return twin.handle_tool(
            "switch_grid",
            {"device_id": device_id, "target_state": target_state, "idempotency_key": idempotency_key})

    @server.tool()
    def dispatch(zone_id: str, incident_type: str, idempotency_key: str = "") -> dict[str, Any]:
        """Send responders to a zone."""
        return twin.handle_tool(
            "dispatch",
            {"zone_id": zone_id, "incident_type": incident_type, "idempotency_key": idempotency_key})

    @server.tool()
    def get_command_result(idempotency_key: str) -> dict[str, Any] | None:
        """What a command with this key did, if the Twin has seen it."""
        return twin.handle_tool("get_command_result", {"idempotency_key": idempotency_key})

    return server


TOOL_NAMES = (
    "list_devices", "get_device_key", "get_device_state", "get_city_state", "set_valve",
    "set_signal_plan", "switch_grid", "dispatch", "get_command_result",
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mocks.twin.mcp_server")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    server = build_mock_mcp_server(MockTwin(seed=args.seed))
    server.settings.host, server.settings.port = args.host, args.port
    server.run(transport="sse")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
