"""MCP (Model Context Protocol) Server for Twin Layer Command Interface.

Assigned to: Agent_MCP
Exposes synchronous RPC tools for Layer 2 (City Brain) and Layer 3 (Guardian).
Does NOT route over Kafka. Strictly isolated from Ground Truth SQLite database.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
try:
    from mcp.server.mcpserver import MCPServer
except ImportError:
    from mcp.server.fastmcp import FastMCP as MCPServer

from twin.simulation import CitySimulator

logger = logging.getLogger("twin.mcp_server")

# Global singleton simulator reference for the MCP runtime
_ACTIVE_SIMULATOR: Optional[CitySimulator] = None


def get_active_simulator() -> CitySimulator:
    """Get active simulator instance or initialize default."""
    global _ACTIVE_SIMULATOR
    if _ACTIVE_SIMULATOR is None:
        _ACTIVE_SIMULATOR = CitySimulator(seed=42)
    return _ACTIVE_SIMULATOR


def set_active_simulator(sim: CitySimulator) -> None:
    """Set active simulator instance for testing or runtime binding."""
    global _ACTIVE_SIMULATOR
    _ACTIVE_SIMULATOR = sim


# MCPServer Instance
mcp = MCPServer("TwinLayerCommandServer")


# ============================================================================
# 1. QUERIES (Layer 2 - Brain & Layer 3 - Guardian)
# ============================================================================

@mcp.tool()
def list_devices(zone_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return inventory and physical ranges (upper/lower bounds) of registered devices.

    Args:
        zone_id: Optional filter by zone_id (e.g., 'zone-1' to 'zone-6').
    """
    sim = get_active_simulator()
    devices = sim.list_devices_spec()
    if zone_id:
        devices = [d for d in devices if d["zone_id"] == zone_id]
    return devices


@mcp.tool()
def get_device_key(device_id: str) -> Optional[str]:
    """Return the Ed25519 public key (base64) for a given device_id.

    Used by Guardian (Layer 3) to verify cryptographic signatures of Kafka sensor readings.
    """
    sim = get_active_simulator()
    return sim.get_device_public_key(device_id)


@mcp.tool()
def get_device_state(device_id: str) -> Optional[Dict[str, Any]]:
    """Fetch current telemetry and operational state for a specific device.

    Args:
        device_id: The identifier of the device.
    """
    sim = get_active_simulator()
    return sim.get_device_state(device_id)


@mcp.tool()
def get_city_state() -> Dict[str, Any]:
    """Fetch high-level overview of city state across all 6 zones and active dispatches."""
    sim = get_active_simulator()
    return sim.get_city_state()


@mcp.tool()
def get_reading(reading_id: str) -> Optional[Dict[str, Any]]:
    """Fetch past published reading using its reading_id from the broadcast cache."""
    sim = get_active_simulator()
    return sim.get_cached_reading(reading_id)


@mcp.tool()
def get_readings(
    device_id: Optional[str] = None,
    since_ts: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Fetch list of recent published readings from the broadcast cache.

    Args:
        device_id: Optional filter by device ID.
        since_ts: Optional RFC 3339 UTC timestamp to fetch readings occurring at or after this time.
        limit: Maximum number of readings to return (default: 50).
    """
    sim = get_active_simulator()
    readings = list(sim.recent_readings_buffer.values())
    if device_id:
        readings = [r for r in readings if r.get("device_id") == device_id]
    if since_ts:
        readings = [r for r in readings if r.get("ts", "") >= since_ts]
    return readings[-limit:]


# ============================================================================
# 2. ACTUATION (Called by Layer 2 - City Brain)
# ============================================================================

@mcp.tool()
def dispatch(incident_type: str, zone_id: str, idempotency_key: Optional[str] = None) -> Dict[str, Any]:
    """Dispatch emergency response units to an incident in a specific zone.

    Reduces emergency call volume and resolves incidents in the target zone.
    """
    sim = get_active_simulator()
    return sim.handle_dispatch(incident_type=incident_type, zone_id=zone_id, idempotency_key=idempotency_key)


@mcp.tool()
def set_signal_plan(device_id: str, target_state: str, idempotency_key: Optional[str] = None) -> Dict[str, Any]:
    """Update traffic signal timing plan (e.g., 'green_wave', 'normal', 'red_restriction').

    Directly influences vehicle throughput and congestion in the next physics tick.
    """
    sim = get_active_simulator()
    return sim.handle_set_signal_plan(device_id=device_id, target_state=target_state, idempotency_key=idempotency_key)


@mcp.tool()
def set_valve(device_id: str, target_state: str, idempotency_key: Optional[str] = None) -> Dict[str, Any]:
    """Actuate drainage or flood-control water valve (e.g., 'open', 'closed', '50%').

    Directly changes water level discharge dynamics in the next physics tick.
    """
    sim = get_active_simulator()
    return sim.handle_set_valve(device_id=device_id, target_state=target_state, idempotency_key=idempotency_key)


@mcp.tool()
def switch_grid(device_id: str, target_state: str, idempotency_key: Optional[str] = None) -> Dict[str, Any]:
    """Switch power grid substation feeder or load shedding state (e.g., 'grid_a', 'grid_b', 'shed', 'standby').

    Directly modulates electrical power load consumption in the next physics tick.
    """
    sim = get_active_simulator()
    return sim.handle_switch_grid(device_id=device_id, target_state=target_state, idempotency_key=idempotency_key)


@mcp.tool()
def get_command_result(idempotency_key: str) -> Optional[Dict[str, Any]]:
    """Retrieve execution result of an actuation command by its idempotency key."""
    sim = get_active_simulator()
    return sim.get_command_result(idempotency_key)


# ============================================================================
# 3. CONTAINMENT (Called by Layer 3 - Guardian)
# ============================================================================

@mcp.tool()
def isolate_device(device_id: str) -> Dict[str, Any]:
    """Isolate a suspect device from physical actuation and mark its telemetry status as 'isolated'."""
    sim = get_active_simulator()
    return sim.update_device_status(device_id=device_id, new_status="isolated")


@mcp.tool()
def quarantine_device(device_id: str) -> Dict[str, Any]:
    """Quarantine an anomalous or compromised device, updating its status to 'quarantined' in the stream."""
    sim = get_active_simulator()
    return sim.update_device_status(device_id=device_id, new_status="quarantined")


@mcp.tool()
def reinstate_device(device_id: str) -> Dict[str, Any]:
    """Reinstate an isolated or quarantined device back to operational status 'ok'."""
    sim = get_active_simulator()
    return sim.update_device_status(device_id=device_id, new_status="ok")


@mcp.tool()
def release_device(device_id: str) -> Dict[str, Any]:
    """Release a device from containment back to operational status 'ok'."""
    sim = get_active_simulator()
    return sim.update_device_status(device_id=device_id, new_status="ok")


@mcp.tool()
def rollback_reading(reading_id: str) -> Dict[str, Any]:
    """Flag a falsified or spoofed reading in the stream history as 'rolled_back'."""
    sim = get_active_simulator()
    return sim.rollback_reading(reading_id=reading_id)


if __name__ == "__main__":
    import os
    import uvicorn

    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8000"))
    print(f"Starting Twin MCP Command Server on http://{host}:{port}/sse...")
    uvicorn.run(mcp.sse_app(host=host), host=host, port=port)
