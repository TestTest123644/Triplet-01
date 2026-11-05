"""MCP (Model Context Protocol) Server for Twin Layer Command Interface.

Assigned to: Agent_MCP
Exposes synchronous RPC tools for Layer 2 (City Brain) and Layer 3 (Guardian).
Does NOT route over Kafka. Strictly isolated from Ground Truth SQLite database.
"""

from __future__ import annotations

import hmac
import logging
import os
import re
from typing import Any, Dict, List, Optional, Union

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

try:
    from mcp.server.mcpserver import Context, MCPServer
except ImportError:
    from mcp.server.fastmcp import Context
    from mcp.server.fastmcp import FastMCP as MCPServer

from mcp.server.transport_security import TransportSecuritySettings

from twin.db_ledger import parse_sim_time
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
# Authentication and Request Context Handling
# ============================================================================

def parse_auth_tokens() -> Dict[str, str]:
    """Parse configured MCP_AUTH_TOKENS into a dict of {token: principal_name}."""
    raw = os.getenv("MCP_AUTH_TOKENS", "").strip()
    if not raw:
        return {}
    tokens: Dict[str, str] = {}
    known_principals = {"brain", "guardian", "scenario"}
    for part in raw.split(","):
        part = part.strip()
        if ":" in part:
            left, right = part.split(":", 1)
            left_s = left.strip()
            right_s = right.strip()
            if right_s.lower() in known_principals and left_s.lower() not in known_principals:
                tokens[left_s] = right_s
            else:
                tokens[right_s] = left_s
    return tokens


def get_caller(ctx: Optional[Context] = None) -> str:
    """Determine authenticated caller identity from tool execution context."""
    tokens = parse_auth_tokens()
    if not tokens:
        return "anonymous"
    if ctx is None:
        return "anonymous"
    try:
        headers = ctx.headers or {}
        auth_hdr = None
        for k, v in headers.items():
            if k.lower() == "authorization":
                auth_hdr = v
                break
        if auth_hdr and auth_hdr.startswith("Bearer "):
            bearer = auth_hdr[7:].strip()
            for tok, principal in tokens.items():
                if hmac.compare_digest(bearer, tok):
                    return principal
    except Exception:
        pass
    return "anonymous"


class MCPAuthMiddleware:
    """ASGI Middleware enforcing bearer token authentication on MCP HTTP endpoints."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            tokens = parse_auth_tokens()
            if tokens:
                path = scope.get("path", "")
                if path.startswith(("/sse", "/messages", "/mcp")):
                    headers = dict(scope.get("headers", []))
                    auth_hdr = headers.get(b"authorization", b"").decode("utf-8", errors="ignore")
                    authenticated = False
                    if auth_hdr.startswith("Bearer "):
                        bearer = auth_hdr[7:].strip()
                        for tok in tokens:
                            if hmac.compare_digest(bearer, tok):
                                authenticated = True
                                break
                    if not authenticated:
                        res = JSONResponse(
                            {
                                "status": "error",
                                "code": "UNAUTHORIZED",
                                "message": "Missing or invalid authorization token.",
                            },
                            status_code=401,
                        )
                        await res(scope, receive, send)
                        return
        await self.app(scope, receive, send)


class CatchValidationFailedMiddleware:
    """Catches ValueError('Request validation failed') raised by connect_sse on rejected Host.

    SseServerTransport sends the 421 HTTP response before raising this exception;
    catching it prevents Starlette/uvicorn from logging an unhandled ASGI exception traceback.
    """

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await self.app(scope, receive, send)
        except ValueError as exc:
            if str(exc) == "Request validation failed":
                return
            raise


def build_mcp_app() -> Starlette:
    """Build unified Starlette ASGI application exposing /sse, /messages/, and /mcp transports."""
    tokens = parse_auth_tokens()
    if not tokens:
        logger.warning("[ALERT] MCP authentication disabled: open access mode active (MCP_AUTH_TOKENS unset)")

    raw_hosts = os.getenv("MCP_ALLOWED_HOSTS", "").strip()
    if not raw_hosts or raw_hosts == "*":
        sec = TransportSecuritySettings(enable_dns_rebinding_protection=False)
        logger.info("[NOMINAL] MCP DNS-rebinding / Host checking is disabled (open LAN access)")
    else:
        allowed: List[str] = []
        for h in raw_hosts.split(","):
            h = h.strip()
            if h:
                allowed.extend([h, f"{h}:*"])
        sec = TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=allowed,
        )
        logger.info(f"[NOMINAL] MCP DNS-rebinding / Host checking is enabled: {allowed}")

    fresh_mcp = MCPServer("TwinLayerCommandServer")
    fresh_mcp._tool_manager = mcp._tool_manager
    fresh_mcp._custom_starlette_routes = mcp._custom_starlette_routes

    sse_sub_app = fresh_mcp.sse_app(transport_security=sec)
    streamable_sub_app = fresh_mcp.streamable_http_app(transport_security=sec)

    combined = Starlette(
        routes=list(sse_sub_app.routes) + list(streamable_sub_app.routes),
        lifespan=streamable_sub_app.router.lifespan_context,
        middleware=[
            Middleware(CatchValidationFailedMiddleware),
            Middleware(MCPAuthMiddleware),
        ],
    )
    return combined


# ============================================================================
# 1. QUERIES (Layer 2 - Brain & Layer 3 - Guardian)
# ============================================================================

@mcp.tool()
def list_devices(zone_id: Optional[str] = None, ctx: Optional[Context] = None) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Return inventory and physical ranges (upper/lower bounds) of registered devices.

    Args:
        zone_id: Optional filter by zone_id (e.g., 'zone-1' to 'zone-6').
    """
    sim = get_active_simulator()
    if zone_id is not None:
        if len(zone_id) > 64:
            return {
                "status": "error",
                "code": "INVALID_ARGUMENT",
                "message": "zone_id exceeds maximum length of 64 characters.",
            }
        if zone_id not in sim.ALLOWED_ZONES:
            return {
                "status": "error",
                "code": "ZONE_NOT_FOUND",
                "message": f"Zone '{zone_id}' not found.",
                "allowed": list(sim.ALLOWED_ZONES),
            }
    devices = sim.list_devices_spec()
    if zone_id:
        devices = [d for d in devices if d["zone_id"] == zone_id]
    return devices


@mcp.tool()
def get_device_key(device_id: str, ctx: Optional[Context] = None) -> Optional[str]:
    """Return the Ed25519 public key (base64) for a given device_id.

    Used by Guardian (Layer 3) to verify cryptographic signatures of Kafka sensor readings.
    Returns null if device_id is unknown or invalid (never returns an error object, so
    guardian's falsy check 'if not pub_key' works correctly).
    """
    sim = get_active_simulator()
    return sim.get_device_public_key(device_id)


@mcp.tool()
def get_device_state(device_id: str, ctx: Optional[Context] = None) -> Dict[str, Any]:
    """Fetch current telemetry and operational state for a specific device.

    Args:
        device_id: The identifier of the device.
    """
    sim = get_active_simulator()
    return sim.get_device_state(device_id)


@mcp.tool()
def get_city_state(ctx: Optional[Context] = None) -> Dict[str, Any]:
    """Fetch high-level overview of city state across all 6 zones and active dispatches."""
    sim = get_active_simulator()
    return sim.get_city_state()


@mcp.tool()
def get_reading(reading_id: str, ctx: Optional[Context] = None) -> Dict[str, Any]:
    """Fetch past published reading using its reading_id from the broadcast cache."""
    if len(reading_id) > 64:
        return {
            "status": "error",
            "code": "INVALID_ARGUMENT",
            "message": "reading_id exceeds maximum length of 64 characters.",
        }
    sim = get_active_simulator()
    reading = sim.get_cached_reading(reading_id)
    if reading is None:
        return {
            "status": "error",
            "code": "READING_NOT_FOUND",
            "message": f"Reading {reading_id} not found in the reading cache: unknown id, or older than the last {sim.reading_cache_max} readings",
        }
    return {k: v for k, v in reading.items() if not k.startswith("_")}


@mcp.tool()
def get_readings(
    device_id: Optional[str] = None,
    since_ts: Optional[str] = None,
    limit: int = 50,
    ctx: Optional[Context] = None,
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Fetch list of recent published readings from the broadcast cache.

    Args:
        device_id: Optional filter by device ID.
        since_ts: Optional RFC 3339 UTC timestamp to fetch readings occurring at or after this time.
        limit: Maximum number of readings to return (default: 50).
    """
    if device_id is not None and len(device_id) > 64:
        return {
            "status": "error",
            "code": "INVALID_ARGUMENT",
            "message": "device_id exceeds maximum length of 64 characters.",
        }
    if not isinstance(limit, int) or limit < 1 or limit > 500:
        return {
            "status": "error",
            "code": "INVALID_ARGUMENT",
            "message": "limit must be an integer between 1 and 500.",
        }
    if since_ts is not None:
        if len(since_ts) > 64 or not since_ts.endswith("Z"):
            return {
                "status": "error",
                "code": "INVALID_TIMESTAMP",
                "message": f"Invalid timestamp format: '{since_ts}' must be RFC 3339 UTC ending with Z.",
            }
        try:
            parse_sim_time(since_ts)
        except Exception:
            return {
                "status": "error",
                "code": "INVALID_TIMESTAMP",
                "message": f"Invalid timestamp format: '{since_ts}' must be RFC 3339 UTC ending with Z.",
            }

    sim = get_active_simulator()
    matches: List[Dict[str, Any]] = []

    # Iterate newest-first directly over OrderedDict, skipping entries older than since_ts
    for r in reversed(sim.recent_readings_buffer.values()):
        if since_ts and r.get("ts", "") < since_ts:
            continue
        if device_id and r.get("device_id") != device_id:
            continue
        matches.append({k: v for k, v in r.items() if not k.startswith("_")})
        if len(matches) >= limit:
            break

    # Return in chronological order (oldest first)
    matches.reverse()
    return matches


# ============================================================================
# 2. ACTUATION (Called by Layer 2 - City Brain)
# ============================================================================

@mcp.tool()
def dispatch(
    incident_type: str,
    zone_id: str,
    idempotency_key: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> Dict[str, Any]:
    """Dispatch emergency response units to an incident in a specific zone.

    Reduces emergency call volume and resolves incidents in the target zone.
    """
    sim = get_active_simulator()
    caller = get_caller(ctx)
    return sim.handle_dispatch(
        incident_type=incident_type,
        zone_id=zone_id,
        idempotency_key=idempotency_key,
        caller=caller,
    )


@mcp.tool()
def set_signal_plan(
    device_id: str,
    target_state: str,
    idempotency_key: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> Dict[str, Any]:
    """Update traffic signal timing plan (e.g., 'green_wave', 'normal', 'red_restriction').

    Directly influences vehicle throughput and congestion in the next physics tick.
    """
    sim = get_active_simulator()
    caller = get_caller(ctx)
    return sim.handle_set_signal_plan(
        device_id=device_id,
        target_state=target_state,
        idempotency_key=idempotency_key,
        caller=caller,
    )


@mcp.tool()
def set_valve(
    device_id: str,
    target_state: str,
    idempotency_key: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> Dict[str, Any]:
    """Actuate drainage or flood-control water valve (e.g., 'open', 'closed', '50%').

    Directly changes water level discharge dynamics in the next physics tick.
    """
    sim = get_active_simulator()
    caller = get_caller(ctx)
    return sim.handle_set_valve(
        device_id=device_id,
        target_state=target_state,
        idempotency_key=idempotency_key,
        caller=caller,
    )


@mcp.tool()
def switch_grid(
    device_id: str,
    target_state: str,
    idempotency_key: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> Dict[str, Any]:
    """Switch power grid substation feeder or load shedding state (e.g., 'grid_a', 'grid_b', 'shed', 'standby').

    Directly modulates electrical power load consumption in the next physics tick.
    """
    sim = get_active_simulator()
    caller = get_caller(ctx)
    return sim.handle_switch_grid(
        device_id=device_id,
        target_state=target_state,
        idempotency_key=idempotency_key,
        caller=caller,
    )


@mcp.tool()
def get_command_result(idempotency_key: str, ctx: Optional[Context] = None) -> Dict[str, Any]:
    """Retrieve execution result of an actuation command by its idempotency key."""
    sim = get_active_simulator()
    return sim.get_command_result(idempotency_key)


# ============================================================================
# 3. CONTAINMENT (Called by Layer 3 - Guardian)
# ============================================================================

@mcp.tool()
def isolate_device(device_id: str, ctx: Optional[Context] = None) -> Dict[str, Any]:
    """Isolate a suspect device from physical actuation and mark its telemetry status as 'isolated'."""
    sim = get_active_simulator()
    caller = get_caller(ctx)
    return sim.update_device_status(
        device_id=device_id,
        new_status="isolated",
        tool_name="isolate_device",
        caller=caller,
    )


@mcp.tool()
def quarantine_device(device_id: str, ctx: Optional[Context] = None) -> Dict[str, Any]:
    """Quarantine an anomalous or compromised device, updating its status to 'quarantined' in the stream."""
    sim = get_active_simulator()
    caller = get_caller(ctx)
    return sim.update_device_status(
        device_id=device_id,
        new_status="quarantined",
        tool_name="quarantine_device",
        caller=caller,
    )


@mcp.tool()
def reinstate_device(device_id: str, ctx: Optional[Context] = None) -> Dict[str, Any]:
    """Reinstate an isolated or quarantined device back to operational status 'ok'."""
    sim = get_active_simulator()
    caller = get_caller(ctx)
    return sim.update_device_status(
        device_id=device_id,
        new_status="ok",
        tool_name="reinstate_device",
        caller=caller,
    )


@mcp.tool()
def release_device(device_id: str, ctx: Optional[Context] = None) -> Dict[str, Any]:
    """Release a device from containment back to operational status 'ok'."""
    sim = get_active_simulator()
    caller = get_caller(ctx)
    return sim.update_device_status(
        device_id=device_id,
        new_status="ok",
        tool_name="release_device",
        caller=caller,
    )


@mcp.tool()
def rollback_reading(reading_id: str, ctx: Optional[Context] = None) -> Dict[str, Any]:
    """Flag a falsified or spoofed reading in the stream history as 'rolled_back'."""
    sim = get_active_simulator()
    caller = get_caller(ctx)
    return sim.rollback_reading(reading_id=reading_id, caller=caller)


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8000"))
    app = build_mcp_app()
    print(f"Starting Twin MCP Command Server on http://{host}:{port} (/sse and /mcp)...")
    uvicorn.run(app, host=host, port=port)
