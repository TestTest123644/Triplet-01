"""Red Team Adversary REST API for Twin Layer (Layer 1).

Exposes administrative attack injection and inspection endpoints on an isolated
port (default: 8100). Every route is protected with header authentication
(X-Red-Team-Token) to enforce zero-trust isolation from external layers.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import hmac
import json
import logging
import os
import secrets
import sqlite3
from typing import Any, Dict, List, Optional

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from twin.simulation import AttackConflictError, CitySimulator

logger = logging.getLogger("twin.red_team_api")


from twin.secrets import get_secret

_ACTIVE_RED_TEAM_TOKEN: Optional[str] = None


def get_active_red_team_token() -> str:
    """Get active Red Team API token, initializing lazily if needed."""
    global _ACTIVE_RED_TEAM_TOKEN
    if _ACTIVE_RED_TEAM_TOKEN is None:
        _ACTIVE_RED_TEAM_TOKEN = get_or_create_red_team_token()
    return _ACTIVE_RED_TEAM_TOKEN


def set_active_red_team_token(token: str) -> None:
    """Set active Red Team API token atomically."""
    global _ACTIVE_RED_TEAM_TOKEN
    _ACTIVE_RED_TEAM_TOKEN = token


def get_or_create_red_team_token() -> str:
    """Retrieve RED_TEAM_TOKEN via get_secret or generate/persist token to RED_TEAM_TOKEN_FILE with mode 600."""
    sec = get_secret("RED_TEAM_TOKEN")
    if sec:
        token = sec.strip()
        set_active_red_team_token(token)
        return token

    token_file = os.getenv("RED_TEAM_TOKEN_FILE")
    if not token_file:
        db_path = os.getenv("GROUND_TRUTH_DB_PATH", "ground_truth.db")
        db_dir = os.path.dirname(os.path.abspath(db_path)) or "."
        token_file = os.path.join(db_dir, "red_team.token")

    token_file = os.path.abspath(token_file)

    if os.path.isfile(token_file):
        try:
            with open(token_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    set_active_red_team_token(content)
                    return content
        except Exception as e:
            logger.warning(f"Failed to read existing token file {token_file}: {e}")

    new_token = secrets.token_urlsafe(32)
    os.makedirs(os.path.dirname(token_file), exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(token_file, flags, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(new_token)
    try:
        os.chmod(token_file, 0o600)
    except OSError:
        pass

    logger.info(f"Generated new Red Team API token and persisted to {token_file}")
    set_active_red_team_token(new_token)
    return new_token


def reload_red_team_token() -> str:
    """Re-resolve RED_TEAM_TOKEN and swap active token atomically."""
    current = get_active_red_team_token()
    new_token = get_secret("RED_TEAM_TOKEN")
    if new_token and new_token.strip() != current:
        set_active_red_team_token(new_token.strip())
        logger.info("Red Team adversary API token rotated successfully")
        return "changed"
    return "unchanged"


class RedTeamAuthMiddleware(BaseHTTPMiddleware):
    """Enforces X-Red-Team-Token header validation on every incoming request."""

    def __init__(self, app, token: Optional[str] = None):
        super().__init__(app)
        if token:
            set_active_red_team_token(token)

    async def dispatch(self, request: Request, call_next):
        header_token = request.headers.get("X-Red-Team-Token")
        expected_token = get_active_red_team_token()
        if not header_token or not hmac.compare_digest(header_token, expected_token):
            return JSONResponse(
                {
                    "status": "error",
                    "code": "UNAUTHORIZED",
                    "message": "Missing or invalid X-Red-Team-Token header",
                },
                status_code=401,
            )
        return await call_next(request)


def _validate_rfc3339(ts_str: str) -> bool:
    """Validate that a string conforms to RFC 3339 format."""
    try:
        s = ts_str.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        datetime.fromisoformat(s)
        return True
    except Exception:
        return False


def _query_journal_sync(db_path: str, since_sim_ts: Optional[str], limit: int) -> List[Dict[str, Any]]:
    """Synchronous read-only SQL query helper for journal API, executed in thread pool."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        entries: List[Dict[str, Any]] = []

        if since_sim_ts:
            # Query forward from since_sim_ts
            # 1. commands_log
            cur = conn.execute(
                "SELECT id AS rowid, sim_ts, caller, tool, args_json, executed, refused_code "
                "FROM commands_log WHERE sim_ts >= ? ORDER BY sim_ts ASC LIMIT ?",
                (since_sim_ts, limit),
            )
            for r in cur.fetchall():
                try:
                    args = json.loads(r["args_json"]) if r["args_json"] else {}
                except Exception:
                    args = {}
                entries.append({
                    "key": f"command:{r['rowid']}",
                    "sim_ts": r["sim_ts"],
                    "source": "command",
                    "detail": {
                        "caller": r["caller"],
                        "tool": r["tool"],
                        "args": args,
                        "executed": bool(r["executed"]),
                        "refused_code": r["refused_code"],
                    },
                })

            # 2. containment_log
            cur = conn.execute(
                "SELECT id AS rowid, sim_ts, caller, tool, target, from_status, to_status "
                "FROM containment_log WHERE sim_ts >= ? ORDER BY sim_ts ASC LIMIT ?",
                (since_sim_ts, limit),
            )
            for r in cur.fetchall():
                entries.append({
                    "key": f"containment:{r['rowid']}",
                    "sim_ts": r["sim_ts"],
                    "source": "containment",
                    "detail": {
                        "caller": r["caller"],
                        "tool": r["tool"],
                        "target": r["target"],
                        "from_status": r["from_status"],
                        "to_status": r["to_status"],
                    },
                })

            # 3. physical_events
            cur = conn.execute(
                "SELECT event_id, type, zone_id, device_id, params_json, start_sim_ts, resolved_sim_ts, resolved_by, escalated_sim_ts, escalated_to "
                "FROM physical_events WHERE start_sim_ts >= ? OR (resolved_sim_ts IS NOT NULL AND resolved_sim_ts >= ?) OR (escalated_sim_ts IS NOT NULL AND escalated_sim_ts >= ?) "
                "ORDER BY start_sim_ts ASC LIMIT ?",
                (since_sim_ts, since_sim_ts, since_sim_ts, limit),
            )
            for r in cur.fetchall():
                try:
                    params = json.loads(r["params_json"]) if r["params_json"] else {}
                except Exception:
                    params = {}
                if r["start_sim_ts"] >= since_sim_ts:
                    entries.append({
                        "key": f"event_start:{r['event_id']}",
                        "sim_ts": r["start_sim_ts"],
                        "source": "event",
                        "detail": {
                            "event_id": r["event_id"],
                            "action": "start",
                            "type": r["type"],
                            "zone_id": r["zone_id"],
                            "device_id": r["device_id"],
                            "params": params,
                        },
                    })
                if r["escalated_sim_ts"] and r["escalated_sim_ts"] >= since_sim_ts:
                    entries.append({
                        "key": f"event_escalated:{r['event_id']}",
                        "sim_ts": r["escalated_sim_ts"],
                        "source": "event",
                        "detail": {
                            "event_id": r["event_id"],
                            "action": "escalated",
                            "type": r["type"],
                            "to": r["escalated_to"] or "blackout",
                            "zone_id": r["zone_id"],
                        },
                    })
                if r["resolved_sim_ts"] and r["resolved_sim_ts"] >= since_sim_ts:
                    entries.append({
                        "key": f"event_resolved:{r['event_id']}",
                        "sim_ts": r["resolved_sim_ts"],
                        "source": "event",
                        "detail": {
                            "event_id": r["event_id"],
                            "action": "resolved",
                            "type": r["type"],
                            "zone_id": r["zone_id"],
                            "resolved_by": r["resolved_by"],
                        },
                    })

            # 4. attacks_injected
            cur = conn.execute(
                "SELECT attack_id, capability, target_device_id, target_zone_id, params_json, start_sim_time, end_sim_time, description "
                "FROM attacks_injected WHERE start_sim_time >= ? OR (end_sim_time IS NOT NULL AND end_sim_time >= ?) "
                "ORDER BY start_sim_time ASC LIMIT ?",
                (since_sim_ts, since_sim_ts, limit),
            )
            for r in cur.fetchall():
                try:
                    params = json.loads(r["params_json"]) if r["params_json"] else {}
                except Exception:
                    params = {}
                if r["start_sim_time"] >= since_sim_ts:
                    entries.append({
                        "key": f"attack_inject:{r['attack_id']}",
                        "sim_ts": r["start_sim_time"],
                        "source": "attack",
                        "detail": {
                            "attack_id": r["attack_id"],
                            "action": "inject",
                            "capability": r["capability"],
                            "target_device_id": r["target_device_id"],
                            "target_zone_id": r["target_zone_id"],
                            "params": params,
                            "description": r["description"],
                        },
                    })
                if r["end_sim_time"] and r["end_sim_time"] >= since_sim_ts:
                    entries.append({
                        "key": f"attack_clear:{r['attack_id']}",
                        "sim_ts": r["end_sim_time"],
                        "source": "attack",
                        "detail": {
                            "attack_id": r["attack_id"],
                            "action": "clear",
                            "capability": r["capability"],
                            "target_device_id": r["target_device_id"],
                            "target_zone_id": r["target_zone_id"],
                        },
                    })

            # 5. secret_rotations
            try:
                cur = conn.execute(
                    "SELECT id AS rowid, sim_ts, results_json "
                    "FROM secret_rotations WHERE sim_ts >= ? ORDER BY sim_ts ASC LIMIT ?",
                    (since_sim_ts, limit),
                )
                for r in cur.fetchall():
                    try:
                        res = json.loads(r["results_json"]) if r["results_json"] else {}
                    except Exception:
                        res = {}
                    entries.append({
                        "key": f"secrets:{r['rowid']}",
                        "sim_ts": r["sim_ts"],
                        "source": "secrets",
                        "detail": {
                            "results": res,
                        },
                    })
            except sqlite3.OperationalError:
                pass

            entries.sort(key=lambda x: x["sim_ts"])
            return entries[:limit]

        else:
            # Without since_sim_ts: return LATEST limit entries (sorted ascending)
            # 1. commands_log
            cur = conn.execute(
                "SELECT id AS rowid, sim_ts, caller, tool, args_json, executed, refused_code "
                "FROM commands_log ORDER BY sim_ts DESC LIMIT ?",
                (limit,),
            )
            for r in cur.fetchall():
                try:
                    args = json.loads(r["args_json"]) if r["args_json"] else {}
                except Exception:
                    args = {}
                entries.append({
                    "key": f"command:{r['rowid']}",
                    "sim_ts": r["sim_ts"],
                    "source": "command",
                    "detail": {
                        "caller": r["caller"],
                        "tool": r["tool"],
                        "args": args,
                        "executed": bool(r["executed"]),
                        "refused_code": r["refused_code"],
                    },
                })

            # 2. containment_log
            cur = conn.execute(
                "SELECT id AS rowid, sim_ts, caller, tool, target, from_status, to_status "
                "FROM containment_log ORDER BY sim_ts DESC LIMIT ?",
                (limit,),
            )
            for r in cur.fetchall():
                entries.append({
                    "key": f"containment:{r['rowid']}",
                    "sim_ts": r["sim_ts"],
                    "source": "containment",
                    "detail": {
                        "caller": r["caller"],
                        "tool": r["tool"],
                        "target": r["target"],
                        "from_status": r["from_status"],
                        "to_status": r["to_status"],
                    },
                })

            # 3. physical_events
            cur = conn.execute(
                "SELECT event_id, type, zone_id, device_id, params_json, start_sim_ts, resolved_sim_ts, resolved_by, escalated_sim_ts, escalated_to "
                "FROM physical_events ORDER BY start_sim_ts DESC LIMIT ?",
                (limit,),
            )
            for r in cur.fetchall():
                try:
                    params = json.loads(r["params_json"]) if r["params_json"] else {}
                except Exception:
                    params = {}
                entries.append({
                    "key": f"event_start:{r['event_id']}",
                    "sim_ts": r["start_sim_ts"],
                    "source": "event",
                    "detail": {
                        "event_id": r["event_id"],
                        "action": "start",
                        "type": r["type"],
                        "zone_id": r["zone_id"],
                        "device_id": r["device_id"],
                        "params": params,
                    },
                })
                if r["escalated_sim_ts"]:
                    entries.append({
                        "key": f"event_escalated:{r['event_id']}",
                        "sim_ts": r["escalated_sim_ts"],
                        "source": "event",
                        "detail": {
                            "event_id": r["event_id"],
                            "action": "escalated",
                            "type": r["type"],
                            "to": r["escalated_to"] or "blackout",
                            "zone_id": r["zone_id"],
                        },
                    })
                if r["resolved_sim_ts"]:
                    entries.append({
                        "key": f"event_resolved:{r['event_id']}",
                        "sim_ts": r["resolved_sim_ts"],
                        "source": "event",
                        "detail": {
                            "event_id": r["event_id"],
                            "action": "resolved",
                            "type": r["type"],
                            "zone_id": r["zone_id"],
                            "resolved_by": r["resolved_by"],
                        },
                    })

            # 4. attacks_injected
            cur = conn.execute(
                "SELECT attack_id, capability, target_device_id, target_zone_id, params_json, start_sim_time, end_sim_time, description "
                "FROM attacks_injected ORDER BY start_sim_time DESC LIMIT ?",
                (limit,),
            )
            for r in cur.fetchall():
                try:
                    params = json.loads(r["params_json"]) if r["params_json"] else {}
                except Exception:
                    params = {}
                entries.append({
                    "key": f"attack_inject:{r['attack_id']}",
                    "sim_ts": r["start_sim_time"],
                    "source": "attack",
                    "detail": {
                        "attack_id": r["attack_id"],
                        "action": "inject",
                        "capability": r["capability"],
                        "target_device_id": r["target_device_id"],
                        "target_zone_id": r["target_zone_id"],
                        "params": params,
                        "description": r["description"],
                    },
                })
                if r["end_sim_time"]:
                    entries.append({
                        "key": f"attack_clear:{r['attack_id']}",
                        "sim_ts": r["end_sim_time"],
                        "source": "attack",
                        "detail": {
                            "attack_id": r["attack_id"],
                            "action": "clear",
                            "capability": r["capability"],
                            "target_device_id": r["target_device_id"],
                            "target_zone_id": r["target_zone_id"],
                        },
                    })

            # 5. secret_rotations
            try:
                cur = conn.execute(
                    "SELECT id AS rowid, sim_ts, results_json "
                    "FROM secret_rotations ORDER BY sim_ts DESC LIMIT ?",
                    (limit,),
                )
                for r in cur.fetchall():
                    try:
                        res = json.loads(r["results_json"]) if r["results_json"] else {}
                    except Exception:
                        res = {}
                    entries.append({
                        "key": f"secrets:{r['rowid']}",
                        "sim_ts": r["sim_ts"],
                        "source": "secrets",
                        "detail": {
                            "results": res,
                        },
                    })
            except sqlite3.OperationalError:
                pass

            # Sort descending to find the top latest limit entries
            entries.sort(key=lambda x: x["sim_ts"], reverse=True)
            latest_slice = entries[:limit]
            # Return them sorted ascending
            latest_slice.sort(key=lambda x: x["sim_ts"])
            return latest_slice
    finally:
        conn.close()


def build_red_team_app(sim: CitySimulator, token: str, streamer: Optional[Any] = None) -> Starlette:
    """Build Starlette ASGI application hosting Red Team control endpoints."""
    if streamer is not None:
        sim.streamer = streamer

    async def api_status(request: Request) -> JSONResponse:
        delayed_pending = (
            streamer.get_delayed_frames_pending()
            if streamer is not None and hasattr(streamer, "get_delayed_frames_pending")
            else 0
        )
        data = {
            "status": "ok",
            "sim_time": sim.get_sim_time(),
            "sim_speed": getattr(sim.clock, "sim_speed", 1.0),
            "tick_count": sim.clock.tick_count,
            "total_devices": len(sim.devices),
            "active_attacks_count": len(sim.active_attacks),
            "active_events_count": len(sim.active_events),
            "blocked_frames": dict(getattr(sim, "blocked_rogue_frames", {})),
            "delayed_frames_pending": delayed_pending,
        }
        if streamer is not None:
            stream_stats = streamer.stats()
            stream_stats["msgs_per_sec"] = streamer.get_msgs_per_sec()
            data["stream"] = stream_stats
        else:
            data["stream"] = {
                "produced": 0,
                "delivered": 0,
                "delivery_failed": 0,
                "sunk": 0,
                "dropped": 0,
                "rejected_oversize": 0,
                "rejected_invalid": 0,
                "msgs_per_sec": 0.0,
            }
        return JSONResponse(data)

    async def api_inject_attack(request: Request) -> JSONResponse:
        try:
            body = await request.json()
            capability = body.get("capability")
            target_device_id = body.get("target_device_id")
            target_zone_id = body.get("target_zone_id")
            params = body.get("params", {})
            description = body.get("description")

            if not capability:
                return JSONResponse(
                    {"status": "error", "message": "Missing capability field"},
                    status_code=400,
                )

            if not isinstance(params, dict):
                params = {}
            else:
                params = dict(params)

            # Accept params.value as an alias of spoofed_value (spoofed_value wins if both)
            val = params.get("spoofed_value")
            if val is None:
                val = params.get("value")
            if val is not None:
                try:
                    num_val = float(val)
                    params["spoofed_value"] = num_val
                except (ValueError, TypeError):
                    return JSONResponse(
                        {
                            "status": "error",
                            "code": "INVALID_ARGUMENT",
                            "message": f"Value '{val}' must be numeric.",
                        },
                        status_code=400,
                    )

            attack_id = sim.inject_attack(
                capability=capability,
                target_device_id=target_device_id,
                target_zone_id=target_zone_id,
                params=params,
                description=description,
            )
            return JSONResponse({
                "status": "ok",
                "attack_id": attack_id,
                "capability": capability,
                "target_device_id": target_device_id,
                "sim_time": sim.get_sim_time(),
            })
        except AttackConflictError as e:
            return JSONResponse(
                {
                    "status": "error",
                    "code": "ATTACK_CONFLICT",
                    "message": str(e),
                },
                status_code=409,
            )
        except ValueError as e:
            return JSONResponse(
                {
                    "status": "error",
                    "code": "INVALID_ATTACK",
                    "message": str(e),
                },
                status_code=400,
            )
        except Exception as e:
            logger.error(f"Error injecting attack via API: {e}")
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

    async def api_clear_attack(request: Request) -> JSONResponse:
        try:
            body = await request.json() if request.headers.get("content-length") not in (None, "0") else {}
            attack_id = body.get("attack_id")
            if attack_id and attack_id != "all":
                cleared = sim.clear_attack(attack_id)
                return JSONResponse({"status": "ok", "cleared": cleared, "attack_id": attack_id})
            else:
                sim.clear_all_attacks()
                return JSONResponse({"status": "ok", "cleared_all": True})
        except Exception as e:
            logger.error(f"Error clearing attacks via API: {e}")
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

    async def api_active_attacks(request: Request) -> JSONResponse:
        active = []
        blocked_frames_map = getattr(sim, "blocked_rogue_frames", {})
        for atk in sim.active_attacks.values():
            atk_copy = dict(atk)
            if atk.get("capability") == "rogue":
                r_id = atk.get("params", {}).get("rogue_device_id", "rg-01")
                atk_copy["blocked_frames"] = blocked_frames_map.get(r_id, 0)
            active.append(atk_copy)
        return JSONResponse({
            "status": "ok",
            "active_attacks": active,
            "blocked_frames": dict(blocked_frames_map),
        })

    async def api_inject_event(request: Request) -> JSONResponse:
        try:
            body = await request.json()
            if not isinstance(body, dict):
                return JSONResponse(
                    {
                        "status": "error",
                        "code": "INVALID_EVENT",
                        "message": "Request body must be a JSON object",
                    },
                    status_code=400,
                )

            event_type = body.get("type")
            zone_id = body.get("zone_id")
            device_id = body.get("device_id")
            intensity = body.get("intensity")
            duration_s = body.get("duration_s")
            params = body.get("params")

            if not event_type or not zone_id:
                return JSONResponse(
                    {
                        "status": "error",
                        "code": "INVALID_EVENT",
                        "message": "Missing required fields: 'type' and 'zone_id'",
                    },
                    status_code=400,
                )

            if params is not None and not isinstance(params, dict):
                return JSONResponse(
                    {
                        "status": "error",
                        "code": "INVALID_EVENT",
                        "message": "Field 'params' must be a JSON object",
                    },
                    status_code=400,
                )

            event_id = sim.inject_event(
                type=event_type,
                zone_id=zone_id,
                device_id=device_id,
                intensity=intensity,
                duration_s=duration_s,
                params=params,
            )
            return JSONResponse({
                "status": "ok",
                "event_id": event_id,
                "type": event_type,
                "zone_id": zone_id,
                "sim_time": sim.get_sim_time(),
            })
        except ValueError as e:
            return JSONResponse(
                {
                    "status": "error",
                    "code": "INVALID_EVENT",
                    "message": str(e),
                },
                status_code=400,
            )
        except Exception as e:
            logger.error(f"Error injecting physical event via API: {e}")
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

    async def api_clear_event(request: Request) -> JSONResponse:
        try:
            body = await request.json() if request.headers.get("content-length") not in (None, "0") else {}
            if not isinstance(body, dict) or "event_id" not in body or not body.get("event_id"):
                return JSONResponse(
                    {
                        "status": "error",
                        "code": "INVALID_EVENT",
                        "message": "Missing required field: 'event_id'",
                    },
                    status_code=400,
                )

            event_id = body["event_id"]
            if event_id == "all":
                count = sim.clear_all_events()
                return JSONResponse({"status": "ok", "cleared_all": True, "count": count})
            else:
                cleared = sim.clear_event(event_id)
                if not cleared:
                    return JSONResponse(
                        {
                            "status": "error",
                            "code": "EVENT_NOT_FOUND",
                            "message": f"Event '{event_id}' not found",
                        },
                        status_code=404,
                    )
                return JSONResponse({"status": "ok", "cleared": True, "event_id": event_id})
        except Exception as e:
            logger.error(f"Error clearing physical events via API: {e}")
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

    async def api_active_events(request: Request) -> JSONResponse:
        return JSONResponse({
            "status": "ok",
            "active_events": sim.get_active_events(),
        })

    async def api_journal(request: Request) -> JSONResponse:
        since_sim_ts = request.query_params.get("since_sim_ts")
        if since_sim_ts:
            if not _validate_rfc3339(since_sim_ts):
                return JSONResponse(
                    {
                        "status": "error",
                        "code": "INVALID_ARGUMENT",
                        "message": f"Invalid RFC 3339 timestamp format for 'since_sim_ts': {since_sim_ts}",
                    },
                    status_code=400,
                )

        raw_limit = request.query_params.get("limit", "100")
        try:
            limit = min(1000, max(1, int(raw_limit)))
        except ValueError:
            limit = 100

        # Read directly using an isolated read-only connection in a thread pool
        db_path = os.path.abspath(sim.ledger.db_path)
        if not os.path.exists(db_path):
            return JSONResponse({"status": "ok", "count": 0, "journal": []})

        truncated_entries = await asyncio.to_thread(_query_journal_sync, db_path, since_sim_ts, limit)

        return JSONResponse({
            "status": "ok",
            "count": len(truncated_entries),
            "journal": truncated_entries,
        })

    async def api_secrets_reload(request: Request) -> JSONResponse:
        from twin.secrets import reload_secrets

        result = reload_secrets(sim=sim, streamer=streamer)
        return JSONResponse(result)

    app = Starlette(
        middleware=[Middleware(RedTeamAuthMiddleware, token=token)],
    )
    app.add_route("/api/status", api_status, methods=["GET"])
    app.add_route("/api/attack/inject", api_inject_attack, methods=["POST"])
    app.add_route("/api/attack/clear", api_clear_attack, methods=["POST"])
    app.add_route("/api/attack/active", api_active_attacks, methods=["GET"])
    app.add_route("/api/event/inject", api_inject_event, methods=["POST"])
    app.add_route("/api/event/clear", api_clear_event, methods=["POST"])
    app.add_route("/api/event/active", api_active_events, methods=["GET"])
    app.add_route("/api/journal", api_journal, methods=["GET"])
    app.add_route("/api/secrets/reload", api_secrets_reload, methods=["POST"])

    return app

