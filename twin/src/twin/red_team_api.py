"""Red Team Adversary REST API for Twin Layer (Layer 1).

Exposes administrative attack injection and inspection endpoints on an isolated
port (default: 8100). Every route is protected with header authentication
(X-Red-Team-Token) to enforce zero-trust isolation from external layers.
"""

from __future__ import annotations

import hmac
import logging
import os
import secrets
from typing import Any, Optional

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from twin.simulation import CitySimulator

logger = logging.getLogger("twin.red_team_api")


def get_or_create_red_team_token() -> str:
    """Retrieve RED_TEAM_TOKEN from env or generate/persist token to RED_TEAM_TOKEN_FILE with mode 600."""
    env_token = os.getenv("RED_TEAM_TOKEN")
    if env_token:
        return env_token.strip()

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
    return new_token


class RedTeamAuthMiddleware(BaseHTTPMiddleware):
    """Enforces X-Red-Team-Token header validation on every incoming request."""

    def __init__(self, app, token: str):
        super().__init__(app)
        self.token = token

    async def dispatch(self, request: Request, call_next):
        header_token = request.headers.get("X-Red-Team-Token")
        if not header_token or not hmac.compare_digest(header_token, self.token):
            return JSONResponse(
                {
                    "status": "error",
                    "code": "UNAUTHORIZED",
                    "message": "Missing or invalid X-Red-Team-Token header",
                },
                status_code=401,
            )
        return await call_next(request)


def build_red_team_app(sim: CitySimulator, token: str, streamer: Optional[Any] = None) -> Starlette:
    """Build Starlette ASGI application hosting Red Team control endpoints."""

    async def api_status(request: Request) -> JSONResponse:
        data = {
            "status": "ok",
            "sim_time": sim.get_sim_time(),
            "tick_count": sim.clock.tick_count,
            "total_devices": len(sim.devices),
            "active_attacks_count": len(sim.active_attacks),
            "active_events_count": len(sim.active_events),
            "blocked_frames": dict(getattr(sim, "blocked_rogue_frames", {})),
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
            event_type = body.get("type")
            zone_id = body.get("zone_id")
            device_id = body.get("device_id")
            intensity = body.get("intensity")
            duration_s = body.get("duration_s")
            params = body.get("params", {})

            if not event_type or not zone_id:
                return JSONResponse(
                    {
                        "status": "error",
                        "code": "INVALID_EVENT",
                        "message": "Missing required fields: 'type' and 'zone_id'",
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
            event_id = body.get("event_id")
            if event_id and event_id != "all":
                cleared = sim.clear_event(event_id)
                return JSONResponse({"status": "ok", "cleared": cleared, "event_id": event_id})
            else:
                sim.clear_all_events()
                return JSONResponse({"status": "ok", "cleared_all": True})
        except Exception as e:
            logger.error(f"Error clearing physical events via API: {e}")
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

    async def api_active_events(request: Request) -> JSONResponse:
        return JSONResponse({
            "status": "ok",
            "active_events": sim.get_active_events(),
        })

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

    return app

