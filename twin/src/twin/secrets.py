"""Unified secret management and runtime rotation engine for the Twin Layer.

Supports three resolution tiers in strict priority order:
1. Optional Python hook function via TWIN_SECRET_HOOK="package.module:function"
2. Secret file via <NAME>_FILE (Docker / Kubernetes / Vault Agent secrets)
3. Environment variable via <NAME>

Handles zero-downtime runtime reload across:
- MCP bearer authentication tokens
- Red Team adversary API token
- Kafka SASL password (producer hot-swap)
"""

from __future__ import annotations

import datetime
import importlib
import logging
import os
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("twin.secrets")

SECRET_NAMES = ("KAFKA_PASSWORD", "MCP_AUTH_TOKENS", "RED_TEAM_TOKEN")

_LAST_SECRETS: Dict[str, Optional[str]] = {
    "KAFKA_PASSWORD": None,
    "MCP_AUTH_TOKENS": None,
    "RED_TEAM_TOKEN": None,
}


def _invoke_hook(hook_spec: str, name: str) -> Optional[str]:
    """Dynamically import and invoke partner secret hook function."""
    if ":" not in hook_spec:
        raise ValueError(
            f"Invalid TWIN_SECRET_HOOK format: '{hook_spec}'. Expected 'package.module:function'"
        )
    mod_name, func_name = hook_spec.split(":", 1)
    mod = importlib.import_module(mod_name)
    func = getattr(mod, func_name)
    return func(name)


def get_secret(name: str) -> Optional[str]:
    """Resolve secret value by name across hook, file, and environment tiers.

    Resolution order:
    1. Hook: TWIN_SECRET_HOOK="package.module:function" -> function(name) -> str | None
    2. File: <NAME>_FILE (trailing newline stripped)
    3. Env: <NAME>

    Never logs secret values.
    """
    # Tier 1: Optional hook
    hook_spec = os.getenv("TWIN_SECRET_HOOK", "").strip()
    if hook_spec:
        try:
            val = _invoke_hook(hook_spec, name)
            if val is not None and isinstance(val, str) and len(val) > 0:
                logger.debug(f"Secret '{name}' resolved from hook '{hook_spec}'")
                return val
        except Exception as e:
            logger.warning(
                f"Error invoking TWIN_SECRET_HOOK for secret '{name}': {type(e).__name__}: {e}"
            )

    # Tier 2: File <NAME>_FILE
    file_path = os.getenv(f"{name}_FILE", "").strip()
    if file_path:
        if not os.path.exists(file_path):
            if name == "RED_TEAM_TOKEN":
                logger.debug(f"{name}_FILE '{file_path}' does not exist yet")
            else:
                raise ValueError(
                    f"Password file specified by {name}_FILE does not exist: '{file_path}'"
                )
        else:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                if content.endswith("\r\n"):
                    content = content[:-2]
                elif content.endswith("\n"):
                    content = content[:-1]
            except Exception as e:
                raise ValueError(
                    f"Unreadable secret file specified by {name}_FILE '{file_path}': {e}"
                )
            if not content:
                raise ValueError(
                    f"Password file specified by {name}_FILE is empty: '{file_path}'"
                )
            logger.debug(f"Secret '{name}' resolved from file '{file_path}'")
            return content

    # Tier 3: Environment variable <NAME>
    if name in os.environ:
        val = os.environ[name]
        if val != "":
            logger.debug(f"Secret '{name}' resolved from environment variable '{name}'")
            return val

    return None


def reload_secrets(sim: Optional[Any] = None, streamer: Optional[Any] = None) -> Dict[str, Any]:
    """Re-resolve all runtime secrets and apply zero-downtime hot-swap.

    Returns:
        Dictionary with status, per-secret results ('changed', 'unchanged', or 'error: <reason>'),
        and simulation/wall timestamps.
    """
    from twin.mcp_server import reload_auth_tokens
    from twin.red_team_api import reload_red_team_token

    results: Dict[str, str] = {}

    # 1. MCP Tokens reload
    try:
        results["MCP_AUTH_TOKENS"] = reload_auth_tokens()
    except Exception as e:
        results["MCP_AUTH_TOKENS"] = f"error: {e}"

    # 2. Red Team Token reload
    try:
        results["RED_TEAM_TOKEN"] = reload_red_team_token()
    except Exception as e:
        results["RED_TEAM_TOKEN"] = f"error: {e}"

    # 3. Kafka Password reload
    if streamer is not None and hasattr(streamer, "reload_kafka_password"):
        try:
            results["KAFKA_PASSWORD"] = streamer.reload_kafka_password()
        except Exception as e:
            results["KAFKA_PASSWORD"] = f"error: {e}"
    else:
        new_pw = get_secret("KAFKA_PASSWORD")
        old_pw = _LAST_SECRETS["KAFKA_PASSWORD"]
        if new_pw != old_pw:
            _LAST_SECRETS["KAFKA_PASSWORD"] = new_pw
            results["KAFKA_PASSWORD"] = "changed"
        else:
            results["KAFKA_PASSWORD"] = "unchanged"

    # 4. Vault Keys reload
    try:
        from twin.vault_keys import reload_vault_keys
        results["VAULT_KEYS"] = reload_vault_keys()
    except Exception as e:
        results["VAULT_KEYS"] = f"error: {e}"

    now_wall = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    now_sim = sim.get_sim_time() if sim is not None else now_wall

    if sim is not None and hasattr(sim, "ledger") and sim.ledger is not None:
        try:
            sim.ledger.log_secret_rotation(sim_ts=now_sim, results=results, wall_ts=now_wall)
        except Exception as e:
            logger.warning(f"Failed to record secret rotation to ledger: {e}")

    logger.info(f"Secrets reloaded: {results}")

    return {
        "status": "ok",
        "results": results,
        "reloaded_at_sim": now_sim,
        "reloaded_at_wall": now_wall,
    }
