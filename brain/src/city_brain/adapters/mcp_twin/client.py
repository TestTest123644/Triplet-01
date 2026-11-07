"""A `TwinPort` over the Twin's MCP server (spec 5.2, 10.5).

The core must never see a network failure, so every way this can go wrong — a timeout, a
dropped connection, a reply that is not what the contract says — comes back as an outcome,
never an exception. `UNKNOWN` is the honest answer when the Brain cannot tell whether the
Twin acted: the coordinator then freezes the target and tells the operator rather than
resending (spec 8.8).

Calls are serialised: one MCP session, one call at a time, so a reply can never be matched
to the wrong command.
"""

import asyncio
import json
from contextlib import AsyncExitStack
from typing import Any

from mcp import ClientSession
from mcp.client.sse import sse_client

from city_brain.edge.profiles import ToolCall
from city_brain.ports import CommandOutcome

REGISTRY_TOOLS = ("list_devices", "get_device_key", "get_city_state")
RESULT_TOOL = "get_command_result"


def _content_to_json(result: Any) -> Any:
    """The payload of an MCP tool result, whichever shape the server used."""
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict):
        return structured.get("result", structured)
    for item in getattr(result, "content", ()) or ():
        text = getattr(item, "text", None)
        if text is None:
            continue
        try:
            return json.loads(text)
        except ValueError:
            return text
    return None


class McpTwinClient:
    """Holds one MCP session to the Twin, reconnecting when it has been lost."""

    def __init__(self, sse_url: str, timeout_s: float = 2.0):
        self._url = sse_url
        self._timeout = timeout_s
        self._lock = asyncio.Lock()
        self._stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None

    # ----- the connection -----

    async def connect(self) -> None:
        if self._session is not None:
            return
        stack = AsyncExitStack()
        try:
            read, write = await asyncio.wait_for(
                stack.enter_async_context(sse_client(self._url)), timeout=self._timeout)
            session = await stack.enter_async_context(ClientSession(read, write))
            await asyncio.wait_for(session.initialize(), timeout=self._timeout)
        except BaseException:
            await stack.aclose()
            raise
        self._stack, self._session = stack, session

    async def close(self) -> None:
        stack, self._stack, self._session = self._stack, None, None
        if stack is not None:
            try:
                await stack.aclose()
            except Exception:  # a connection that is already gone is closed enough
                pass

    async def _drop(self) -> None:
        """Forget a session that failed, so the next call opens a fresh one."""
        await self.close()

    async def _call_tool(self, tool: str, args: dict[str, Any]) -> Any:
        async with self._lock:
            await self.connect()
            session = self._session
            assert session is not None
            try:
                result = await asyncio.wait_for(
                    session.call_tool(tool, args), timeout=self._timeout)
            except BaseException:
                await self._drop()
                raise
        return _content_to_json(result)

    # ----- TwinPort -----

    async def load_registry(self) -> tuple[list[dict], dict[str, str], dict]:
        devices = await self._call_tool("list_devices", {})
        if isinstance(devices, dict):
            devices = devices.get("devices", [])
        if not isinstance(devices, list):
            raise ValueError(f"list_devices did not return a list: {devices!r:.80}")
        keys: dict[str, str] = {}
        for device in devices:
            device_id = device.get("device_id")
            if device_id is None:
                continue
            key = await self._call_tool("get_device_key", {"device_id": device_id})
            if isinstance(key, dict):
                key = key.get("public_key") or key.get("key")
            if isinstance(key, str):
                keys[device_id] = key
        city = await self._call_tool("get_city_state", {})
        return devices, keys, city if isinstance(city, dict) else {}

    async def call(self, tool_call: ToolCall, idempotency_key: str) -> CommandOutcome:
        args = {**tool_call.args, "idempotency_key": idempotency_key}
        try:
            reply = await self._call_tool(tool_call.tool, args)
        except (asyncio.TimeoutError, OSError, RuntimeError, ValueError, ExceptionGroup) as exc:
            # The Twin may have obeyed and the reply been lost: say so, do not guess.
            return CommandOutcome("UNKNOWN", {"error": type(exc).__name__})
        except Exception as exc:  # an MCP error type we do not know is still not the core's problem
            return CommandOutcome("UNKNOWN", {"error": type(exc).__name__})
        if isinstance(reply, dict) and reply.get("status") == "ok":
            return CommandOutcome("ACCEPTED", reply)
        if isinstance(reply, dict) and reply.get("status") == "error":
            return CommandOutcome("REJECTED", reply)
        return CommandOutcome("UNKNOWN", {"error": "UnexpectedReply", "reply": reply})

    async def get_result(self, idempotency_key: str) -> dict | None:
        """What the Twin remembers about a command, for reconciliation after a crash."""
        try:
            reply = await self._call_tool(RESULT_TOOL, {"idempotency_key": idempotency_key})
        except Exception:
            return None
        return reply if isinstance(reply, dict) else None
