"""TwinPort over an in-process twin: calls its `handle_tool` directly."""
from typing import Any, Protocol

from ..edge.profiles import ToolCall
from ..ports import CommandOutcome


class _Twin(Protocol):  # structural: src/ must not import the mocks
    def list_devices(self) -> list[dict[str, Any]]: ...
    def get_device_key(self, device_id: str) -> str | None: ...
    def get_city_state(self) -> dict[str, Any]: ...
    def handle_tool(self, tool: str, args: dict[str, Any]) -> dict[str, Any] | None: ...


class InProcTwin:
    def __init__(self, twin: _Twin):
        self._twin = twin

    async def load_registry(self) -> tuple[list[dict], dict[str, str], dict]:
        devices = self._twin.list_devices()
        keys = {d["device_id"]: self._twin.get_device_key(d["device_id"]) for d in devices}
        return devices, keys, self._twin.get_city_state()

    async def call(self, tool_call: ToolCall, idempotency_key: str) -> CommandOutcome:
        args = {**tool_call.args, "idempotency_key": idempotency_key}
        try:
            reply = self._twin.handle_tool(tool_call.tool, args)
        except Exception as exc:
            return CommandOutcome("UNKNOWN", {"error": type(exc).__name__})
        if isinstance(reply, dict) and reply.get("status") == "ok":
            return CommandOutcome("ACCEPTED", reply)
        if isinstance(reply, dict) and reply.get("status") == "error":
            return CommandOutcome("REJECTED", reply)
        return CommandOutcome("UNKNOWN", {"error": "UnexpectedReply"})

    async def get_result(self, idempotency_key: str) -> dict | None:
        return self._twin.handle_tool("get_command_result", {"idempotency_key": idempotency_key})
