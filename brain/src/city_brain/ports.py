"""Ports the Brain depends on; adapters implement them."""
from dataclasses import dataclass
from typing import Literal, Protocol

from .edge.profiles import ToolCall


@dataclass(frozen=True)
class CommandOutcome:
    outcome: Literal["ACCEPTED", "REJECTED", "UNKNOWN"]
    reply: dict


class TwinPort(Protocol):
    async def load_registry(self) -> tuple[list[dict], dict[str, str], dict]:
        """Devices, public keys by device id, and the city state."""
        ...

    async def call(self, tool_call: ToolCall, idempotency_key: str) -> CommandOutcome: ...

    async def get_result(self, idempotency_key: str) -> dict | None: ...
