"""Runs an Action against the twin's MCP server through the shared McpToolClient.

Adds what #116 asks on top of the client: retries with exponential backoff while
MCP is unreachable, dry run, and the state check that keeps reinstate idempotent.
The twin's containment tools only set a status, so repeating isolate, quarantine
or rollback after a redelivery returns "ok" again and needs no check.
"""

import threading
from typing import Any, Protocol

from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    stop_when_event_set,
    wait_exponential,
)

from guardian_layer.consumer_producer.action_taker.exceptions import McpUnavailable
from guardian_layer.consumer_producer.action_taker.schema import (
    Action,
    ActionOutcome,
    OutcomeStatus,
    ToolName,
)
from guardian_layer.mcp_client.client import McpError
from guardian_layer.utils.logger import get_logger

logger = get_logger(__name__)

CONTAINED = frozenset({"isolated", "quarantined"})


class ToolClient(Protocol):
    """What ActionExecutor needs from McpToolClient (lets tests pass a fake)."""

    def call_tool(self, name: str, args: dict[str, Any]) -> Any: ...


class ActionExecutor:
    def __init__(
        self,
        client: ToolClient,
        retry_attempts: int,
        backoff_seconds: float,
        dry_run: bool,
        stop_event: threading.Event,
    ):
        self._client = client
        self._dry_run = dry_run
        self._stop_event = stop_event
        # McpError = unreachable or timed out: worth retrying. A twin "error" reply
        # is an answer, not an exception, so it is never retried. The stop event
        # (SIGTERM) ends the retries so shutdown stays within the grace period.
        self._retrying = Retrying(
            stop=stop_after_attempt(retry_attempts) | stop_when_event_set(stop_event),
            wait=wait_exponential(multiplier=backoff_seconds),
            retry=retry_if_exception_type(McpError),
            # the backoff sleeps on the stop event, so SIGTERM also cuts a sleep short
            sleep=self._sleep,
            reraise=True,
        )

    def execute(self, action: Action) -> ActionOutcome:
        """Run one action; raises McpUnavailable when MCP stays unreachable."""
        if self._dry_run:
            return ActionOutcome(
                OutcomeStatus.DRY_RUN, f"would call {action.tool} on {action.target}"
            )
        if action.tool is ToolName.REINSTATE_DEVICE:
            state = self._call(ToolName.GET_DEVICE_STATE, action.arguments)
            status = state.get("status") if isinstance(state, dict) else None
            if status not in CONTAINED:
                return ActionOutcome(
                    OutcomeStatus.SKIPPED, f"device status is {status!r}, not contained"
                )
        return self._outcome(self._call(action.tool, action.arguments))

    def _sleep(self, seconds: float) -> None:
        self._stop_event.wait(seconds)

    def _call(self, tool: ToolName, arguments: dict[str, str]) -> Any:
        try:
            reply = self._retrying(self._client.call_tool, tool.value, arguments)
        except McpError as error:
            raise McpUnavailable(f"{tool} unreachable: {error}") from error
        # fastmcp hands back the twin's dict wrapped in a generated
        # "<tool>Output(result=...)" object (checked against the live server)
        return reply if isinstance(reply, dict) else getattr(reply, "result", reply)

    def _outcome(self, reply: Any) -> ActionOutcome:
        if isinstance(reply, dict) and reply.get("status") == "ok":
            return ActionOutcome(OutcomeStatus.DONE, str(reply))
        if isinstance(reply, dict) and reply.get("status") == "error":
            return ActionOutcome(OutcomeStatus.REFUSED, str(reply.get("message")))
        return ActionOutcome(OutcomeStatus.REFUSED, f"unexpected reply {reply!r}")
