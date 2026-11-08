import threading
import time
from dataclasses import dataclass
from typing import Any

import pytest

from guardian_layer.consumer_producer.action_taker.exceptions import McpUnavailable
from guardian_layer.consumer_producer.action_taker.mcp_client import ActionExecutor
from guardian_layer.consumer_producer.action_taker.schema import (
    Action,
    OutcomeStatus,
    ToolName,
)
from guardian_layer.mcp_client.client import McpError

DEVICE = "z1-tc1"
OK = {"status": "ok", "device_id": DEVICE}


class FakeClient:
    """Answers each call with the next scripted reply; an exception is raised."""

    def __init__(self, *replies: Any):
        self.replies = list(replies)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def call_tool(self, name: str, args: dict[str, Any]) -> Any:
        self.calls.append((name, args))
        reply = self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]
        if isinstance(reply, Exception):
            raise reply
        return reply


def executor(
    client: FakeClient,
    dry_run: bool = False,
    retry_attempts: int = 3,
    stop_event: threading.Event | None = None,
) -> ActionExecutor:
    return ActionExecutor(
        client,
        retry_attempts=retry_attempts,
        backoff_seconds=0,
        dry_run=dry_run,
        stop_event=stop_event or threading.Event(),
    )


@dataclass
class WrappedReply:
    """What fastmcp's ``result.data`` really is for the twin's tools: the dict is
    wrapped in a generated ``<tool>Output(result=...)`` object."""

    result: Any


ISOLATE = Action(ToolName.ISOLATE_DEVICE, DEVICE)
REINSTATE = Action(ToolName.REINSTATE_DEVICE, DEVICE)


def test_ok_reply_is_done():
    client = FakeClient(OK)
    outcome = executor(client).execute(ISOLATE)
    assert outcome.status is OutcomeStatus.DONE
    assert client.calls == [("isolate_device", {"device_id": DEVICE})]


def test_wrapped_ok_reply_is_done():
    client = FakeClient(WrappedReply(OK))
    assert executor(client).execute(ISOLATE).status is OutcomeStatus.DONE


def test_wrapped_error_reply_is_refused_with_message():
    reply = WrappedReply({"status": "error", "message": "Device rogue-01 not found"})
    outcome = executor(FakeClient(reply)).execute(ISOLATE)
    assert outcome.status is OutcomeStatus.REFUSED
    assert outcome.detail == "Device rogue-01 not found"


def test_reinstate_reads_wrapped_device_state():
    client = FakeClient(
        WrappedReply({"device_id": DEVICE, "status": "quarantined"}), WrappedReply(OK)
    )
    assert executor(client).execute(REINSTATE).status is OutcomeStatus.DONE
    assert len(client.calls) == 2


def test_reinstate_skips_wrapped_unknown_device():
    client = FakeClient(WrappedReply(None))
    assert executor(client).execute(REINSTATE).status is OutcomeStatus.SKIPPED


def test_rollback_passes_reading_id():
    client = FakeClient({"status": "ok", "reading_id": "r-1"})
    executor(client).execute(Action(ToolName.ROLLBACK_READING, "r-1"))
    assert client.calls == [("rollback_reading", {"reading_id": "r-1"})]


def test_error_reply_is_refused_and_not_retried():
    client = FakeClient({"status": "error", "message": "Device z1-tc1 not found"})
    outcome = executor(client).execute(ISOLATE)
    assert outcome.status is OutcomeStatus.REFUSED
    assert "Device z1-tc1 not found" in outcome.detail
    assert len(client.calls) == 1


@pytest.mark.parametrize("reply", ["weird", None])
def test_non_dict_reply_is_refused(reply):
    client = FakeClient(reply)
    outcome = executor(client).execute(ISOLATE)
    assert outcome.status is OutcomeStatus.REFUSED
    assert len(client.calls) == 1


def test_mcp_error_is_retried_then_unavailable():
    client = FakeClient(McpError("down"))
    with pytest.raises(McpUnavailable):
        executor(client, retry_attempts=3).execute(ISOLATE)
    assert len(client.calls) == 3


def test_mcp_error_then_success_is_done():
    client = FakeClient(McpError("blip"), OK)
    outcome = executor(client).execute(ISOLATE)
    assert outcome.status is OutcomeStatus.DONE
    assert len(client.calls) == 2


def test_stop_event_ends_retries():
    stop_event = threading.Event()
    stop_event.set()
    client = FakeClient(McpError("down"))
    with pytest.raises(McpUnavailable):
        executor(client, retry_attempts=5, stop_event=stop_event).execute(ISOLATE)
    assert len(client.calls) == 1


def test_stop_event_cuts_the_backoff_sleep():
    # SIGTERM during a long backoff must not hold shutdown past the grace period
    stop_event = threading.Event()
    client = FakeClient(McpError("down"))
    slow = ActionExecutor(
        client,
        retry_attempts=5,
        backoff_seconds=30,
        dry_run=False,
        stop_event=stop_event,
    )
    threading.Timer(0.2, stop_event.set).start()
    started = time.monotonic()
    with pytest.raises(McpUnavailable):
        slow.execute(ISOLATE)
    assert time.monotonic() - started < 5


@pytest.mark.parametrize("action", [ISOLATE, REINSTATE])
def test_dry_run_makes_no_call(action):
    client = FakeClient(OK)
    outcome = executor(client, dry_run=True).execute(action)
    assert outcome.status is OutcomeStatus.DRY_RUN
    assert client.calls == []


@pytest.mark.parametrize("status", ["isolated", "quarantined"])
def test_reinstate_when_contained(status):
    client = FakeClient({"device_id": DEVICE, "status": status}, OK)
    outcome = executor(client).execute(REINSTATE)
    assert outcome.status is OutcomeStatus.DONE
    assert client.calls == [
        ("get_device_state", {"device_id": DEVICE}),
        ("reinstate_device", {"device_id": DEVICE}),
    ]


@pytest.mark.parametrize("state", [{"device_id": DEVICE, "status": "ok"}, None])
def test_reinstate_skipped_when_ok_or_unknown(state):
    client = FakeClient(state)
    outcome = executor(client).execute(REINSTATE)
    assert outcome.status is OutcomeStatus.SKIPPED
    assert client.calls == [("get_device_state", {"device_id": DEVICE})]
