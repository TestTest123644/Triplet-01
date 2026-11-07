"""The Twin over MCP, against the mock Twin's own MCP server (spec 5.2, 10.5)."""

import asyncio
import socket
import threading
import time

import pytest
import uvicorn

from city_brain.adapters.mcp_twin import McpTwinClient
from city_brain.edge.profiles import ToolCall
from mocks.twin import MockTwin
from mocks.twin.mcp_server import TOOL_NAMES, build_mock_mcp_server


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class Server:
    """The mock Twin's MCP server on a thread, so a client can reach it over a socket."""

    def __init__(self, twin: MockTwin):
        self.twin = twin
        self.port = free_port()
        app = build_mock_mcp_server(twin).sse_app()
        self._server = uvicorn.Server(uvicorn.Config(
            app, host="127.0.0.1", port=self.port, log_level="warning"))
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/sse"

    def __enter__(self) -> "Server":
        self._thread.start()
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            if getattr(self._server, "started", False):
                return self
            time.sleep(0.05)
        raise RuntimeError("the mock Twin's MCP server did not start")

    def __exit__(self, *exc: object) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=10.0)


@pytest.fixture
def twin():
    return MockTwin()


@pytest.fixture
def server(twin):
    with Server(twin) as server:
        yield server


@pytest.fixture
async def client(server):
    client = McpTwinClient(server.url, timeout_s=10.0)
    try:
        yield client
    finally:
        await client.close()


# ----- the tool list is the contract -----


async def test_the_mock_offers_exactly_the_twins_tools(server):
    from mcp import ClientSession
    from mcp.client.sse import sse_client

    async with sse_client(server.url) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
    assert sorted(t.name for t in tools.tools) == sorted(TOOL_NAMES)


# ----- the registry -----


async def test_the_registry_comes_back_with_every_device_and_key(client, twin):
    devices, keys, city = await client.load_registry()
    assert len(devices) == len(twin.devices) == 42
    assert set(keys) == set(twin.devices)
    assert keys["z1-wl1"] == twin.get_device_key("z1-wl1")
    assert city["total_devices"] == 42


async def test_the_registry_maps_onto_canonical_devices(client, twin):
    devices, _, _ = await client.load_registry()
    profile = twin.profile.map_device(devices[0])
    assert profile.device_id and profile.zone_id and profile.kind


# ----- commands -----


async def test_a_valve_command_is_accepted_and_takes_effect(client, twin):
    outcome = await client.call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"}), "k-1")
    assert outcome.outcome == "ACCEPTED" and outcome.reply["status"] == "ok"
    assert twin.devices["z1-wl1"].actuator_state["valve_position"] == "open"


async def test_an_unknown_device_is_rejected(client):
    outcome = await client.call(ToolCall("set_valve", {"device_id": "nope", "target_state": "open"}), "k-2")
    assert outcome.outcome == "REJECTED"


async def test_a_dispatch_reaches_the_zone(client, twin):
    outcome = await client.call(
        ToolCall("dispatch", {"zone_id": "zone-1", "incident_type": "flood"}), "k-3")
    assert outcome.outcome == "ACCEPTED"
    assert twin.zone_dispatches["zone-1"], "the city got the callout"


async def test_a_result_can_be_read_back_by_its_key(client):
    await client.call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "closed"}), "k-4")
    assert (await client.get_result("k-4"))["status"] == "ok"


async def test_an_unknown_key_has_no_result(client):
    assert await client.get_result("never-sent") is None


async def test_calls_are_serialised(client, twin):
    """Many commands at once must not cross wires: each reply belongs to its own call."""
    targets = [f"z{z}-wl1" for z in range(1, 7)]
    outcomes = await asyncio.gather(*(
        client.call(ToolCall("set_valve", {"device_id": t, "target_state": "open"}), f"key-{t}")
        for t in targets
    ))
    assert all(o.outcome == "ACCEPTED" for o in outcomes)
    assert {o.reply["device_id"] for o in outcomes} == set(targets)


# ----- failure never reaches the core -----


async def test_an_unreachable_twin_is_unknown_not_an_exception():
    client = McpTwinClient(f"http://127.0.0.1:{free_port()}/sse", timeout_s=0.5)
    outcome = await client.call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"}), "k")
    assert outcome.outcome == "UNKNOWN" and "error" in outcome.reply
    await client.close()


async def test_a_timeout_is_unknown(server, monkeypatch):
    client = McpTwinClient(server.url, timeout_s=10.0)
    await client.connect()

    async def crawl(*args, **kwargs):
        await asyncio.sleep(5.0)

    monkeypatch.setattr(client._session, "call_tool", crawl)
    client._timeout = 0.2
    outcome = await client.call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"}), "k")
    assert outcome.outcome == "UNKNOWN"
    await client.close()


async def test_an_unexpected_reply_is_unknown(client, monkeypatch):
    async def odd(tool, args):
        return ["not", "a", "reply"]

    monkeypatch.setattr(client, "_call_tool", odd)
    outcome = await client.call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"}), "k")
    assert outcome.outcome == "UNKNOWN" and outcome.reply["error"] == "UnexpectedReply"


async def test_a_lost_session_is_reopened_on_the_next_call(client, twin):
    await client.call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"}), "k-a")
    await client.close()
    outcome = await client.call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "closed"}), "k-b")
    assert outcome.outcome == "ACCEPTED"


async def test_closing_twice_is_harmless(client):
    await client.close()
    await client.close()


# ----- the Brain's own read-only server -----


async def test_the_brain_mcp_server_offers_only_read_tools(tmp_path):
    from mcp.server.fastmcp import FastMCP

    from city_brain.adapters.mcp_server import build_brain_mcp_server
    from city_brain.adapters.mcp_server.read_only import TOOL_NAMES as BRAIN_TOOLS

    class FakeApp:
        state = None

    server = build_brain_mcp_server(FakeApp())
    assert isinstance(server, FastMCP)
    tools = await server.list_tools()
    assert sorted(t.name for t in tools) == sorted(BRAIN_TOOLS)
    assert not any("set" in t.name or "dispatch" in t.name for t in tools)
