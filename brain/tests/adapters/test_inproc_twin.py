from city_brain.adapters.inproc_twin import InProcTwin
from city_brain.edge.profiles import ToolCall
from mocks.twin.mock_twin import MockTwin


class ExplodingTwin:
    def handle_tool(self, tool, args):
        raise RuntimeError("boom")


async def test_load_registry_returns_42_devices_and_keys():
    devices, keys, city = await InProcTwin(MockTwin()).load_registry()
    assert len(devices) == 42 and set(keys) == {d["device_id"] for d in devices}
    assert city["total_devices"] == 42


async def test_set_valve_accepted():
    out = await InProcTwin(MockTwin()).call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"}), "brain-0-act-x")
    assert out.outcome == "ACCEPTED"


async def test_unknown_device_is_rejected():
    out = await InProcTwin(MockTwin()).call(ToolCall("set_valve", {"device_id": "nope", "target_state": "open"}), "k2")
    assert out.outcome == "REJECTED"


async def test_transport_error_is_unknown():
    out = await InProcTwin(ExplodingTwin()).call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"}), "k3")
    assert out.outcome == "UNKNOWN" and out.reply == {"error": "RuntimeError"}


async def test_result_is_cached_by_key():
    p = InProcTwin(MockTwin())
    await p.call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"}), "k4")
    assert (await p.get_result("k4"))["status"] == "ok"


async def test_get_result_unknown_key_is_none():
    assert await InProcTwin(MockTwin()).get_result("never") is None


async def test_call_does_not_mutate_tool_call_args():
    tc = ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"})
    await InProcTwin(MockTwin()).call(tc, "k5")
    assert "idempotency_key" not in tc.args
