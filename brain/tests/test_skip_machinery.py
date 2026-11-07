import socket
from contextlib import closing

import pytest

from tests.partners import ENDPOINTS, reachable, skip_reason


def test_reachable_is_false_for_closed_port():
    assert reachable("localhost:1", timeout_s=0.1) is False


def _listening_port():
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen()
    return closing(srv), srv.getsockname()[1]


def test_reachable_is_true_for_listening_port():
    guard, port = _listening_port()
    with guard:
        assert reachable(f"127.0.0.1:{port}") is True


def test_reachable_is_true_for_listening_port_via_url():
    guard, port = _listening_port()
    with guard:
        assert reachable(f"http://127.0.0.1:{port}/sse") is True


def test_reachable_accepts_a_url():
    assert reachable("http://localhost:1/sse", timeout_s=0.1) is False


def test_skip_reason_names_env_var_and_endpoint():
    ep = ENDPOINTS["twin_live"]
    assert skip_reason(ep, "http://x:1/sse") == "Twin not reachable at http://x:1/sse ($BRAIN_TWIN_MCP)"


@pytest.mark.twin_live
def test_marker_skips_when_twin_is_down():
    raise AssertionError("must be skipped with nothing running")
