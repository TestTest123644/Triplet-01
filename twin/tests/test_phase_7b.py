"""Phase 7B Conformance Tests: End-to-end execution of scenarios t1 through t11 in CI mode.

Each test runs the scenario script against an in-process Twin simulation with
QueueSource mocks and verifies that a valid PASS log is produced in tmp_path.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import queue
import sys
import threading
import time
from typing import Any, Dict, Generator, List, Optional, Tuple

import pytest
import uvicorn

# Add root and twin to sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
TWIN_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if TWIN_ROOT not in sys.path:
    sys.path.insert(0, TWIN_ROOT)

from mocks.mock_brain import MockCityBrain
from mocks.mock_guardian import MockGuardian
from mocks.reading_source import QueueSource
from scenarios._twin_common import ScenarioContext
import scenarios.t1_baseline_stream as sc_t1
import scenarios.t2_traffic_accident as sc_t2
import scenarios.t3_port_flood as sc_t3
import scenarios.t4_fire as sc_t4
import scenarios.t5_grid_overload as sc_t5
import scenarios.t6_fake_port_flood as sc_t6
import scenarios.t7_stuck_sensor as sc_t7
import scenarios.t8_rogue_device as sc_t8
import scenarios.t9_stolen_key as sc_t9
import scenarios.t10_link_tamper as sc_t10
import scenarios.t11_actuator_refusal as sc_t11

from twin.db_ledger import GroundTruthLedger
from twin.kafka_streamer import KafkaStreamer
from twin.mcp_server import build_mcp_app, set_active_simulator
from twin.red_team_api import build_red_team_app
from twin.simulation import CitySimulator


# ==============================================================================
# Ephemeral Server Helpers
# ==============================================================================

@contextlib.contextmanager
def running_mcp_server(sim: CitySimulator) -> Generator[str, None, None]:
    """Start an in-process MCP server on an ephemeral port."""
    set_active_simulator(sim)
    app = build_mcp_app()
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    thread_exc: List[BaseException] = []

    def _runner():
        try:
            server.run()
        except BaseException as exc:
            thread_exc.append(exc)

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()

    deadline = time.time() + 10.0
    while not server.started and thread.is_alive() and time.time() < deadline:
        time.sleep(0.01)

    if not server.started:
        server.should_exit = True
        thread.join(timeout=1.0)
        if thread_exc:
            raise RuntimeError(f"MCP server thread died: {thread_exc[0]}") from thread_exc[0]
        raise TimeoutError("Timed out waiting for MCP server to start.")

    port = None
    for s in getattr(server, "servers", []):
        for sock in getattr(s, "sockets", []):
            port = sock.getsockname()[1]
            break
        if port:
            break
    if not port:
        server.should_exit = True
        thread.join(timeout=1.0)
        raise RuntimeError("Could not find ephemeral port for MCP server.")

    url = f"http://127.0.0.1:{port}"
    try:
        yield url
    finally:
        server.should_exit = True
        thread.join(timeout=3.0)


@contextlib.contextmanager
def running_red_team_server(sim: CitySimulator, token: str, streamer: KafkaStreamer) -> Generator[str, None, None]:
    """Start an in-process Red Team REST server on an ephemeral port."""
    app = build_red_team_app(sim, token=token, streamer=streamer)
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    thread_exc: List[BaseException] = []

    def _runner():
        try:
            server.run()
        except BaseException as exc:
            thread_exc.append(exc)

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()

    deadline = time.time() + 10.0
    while not server.started and thread.is_alive() and time.time() < deadline:
        time.sleep(0.01)

    if not server.started:
        server.should_exit = True
        thread.join(timeout=1.0)
        if thread_exc:
            raise RuntimeError(f"Red Team server thread died: {thread_exc[0]}") from thread_exc[0]
        raise TimeoutError("Timed out waiting for Red Team server to start.")

    port = None
    for s in getattr(server, "servers", []):
        for sock in getattr(s, "sockets", []):
            port = sock.getsockname()[1]
            break
        if port:
            break
    if not port:
        server.should_exit = True
        thread.join(timeout=1.0)
        raise RuntimeError("Could not find ephemeral port for Red Team server.")

    url = f"http://127.0.0.1:{port}"
    try:
        yield url
    finally:
        server.should_exit = True
        thread.join(timeout=3.0)


# ==============================================================================
# Unified In-Process Test Harness
# ==============================================================================

class DualQueueSource:
    """Delivers telemetry to both internal mock processing and scenario wire consumer."""

    def __init__(self):
        self.mock_q: queue.Queue[Any] = queue.Queue(maxsize=10000)
        self.wire_q: queue.Queue[Any] = queue.Queue(maxsize=10000)

    def put(self, item: Any) -> None:
        try:
            self.mock_q.put_nowait(item)
        except queue.Full:
            pass
        try:
            self.wire_q.put_nowait(item)
        except queue.Full:
            pass

    def get_mock_reading(self, timeout: float = 0.001) -> Optional[Any]:
        try:
            return self.mock_q.get(timeout=timeout)
        except queue.Empty:
            return None

    def get_reading(self, timeout: float = 0.05) -> Optional[Any]:
        """Method called by WireConsumer."""
        try:
            return self.wire_q.get(timeout=timeout)
        except queue.Empty:
            return None


@contextlib.contextmanager
def running_ci_environment(
    tmp_path,
    sim_seed: int = 100,
    sim_speed: float = 60.0,
    enable_brain: bool = True,
) -> Generator[Dict[str, Any], None, None]:
    """Start in-process twin simulation with ephemeral servers, QueueSource, and live mocks."""
    db_path = str(tmp_path / f"sim_{sim_seed}.db")
    sim = CitySimulator(seed=sim_seed, ledger=GroundTruthLedger(db_path=db_path), sim_speed=sim_speed)
    streamer = KafkaStreamer(mock_mode=True)
    token = f"ci-token-{sim_seed}"
    dual_qs = DualQueueSource()

    with running_mcp_server(sim) as mcp_url:
        with running_red_team_server(sim, token=token, streamer=streamer) as twin_host:
            guardian = MockGuardian(reading_source=None, mcp_url=mcp_url)
            brain = MockCityBrain(reading_source=None, mcp_url=mcp_url) if enable_brain else None

            stop_event = threading.Event()

            def _ticker_thread():
                async def _run():
                    await guardian.mcp.connect()
                    if brain:
                        await brain.mcp.connect()
                        await brain.refresh_zone_cameras()

                    while not stop_event.is_set():
                        # Tick simulation
                        readings = sim.tick()
                        for r in readings:
                            dev_id = r.get("device_id")
                            dev_key = sim.signing_key_for(dev_id)
                            res = streamer.stream_reading(r, dev_key)
                            if res:
                                dual_qs.put(res[1])

                        # Pump readings into Guardian and Brain mocks
                        while True:
                            item = dual_qs.get_mock_reading(timeout=0.0005)
                            if item is None:
                                break
                            await guardian.process_reading_async(item)
                            if brain:
                                await brain.evaluate_reading_async(item)

                        await asyncio.sleep(0.01)

                    await guardian.mcp.close()
                    if brain:
                        await brain.mcp.close()

                asyncio.run(_run())

            t = threading.Thread(target=_ticker_thread, daemon=True)
            t.start()

            # Warm up stream with 15 ticks
            time.sleep(0.3)

            env_info = {
                "sim": sim,
                "twin_host": twin_host,
                "mcp_url": mcp_url,
                "token": token,
                "dual_qs": dual_qs,
                "guardian": guardian,
                "brain": brain,
            }
            try:
                yield env_info
            finally:
                stop_event.set()
                t.join(timeout=3.0)


def verify_log_file(log_path: str) -> None:
    """Verify that scenario log file exists, contains valid JSON lines, and ends with result=PASS."""
    assert os.path.exists(log_path), f"Log file does not exist: {log_path}"
    lines = []
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line_s = line.strip()
            if line_s:
                lines.append(json.loads(line_s))

    assert len(lines) >= 2, f"Expected at least 2 log entries, got {len(lines)}"
    last_line = lines[-1]
    assert last_line.get("event") == "result", f"Final log line must have event='result', got {last_line}"
    assert last_line.get("pass") is True, f"Final result pass must be True, got {last_line}"
    assert len(last_line.get("checks", [])) > 0, "Final result must contain at least 1 check"


# ==============================================================================
# CI Test Suite for Scenarios t1 through t11
# ==============================================================================

def test_ci_t1_baseline_stream(tmp_path):
    with running_ci_environment(tmp_path, sim_seed=701, sim_speed=1.0) as env:
        ctx = ScenarioContext(
            scenario_id="t1",
            name="t1_baseline_stream",
            twin_host=env["twin_host"],
            mcp_url=env["mcp_url"],
            red_team_token=env["token"],
            log_dir=str(tmp_path),
            queue_source=env["dual_qs"],
        )
        with ctx:
            passed = sc_t1.run(ctx)
        assert passed is True
        verify_log_file(ctx.log_file)


def test_ci_t2_traffic_accident(tmp_path):
    with running_ci_environment(tmp_path, sim_seed=702, sim_speed=60.0) as env:
        ctx = ScenarioContext(
            scenario_id="t2",
            name="t2_traffic_accident",
            twin_host=env["twin_host"],
            mcp_url=env["mcp_url"],
            red_team_token=env["token"],
            log_dir=str(tmp_path),
            queue_source=env["dual_qs"],
        )
        with ctx:
            passed = sc_t2.run(ctx)
        assert passed is True
        verify_log_file(ctx.log_file)


def test_ci_t3_port_flood(tmp_path):
    # Test mitigated storm with Brain
    with running_ci_environment(tmp_path, sim_seed=703, sim_speed=60.0, enable_brain=True) as env:
        ctx = ScenarioContext(
            scenario_id="t3",
            name="t3_port_flood",
            twin_host=env["twin_host"],
            mcp_url=env["mcp_url"],
            red_team_token=env["token"],
            log_dir=str(tmp_path),
            queue_source=env["dual_qs"],
        )
        with ctx:
            passed = sc_t3.run(ctx)
        assert passed is True
        verify_log_file(ctx.log_file)


def test_ci_t4_fire(tmp_path):
    with running_ci_environment(tmp_path, sim_seed=704, sim_speed=60.0) as env:
        ctx = ScenarioContext(
            scenario_id="t4",
            name="t4_fire",
            twin_host=env["twin_host"],
            mcp_url=env["mcp_url"],
            red_team_token=env["token"],
            log_dir=str(tmp_path),
            queue_source=env["dual_qs"],
        )
        with ctx:
            passed = sc_t4.run(ctx)
        assert passed is True
        verify_log_file(ctx.log_file)


def test_ci_t5_grid_overload(tmp_path):
    with running_ci_environment(tmp_path, sim_seed=705, sim_speed=60.0) as env:
        ctx = ScenarioContext(
            scenario_id="t5",
            name="t5_grid_overload",
            twin_host=env["twin_host"],
            mcp_url=env["mcp_url"],
            red_team_token=env["token"],
            log_dir=str(tmp_path),
            queue_source=env["dual_qs"],
        )
        with ctx:
            passed = sc_t5.run(ctx)
        assert passed is True
        verify_log_file(ctx.log_file)


def test_ci_t6_fake_port_flood(tmp_path):
    with running_ci_environment(tmp_path, sim_seed=706, sim_speed=60.0) as env:
        ctx = ScenarioContext(
            scenario_id="t6",
            name="t6_fake_port_flood",
            twin_host=env["twin_host"],
            mcp_url=env["mcp_url"],
            red_team_token=env["token"],
            log_dir=str(tmp_path),
            queue_source=env["dual_qs"],
        )
        with ctx:
            passed = sc_t6.run(ctx)
        assert passed is True
        verify_log_file(ctx.log_file)


def test_ci_t7_stuck_sensor(tmp_path):
    with running_ci_environment(tmp_path, sim_seed=707, sim_speed=60.0) as env:
        ctx = ScenarioContext(
            scenario_id="t7",
            name="t7_stuck_sensor",
            twin_host=env["twin_host"],
            mcp_url=env["mcp_url"],
            red_team_token=env["token"],
            log_dir=str(tmp_path),
            queue_source=env["dual_qs"],
        )
        with ctx:
            passed = sc_t7.run(ctx)
        assert passed is True
        verify_log_file(ctx.log_file)


def test_ci_t8_rogue_device(tmp_path):
    with running_ci_environment(tmp_path, sim_seed=708, sim_speed=60.0) as env:
        ctx = ScenarioContext(
            scenario_id="t8",
            name="t8_rogue_device",
            twin_host=env["twin_host"],
            mcp_url=env["mcp_url"],
            red_team_token=env["token"],
            log_dir=str(tmp_path),
            queue_source=env["dual_qs"],
        )
        with ctx:
            passed = sc_t8.run(ctx)
        assert passed is True
        verify_log_file(ctx.log_file)


def test_ci_t9_stolen_key(tmp_path):
    with running_ci_environment(tmp_path, sim_seed=709, sim_speed=60.0) as env:
        ctx = ScenarioContext(
            scenario_id="t9",
            name="t9_stolen_key",
            twin_host=env["twin_host"],
            mcp_url=env["mcp_url"],
            red_team_token=env["token"],
            log_dir=str(tmp_path),
            queue_source=env["dual_qs"],
        )
        with ctx:
            passed = sc_t9.run(ctx)
        assert passed is True
        verify_log_file(ctx.log_file)


def test_ci_t10_link_tamper(tmp_path):
    with running_ci_environment(tmp_path, sim_seed=710, sim_speed=60.0) as env:
        ctx = ScenarioContext(
            scenario_id="t10",
            name="t10_link_tamper",
            twin_host=env["twin_host"],
            mcp_url=env["mcp_url"],
            red_team_token=env["token"],
            log_dir=str(tmp_path),
            queue_source=env["dual_qs"],
        )
        with ctx:
            passed = sc_t10.run(ctx, mode="corrupt_bytes")
        assert passed is True
        verify_log_file(ctx.log_file)


def test_ci_t11_actuator_refusal(tmp_path):
    with running_ci_environment(tmp_path, sim_seed=711, sim_speed=60.0) as env:
        ctx = ScenarioContext(
            scenario_id="t11",
            name="t11_actuator_refusal",
            twin_host=env["twin_host"],
            mcp_url=env["mcp_url"],
            red_team_token=env["token"],
            log_dir=str(tmp_path),
            queue_source=env["dual_qs"],
        )
        with ctx:
            passed = sc_t11.run(ctx)
        assert passed is True
        verify_log_file(ctx.log_file)

