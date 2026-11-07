"""Automated QA & Conformance Testing Suite (Phase 6).

Assigned to: Agent_QA & Agent_Reviewer
Validates:
1. Mock Guardian stream verification, anomaly detection, and MCP containment.
2. Mock City Brain threshold actuation via MCP.
3. 30-second stress test: 300 ticks * 42 devices (12,600 messages) at 420 msgs/sec.
4. SQLite Truth Ledger integrity locking during physical spoofing.
5. Zero cross-contamination review gate.
"""

import json
import os
import resource
import tempfile
import time
import pytest

from mocks.mock_brain import MockCityBrain
from mocks.mock_guardian import MockGuardian
from twin.db_ledger import GroundTruthLedger
from twin.kafka_streamer import KafkaStreamer
from twin.mcp_server import set_active_simulator
from twin.simulation import CitySimulator


@pytest.fixture
def test_env():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "qa_ground_truth.db")
        ledger = GroundTruthLedger(db_path=db_path)
        sim = CitySimulator(seed=1234, ledger=ledger)
        set_active_simulator(sim)
        streamer = KafkaStreamer(mock_mode=True)

        def _call_tool(tool_name: str, args: dict):
            import twin.mcp_server as srv
            fn = getattr(srv, tool_name, None)
            if fn:
                return fn(**args)
            raise ValueError(f"Unknown tool: {tool_name}")

        guardian = MockGuardian(call_tool_fn=_call_tool)
        brain = MockCityBrain(call_tool_fn=_call_tool)
        yield {
            "ledger": ledger,
            "sim": sim,
            "streamer": streamer,
            "guardian": guardian,
            "brain": brain,
        }
        set_active_simulator(None)


def test_mock_guardian_verification_and_containment(test_env):
    sim: CitySimulator = test_env["sim"]
    streamer: KafkaStreamer = test_env["streamer"]
    guardian: MockGuardian = test_env["guardian"]

    # 1. Normal clean tick: all 42 signatures must verify authentic
    readings = sim.tick()
    assert len(readings) == 42
    for raw in readings:
        dev = sim.devices[raw["device_id"]]
        _, raw_bytes = streamer.stream_reading(raw, dev.private_key)
        res = guardian.process_reading(raw_bytes)
        assert res["action"] == "verified_authentic"
    assert guardian.verified_readings_count == 42
    assert len(guardian.quarantined_devices) == 0

    # 2. Injected link corruption: signature verification must fail -> device quarantined
    target_dev = "z1-tc1"
    sim.inject_attack(
        capability="link",
        target_device_id=target_dev,
        params={"link_mode": "corrupt_bytes"},
    )
    corrupt_readings = sim.tick()
    target_raw = next(r for r in corrupt_readings if r["device_id"] == target_dev)
    dev = sim.devices[target_dev]
    _, corrupt_bytes = streamer.stream_reading(target_raw, dev.private_key)

    for _ in range(3):
        res = guardian.process_reading(corrupt_bytes)
    assert res["action"] == "signature_verification_failed"
    assert target_dev in guardian.quarantined_devices
    assert sim.devices[target_dev].status == "quarantined"

    # 3. Injected rogue attack: unknown device ID -> rogue detected & quarantined
    sim.clear_all_attacks()
    sim.inject_attack(
        capability="rogue",
        params={"rogue_device_id": "rg-99", "value": 999.0},
    )
    rogue_readings = sim.tick()
    rogue_raw = next(r for r in rogue_readings if r["device_id"] == "rg-99")
    # Stream with a random key
    from twin.crypto_utils import generate_keypair
    fake_priv, _ = generate_keypair()
    _, rogue_bytes = streamer.stream_reading(rogue_raw, fake_priv)

    res = guardian.process_reading(rogue_bytes)
    assert res["action"] == "rogue_device_detected"
    assert "rg-99" in guardian.quarantined_devices


def test_mock_brain_actuation(test_env):
    sim: CitySimulator = test_env["sim"]
    brain: MockCityBrain = test_env["brain"]

    # Feed camera baseline and drop to corroborate emergency call in zone-1 (< 50% of 5-min mean)
    brain.evaluate_reading({
        "reading_id": "r-z1-tc1-hist",
        "device_id": "z1-tc1",
        "device_type": "traffic_camera",
        "value": 100.0,
        "location": "zone-1",
        "status": "ok",
    })
    brain.evaluate_reading({
        "reading_id": "r-z1-tc1-drop",
        "device_id": "z1-tc1",
        "device_type": "traffic_camera",
        "value": 10.0,
        "location": "zone-1",
        "status": "ok",
    })
    # Simulate emergency surge payload
    emg_payload = {
        "reading_id": "r-z1-ec1-test",
        "device_id": "z1-ec1",
        "device_type": "emergency_call",
        "value": 12.0,  # exceeds 6.0 threshold
        "location": "zone-1",
        "status": "ok",
    }
    actions = brain.evaluate_reading(emg_payload)
    assert len(actions) == 1
    assert actions[0]["type"] == "dispatch"
    assert brain.dispatches_sent == 1
    # Check that dispatch was recorded in CitySimulator
    assert len(sim.zone_dispatches["zone-1"]) >= 1

    # Simulate flood level payload (both zone water sensors > 7.0m and rising)
    brain.evaluate_reading({
        "reading_id": "r-z1-wl2-test",
        "device_id": "z1-wl2",
        "device_type": "water_level",
        "value": 7.1,
        "location": "zone-1",
        "status": "ok",
    })
    water_payload = {
        "reading_id": "r-z1-wl1-test",
        "device_id": "z1-wl1",
        "device_type": "water_level",
        "value": 7.5,
        "location": "zone-1",
        "status": "ok",
    }
    actions = brain.evaluate_reading(water_payload)
    assert len(actions) >= 1
    assert any(a["type"] == "set_valve" for a in actions)
    assert brain.valve_commands_sent >= 1
    assert sim.devices["z1-wl1"].actuator_state["valve_position"] == "open"


def test_sqlite_truth_locking_during_physical_attack(test_env):
    sim: CitySimulator = test_env["sim"]
    ledger: GroundTruthLedger = test_env["ledger"]
    streamer: KafkaStreamer = test_env["streamer"]
    dev_id = "z3-tc1" if "z3-tc1" in sim.devices else list(sim.devices.keys())[0]

    # Inject physical attack: perception forced to 199.99
    sim.inject_attack(
        capability="device_physical",
        target_device_id=dev_id,
        params={"spoofed_value": 199.99},
        description="Physical sensor spoofed during test",
    )

    readings = sim.tick()
    target_raw = next(r for r in readings if r["device_id"] == dev_id)
    assert target_raw["value"] == 199.99

    dev = sim.devices[dev_id]
    reading_model, raw_bytes = streamer.stream_reading(target_raw, dev.private_key)

    # External stream received 199.99
    payload_dict = json.loads(raw_bytes.decode("utf-8"))
    assert payload_dict["value"] == 199.99

    # SQLite database locked the TRUE physical value
    truth = ledger.get_reading_truth(target_raw["reading_id"])
    assert truth is not None
    assert truth["broadcast_value"] == 199.99
    assert truth["true_value"] != 199.99  # Ground truth was not corrupted!
    assert truth["is_attacked"] == 1
    assert truth["attack_type"] == "device_physical"


def test_30_second_high_throughput_stress_benchmark(test_env):
    """Stress test: 300 simulation cycles (30 simulated seconds) simulating 42 devices at 10 Hz (12,600 total messages)."""
    sim: CitySimulator = test_env["sim"]
    streamer: KafkaStreamer = test_env["streamer"]

    # Measure memory before stress run
    mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    start_wall_time = time.monotonic()
    total_messages = 0
    total_cycles = 300  # 300 cycles * 0.1s step = 30 simulated seconds

    for _ in range(total_cycles):
        tick_readings = sim.tick()
        assert len(tick_readings) == 42
        for raw in tick_readings:
            dev = sim.devices[raw["device_id"]]
            _, raw_bytes = streamer.stream_reading(raw, dev.private_key)
            assert len(raw_bytes) <= 300
            total_messages += 1

    elapsed_wall = time.monotonic() - start_wall_time
    mem_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    # macOS ru_maxrss is in bytes, Linux in kilobytes
    mem_delta_mb = (mem_after - mem_before) / (1024 * 1024) if os.uname().sysname == "Darwin" else (mem_after - mem_before) / 1024
    throughput_msgs_per_sec = total_messages / elapsed_wall

    print(
        f"\n--- STRESS TEST BENCHMARK RESULTS ---\n"
        f"Simulated Cycles: {total_cycles} (30.0 simulated seconds at 10 Hz)\n"
        f"Total Messages Produced: {total_messages}\n"
        f"Wall-Clock Execution Time: {elapsed_wall:.2f}s\n"
        f"Sustained Production Throughput: {throughput_msgs_per_sec:.1f} msgs/sec\n"
        f"Initial Memory RSS: {mem_before}\n"
        f"Final Memory RSS: {mem_after}\n"
        f"Memory Delta: {mem_delta_mb:.2f} MB\n"
        f"------------------------------------"
    )

    assert total_messages == 12600
    # Throughput must comfortably exceed 420 msgs/sec target
    assert throughput_msgs_per_sec > 420.0
    # No memory leak: memory delta should be minimal (< 50MB)
    assert abs(mem_delta_mb) < 50.0


def test_final_review_gate_zero_cross_contamination(test_env):
    """Final Review Gate: zero cross-contamination, schema adherence, clock integrity."""
    import twin.mcp_server as mcp_mod
    import twin.kafka_streamer as kafka_mod

    # 1. MCP module must NOT import or possess GroundTruthLedger
    assert not hasattr(mcp_mod, "GroundTruthLedger")

    # 2. Kafka module must NOT possess incoming RPC tools
    assert not hasattr(kafka_mod, "FastMCP")
    assert not hasattr(kafka_mod, "MCPServer")

    # 3. Verify all device payloads adhere to RFC 3339 UTC without wall-clock contamination
    sim: CitySimulator = test_env["sim"]
    readings = sim.tick()
    for r in readings:
        assert r["ts"].endswith("Z")
        assert " " not in r["ts"]  # Strict ISO-8601 / RFC 3339
