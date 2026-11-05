"""End-to-End Hackathon Presentation & Integration Conformance Suite.

Simulates the complete presentation walkthrough:
1. Pristine Twin Layer initialization (42 devices, 6 zones).
2. 10 Hz telemetry streaming under strict Phase 0 contract (<300 bytes, RFC 3339 UTC).
3. Live execution of all 6 Hackathon Demo Presets:
   - Preset 1: Physical Hardware Spoofing (z1-tc1)
   - Preset 2: Compromised Key Forgery (z3-pl1)
   - Preset 3: Link Layer Signature Corruption (z2-wl1)
   - Preset 4: Rogue Unauthorized Transmitter (rg-x9)
   - Preset 5: Continuous Sensor Drift Fault (z5-aq1)
   - Preset 6: Actuator Command Disobedience (z1-wl1)
4. Mock Guardian zero-trust detection & containment.
5. Mock City Brain threshold-triggered actuation.
6. Full threat disarm & nominal recovery.
7. Complete MCP Tool surface verification (Queries, Actuation, Containment).
"""

import json
import os
import tempfile
import pytest

from mocks.mock_brain import MockCityBrain
from mocks.mock_guardian import MockGuardian
from twin.crypto_utils import verify_payload_signature
from twin.db_ledger import GroundTruthLedger
from twin.kafka_streamer import KafkaStreamer
from twin.mcp_server import (
    dispatch,
    get_city_state,
    get_command_result,
    get_device_key,
    get_device_state,
    get_reading,
    get_readings,
    isolate_device,
    list_devices,
    quarantine_device,
    reinstate_device,
    release_device,
    rollback_reading,
    set_active_simulator,
    set_signal_plan,
    set_valve,
    switch_grid,
)
from twin.simulation import CitySimulator


@pytest.fixture
def presentation_fixture():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "presentation_ground_truth.db")
        ledger = GroundTruthLedger(db_path=db_path)
        sim = CitySimulator(seed=2026, ledger=ledger)
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
            "tmpdir": tmpdir,
            "db_path": db_path,
            "ledger": ledger,
            "sim": sim,
            "streamer": streamer,
            "guardian": guardian,
            "brain": brain,
        }
        set_active_simulator(None)


def test_full_presentation_walkthrough(presentation_fixture):
    sim: CitySimulator = presentation_fixture["sim"]
    streamer: KafkaStreamer = presentation_fixture["streamer"]
    ledger: GroundTruthLedger = presentation_fixture["ledger"]
    guardian: MockGuardian = presentation_fixture["guardian"]
    brain: MockCityBrain = presentation_fixture["brain"]

    # -------------------------------------------------------------------------
    # STEP 1: Verify Initial Clean Baseline Across 42 Devices
    # -------------------------------------------------------------------------
    assert len(sim.devices) == 42
    assert len(sim.ZONES) == 6

    initial_readings = sim.tick()
    assert len(initial_readings) == 42
    for raw in initial_readings:
        dev = sim.devices[raw["device_id"]]
        model, serialized = streamer.stream_reading(raw, dev.private_key)
        assert len(serialized) <= 300
        assert model.ts.endswith("Z")
        verification = guardian.process_reading(serialized)
        assert verification["action"] == "verified_authentic"

    assert guardian.verified_readings_count == 42
    assert len(guardian.quarantined_devices) == 0

    # -------------------------------------------------------------------------
    # STEP 2: Execute Preset 1 - Physical Hardware Spoofing (z1-tc1)
    # -------------------------------------------------------------------------
    p1_id = sim.inject_attack(
        capability="device_physical",
        target_device_id="z1-tc1",
        params={"spoofed_value": 185.0},
        description="Demo: Physical Hardware Spoofing on Downtown traffic camera",
    )
    p1_readings = sim.tick()
    p1_raw = next(r for r in p1_readings if r["device_id"] == "z1-tc1")
    assert p1_raw["value"] == 185.0

    p1_dev = sim.devices["z1-tc1"]
    _, p1_bytes = streamer.stream_reading(p1_raw, p1_dev.private_key)
    # Guardian verifies signature because key was honest
    res = guardian.process_reading(p1_bytes)
    assert res["action"] == "verified_authentic"

    # Truth Ledger locked true value (deviation detected in ledger)
    truth = ledger.get_reading_truth(p1_raw["reading_id"])
    assert truth["broadcast_value"] == 185.0
    assert abs(truth["true_value"] - 185.0) > 20.0
    assert truth["is_attacked"] == 1
    assert truth["attack_type"] == "device_physical"

    # -------------------------------------------------------------------------
    # STEP 3: Execute Preset 2 - Compromised Key Forgery (z3-pl1)
    # -------------------------------------------------------------------------
    p2_id = sim.inject_attack(
        capability="device_key",
        target_device_id="z3-pl1",
        params={"spoofed_value": 112.5},
        description="Demo: Compromised Key Forgery on North Industrial Substation",
    )
    p2_readings = sim.tick()
    p2_raw = next(r for r in p2_readings if r["device_id"] == "z3-pl1")
    assert p2_raw["value"] == 112.5
    p2_truth = ledger.get_reading_truth(p2_raw["reading_id"])
    assert p2_truth["broadcast_value"] == 112.5
    assert p2_truth["attack_type"] == "device_key"

    # -------------------------------------------------------------------------
    # STEP 4: Execute Preset 3 - Link Layer Signature Corruption (z2-wl1)
    # -------------------------------------------------------------------------
    p3_id = sim.inject_attack(
        capability="link",
        target_device_id="z2-wl1",
        params={"link_mode": "corrupt_sig"},
        description="Demo: Link Layer Signature Corruption on Harbor Water Sensor",
    )
    p3_readings = sim.tick()
    p3_raw = next(r for r in p3_readings if r["device_id"] == "z2-wl1")
    assert p3_raw.get("_corrupt_sig") is True
    p3_dev = sim.devices["z2-wl1"]
    _, p3_bytes = streamer.stream_reading(p3_raw, p3_dev.private_key)

    # Guardian intercepts broken signature and quarantines after 3 failures
    for _ in range(3):
        p3_res = guardian.process_reading(p3_bytes)
    assert p3_res["action"] == "signature_verification_failed"
    assert "z2-wl1" in guardian.quarantined_devices
    assert sim.devices["z2-wl1"].status == "quarantined"

    # -------------------------------------------------------------------------
    # STEP 5: Execute Preset 4 - Rogue Unauthorized Transmitter (rg-x9)
    # -------------------------------------------------------------------------
    p4_id = sim.inject_attack(
        capability="rogue",
        params={"rogue_device_id": "rg-x9", "value": 888.0, "zone_id": "zone-4"},
        description="Demo: Rogue transmitter in West Residential Hills",
    )
    p4_readings = sim.tick()
    p4_raw = next(r for r in p4_readings if r["device_id"] == "rg-x9")
    assert p4_raw["value"] == 888.0

    from twin.crypto_utils import generate_keypair
    rogue_priv, _ = generate_keypair()
    _, p4_bytes = streamer.stream_reading(p4_raw, rogue_priv)

    p4_res = guardian.process_reading(p4_bytes)
    assert p4_res["action"] == "rogue_device_detected"
    assert "rg-x9" in guardian.quarantined_devices

    # -------------------------------------------------------------------------
    # STEP 6: Execute Preset 5 - Sensor Drift Fault (z5-aq1)
    # -------------------------------------------------------------------------
    p5_id = sim.inject_attack(
        capability="sensor_fault",
        target_device_id="z5-aq1",
        params={"fault_mode": "drift", "drift_rate": 2.5},
        description="Demo: AQI sensor drift in Central University",
    )
    drift_initial = sim.devices["z5-aq1"].drift_offset
    for _ in range(5):
        sim.tick()
    drift_after = sim.devices["z5-aq1"].drift_offset
    assert drift_after > drift_initial

    # -------------------------------------------------------------------------
    # STEP 7: Execute Preset 6 - Actuator Non-Execution (z1-wl1)
    # -------------------------------------------------------------------------
    p6_id = sim.inject_attack(
        capability="actuator",
        target_device_id="z1-wl1",
        params={"suppress_actuation": True},
        description="Demo: Actuator silent command suppression on flood valve",
    )
    act_res = set_valve(device_id="z1-wl1", target_state="open", idempotency_key="demo-valve-act")
    assert act_res["status"] == "ok"
    # Physical actuator state was suppressed by attack!
    assert sim.devices["z1-wl1"].actuator_state.get("valve_position") != "open"

    # -------------------------------------------------------------------------
    # STEP 8: Mock City Brain Autonomous Threshold Actuation
    # -------------------------------------------------------------------------
    surge_telemetry = {
        "reading_id": "r-surge-02",
        "device_id": "z2-ec1",
        "device_type": "emergency_call",
        "value": 14.5,
        "location": "zone-2",
        "status": "ok",
    }
    actions = brain.evaluate_reading(surge_telemetry)
    assert len(actions) == 1
    assert actions[0]["type"] == "dispatch"
    assert len(sim.zone_dispatches["zone-2"]) >= 1

    # -------------------------------------------------------------------------
    # STEP 9: Disarm All Threats & Nominal Recovery
    # -------------------------------------------------------------------------
    sim.clear_all_attacks()
    assert len(sim.active_attacks) == 0
    assert len(sim.rogue_attacks) == 0

    reinstate_device("z2-wl1")
    assert sim.devices["z2-wl1"].status == "ok"

    # Advance 3 ticks to verify return to nominal
    nominal_readings = sim.tick()
    nom_wl = next(r for r in nominal_readings if r["device_id"] == "z2-wl1")
    assert nom_wl.get("_corrupt_sig") is None

    # -------------------------------------------------------------------------
    # STEP 10: Complete MCP Tool Surface Verification
    # -------------------------------------------------------------------------
    # 1. Queries
    devs = list_devices()
    assert len(devs) == 42
    zone1_devs = list_devices(zone_id="zone-1")
    assert len(zone1_devs) == 7

    pub_key = get_device_key("z1-tc1")
    assert pub_key is not None and len(pub_key) > 20

    dev_state = get_device_state("z1-tc1")
    assert dev_state["device_id"] == "z1-tc1"

    city_state = get_city_state()
    assert city_state["total_devices"] == 42
    assert "zone-1" in city_state["zones"]

    # 2. Time-filtered readings
    cached_readings = get_readings(device_id="z1-tc1", limit=10)
    assert len(cached_readings) >= 1
    recent_ts = cached_readings[0]["ts"]
    filtered_readings = get_readings(device_id="z1-tc1", since_ts=recent_ts)
    assert len(filtered_readings) >= 1

    # 3. Actuation tools
    disp_res = dispatch("fire_alarm", "zone-2", idempotency_key="idemp-disp-01")
    assert disp_res["status"] == "ok"
    assert get_command_result("idemp-disp-01") == disp_res

    sig_res = set_signal_plan("z1-tc1", "green_wave")
    assert sig_res["status"] == "ok"
    assert sim.devices["z1-tc1"].actuator_state["signal_plan"] == "green_wave"

    grid_res = switch_grid("z1-pl1", "grid_b")
    assert grid_res["status"] == "ok"
    assert sim.devices["z1-pl1"].actuator_state["grid_state"] == "grid_b"

    # 4. Containment tools
    iso_res = isolate_device("z1-pl1")
    assert iso_res["new_status"] == "isolated"
    assert sim.devices["z1-pl1"].status == "isolated"

    rel_res = release_device("z1-pl1")
    assert rel_res["new_status"] == "ok"
    assert sim.devices["z1-pl1"].status == "ok"

    rollback_res = rollback_reading(nominal_readings[0]["reading_id"])
    assert rollback_res["status"] == "ok"
