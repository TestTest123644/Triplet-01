"""Unit tests for Phase 2: Core Physics, Dual-Clock Engine, and Ledger Integration."""

import os
import tempfile
from datetime import datetime, timezone
import pytest

from twin.db_ledger import GroundTruthLedger
from twin.simulation import CitySimulator, SimulationClock, format_sim_time


@pytest.fixture
def temp_ledger():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_sim_ledger.db")
        ledger = GroundTruthLedger(db_path=db_path)
        yield ledger


def test_simulation_clock():
    start_dt = datetime(2026, 10, 17, 12, 0, 0, tzinfo=timezone.utc)
    clock = SimulationClock(start_time=start_dt, step_seconds=0.1)

    assert clock.get_sim_time() == "2026-10-17T12:00:00Z"
    t1 = clock.advance_tick()
    assert t1 == "2026-10-17T12:00:00Z"
    assert clock.tick_count == 1
    t2 = clock.advance_tick()
    assert clock.tick_count == 2


def test_device_inventory(temp_ledger):
    sim = CitySimulator(seed=123, ledger=temp_ledger)

    # Must have exactly 42 devices
    assert len(sim.devices) == 42

    # Check 6 zones with 7 devices each
    zones = set()
    device_types = set()
    for dev_id, dev in sim.devices.items():
        zones.add(dev.zone_id)
        device_types.add(dev.device_type)
        assert dev.min_val <= dev.current_value <= dev.max_val
        assert dev.public_key is not None
        assert dev.private_key is not None

    assert len(zones) == 6
    assert device_types == {"traffic_camera", "water_level", "power_load", "air_quality", "emergency_call"}

    # Verify each zone has exactly 7 devices
    for zone_id in zones:
        zone_devs = [d for d in sim.devices.values() if d.zone_id == zone_id]
        assert len(zone_devs) == 7

    # Verify devices are in SQLite ledger
    stored_devices = temp_ledger.list_devices()
    assert len(stored_devices) == 42


def test_tick_loop_and_ledger_integration(temp_ledger):
    sim = CitySimulator(seed=999, ledger=temp_ledger)

    # Run 5 ticks (0.5 simulated seconds)
    for _ in range(5):
        broadcast_readings = sim.tick()
        assert len(broadcast_readings) == 42

    # Check SQLite ledger contains 5 * 42 = 210 readings
    all_readings = temp_ledger.get_readings_truth(limit=500)
    assert len(all_readings) == 210

    # Ensure all readings have strict RFC 3339 UTC timestamps
    for r in all_readings:
        assert r["ts"].endswith("Z")
        assert r["true_value"] == r["broadcast_value"]  # Normal simulation
        assert r["is_attacked"] == 0


def test_actuation_physics_impact(temp_ledger):
    sim = CitySimulator(seed=42, ledger=temp_ledger)

    # Test valve opening impact on water level
    water_dev_id = "z1-wl1"
    init_val = sim.devices[water_dev_id].current_value

    # Open valve
    res = sim.handle_set_valve(water_dev_id, "open")
    assert res["status"] == "ok"
    assert sim.devices[water_dev_id].actuator_state["valve_position"] == "open"

    # Tick 10 times (1 second)
    for _ in range(10):
        sim.tick()

    final_val = sim.devices[water_dev_id].current_value
    # Water level should drop due to open valve discharge
    assert final_val < init_val


def test_containment_status_updates(temp_ledger):
    sim = CitySimulator(seed=42, ledger=temp_ledger)
    dev_id = "z2-tc1"

    res = sim.update_device_status(dev_id, "quarantined")
    assert res["status"] == "ok"
    assert sim.devices[dev_id].status == "quarantined"

    readings = sim.tick()
    target_reading = next(r for r in readings if r["device_id"] == dev_id)
    assert target_reading["status"] == "quarantined"


def test_water_sensor_variety_and_determinism(temp_ledger):
    """Water sensors have distinct levels after 100 ticks and are deterministic for a given seed."""
    sim1 = CitySimulator(seed=42, ledger=temp_ledger)
    for _ in range(100):
        sim1.clock.advance_tick()
        sim1.update_physics()
    vals1 = {d_id: dev.current_value for d_id, dev in sim1.devices.items() if dev.device_type == "water_level"}

    assert len(vals1) == 12
    # Verify values are not all equal
    assert len(set(vals1.values())) > 1

    # Verify identical values with same seed
    sim2 = CitySimulator(seed=42, ledger=temp_ledger)
    for _ in range(100):
        sim2.clock.advance_tick()
        sim2.update_physics()
    vals2 = {d_id: dev.current_value for d_id, dev in sim2.devices.items() if dev.device_type == "water_level"}

    assert vals1 == vals2


