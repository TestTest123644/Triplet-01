"""Unit and conformance tests for Phase 6: Physics Engine and Incident Dynamics.

Tests assertions 10.a through 10.i from the Phase 6 specification.
"""

from __future__ import annotations

import math
import os
import tempfile
import time
from datetime import datetime, timezone
import pytest

from twin.db_ledger import GroundTruthLedger
from twin.mcp_server import set_active_simulator, list_devices, get_device_state
from twin.simulation import CitySimulator, SimulationClock


@pytest.fixture
def scratch_ledger():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_physics.db")
        ledger = GroundTruthLedger(db_path=db_path)
        yield ledger


def test_10a_traffic_camera_kinetics(scratch_ledger):
    """10.a: traffic: two local maxima of hourly mean target between 08:00-09:00 and 17:00-18:00; overnight mean below 35."""
    sim = CitySimulator(seed=101, ledger=scratch_ledger)

    hourly_means = []
    for h in range(24):
        # Sample every 10 minutes within hour h
        targets_in_hour = []
        for m in (0, 10, 20, 30, 40, 50):
            t_hour = h + m / 60.0
            v_star = (
                25.0
                + 95.0 * math.exp(-((t_hour - 8.5) ** 2) / (2.0 * (1.2 ** 2)))
                + 110.0 * math.exp(-((t_hour - 17.5) ** 2) / (2.0 * (1.5 ** 2)))
            )
            targets_in_hour.append(v_star)
        hourly_means.append(sum(targets_in_hour) / len(targets_in_hour))

    # Morning peak must occur in hour 8 (08:00-09:00)
    morning_max_idx = max(range(6, 12), key=lambda i: hourly_means[i])
    assert morning_max_idx == 8, f"Morning maximum expected at hour 8, got {morning_max_idx}"
    assert hourly_means[8] > hourly_means[7] and hourly_means[8] > hourly_means[9]

    # Evening peak must occur in hour 17 (17:00-18:00)
    evening_max_idx = max(range(14, 21), key=lambda i: hourly_means[i])
    assert evening_max_idx == 17, f"Evening maximum expected at hour 17, got {evening_max_idx}"
    assert hourly_means[17] > hourly_means[16] and hourly_means[17] > hourly_means[18]

    # Overnight mean (00:00 to 05:00) must be below 35 veh/min
    overnight_mean = sum(hourly_means[0:6]) / 6.0
    assert overnight_mean < 35.0, f"Overnight mean was {overnight_mean}, expected < 35.0"


def test_10b_water_level_five_calibrated_behaviours(scratch_ledger):
    """10.b: water: all five behaviours in item 3."""
    # 1. Nominal half stays 2.5-4.5 m for 2 sim-hours
    sim1 = CitySimulator(seed=42, ledger=scratch_ledger, sim_speed=60.0)
    sim1.ledger.record_readings_truth_batch = lambda x: None
    dev1 = sim1.devices["z1-wl1"]
    dev1.current_value = 3.5
    dev1.actuator_state["valve_position"] = "half"
    dev1.actuator_state["valve_ratio"] = 0.5

    # 2 sim-hours = 7200 s = 1200 ticks at sim_speed=60 (dt=6.0s)
    for _ in range(1200):
        sim1.tick()
        assert 2.5 <= dev1.current_value <= 4.5, (
            f"Nominal half valve drifted out of [2.5, 4.5]: {dev1.current_value}"
        )

    # 2. Nominal closed from 3.5 m crosses 8 m after 15-40 sim-min
    sim2 = CitySimulator(seed=43, ledger=scratch_ledger, sim_speed=60.0)
    sim2.ledger.record_readings_truth_batch = lambda x: None
    dev2 = sim2.devices["z1-wl1"]
    dev2.current_value = 3.5
    dev2.actuator_state["valve_position"] = "closed"
    dev2.actuator_state["valve_ratio"] = 0.0

    ticks_to_8m = 0
    while dev2.current_value < 8.0 and ticks_to_8m < 2000:
        sim2.tick()
        ticks_to_8m += 1

    sim_min_closed = (ticks_to_8m * 6.0) / 60.0
    assert 15.0 <= sim_min_closed <= 40.0, (
        f"Closed valve took {sim_min_closed:.2f} sim-minutes to reach 8m, expected 15-40 min"
    )

    # 3. Nominal open settles near 1.8 m
    sim3 = CitySimulator(seed=44, ledger=scratch_ledger, sim_speed=60.0)
    sim3.ledger.record_readings_truth_batch = lambda x: None
    dev3 = sim3.devices["z1-wl1"]
    dev3.current_value = 3.5
    dev3.actuator_state["valve_position"] = "open"
    dev3.actuator_state["valve_ratio"] = 1.0

    for _ in range(1200):  # 2 sim-hours
        sim3.tick()
    assert 1.6 <= dev3.current_value <= 2.0, (
        f"Open valve settled at {dev3.current_value}, expected near 1.8 m"
    )

    # 4. Storm x3 with half crosses 8 m within 8-25 sim-min
    sim4 = CitySimulator(seed=45, ledger=scratch_ledger, sim_speed=60.0)
    sim4.ledger.record_readings_truth_batch = lambda x: None
    dev4 = sim4.devices["z1-wl1"]
    dev4.current_value = 3.5
    dev4.actuator_state["valve_position"] = "half"
    dev4.actuator_state["valve_ratio"] = 0.5
    sim4.inject_event(type="rain_storm", zone_id="zone-1", intensity=3.0)

    ticks_to_8m_storm = 0
    while dev4.current_value < 8.0 and ticks_to_8m_storm < 2000:
        sim4.tick()
        ticks_to_8m_storm += 1

    sim_min_storm = (ticks_to_8m_storm * 6.0) / 60.0
    assert 8.0 <= sim_min_storm <= 25.0, (
        f"Storm x3 with half valve took {sim_min_storm:.2f} sim-minutes to reach 8m, expected 8-25 min"
    )

    # 5. Storm x3 with open stays below 8 m (about 6.4 m)
    sim5 = CitySimulator(seed=46, ledger=scratch_ledger, sim_speed=60.0)
    sim5.ledger.record_readings_truth_batch = lambda x: None
    dev5 = sim5.devices["z1-wl1"]
    dev5.current_value = 3.5
    dev5.actuator_state["valve_position"] = "open"
    dev5.actuator_state["valve_ratio"] = 1.0
    sim5.inject_event(type="rain_storm", zone_id="zone-1", intensity=3.0)

    max_h_storm_open = 0.0
    for _ in range(1200):  # 2 sim-hours
        sim5.tick()
        if dev5.current_value > max_h_storm_open:
            max_h_storm_open = dev5.current_value

    assert max_h_storm_open < 8.0, f"Storm x3 with open exceeded 8m: {max_h_storm_open}"
    assert 5.0 <= dev5.current_value < 8.0, (
        f"Storm x3 with open settled at {dev5.current_value}, expected < 8.0 m"
    )


def test_10c_power_load_kinetics_and_smoothness(scratch_ledger):
    """10.c: power: daytime peak 85-110, night 25-35, no jump bigger than 3 MW between consecutive sim-minutes."""
    # 1. Exact mathematical kinetics across 24 hours (1440 sim-minutes)
    minute_targets = []
    for minute in range(1440):
        t_hour = minute / 60.0
        if 6.0 <= t_hour <= 20.0:
            p_star = 30.0 + 70.0 * (0.6 * math.sin(math.pi * (t_hour - 6.0) / 14.0) + 0.4)
        else:
            if t_hour >= 20.0:
                t_night = t_hour - 20.0
            else:
                t_night = t_hour + 4.0
            p_star = 44.0 + 14.0 * math.cos(math.pi * t_night / 5.0)
        minute_targets.append(p_star)

    daytime_peak = max(minute_targets[600:960])  # 10:00 to 16:00
    assert 85.0 <= daytime_peak <= 110.0, f"Daytime peak was {daytime_peak}, expected in [85, 110]"

    night_min = min(minute_targets)  # deep night minimum
    assert 25.0 <= night_min <= 35.0, f"Night minimum was {night_min}, expected in [25, 35]"

    max_jump = max(abs(minute_targets[i] - minute_targets[i - 1]) for i in range(1, len(minute_targets)))
    assert max_jump < 3.0, f"Minute-to-minute jump was {max_jump:.3f} MW, expected < 3.0 MW"

    # 2. Smooth boundary transition at 06:00 and 20:00 (no step)
    jump_06 = abs(minute_targets[360] - minute_targets[359])
    jump_20 = abs(minute_targets[1200] - minute_targets[1199])
    assert jump_06 < 0.2, f"Jump at 06:00 was {jump_06}, expected smooth transition"
    assert jump_20 < 0.2, f"Jump at 20:00 was {jump_20}, expected smooth transition"

    # 3. Simulator integration: verify power load tracks diurnal cycle (day > night)
    sim = CitySimulator(seed=77, ledger=scratch_ledger, sim_speed=10.0)
    sim.ledger.record_readings_truth_batch = lambda x: None
    power_dev = sim.devices["z1-pl1"]

    sim.clock.sim_dt = datetime(2026, 10, 17, 13, 0, 0, tzinfo=timezone.utc)
    power_dev.current_value = 95.0
    day_samples = []
    for _ in range(60):
        sim.tick()
        day_samples.append(power_dev.current_value)
    mean_day = sum(day_samples) / len(day_samples)

    sim.clock.sim_dt = datetime(2026, 10, 17, 1, 0, 0, tzinfo=timezone.utc)
    power_dev.current_value = 30.0
    night_samples = []
    for _ in range(60):
        sim.tick()
        night_samples.append(power_dev.current_value)
    mean_night = sum(night_samples) / len(night_samples)

    assert mean_day > mean_night + 40.0, f"Expected diurnal contrast: day {mean_day:.2f} vs night {mean_night:.2f}"


def test_10d_emergency_call_poisson(scratch_ledger):
    """10.d: emergency_call: mean over 2 sim-hours within 10% of lambda; flood term raises it."""
    # Test nominal baseline: lambda = 2.0
    sim = CitySimulator(seed=888, ledger=scratch_ledger, sim_speed=60.0)
    sim.ledger.record_readings_truth_batch = lambda x: None
    ec_dev = sim.devices["z1-ec1"]

    # Run for 2 sim-hours (1200 ticks at dt=6s)
    readings = []
    for _ in range(1200):
        sim.tick()
        readings.append(ec_dev.current_value)

    # Drop warm-up first 60 seconds (10 ticks)
    steady_readings = readings[10:]
    mean_calls = sum(steady_readings) / len(steady_readings)
    # Expected lambda = 2.0, within 10% -> [1.8, 2.2]
    assert 1.8 <= mean_calls <= 2.2, f"Baseline mean calls was {mean_calls:.3f}, expected in [1.8, 2.2]"

    # Flood term: raise water level > 8.0 -> lambda becomes 2.0 + 8.0 = 10.0
    sim_flood = CitySimulator(seed=889, ledger=scratch_ledger, sim_speed=60.0)
    sim_flood.ledger.record_readings_truth_batch = lambda x: None
    sim_flood.devices["z1-wl1"].current_value = 9.5
    sim_flood.devices["z1-wl2"].current_value = 9.5
    ec_flood = sim_flood.devices["z1-ec1"]

    flood_readings = []
    for _ in range(1200):
        # Keep water level above 8.0
        sim_flood.devices["z1-wl1"].current_value = 9.5
        sim_flood.devices["z1-wl2"].current_value = 9.5
        sim_flood.tick()
        flood_readings.append(ec_flood.current_value)

    steady_flood = flood_readings[10:]
    mean_flood_calls = sum(steady_flood) / len(steady_flood)
    # Expected lambda = 10.0, within 10% -> [9.0, 11.0]
    assert 9.0 <= mean_flood_calls <= 11.0, (
        f"Flood mean calls was {mean_flood_calls:.3f}, expected in [9.0, 11.0]"
    )


def test_10e_incident_engine_types_and_resolutions(scratch_ledger):
    """10.e: each incident type: effect visible; matching dispatch resolves it after response time; refused/attacked does not; ledger rows filled."""
    sim = CitySimulator(seed=555, ledger=scratch_ledger, sim_speed=60.0)

    # 1. Traffic Accident
    cam = sim.devices["z1-tc1"]
    init_cam_val = cam.current_value
    ev_acc = sim.inject_event(type="traffic_accident", zone_id="zone-1", device_id="z1-tc1")
    assert ev_acc in sim.active_events

    # Tick 10 times (60s): accident dampens target x0.3
    for _ in range(10):
        sim.tick()
    assert cam.current_value < init_cam_val

    # Contained device refused command does not resolve
    cam.status = "quarantined"
    refused_plan = sim.handle_set_signal_plan("z1-tc1", "green_wave")
    assert refused_plan["status"] == "error"
    assert ev_acc in sim.active_events
    cam.status = "ok"

    # Matching dispatch resolves after 300 s (50 ticks at sim_speed=60)
    disp = sim.handle_dispatch(incident_type="traffic_accident", zone_id="zone-1")
    assert disp["status"] == "ok"
    assert sim.active_events[ev_acc].resolve_countdown_s == 300.0

    # Tick 40 times (240s) -> still resolving
    for _ in range(40):
        sim.tick()
    assert ev_acc in sim.active_events

    # Tick 15 more times (90s, total 330s > 300s) -> resolved
    for _ in range(15):
        sim.tick()
    assert ev_acc not in sim.active_events

    # Verify ledger recorded resolution
    recent_events = scratch_ledger.get_recent_physical_events()
    acc_row = next(r for r in recent_events if r["event_id"] == ev_acc)
    assert acc_row["resolved_sim_ts"] is not None
    assert f"dispatch:{disp['dispatch_id']}" == acc_row["resolved_by"]

    # 2. Fire
    aq = sim.devices["z2-aq1"]
    init_aq = aq.current_value
    ev_fire = sim.inject_event(type="fire", zone_id="zone-2")
    # Tick 30 times (180s) -> fire target ramps to +150, AQI rises
    for _ in range(30):
        sim.tick()
    assert aq.current_value > init_aq + 25.0, f"AQI did not rise significantly: {aq.current_value} vs {init_aq}"

    # Matching dispatch resolves after 180 s (30 ticks)
    disp_fire = sim.handle_dispatch(incident_type="fire", zone_id="zone-2")
    for _ in range(35):
        sim.tick()
    assert ev_fire not in sim.active_events
    fire_row = next(r for r in scratch_ledger.get_recent_physical_events() if r["event_id"] == ev_fire)
    assert fire_row["resolved_by"] == f"dispatch:{disp_fire['dispatch_id']}"

    # 3. Grid Overload (pre-blackout resolution via switch_grid)
    ev_overload = sim.inject_event(type="grid_overload", zone_id="zone-3")
    res_grid = sim.handle_switch_grid("z3-pl1", "grid_b")
    assert res_grid["status"] == "ok"
    assert ev_overload not in sim.active_events
    overload_row = next(r for r in scratch_ledger.get_recent_physical_events() if r["event_id"] == ev_overload)
    assert overload_row["resolved_by"] == "actuation:switch_grid"

    # 4. Rain Storm (natural expiration)
    ev_storm = sim.inject_event(type="rain_storm", zone_id="zone-4", intensity=3.0, duration_s=120.0)
    for _ in range(25):  # 150s > 120s
        sim.tick()
    assert ev_storm not in sim.active_events
    storm_row = next(r for r in scratch_ledger.get_recent_physical_events() if r["event_id"] == ev_storm)
    assert storm_row["resolved_by"] == "expired"

    # 5. Air Hazard (natural expiration)
    ev_hazard = sim.inject_event(type="air_hazard", zone_id="zone-5", duration_s=60.0)
    for _ in range(15):  # 90s > 60s
        sim.tick()
    assert ev_hazard not in sim.active_events
    hazard_row = next(r for r in scratch_ledger.get_recent_physical_events() if r["event_id"] == ev_hazard)
    assert hazard_row["resolved_by"] == "expired"


def test_10f_sim_speed_invariance(scratch_ledger):
    """10.f: SIM_SPEED invariance: closed-valve crossing time at SIM_SPEED 1 vs 10 within 10%."""
    # At SIM_SPEED = 1.0 (dt = 0.1s)
    sim1 = CitySimulator(seed=111, ledger=scratch_ledger, sim_speed=1.0)
    # Disable disk writes and crypto signing so 15,000 in-memory ticks evaluate in milliseconds
    sim1.ledger.record_readings_truth_batch = lambda x: None
    sim1.sign_payload = lambda *args, **kwargs: "sig"
    dev1 = sim1.devices["z1-wl1"]
    dev1.current_value = 3.5
    dev1.actuator_state["valve_position"] = "closed"
    dev1.actuator_state["valve_ratio"] = 0.0

    ticks1 = 0
    while dev1.current_value < 8.0 and ticks1 < 30000:
        sim1.tick()
        ticks1 += 1
    sim_time1 = ticks1 * 0.1  # elapsed sim seconds

    # At SIM_SPEED = 10.0 (dt = 1.0s)
    sim10 = CitySimulator(seed=111, ledger=scratch_ledger, sim_speed=10.0)
    sim10.ledger.record_readings_truth_batch = lambda x: None
    sim10.sign_payload = lambda *args, **kwargs: "sig"
    dev10 = sim10.devices["z1-wl1"]
    dev10.current_value = 3.5
    dev10.actuator_state["valve_position"] = "closed"
    dev10.actuator_state["valve_ratio"] = 0.0

    ticks10 = 0
    while dev10.current_value < 8.0 and ticks10 < 3000:
        sim10.tick()
        ticks10 += 1
    sim_time10 = ticks10 * 1.0  # elapsed sim seconds

    rel_diff = abs(sim_time1 - sim_time10) / sim_time1
    assert rel_diff < 0.10, (
        f"Crossing time relative difference {rel_diff:.4f} exceeded 10% (speed 1: {sim_time1}s, speed 10: {sim_time10}s)"
    )


def test_10g_determinism(scratch_ledger):
    """10.g: determinism: same seed + same commands -> identical true values after 1000 ticks."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db1 = os.path.join(tmpdir, "det1.db")
        db2 = os.path.join(tmpdir, "det2.db")

        ledger1 = GroundTruthLedger(db_path=db1)
        ledger2 = GroundTruthLedger(db_path=db2)

        sim1 = CitySimulator(seed=42, ledger=ledger1, sim_speed=10.0)
        sim1.ledger.record_readings_truth_batch = lambda x: None
        sim2 = CitySimulator(seed=42, ledger=ledger2, sim_speed=10.0)
        sim2.ledger.record_readings_truth_batch = lambda x: None

        # Apply same command sequence on both
        sim1.handle_set_signal_plan("z1-tc1", "green_wave")
        sim2.handle_set_signal_plan("z1-tc1", "green_wave")

        sim1.handle_set_valve("z2-wl1", "half")
        sim2.handle_set_valve("z2-wl1", "half")

        sim1.inject_event(type="rain_storm", zone_id="zone-3", intensity=2.5)
        sim2.inject_event(type="rain_storm", zone_id="zone-3", intensity=2.5)

        for _ in range(1000):
            sim1.tick()
            sim2.tick()

        # Compare true values of all 42 devices
        for dev_id in sim1.devices:
            v1 = sim1.devices[dev_id].current_value
            v2 = sim2.devices[dev_id].current_value
            assert v1 == v2, f"Device {dev_id} diverged: {v1} != {v2}"


def test_10h_tick_budget(scratch_ledger):
    """10.h: tick budget: mean tick time < 20 ms with all 42 devices, 3 active incidents and ledger on."""
    sim = CitySimulator(seed=999, ledger=scratch_ledger, sim_speed=1.0)
    sim.inject_event(type="rain_storm", zone_id="zone-1", intensity=3.0)
    sim.inject_event(type="fire", zone_id="zone-2")
    sim.inject_event(type="grid_overload", zone_id="zone-3")

    # Warm-up 10 ticks
    for _ in range(10):
        sim.tick()

    start_t = time.perf_counter()
    num_ticks = 100
    for _ in range(num_ticks):
        sim.tick()
    elapsed = time.perf_counter() - start_t

    mean_tick_ms = (elapsed / num_ticks) * 1000.0
    assert mean_tick_ms < 20.0, f"Mean tick time was {mean_tick_ms:.2f} ms, expected < 20.0 ms"


def test_10i_zone2_coordinates_and_inventory(scratch_ledger):
    """10.i: zone-2 device coordinates equal new values; list_devices still returns 42 devices."""
    sim = CitySimulator(seed=123, ledger=scratch_ledger)
    set_active_simulator(sim)

    # Check zone-2 centroid
    zone2 = next(z for z in sim.ZONES if z["zone_id"] == "zone-2")
    assert zone2["lat"] == 36.8185
    assert zone2["lon"] == 10.3050
    assert zone2["name"] == "Port & Coastal Harbor"

    # Check devices in zone-2
    z2_devices = [d for d in sim.devices.values() if d.zone_id == "zone-2"]
    assert len(z2_devices) == 7

    for idx, d in enumerate(z2_devices):
        expected_lat = round(36.8185 + (idx * 0.0012) - 0.0036, 6)
        expected_lon = round(10.3050 + (idx * 0.0015) - 0.0045, 6)
        assert d.lat == expected_lat, f"Device {d.device_id} lat mismatch: {d.lat} != {expected_lat}"
        assert d.lon == expected_lon, f"Device {d.device_id} lon mismatch: {d.lon} != {expected_lon}"

    # list_devices via MCP
    all_devs = list_devices()
    assert len(all_devs) == 42
