"""Unit tests for Phase 5: Adversary Capability Model."""

import json
import os
import tempfile
import pytest

from twin.crypto_utils import verify_payload_signature
from twin.db_ledger import GroundTruthLedger
from twin.kafka_streamer import KafkaStreamer
from twin.simulation import CitySimulator


@pytest.fixture
def temp_ledger():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_adv_ledger.db")
        ledger = GroundTruthLedger(db_path=db_path)
        yield ledger


def test_device_physical_attack(temp_ledger):
    sim = CitySimulator(seed=111, ledger=temp_ledger)
    dev_id = "z1-tc1"

    # Inject device_physical attack
    atk_id = sim.inject_attack(
        capability="device_physical",
        target_device_id=dev_id,
        params={"spoofed_value": 185.5},
        description="Camera sensor lens spoofed",
    )
    assert atk_id.startswith("atk-")

    # Tick simulation
    broadcast_readings = sim.tick()
    target_reading = next(r for r in broadcast_readings if r["device_id"] == dev_id)
    assert target_reading["value"] == 185.5

    # Verify Ground Truth Ledger recorded BOTH true and broadcast values
    truth_reading = temp_ledger.get_reading_truth(target_reading["reading_id"])
    assert truth_reading is not None
    assert truth_reading["is_attacked"] == 1
    assert truth_reading["attack_type"] == "device_physical"
    assert truth_reading["broadcast_value"] == 185.5
    assert truth_reading["true_value"] != 185.5  # True physical state was preserved!


def test_device_key_attack(temp_ledger):
    sim = CitySimulator(seed=222, ledger=temp_ledger)
    streamer = KafkaStreamer(mock_mode=True)
    dev_id = "z1-pl1"

    sim.inject_attack(
        capability="device_key",
        target_device_id=dev_id,
        params={"spoofed_value": 115.0},
        description="Stolen key forged reading",
    )

    readings = sim.tick()
    target_reading = next(r for r in readings if r["device_id"] == dev_id)
    assert target_reading["value"] == 115.0

    # Key is valid, so signature must verify cleanly!
    dev = sim.devices[dev_id]
    reading_model, raw_bytes = streamer.stream_reading(target_reading, dev.private_key)
    payload_dict = json.loads(raw_bytes.decode("utf-8"))
    assert verify_payload_signature(payload_dict, dev.public_key) is True

    # But Truth Ledger shows it was attacked!
    truth = temp_ledger.get_reading_truth(target_reading["reading_id"])
    assert truth["is_attacked"] == 1
    assert truth["attack_type"] == "device_key"


def test_link_attack_corrupt_and_drop(temp_ledger):
    sim = CitySimulator(seed=333, ledger=temp_ledger)
    streamer = KafkaStreamer(mock_mode=True)
    dev_id = "z1-wl1"

    # 1. Test link corrupt signature (today's behavior under new name)
    sim.inject_attack(
        capability="link",
        target_device_id=dev_id,
        params={"link_mode": "corrupt_sig"},
    )
    readings = sim.tick()
    target = next(r for r in readings if r["device_id"] == dev_id)
    assert target.get("_corrupt_sig") is True
    dev = sim.devices[dev_id]
    reading_model, raw_bytes = streamer.stream_reading(target, dev.private_key)
    payload_dict = json.loads(raw_bytes.decode("utf-8"))
    assert verify_payload_signature(payload_dict, dev.public_key) is False

    # 1b. Test link corrupt bytes (in-transit value digit tampering)
    sim.clear_all_attacks()
    sim.inject_attack(
        capability="link",
        target_device_id=dev_id,
        params={"link_mode": "corrupt_bytes"},
    )
    readings_bytes = sim.tick()
    target_b = next(r for r in readings_bytes if r["device_id"] == dev_id)
    assert target_b.get("_corrupt_bytes") is True
    model_b, tampered_bytes = streamer.stream_reading(target_b, dev.private_key)
    dict_b = json.loads(tampered_bytes.decode("utf-8"))
    assert verify_payload_signature(dict_b, dev.public_key) is False

    # 2. Test link drop
    sim.clear_all_attacks()
    sim.inject_attack(
        capability="link",
        target_device_id=dev_id,
        params={"link_mode": "drop"},
    )
    readings_after_drop = sim.tick()
    # Device reading must be dropped from broadcast
    assert not any(r["device_id"] == dev_id for r in readings_after_drop)
    # But SQLite Truth Ledger MUST have logged the true physical state!
    truth_readings = temp_ledger.get_readings_truth(device_id=dev_id, limit=1)
    assert len(truth_readings) == 1
    assert truth_readings[0]["attack_type"] == "link"


def test_rogue_attack(temp_ledger):
    sim = CitySimulator(seed=444, ledger=temp_ledger)
    sim.inject_attack(
        capability="rogue",
        params={"rogue_device_id": "rg-x99", "value": 777.7},
    )

    readings = sim.tick()
    rogue_reading = next((r for r in readings if r["device_id"] == "rg-x99"), None)
    assert rogue_reading is not None
    assert rogue_reading["value"] == 777.7

    # Device does not exist in legitimate device inventory
    assert "rg-x99" not in sim.devices


def test_actuator_attack(temp_ledger):
    sim = CitySimulator(seed=555, ledger=temp_ledger)
    water_dev = "z1-wl1"

    # Inject actuator attack on water valve
    sim.inject_attack(
        capability="actuator",
        target_device_id=water_dev,
        description="Valve actuator compromised by adversary",
    )

    # Brain attempts to open valve
    init_ratio = sim.devices[water_dev].actuator_state.get("valve_ratio", 0.0)
    res = sim.handle_set_valve(water_dev, "open")

    # Command is acknowledged "ok" to deceive caller...
    assert res["status"] == "ok"

    # ...but internal physical state was NOT executed!
    assert sim.devices[water_dev].actuator_state.get("valve_ratio", 0.0) == init_ratio


def test_sensor_fault_attack(temp_ledger):
    sim = CitySimulator(seed=666, ledger=temp_ledger)
    dev_id = "z1-aq1"

    # 1. Stuck fault
    sim.inject_attack(
        capability="sensor_fault",
        target_device_id=dev_id,
        params={"fault_mode": "stuck", "stuck_value": 99.0},
    )
    for _ in range(3):
        readings = sim.tick()
        target = next(r for r in readings if r["device_id"] == dev_id)
        assert target["value"] == 99.0

    # 2. Drift fault
    sim.clear_all_attacks()
    sim.inject_attack(
        capability="sensor_fault",
        target_device_id=dev_id,
        params={"fault_mode": "drift", "drift_rate": 2.0},
    )
    drifted_vals = []
    for _ in range(5):
        readings = sim.tick()
        target = next(r for r in readings if r["device_id"] == dev_id)
        drifted_vals.append(target["value"])

    # Drifted values should be monotonically increasing due to drift rate
    assert drifted_vals[-1] > drifted_vals[0]
