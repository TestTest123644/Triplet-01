"""Tests for Phase 3: Stream correctness, wire limits, rogue streaming, and bounded memory."""

import asyncio
import collections
import json
import logging
import math
import time
import pytest
from starlette.testclient import TestClient

from twin.crypto_utils import generate_keypair, verify_payload_signature
from twin.kafka_streamer import KafkaStreamer, TwinSensorReading, serialize_reading
from twin.main import run_simulation_stream_loop
from twin.mcp_server import get_device_key, get_reading, get_readings, rollback_reading, set_active_simulator
from twin.red_team_api import build_red_team_app
from twin.simulation import CitySimulator
from twin.wire_limits import ROGUE_ID_MAX_LEN, SEQ_SAFE_MAX, WIRE_VALUE_MAX, WIRE_VALUE_MIN


@pytest.mark.asyncio
async def test_rogue_streaming_and_key_isolation(temp_ledger):
    """5.a: Rogue readings are streamed, signed with throwaway key, isolated from get_device_key."""
    sim = CitySimulator(seed=123, ledger=temp_ledger)
    set_active_simulator(sim)
    streamer = KafkaStreamer(mock_mode=True)

    atk_id = sim.inject_attack(
        capability="rogue",
        params={"rogue_device_id": "rg-01", "value": 142.5, "zone_id": "zone-1"},
    )
    assert atk_id.startswith("atk-")
    assert "rg-01" in sim._rogue_keys
    rogue_pub = sim._rogue_keys["rg-01"][1]

    # Run stream loop for ~20 ticks
    stop_event = asyncio.Event()
    loop_task = asyncio.create_task(
        run_simulation_stream_loop(sim, streamer, stop_event, target_hz=100.0)
    )
    await asyncio.sleep(0.25)
    stop_event.set()
    await loop_task

    # Rogue readings must be in sink_buffer
    assert len(streamer.sink_buffer) > 0
    rogue_payloads = [
        json.loads(b.decode("utf-8"))
        for b in streamer.sink_buffer
        if json.loads(b.decode("utf-8")).get("device_id") == "rg-01"
    ]
    assert len(rogue_payloads) >= 15

    sample = rogue_payloads[0]
    assert sample["reading_id"].startswith("r-rg-01-")
    assert sample["value"] == 142.5

    # Verifies with throwaway rogue public key
    assert verify_payload_signature(sample, rogue_pub) is True

    # Fails against every registered device public key
    for dev in sim.devices.values():
        assert verify_payload_signature(sample, dev.public_key) is False

    # get_device_key for rogue device MUST return None
    assert get_device_key("rg-01") is None


def test_bounded_reading_cache(temp_ledger):
    """5.b: Reading cache is bounded by READING_CACHE_MAX and evicts oldest entries."""
    sim = CitySimulator(seed=234, ledger=temp_ledger)
    set_active_simulator(sim)
    sim.reading_cache_max = 50

    # 42 devices per tick -> 2 ticks will exceed 50 entries
    readings_t1 = sim.tick()
    old_id = readings_t1[0]["reading_id"]

    for _ in range(100):
        sim.tick()

    assert len(sim.recent_readings_buffer) <= 50
    assert old_id not in sim.recent_readings_buffer

    # get_reading and rollback_reading on evicted id report cache window error
    res_get = get_reading(old_id)
    assert res_get is not None
    assert res_get.get("status") == "error"
    assert res_get.get("code") == "READING_NOT_FOUND"
    assert res_get.get("message") == f"Reading {old_id} not found in the reading cache: unknown id, or older than the last 50 readings"

    res_rb = rollback_reading(old_id)
    assert res_rb.get("status") == "error"
    assert res_rb.get("code") == "READING_NOT_FOUND"
    assert res_rb.get("message") == f"Reading {old_id} not found in the reading cache: unknown id, or older than the last 50 readings"


def test_get_readings_filtering_and_order(temp_ledger):
    """5.c: get_readings returns chronological order, respects limit and since_ts, caps at 500."""
    sim = CitySimulator(seed=345, ledger=temp_ledger)
    set_active_simulator(sim)

    for _ in range(15):
        sim.tick()

    # 1. Respects limit
    res_10 = get_readings(limit=10)
    assert len(res_10) == 10

    # 2. Chronological order (oldest first)
    seqs = [r["seq"] for r in res_10]
    assert seqs == sorted(seqs)

    # 3. Caps limit at 500
    res_huge = get_readings(limit=9999)
    assert len(res_huge) <= 500

    # 4. Filter by device_id
    dev_readings = get_readings(device_id="z1-tc1", limit=100)
    assert len(dev_readings) > 0
    assert all(r["device_id"] == "z1-tc1" for r in dev_readings)

    # 5. Filter by since_ts
    all_readings = list(sim.recent_readings_buffer.values())
    mid_ts = all_readings[len(all_readings) // 2]["ts"]
    filtered = get_readings(since_ts=mid_ts, limit=500)
    assert all(r["ts"] >= mid_ts for r in filtered)


def test_byte_budget_worst_case():
    """5.d: Byte budget <= 300 B for all device types, max seq, worst values."""
    priv, pub = generate_keypair()
    device_types = [
        ("emergency_call", "calls/hr"),
        ("traffic_camera", "veh/min"),
        ("water_level", "meters"),
        ("power_load", "MW"),
        ("air_quality", "AQI"),
    ]

    for dev_type, unit in device_types:
        for val in (WIRE_VALUE_MIN, WIRE_VALUE_MAX):
            raw = {
                "reading_id": f"r-z6-ec1-{SEQ_SAFE_MAX}",
                "device_id": "z6-ec1",
                "device_type": dev_type,
                "ts": "2026-10-17T12:00:00Z",
                "seq": SEQ_SAFE_MAX,
                "value": val,
                "unit": unit,
                "location": "zone-6",
                "status": "quarantined",
            }
            streamer = KafkaStreamer(mock_mode=True)
            reading = streamer.sign_and_build_reading(raw, priv)
            b = serialize_reading(reading)
            assert len(b) <= 300, f"{dev_type} at {val} exceeded 300 B: {len(b)} B"

    # Rogue 6-char id
    for val in (WIRE_VALUE_MIN, WIRE_VALUE_MAX):
        raw = {
            "reading_id": f"r-rg-999-{SEQ_SAFE_MAX}",
            "device_id": "rg-999",
            "device_type": "traffic_camera",
            "ts": "2026-10-17T12:00:00Z",
            "seq": SEQ_SAFE_MAX,
            "value": val,
            "unit": "veh/min",
            "location": "zone-1",
            "status": "quarantined",
        }
        streamer = KafkaStreamer(mock_mode=True)
        reading = streamer.sign_and_build_reading(raw, priv)
        b = serialize_reading(reading)
        assert len(b) <= 300, f"Rogue at {val} exceeded 300 B: {len(b)} B"


def test_wire_clamping_and_rejection_of_non_finite(temp_ledger):
    """5.e: Spoofed 1e9 clamped to WIRE_VALUE_MAX; NaN/inf params rejected."""
    sim = CitySimulator(seed=456, ledger=temp_ledger)
    set_active_simulator(sim)

    # 1. Clamping 1e9 to WIRE_VALUE_MAX
    sim.inject_attack(
        capability="device_physical",
        target_device_id="z1-tc1",
        params={"spoofed_value": 1e9},
    )
    readings = sim.tick()
    target = next(r for r in readings if r["device_id"] == "z1-tc1")
    assert target["value"] == WIRE_VALUE_MAX

    # get_device_state returns clamped value
    st = sim.get_device_state("z1-tc1")
    assert st["current_value"] == WIRE_VALUE_MAX

    # Ledger broadcast_value agrees
    truth = temp_ledger.get_reading_truth(target["reading_id"])
    assert truth["broadcast_value"] == WIRE_VALUE_MAX

    # 2. Rejection of NaN / inf at inject time
    with pytest.raises(ValueError, match="Non-finite"):
        sim.inject_attack(
            capability="device_physical",
            target_device_id="z1-tc1",
            params={"spoofed_value": float("nan")},
        )

    with pytest.raises(ValueError, match="Non-finite"):
        sim.inject_attack(
            capability="device_physical",
            target_device_id="z1-tc1",
            params={"spoofed_value": float("inf")},
        )

    # 3. Red Team API returns 400 for non-finite params
    token = "test-token"
    app = build_red_team_app(sim, token=token)
    client = TestClient(app)

    # Raw JSON payload with NaN (Python's json.loads parses this into float('nan'))
    raw_payload_nan = b'{"capability": "device_physical", "target_device_id": "z1-tc1", "params": {"spoofed_value": NaN}}'
    res_nan = client.post(
        "/api/attack/inject",
        content=raw_payload_nan,
        headers={"X-Red-Team-Token": token, "Content-Type": "application/json"},
    )
    assert res_nan.status_code == 400
    assert res_nan.json()["code"] == "INVALID_ATTACK"


def test_rogue_id_validation_rules(temp_ledger):
    """5.f: Rogue id 'rogue-x9' (too long) and 'z1-tc1' (collision) are rejected."""
    sim = CitySimulator(seed=567, ledger=temp_ledger)

    # 1. Too long (> 6 chars)
    with pytest.raises(ValueError, match="Invalid rogue_device_id"):
        sim.inject_attack(
            capability="rogue",
            params={"rogue_device_id": "rogue-x9"},
        )

    # 2. Collides with registered device_id
    with pytest.raises(ValueError, match="collides with registered"):
        sim.inject_attack(
            capability="rogue",
            params={"rogue_device_id": "z1-tc1"},
        )

    # 3. Valid 6-char id succeeds
    atk = sim.inject_attack(
        capability="rogue",
        params={"rogue_device_id": "rg-x9"},
    )
    assert atk.startswith("atk-")


def test_buffer_error_retry_and_deque_overflow():
    """5.g: Forced BufferError retries once and sinks; deque overflow increments dropped."""
    priv, _ = generate_keypair()
    streamer = KafkaStreamer(mock_mode=False)

    class MockProducer:
        def __init__(self, fail_times=1):
            self.fail_times = fail_times
            self.calls = 0
            self.poll_calls = []

        def produce(self, topic, key, value, on_delivery=None):
            self.calls += 1
            if self.calls <= self.fail_times:
                raise BufferError("Queue full")
            if on_delivery:
                on_delivery(None, None)

        def poll(self, timeout):
            self.poll_calls.append(timeout)

    # 1. Succeeds on retry
    streamer.producer = MockProducer(fail_times=1)
    raw = {
        "reading_id": "r-z1-tc1-1",
        "device_id": "z1-tc1",
        "device_type": "traffic_camera",
        "ts": "2026-10-17T12:00:00Z",
        "seq": 1,
        "value": 45.0,
        "unit": "veh/min",
        "location": "zone-1",
        "status": "ok",
    }
    reading, b = streamer.stream_reading(raw, priv)
    assert streamer.producer.calls == 2
    assert 0 in streamer.producer.poll_calls
    assert streamer.produced == 1
    assert streamer.sunk == 0

    # 2. Fails both times -> sinks it
    streamer.producer = MockProducer(fail_times=2)
    reading, b = streamer.stream_reading(raw, priv)
    assert streamer.producer.calls == 2
    assert streamer.sunk == 1

    # 3. Deque overflow
    streamer.sink_max = 5
    streamer.sink_buffer = collections.deque(maxlen=5)
    streamer.sunk = 0
    streamer.dropped = 0
    streamer.producer = None  # Force sink mode

    for i in range(10):
        raw["seq"] = i + 1
        streamer.stream_reading(raw, priv)

    assert len(streamer.sink_buffer) == 5
    assert streamer.sunk == 10
    assert streamer.dropped == 5

    # Stats validation
    st = streamer.stats()
    assert st["sunk"] == 10
    assert st["dropped"] == 5
    assert st["rejected_oversize"] == 0
    assert st["rejected_invalid"] == 0


def test_kafka_outage_non_blocking_performance(caplog):
    """3.1 Item 1: Kafka queue full does not stall the loop (< 0.1 s for 42 msgs, <= 1 log warning)."""
    priv, _ = generate_keypair()
    # Producer pointing to unreachable address with queue size 100
    streamer = KafkaStreamer(
        bootstrap_servers="127.0.0.1:9",
        producer_config={"queue.buffering.max.messages": 100},
    )
    raw = {
        "reading_id": "r-z1-tc1-1",
        "device_id": "z1-tc1",
        "device_type": "traffic_camera",
        "ts": "2026-10-17T12:00:00Z",
        "seq": 1,
        "value": 45.0,
        "unit": "veh/min",
        "location": "zone-1",
        "status": "ok",
    }

    # Fill the 100-message queue until buffer error occurs
    for i in range(120):
        raw["seq"] = i + 1
        streamer.stream_reading(raw, priv)

    # Verify 42 stream_reading calls take < 0.1 s in total and emit at most 1 WARNING+ log record
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        start = time.monotonic()
        for i in range(42):
            raw["seq"] = 200 + i
            streamer.stream_reading(raw, priv)
        elapsed = time.monotonic() - start

    assert elapsed < 0.1, f"42 stream_reading calls took {elapsed:.4f} s (expected < 0.1 s)"
    warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warning_records) <= 1, f"Expected <= 1 warning log, got {len(warning_records)}: {[r.message for r in warning_records]}"

    st = streamer.stats()
    assert st["kafka_state"] == "degraded"


def test_rogue_reading_id_continuity_across_attack_clears(temp_ledger):
    """3.1 Item 2: Re-injecting rogue device continues sequence counter without reuse."""
    sim = CitySimulator(seed=111, ledger=temp_ledger)

    # Inject rogue "rg-x9" and tick 3 times
    atk1 = sim.inject_attack(
        capability="rogue",
        params={"rogue_device_id": "rg-x9", "value": 100.0},
    )
    r1 = sim.tick()
    r2 = sim.tick()
    r3 = sim.tick()

    rg1 = next(r for r in r1 if r["device_id"] == "rg-x9")
    rg2 = next(r for r in r2 if r["device_id"] == "rg-x9")
    rg3 = next(r for r in r3 if r["device_id"] == "rg-x9")
    assert rg1["reading_id"] == "r-rg-x9-1"
    assert rg2["reading_id"] == "r-rg-x9-2"
    assert rg3["reading_id"] == "r-rg-x9-3"

    # Clear attack
    sim.clear_attack(atk1)

    # Re-inject same rogue id
    atk2 = sim.inject_attack(
        capability="rogue",
        params={"rogue_device_id": "rg-x9", "value": 200.0},
    )
    r4 = sim.tick()
    rg4 = next(r for r in r4 if r["device_id"] == "rg-x9")
    assert rg4["reading_id"] == "r-rg-x9-4"
    assert rg4["seq"] == 4


def test_duplicate_active_rogue_id_rejected(temp_ledger):
    """3.1 Item 3: Injecting duplicate active rogue id rejected with ValueError (400 INVALID_ATTACK)."""
    sim = CitySimulator(seed=222, ledger=temp_ledger)

    # Inject first active rogue attack
    atk = sim.inject_attack(
        capability="rogue",
        params={"rogue_device_id": "rg-x9", "value": 100.0},
    )
    assert atk.startswith("atk-")

    # Second injection with same active rogue id must be rejected
    with pytest.raises(ValueError, match="already active"):
        sim.inject_attack(
            capability="rogue",
            params={"rogue_device_id": "rg-x9", "value": 200.0},
        )

    # Test via Red Team HTTP API
    token = "test-token"
    app = build_red_team_app(sim, token=token)
    client = TestClient(app)

    res = client.post(
        "/api/attack/inject",
        json={
            "capability": "rogue",
            "params": {"rogue_device_id": "rg-x9", "value": 300.0},
        },
        headers={"X-Red-Team-Token": token},
    )
    assert res.status_code == 400
    assert res.json()["code"] == "INVALID_ATTACK"

    # After clearing the attack, injecting the rogue id is accepted again
    sim.clear_attack(atk)
    atk3 = sim.inject_attack(
        capability="rogue",
        params={"rogue_device_id": "rg-x9", "value": 400.0},
    )
    assert atk3.startswith("atk-")


def test_get_readings_since_ts_link_replay_no_truncation(temp_ledger):
    """3.1 Item 4: get_readings(since_ts) skips replayed readings instead of truncating."""
    sim = CitySimulator(seed=333, ledger=temp_ledger)
    set_active_simulator(sim)

    # Initial tick
    sim.tick()

    # Inject link replay attack on z1-tc1 before since_ts
    sim.inject_attack(
        capability="link",
        target_device_id="z1-tc1",
        params={"link_mode": "replay"},
    )
    for _ in range(5):
        sim.tick()

    since_ts = sim.get_sim_time()

    # Run 20 ticks: 42 readings per tick = 840 readings total
    # 41 devices emit fresh readings (ts >= since_ts); 1 device (z1-tc1) emits replayed readings (ts < since_ts)
    for _ in range(20):
        sim.tick()

    res = get_readings(since_ts=since_ts, limit=500)
    # Must NOT truncate to 41 readings
    assert len(res) == 500
    assert all(r["ts"] >= since_ts for r in res)


def test_error_messages_for_missing_readings(temp_ledger):
    """3.1 Item 5: get_reading and rollback_reading return standardized READING_NOT_FOUND error."""
    sim = CitySimulator(seed=444, ledger=temp_ledger)
    set_active_simulator(sim)

    missing_id = "r-unknown-999"
    expected = {
        "status": "error",
        "code": "READING_NOT_FOUND",
        "message": f"Reading {missing_id} not found in the reading cache: unknown id, or older than the last {sim.reading_cache_max} readings",
    }

    res_get = get_reading(missing_id)
    assert res_get == expected

    res_rb = rollback_reading(missing_id)
    assert res_rb == expected
