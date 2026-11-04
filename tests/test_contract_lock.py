"""Contract lock and schema conformance verification tests.

Ensures byte-for-byte contract immutability for Phase 0:
1. Validates MCP tools and TwinSensorReading JSON schema against contract_snapshot.json.
2. Asserts the exact set of 16 MCP tools.
3. Simulates 100 real ticks (4,200 readings) verifying ts and reading_id regex patterns.
4. Streams all 4,200 readings through KafkaStreamer(mock_mode=True) ensuring round-trip
   parse into TwinSensorReading and strict adherence to the <= 300-byte budget.
"""

import asyncio
import json
from pathlib import Path
import re
import pytest

from twin.db_ledger import GroundTruthLedger
from twin.kafka_streamer import KafkaStreamer, TwinSensorReading
from twin.mcp_server import mcp
from twin.simulation import CitySimulator


EXPECTED_TOOLS = [
    "dispatch",
    "get_city_state",
    "get_command_result",
    "get_device_key",
    "get_device_state",
    "get_reading",
    "get_readings",
    "isolate_device",
    "list_devices",
    "quarantine_device",
    "reinstate_device",
    "release_device",
    "rollback_reading",
    "set_signal_plan",
    "set_valve",
    "switch_grid",
]


def test_contract_snapshot_equality():
    """Regenerate MCP tools schema and Kafka schema, asserting equality with contract_snapshot.json."""
    snapshot_path = Path(__file__).parent / "contract_snapshot.json"
    assert snapshot_path.exists(), f"Snapshot file not found: {snapshot_path}"

    with open(snapshot_path, "r", encoding="utf-8") as f:
        snapshot = json.load(f)

    tools = {t.name: t.input_schema for t in asyncio.run(mcp.list_tools())}
    kafka_schema = TwinSensorReading.model_json_schema()

    assert tools == snapshot.get("tools")
    assert kafka_schema == snapshot.get("kafka_schema")


def test_mcp_tool_names_exact_sixteen():
    """Assert exactly the 16 expected MCP tools are registered."""
    tools = {t.name: t.input_schema for t in asyncio.run(mcp.list_tools())}
    assert sorted(tools.keys()) == sorted(EXPECTED_TOOLS)
    assert len(tools) == 16


def test_simulation_ticks_and_streaming_budget(tmp_path):
    """Run 100 real ticks with a temporary ledger, asserting regex patterns and <= 300 bytes streaming."""
    ts_regex = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
    reading_id_regex = re.compile(r"^r-[a-z0-9-]+-\d+$")

    ledger_path = str(tmp_path / "contract_lock_truth.db")
    ledger = GroundTruthLedger(db_path=ledger_path)
    sim = CitySimulator(seed=42, ledger=ledger)
    streamer = KafkaStreamer(mock_mode=True)

    reading_count = 0
    max_payload_bytes = 0

    for tick in range(100):
        readings = sim.tick()
        assert len(readings) == 42
        for raw in readings:
            reading_count += 1
            # 2c. Assert ts and reading_id formats
            assert ts_regex.match(raw["ts"]), f"Invalid ts format: {raw['ts']}"
            assert reading_id_regex.match(raw["reading_id"]), f"Invalid reading_id: {raw['reading_id']}"

            # 2d. Stream through KafkaStreamer(mock_mode=True)
            dev = sim.devices[raw["device_id"]]
            _, raw_bytes = streamer.stream_reading(raw, dev.private_key)

            # Assert <= 300 bytes
            assert len(raw_bytes) <= 300, f"Payload {len(raw_bytes)} bytes exceeds 300-byte budget"
            max_payload_bytes = max(max_payload_bytes, len(raw_bytes))

            # Assert bytes parse back into TwinSensorReading
            payload_dict = json.loads(raw_bytes.decode("utf-8"))
            parsed = TwinSensorReading(**payload_dict)
            assert parsed.reading_id == raw["reading_id"]
            assert parsed.device_id == raw["device_id"]
            assert parsed.status == raw["status"]

    assert reading_count == 4200
    assert max_payload_bytes <= 300

