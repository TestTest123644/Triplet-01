"""Mock Twin Layer Service.

Simulates the Digital Twin layer (Layer 1):
1. Manages virtual IoT devices, physical limits, and containment status.
2. Generates realistic sensor telemetry batches for normal and anomaly scenarios.
3. Exposes MCP command tools for containment (Guardian) and actuation (Brain).
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

# Ensure mocks/twin is importable
twin_dir = str(Path(__file__).parent)
if twin_dir not in sys.path:
    sys.path.insert(0, twin_dir)

import mcp_server  # noqa: E402
from city_simulation import MockCitySimulator  # noqa: E402

from guardian_layer.utils.schema import FlushedBatch, Metadata, Sensor  # noqa: E402


class MockTwinService:
    """Mock Digital Twin service providing simulated IoT devices, broker emission,
    telemetry-confirmation subscriber, and MCP tool execution.
    """

    def __init__(self, seed: int = 42):
        self.simulator = MockCitySimulator(seed=seed)
        mcp_server.set_active_simulator(self.simulator)
        self.device_trust_status: dict[str, str] = {}
        self.confirmation_history: list[dict[str, Any]] = []
        self.sent_telemetry_history: list[dict[str, Any]] = []

    def call_tool(self, name: str, args: dict[str, Any]) -> Any:
        """ToolClient interface used by Guardian's ActionExecutor and Brain."""
        func = getattr(mcp_server, name, None)
        if func is None or not callable(func):
            raise AttributeError(f"Twin MCP server has no tool {name}")
        return func(**args)

    # -------------------------------------------------------------------------
    # Twin Broker Communication (Producer & Confirmation Listener)
    # -------------------------------------------------------------------------

    def send_telemetry_to_broker(
        self,
        batch: FlushedBatch,
        producer: Any = None,
        topic: str = "telemetry-validated",
    ) -> int:
        """Send generated telemetry batch to Kafka message broker topic."""
        sent_count = 0
        for r in batch.readings:
            payload = r.model_dump_json() if hasattr(r, "model_dump_json") else str(r)
            if producer is not None:
                import json

                data_bytes = (
                    payload.encode("utf-8")
                    if isinstance(payload, str)
                    else json.dumps(payload).encode("utf-8")
                )
                producer.produce(
                    topic=topic,
                    key=r.sensor_id.encode("utf-8"),
                    value=data_bytes,
                )
            sent_count += 1

        self.sent_telemetry_history.append(
            {
                "batch_id": batch.id,
                "sensor_id": batch.sensor_id,
                "count": sent_count,
                "topic": topic,
                "timestamp": datetime.now(UTC),
            }
        )
        if producer is not None and hasattr(producer, "poll"):
            producer.poll(0)
        return sent_count

    def on_telemetry_confirmation(self, confirmation: dict[str, Any]) -> str:
        """Twin listens on broker topic 'telemetry-confirmation'.

        Returns 'trusted message' if trusted or 'not trusted' if rejected.
        """
        device_id = confirmation.get("device_id", "unknown")
        is_trusted = bool(
            confirmation.get("trusted", confirmation.get("state") == "TRUSTED")
        )
        self.confirmation_history.append(confirmation)
        if is_trusted:
            self.device_trust_status[device_id] = "trusted"
            return "trusted message"
        else:
            self.device_trust_status[device_id] = "not trusted"
            return "not trusted"

    # -------------------------------------------------------------------------
    # Twin State & Containment Tools
    # -------------------------------------------------------------------------

    def get_device_state(self, device_id: str) -> dict[str, Any] | None:
        """Fetch current operational state and containment status from Twin."""
        return mcp_server.get_device_state(device_id)

    def isolate_device(self, device_id: str) -> dict[str, Any]:
        """Isolate device from actuation and mark telemetry status as 'isolated'."""
        return mcp_server.isolate_device(device_id)

    def quarantine_device(self, device_id: str) -> dict[str, Any]:
        """Quarantine device and mark status as 'quarantined'."""
        return mcp_server.quarantine_device(device_id)

    def reinstate_device(self, device_id: str) -> dict[str, Any]:
        """Reinstate device back to operational status 'ok'."""
        return mcp_server.reinstate_device(device_id)

    def rollback_reading(self, reading_id: str) -> dict[str, Any]:
        """Roll back and flag a falsified reading as 'rolled_back'."""
        return mcp_server.rollback_reading(reading_id)

    def set_valve(self, device_id: str, target_state: str) -> dict[str, Any]:
        """Actuate flood-control drainage valve (e.g. 'open', 'closed')."""
        return mcp_server.set_valve(device_id, target_state)

    def switch_grid(self, device_id: str, target_state: str) -> dict[str, Any]:
        """Modulate electrical power grid feeder or load shedding state."""
        return mcp_server.switch_grid(device_id, target_state)

    def set_signal_plan(self, device_id: str, target_state: str) -> dict[str, Any]:
        """Actuate traffic signal timing plan (e.g. 'green_wave', 'normal')."""
        return mcp_server.set_signal_plan(device_id, target_state)

    def dispatch(self, incident_type: str, zone_id: str) -> dict[str, Any]:
        """Dispatch municipal emergency response or technician units to a zone."""
        return mcp_server.dispatch(incident_type, zone_id)

    # -------------------------------------------------------------------------
    # Scenario Telemetry Batch Generators
    # -------------------------------------------------------------------------

    def generate_normal_batch(
        self,
        batch_id: str = "batch-normal",
        sensor_id: str = "z1-wl1",
        count: int = 100,
    ) -> FlushedBatch:
        """Scenario 1: Clean, nominal telemetry operating within normal bounds."""
        now = datetime.now(UTC)
        readings = [
            Sensor(
                sensor_id=sensor_id,
                cluster_id="water_level",
                timestamp=now - timedelta(seconds=count - i),
                value=3.2 + 0.1 * np.sin(i / 10.0),
                unit="m",
                status="ok",
                metadata=Metadata(
                    lat=36.8,
                    lon=10.18,
                    packet_order=i + 1,
                    type="water_level",
                    lower_range=0.0,
                    upper_range=12.0,
                    zone_id="zone-1",
                ),
            )
            for i in range(count)
        ]
        return FlushedBatch(
            id=batch_id,
            sensor_id=sensor_id,
            readings=tuple(readings),
            flushed_at=now,
        )

    def generate_out_of_range_batch(
        self,
        batch_id: str = "batch-oor",
        sensor_id: str = "z1-wl1",
        count: int = 100,
    ) -> FlushedBatch:
        """Scenario 2: Value breaches physical limits (e.g. 15.5m > 12.0m)."""
        now = datetime.now(UTC)
        readings = [
            Sensor(
                sensor_id=sensor_id,
                cluster_id="water_level",
                timestamp=now - timedelta(seconds=count - i),
                value=15.5 if i >= count - 10 else 3.2,
                unit="m",
                status="ok",
                metadata=Metadata(
                    lat=36.8,
                    lon=10.18,
                    packet_order=i + 1,
                    type="water_level",
                    lower_range=0.0,
                    upper_range=12.0,
                    zone_id="zone-1",
                ),
            )
            for i in range(count)
        ]
        return FlushedBatch(
            id=batch_id,
            sensor_id=sensor_id,
            readings=tuple(readings),
            flushed_at=now,
        )

    def generate_stale_down_batch(
        self,
        batch_id: str = "batch-down",
        sensor_id: str = "z2-wl2",
        count: int = 20,
    ) -> FlushedBatch:
        """Scenario 3: Sensor ceased transmitting 120s ago (>60s silence)."""
        now = datetime.now(UTC)
        old_start = now - timedelta(seconds=120)
        readings = [
            Sensor(
                sensor_id=sensor_id,
                cluster_id="water_level",
                timestamp=old_start + timedelta(seconds=i),
                value=2.8,
                unit="m",
                status="ok",
                metadata=Metadata(
                    lat=36.8,
                    lon=10.18,
                    packet_order=i + 1,
                    type="water_level",
                    lower_range=0.0,
                    upper_range=12.0,
                    zone_id="zone-2",
                ),
            )
            for i in range(count)
        ]
        return FlushedBatch(
            id=batch_id,
            sensor_id=sensor_id,
            readings=tuple(readings),
            flushed_at=now,
        )

    def generate_spoofed_spatial_batch(
        self,
        batch_id: str = "batch-spoof",
        target_sensor_id: str = "z1-wl1",
        count: int = 20,
    ) -> FlushedBatch:
        """Scenario 4: 4 sensors in cluster; peers report 2.8m, target reports 8.5m."""
        now = datetime.now(UTC)
        readings: list[Sensor] = []
        peer_ids = ["z1-wl2", "z2-wl1", "z2-wl2"]
        for i in range(count):
            t = now - timedelta(seconds=count - i)
            # Peers
            for p_id in peer_ids:
                readings.append(
                    Sensor(
                        sensor_id=p_id,
                        cluster_id="water_level",
                        timestamp=t,
                        value=2.8,
                        unit="m",
                        status="ok",
                        metadata=Metadata(
                            lat=36.8,
                            lon=10.18,
                            packet_order=i + 1,
                            type="water_level",
                            lower_range=0.0,
                            upper_range=12.0,
                            zone_id="zone-1",
                        ),
                    )
                )
            # Spoofed sensor
            readings.append(
                Sensor(
                    sensor_id=target_sensor_id,
                    cluster_id="water_level",
                    timestamp=t,
                    value=8.5,
                    unit="m",
                    status="ok",
                    metadata=Metadata(
                        lat=36.8,
                        lon=10.18,
                        packet_order=i + 1,
                        type="water_level",
                        lower_range=0.0,
                        upper_range=12.0,
                        zone_id="zone-1",
                    ),
                )
            )
        return FlushedBatch(
            id=batch_id,
            sensor_id=target_sensor_id,
            readings=tuple(readings),
            flushed_at=now,
        )

    def generate_jitter_malfunction_batch(
        self,
        batch_id: str = "batch-malfunction",
        sensor_id: str = "z3-tc1",
        count: int = 100,
    ) -> FlushedBatch:
        """Scenario 5: Step pulses classified as sensor_malfunction."""
        now = datetime.now(UTC)
        readings = [
            Sensor(
                sensor_id=sensor_id,
                cluster_id="traffic_camera",
                timestamp=now - timedelta(seconds=count - i),
                value=100.0 if (i // 5) % 2 == 0 else 10.0,
                unit="veh/min",
                status="ok",
                metadata=Metadata(
                    lat=36.8,
                    lon=10.18,
                    packet_order=i + 1,
                    type="traffic_camera",
                    lower_range=0.0,
                    upper_range=200.0,
                    zone_id="zone-3",
                ),
            )
            for i in range(count)
        ]
        return FlushedBatch(
            id=batch_id,
            sensor_id=sensor_id,
            readings=tuple(readings),
            flushed_at=now,
        )

    def generate_corrupted_not_sensor_batch(
        self,
        batch_id: str = "batch-notsensor",
        sensor_id: str = "z4-tc1",
        count: int = 100,
    ) -> FlushedBatch:
        """Scenario 6: Non-physical synthetic flatline classified as not_sensor_data."""
        now = datetime.now(UTC)
        readings = [
            Sensor(
                sensor_id=sensor_id,
                cluster_id="traffic_camera",
                timestamp=now - timedelta(seconds=count - i),
                value=42.0 + (0.01 if i % 2 == 0 else -0.01),
                unit="veh/min",
                status="ok",
                metadata=Metadata(
                    lat=36.8,
                    lon=10.18,
                    packet_order=i + 1,
                    type="traffic_camera",
                    lower_range=0.0,
                    upper_range=200.0,
                    zone_id="zone-4",
                ),
            )
            for i in range(count)
        ]
        return FlushedBatch(
            id=batch_id,
            sensor_id=sensor_id,
            readings=tuple(readings),
            flushed_at=now,
        )
