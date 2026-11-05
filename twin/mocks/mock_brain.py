"""Mock City Brain (Layer 2) Client.

Assigned to: Agent_QA
Simulates Layer 2 City Brain:
1. Consumes sensor telemetry stream via ReadingSource (Kafka or Queue).
2. Evaluates operational thresholds with corroboration and hysteresis.
3. Invokes MCP synchronous actuation endpoints:
   - 'dispatch' on medical emergency or fire.
   - 'set_valve' on corroborated high water levels (>7.0m both rising).
   - 'switch_grid' on high electrical load (>108 MW shed, <90 MW grid_a).
4. Ignores isolated/quarantined devices.
5. Emits idempotency keys with every command.

CRITICAL ARCHITECTURAL CONSTRAINT:
Strict external client: zero imports from Layer 1 internal packages.
Source of truth is INTERFACE.md and context.md.
"""

from __future__ import annotations

import argparse
import collections
from datetime import datetime, timezone
import json
import logging
import os
import statistics
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from mocks.mcp_client import McpCaller, run_sync
from mocks.reading_source import KafkaSource, QueueSource, ReadingSource

logger = logging.getLogger("mock_brain")


class MockCityBrain:
    """Mock City Brain decision engine orchestrating smart city actuations via MCP."""

    def __init__(
        self,
        reading_source: Optional[ReadingSource] = None,
        mcp_caller: Optional[McpCaller] = None,
        mcp_url: str = "http://localhost:8000",
        mcp_token: Optional[str] = None,
        call_tool_fn: Optional[Any] = None,
        log_file: Optional[str] = None,
    ):
        self.reading_source = reading_source or QueueSource()
        if mcp_caller:
            self.mcp = mcp_caller
        else:
            self.mcp = McpCaller(base_url=mcp_url, token=mcp_token, call_tool_fn=call_tool_fn)

        self.log_file = log_file or os.getenv("BRAIN_LOG_PATH")
        if self.log_file:
            os.makedirs(os.path.dirname(os.path.abspath(self.log_file)) or ".", exist_ok=True)

        self.dispatches_sent = 0
        self.valve_commands_sent = 0
        self.signal_plans_sent = 0
        self.grid_switches_sent = 0
        self.actuations_log: List[Dict[str, Any]] = []

        # Operational hysteresis state
        self.water_levels: Dict[str, Tuple[float, float]] = {}  # dev_id -> (current, prev)
        self.valve_states: Dict[str, str] = {}  # zone_id -> "open" | "half"
        self.camera_histories: Dict[str, collections.deque[float]] = collections.defaultdict(
            lambda: collections.deque(maxlen=300)
        )
        self.latest_camera_vals: Dict[str, float] = {}  # zone_id -> latest val
        self.dispatched_medical: Set[str] = set()
        self.dispatched_fire: Set[str] = set()
        self.grid_states: Dict[str, str] = {}  # dev_id -> "shed" | "grid_a"

    def _log_event(self, sim_ts: str, event: str, details: Dict[str, Any]) -> None:
        """Log JSON line to stdout and log file."""
        line_data = {
            "wall_ts": datetime.now(timezone.utc).isoformat(),
            "sim_ts": sim_ts,
            "layer": "brain",
            "event": event,
            "details": details,
        }
        line_str = json.dumps(line_data)
        print(line_str, flush=True)
        if self.log_file:
            try:
                with open(self.log_file, "a", encoding="utf-8") as f:
                    f.write(line_str + "\n")
            except Exception as e:
                logger.error(f"Error writing to log file {self.log_file}: {e}")

    async def evaluate_reading_async(self, payload_input: Union[bytes, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Evaluate sensor reading and trigger MCP actuations enforcing corroboration and hysteresis."""
        if isinstance(payload_input, bytes):
            try:
                payload = json.loads(payload_input.decode("utf-8"))
            except Exception as e:
                return [{"type": "error", "error": f"Invalid JSON bytes: {e}"}]
        else:
            payload = payload_input

        actions_taken = []
        device_id = str(payload.get("device_id", ""))
        dtype = str(payload.get("device_type", ""))
        val = float(payload.get("value", 0.0))
        status = str(payload.get("status", "ok"))
        sim_ts = str(payload.get("ts", ""))
        location = payload.get("location")
        zone_id = location if isinstance(location, str) else (location.get("zone_id") if isinstance(location, dict) else "zone-1")

        # Ignore devices that are contained (isolated or quarantined)
        if status in ("isolated", "quarantined"):
            return actions_taken

        # 1. Water Flood Defense: Act only when BOTH zone water sensors are > 7.0 m and rising
        if dtype == "water_level":
            prev_val = self.water_levels.get(device_id, (val, val))[0]
            self.water_levels[device_id] = (val, prev_val)

            prefix = device_id.split("-")[0]
            wl1_id = f"{prefix}-wl1"
            wl2_id = f"{prefix}-wl2"
            if wl1_id in self.water_levels and wl2_id in self.water_levels:
                val1, prev1 = self.water_levels[wl1_id]
                val2, prev2 = self.water_levels[wl2_id]

                # Rising condition: current > 7.0 on both and rising
                both_high = val1 > 7.0 and val2 > 7.0
                rising = (val1 >= prev1) or (val2 >= prev2)
                current_valve = self.valve_states.get(zone_id, "half")

                if both_high and rising and current_valve != "open":
                    res1 = await self.mcp.call_tool_async(
                        "set_valve",
                        {"device_id": wl1_id, "target_state": "open", "idempotency_key": f"act-valve-{wl1_id}-open-{sim_ts}"}
                    )
                    res2 = await self.mcp.call_tool_async(
                        "set_valve",
                        {"device_id": wl2_id, "target_state": "open", "idempotency_key": f"act-valve-{wl2_id}-open-{sim_ts}"}
                    )
                    self.valve_states[zone_id] = "open"
                    self.valve_commands_sent += 2
                    act1 = {"type": "set_valve", "device_id": wl1_id, "target_state": "open", "result": res1}
                    act2 = {"type": "set_valve", "device_id": wl2_id, "target_state": "open", "result": res2}
                    self.actuations_log.extend([act1, act2])
                    actions_taken.extend([act1, act2])
                    self._log_event(sim_ts, "set_valve", {"zone_id": zone_id, "target_state": "open", "val1": val1, "val2": val2})

                elif val1 < 4.0 and val2 < 4.0 and current_valve == "open":
                    res1 = await self.mcp.call_tool_async(
                        "set_valve",
                        {"device_id": wl1_id, "target_state": "half", "idempotency_key": f"act-valve-{wl1_id}-half-{sim_ts}"}
                    )
                    res2 = await self.mcp.call_tool_async(
                        "set_valve",
                        {"device_id": wl2_id, "target_state": "half", "idempotency_key": f"act-valve-{wl2_id}-half-{sim_ts}"}
                    )
                    self.valve_states[zone_id] = "half"
                    self.valve_commands_sent += 2
                    act1 = {"type": "set_valve", "device_id": wl1_id, "target_state": "half", "result": res1}
                    act2 = {"type": "set_valve", "device_id": wl2_id, "target_state": "half", "result": res2}
                    self.actuations_log.extend([act1, act2])
                    actions_taken.extend([act1, act2])
                    self._log_event(sim_ts, "set_valve", {"zone_id": zone_id, "target_state": "half", "val1": val1, "val2": val2})

        # 2. Traffic Camera: Record rolling history
        elif dtype == "traffic_camera":
            self.camera_histories[device_id].append(val)
            self.latest_camera_vals[zone_id] = val

        # 3. Emergency Surge: emergency_call >= 6 together with a zone camera < 50% of its 5-min mean
        elif dtype == "emergency_call":
            if val >= 6.0:
                cam_hist = self.camera_histories.get(f"{zone_id}-tc1") or self.camera_histories.get(f"{zone_id}-tc2")
                cam_mean = statistics.mean(cam_hist) if cam_hist and len(cam_hist) >= 5 else 45.0
                cam_val = self.latest_camera_vals.get(zone_id, cam_mean)

                # Accident congestion check: camera flow dropped below 50% of recent mean (or zero baseline)
                if cam_val < 0.5 * cam_mean or len(cam_hist or []) < 5:
                    if zone_id not in self.dispatched_medical:
                        res = await self.mcp.call_tool_async(
                            "dispatch",
                            {"incident_type": "medical_emergency", "zone_id": zone_id, "idempotency_key": f"act-disp-med-{zone_id}-{sim_ts}"}
                        )
                        self.dispatched_medical.add(zone_id)
                        self.dispatches_sent += 1
                        act = {"type": "dispatch", "incident_type": "medical_emergency", "zone_id": zone_id, "result": res}
                        self.actuations_log.append(act)
                        actions_taken.append(act)
                        self._log_event(sim_ts, "dispatch", {"incident_type": "medical_emergency", "zone_id": zone_id})
            elif val < 3.0:
                self.dispatched_medical.discard(zone_id)

        # 4. Air Quality: AQI > 150 -> dispatch fire
        elif dtype == "air_quality":
            if val > 150.0:
                if zone_id not in self.dispatched_fire:
                    res = await self.mcp.call_tool_async(
                        "dispatch",
                        {"incident_type": "fire", "zone_id": zone_id, "idempotency_key": f"act-disp-fire-{zone_id}-{sim_ts}"}
                    )
                    self.dispatched_fire.add(zone_id)
                    self.dispatches_sent += 1
                    act = {"type": "dispatch", "incident_type": "fire", "zone_id": zone_id, "result": res}
                    self.actuations_log.append(act)
                    actions_taken.append(act)
                    self._log_event(sim_ts, "dispatch", {"incident_type": "fire", "zone_id": zone_id})
            elif val < 100.0:
                self.dispatched_fire.discard(zone_id)

        # 5. Power Load: power > 108 MW -> switch_grid shed, back to grid_a below 90
        elif dtype == "power_load":
            current_grid = self.grid_states.get(device_id, "grid_a")
            if val > 108.0 and current_grid != "shed":
                res = await self.mcp.call_tool_async(
                    "switch_grid",
                    {"device_id": device_id, "target_state": "shed", "idempotency_key": f"act-grid-shed-{device_id}-{sim_ts}"}
                )
                self.grid_states[device_id] = "shed"
                self.grid_switches_sent += 1
                act = {"type": "switch_grid", "device_id": device_id, "target_state": "shed", "result": res}
                self.actuations_log.append(act)
                actions_taken.append(act)
                self._log_event(sim_ts, "switch_grid", {"device_id": device_id, "target_state": "shed", "value": val})
            elif val < 90.0 and current_grid == "shed":
                res = await self.mcp.call_tool_async(
                    "switch_grid",
                    {"device_id": device_id, "target_state": "grid_a", "idempotency_key": f"act-grid-grida-{device_id}-{sim_ts}"}
                )
                self.grid_states[device_id] = "grid_a"
                self.grid_switches_sent += 1
                act = {"type": "switch_grid", "device_id": device_id, "target_state": "grid_a", "result": res}
                self.actuations_log.append(act)
                actions_taken.append(act)
                self._log_event(sim_ts, "switch_grid", {"device_id": device_id, "target_state": "grid_a", "value": val})

        return actions_taken

    def evaluate_reading(self, payload_input: Union[bytes, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Synchronous wrapper for evaluate_reading_async."""
        return run_sync(self.evaluate_reading_async(payload_input))

    async def run_live(self) -> None:
        """Run continuous live decision loop."""
        logger.info(f"[BRAIN] Running live decision engine. Writing to {self.log_file}")
        try:
            while True:
                reading = self.reading_source.get_reading(timeout=1.0)
                if reading is not None:
                    await self.evaluate_reading_async(reading)
                else:
                    await asyncio.sleep(0.01)
        except (KeyboardInterrupt, SystemExit):
            logger.info("[BRAIN] Shutting down live engine.")
        finally:
            await self.mcp.close()
            self.reading_source.close()


def main():
    parser = argparse.ArgumentParser(description="Layer 2 Mock City Brain Client")
    parser.add_argument("--live", action="store_true", help="Run live decision loop")
    parser.add_argument("--mcp-url", default=os.getenv("TWIN_MCP_URL", "http://localhost:8000"), help="Twin MCP server URL")
    parser.add_argument("--mcp-token", default=os.getenv("MCP_AUTH_TOKEN"), help="Bearer token for MCP")
    parser.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"), help="Kafka bootstrap broker")
    parser.add_argument("--log-file", default=None, help="Path for JSON lines event log")
    args = parser.parse_args()

    source = KafkaSource(bootstrap_servers=args.kafka_bootstrap, group_id="mock-brain-group")
    brain = MockCityBrain(
        reading_source=source,
        mcp_url=args.mcp_url,
        mcp_token=args.mcp_token,
        log_file=args.log_file,
    )
    if args.live:
        import asyncio
        asyncio.run(brain.run_live())
    else:
        print("[MOCK CITY BRAIN] Ready in passive test mode.")


if __name__ == "__main__":
    main()
