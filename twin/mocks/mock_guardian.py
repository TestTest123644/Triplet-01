"""Mock Guardian (Layer 3) Client.

Assigned to: Agent_QA
Simulates Layer 3 Guardian:
1. Consumes sensor telemetry stream via ReadingSource (Kafka or Queue).
2. Queries MCP 'get_device_key' to fetch public Ed25519 signing keys.
3. Cryptographically validates signatures using cryptography package.
4. Quarantines suspect/rogue devices via MCP 'quarantine_device'.
5. Isolates diverging or replay/delay devices via MCP 'isolate_device'.
6. Rolls back flagged readings via MCP 'rollback_reading'.

CRITICAL ARCHITECTURAL CONSTRAINT:
Strict external client: zero imports from Layer 1 internal packages.
Source of truth is INTERFACE.md and context.md.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import collections
from datetime import datetime, timezone
import json
import logging
import os
import statistics
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from cryptography.hazmat.primitives.asymmetric import ed25519

from mocks.mcp_client import McpCaller, run_sync
from mocks.reading_source import KafkaSource, QueueSource, ReadingSource

logger = logging.getLogger("mock_guardian")


def verify_twin_reading(payload: Dict[str, Any], pub_key_b64: str) -> bool:
    """Verify Ed25519 signature of payload as documented in INTERFACE.md."""
    sig_b64 = payload.get("sig")
    if not sig_b64:
        return False
    clean = {k: v for k, v in payload.items() if k != "sig"}
    canonical_bytes = json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")

    try:
        pub_bytes = base64.b64decode(pub_key_b64)
        public_key = ed25519.Ed25519PublicKey.from_public_bytes(pub_bytes)
        public_key.verify(base64.b64decode(sig_b64), canonical_bytes)
        return True
    except Exception:
        return False


def _parse_sim_seconds(ts_str: str) -> float:
    """Parse RFC 3339 UTC simulation timestamp into seconds."""
    s = ts_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).timestamp()


class MockGuardian:
    """Mock Guardian security agent enforcing zero trust on Twin sensor streams."""

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

        self.log_file = log_file or os.getenv("GUARDIAN_LOG_PATH")
        if self.log_file:
            os.makedirs(os.path.dirname(os.path.abspath(self.log_file)) or ".", exist_ok=True)

        self.key_cache: Dict[str, Optional[str]] = {}
        self.verified_readings_count = 0
        self.quarantined_devices: List[str] = []
        self.isolated_devices: List[str] = []
        self.rolled_back_readings: List[str] = []
        self.containment_sim_time: Dict[str, float] = {}
        self.last_mcp_check_sim_time: Dict[str, float] = {}
        self.already_rolled_back_ids: collections.OrderedDict[str, None] = collections.OrderedDict()

        # Tracking state for Guardian rules
        self.last_device_reading_id: Dict[str, str] = {}
        self.device_ts_history: Dict[str, collections.deque[float]] = collections.defaultdict(
            lambda: collections.deque(maxlen=20)
        )
        self.sig_failures: Dict[str, List[float]] = collections.defaultdict(list)
        self.replay_delay_events: Dict[str, List[float]] = collections.defaultdict(list)
        self.seen_reading_ids: collections.OrderedDict[str, None] = collections.OrderedDict()
        self.last_device_seq: Dict[str, int] = {}
        self.newest_stream_sim_sec: float = 0.0
        self.late_events: Dict[str, List[float]] = collections.defaultdict(list)

        # Water sensor discrepancy tracking (paired water sensors per zone)
        self.latest_water_readings: Dict[str, float] = {}
        self.water_histories: Dict[str, collections.deque[float]] = collections.defaultdict(
            lambda: collections.deque(maxlen=600)
        )
        self.water_divergence_counts: Dict[str, int] = collections.defaultdict(int)

        # Stuck sensor value tracking
        self.stuck_tracker: Dict[str, Tuple[float, int]] = {}

    def _log_event(self, sim_ts: str, event: str, details: Dict[str, Any]) -> None:
        """Log JSON line to stdout and log file."""
        line_data = {
            "wall_ts": datetime.now(timezone.utc).isoformat(),
            "sim_ts": sim_ts,
            "layer": "guardian",
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

    async def _safe_call_tool(self, tool_name: str, args: Dict[str, Any]) -> Tuple[bool, Any]:
        """Safely invoke MCP tool with exception handling. Never crashes loop."""
        try:
            res = await self.mcp.call_tool_async(tool_name, args)
            return True, res
        except Exception as e:
            logger.error(f"MCP tool call failed ({tool_name}, {args}): {e}")
            return False, None

    async def _rollback_reading_safe(self, reading_id: Optional[str]) -> bool:
        """Roll back a reading exactly once. Never sends duplicate rollback calls."""
        if not reading_id:
            return False
        if reading_id in self.already_rolled_back_ids:
            return False
        self.already_rolled_back_ids[reading_id] = None
        if len(self.already_rolled_back_ids) > 10000:
            self.already_rolled_back_ids.popitem(last=False)
        self.rolled_back_readings.append(reading_id)
        await self._safe_call_tool("rollback_reading", {"reading_id": reading_id})
        return True

    async def get_public_key(self, device_id: str) -> Tuple[bool, Optional[str]]:
        """Fetch and cache public key via MCP 'get_device_key'.

        Returns (is_known, public_key_b64).
        - Explicit null from server -> (False, None): device is unknown (rogue).
        - Transport error / exception -> (True, None): transient error, do not mark unknown.
        - Valid key -> (True, key_str): registered device.
        """
        if device_id in self.key_cache:
            cached = self.key_cache[device_id]
            return (False, None) if cached is None else (True, cached)

        success, res = await self._safe_call_tool("get_device_key", {"device_id": device_id})
        if not success:
            logger.warning(f"Transport error fetching key for {device_id}; skipping check for this tick")
            return (True, None)

        if isinstance(res, dict) and res.get("status") == "error":
            logger.warning(f"Error response from get_device_key for {device_id}: {res}")
            return (True, None)

        val = None
        if isinstance(res, dict):
            val = res.get("result")
        elif isinstance(res, str):
            val = res

        if val is None or val == "null" or val == '""':
            self.key_cache[device_id] = None
            return (False, None)

        key_str = str(val).strip().strip('"')
        if not key_str:
            self.key_cache[device_id] = None
            return (False, None)

        self.key_cache[device_id] = key_str
        return (True, key_str)

    async def process_reading_async(self, payload_input: Union[bytes, Dict[str, Any]]) -> Dict[str, Any]:
        """Ingest single telemetry reading, evaluate all Guardian rules, and actuate containment."""
        if isinstance(payload_input, bytes):
            try:
                payload = json.loads(payload_input.decode("utf-8"))
            except Exception as e:
                return {"action": "rejected_malformed_json", "error": str(e)}
        else:
            payload = payload_input

        device_id = str(payload.get("device_id", ""))
        reading_id = str(payload.get("reading_id", ""))
        device_type = str(payload.get("device_type", ""))
        val = float(payload.get("value", 0.0))
        seq = int(payload.get("seq", 0))
        sim_ts = str(payload.get("ts", ""))
        location = payload.get("location")
        zone_id = location if isinstance(location, str) else (location.get("zone_id") if isinstance(location, dict) else "zone-1")

        sim_sec = _parse_sim_seconds(sim_ts) if sim_ts else time.time()

        if device_id and reading_id:
            self.last_device_reading_id[device_id] = reading_id

        if sim_sec > self.newest_stream_sim_sec:
            self.newest_stream_sim_sec = sim_sec

        # Estimate device's ts_step from recent frames (ts span over last 20 frames / 19)
        def _calc_step(h: collections.deque[float]) -> float:
            if len(h) >= 20:
                return (h[-1] - h[0]) / 19.0
            if len(h) >= 2 and h[-1] > h[0]:
                return (h[-1] - h[0]) / (len(h) - 1)
            return 0.0

        ts_hist = self.device_ts_history[device_id]
        ts_step = _calc_step(ts_hist)
        if ts_step <= 0.0:
            other_steps = [_calc_step(h) for h in self.device_ts_history.values()]
            valid = [s for s in other_steps if s > 0.0]
            if valid:
                ts_step = statistics.median(valid)
            else:
                ts_step = 0.0

        late_threshold = max(2.0, 15.0 * ts_step)
        late_window = max(10.0, 5.0 * late_threshold)
        is_late = bool(len(ts_hist) >= 5 and ts_step > 0.0 and (self.newest_stream_sim_sec - sim_sec) > late_threshold)

        # Once a device is contained (isolated or quarantined), stop issuing new containment actions/rollbacks for it.
        # If a fresh, current (not replayed, not late) frame arrives with status == "ok", external restoration occurred: clear containment.
        status = str(payload.get("status", "ok"))
        last_seq = self.last_device_seq.get(device_id)
        is_replayed = (reading_id in self.seen_reading_ids) or (last_seq is not None and seq <= last_seq)

        if device_id in self.quarantined_devices or device_id in self.isolated_devices:
            cont_time = self.containment_sim_time.get(device_id, 0.0)
            if status == "ok" and not is_replayed and not is_late and sim_sec > cont_time + 5.0:
                last_check = self.last_mcp_check_sim_time.get(device_id, 0.0)
                if sim_sec - last_check >= 5.0:
                    self.last_mcp_check_sim_time[device_id] = sim_sec
                    success, dev_list = await self._safe_call_tool("list_devices", {})
                    if success:
                        if isinstance(dev_list, list):
                            items = dev_list
                        elif isinstance(dev_list, dict):
                            if "devices" in dev_list:
                                items = dev_list["devices"]
                            elif "device_id" in dev_list:
                                items = [dev_list]
                            else:
                                items = []
                        else:
                            items = []
                        twin_dev = next((d for d in items if isinstance(d, dict) and d.get("device_id") == device_id), None)
                        if twin_dev and twin_dev.get("status") == "ok":
                            if device_id in self.isolated_devices:
                                self.isolated_devices.remove(device_id)
                                self._log_event(sim_ts, "containment_cleared_externally", {"device_id": device_id, "type": "isolated"})
                            if device_id in self.quarantined_devices:
                                self.quarantined_devices.remove(device_id)
                                self._log_event(sim_ts, "containment_cleared_externally", {"device_id": device_id, "type": "quarantined"})
                            self.containment_sim_time.pop(device_id, None)
                            self.sig_failures[device_id].clear()
                            self.late_events[device_id].clear()
                            self.replay_delay_events[device_id].clear()
                            self.last_device_seq.pop(device_id, None)
                            return {
                                "action": "containment_cleared_externally",
                                "device_id": device_id,
                                "reading_id": reading_id,
                                "status": "ok",
                            }

            return {
                "action": "device_already_contained",
                "device_id": device_id,
                "reading_id": reading_id,
                "status": "quarantined" if device_id in self.quarantined_devices else "isolated",
            }

        # 1. Late-frame rule: frame ts is > late_threshold older than newest stream ts -> flag late; 3 in late_window -> isolate
        if len(ts_hist) >= 5 and ts_step > 0.0 and (self.newest_stream_sim_sec - sim_sec) > late_threshold:
            window = [t for t in self.late_events[device_id] if sim_sec - t <= late_window]
            window.append(sim_sec)
            self.late_events[device_id] = window

            if len(window) >= 3 and device_id not in self.isolated_devices:
                self.isolated_devices.append(device_id)
                self.containment_sim_time[device_id] = sim_sec
                await self._safe_call_tool("isolate_device", {"device_id": device_id})
                await self._rollback_reading_safe(reading_id)
                self._log_event(sim_ts, "isolate_device", {"device_id": device_id, "reason": f"3 late frames (>{late_threshold:.1f}s old) within {late_window:.1f} sim-s"})
                return {"action": "late_frame_detected", "device_id": device_id, "reading_id": reading_id, "status": "isolated"}

            await self._rollback_reading_safe(reading_id)
            return {"action": "late_frame_detected", "device_id": device_id, "reading_id": reading_id, "status": "flagged"}

        ts_hist.append(sim_sec)

        # 2. Unknown device_id (rogue device detection: only explicit null from get_device_key)
        is_known, pub_key = await self.get_public_key(device_id)
        if not is_known:
            if device_id not in self.quarantined_devices:
                self.quarantined_devices.append(device_id)
                self.containment_sim_time[device_id] = sim_sec
                await self._safe_call_tool("quarantine_device", {"device_id": device_id})
            await self._rollback_reading_safe(reading_id)
            self._log_event(sim_ts, "quarantine_device", {"device_id": device_id, "reason": "unknown device key"})
            return {"action": "rogue_device_detected", "device_id": device_id, "reading_id": reading_id, "status": "quarantined"}

        if not pub_key:
            # Transport error fetching key; skip signature check for this tick without quarantining registered device
            return {"action": "key_fetch_retry", "device_id": device_id, "reading_id": reading_id, "status": "pending"}

        # 3. Cryptographic Ed25519 signature verification
        is_valid = verify_twin_reading(payload, pub_key)
        if not is_valid:
            await self._rollback_reading_safe(reading_id)

            window = [t for t in self.sig_failures[device_id] if sim_sec - t <= late_window]
            window.append(sim_sec)
            self.sig_failures[device_id] = window

            if len(window) >= 3 and device_id not in self.quarantined_devices:
                self.quarantined_devices.append(device_id)
                self.containment_sim_time[device_id] = sim_sec
                await self._safe_call_tool("quarantine_device", {"device_id": device_id})
                self._log_event(sim_ts, "quarantine_device", {"device_id": device_id, "reason": f"3 signature failures within {late_window:.1f} sim-s"})

            return {"action": "signature_verification_failed", "device_id": device_id, "reading_id": reading_id, "status": "quarantined" if device_id in self.quarantined_devices else "flagged"}

        # 4. Duplicate reading_id or seq regression > 0 (replay / delay)
        last_seq = self.last_device_seq.get(device_id)
        is_replay_or_delay = (reading_id in self.seen_reading_ids) or (last_seq is not None and seq <= last_seq)
        if is_replay_or_delay:
            await self._rollback_reading_safe(reading_id)

            window = [t for t in self.replay_delay_events[device_id] if sim_sec - t <= late_window]
            window.append(sim_sec)
            self.replay_delay_events[device_id] = window

            if len(window) >= 3 and device_id not in self.isolated_devices:
                self.isolated_devices.append(device_id)
                self.containment_sim_time[device_id] = sim_sec
                await self._safe_call_tool("isolate_device", {"device_id": device_id})
                self._log_event(sim_ts, "isolate_device", {"device_id": device_id, "reason": f"3 replay/delay events within {late_window:.1f} sim-s"})

            return {"action": "replay_delay_detected", "device_id": device_id, "reading_id": reading_id, "status": "isolated" if device_id in self.isolated_devices else "flagged"}

        # Bounded LRU memory for seen reading ids (max 5,000)
        self.seen_reading_ids[reading_id] = None
        if len(self.seen_reading_ids) > 5000:
            self.seen_reading_ids.popitem(last=False)
        self.last_device_seq[device_id] = seq

        # 5. Paired water sensors (zN-wl1 vs zN-wl2) differing by > 2.0 m for 20 readings
        if device_type == "water_level":
            self.latest_water_readings[device_id] = val
            self.water_histories[device_id].append(val)

            # Check counterpart in same zone (only if neither is already isolated)
            prefix = device_id.split("-")[0]
            other_dev_id = f"{prefix}-wl2" if device_id.endswith("-wl1") else f"{prefix}-wl1"
            if (
                other_dev_id in self.latest_water_readings
                and device_id not in self.isolated_devices
                and other_dev_id not in self.isolated_devices
            ):
                other_val = self.latest_water_readings[other_dev_id]
                diff = abs(val - other_val)
                if diff > 2.0:
                    self.water_divergence_counts[zone_id] += 1
                    if self.water_divergence_counts[zone_id] >= 20:
                        h1 = list(self.water_histories[device_id])
                        h2 = list(self.water_histories[other_dev_id])

                        # Outlier is the sensor whose value jumped at divergence onset (~20 readings before divergence)
                        idx1 = max(0, len(h1) - 40) if len(h1) > 20 else 0
                        idx2 = max(0, len(h2) - 40) if len(h2) > 20 else 0

                        pre_val1 = h1[idx1] if h1 else val
                        pre_val2 = h2[idx2] if h2 else other_val

                        jump1 = abs(val - pre_val1)
                        jump2 = abs(other_val - pre_val2)

                        if abs(jump1 - jump2) > 0.1:
                            outlier_id = device_id if jump1 > jump2 else other_dev_id
                        else:
                            med1 = statistics.median(h1) if h1 else val
                            med2 = statistics.median(h2) if h2 else other_val
                            dev1 = abs(val - med1)
                            dev2 = abs(other_val - med2)
                            outlier_id = device_id if dev1 >= dev2 else other_dev_id

                        if outlier_id not in self.isolated_devices:
                            self.isolated_devices.append(outlier_id)
                            self.containment_sim_time[outlier_id] = sim_sec
                            await self._safe_call_tool(
                                "isolate_device",
                                {"device_id": outlier_id}
                            )
                            outlier_reading_id = self.last_device_reading_id.get(outlier_id)
                            await self._rollback_reading_safe(outlier_reading_id)
                            self._log_event(sim_ts, "isolate_device", {"device_id": outlier_id, "reason": "paired water divergence > 2.0m for 20 readings"})
                        self.water_divergence_counts[zone_id] = 0
                else:
                    self.water_divergence_counts[zone_id] = 0

        # 6. Stuck sensor: value constant to 2 decimals for 50 readings on a noisy type (water excluded)
        noisy_types = {"traffic_camera", "power_load", "air_quality"}
        if device_type in noisy_types:
            r_val = round(val, 2)
            if device_id in self.stuck_tracker:
                prev_val, count = self.stuck_tracker[device_id]
                if r_val == prev_val:
                    count += 1
                    self.stuck_tracker[device_id] = (r_val, count)
                    if count >= 50 and device_id not in self.isolated_devices:
                        self.isolated_devices.append(device_id)
                        self.containment_sim_time[device_id] = sim_sec
                        await self._safe_call_tool("isolate_device", {"device_id": device_id})
                        await self._rollback_reading_safe(reading_id)
                        self._log_event(sim_ts, "isolate_device", {"device_id": device_id, "reason": "sensor stuck at constant value for 50 readings"})
                else:
                    self.stuck_tracker[device_id] = (r_val, 1)
            else:
                self.stuck_tracker[device_id] = (r_val, 1)

        # 7. Compromised key detection (device_key): signatures verify, but severe physical anomaly (power > 140 MW)
        if device_type == "power_load" and val > 140.0:
            if device_id not in self.quarantined_devices:
                self.quarantined_devices.append(device_id)
                self.containment_sim_time[device_id] = sim_sec
                await self._safe_call_tool("quarantine_device", {"device_id": device_id})
            await self._rollback_reading_safe(reading_id)
            self._log_event(sim_ts, "quarantine_device", {"device_id": device_id, "reason": f"physical limit violation (power {val} MW > 140 MW), probable compromised key"})
            return {"action": "compromised_key_detected", "device_id": device_id, "reading_id": reading_id, "status": "quarantined"}

        self.verified_readings_count += 1
        return {"action": "verified_authentic", "device_id": device_id, "reading_id": reading_id, "status": "ok"}

    def process_reading(self, payload_input: Union[bytes, Dict[str, Any]]) -> Dict[str, Any]:
        """Synchronous wrapper for process_reading_async."""
        return run_sync(self.process_reading_async(payload_input))

    async def run_live(self) -> None:
        """Run continuous live consumption loop."""
        logger.info(f"[GUARDIAN] Running live monitoring. Writing to {self.log_file}")
        try:
            while True:
                reading = self.reading_source.get_reading(timeout=1.0)
                if reading is not None:
                    await self.process_reading_async(reading)
                else:
                    await asyncio.sleep(0.01)
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            logger.info("[GUARDIAN] Shutting down live sentinel.")
        finally:
            await self.mcp.close()
            self.reading_source.close()


def main():
    parser = argparse.ArgumentParser(description="Layer 3 Mock Guardian Client")
    parser.add_argument("--live", action="store_true", help="Run live monitoring loop")
    parser.add_argument("--mcp-url", default=os.getenv("TWIN_MCP_URL", "http://localhost:8000"), help="Twin MCP server URL")
    parser.add_argument("--mcp-token", default=os.getenv("MCP_AUTH_TOKEN"), help="Bearer token for MCP")
    parser.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"), help="Kafka bootstrap broker")
    parser.add_argument("--group-id", default=None, help="Kafka consumer group ID")
    parser.add_argument("--log-file", default=None, help="Path for JSON lines event log")
    args = parser.parse_args()

    gid = args.group_id or f"mock-guardian-{os.getpid()}-{int(time.time()*1000)}"
    source = KafkaSource(bootstrap_servers=args.kafka_bootstrap, group_id=gid)
    guardian = MockGuardian(
        reading_source=source,
        mcp_url=args.mcp_url,
        mcp_token=args.mcp_token,
        log_file=args.log_file,
    )
    if args.live:
        try:
            asyncio.run(guardian.run_live())
        except (KeyboardInterrupt, SystemExit):
            pass
    else:
        print("[MOCK GUARDIAN] Ready in passive test mode.")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
