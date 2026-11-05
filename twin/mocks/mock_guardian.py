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

        # Tracking state for Guardian rules
        self.sig_failures: Dict[str, List[float]] = collections.defaultdict(list)
        self.replay_delay_events: Dict[str, List[float]] = collections.defaultdict(list)
        self.seen_reading_ids: set[str] = set()
        self.last_device_seq: Dict[str, int] = {}

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

    async def get_public_key(self, device_id: str) -> Optional[str]:
        """Fetch and cache public key via MCP 'get_device_key' tool."""
        if device_id not in self.key_cache:
            res = await self.mcp.call_tool_async("get_device_key", {"device_id": device_id})
            if isinstance(res, dict):
                val = res.get("result")
                if isinstance(val, str) and val.strip():
                    self.key_cache[device_id] = val.strip().strip('"')
                else:
                    self.key_cache[device_id] = None
            elif isinstance(res, str) and res.strip():
                self.key_cache[device_id] = res.strip().strip('"')
            else:
                self.key_cache[device_id] = None
        return self.key_cache.get(device_id)

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

        # 1. Unknown device_id (rogue device detection)
        pub_key = await self.get_public_key(device_id)
        if not pub_key:
            if device_id not in self.quarantined_devices:
                self.quarantined_devices.append(device_id)
                await self.mcp.call_tool_async("quarantine_device", {"device_id": device_id})
            if reading_id:
                self.rolled_back_readings.append(reading_id)
                await self.mcp.call_tool_async("rollback_reading", {"reading_id": reading_id})
            self._log_event(sim_ts, "quarantine_device", {"device_id": device_id, "reason": "unknown device key"})
            return {"action": "rogue_device_detected", "device_id": device_id, "reading_id": reading_id, "status": "quarantined"}

        # 2. Cryptographic Ed25519 signature verification
        is_valid = verify_twin_reading(payload, pub_key)
        if not is_valid:
            # Flag failure & rollback
            if reading_id:
                self.rolled_back_readings.append(reading_id)
                await self.mcp.call_tool_async("rollback_reading", {"reading_id": reading_id})

            # Track failures within 10 sim-s window
            window = [t for t in self.sig_failures[device_id] if sim_sec - t <= 10.0]
            window.append(sim_sec)
            self.sig_failures[device_id] = window

            if len(window) >= 3 and device_id not in self.quarantined_devices:
                self.quarantined_devices.append(device_id)
                await self.mcp.call_tool_async("quarantine_device", {"device_id": device_id})
                self._log_event(sim_ts, "quarantine_device", {"device_id": device_id, "reason": "3 signature failures within 10 sim-s"})

            return {"action": "signature_verification_failed", "device_id": device_id, "reading_id": reading_id, "status": "quarantined" if device_id in self.quarantined_devices else "flagged"}

        # 3. Duplicate reading_id or seq regression > 0 (replay / delay)
        last_seq = self.last_device_seq.get(device_id)
        is_replay_or_delay = (reading_id in self.seen_reading_ids) or (last_seq is not None and seq <= last_seq)
        if is_replay_or_delay:
            if reading_id:
                self.rolled_back_readings.append(reading_id)
                await self.mcp.call_tool_async("rollback_reading", {"reading_id": reading_id})

            window = [t for t in self.replay_delay_events[device_id] if sim_sec - t <= 10.0]
            window.append(sim_sec)
            self.replay_delay_events[device_id] = window

            if len(window) >= 3 and device_id not in self.isolated_devices:
                self.isolated_devices.append(device_id)
                await self.mcp.call_tool_async("isolate_device", {"device_id": device_id})
                self._log_event(sim_ts, "isolate_device", {"device_id": device_id, "reason": "3 replay/delay events within 10 sim-s"})

            return {"action": "replay_delay_detected", "device_id": device_id, "reading_id": reading_id, "status": "isolated" if device_id in self.isolated_devices else "flagged"}

        self.seen_reading_ids.add(reading_id)
        self.last_device_seq[device_id] = seq

        # 4. Paired water sensors (zN-wl1 vs zN-wl2) differing by > 2.0 m for 20 readings
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
                        # Determine outlier: further from its own 60s median
                        hist1 = self.water_histories[device_id]
                        hist2 = self.water_histories[other_dev_id]
                        med1 = statistics.median(hist1) if hist1 else val
                        med2 = statistics.median(hist2) if hist2 else other_val
                        dev1 = abs(val - med1)
                        dev2 = abs(other_val - med2)
                        outlier_id = device_id if dev1 >= dev2 else other_dev_id

                        if outlier_id not in self.isolated_devices:
                            self.isolated_devices.append(outlier_id)
                            await self.mcp.call_tool_async(
                                "isolate_device",
                                {"device_id": outlier_id}
                            )
                            if reading_id:
                                self.rolled_back_readings.append(reading_id)
                                await self.mcp.call_tool_async("rollback_reading", {"reading_id": reading_id})
                            self._log_event(sim_ts, "isolate_device", {"device_id": outlier_id, "reason": "paired water divergence > 2.0m for 20 readings"})
                        self.water_divergence_counts[zone_id] = 0
                else:
                    self.water_divergence_counts[zone_id] = 0

        # 5. Value constant to 2 decimals for 50 readings on a noisy type
        noisy_types = {"traffic_camera", "water_level", "power_load", "air_quality"}
        if device_type in noisy_types:
            r_val = round(val, 2)
            if device_id in self.stuck_tracker:
                prev_val, count = self.stuck_tracker[device_id]
                if r_val == prev_val:
                    count += 1
                    self.stuck_tracker[device_id] = (r_val, count)
                    if count >= 50 and device_id not in self.isolated_devices:
                        self.isolated_devices.append(device_id)
                        await self.mcp.call_tool_async("isolate_device", {"device_id": device_id})
                        if reading_id:
                            self.rolled_back_readings.append(reading_id)
                            await self.mcp.call_tool_async("rollback_reading", {"reading_id": reading_id})
                        self._log_event(sim_ts, "isolate_device", {"device_id": device_id, "reason": "sensor stuck at constant value for 50 readings"})
                else:
                    self.stuck_tracker[device_id] = (r_val, 1)
            else:
                self.stuck_tracker[device_id] = (r_val, 1)

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
        except (KeyboardInterrupt, SystemExit):
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
    parser.add_argument("--log-file", default=None, help="Path for JSON lines event log")
    args = parser.parse_args()

    source = KafkaSource(bootstrap_servers=args.kafka_bootstrap, group_id="mock-guardian-group")
    guardian = MockGuardian(
        reading_source=source,
        mcp_url=args.mcp_url,
        mcp_token=args.mcp_token,
        log_file=args.log_file,
    )
    if args.live:
        import asyncio
        asyncio.run(guardian.run_live())
    else:
        print("[MOCK GUARDIAN] Ready in passive test mode.")


if __name__ == "__main__":
    main()
