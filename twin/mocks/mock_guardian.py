"""Mock Guardian (Layer 3) Client.

Assigned to: Agent_QA
Simulates Layer 3 Guardian:
1. Consumes sensor telemetry stream.
2. Queries MCP 'get_device_key' to fetch public Ed25519 signing keys.
3. Cryptographically validates signatures.
4. Quarantines suspect or rogue devices via MCP 'quarantine_device'.
5. Rolls back falsified readings via MCP 'rollback_reading'.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from twin.crypto_utils import verify_payload_signature
from twin.mcp_server import (
    get_device_key,
    quarantine_device,
    rollback_reading,
)

logger = logging.getLogger("mock_guardian")


class MockGuardian:
    """Mock Guardian security agent enforcing zero trust on Twin sensor streams."""

    def __init__(self):
        self.key_cache: Dict[str, str] = {}
        self.verified_readings_count = 0
        self.quarantined_devices: List[str] = []
        self.rolled_back_readings: List[str] = []
        self.signature_failures: List[Dict[str, Any]] = []

    def get_public_key(self, device_id: str) -> Optional[str]:
        """Fetch and cache public key via MCP 'get_device_key' tool."""
        if device_id not in self.key_cache:
            pub_key = get_device_key(device_id)
            if pub_key:
                self.key_cache[device_id] = pub_key
        return self.key_cache.get(device_id)

    def process_reading(self, payload_bytes: bytes) -> Dict[str, Any]:
        """Ingest single telemetry envelope, verify signature, and trigger containment if malicious."""
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception as e:
            return {"action": "rejected_malformed_json", "error": str(e)}

        device_id = payload.get("device_id")
        reading_id = payload.get("reading_id")
        sig = payload.get("sig")

        # 1. Check for unissued or rogue keys
        pub_key = self.get_public_key(device_id)
        if not pub_key:
            # Rogue device detected!
            self.quarantined_devices.append(device_id)
            quarantine_device(device_id)
            if reading_id:
                rollback_reading(reading_id)
                self.rolled_back_readings.append(reading_id)
            return {
                "action": "rogue_device_detected",
                "device_id": device_id,
                "reading_id": reading_id,
                "status": "quarantined",
            }

        # 2. Cryptographic Ed25519 signature verification
        is_valid = verify_payload_signature(payload, pub_key)
        if not is_valid:
            # Signature broken (link tampering or unauthorized payload alteration)
            self.signature_failures.append(payload)
            quarantine_device(device_id)
            self.quarantined_devices.append(device_id)
            if reading_id:
                rollback_reading(reading_id)
                self.rolled_back_readings.append(reading_id)
            return {
                "action": "signature_verification_failed",
                "device_id": device_id,
                "reading_id": reading_id,
                "status": "quarantined",
            }

        # 3. Payload signature is authentic
        self.verified_readings_count += 1
        return {
            "action": "verified_authentic",
            "device_id": device_id,
            "reading_id": reading_id,
            "status": "ok",
        }


if __name__ == "__main__":
    print("[MOCK GUARDIAN] Initializing Layer 3 Zero-Trust Security Sentinel...")
    guardian = MockGuardian()
    print("[MOCK GUARDIAN] Connected to MCP get_device_key and quarantine tools.")

    from twin.simulation import CitySimulator
    from twin.kafka_streamer import KafkaStreamer
    from twin.mcp_server import set_active_simulator

    sim = CitySimulator(seed=99)
    set_active_simulator(sim)
    streamer = KafkaStreamer(mock_mode=True)

    # 1. Authentic Reading
    readings = sim.tick()
    target_raw = readings[0]
    dev = sim.devices[target_raw["device_id"]]
    _, valid_bytes = streamer.stream_reading(target_raw, dev.private_key)
    res = guardian.process_reading(valid_bytes)
    print(f"\n[TEST 1: AUTHENTIC PAYLOAD] Result: {res}")

    # 2. Tampered Reading (Corrupt Signature)
    corrupted_dict = json.loads(valid_bytes.decode("utf-8"))
    corrupted_dict["sig"] = corrupted_dict["sig"][:-4] + "XXXX"
    corrupt_bytes = json.dumps(corrupted_dict).encode("utf-8")
    res = guardian.process_reading(corrupt_bytes)
    print(f"\n[TEST 2: CORRUPTED SIGNATURE] Result: {res}")

    # 3. Rogue Device
    rogue_payload = {
        "reading_id": "r-rg-001",
        "device_id": "rg-001",
        "device_type": "traffic_camera",
        "ts": sim.get_sim_time(),
        "seq": 1,
        "value": 99.0,
        "unit": "veh/min",
        "location": "zone-1",
        "status": "ok",
        "sig": "fake_sig",
    }
    res = guardian.process_reading(json.dumps(rogue_payload).encode("utf-8"))
    print(f"\n[TEST 3: ROGUE DEVICE] Result: {res}")

    print("\n[MOCK GUARDIAN] Zero-trust security sentinel demonstration complete.")
    print(f"Summary: Verified={guardian.verified_readings_count}, Quarantined={len(guardian.quarantined_devices)}, Rolled Back={len(guardian.rolled_back_readings)}")
