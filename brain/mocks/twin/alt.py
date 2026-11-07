"""A different partner, same city: the Twin re-dressed in the alternative wire format.

`profiles/twin-alt-example.yaml` describes a partner that calls a tank a `flood_sensor`,
reports centimetres instead of metres, and names its fields `device_category` and `reading`.
Nothing about the city changes — only the words on the wire — so a run against this partner
must produce the same canonical decisions as a run against the standard one. That is what
the anti-corruption layer is for (spec 10.1), and `T13` is the proof.
"""

import json
from pathlib import Path
from typing import Any

from city_brain.edge.profiles import TwinProfile
from city_brain.edge.signatures import sign
from mocks.twin import MockTwin

ALT_PROFILE = Path(__file__).resolve().parents[2] / "profiles" / "twin-alt-example.yaml"
# The canonical kind each partner's own word maps to, so one can be rewritten as the other.
STANDARD_TO_ALT_TYPE = {
    "water_level": "flood_sensor",
    "traffic_camera": "traffic_camera",
    "power_load": "power_load",
    "air_quality": "air_quality",
    "emergency_call": "emergency_call",
}
STANDARD_TO_ALT_UNIT = {"meters": "cm"}
UNIT_SCALE = {"cm": 100.0}  # canonical metres * 100 = partner centimetres


class AltTwin:
    """Wraps a `MockTwin` and speaks the alternative partner's dialect."""

    def __init__(self, twin: MockTwin | None = None, profile: TwinProfile | None = None):
        self.twin = twin if twin is not None else MockTwin()
        self.profile = profile if profile is not None else TwinProfile.load(ALT_PROFILE)
        self._keys: dict[str, str] = {}

    # ----- the registry, in the partner's words -----

    def list_devices(self) -> list[dict[str, Any]]:
        out = []
        for device in self.twin.list_devices():
            unit = STANDARD_TO_ALT_UNIT.get(device["unit"], device["unit"])
            scale = UNIT_SCALE.get(unit, 1.0)
            out.append({
                **device,
                "device_type": STANDARD_TO_ALT_TYPE[device["device_type"]],
                "unit": unit,
                "min_val": device["min_val"] * scale,
                "max_val": device["max_val"] * scale,
            })
        return out

    def get_device_key(self, device_id: str) -> str | None:
        return self.twin.get_device_key(device_id)

    def get_city_state(self) -> dict[str, Any]:
        return self.twin.get_city_state()

    def handle_tool(self, tool: str, args: dict[str, Any]) -> dict[str, Any] | None:
        return self.twin.handle_tool(tool, args)

    # ----- readings, in the partner's words -----

    def tick(self) -> list[dict[str, Any]]:
        return [self.translate(reading) for reading in self.twin.tick()]

    def translate(self, reading: dict[str, Any]) -> dict[str, Any]:
        """One standard reading as the alternative partner would have sent it."""
        unit = STANDARD_TO_ALT_UNIT.get(reading["unit"], reading["unit"])
        scale = UNIT_SCALE.get(unit, 1.0)
        payload = {
            "reading_id": reading["reading_id"],
            "device_id": reading["device_id"],
            "device_category": STANDARD_TO_ALT_TYPE[reading["device_type"]],
            "reading": reading["value"] * scale,
            "unit": unit,
            "location": reading["location"],
            "ts": reading["ts"],
            "seq": reading["seq"],
            "status": reading["status"],
        }
        private_key = self._private_key(reading["device_id"])
        return {**payload, "sig": sign(payload, private_key)}

    def _private_key(self, device_id: str) -> str:
        return self.twin.devices[device_id].private_key

    def raw(self, reading: dict[str, Any]) -> bytes:
        return json.dumps(reading, separators=(",", ":")).encode("utf-8")
