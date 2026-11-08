"""Mock CitySimulator: static values, no physics, no crypto, no SQLite.

Drop-in replacement for the public API of the real CitySimulator, meant for
unit-testing Layers 2/3 without randomness, wall-clock pacing, or a ledger.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

ZONES = [f"zone-{i}" for i in range(1, 7)]

# suffix, type, unit, static value, min, max
TEMPLATES = [
    ("tc1", "traffic_camera", "veh/min", 45.0, 0.0, 200.0),
    ("tc2", "traffic_camera", "veh/min", 38.0, 0.0, 200.0),
    ("wl1", "water_level", "meters", 3.2, 0.0, 12.0),
    ("wl2", "water_level", "meters", 2.8, 0.0, 12.0),
    ("pl1", "power_load", "MW", 48.0, 5.0, 120.0),
    ("aq1", "air_quality", "AQI", 42.0, 10.0, 350.0),
    ("ec1", "emergency_call", "call/min", 1.0, 0.0, 30.0),
]

INITIAL_ACTUATORS = {
    "traffic_camera": {"signal_plan": "normal"},
    "water_level": {"valve_position": "closed", "valve_ratio": 0.0},
    "power_load": {"grid_state": "grid_a"},
}

VALID_CAPS = {
    "link",
    "rogue",
    "device_key",
    "device_physical",
    "actuator",
    "sensor_fault",
}
VALID_STATUSES = {"ok", "isolated", "quarantined", "rolled_back", "modelled"}


class MockCitySimulator:
    """42 devices (7 x 6 zones), constant readings, simple attack effects."""

    def __init__(self, seed: int = 42, ledger: Any = None):
        self.seed = seed
        self.ledger = ledger  # accepted for signature compatibility, unused
        self.sim_dt = datetime(2026, 10, 17, 12, 0, 0, tzinfo=UTC)
        self.tick_count = 0
        self.devices: dict[str, dict[str, Any]] = {}
        self.zone_dispatches: dict[str, list[dict[str, Any]]] = {z: [] for z in ZONES}
        self.command_results: dict[str, dict[str, Any]] = {}
        self.recent_readings_buffer: dict[str, dict[str, Any]] = {}
        self.active_attacks: dict[str, dict[str, Any]] = {}
        self.attack_log: list[dict[str, Any]] = []  # replaces the SQLite ledger

        for zone_id in ZONES:
            n = zone_id.split("-")[-1]
            for suffix, dtype, unit, val, lo, hi in TEMPLATES:
                did = f"z{n}-{suffix}"
                self.devices[did] = {
                    "device_id": did,
                    "zone_id": zone_id,
                    "device_type": dtype,
                    "unit": unit,
                    "lat": 36.8,
                    "lon": 10.18,
                    "min_val": lo,
                    "max_val": hi,
                    "status": "ok",
                    "seq": 0,
                    "current_value": val,
                    "actuator_state": dict(INITIAL_ACTUATORS.get(dtype, {})),
                    "public_key": f"mock-pub-{did}",
                }

    # ---- clock -----------------------------------------------------------
    def get_sim_time(self) -> str:
        return self.sim_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def pace_wall_clock(self) -> float:
        return 0.0  # no sleeping in the mock

    # ---- tick ------------------------------------------------------------
    def tick(self) -> list[dict[str, Any]]:
        ts = self.get_sim_time()
        out: list[dict[str, Any]] = []

        for dev in self.devices.values():
            dev["seq"] += 1
            rid = f"r-{dev['device_id']}-{dev['seq']}"
            value = dev["current_value"]
            reading = {
                "reading_id": rid,
                "device_id": dev["device_id"],
                "device_type": dev["device_type"],
                "ts": ts,
                "seq": dev["seq"],
                "value": value,
                "unit": dev["unit"],
                "location": dev["zone_id"],
                "status": dev["status"],
            }

            for atk in self._attacks_for(dev):
                cap, p = atk["capability"], atk["params"]
                if cap in ("device_physical", "device_key"):
                    reading["value"] = p.get("spoofed_value", value * 2.5)
                elif cap == "sensor_fault":
                    mode = p.get("fault_mode", "drift")
                    reading["value"] = (
                        p.get("stuck_value", 42.0)
                        if mode == "stuck"
                        else 0.0
                        if mode == "dropout"
                        else value + p.get("drift_rate", 0.5) * self.tick_count * 0.1
                    )
                elif cap == "link":
                    mode = p.get("link_mode", "corrupt_bytes")
                    if mode == "drop":
                        reading = None
                        break
                    if mode == "corrupt_bytes":
                        reading["_corrupt_sig"] = True

            if reading is not None:
                out.append(reading)
                self.recent_readings_buffer[rid] = reading

        for atk in self.active_attacks.values():
            if atk["capability"] == "rogue":
                p = atk["params"]
                rid = f"r-{p.get('rogue_device_id', 'rogue-01')}-{self.tick_count}"
                rogue = {
                    "reading_id": rid,
                    "device_id": p.get("rogue_device_id", "rogue-01"),
                    "device_type": "traffic_camera",
                    "ts": ts,
                    "seq": self.tick_count,
                    "value": float(p.get("value", 88.88)),
                    "unit": "veh/min",
                    "location": p.get("zone_id", "zone-1"),
                    "status": "ok",
                }
                out.append(rogue)
                self.recent_readings_buffer[rid] = rogue

        self.tick_count += 1
        self.sim_dt += timedelta(seconds=0.1)
        return out

    def _attacks_for(self, dev: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            a
            for a in self.active_attacks.values()
            if a["capability"] != "rogue"
            and (
                a["target_device_id"] == dev["device_id"]
                or (
                    a["target_device_id"] is None
                    and a["target_zone_id"] == dev["zone_id"]
                )
            )
        ]

    def _is_actuator_blocked(self, dev: dict[str, Any]) -> bool:
        return any(a["capability"] == "actuator" for a in self._attacks_for(dev))

    # ---- injection -------------------------------------------------------
    def inject_attack(
        self,
        capability: str,
        target_device_id: str | None = None,
        target_zone_id: str | None = None,
        params: dict[str, Any] | None = None,
        description: str | None = None,
    ) -> str:
        if capability not in VALID_CAPS:
            raise ValueError(
                f"Invalid adversary capability: {capability}."
                + f"Must be one of {VALID_CAPS}"
            )
        attack_id = f"atk-{uuid.uuid4().hex[:8]}"
        info = {
            "attack_id": attack_id,
            "capability": capability,
            "target_device_id": target_device_id,
            "target_zone_id": target_zone_id,
            "params": params or {},
            "start_sim_time": self.get_sim_time(),
            "end_sim_time": None,
            "active": True,
            "description": description
            or f"Injected adversary capability: {capability}",
        }
        self.active_attacks[attack_id] = info
        self.attack_log.append(info)
        return attack_id

    def clear_attack(self, attack_id: str) -> bool:
        info = self.active_attacks.pop(attack_id, None)
        if not info:
            return False
        info["active"] = False
        info["end_sim_time"] = self.get_sim_time()
        return True

    def clear_all_attacks(self) -> None:
        for aid in list(self.active_attacks):
            self.clear_attack(aid)

    # ---- queries ---------------------------------------------------------
    def list_devices_spec(self) -> list[dict[str, Any]]:
        keys = (
            "device_id",
            "zone_id",
            "device_type",
            "unit",
            "lat",
            "lon",
            "min_val",
            "max_val",
            "status",
            "actuator_state",
        )
        return [{k: d[k] for k in keys} for d in self.devices.values()]

    def get_device_public_key(self, device_id: str) -> str | None:
        d = self.devices.get(device_id)
        return d["public_key"] if d else None

    def get_device_state(self, device_id: str) -> dict[str, Any] | None:
        d = self.devices.get(device_id)
        if not d:
            return None
        keys = (
            "device_id",
            "zone_id",
            "device_type",
            "current_value",
            "unit",
            "seq",
            "status",
            "actuator_state",
        )
        return {**{k: d[k] for k in keys}, "ts": self.get_sim_time()}

    def get_city_state(self) -> dict[str, Any]:
        return {
            "sim_time": self.get_sim_time(),
            "total_devices": len(self.devices),
            "zones": {
                z: {
                    "name": z,
                    "active_dispatches": len(self.zone_dispatches[z]),
                    "devices": [
                        {
                            "device_id": d["device_id"],
                            "type": d["device_type"],
                            "value": d["current_value"],
                            "unit": d["unit"],
                            "status": d["status"],
                        }
                        for d in self.devices.values()
                        if d["zone_id"] == z
                    ],
                }
                for z in ZONES
            },
        }

    def get_cached_reading(self, reading_id: str) -> dict[str, Any] | None:
        return self.recent_readings_buffer.get(reading_id)

    # ---- actuation (Layer 2) --------------------------------------------
    def _store(self, key: str | None, result: dict[str, Any]) -> dict[str, Any]:
        if key:
            self.command_results[key] = result
        return result

    def _actuate(
        self, device_id: str, dtype: str, field: str, value: str, key: str | None
    ) -> dict[str, Any]:
        dev = self.devices.get(device_id)
        if not dev or dev["device_type"] != dtype:
            return self._store(
                key,
                {
                    "status": "error",
                    "message": f"Device {device_id} is not a valid {dtype}",
                },
            )
        if not self._is_actuator_blocked(dev):  # blocked = accepted but not executed
            dev["actuator_state"][field] = value
        return self._store(key, {"status": "ok", "device_id": device_id, field: value})

    def handle_dispatch(
        self, incident_type: str, zone_id: str, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        if zone_id not in self.zone_dispatches:
            return self._store(
                idempotency_key,
                {"status": "error", "message": f"Unknown zone_id: {zone_id}"},
            )
        dispatch_id = f"disp-{uuid.uuid4().hex[:8]}"
        blocked = any(
            a["capability"] == "actuator" and a["target_zone_id"] == zone_id
            for a in self.active_attacks.values()
        )
        if not blocked:
            self.zone_dispatches[zone_id].append(
                {
                    "dispatch_id": dispatch_id,
                    "incident_type": incident_type,
                    "zone_id": zone_id,
                    "dispatched_at": self.get_sim_time(),
                }
            )
        return self._store(
            idempotency_key,
            {"status": "ok", "dispatch_id": dispatch_id, "zone_id": zone_id},
        )

    def handle_set_signal_plan(self, device_id, target_state, idempotency_key=None):
        return self._actuate(
            device_id, "traffic_camera", "signal_plan", target_state, idempotency_key
        )

    def handle_set_valve(self, device_id, target_state, idempotency_key=None):
        res = self._actuate(
            device_id, "water_level", "valve_position", target_state, idempotency_key
        )
        dev = self.devices.get(device_id)
        if not dev:
            return res
        if res["status"] == "ok" and not self._is_actuator_blocked(dev):
            dev["actuator_state"]["valve_ratio"] = (
                1.0 if target_state == "open" else 0.0
            )
        return res

    def handle_switch_grid(self, device_id, target_state, idempotency_key=None):
        return self._actuate(
            device_id, "power_load", "grid_state", target_state, idempotency_key
        )

    def get_command_result(self, idempotency_key: str) -> dict[str, Any] | None:
        return self.command_results.get(idempotency_key)

    # ---- containment (Layer 3) ------------------------------------------
    def update_device_status(self, device_id: str, new_status: str) -> dict[str, Any]:
        if new_status not in VALID_STATUSES:
            return {"status": "error", "message": f"Invalid status: {new_status}"}
        dev = self.devices.get(device_id)
        if not dev:
            return {"status": "error", "message": f"Device {device_id} not found"}
        dev["status"] = new_status
        return {"status": "ok", "device_id": device_id, "new_status": new_status}

    def rollback_reading(self, reading_id: str) -> dict[str, Any]:
        reading = self.recent_readings_buffer.get(reading_id)
        if not reading:
            return {
                "status": "error",
                "message": f"Reading {reading_id} not found in buffer",
            }
        reading["status"] = "rolled_back"
        return {
            "status": "ok",
            "reading_id": reading_id,
            "reading_status": "rolled_back",
        }


if __name__ == "__main__":
    sim = MockCitySimulator()
    print(len(sim.devices), "devices;", len(sim.tick()), "readings on a clean tick")
    aid = sim.inject_attack(
        "device_key", target_device_id="z1-tc1", params={"spoofed_value": 999.0}
    )
    r = next(x for x in sim.tick() if x["device_id"] == "z1-tc1")
    print("attacked z1-tc1 value:", r["value"])
    sim.clear_attack(aid)
    r = next(x for x in sim.tick() if x["device_id"] == "z1-tc1")
    print("after clear:", r["value"])
