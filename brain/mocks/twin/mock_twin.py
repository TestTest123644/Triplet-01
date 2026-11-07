"""A Twin stand-in built from the partner profile: same wire format, replies, physics and defects.

Every Twin fact except the physics equations (mocks/twin/physics.py) is read from the profile.
Mirrors twin/simulation.py and twin/kafka_streamer.py; imports nothing from twin/.
"""

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from city_brain.edge.profiles import ProfileError, TwinProfile
from city_brain.edge.signatures import sign
from mocks.keys import derive_keypair
from mocks.twin import physics

_PROFILE = Path(__file__).resolve().parents[2] / "profiles" / "twin-triplet07.yaml"
_START = datetime(2026, 10, 17, 12, 0, 0, tzinfo=timezone.utc)
_CAPABILITIES = ("device_physical", "device_key", "sensor_fault", "link", "actuator")
# wire order of a reading (kafka_streamer.TwinSensorReading), as canonical field names
_WIRE_ORDER = ("reading_id", "device_id", "kind", "sim_time", "seq", "value", "unit", "zone_id", "device_status")
_CONTAINED = ("ISOLATED", "QUARANTINED")  # canonical statuses the Twin keeps actuating


@dataclass(frozen=True)
class MockDefects:
    """Real-Twin behaviours (spec 2.4); the defaults reproduce them, False fixes one."""

    accept_unknown_states: bool = True
    reexecute_repeated_keys: bool = True
    actuate_contained: bool = True


@dataclass
class MockDevice:
    device_id: str
    zone_id: str
    device_type: str
    unit: str
    lat: float
    lon: float
    min_val: float
    max_val: float
    private_key: str
    public_key: str
    value: float
    actuator_state: dict[str, Any] = field(default_factory=dict)
    status: str = "ok"
    seq: int = 0
    drift_offset: float = 0.0
    last_broadcast: dict[str, Any] | None = None
    active_attack: dict[str, Any] | None = None
    state_fields: dict[str, str] = field(default_factory=dict)  # capability -> actuator_state key

    @property
    def valve_ratio(self) -> float:
        return self.actuator_state.get(physics.VALVE_RATIO_KEY, 0.0)

    @property
    def signal_plan(self) -> str:
        return self.actuator_state.get(self.state_fields["signal"], "normal")

    @property
    def grid_state(self) -> str:
        return self.actuator_state.get(self.state_fields["grid"], "grid_a")


def _fmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class MockTwin:
    def __init__(self, profile: TwinProfile | None = None, seed: int = 42,
                 start: datetime = _START, defects: MockDefects = MockDefects()):
        self.profile = profile if profile is not None else TwinProfile.load(_PROFILE)
        self.seed = seed
        self.defects = defects
        self._start = start
        self.restarts = 0
        # canonical field -> partner wire name; the partner status that means OK
        self._wire = {canon: partner for partner, canon in self.profile.fields.items()}
        for canon in _WIRE_ORDER:
            if canon not in self._wire:
                raise ProfileError(f"fields map has no partner field for '{canon}'")
        self._status_ok = next(p for p, c in self.profile.statuses.items() if c == "OK")
        self._state_fields = {cap: spec["state_field"] for cap, spec in self.profile.actuators.items()
                              if "state_field" in spec}
        self._contained = {p for p, c in self.profile.statuses.items() if c in _CONTAINED}
        self._tools = {spec["tool"]: cap for cap, spec in self.profile.actuators.items()}
        self._reset()

    # ----- state -----

    def _reset(self) -> None:
        self.rng = random.Random(self.seed)
        self.sim_dt = self._start
        self.devices: dict[str, MockDevice] = {}
        self.zone_ids: list[str] = []
        self.zone_dispatches: dict[str, list[dict[str, Any]]] = {}
        self.command_results: dict[str, dict[str, Any]] = {}
        self.active_attacks: dict[str, dict[str, Any]] = {}
        self._attack_n = 0
        self._dispatch_n = 0
        inv = self.profile.inventory
        off = inv["device_offset"]
        for zone in range(1, int(inv["zones"]) + 1):
            zone_id = inv["zone_id"].format(zone=zone)
            self.zone_ids.append(zone_id)
            self.zone_dispatches[zone_id] = []
            coords = inv["zone_coords"][zone_id]
            for idx, row in enumerate(inv["devices"]):
                device_id = inv["device_id"].format(zone=zone, suffix=row["suffix"])
                priv, pub = derive_keypair(f"{self.seed}:{self.restarts}:{device_id}")
                self.devices[device_id] = MockDevice(
                    device_id=device_id, zone_id=zone_id, device_type=row["type"], unit=row["unit"],
                    lat=round(coords["lat"] + idx * off["lat_step"] + off["lat_base"], 6),
                    lon=round(coords["lon"] + idx * off["lon_step"] + off["lon_base"], 6),
                    min_val=float(row["min"]), max_val=float(row["max"]),
                    private_key=priv, public_key=pub, value=float(row["init"]),
                    actuator_state=dict(row.get("initial_actuator") or {}),
                    status=self._status_ok, state_fields=self._state_fields)

    def restart(self) -> None:
        """A Twin process restart: new keys, seq, clock, attacks and caches."""
        self.restarts += 1
        self._reset()

    def get_sim_time(self) -> str:
        return _fmt(self.sim_dt)

    # ----- tick -----

    def _channel(self, dev: MockDevice) -> str:
        return physics.CHANNELS[self.profile.kinds[dev.device_type]]

    def _physics(self) -> list[tuple[MockDevice, float]]:
        d_traffic, d_power, t_norm = physics.diurnal(self.sim_dt)
        out = []
        for dev in self.devices.values():
            channel, v = self._channel(dev), dev.value
            if channel == "traffic":
                new = physics.traffic(v, dev.signal_plan, d_traffic, self.rng)
            elif channel == "water":
                new = physics.water(v, dev.valve_ratio, t_norm, self.rng)
            elif channel == "power":
                new = physics.power(v, dev.grid_state, d_power, self.rng)
            elif channel == "air_quality":
                zone = [d.value for d in self.devices.values()
                        if d.zone_id == dev.zone_id and self._channel(d) == "traffic"]
                new = physics.air_quality(v, zone, self.rng)
            else:
                new = physics.emergency_calls(v, len(self.zone_dispatches.get(dev.zone_id, [])), self.rng)
            dev.value = round(max(dev.min_val, min(dev.max_val, new)), 4)
            dev.seq += 1
            out.append((dev, dev.value))
        return out

    def _signed(self, dev: MockDevice, ts: str, value: float, corrupt: bool) -> dict[str, Any]:
        canon = {"reading_id": f"r-{dev.device_id}-{dev.seq}", "device_id": dev.device_id,
                 "kind": dev.device_type, "sim_time": ts, "seq": dev.seq, "value": round(float(value), 2),
                 "unit": dev.unit, "zone_id": dev.zone_id, "device_status": dev.status}
        wire = {self._wire[c]: canon[c] for c in _WIRE_ORDER}
        sig = sign(wire, dev.private_key)
        if corrupt:  # kafka_streamer: link-level byte mutation of the signature tail
            sig = sig[:-4] + ("AAAA" if sig.endswith("==") else "BB==")
        wire["sig"] = sig
        return wire

    def tick(self) -> list[dict[str, Any]]:
        ts = self.get_sim_time()
        results = self._physics()
        out: list[dict[str, Any]] = []
        for dev, true_val in results:
            value = true_val
            corrupt = drop = replay = False
            attack = dev.active_attack
            if attack:
                cap, params = attack["capability"], attack["params"]
                if cap == "device_physical":
                    value = params.get("spoofed_value", round(true_val * 2.5, 2))
                elif cap == "device_key":
                    value = params.get("spoofed_value", round(true_val + 50.0, 2))
                elif cap == "sensor_fault":
                    mode = params.get("fault_mode", "drift")
                    if mode == "stuck":
                        value = params.get("stuck_value", 42.0)
                    elif mode == "dropout":
                        value = 0.0
                    else:
                        dev.drift_offset += params.get("drift_rate", 0.5) * physics.DT
                        value = round(true_val + dev.drift_offset, 2)
                elif cap == "link":
                    mode = params.get("link_mode", "corrupt_bytes")
                    drop = mode == "drop"
                    corrupt = mode == "corrupt_bytes"
                    replay = mode == "replay" and dev.last_broadcast is not None
            if drop:
                continue
            wire = dict(dev.last_broadcast) if replay else self._signed(dev, ts, value, corrupt)
            out.append(wire)
            dev.last_broadcast = wire
        self.sim_dt += timedelta(seconds=physics.DT)
        return out

    # ----- queries -----

    def list_devices(self) -> list[dict[str, Any]]:
        return [{"device_id": d.device_id, "zone_id": d.zone_id, "device_type": d.device_type,
                 "unit": d.unit, "lat": d.lat, "lon": d.lon, "min_val": d.min_val, "max_val": d.max_val,
                 "status": d.status, "actuator_state": dict(d.actuator_state)}
                for d in self.devices.values()]

    def get_device_key(self, device_id: str) -> str | None:
        dev = self.devices.get(device_id)
        return dev.public_key if dev else None

    def get_city_state(self) -> dict[str, Any]:
        return {
            "sim_time": self.get_sim_time(),
            "total_devices": len(self.devices),
            "zones": {
                z: {"active_dispatches": len(self.zone_dispatches[z]),
                    "devices": [{"device_id": d.device_id, "type": d.device_type, "value": d.value,
                                 "unit": d.unit, "status": d.status}
                                for d in self.devices.values() if d.zone_id == z]}
                for z in self.zone_ids},
        }

    # ----- commands -----

    def handle_tool(self, tool: str, args: dict[str, Any]) -> dict[str, Any] | None:
        if tool == "get_command_result":
            return self.command_results.get(args["idempotency_key"])
        cap = self._tools.get(tool)
        if cap is None:
            return {"status": "error", "message": f"Unknown tool: {tool}"}
        key = args.get("idempotency_key")
        if key and not self.defects.reexecute_repeated_keys and key in self.command_results:
            return self.command_results[key]
        spec = self.profile.actuators[cap]
        if spec["target"] == "zone":
            result = self._dispatch(args["zone_id"], args["incident_type"])
        else:
            result = self._set_state(cap, spec, args["device_id"], args["target_state"])
        if key:
            self.command_results[key] = result
        return result

    def _set_state(self, cap: str, spec: dict[str, Any], device_id: str, target: str) -> dict[str, Any]:
        dev = self.devices.get(device_id)
        if not dev or dev.device_type != spec["device_type"]:
            return {"status": "error", "message": f"Device {device_id} is not a valid {spec['device_type']}"}
        if not self.defects.actuate_contained and dev.status in self._contained:
            return {"status": "error", "message": f"Device {device_id} is {dev.status}"}
        if not self.defects.accept_unknown_states and not self.profile.accepts_partner_state(cap, target):
            return {"status": "error", "message": f"Invalid target_state for {cap}: {target}"}
        blocked = dev.active_attack is not None and dev.active_attack["capability"] == "actuator"
        if not blocked:  # actuator attack: replies ok, executes nothing
            dev.actuator_state[spec["state_field"]] = target
            if "ratio" in spec["states"]:
                dev.actuator_state[physics.VALVE_RATIO_KEY] = physics.valve_ratio(target)
        return {"status": "ok", "device_id": device_id, spec["state_field"]: target}

    def _dispatch(self, zone_id: str, incident_type: str) -> dict[str, Any]:
        if zone_id not in self.zone_dispatches:
            return {"status": "error", "message": f"Unknown zone_id: {zone_id}"}
        self._dispatch_n += 1
        dispatch_id = f"disp-{self._dispatch_n:08x}"
        blocked = any(a["capability"] == "actuator" and a["target_zone_id"] == zone_id
                      for a in self.active_attacks.values())
        if not blocked:
            self.zone_dispatches[zone_id].append({"dispatch_id": dispatch_id, "incident_type": incident_type,
                                                  "zone_id": zone_id, "dispatched_at": self.get_sim_time()})
        return {"status": "ok", "dispatch_id": dispatch_id, "zone_id": zone_id}

    # ----- adversary -----

    def inject(self, capability: str, target: str, params: dict[str, Any]) -> str:
        """Start an attack on a device id or a zone id; returns the attack id."""
        if capability not in _CAPABILITIES:
            raise ValueError(f"Invalid adversary capability: {capability}. Must be one of {_CAPABILITIES}")
        if target not in self.devices and target not in self.zone_dispatches:
            raise ValueError(f"Unknown target: {target}")
        self._attack_n += 1
        attack_id = f"atk-{self._attack_n:08x}"
        is_device = target in self.devices
        attack = {"attack_id": attack_id, "capability": capability, "params": dict(params or {}),
                  "target_device_id": target if is_device else None,
                  "target_zone_id": None if is_device else target, "start_sim_time": self.get_sim_time()}
        self.active_attacks[attack_id] = attack
        for dev in self.devices.values():
            if dev.device_id == target or (not is_device and dev.zone_id == target):
                dev.active_attack = attack
        return attack_id
