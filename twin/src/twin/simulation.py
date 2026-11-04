"""Core Physics Simulator and Dual-Clock Engine for Twin Layer (Layer 1).

Assigned to: Agent_Physics
Operates strictly in Simulation Time (RFC 3339 UTC).
Wall-clock time is exclusively used for cycle pacing (10 Hz).
"""

from __future__ import annotations

import json
import math
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from twin.crypto_utils import generate_keypair
from twin.db_ledger import (
    DeviceRecord,
    GroundTruthLedger,
    InjectedAttackRecord,
    TruthReadingRecord,
)


def format_sim_time(sim_dt: datetime) -> str:
    """Format simulation datetime to RFC 3339 UTC string."""
    if sim_dt.tzinfo is None:
        sim_dt = sim_dt.replace(tzinfo=timezone.utc)
    else:
        sim_dt = sim_dt.astimezone(timezone.utc)
    return sim_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class SimulationClock:
    """Dual-clock system coordinator.

    Simulation Time is strictly deterministic RFC 3339 UTC.
    Wall-clock time is ONLY used for cycle pacing (10 Hz).
    """

    def __init__(self, start_time: Optional[datetime] = None, step_seconds: float = 0.1):
        self.step_seconds = step_seconds  # 10 Hz = 0.1s
        self.sim_dt = start_time or datetime(2026, 10, 17, 12, 0, 0, tzinfo=timezone.utc)
        self.tick_count = 0
        self._last_wall_time = time.monotonic()

    def get_sim_time(self) -> str:
        """Return current simulation time in strict RFC 3339 UTC."""
        return format_sim_time(self.sim_dt)

    def advance_tick(self) -> str:
        """Advance simulation clock by exactly one fixed timestep (0.1s)."""
        self.tick_count += 1
        self.sim_dt += timedelta(seconds=self.step_seconds)
        return self.get_sim_time()

    def pace_wall_clock(self) -> float:
        """Sleep remaining time to enforce 10 Hz pacing on wall-clock time.

        Returns the elapsed wall-clock duration of the cycle in seconds.
        """
        now = time.monotonic()
        elapsed = now - self._last_wall_time
        sleep_needed = self.step_seconds - elapsed
        if sleep_needed > 0:
            time.sleep(sleep_needed)
            actual_elapsed = time.monotonic() - self._last_wall_time
        else:
            actual_elapsed = elapsed
        self._last_wall_time = time.monotonic()
        return actual_elapsed


@dataclass
class DeviceState:
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
    status: str = "ok"  # ok, isolated, quarantined, rolled_back, modelled
    seq: int = 0
    current_value: float = 0.0
    drift_offset: float = 0.0
    last_broadcast_reading: Optional[Dict[str, Any]] = None
    actuator_state: Dict[str, Any] = field(default_factory=dict)
    active_attack: Optional[Dict[str, Any]] = None


class CitySimulator:
    """Simulates 42 devices across 6 zones with dynamic physical models."""

    ZONES = [
        {"zone_id": "zone-1", "name": "Downtown Financial", "lat": 36.8000, "lon": 10.1800},
        {"zone_id": "zone-2", "name": "Port & Coastal Harbor", "lat": 36.8050, "lon": 10.2700},
        {"zone_id": "zone-3", "name": "North Industrial Park", "lat": 36.8480, "lon": 10.2050},
        {"zone_id": "zone-4", "name": "West Residential Hills", "lat": 36.8150, "lon": 10.1300},
        {"zone_id": "zone-5", "name": "Central University & Med", "lat": 36.8320, "lon": 10.1550},
        {"zone_id": "zone-6", "name": "South Logistics Hub", "lat": 36.7550, "lon": 10.2200},
    ]

    DEVICE_TEMPLATES = [
        {"suffix": "tc1", "type": "traffic_camera", "unit": "veh/min", "min": 0.0, "max": 200.0, "init": 45.0},
        {"suffix": "tc2", "type": "traffic_camera", "unit": "veh/min", "min": 0.0, "max": 200.0, "init": 38.0},
        {"suffix": "wl1", "type": "water_level", "unit": "meters", "min": 0.0, "max": 12.0, "init": 3.2},
        {"suffix": "wl2", "type": "water_level", "unit": "meters", "min": 0.0, "max": 12.0, "init": 2.8},
        {"suffix": "pl1", "type": "power_load", "unit": "MW", "min": 5.0, "max": 120.0, "init": 48.0},
        {"suffix": "aq1", "type": "air_quality", "unit": "AQI", "min": 10.0, "max": 350.0, "init": 42.0},
        {"suffix": "ec1", "type": "emergency_call", "unit": "call/min", "min": 0.0, "max": 30.0, "init": 1.0},
    ]

    def __init__(self, seed: int = 42, ledger: Optional[GroundTruthLedger] = None):
        self.seed = seed
        self.session_id = uuid.uuid4().hex[:4]
        self.rng = random.Random(seed)
        self.clock = SimulationClock()
        self.ledger = ledger or GroundTruthLedger()
        self.devices: Dict[str, DeviceState] = {}
        self.zone_dispatches: Dict[str, List[Dict[str, Any]]] = {z["zone_id"]: [] for z in self.ZONES}
        self.command_results: Dict[str, Dict[str, Any]] = {}
        self.recent_readings_buffer: Dict[str, Dict[str, Any]] = {}
        self.active_attacks: Dict[str, Dict[str, Any]] = {}
        self.rogue_attacks: Dict[str, Dict[str, Any]] = {}

        self._generate_device_inventory()


    def _generate_device_inventory(self) -> None:
        """Generate the exact 42 devices (7 per zone across 6 zones)."""
        device_records: List[DeviceRecord] = []
        for zone in self.ZONES:
            zone_id = zone["zone_id"]
            zone_num = zone_id.split("-")[-1]
            z_lat = zone["lat"]
            z_lon = zone["lon"]

            for idx, tmpl in enumerate(self.DEVICE_TEMPLATES):
                device_id = f"z{zone_num}-{tmpl['suffix']}"
                # Slight geographic offset per sensor within the zone
                lat = round(z_lat + (idx * 0.0012) - 0.0036, 6)
                lon = round(z_lon + (idx * 0.0015) - 0.0045, 6)

                priv_key, pub_key = generate_keypair()

                initial_actuator: Dict[str, Any] = {}
                if tmpl["type"] == "traffic_camera":
                    initial_actuator = {"signal_plan": "normal"}
                elif tmpl["type"] == "water_level":
                    initial_actuator = {"valve_position": "closed", "valve_ratio": 0.0}
                elif tmpl["type"] == "power_load":
                    initial_actuator = {"grid_state": "grid_a"}

                dev_state = DeviceState(
                    device_id=device_id,
                    zone_id=zone_id,
                    device_type=tmpl["type"],
                    unit=tmpl["unit"],
                    lat=lat,
                    lon=lon,
                    min_val=tmpl["min"],
                    max_val=tmpl["max"],
                    private_key=priv_key,
                    public_key=pub_key,
                    status="ok",
                    seq=0,
                    current_value=tmpl["init"],
                    actuator_state=initial_actuator,
                )
                self.devices[device_id] = dev_state

                device_records.append(
                    DeviceRecord(
                        device_id=device_id,
                        zone_id=zone_id,
                        device_type=tmpl["type"],
                        unit=tmpl["unit"],
                        lat=lat,
                        lon=lon,
                        public_key=pub_key,
                        status="ok",
                        min_val=tmpl["min"],
                        max_val=tmpl["max"],
                    )
                )

        # Batch insert all 42 devices into SQLite ledger
        self.ledger.register_devices_batch(device_records)

    def get_sim_time(self) -> str:
        """Return simulation time."""
        return self.clock.get_sim_time()

    def update_physics(self) -> List[Tuple[DeviceState, float]]:
        """Calculate standard mathematical formulas for normal physical city behavior.

        Returns list of (device_state, true_value).
        """
        results: List[Tuple[DeviceState, float]] = []
        sim_dt = self.clock.sim_dt
        sec_of_day = (
            sim_dt.hour * 3600
            + sim_dt.minute * 60
            + sim_dt.second
            + sim_dt.microsecond / 1_000_000.0
        )
        # Diurnal normalized wave (0 to 1) with 2 peaks: 8:00 (morning) and 18:00 (evening)
        # Using a superposition of daily diurnal harmonic frequencies
        t_norm = (sec_of_day % 86400) / 86400.0
        diurnal_traffic = 0.5 + 0.35 * math.sin(2 * math.pi * (t_norm - 0.25)) + 0.15 * math.sin(4 * math.pi * (t_norm - 0.2))
        diurnal_power = 0.55 + 0.3 * math.sin(2 * math.pi * (t_norm - 0.3)) + 0.15 * math.cos(2 * math.pi * t_norm)

        for dev_id, dev in self.devices.items():
            true_val = dev.current_value
            dt = self.clock.step_seconds

            if dev.device_type == "traffic_camera":
                # Mathematical formula: diurnal demand modulated by signal plan
                plan = dev.actuator_state.get("signal_plan", "normal")
                throughput_multiplier = 1.0
                if plan == "green_wave":
                    throughput_multiplier = 1.35
                elif plan == "red_restriction":
                    throughput_multiplier = 0.55

                target_flow = (40.0 + 80.0 * max(0.0, min(1.0, diurnal_traffic))) * throughput_multiplier
                # Low-pass filter inertia with Gaussian noise
                noise = self.rng.gauss(0.0, 1.2)
                true_val = dev.current_value + 0.05 * (target_flow - dev.current_value) + noise * dt

            elif dev.device_type == "water_level":
                # Drainage model: valve position controls discharge rate vs baseline runoff
                valve = dev.actuator_state.get("valve_position", "closed")
                valve_ratio = dev.actuator_state.get("valve_ratio", 1.0 if valve == "open" else 0.0)

                runoff_inflow = 0.015 * (1.0 + 0.2 * math.sin(2 * math.pi * t_norm * 4))  # slight inflow
                drainage_outflow = 0.045 * valve_ratio  # discharge capacity

                net_delta = (runoff_inflow - drainage_outflow) * dt
                noise = self.rng.gauss(0.0, 0.02)
                true_val = dev.current_value + net_delta + (noise * dt)

            elif dev.device_type == "power_load":
                # Electrical substation load: diurnal curve + grid switching
                grid_state = dev.actuator_state.get("grid_state", "grid_a")
                grid_factor = 1.0
                if grid_state == "grid_b":
                    grid_factor = 0.85
                elif grid_state == "shed":
                    grid_factor = 0.40
                elif grid_state == "standby":
                    grid_factor = 0.20

                target_power = (30.0 + 55.0 * max(0.0, min(1.0, diurnal_power))) * grid_factor
                noise = self.rng.gauss(0.0, 0.5)
                true_val = dev.current_value + 0.08 * (target_power - dev.current_value) + (noise * dt)

            elif dev.device_type == "air_quality":
                # AQI correlated with zone's average traffic camera readings
                # Find traffic in same zone
                zone_traffic = [
                    d.current_value for d in self.devices.values()
                    if d.zone_id == dev.zone_id and d.device_type == "traffic_camera"
                ]
                avg_traffic = sum(zone_traffic) / len(zone_traffic) if zone_traffic else 50.0
                target_aqi = 20.0 + (avg_traffic * 0.45)
                noise = self.rng.gauss(0.0, 0.4)
                true_val = dev.current_value + 0.02 * (target_aqi - dev.current_value) + (noise * dt)

            elif dev.device_type == "emergency_call":
                # Emergency call rate: low baseline with decay when dispatched
                dispatches = self.zone_dispatches.get(dev.zone_id, [])
                active_units = len(dispatches)

                if active_units > 0:
                    target_calls = max(0.2, 0.8 - (active_units * 0.3))
                else:
                    target_calls = 1.2

                noise = max(0.0, self.rng.gauss(0.0, 0.15))
                true_val = max(0.0, dev.current_value + 0.1 * (target_calls - dev.current_value) + (noise * dt))

            # Clamp within physical plausible bounds
            clamped_val = round(max(dev.min_val, min(dev.max_val, true_val)), 4)
            dev.current_value = clamped_val
            dev.seq += 1
            results.append((dev, clamped_val))

        return results

    def inject_attack(
        self,
        capability: str,
        target_device_id: Optional[str] = None,
        target_zone_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
    ) -> str:
        """Inject adversary capability dynamically into the simulation engine.

        Capabilities: 'link', 'rogue', 'device_key', 'device_physical', 'actuator', 'sensor_fault'
        """
        valid_caps = {"link", "rogue", "device_key", "device_physical", "actuator", "sensor_fault"}
        if capability not in valid_caps:
            raise ValueError(f"Invalid adversary capability: {capability}. Must be one of {valid_caps}")

        attack_id = f"atk-{uuid.uuid4().hex[:8]}"
        params_dict = params or {}
        sim_time = self.get_sim_time()

        attack_info = {
            "attack_id": attack_id,
            "capability": capability,
            "target_device_id": target_device_id,
            "target_zone_id": target_zone_id,
            "params": params_dict,
            "start_sim_time": sim_time,
            "description": description or f"Injected adversary capability: {capability}",
        }

        # Log attack directly into SQLite Ground Truth Ledger
        self.ledger.record_attack(
            InjectedAttackRecord(
                attack_id=attack_id,
                capability=capability,
                target_device_id=target_device_id,
                target_zone_id=target_zone_id,
                params_json=json.dumps(params_dict),
                start_sim_time=sim_time,
                active=True,
                description=attack_info["description"],
            )
        )

        self.active_attacks[attack_id] = attack_info

        if capability == "rogue":
            self.rogue_attacks[attack_id] = attack_info
        elif target_device_id and target_device_id in self.devices:
            self.devices[target_device_id].active_attack = attack_info
        elif target_zone_id:
            for d in self.devices.values():
                if d.zone_id == target_zone_id:
                    d.active_attack = attack_info

        return attack_id

    def clear_attack(self, attack_id: str) -> bool:
        """Deactivate and clear an active attack."""
        if attack_id not in self.active_attacks:
            # Also ensure ledger is updated if attack exists from previous session
            return self.ledger.deactivate_attack(attack_id, end_sim_time=self.get_sim_time())

        attack_info = self.active_attacks.pop(attack_id)
        if attack_id in self.rogue_attacks:
            self.rogue_attacks.pop(attack_id)

        target_dev = attack_info.get("target_device_id")
        target_zone = attack_info.get("target_zone_id")

        if target_dev and target_dev in self.devices:
            if self.devices[target_dev].active_attack and self.devices[target_dev].active_attack.get("attack_id") == attack_id:
                self.devices[target_dev].active_attack = None
                self.devices[target_dev].drift_offset = 0.0
        elif target_zone:
            for d in self.devices.values():
                if d.zone_id == target_zone and d.active_attack and d.active_attack.get("attack_id") == attack_id:
                    d.active_attack = None
                    d.drift_offset = 0.0

        # Update end time in SQLite ledger
        self.ledger.record_attack(
            InjectedAttackRecord(
                attack_id=attack_id,
                capability=attack_info["capability"],
                target_device_id=target_dev,
                target_zone_id=target_zone,
                params_json=json.dumps(attack_info.get("params", {})),
                start_sim_time=attack_info["start_sim_time"],
                end_sim_time=self.get_sim_time(),
                active=False,
                description=attack_info.get("description"),
            )
        )
        return True

    def clear_all_attacks(self) -> None:
        """Clear all currently active injected attacks."""
        for aid in list(self.active_attacks.keys()):
            self.clear_attack(aid)
        self.active_attacks.clear()
        self.rogue_attacks.clear()
        for d in self.devices.values():
            d.active_attack = None
            d.drift_offset = 0.0
        self.ledger.deactivate_all_attacks(end_sim_time=self.get_sim_time())

    def tick(self) -> List[Dict[str, Any]]:
        """Execute one complete 10 Hz simulation tick.

        1. Updates physical mathematical formulas.
        2. Applies active adversary capability manipulations.
        3. Records true physical readings and broadcast values to SQLite ledger.
        4. Updates live telemetry state cache.
        5. Advances simulation clock by 0.1s.
        """
        sim_time = self.clock.get_sim_time()
        physics_results = self.update_physics()

        truth_records: List[TruthReadingRecord] = []
        broadcast_readings: List[Dict[str, Any]] = []

        for dev, true_val in physics_results:
            reading_id = f"r-{dev.device_id}-{dev.seq}"
            broadcast_val = true_val
            is_attacked = False
            attack_type: Optional[str] = None
            corrupt_sig = False
            drop_reading = False
            replay_reading = False

            # Dynamic Adversary Capability Processing
            if dev.active_attack:
                attack_info = dev.active_attack
                capability = attack_info.get("capability")
                params = attack_info.get("params", {})
                attack_type = capability
                is_attacked = True

                if capability == "device_physical":
                    # Sensor perception physically corrupted
                    broadcast_val = params.get("spoofed_value", round(true_val * 2.5, 2))
                elif capability == "device_key":
                    # Attacker holds key, sends arbitrary forged reading
                    broadcast_val = params.get("spoofed_value", round(true_val + 50.0, 2))
                elif capability == "sensor_fault":
                    fault_mode = params.get("fault_mode", "drift")
                    if fault_mode == "stuck":
                        broadcast_val = params.get("stuck_value", 42.0)
                    elif fault_mode == "dropout":
                        broadcast_val = 0.0
                    else:  # drift
                        dev.drift_offset += params.get("drift_rate", 0.5) * self.clock.step_seconds
                        broadcast_val = round(true_val + dev.drift_offset, 2)
                elif capability == "actuator":
                    # Physical truth reflects non-execution
                    pass
                elif capability == "link":
                    link_mode = params.get("link_mode", "corrupt_bytes")
                    if link_mode == "drop":
                        drop_reading = True
                    elif link_mode == "corrupt_bytes":
                        corrupt_sig = True
                    elif link_mode == "replay" and dev.last_broadcast_reading:
                        replay_reading = True

            # Record truth into SQLite Ground Truth Ledger
            truth_records.append(
                TruthReadingRecord(
                    reading_id=reading_id,
                    device_id=dev.device_id,
                    ts=sim_time,
                    seq=dev.seq,
                    true_value=true_val,
                    broadcast_value=broadcast_val,
                    unit=dev.unit,
                    status=dev.status,
                    sig=None,
                    attack_type=attack_type,
                    is_attacked=is_attacked,
                )
            )

            if not drop_reading:
                if replay_reading and dev.last_broadcast_reading:
                    reading_dict = dict(dev.last_broadcast_reading)
                else:
                    reading_dict = {
                        "reading_id": reading_id,
                        "device_id": dev.device_id,
                        "device_type": dev.device_type,
                        "ts": sim_time,
                        "seq": dev.seq,
                        "value": broadcast_val,
                        "unit": dev.unit,
                        "location": dev.zone_id,
                        "status": dev.status,
                    }
                    if corrupt_sig:
                        reading_dict["_corrupt_sig"] = True

                broadcast_readings.append(reading_dict)
                self.recent_readings_buffer[reading_id] = reading_dict
                dev.last_broadcast_reading = reading_dict

        # Process active rogue attacks (unregistered devices broadcasting)
        for atk_id, rogue_info in list(self.rogue_attacks.items()):
            rogue_params = rogue_info.get("params", {})
            rogue_dev_id = rogue_params.get("rogue_device_id", "rogue-01")
            rogue_reading_id = f"r-{rogue_dev_id}-{self.clock.tick_count}"
            rogue_val = float(rogue_params.get("value", 88.88))
            rogue_dict = {
                "reading_id": rogue_reading_id,
                "device_id": rogue_dev_id,
                "device_type": "traffic_camera",
                "ts": sim_time,
                "seq": self.clock.tick_count,
                "value": rogue_val,
                "unit": "veh/min",
                "location": rogue_params.get("zone_id", "zone-1"),
                "status": "ok",
            }
            broadcast_readings.append(rogue_dict)
            self.recent_readings_buffer[rogue_reading_id] = rogue_dict

            truth_records.append(
                TruthReadingRecord(
                    reading_id=rogue_reading_id,
                    device_id=rogue_dev_id,
                    ts=sim_time,
                    seq=self.clock.tick_count,
                    true_value=0.0,
                    broadcast_value=rogue_val,
                    unit="veh/min",
                    status="ok",
                    sig=None,
                    attack_type="rogue",
                    is_attacked=True,
                )
            )

        # Batch record ground truth to SQLite ledger
        self.ledger.record_readings_truth_batch(truth_records)

        # Advance clock
        self.clock.advance_tick()
        return broadcast_readings


    # Public State Inquiries (Isolated from SQLite Ledger)
    def list_devices_spec(self) -> List[Dict[str, Any]]:
        """List device inventory with physical ranges and zone assignments."""
        return [
            {
                "device_id": d.device_id,
                "zone_id": d.zone_id,
                "device_type": d.device_type,
                "unit": d.unit,
                "lat": d.lat,
                "lon": d.lon,
                "min_val": d.min_val,
                "max_val": d.max_val,
                "status": d.status,
                "actuator_state": d.actuator_state,
            }
            for d in self.devices.values()
        ]

    def get_device_public_key(self, device_id: str) -> Optional[str]:
        """Fetch device public key."""
        dev = self.devices.get(device_id)
        return dev.public_key if dev else None

    def get_device_state(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Fetch current telemetry and status for a single device."""
        dev = self.devices.get(device_id)
        if not dev:
            return None
        return {
            "device_id": dev.device_id,
            "zone_id": dev.zone_id,
            "device_type": dev.device_type,
            "current_value": dev.current_value,
            "unit": dev.unit,
            "seq": dev.seq,
            "status": dev.status,
            "actuator_state": dev.actuator_state,
            "ts": self.get_sim_time(),
        }

    def get_city_state(self) -> Dict[str, Any]:
        """Fetch high-level overview of city state across all zones."""
        return {
            "sim_time": self.get_sim_time(),
            "total_devices": len(self.devices),
            "zones": {
                z["zone_id"]: {
                    "name": z["name"],
                    "active_dispatches": len(self.zone_dispatches.get(z["zone_id"], [])),
                    "devices": [
                        {
                            "device_id": d.device_id,
                            "type": d.device_type,
                            "value": d.current_value,
                            "unit": d.unit,
                            "status": d.status,
                        }
                        for d in self.devices.values()
                        if d.zone_id == z["zone_id"]
                    ],
                }
                for z in self.ZONES
            },
        }

    def get_cached_reading(self, reading_id: str) -> Optional[Dict[str, Any]]:
        """Fetch past reading from the public broadcast cache."""
        return self.recent_readings_buffer.get(reading_id)

    # Actuation Handlers (Layer 2 - City Brain)
    def handle_dispatch(self, incident_type: str, zone_id: str, idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        """Handle dispatch command from City Brain."""
        if zone_id not in self.zone_dispatches:
            result = {"status": "error", "message": f"Unknown zone_id: {zone_id}"}
        else:
            dispatch_id = f"disp-{uuid.uuid4().hex[:8]}"
            zone_attack = any(
                atk.get("capability") == "actuator" and atk.get("target_zone_id") == zone_id
                for atk in self.active_attacks.values()
            )
            if not zone_attack:
                record = {
                    "dispatch_id": dispatch_id,
                    "incident_type": incident_type,
                    "zone_id": zone_id,
                    "dispatched_at": self.get_sim_time(),
                }
                self.zone_dispatches[zone_id].append(record)
            result = {"status": "ok", "dispatch_id": dispatch_id, "zone_id": zone_id}

        if idempotency_key:
            self.command_results[idempotency_key] = result
        return result

    def handle_set_signal_plan(self, device_id: str, target_state: str, idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        """Handle signal plan configuration for traffic cameras."""
        dev = self.devices.get(device_id)
        if not dev or dev.device_type != "traffic_camera":
            result = {"status": "error", "message": f"Device {device_id} is not a valid traffic_camera"}
        else:
            # Check if actuator attack is active: accepts command but does not execute
            if not (dev.active_attack and dev.active_attack.get("capability") == "actuator"):
                dev.actuator_state["signal_plan"] = target_state
            result = {"status": "ok", "device_id": device_id, "signal_plan": target_state}

        if idempotency_key:
            self.command_results[idempotency_key] = result
        return result

    def handle_set_valve(self, device_id: str, target_state: str, idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        """Handle water valve actuation."""
        dev = self.devices.get(device_id)
        if not dev or dev.device_type != "water_level":
            result = {"status": "error", "message": f"Device {device_id} is not a valid water_level sensor"}
        else:
            # Check if actuator attack is active: accepts command but does not execute
            if not (dev.active_attack and dev.active_attack.get("capability") == "actuator"):
                ratio = 1.0 if target_state == "open" else 0.0
                try:
                    if target_state.endswith("%"):
                        ratio = float(target_state.rstrip("%")) / 100.0
                except ValueError:
                    pass
                dev.actuator_state["valve_position"] = target_state
                dev.actuator_state["valve_ratio"] = max(0.0, min(1.0, ratio))
            result = {"status": "ok", "device_id": device_id, "valve_position": target_state}

        if idempotency_key:
            self.command_results[idempotency_key] = result
        return result

    def handle_switch_grid(self, device_id: str, target_state: str, idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        """Handle power grid switching actuation."""
        dev = self.devices.get(device_id)
        if not dev or dev.device_type != "power_load":
            result = {"status": "error", "message": f"Device {device_id} is not a valid power_load sensor"}
        else:
            # Check if actuator attack is active: accepts command but does not execute
            if not (dev.active_attack and dev.active_attack.get("capability") == "actuator"):
                dev.actuator_state["grid_state"] = target_state
            result = {"status": "ok", "device_id": device_id, "grid_state": target_state}

        if idempotency_key:
            self.command_results[idempotency_key] = result
        return result

    def get_command_result(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve command actuation result by idempotency key."""
        return self.command_results.get(idempotency_key)

    # Containment Handlers (Layer 3 - Guardian)
    def update_device_status(self, device_id: str, new_status: str) -> Dict[str, Any]:
        """Update device containment status (ok, isolated, quarantined, rolled_back, modelled)."""
        valid_statuses = {"ok", "isolated", "quarantined", "rolled_back", "modelled"}
        if new_status not in valid_statuses:
            return {"status": "error", "message": f"Invalid status: {new_status}"}

        dev = self.devices.get(device_id)
        if not dev:
            return {"status": "error", "message": f"Device {device_id} not found"}

        dev.status = new_status
        return {"status": "ok", "device_id": device_id, "new_status": new_status}

    def rollback_reading(self, reading_id: str) -> Dict[str, Any]:
        """Mark a specific reading as rolled back."""
        reading = self.recent_readings_buffer.get(reading_id)
        if not reading:
            return {"status": "error", "message": f"Reading {reading_id} not found in buffer"}
        reading["status"] = "rolled_back"
        return {"status": "ok", "reading_id": reading_id, "reading_status": "rolled_back"}


if __name__ == "__main__":
    print("Initializing standalone CitySimulator loop (10 Hz pacing)...")
    sim = CitySimulator(seed=42)
    print(f"Generated {len(sim.devices)} devices across 6 zones.")
    for tick_idx in range(1, 51):
        readings = sim.tick()
        if tick_idx % 10 == 0:
            print(f"Tick {tick_idx:03d} | Sim Time: {sim.get_sim_time()} | Generated {len(readings)} readings")
        sim.clock.pace_wall_clock()
    print("Standalone simulation loop complete.")
