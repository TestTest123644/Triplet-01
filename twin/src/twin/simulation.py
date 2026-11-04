"""Core Physics Simulator and Dual-Clock Engine for Twin Layer (Layer 1).

Assigned to: Agent_Physics
Operates strictly in Simulation Time (RFC 3339 UTC).
Wall-clock time is exclusively used for cycle pacing (10 Hz).
"""

from __future__ import annotations

import collections
import json
import logging
import math
import os
import random
import re
import sqlite3
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
    parse_sim_time,
)
from twin.keystore import DeviceKeystore
from twin.wire_limits import (
    ROGUE_ID_MAX_LEN,
    SEQ_SAFE_MAX,
    WIRE_VALUE_MAX,
    WIRE_VALUE_MIN,
)

logger = logging.getLogger("twin.simulation")


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
    last_broadcast_value: float = 0.0
    drift_offset: float = 0.0
    last_broadcast_reading: Optional[Dict[str, Any]] = None
    actuator_state: Dict[str, Any] = field(default_factory=dict)
    reported_actuator_state: Dict[str, Any] = field(default_factory=dict)
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

    ALLOWED_ZONES = ["zone-1", "zone-2", "zone-3", "zone-4", "zone-5", "zone-6"]
    KNOWN_INCIDENTS = {
        "medical_surge",
        "medical_emergency",
        "flood",
        "traffic_accident",
        "fire",
        "blackout",
        "air_hazard",
    }
    ALLOWED_SIGNAL_PLANS = ["normal", "green_wave", "red_restriction", "all_red", "adaptive"]
    ALLOWED_VALVE_STATES = ["open", "half", "closed", "0%-100%"]
    ALLOWED_GRID_STATES = ["grid_a", "grid_b", "island", "shed", "standby"]

    def __init__(
        self,
        seed: int = 42,
        ledger: Optional[GroundTruthLedger] = None,
        keystore_path: Optional[str] = None,
    ):
        self.seed = seed
        self.run_id = uuid.uuid4().hex
        self.session_id = self.run_id[:4]
        self.rng = random.Random(seed)
        self.ledger = ledger or GroundTruthLedger()
        self.keystore = DeviceKeystore(keystore_path=keystore_path)

        # 2.b: Resumption clock: if SIM_START_TIME is not set, resume sim time from
        # the MAX(ts) in readings_truth plus 1 second. If empty or missing, default to 2026-10-17T12:00:00Z.
        sim_start_env = os.getenv("SIM_START_TIME")
        if sim_start_env:
            start_time = parse_sim_time(sim_start_env)
        else:
            latest_ts = self.ledger.get_latest_sim_time()
            if latest_ts:
                start_time = parse_sim_time(latest_ts) + timedelta(seconds=1)
            else:
                start_time = datetime(2026, 10, 17, 12, 0, 0, tzinfo=timezone.utc)

        self.clock = SimulationClock(start_time=start_time)
        self.devices: Dict[str, DeviceState] = {}
        self.zone_dispatches: Dict[str, List[Dict[str, Any]]] = {z["zone_id"]: [] for z in self.ZONES}
        self.reported_zone_dispatches: Dict[str, List[Dict[str, Any]]] = {z["zone_id"]: [] for z in self.ZONES}
        self.command_results_max = 10000
        self.command_results: collections.OrderedDict[str, Dict[str, Any]] = collections.OrderedDict()
        self.reading_cache_max = int(os.getenv("READING_CACHE_MAX", "50400"))
        self.recent_readings_buffer: collections.OrderedDict[str, Dict[str, Any]] = collections.OrderedDict()
        self.active_attacks: Dict[str, Dict[str, Any]] = {}
        self.rogue_attacks: Dict[str, Dict[str, Any]] = {}
        self._rogue_keys: Dict[str, Tuple[str, str]] = {}
        self._rogue_seqs: Dict[str, int] = {}
        self._clamped_attacks: set[str] = set()

        self._generate_device_inventory()

        # 2.a, 2.c: Resume sequence numbers per device and check 0.9 * SEQ_SAFE_MAX threshold
        max_seqs = self.ledger.get_max_sequences_per_device()
        for dev in self.devices.values():
            if dev.device_id in max_seqs:
                dev.seq = max_seqs[dev.device_id]
                if dev.seq >= 0.9 * SEQ_SAFE_MAX:
                    logger.warning(
                        f"[ALERT] Device {dev.device_id} resumed seq {dev.seq} approaches safe wire limit ({SEQ_SAFE_MAX})"
                    )

        for rogue_id, r_seq in max_seqs.items():
            if rogue_id not in self.devices:
                self._rogue_seqs[rogue_id] = r_seq
                if r_seq >= 0.9 * SEQ_SAFE_MAX:
                    logger.warning(
                        f"[ALERT] Rogue device {rogue_id} resumed seq {r_seq} approaches safe wire limit ({SEQ_SAFE_MAX})"
                    )

        # 3.b: Record start of run in runs table
        self.ledger.start_run(self.run_id, self.seed, self.get_sim_time())

    def _add_reading_to_cache(self, reading_id: str, reading_dict: Dict[str, Any]) -> None:
        """Add reading to bounded OrderedDict cache, evicting oldest when capacity is reached."""
        if reading_id in self.recent_readings_buffer:
            del self.recent_readings_buffer[reading_id]
        elif len(self.recent_readings_buffer) >= self.reading_cache_max:
            self.recent_readings_buffer.popitem(last=False)
        self.recent_readings_buffer[reading_id] = reading_dict

    def check_idempotency(
        self, idempotency_key: str, tool: str, args: Dict[str, Any]
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Check idempotency key against LRU cache.

        Returns (is_handled, response_dict):
        - If conflict: (True, {"status": "error", "code": "IDEMPOTENCY_CONFLICT", ...})
        - If replay: (True, cached_result with "idempotent_replay": True)
        - If new key: (False, None)
        """
        if idempotency_key in self.command_results:
            cached = self.command_results[idempotency_key]
            self.command_results.move_to_end(idempotency_key)
            if cached.get("tool") == tool and cached.get("args") == args:
                replay = dict(cached["result"])
                replay["idempotent_replay"] = True
                return True, replay
            else:
                conflict = {
                    "status": "error",
                    "code": "IDEMPOTENCY_CONFLICT",
                    "message": f"Idempotency key '{idempotency_key}' was previously used for a different command or arguments.",
                }
                return True, conflict
        return False, None

    def record_idempotency_result(
        self, idempotency_key: str, tool: str, args: Dict[str, Any], result: Dict[str, Any]
    ) -> None:
        """Record command execution result in LRU cache."""
        if idempotency_key in self.command_results:
            self.command_results.move_to_end(idempotency_key)
        elif len(self.command_results) >= self.command_results_max:
            self.command_results.popitem(last=False)
        self.command_results[idempotency_key] = {"tool": tool, "args": args, "result": result}

    def signing_key_for(self, device_id: str) -> Optional[str]:
        """Return private Ed25519 signing key for registered device or active rogue device."""
        dev = self.devices.get(device_id)
        if dev:
            return dev.private_key
        if device_id in self._rogue_keys:
            return self._rogue_keys[device_id][0]
        return None


    def _generate_device_inventory(self) -> None:
        """Generate the exact 42 devices (7 per zone across 6 zones)."""
        device_records: List[DeviceRecord] = []
        all_device_ids = [
            f"z{zone['zone_id'].split('-')[-1]}-{tmpl['suffix']}"
            for zone in self.ZONES
            for tmpl in self.DEVICE_TEMPLATES
        ]
        keys_map = self.keystore.ensure_device_keys(all_device_ids)

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

                priv_key, pub_key = keys_map[device_id]

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
                    last_broadcast_value=tmpl["init"],
                    actuator_state=dict(initial_actuator),
                    reported_actuator_state=dict(initial_actuator),
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

        params_dict = dict(params or {})

        # 1.b: Reject non-finite numeric params (NaN/inf) with ValueError
        def _check_finite(obj: Any) -> None:
            if isinstance(obj, float):
                if not math.isfinite(obj):
                    raise ValueError(f"Non-finite numeric parameter detected: {obj}")
            elif isinstance(obj, dict):
                for v in obj.values():
                    _check_finite(v)
            elif isinstance(obj, (list, tuple)):
                for v in obj:
                    _check_finite(v)

        _check_finite(params_dict)

        # 2.a, 2.c: Handle rogue device ID validation and throwaway Ed25519 keypair
        if capability == "rogue":
            rogue_dev_id = params_dict.get("rogue_device_id", "rg-01")
            if not re.match(r"^[a-z0-9-]{1,6}$", rogue_dev_id):
                raise ValueError(
                    f"Invalid rogue_device_id '{rogue_dev_id}': must match ^[a-z0-9-]{{1,{ROGUE_ID_MAX_LEN}}}$ (max length {ROGUE_ID_MAX_LEN})"
                )
            if rogue_dev_id in self.devices:
                raise ValueError(
                    f"Invalid rogue_device_id '{rogue_dev_id}': collides with registered device_id"
                )
            if rogue_dev_id in self._rogue_keys:
                raise ValueError(
                    f"Rogue device_id '{rogue_dev_id}' is already active in an ongoing rogue attack"
                )
            params_dict["rogue_device_id"] = rogue_dev_id
            # Throwaway keypair per rogue attack stored privately, NOT in attack_info or params
            priv_b64, pub_b64 = generate_keypair()
            self._rogue_keys[rogue_dev_id] = (priv_b64, pub_b64)

        attack_id = f"atk-{uuid.uuid4().hex[:8]}"
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
                run_id=self.run_id,
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
            rogue_info = self.rogue_attacks.pop(attack_id)
            r_dev_id = rogue_info.get("params", {}).get("rogue_device_id")
            if r_dev_id:
                self._rogue_keys.pop(r_dev_id, None)
                # NOTE: _rogue_seqs is preserved across clear_attack to prevent reading_id collision on re-injection

        self._clamped_attacks.discard(attack_id)

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
                run_id=self.run_id,
            )
        )
        return True

    def clear_all_attacks(self) -> None:
        """Clear all currently active injected attacks."""
        for aid in list(self.active_attacks.keys()):
            self.clear_attack(aid)
        self.active_attacks.clear()
        self.rogue_attacks.clear()
        self._rogue_keys.clear()
        # NOTE: _rogue_seqs is preserved across clear_all_attacks to prevent reading_id collision
        self._clamped_attacks.clear()
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

            # 1.a: Clamp every broadcast value into [WIRE_VALUE_MIN, WIRE_VALUE_MAX]
            clamped_val = max(WIRE_VALUE_MIN, min(WIRE_VALUE_MAX, float(broadcast_val)))
            if clamped_val != broadcast_val:
                if is_attacked and dev.active_attack:
                    atk_id = dev.active_attack.get("attack_id")
                    if atk_id and atk_id not in self._clamped_attacks:
                        self._clamped_attacks.add(atk_id)
                        logger.warning(
                            f"[ALERT] Attack {atk_id} broadcast value {broadcast_val} clamped to "
                            f"{clamped_val} within wire limits [{WIRE_VALUE_MIN}, {WIRE_VALUE_MAX}]"
                        )
                broadcast_val = clamped_val
            else:
                broadcast_val = clamped_val

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
                    run_id=self.run_id,
                )
            )

            if not drop_reading:
                if replay_reading and dev.last_broadcast_reading:
                    reading_dict = dict(dev.last_broadcast_reading)
                    dev.last_broadcast_value = reading_dict["value"]
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
                    dev.last_broadcast_value = broadcast_val

                broadcast_readings.append(reading_dict)
                self._add_reading_to_cache(reading_id, reading_dict)
                dev.last_broadcast_reading = reading_dict

        # Process active rogue attacks (unregistered devices broadcasting)
        for atk_id, rogue_info in list(self.rogue_attacks.items()):
            rogue_params = rogue_info.get("params", {})
            rogue_dev_id = rogue_params.get("rogue_device_id", "rg-01")
            seq = self._rogue_seqs.get(rogue_dev_id, 0) + 1
            self._rogue_seqs[rogue_dev_id] = seq
            rogue_reading_id = f"r-{rogue_dev_id}-{seq}"
            raw_rogue_val = float(rogue_params.get("value", 88.88))
            rogue_val = max(WIRE_VALUE_MIN, min(WIRE_VALUE_MAX, raw_rogue_val))
            if rogue_val != raw_rogue_val and atk_id not in self._clamped_attacks:
                self._clamped_attacks.add(atk_id)
                logger.warning(
                    f"[ALERT] Attack {atk_id} broadcast value {raw_rogue_val} clamped to "
                    f"{rogue_val} within wire limits [{WIRE_VALUE_MIN}, {WIRE_VALUE_MAX}]"
                )
            rogue_dict = {
                "reading_id": rogue_reading_id,
                "device_id": rogue_dev_id,
                "device_type": "traffic_camera",
                "ts": sim_time,
                "seq": seq,
                "value": rogue_val,
                "unit": "veh/min",
                "location": rogue_params.get("zone_id", "zone-1"),
                "status": "ok",
            }
            broadcast_readings.append(rogue_dict)
            self._add_reading_to_cache(rogue_reading_id, rogue_dict)

            truth_records.append(
                TruthReadingRecord(
                    reading_id=rogue_reading_id,
                    device_id=rogue_dev_id,
                    ts=sim_time,
                    seq=seq,
                    true_value=0.0,
                    broadcast_value=rogue_val,
                    unit="veh/min",
                    status="ok",
                    sig=None,
                    attack_type="rogue",
                    is_attacked=True,
                    run_id=self.run_id,
                )
            )

        # Batch record ground truth to SQLite ledger
        try:
            self.ledger.record_readings_truth_batch(truth_records)
        except sqlite3.IntegrityError as e:
            logger.error(
                f"[CRITICAL THREAT] Duplicate reading_id detected in ground truth ledger: {e}"
            )

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
                "actuator_state": d.reported_actuator_state,
            }
            for d in self.devices.values()
        ]

    def get_device_public_key(self, device_id: str) -> Optional[str]:
        """Fetch device public key. Returns None for unknown devices (never an error object)."""
        if len(device_id) > 64:
            return None
        dev = self.devices.get(device_id)
        return dev.public_key if dev else None

    def get_device_state(self, device_id: str) -> Dict[str, Any]:
        """Fetch current telemetry and status for a single device."""
        if len(device_id) > 64:
            return {
                "status": "error",
                "code": "INVALID_ARGUMENT",
                "message": "device_id exceeds maximum length of 64 characters.",
            }
        dev = self.devices.get(device_id)
        if not dev:
            return {
                "status": "error",
                "code": "DEVICE_NOT_FOUND",
                "message": f"Device '{device_id}' not found.",
            }
        return {
            "device_id": dev.device_id,
            "zone_id": dev.zone_id,
            "device_type": dev.device_type,
            "current_value": dev.last_broadcast_value,
            "unit": dev.unit,
            "seq": dev.seq,
            "status": dev.status,
            "actuator_state": dev.reported_actuator_state,
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
                    "active_dispatches": len(self.reported_zone_dispatches.get(z["zone_id"], [])),
                    "devices": [
                        {
                            "device_id": d.device_id,
                            "type": d.device_type,
                            "value": d.last_broadcast_value,
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
    def handle_dispatch(
        self,
        incident_type: str,
        zone_id: str,
        idempotency_key: Optional[str] = None,
        caller: str = "anonymous",
    ) -> Dict[str, Any]:
        """Handle dispatch command from City Brain."""
        # 1. String length check
        if len(zone_id) > 64 or len(incident_type) > 64 or (idempotency_key and len(idempotency_key) > 64):
            res = {
                "status": "error",
                "code": "INVALID_ARGUMENT",
                "message": "Argument string exceeds maximum length of 64 characters.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "dispatch",
                {"incident_type": incident_type, "zone_id": zone_id, "idempotency_key": idempotency_key},
                False, "INVALID_ARGUMENT", res,
            )
            return res

        # 2. Zone lookup
        if zone_id not in self.ALLOWED_ZONES:
            res = {
                "status": "error",
                "code": "ZONE_NOT_FOUND",
                "message": f"Zone '{zone_id}' not found.",
                "allowed": list(self.ALLOWED_ZONES),
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "dispatch",
                {"incident_type": incident_type, "zone_id": zone_id, "idempotency_key": idempotency_key},
                False, "ZONE_NOT_FOUND", res,
            )
            return res

        # 3. Incident type syntax validation
        if not re.match(r"^[a-z_]{1,32}$", incident_type):
            res = {
                "status": "error",
                "code": "INVALID_INCIDENT_TYPE",
                "message": f"Invalid incident_type '{incident_type}': must match ^[a-z_]{{1,32}}$.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "dispatch",
                {"incident_type": incident_type, "zone_id": zone_id, "idempotency_key": idempotency_key},
                False, "INVALID_INCIDENT_TYPE", res,
            )
            return res

        # 4. Idempotency check
        args = {"incident_type": incident_type, "zone_id": zone_id}
        if idempotency_key:
            handled, replay = self.check_idempotency(idempotency_key, "dispatch", args)
            if handled:
                if replay.get("status") == "error":
                    self.ledger.log_command(
                        self.run_id, self.get_sim_time(), caller, "dispatch", args, False, replay.get("code"), replay
                    )
                return replay

        # 5. Generic incident note
        note = None
        if incident_type not in self.KNOWN_INCIDENTS:
            note = "unrecognized incident_type, dispatched as generic"

        # 6. Physical execution & Actuator attack
        dispatch_id = f"disp-{uuid.uuid4().hex[:8]}"
        record = {
            "dispatch_id": dispatch_id,
            "incident_type": incident_type,
            "zone_id": zone_id,
            "dispatched_at": self.get_sim_time(),
        }
        self.reported_zone_dispatches[zone_id].append(record)

        zone_attack = any(
            atk.get("capability") == "actuator" and atk.get("target_zone_id") == zone_id
            for atk in self.active_attacks.values()
        )
        if zone_attack:
            executed = False
            refused_code = "ACTUATOR_ATTACK"
        else:
            self.zone_dispatches[zone_id].append(record)
            executed = True
            refused_code = None

        result: Dict[str, Any] = {"status": "ok", "dispatch_id": dispatch_id, "zone_id": zone_id}
        if note:
            result["note"] = note

        if idempotency_key:
            self.record_idempotency_result(idempotency_key, "dispatch", args, result)

        self.ledger.log_command(
            self.run_id, self.get_sim_time(), caller, "dispatch", args, executed, refused_code, result
        )
        return result

    def handle_set_signal_plan(
        self,
        device_id: str,
        target_state: str,
        idempotency_key: Optional[str] = None,
        caller: str = "anonymous",
    ) -> Dict[str, Any]:
        """Handle signal plan configuration for traffic cameras."""
        args = {"device_id": device_id, "target_state": target_state}

        # 1. Length checks
        if len(device_id) > 64 or len(target_state) > 64 or (idempotency_key and len(idempotency_key) > 64):
            res = {
                "status": "error",
                "code": "INVALID_ARGUMENT",
                "message": "Argument string exceeds maximum length of 64 characters.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "set_signal_plan", args, False, "INVALID_ARGUMENT", res
            )
            return res

        # 2. Device lookup
        dev = self.devices.get(device_id)
        if not dev:
            res = {
                "status": "error",
                "code": "DEVICE_NOT_FOUND",
                "message": f"Device '{device_id}' not found.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "set_signal_plan", args, False, "DEVICE_NOT_FOUND", res
            )
            return res

        # 3. Device type validation
        if dev.device_type != "traffic_camera":
            res = {
                "status": "error",
                "code": "WRONG_DEVICE_TYPE",
                "message": f"Device '{device_id}' is of type '{dev.device_type}', expected 'traffic_camera'.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "set_signal_plan", args, False, "WRONG_DEVICE_TYPE", res
            )
            return res

        # 4. Containment check
        if dev.status in ("isolated", "quarantined"):
            res = {
                "status": "error",
                "code": "DEVICE_CONTAINED",
                "message": f"Device '{device_id}' is currently {dev.status} and cannot be actuated.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "set_signal_plan", args, False, "DEVICE_CONTAINED", res
            )
            return res

        # 5. Target state validation
        if target_state not in self.ALLOWED_SIGNAL_PLANS:
            res = {
                "status": "error",
                "code": "INVALID_TARGET_STATE",
                "message": f"Invalid target_state '{target_state}' for traffic_camera.",
                "allowed": list(self.ALLOWED_SIGNAL_PLANS),
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "set_signal_plan", args, False, "INVALID_TARGET_STATE", res
            )
            return res

        # 6. Idempotency check
        if idempotency_key:
            handled, replay = self.check_idempotency(idempotency_key, "set_signal_plan", args)
            if handled:
                if replay.get("status") == "error":
                    self.ledger.log_command(
                        self.run_id, self.get_sim_time(), caller, "set_signal_plan", args, False, replay.get("code"), replay
                    )
                return replay

        # 7. Execution & Actuator attack
        dev.reported_actuator_state["signal_plan"] = target_state
        actuator_attack = (dev.active_attack and dev.active_attack.get("capability") == "actuator")
        if actuator_attack:
            executed = False
            refused_code = "ACTUATOR_ATTACK"
        else:
            dev.actuator_state["signal_plan"] = target_state
            executed = True
            refused_code = None

        result = {"status": "ok", "device_id": device_id, "signal_plan": target_state}
        if idempotency_key:
            self.record_idempotency_result(idempotency_key, "set_signal_plan", args, result)

        self.ledger.log_command(
            self.run_id, self.get_sim_time(), caller, "set_signal_plan", args, executed, refused_code, result
        )
        return result

    def handle_set_valve(
        self,
        device_id: str,
        target_state: str,
        idempotency_key: Optional[str] = None,
        caller: str = "anonymous",
    ) -> Dict[str, Any]:
        """Handle water valve actuation."""
        args = {"device_id": device_id, "target_state": target_state}

        # 1. Length checks
        if len(device_id) > 64 or len(target_state) > 64 or (idempotency_key and len(idempotency_key) > 64):
            res = {
                "status": "error",
                "code": "INVALID_ARGUMENT",
                "message": "Argument string exceeds maximum length of 64 characters.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "set_valve", args, False, "INVALID_ARGUMENT", res
            )
            return res

        # 2. Device lookup
        dev = self.devices.get(device_id)
        if not dev:
            res = {
                "status": "error",
                "code": "DEVICE_NOT_FOUND",
                "message": f"Device '{device_id}' not found.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "set_valve", args, False, "DEVICE_NOT_FOUND", res
            )
            return res

        # 3. Device type validation
        if dev.device_type != "water_level":
            res = {
                "status": "error",
                "code": "WRONG_DEVICE_TYPE",
                "message": f"Device '{device_id}' is of type '{dev.device_type}', expected 'water_level'.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "set_valve", args, False, "WRONG_DEVICE_TYPE", res
            )
            return res

        # 4. Containment check
        if dev.status in ("isolated", "quarantined"):
            res = {
                "status": "error",
                "code": "DEVICE_CONTAINED",
                "message": f"Device '{device_id}' is currently {dev.status} and cannot be actuated.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "set_valve", args, False, "DEVICE_CONTAINED", res
            )
            return res

        # 5. Target state & ratio validation (open -> 1.0, half -> 0.5, closed -> 0.0, NN% -> NN/100)
        ratio: Optional[float] = None
        if target_state == "open":
            ratio = 1.0
        elif target_state == "half":
            ratio = 0.5
        elif target_state == "closed":
            ratio = 0.0
        elif target_state.endswith("%"):
            m = re.match(r"^(\d+(?:\.\d+)?)%$", target_state)
            if m:
                pct = float(m.group(1))
                if 0.0 <= pct <= 100.0:
                    ratio = pct / 100.0

        if ratio is None:
            res = {
                "status": "error",
                "code": "INVALID_TARGET_STATE",
                "message": f"Invalid target_state '{target_state}' for water_level.",
                "allowed": ["open", "half", "closed", "0%-100%"],
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "set_valve", args, False, "INVALID_TARGET_STATE", res
            )
            return res

        # 6. Idempotency check
        if idempotency_key:
            handled, replay = self.check_idempotency(idempotency_key, "set_valve", args)
            if handled:
                if replay.get("status") == "error":
                    self.ledger.log_command(
                        self.run_id, self.get_sim_time(), caller, "set_valve", args, False, replay.get("code"), replay
                    )
                return replay

        # 7. Execution & Actuator attack
        dev.reported_actuator_state["valve_position"] = target_state
        dev.reported_actuator_state["valve_ratio"] = ratio
        actuator_attack = (dev.active_attack and dev.active_attack.get("capability") == "actuator")
        if actuator_attack:
            executed = False
            refused_code = "ACTUATOR_ATTACK"
        else:
            dev.actuator_state["valve_position"] = target_state
            dev.actuator_state["valve_ratio"] = ratio
            executed = True
            refused_code = None

        result = {"status": "ok", "device_id": device_id, "valve_position": target_state}
        if idempotency_key:
            self.record_idempotency_result(idempotency_key, "set_valve", args, result)

        self.ledger.log_command(
            self.run_id, self.get_sim_time(), caller, "set_valve", args, executed, refused_code, result
        )
        return result

    def handle_switch_grid(
        self,
        device_id: str,
        target_state: str,
        idempotency_key: Optional[str] = None,
        caller: str = "anonymous",
    ) -> Dict[str, Any]:
        """Handle power grid switching actuation."""
        args = {"device_id": device_id, "target_state": target_state}

        # 1. Length checks
        if len(device_id) > 64 or len(target_state) > 64 or (idempotency_key and len(idempotency_key) > 64):
            res = {
                "status": "error",
                "code": "INVALID_ARGUMENT",
                "message": "Argument string exceeds maximum length of 64 characters.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "switch_grid", args, False, "INVALID_ARGUMENT", res
            )
            return res

        # 2. Device lookup
        dev = self.devices.get(device_id)
        if not dev:
            res = {
                "status": "error",
                "code": "DEVICE_NOT_FOUND",
                "message": f"Device '{device_id}' not found.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "switch_grid", args, False, "DEVICE_NOT_FOUND", res
            )
            return res

        # 3. Device type validation
        if dev.device_type != "power_load":
            res = {
                "status": "error",
                "code": "WRONG_DEVICE_TYPE",
                "message": f"Device '{device_id}' is of type '{dev.device_type}', expected 'power_load'.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "switch_grid", args, False, "WRONG_DEVICE_TYPE", res
            )
            return res

        # 4. Containment check
        if dev.status in ("isolated", "quarantined"):
            res = {
                "status": "error",
                "code": "DEVICE_CONTAINED",
                "message": f"Device '{device_id}' is currently {dev.status} and cannot be actuated.",
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "switch_grid", args, False, "DEVICE_CONTAINED", res
            )
            return res

        # 5. Target state validation
        if target_state not in self.ALLOWED_GRID_STATES:
            res = {
                "status": "error",
                "code": "INVALID_TARGET_STATE",
                "message": f"Invalid target_state '{target_state}' for power_load.",
                "allowed": list(self.ALLOWED_GRID_STATES),
            }
            self.ledger.log_command(
                self.run_id, self.get_sim_time(), caller, "switch_grid", args, False, "INVALID_TARGET_STATE", res
            )
            return res

        # 6. Idempotency check
        if idempotency_key:
            handled, replay = self.check_idempotency(idempotency_key, "switch_grid", args)
            if handled:
                if replay.get("status") == "error":
                    self.ledger.log_command(
                        self.run_id, self.get_sim_time(), caller, "switch_grid", args, False, replay.get("code"), replay
                    )
                return replay

        # 7. Execution & Actuator attack
        dev.reported_actuator_state["grid_state"] = target_state
        actuator_attack = (dev.active_attack and dev.active_attack.get("capability") == "actuator")
        if actuator_attack:
            executed = False
            refused_code = "ACTUATOR_ATTACK"
        else:
            dev.actuator_state["grid_state"] = target_state
            executed = True
            refused_code = None

        result = {"status": "ok", "device_id": device_id, "grid_state": target_state}
        if idempotency_key:
            self.record_idempotency_result(idempotency_key, "switch_grid", args, result)

        self.ledger.log_command(
            self.run_id, self.get_sim_time(), caller, "switch_grid", args, executed, refused_code, result
        )
        return result

    def get_command_result(self, idempotency_key: str) -> Dict[str, Any]:
        """Retrieve command actuation result by idempotency key."""
        if len(idempotency_key) > 64:
            return {
                "status": "error",
                "code": "INVALID_ARGUMENT",
                "message": "idempotency_key exceeds maximum length of 64 characters.",
            }
        if idempotency_key not in self.command_results:
            return {
                "status": "error",
                "code": "COMMAND_NOT_FOUND",
                "message": f"Command with idempotency key '{idempotency_key}' not found.",
            }
        cached = self.command_results[idempotency_key]
        self.command_results.move_to_end(idempotency_key)
        return cached["result"]

    # Containment Handlers (Layer 3 - Guardian)
    def update_device_status(
        self,
        device_id: str,
        new_status: str,
        tool_name: str = "update_device_status",
        caller: str = "anonymous",
    ) -> Dict[str, Any]:
        """Update device containment status (ok, isolated, quarantined)."""
        if len(device_id) > 64:
            return {
                "status": "error",
                "code": "INVALID_ARGUMENT",
                "message": "device_id exceeds maximum length of 64 characters.",
            }

        # 1. Registered device
        if device_id in self.devices:
            dev = self.devices[device_id]
            from_status = dev.status
            if new_status == "ok" and from_status == "ok":
                self.ledger.log_containment(self.run_id, self.get_sim_time(), caller, tool_name, device_id, "ok", "ok")
                return {"status": "ok", "device_id": device_id, "new_status": "ok", "note": "device already ok"}

            dev.status = new_status
            self.ledger.log_containment(self.run_id, self.get_sim_time(), caller, tool_name, device_id, from_status, new_status)
            return {"status": "ok", "device_id": device_id, "new_status": new_status}

        # 2. Active rogue device (quarantine or isolate)
        if new_status in ("quarantined", "isolated"):
            matching_atk_id = None
            for atk_id, rogue_info in list(self.rogue_attacks.items()):
                r_dev = rogue_info.get("params", {}).get("rogue_device_id")
                if r_dev == device_id:
                    matching_atk_id = atk_id
                    break

            if matching_atk_id:
                # Revoke network authority: stops emitting rogue readings from next tick
                self.clear_attack(matching_atk_id)
                self.ledger.log_containment(self.run_id, self.get_sim_time(), caller, tool_name, device_id, "ok", new_status)
                return {"status": "ok", "device_id": device_id, "blocked": True, "new_status": new_status}

        return {
            "status": "error",
            "code": "DEVICE_NOT_FOUND",
            "message": f"Device '{device_id}' not found.",
        }

    def rollback_reading(self, reading_id: str, caller: str = "anonymous") -> Dict[str, Any]:
        """Mark a specific reading as rolled back."""
        if len(reading_id) > 64:
            return {
                "status": "error",
                "code": "INVALID_ARGUMENT",
                "message": "reading_id exceeds maximum length of 64 characters.",
            }

        reading = self.recent_readings_buffer.get(reading_id)
        if not reading:
            return {
                "status": "error",
                "code": "READING_NOT_FOUND",
                "message": f"Reading {reading_id} not found in the reading cache: unknown id, or older than the last {self.reading_cache_max} readings",
            }
        from_status = reading.get("status", "ok")
        reading["status"] = "rolled_back"
        self.ledger.log_containment(
            self.run_id,
            self.get_sim_time(),
            caller,
            "rollback_reading",
            reading_id,
            from_status,
            "rolled_back",
        )
        return {"status": "ok", "reading_id": reading_id, "reading_status": "rolled_back"}

    def close(self) -> None:
        """Cleanly terminate simulator, record run end in ledger, and close connections."""
        if hasattr(self, "ledger") and self.ledger:
            try:
                self.ledger.end_run(self.run_id, self.get_sim_time())
            except Exception as e:
                logger.warning(f"Failed to record run end: {e}")
            try:
                self.ledger.close()
            except Exception:
                pass


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
