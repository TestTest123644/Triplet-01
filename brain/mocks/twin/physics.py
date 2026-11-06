"""The Twin's per-tick physics (spec 2.2), keyed by canonical sensor kind. Mirrors twin/simulation.py."""

import math
import random
from datetime import datetime

DT = 0.1  # seconds per tick

PLAN_FACTORS = {"green_wave": 1.35, "red_restriction": 0.55}
GRID_FACTORS = {"grid_b": 0.85, "shed": 0.40, "standby": 0.20}


def valve_ratio(target_state: str) -> float:
    """Valve ratio the Twin derives from a target_state string (handle_set_valve, as written).

    "open" -> 1.0; a string ending in '%' is parsed with float() (ValueError leaves the default 0.0);
    the result is clamped with max(0.0, min(1.0, x)), so NaN becomes 1.0 and unknown strings close it.
    """
    ratio = 1.0 if target_state == "open" else 0.0
    try:
        if target_state.endswith("%"):
            ratio = float(target_state.rstrip("%")) / 100.0
    except ValueError:
        pass
    return max(0.0, min(1.0, ratio))


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def diurnal(sim_dt: datetime) -> tuple[float, float, float]:
    """(traffic wave, power wave, normalised time of day) at a simulation instant."""
    sec = sim_dt.hour * 3600 + sim_dt.minute * 60 + sim_dt.second + sim_dt.microsecond / 1_000_000.0
    t = (sec % 86400) / 86400.0
    traffic = 0.5 + 0.35 * math.sin(2 * math.pi * (t - 0.25)) + 0.15 * math.sin(4 * math.pi * (t - 0.2))
    power = 0.55 + 0.3 * math.sin(2 * math.pi * (t - 0.3)) + 0.15 * math.cos(2 * math.pi * t)
    return traffic, power, t


def traffic(v: float, plan: str, d_traffic: float, rng: random.Random) -> float:
    target = (40.0 + 80.0 * _clamp01(d_traffic)) * PLAN_FACTORS.get(plan, 1.0)
    noise = rng.gauss(0.0, 1.2)
    return v + 0.05 * (target - v) + noise * DT


def water(v: float, valve_ratio: float, t_norm: float, rng: random.Random) -> float:
    inflow = 0.015 * (1.0 + 0.2 * math.sin(2 * math.pi * t_norm * 4))
    outflow = 0.045 * valve_ratio
    noise = rng.gauss(0.0, 0.02)
    return v + (inflow - outflow) * DT + noise * DT


def power(v: float, grid_state: str, d_power: float, rng: random.Random) -> float:
    target = (30.0 + 55.0 * _clamp01(d_power)) * GRID_FACTORS.get(grid_state, 1.0)
    noise = rng.gauss(0.0, 0.5)
    return v + 0.08 * (target - v) + noise * DT


def air_quality(v: float, zone_traffic: list[float], rng: random.Random) -> float:
    avg = sum(zone_traffic) / len(zone_traffic) if zone_traffic else 50.0
    target = 20.0 + avg * 0.45
    noise = rng.gauss(0.0, 0.4)
    return v + 0.02 * (target - v) + noise * DT


def emergency_calls(v: float, dispatches: int, rng: random.Random) -> float:
    target = max(0.2, 0.8 - dispatches * 0.3) if dispatches > 0 else 1.2
    noise = max(0.0, rng.gauss(0.0, 0.15))
    return max(0.0, v + 0.1 * (target - v) + noise * DT)


# canonical SensorKind name -> channel
CHANNELS: dict[str, str] = {
    "TRAFFIC_FLOW": "traffic", "WATER_LEVEL": "water", "POWER_LOAD": "power",
    "AIR_QUALITY": "air_quality", "CALL_RATE": "calls",
}
