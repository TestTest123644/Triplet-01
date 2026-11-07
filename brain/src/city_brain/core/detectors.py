"""Per-domain findings: what each channel says on its own, and what its neighbours say about it.

A `Finding` is one reading of the world a detector is prepared to defend (spec 8.3.1). It
carries a `direction`: +1 corroborates the incident it belongs to, -1 contradicts it. The
assessor (spec 8.3.2) turns findings into belief; nothing here decides anything.

Detectors are partner-agnostic: siblings are devices of the same kind in the same zone, and
the air-quality coupling is "this zone's air follows this zone's cameras". Every threshold
comes from `policy`.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from city_brain.core.channel_filter import LagFilter
from city_brain.core.estimator import WaterFilter
from city_brain.domain.types import DeviceProfile, DeviceStatus, SensorKind

GATED = frozenset({"IMPLAUSIBLE_JUMP", "STUCK", "DROPOUT"})


@dataclass(frozen=True)
class Finding:
    device_id: str
    zone_id: str
    kind: str
    direction: Literal[1, -1]
    value: float
    score: float
    sim_time: datetime

    @property
    def sort_key(self) -> tuple[str, str]:
        return (self.device_id, self.kind)


FLOOR = 0.7  # a threshold that has been crossed is already most of the story


def _score(excess: float, span: float) -> float:
    """`FLOOR` just past a threshold, rising to 1.0 a full span beyond it."""
    if span <= 0.0:
        return 1.0
    return min(1.0, FLOOR + (1.0 - FLOOR) * max(0.0, excess) / span)


def detect_findings(
    zone_id: str,
    registry: Mapping[str, DeviceProfile],
    water_filters: Mapping[str, WaterFilter],
    lag_filters: Mapping[str, LagFilter],
    policy: Mapping,
    tick: int,
    *,
    now: datetime,
) -> list[Finding]:
    """Every finding the zone's channels support right now, in a deterministic order.

    `now` is the sim time the findings are stamped with; the core has no clock of its own.
    """
    devices = [
        registry[device_id] for device_id in sorted(registry)
        if registry[device_id].zone_id == zone_id and registry[device_id].status is DeviceStatus.OK
    ]
    findings: list[Finding] = []
    for device in devices:
        if device.kind is SensorKind.WATER_LEVEL:
            findings.extend(_water(device, water_filters.get(device.device_id), policy, tick, now))
        elif device.kind is SensorKind.TRAFFIC_FLOW:
            findings.extend(_traffic(device, lag_filters.get(device.device_id), policy, tick, now))
        elif device.kind is SensorKind.POWER_LOAD:
            findings.extend(_power(device, lag_filters.get(device.device_id), policy, tick, now))
        elif device.kind is SensorKind.CALL_RATE:
            findings.extend(_calls(device, lag_filters.get(device.device_id), policy, tick, now))
    findings.extend(_traffic_contradictions(devices, lag_filters, findings, policy, tick, now))
    findings.extend(_air_quality(devices, lag_filters, policy, tick, now))
    return sorted(findings, key=lambda f: f.sort_key)


# ----- water (spec 8.3.1) -----


def _water(
    device: DeviceProfile, f: WaterFilter | None, policy: Mapping, tick: int, now: datetime
) -> list[Finding]:
    if f is None or not f.initialised or f.is_stale(tick) or f.flags & GATED:
        return []
    water, detectors = policy["water"], policy["detectors"]["water"]
    high, critical = float(water["high_band"]), float(water["critical_band"])
    level = f.level
    out = []
    if level >= critical:
        out.append(_finding(device, "LEVEL_CRITICAL", 1, level, 1.0, now))
    if level >= high:
        out.append(_finding(device, "LEVEL_HIGH", 1, level, _score(level - high, critical - high), now))
    rate = f.rate
    horizon = float(detectors["forecast_s"])
    if rate > float(detectors["rising_rate"]) and level + rate * horizon >= high:
        out.append(_finding(device, "RISING", 1, level, _score(rate, float(detectors["rising_rate"])), now))
    return out


# ----- traffic -----


def _traffic(
    device: DeviceProfile, f: LagFilter | None, policy: Mapping, tick: int, now: datetime
) -> list[Finding]:
    if not _usable(f, tick):
        return []
    detectors = policy["detectors"]["traffic"]
    z = f.zscore()
    level, floor = f.value, float(detectors["anomaly_level"])
    threshold = float(detectors["anomaly_z"])
    if z is None or z < threshold or level < floor:
        return []
    return [_finding(device, "FLOW_ANOMALY", 1, level, _score(z - threshold, threshold), now)]


def _traffic_contradictions(
    devices, lag_filters: Mapping[str, LagFilter], findings, policy: Mapping, tick: int, now: datetime
) -> list[Finding]:
    """A camera reading normal while its sibling screams is evidence against the anomaly."""
    anomalous = {f.device_id for f in findings if f.kind == "FLOW_ANOMALY"}
    if not anomalous:
        return []
    out = []
    for device in devices:
        if device.kind is not SensorKind.TRAFFIC_FLOW or device.device_id in anomalous:
            continue
        f = lag_filters.get(device.device_id)
        if not _usable(f, tick):
            continue
        out.append(_finding(device, "FLOW_NORMAL", -1, f.value, 1.0, now))
    return out


# ----- power -----


def _power(
    device: DeviceProfile, f: LagFilter | None, policy: Mapping, tick: int, now: datetime
) -> list[Finding]:
    if f is None or not f.initialised or f.is_stale(tick):
        return []
    detectors = policy["detectors"]["power"]
    out = []
    if "IMPLAUSIBLE_JUMP" in f.flags:
        out.append(_finding(device, "LOAD_JUMP", 1, f.level, 1.0, now))
    if f.flags & GATED:  # a jumped or stuck channel's level is not evidence of a real load
        return out
    high = float(detectors["load_high"])
    if f.value >= high:
        out.append(_finding(device, "LOAD_HIGH", 1, f.value, _score(f.value - high, high * 0.2), now))
    return out


# ----- air quality -----


def _air_quality(
    devices, lag_filters: Mapping[str, LagFilter], policy: Mapping, tick: int, now: datetime
) -> list[Finding]:
    detectors = policy["detectors"]["air_quality"]
    high = float(detectors["aqi_high"])
    cameras = [d for d in devices if d.kind is SensorKind.TRAFFIC_FLOW]
    out = []
    for device in devices:
        if device.kind is not SensorKind.AIR_QUALITY:
            continue
        f = lag_filters.get(device.device_id)
        if not _usable(f, tick) or f.value < high:
            continue
        out.append(_finding(device, "AQI_HIGH", 1, f.value, _score(f.value - high, high * 0.3), now))
        rose = _rose(cameras, lag_filters, detectors, tick)
        if rose is not None:
            out.append(_finding(device, "AQ_TRAFFIC_COUPLED", 1 if rose else -1, f.value, 1.0, now))
    return out


def _rose(cameras, lag_filters, detectors: Mapping, tick: int) -> bool | None:
    """Whether the zone's cameras rose over the window its air lags behind them by, or None."""
    slopes = []
    for camera in cameras:
        f = lag_filters.get(camera.device_id)
        if not _usable(f, tick):
            continue
        lag_ticks = round(float(detectors["traffic_lag_s"]) / f.period_s)
        slope = f.observed_slope(tick - 3 * lag_ticks, until_tick=tick - lag_ticks)
        if slope is not None:
            slopes.append(slope)
    if not slopes:
        return None
    return max(slopes) >= float(detectors["traffic_rise"])


# ----- emergency calls -----


def _calls(
    device: DeviceProfile, f: LagFilter | None, policy: Mapping, tick: int, now: datetime
) -> list[Finding]:
    if not _usable(f, tick):
        return []
    detectors = policy["detectors"]["calls"]
    baseline, floor = f.baseline_mean, float(detectors["surge_min"])
    factor = float(detectors["surge_factor"])
    if baseline != baseline:  # NaN: no baseline yet
        return []
    threshold = max(factor * baseline, floor)
    if f.value < threshold:
        return []
    return [_finding(device, "CALL_SURGE", 1, f.value, _score(f.value - threshold, threshold), now)]


# ----- helpers -----


def _usable(f: LagFilter | None, tick: int) -> bool:
    """A channel is evidence only when it is initialised, fresh and not flagged."""
    return f is not None and f.initialised and not f.is_stale(tick) and not f.flags & GATED


def _finding(
    device: DeviceProfile, kind: str, direction: int, value: float, score: float, now: datetime
) -> Finding:
    return Finding(
        device_id=device.device_id, zone_id=device.zone_id, kind=kind, direction=direction,
        value=float(value), score=float(score), sim_time=now,
    )
