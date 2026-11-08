"""Per-domain findings across all five channels (spec 8.3.1)."""

import pytest

from city_brain.config import load_config
from city_brain.core.channel_filter import LagFilter, lag_params
from city_brain.core.detectors import Finding, detect_findings
from city_brain.core.estimator import WaterFilter
from city_brain.domain.types import (
    ActuatorCapability,
    CanonicalUnit,
    DeviceProfile,
    DeviceStatus,
    SensorKind,
)
from tests.factories import T0

CFG = load_config("config/standalone.yaml")
POLICY, PARAMS = CFG.policy, CFG.params
PERIOD_S = PARAMS["filter"]["sample_period_s"]
ZONE = "zone-1"
NOISE = 0.05  # a steady channel still dithers; a perfectly flat one is STUCK

UNITS = {
    SensorKind.WATER_LEVEL: CanonicalUnit.M,
    SensorKind.TRAFFIC_FLOW: CanonicalUnit.VEH_PER_MIN,
    SensorKind.POWER_LOAD: CanonicalUnit.MW,
    SensorKind.AIR_QUALITY: CanonicalUnit.AQI,
    SensorKind.CALL_RATE: CanonicalUnit.CALLS_PER_MIN,
}
RANGES = {
    SensorKind.WATER_LEVEL: (0.0, 12.0),
    SensorKind.TRAFFIC_FLOW: (0.0, 200.0),
    SensorKind.POWER_LOAD: (5.0, 120.0),
    SensorKind.AIR_QUALITY: (10.0, 350.0),
    SensorKind.CALL_RATE: (0.0, 30.0),
}


def device(device_id, kind, status=DeviceStatus.OK, zone_id=ZONE, actuator=None):
    low, high = RANGES[kind]
    return DeviceProfile(
        device_id=device_id, zone_id=zone_id, kind=kind, unit=UNITS[kind],
        min_val=low, max_val=high, status=status, actuator=actuator,
    )


class City:
    """A zone's registry plus the filters its readings have built."""

    def __init__(self):
        self.registry: dict[str, DeviceProfile] = {}
        self.water: dict[str, WaterFilter] = {}
        self.lag: dict[str, LagFilter] = {}
        self.tick = 0

    def add(self, device_id, kind, **kw):
        self.registry[device_id] = device(device_id, kind, **kw)
        return self

    def hold(self, device_id, value, ticks=60, weight=1.0, **kw):
        """Feed one channel a value for `ticks` ticks. A flat channel would read as STUCK,
        so a steady value is dithered the way a real sensor's noise dithers it."""
        profile = self.registry[device_id]
        for n in range(1, ticks + 1):
            level = value(n) if callable(value) else value + (NOISE if n % 2 else -NOISE)
            self._update(profile, device_id, level, n, weight, **kw)
        self.tick = max(self.tick, ticks)
        return self

    def push(self, device_id, values, weight=1.0, **kw):
        profile = self.registry[device_id]
        for value in values:
            self.tick += 1
            self._update(profile, device_id, value, self.tick, weight, **kw)
        return self

    def _update(self, profile, device_id, value, tick, weight, u=0.0, at_ceiling=False):
        if profile.kind is SensorKind.WATER_LEVEL:
            f = self.water.setdefault(device_id, WaterFilter(PARAMS["filter"]))
            f.update(value, tick, weight, u=u, at_ceiling=at_ceiling)
        else:
            low, high = RANGES[profile.kind]
            f = self.lag.setdefault(
                device_id, LagFilter(lag_params(PARAMS, profile.kind), min_val=low, max_val=high))
            f.update(value, tick, weight, at_ceiling=at_ceiling)

    def findings(self, zone_id=ZONE, tick=None, policy=POLICY):
        return detect_findings(
            zone_id, self.registry, self.water, self.lag, policy,
            tick if tick is not None else self.tick, now=T0,
        )

    def kinds(self, zone_id=ZONE, tick=None, policy=POLICY):
        return [f.kind for f in self.findings(zone_id, tick, policy=policy)]


def one(findings, kind) -> Finding:
    matches = [f for f in findings if f.kind == kind]
    assert len(matches) == 1, [f"{f.device_id}:{f.kind}" for f in findings]
    return matches[0]


# ----- water -----


def water_city(level, ticks=60):
    return City().add("w1", SensorKind.WATER_LEVEL).hold("w1", level, ticks)


def test_a_calm_tank_produces_nothing():
    assert water_city(4.0).kinds() == []


def test_level_high_at_the_high_band():
    findings = water_city(6.1).findings()
    assert one(findings, "LEVEL_HIGH").direction == 1
    assert "LEVEL_CRITICAL" not in [f.kind for f in findings]


def test_level_critical_at_the_critical_band():
    findings = water_city(9.2).findings()
    assert {f.kind for f in findings} == {"LEVEL_HIGH", "LEVEL_CRITICAL"}
    assert one(findings, "LEVEL_CRITICAL").score == pytest.approx(1.0)


def test_level_high_scores_higher_closer_to_critical():
    near = one(water_city(6.1).findings(), "LEVEL_HIGH").score
    far = one(water_city(8.5).findings(), "LEVEL_HIGH").score
    assert far > near


def test_rising_when_the_forecast_crosses_the_high_band():
    city = City().add("w1", SensorKind.WATER_LEVEL)
    city.push("w1", [5.0 + 0.02 * PERIOD_S * n for n in range(1, 301)])
    assert "RISING" in city.kinds()


def test_not_rising_when_the_forecast_stays_below_the_band():
    city = City().add("w1", SensorKind.WATER_LEVEL)
    city.push("w1", [3.0 + 0.006 * PERIOD_S * n for n in range(1, 301)])
    assert "RISING" not in city.kinds()


def test_a_falling_tank_is_not_rising():
    city = City().add("w1", SensorKind.WATER_LEVEL)
    city.push("w1", [8.0 - 0.02 * PERIOD_S * n for n in range(1, 301)], u=1.0)
    assert "RISING" not in city.kinds()


def test_an_uninitialised_channel_produces_nothing():
    city = City().add("w1", SensorKind.WATER_LEVEL).hold("w1", 9.5, 30, weight=0.0)
    assert city.kinds() == []


def test_a_contained_device_produces_nothing():
    city = City().add("w1", SensorKind.WATER_LEVEL, status=DeviceStatus.QUARANTINED)
    city.hold("w1", 9.5, 60)
    assert city.kinds() == []


def test_a_stale_channel_produces_nothing():
    city = water_city(9.5, ticks=60)
    assert city.kinds(tick=10_000) == []


def test_another_zone_is_not_reported():
    city = City().add("w1", SensorKind.WATER_LEVEL, zone_id="zone-2").hold("w1", 9.5, 60)
    assert city.kinds(zone_id=ZONE) == []
    assert city.kinds(zone_id="zone-2") != []


# ----- traffic -----


def traffic_city(flows, baseline=45.0, ticks=600):
    city = City()
    for device_id in flows:
        city.add(device_id, SensorKind.TRAFFIC_FLOW)
    for device_id in flows:
        city.hold(device_id, lambda n: baseline + (1.0 if n % 2 else -1.0), ticks)
    city.tick = ticks
    for n in range(1, 61):  # every camera keeps reporting: a silent one would just be stale
        for device_id, flow in flows.items():
            base = baseline if flow is None else flow
            level = base + (NOISE if n % 2 else -NOISE)
            city._update(city.registry[device_id], device_id, level, ticks + n, 1.0)
    city.tick = ticks + 60
    return city


def test_calm_traffic_produces_nothing():
    assert traffic_city({"t1": None, "t2": None}).kinds() == []


def test_a_single_high_camera_is_an_anomaly_and_its_sibling_contradicts_it():
    findings = traffic_city({"t1": 160.0, "t2": None}).findings()
    anomaly = one(findings, "FLOW_ANOMALY")
    normal = one(findings, "FLOW_NORMAL")
    assert anomaly.device_id == "t1" and anomaly.direction == 1
    assert normal.device_id == "t2" and normal.direction == -1


def test_both_cameras_high_is_an_anomaly_with_no_contradiction():
    findings = traffic_city({"t1": 160.0, "t2": 155.0}).findings()
    assert {f.device_id for f in findings if f.kind == "FLOW_ANOMALY"} == {"t1", "t2"}
    assert [f for f in findings if f.kind == "FLOW_NORMAL"] == []


def test_a_high_level_without_a_z_score_is_not_an_anomaly():
    # The baseline itself sits high, so a high reading is normal for this camera.
    city = traffic_city({"t1": None, "t2": None}, baseline=150.0)
    assert city.kinds() == []


def test_a_z_score_without_a_high_level_is_not_an_anomaly():
    assert traffic_city({"t1": 60.0, "t2": None}).kinds() == []


def test_no_contradiction_without_a_sibling():
    assert traffic_city({"t1": 160.0}).kinds() == ["FLOW_ANOMALY"]


# ----- power -----


def power_city(load, ticks=120, **kw):
    return City().add("p1", SensorKind.POWER_LOAD).hold("p1", load, ticks, **kw)


def test_a_normal_grid_produces_nothing():
    assert power_city(68.0).kinds() == []


def test_load_high_above_the_threshold():
    assert one(power_city(95.0).findings(), "LOAD_HIGH").direction == 1


def test_triplet_policy_midday_peak_threshold():
    triplet_cfg = load_config("config/triplet.yaml")
    assert triplet_cfg.policy["detectors"]["power"]["load_high"] == 105.0
    city = City().add("p1", SensorKind.POWER_LOAD).hold("p1", 99.0, 120)
    assert city.kinds(policy=triplet_cfg.policy) == []
    city_over = City().add("p1", SensorKind.POWER_LOAD).hold("p1", 110.0, 120)
    assert one(city_over.findings(policy=triplet_cfg.policy), "LOAD_HIGH").direction == 1


def test_a_stolen_key_spike_is_a_load_jump():
    city = power_city(68.0)
    city.push("p1", [118.0])
    findings = city.findings()
    assert one(findings, "LOAD_JUMP").direction == 1


def test_a_sustained_jump_reports_both_the_jump_and_the_level():
    city = power_city(68.0)
    city.push("p1", [118.0, 117.9, 118.1, 118.0])
    assert "LOAD_JUMP" in city.kinds()


# ----- air quality -----


def aq_city(aqi, traffic=None, ticks=600):
    city = City().add("a1", SensorKind.AIR_QUALITY)
    city.add("t1", SensorKind.TRAFFIC_FLOW).add("t2", SensorKind.TRAFFIC_FLOW)
    city.hold("a1", 60.0, ticks)
    for device_id in ("t1", "t2"):
        city.hold(device_id, lambda n: 45.0 + (1.0 if n % 2 else -1.0), ticks)
    city.tick = ticks
    rise = traffic is not None
    for n in range(1, 101):
        tick = ticks + n
        city._update(city.registry["a1"], "a1", aqi + (NOISE if n % 2 else -NOISE), tick, 1.0)
        for device_id in ("t1", "t2"):
            flow = 45.0 + (traffic * n if rise else (1.0 if n % 2 else -1.0))
            city._update(city.registry[device_id], device_id, flow, tick, 1.0)
    city.tick = ticks + 100
    return city


def test_clean_air_produces_nothing():
    assert aq_city(60.0).kinds() == []


def test_aqi_high_above_the_threshold():
    assert "AQI_HIGH" in aq_city(160.0).kinds()


def test_high_aqi_with_calm_cameras_is_contradicted():
    findings = aq_city(160.0).findings()
    coupled = one(findings, "AQ_TRAFFIC_COUPLED")
    assert coupled.direction == -1 and coupled.device_id == "a1"


def test_high_aqi_after_traffic_rose_is_corroborated():
    findings = aq_city(160.0, traffic=0.5).findings()
    assert one(findings, "AQ_TRAFFIC_COUPLED").direction == 1


def test_no_coupling_finding_without_high_aqi():
    assert "AQ_TRAFFIC_COUPLED" not in aq_city(60.0, traffic=0.5).kinds()


# ----- emergency calls -----


def calls_city(rate, baseline=1.0, ticks=600):
    city = City().add("c1", SensorKind.CALL_RATE)
    city.hold("c1", lambda n: baseline + (0.05 if n % 2 else -0.05), ticks)
    city.tick = ticks
    for n in range(1, 101):
        city._update(
            city.registry["c1"], "c1", rate + (NOISE if n % 2 else -NOISE), ticks + n, 1.0)
    city.tick = ticks + 100
    return city


def test_a_quiet_dispatch_centre_produces_nothing():
    assert calls_city(1.0).kinds() == []


def test_a_call_surge_is_reported():
    assert one(calls_city(5.0).findings(), "CALL_SURGE").direction == 1


def test_a_surge_must_clear_the_floor_as_well_as_the_baseline():
    assert calls_city(3.5, baseline=0.5).kinds() == []


def test_a_busy_baseline_is_not_a_surge():
    assert calls_city(5.0, baseline=4.0).kinds() == []


# ----- shape -----


def test_findings_are_sorted_and_carry_the_zone_and_time():
    findings = traffic_city({"t1": 160.0, "t2": None}).findings()
    assert findings == sorted(findings, key=lambda f: (f.device_id, f.kind))
    assert all(f.zone_id == ZONE and f.sim_time == T0 for f in findings)
    assert all(0.0 <= f.score <= 1.0 for f in findings)


def test_a_device_without_a_filter_is_skipped():
    city = City().add("w1", SensorKind.WATER_LEVEL)
    assert city.findings(tick=0) == []
