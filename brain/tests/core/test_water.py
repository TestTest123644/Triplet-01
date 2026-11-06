from datetime import timedelta
from types import SimpleNamespace

import pytest

from city_brain.config import load_config
from city_brain.core.water import WaterContext, plan_water_t0, water_hold_reason
from city_brain.core.estimator import WaterFilter
from city_brain.domain.ids import action_id
from city_brain.domain.types import RiskClass, StateKind, TrustState, EvidenceRef
from tests.factories import T0

CFG = load_config("config/standalone.yaml")
POLICY = {**CFG.policy, "filter": {"stale_s": 2.0}}
DEVICE = "z1-wl1"


def fake_filter(level, flags=(), initialised=True):
    return SimpleNamespace(level=level, flags=frozenset(flags), initialised=initialised)


def refs(n=5, last_at=T0):
    return tuple(
        EvidenceRef(
            reading_id=f"r-{i}", sig_hash="0123456789abcdef", device_id=DEVICE, value=5.0,
            sim_time=last_at - timedelta(seconds=0.1 * (n - 1 - i)), state_kind=StateKind.VALIDATED,
            verdict_id=f"v-{i}", trust_state=TrustState.TRUSTED, weight=1.0,
        )
        for i in range(n)
    )


def ctx(level, valve="closed", flags=(), below_for_s=None, initialised=True, recent=None, now=T0, plan_version=1):
    return WaterContext(
        device_id=DEVICE, zone_id="zone-1", filter=fake_filter(level, flags, initialised),
        valve_state=valve, recent=refs() if recent is None else recent,
        below_low_since=None if below_for_s is None else now - timedelta(seconds=below_for_s),
        now=now, plan_version=plan_version,
    )


def test_opens_at_high_band():
    i = plan_water_t0(ctx(level=6.01, valve="closed"), POLICY)
    assert i.state == "open" and i.risk_class is RiskClass.R1 and i.expected_effect.threshold == -0.01
    assert i.expected_effect.channel == DEVICE and i.expected_effect.metric == "rate"
    assert i.expected_effect.comparator == "<=" and i.expected_effect.window_s == 3.0
    assert i.incident_id == f"flood-risk:{DEVICE}:1" and i.capability == "valve" and i.target == DEVICE
    assert i.action_id == action_id(i.incident_id, "valve", DEVICE, "open", 1)
    assert i.evidence == refs()[-3:] and isinstance(i.evidence, tuple)
    assert i.rationale and [a.option for a in i.alternatives] == ["hold"]
    assert i.alternatives[0].rejected_because == "level at or above high band and rising"


def test_no_open_below_high_band():
    assert plan_water_t0(ctx(level=5.99, valve="closed"), POLICY) is None


def test_ceiling_with_closed_valve_opens():
    assert plan_water_t0(ctx(level=12.0, valve="closed", flags={"CEILING"}), POLICY).state == "open"


def test_ceiling_below_band_opens():
    assert plan_water_t0(ctx(level=4.0, valve="closed", flags={"CEILING"}), POLICY).state == "open"


def test_open_valve_high_level_no_intent():
    assert plan_water_t0(ctx(level=7.0, valve="open"), POLICY) is None


def test_close_needs_sustained_low():
    assert plan_water_t0(ctx(level=2.9, valve="open", below_for_s=4.9), POLICY) is None
    i = plan_water_t0(ctx(level=2.9, valve="open", below_for_s=5.0), POLICY)
    assert i.risk_class is RiskClass.R2 and i.state == "closed"
    assert i.expected_effect.comparator == ">=" and i.expected_effect.threshold == 0.005
    assert i.expected_effect.window_s == 5.0
    assert i.alternatives[0].option == "keep open"
    assert i.alternatives[0].rejected_because == "level below low band for close_sustain_s"


def test_close_without_below_low_since_does_not_fire():
    assert plan_water_t0(ctx(level=2.9, valve="open", below_for_s=None), POLICY) is None


def test_close_not_when_level_above_low_band():
    assert plan_water_t0(ctx(level=3.5, valve="open", below_for_s=10.0), POLICY) is None


def test_closed_valve_low_level_no_intent():
    assert plan_water_t0(ctx(level=2.0, valve="closed", below_for_s=10.0), POLICY) is None


@pytest.mark.parametrize("flag", ["STUCK", "IMPLAUSIBLE_JUMP", "MODEL_MISMATCH"])
def test_flagged_channel_never_planned(flag):
    assert plan_water_t0(ctx(level=8.0, valve="closed", flags={flag}), POLICY) is None
    assert plan_water_t0(ctx(level=2.0, valve="open", below_for_s=9.0, flags={flag}), POLICY) is None
    assert water_hold_reason(ctx(level=8.0, flags={flag}), POLICY) == flag


def test_model_mismatch_level_8_closed_valve():
    c = ctx(level=8.0, valve="closed", flags={"MODEL_MISMATCH"})
    assert plan_water_t0(c, POLICY) is None
    assert water_hold_reason(c, POLICY) == "MODEL_MISMATCH"


def test_uninitialised_filter_never_planned():
    c = ctx(level=float("nan"), valve="closed", initialised=False, flags={"CEILING"})
    assert plan_water_t0(c, POLICY) is None
    assert water_hold_reason(c, POLICY) == "STALE"


def test_stale_by_age_of_last_evidence():
    c = ctx(level=8.0, recent=refs(last_at=T0 - timedelta(seconds=2.0)))
    assert plan_water_t0(c, POLICY) is None and water_hold_reason(c, POLICY) == "STALE"
    fresh = ctx(level=8.0, recent=refs(last_at=T0 - timedelta(seconds=1.9)))
    assert water_hold_reason(fresh, POLICY) is None


def test_no_evidence_is_stale():
    c = ctx(level=8.0, recent=())
    assert plan_water_t0(c, POLICY) is None and water_hold_reason(c, POLICY) == "STALE"


def test_hold_reason_priority():
    c = ctx(level=8.0, flags={"MODEL_MISMATCH", "IMPLAUSIBLE_JUMP", "STUCK"})
    assert water_hold_reason(c, POLICY) == "STUCK"
    c = ctx(level=8.0, flags={"MODEL_MISMATCH", "IMPLAUSIBLE_JUMP"})
    assert water_hold_reason(c, POLICY) == "IMPLAUSIBLE_JUMP"
    c = ctx(level=8.0, flags={"STUCK"}, initialised=False)
    assert water_hold_reason(c, POLICY) == "STALE"
    assert water_hold_reason(ctx(level=8.0, flags={"CEILING"}), POLICY) is None


def test_real_filter_accepted():
    f = WaterFilter(CFG.params["filter"])
    f.update(value=6.5, tick=1, weight=1.0, u=0.0)
    c = WaterContext(DEVICE, "zone-1", f, "closed", refs(), None, T0, 2)
    assert plan_water_t0(c, POLICY).incident_id == f"flood-risk:{DEVICE}:2"
