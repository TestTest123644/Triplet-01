from datetime import timedelta

from hypothesis import given, strategies as st

from city_brain.core.epochs import start_new_epoch
from city_brain.core.events import ReadingReceived, VerdictReceived
from city_brain.core.state import BrainState
from city_brain.core.step import step
from city_brain.core.trust import (
    VerdictLedger,
    estimate_weight,
    evidence_ref,
    meets_requirement,
    trust_weight,
)
from city_brain.domain.types import DeviceStatus, RiskClass, StateKind, TrustState
from tests.factories import T0, make_observation, make_verdict

CAPS = {"TRUSTED": 1.0, "DOUBTFUL": 0.5, "UNTRUSTED": 0.0, "UNKNOWN": 0.0}


def ref(trust_state: TrustState):
    obs = make_observation()
    verdict = make_verdict(state=trust_state)
    return evidence_ref(obs, verdict, trust_weight(verdict, DeviceStatus.OK, CAPS))


def test_verdict_before_reading_is_applied():
    led = VerdictLedger()
    v = make_verdict(reading_id="r-1", sig_hash="h")
    led.add(v)
    assert led.lookup("r-1", "h", now=v.issued_at) == v


def test_wrong_sig_hash_does_not_match():
    led = VerdictLedger()
    led.add(make_verdict(reading_id="r-1", sig_hash="h"))
    assert led.lookup("r-1", "other", now=T0) is None


def test_expired_verdict_is_none():
    v = make_verdict(expires_at=T0 + timedelta(seconds=30))
    led = VerdictLedger()
    led.add(v)
    assert led.lookup(v.reading_id, v.sig_hash, now=T0 + timedelta(seconds=31)) is None
    assert led.lookup(v.reading_id, v.sig_hash, now=T0 + timedelta(seconds=30)) == v


def test_latest_issued_at_wins_and_reports_change():
    led = VerdictLedger()
    old = make_verdict(verdict_id="a", state=TrustState.TRUSTED)
    new = make_verdict(verdict_id="b", state=TrustState.UNTRUSTED, trust=0.0, issued_at=T0 + timedelta(seconds=1))
    assert led.add(old) == [("r-1", "0123456789abcdef")]
    assert led.add(new) == [("r-1", "0123456789abcdef")]
    assert led.add(old) == []
    assert led.lookup("r-1", "0123456789abcdef", T0) == new


def test_tie_keeps_later_arrival():
    led = VerdictLedger()
    led.add(make_verdict(verdict_id="a", state=TrustState.TRUSTED))
    down = make_verdict(verdict_id="b", state=TrustState.UNTRUSTED, trust=0.0)
    assert led.add(down) == [("r-1", "0123456789abcdef")]
    assert led.lookup("r-1", "0123456789abcdef", T0) == down


def test_identical_trust_reports_no_change():
    led = VerdictLedger()
    led.add(make_verdict(verdict_id="a"))
    assert led.add(make_verdict(verdict_id="b", issued_at=T0 + timedelta(seconds=1))) == []


def test_prune_drops_old_and_keeps_recent():
    led = VerdictLedger()
    led.add(make_verdict(reading_id="old", issued_at=T0, expires_at=T0 + timedelta(seconds=999)))
    led.add(make_verdict(reading_id="new", issued_at=T0 + timedelta(seconds=100)))
    led.prune(T0 + timedelta(seconds=130))
    assert led.lookup("old", "0123456789abcdef", T0) is None
    assert led.lookup("new", "0123456789abcdef", T0) is not None


def test_clear_empties_ledger():
    led = VerdictLedger()
    led.add(make_verdict())
    led.clear()
    assert led.lookup("r-1", "0123456789abcdef", T0) is None


def test_contained_device_has_zero_weight():
    assert trust_weight(make_verdict(state=TrustState.TRUSTED), DeviceStatus.QUARANTINED, CAPS) == 0.0


def test_trust_weight_caps_and_missing():
    assert trust_weight(None, DeviceStatus.OK, CAPS) == 0.0
    assert trust_weight(make_verdict(state=TrustState.DOUBTFUL, trust=0.9), DeviceStatus.OK, CAPS) == 0.5
    assert trust_weight(make_verdict(state=TrustState.DOUBTFUL, trust=0.2), DeviceStatus.OK, CAPS) == 0.2
    assert trust_weight(make_verdict(state=TrustState.TRUSTED, trust=None), DeviceStatus.OK, CAPS) == 1.0


def test_filter_weight_while_verdict_pending():
    assert estimate_weight(None, DeviceStatus.OK, CAPS, 0.5) == 0.5
    assert estimate_weight(None, DeviceStatus.QUARANTINED, CAPS, 0.5) == 0.0
    assert estimate_weight(make_verdict(state=TrustState.UNTRUSTED), DeviceStatus.OK, CAPS, 0.5) == 0.0


def test_evidence_ref_state_kinds():
    obs = make_observation()
    assert evidence_ref(obs, None, 0.0).state_kind is StateKind.OBSERVED
    assert evidence_ref(obs, None, 0.0).trust_state is TrustState.UNKNOWN
    assert evidence_ref(obs, make_verdict(state=TrustState.DOUBTFUL), 0.5).state_kind is StateKind.VALIDATED
    assert evidence_ref(obs, make_verdict(state=TrustState.UNTRUSTED), 0.0).state_kind is StateKind.UNKNOWN
    assert evidence_ref(obs, make_verdict(state=TrustState.UNKNOWN), 0.0).state_kind is StateKind.UNKNOWN


def test_r1_accepts_doubtful_with_one_trusted_r2_does_not():
    ev = [ref(TrustState.TRUSTED), ref(TrustState.DOUBTFUL)]
    assert meets_requirement(ev, RiskClass.R1)[0] is True
    assert meets_requirement(ev, RiskClass.R2) == (False, ("TRUST_INSUFFICIENT",))


def test_r1_needs_at_least_one_trusted():
    assert meets_requirement([ref(TrustState.DOUBTFUL)], RiskClass.R1) == (False, ("TRUST_INSUFFICIENT",))


def test_r0_always_passes_and_r3_needs_all_trusted():
    assert meets_requirement([], RiskClass.R0) == (True, ())
    assert meets_requirement([ref(TrustState.TRUSTED)], RiskClass.R3) == (True, ())
    assert meets_requirement([ref(TrustState.DOUBTFUL)], RiskClass.R3)[0] is False


def test_missing_verdict_reason():
    pending = evidence_ref(make_observation(), None, 0.0)
    assert meets_requirement([pending, ref(TrustState.TRUSTED)], RiskClass.R2) == (False, ("VERDICT_MISSING",))
    assert meets_requirement([pending, ref(TrustState.UNTRUSTED)], RiskClass.R2) == (
        False,
        ("TRUST_INSUFFICIENT", "VERDICT_MISSING"),
    )


@given(st.lists(st.sampled_from(list(TrustState)), min_size=1, max_size=5))
def test_property_no_r2_with_untrusted_or_unknown(states):
    ok, _ = meets_requirement([ref(s) for s in states], RiskClass.R2)
    assert not ok or all(s is TrustState.TRUSTED for s in states)


POLICY = {"epochs": {"seq_drop": 100, "ts_back_s": 60}}


def test_verdict_before_any_reading_is_parked_without_sim_now():
    s = BrainState.new(POLICY, {"filter": {"sample_period_s": 0.1}})
    v = make_verdict()
    s, fx = step(s, VerdictReceived(v))
    assert fx == [] and s.ledger.lookup(v.reading_id, v.sig_hash, T0) == v


def test_verdict_after_reading_prunes_old_entries():
    from city_brain.config import load_config

    s = BrainState.new(POLICY, load_config("config/standalone.yaml").params)
    s, _ = step(s, VerdictReceived(make_verdict(reading_id="stale", issued_at=T0 - timedelta(seconds=500))))
    s, _ = step(s, ReadingReceived(make_observation()))
    s, _ = step(s, VerdictReceived(make_verdict(reading_id="fresh")))
    assert s.ledger.lookup("stale", "0123456789abcdef", T0) is None
    assert s.ledger.lookup("fresh", "0123456789abcdef", T0) is not None


def test_new_epoch_clears_ledger():
    s = BrainState.new(POLICY, {})
    s.ledger.add(make_verdict())
    start_new_epoch(s)
    assert s.ledger.lookup("r-1", "0123456789abcdef", T0) is None
