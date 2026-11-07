import pytest
from pydantic import ValidationError

from city_brain.domain.types import Alternative, CanonicalUnit, RiskClass, StateKind, TrustState
from tests.factories import make_observation, make_verdict


def test_types_are_frozen():
    with pytest.raises(ValidationError):
        make_observation().value = 1.0


def test_verdict_is_frozen_with_defaults():
    v = make_verdict()
    assert v.state is TrustState.TRUSTED
    with pytest.raises(ValidationError):
        v.trust = 0.0


def test_overrides_apply():
    assert make_observation(value=7.5).value == 7.5


def test_alternative_carries_rejected_because():
    assert Alternative(option="a", rejected_because="b").rejected_because == "b"


def test_risk_class_values():
    assert [r.value for r in RiskClass] == ["R0", "R1", "R2", "R3"]


def test_enum_wire_values_are_pinned():
    from city_brain.domain.types import (
        ActionStatus, CanonicalUnit, DeviceStatus, SensorKind, StateKind,
    )

    assert {m.name: m.value for m in TrustState} == {
        "TRUSTED": "TRUSTED", "DOUBTFUL": "DOUBTFUL",
        "UNTRUSTED": "UNTRUSTED", "UNKNOWN": "UNKNOWN",
    }
    assert {m.name: m.value for m in StateKind} == {
        "OBSERVED": "OBSERVED", "VALIDATED": "VALIDATED",
        "ESTIMATED": "ESTIMATED", "UNKNOWN": "UNKNOWN",
    }
    assert {m.name: m.value for m in CanonicalUnit} == {
        "M": "m", "VEH_PER_MIN": "veh_per_min", "MW": "MW",
        "AQI": "AQI", "CALLS_PER_MIN": "calls_per_min",
    }
    assert ActionStatus.DISPATCHED.value == "DISPATCHED"
    assert ActionStatus.REFUSED.value == "REFUSED"
    assert DeviceStatus.OK.value == "OK"
    assert SensorKind.WATER_LEVEL.value == "WATER_LEVEL"
