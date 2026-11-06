import pytest
from pydantic import ValidationError

from city_brain.domain.types import Alternative, RiskClass, TrustState
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
