import dataclasses

import numpy as np
import pytest

from guardian_layer.ai.datasets.fault import FAULT_REGISTRY
from guardian_layer.ai.datasets.injection import FaultInjector
from guardian_layer.utils.dataset_config import FaultSettings, InjectionSettings
from guardian_layer.utils.schema import FaultContext


def test_every_configured_kind_has_a_handler():
    s = InjectionSettings()
    assert set(s.fault_kinds) | set(s.fabricated_kinds) <= FAULT_REGISTRY.keys()


@pytest.mark.parametrize("kind", sorted(FAULT_REGISTRY))
def test_a_handler_never_changes_the_context_values(kind):
    values = np.arange(12.0)
    ctx = FaultContext(
        values, np.random.default_rng(0), FaultSettings(), 1.0, 1.0, (-100.0, 100.0)
    )
    FAULT_REGISTRY[kind](ctx)
    np.testing.assert_array_equal(values, np.arange(12.0))


def test_the_context_is_frozen():
    ctx = FaultContext(
        np.zeros(3), np.random.default_rng(0), FaultSettings(), 1.0, 1.0, (0.0, 1.0)
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.spread = 2.0  # pyright: ignore[reportAttributeAccessIssue]


def test_an_unknown_kind_raises():
    with pytest.raises(ValueError, match="unknown fault kind: melt"):
        FaultInjector().apply_fault(
            np.arange(5.0), "melt", 1.0, np.random.default_rng(0), (0.0, 1.0)
        )
