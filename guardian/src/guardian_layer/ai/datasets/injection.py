"""Inject labelled sensor faults into clean real readings.

Sizes are multiples of each type's normal spread (the median within-window std of
its clean training sensor-windows), so a fault means the same thing for any type.
The random draws keep the research notebook's order: a seeded run gives the same
injected values as the notebook.
"""

from collections import Counter

import numpy as np

from guardian_layer.ai.datasets.fault import FAULT_REGISTRY
from guardian_layer.utils.dataset_config import InjectionSettings, get_dataset_settings
from guardian_layer.utils.logger import get_logger
from guardian_layer.utils.schema import FaultContext, FloatArray, IntArray

logger = get_logger(__name__)


class FaultInjector:
    def __init__(self, settings: InjectionSettings | None = None):
        self.settings = settings or get_dataset_settings().injection

    def apply_fault(
        self,
        values: FloatArray,
        kind: str,
        spread: float,
        rng: np.random.Generator,
        value_range: tuple[float, float],
    ) -> FloatArray:
        """A faulty copy of one sensor window (needs >= 2 readings)."""
        sign = rng.choice((-1.0, 1.0))
        handler = FAULT_REGISTRY.get(kind)
        if handler is None:
            raise ValueError(f"unknown fault kind: {kind}")
        out = handler(
            FaultContext(values, rng, self.settings.faults, spread, sign, value_range)
        )
        # fabricated streams stay in range by definition
        if kind in self.settings.fabricated_kinds:
            out = np.clip(out, *value_range)
        return out

    def inject(
        self,
        values: FloatArray,
        targets: list[IntArray],
        types: list[str],
        spreads: dict[str, float],
        ranges: dict[str, tuple[float, float]],
        rng: np.random.Generator,
        kinds: tuple[str, ...],
    ) -> tuple[FloatArray, list[str]]:
        """Fault a random share of the targets (row indices in time order) in a copy.

        Returns the faulty copy and one kind per target ("" when left untouched,
        including faults that changed nothing).
        """
        faulty = values.copy()
        applied = [""] * len(targets)
        if not kinds:  # nothing to inject: draw nothing
            return faulty, applied
        chosen = rng.choice(
            len(targets),
            size=round(self.settings.inject_share * len(targets)),
            replace=False,
        )
        unchanged = 0
        per_kind: Counter[str] = Counter()
        for index in chosen:
            rows = targets[index]
            kind = str(rng.choice(kinds))
            changed = self.apply_fault(
                faulty[rows], kind, spreads[types[index]], rng, ranges[types[index]]
            )
            if np.array_equal(changed, faulty[rows]):
                unchanged += 1
                continue  # e.g. stuck on a flat window: indistinguishable, stays clean
            faulty[rows] = changed
            applied[index] = kind
            per_kind[kind] += 1
        logger.info(
            "Injected faults",
            targets=len(targets),
            chosen=len(chosen),
            faulted=per_kind.total(),
            unchanged=unchanged,
            per_kind=dict(per_kind),
        )
        return faulty, applied

    def within_window_spreads(
        self, windows: dict[str, list[FloatArray]], min_samples: int
    ) -> dict[str, float]:
        """Per type, the median std inside one sensor-window, over its windows.

        Faults are sized in multiples of this normal variation, so they stay subtle.
        Windows with too few readings are skipped, and so are flat ones (std 0, e.g.
        light at night), which would drag the median to 0.
        """
        spreads = {}
        flat = []
        for name, arrays in windows.items():
            stds = np.array([a.std() for a in arrays if len(a) >= min_samples])
            positive = stds[stds > 0]
            if positive.size:
                spreads[name] = float(np.median(positive))
            else:
                spreads[name] = 0.0
                flat.append(name)
        if flat:
            logger.warning(
                "Types without a spread: their spread-sized faults change nothing",
                types=sorted(flat),
            )
        return spreads
