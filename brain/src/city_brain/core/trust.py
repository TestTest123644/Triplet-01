"""Verdict ledger, trust weights and risk-class trust requirements (spec 7.2, 8.1, 8.6)."""

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta

from city_brain.domain.types import (
    DeviceStatus,
    EvidenceRef,
    Observation,
    RiskClass,
    StateKind,
    TrustState,
    Verdict,
)

_Key = tuple[str, str]


class VerdictLedger:
    """Verdicts keyed by `(reading_id, sig_hash)`. A verdict may arrive before its reading."""

    def __init__(self) -> None:
        self._verdicts: dict[_Key, Verdict] = {}

    def add(self, verdict: Verdict) -> list[_Key]:
        """Keep the latest verdict per key by `issued_at`.

        On an `issued_at` tie the later arrival wins, so a same-timestamp
        revocation-style downgrade replaces the verdict it revokes.
        Returns the keys whose trust (state or value) changed, in sorted order.
        """
        key = (verdict.reading_id, verdict.sig_hash)
        current = self._verdicts.get(key)
        if current is not None and verdict.issued_at < current.issued_at:
            return []
        self._verdicts[key] = verdict
        if current is not None and (current.state, current.trust) == (verdict.state, verdict.trust):
            return []
        return [key]

    def lookup(self, reading_id: str, sig_hash: str, now: datetime) -> Verdict | None:
        verdict = self._verdicts.get((reading_id, sig_hash))
        if verdict is None or now > verdict.expires_at:
            return None
        return verdict

    def prune(self, now: datetime, keep_s: float = 120.0) -> None:
        cutoff = now - timedelta(seconds=keep_s)
        for key in [k for k, v in self._verdicts.items() if v.issued_at < cutoff]:
            del self._verdicts[key]

    def clear(self) -> None:
        self._verdicts.clear()


def trust_weight(verdict: Verdict | None, device_status: DeviceStatus, caps: Mapping[str, float]) -> float:
    """Weight recorded on evidence: zero without a verdict or for a contained device."""
    if device_status is not DeviceStatus.OK or verdict is None:
        return 0.0
    trust = verdict.trust if verdict.trust is not None else 1.0
    return min(trust, caps[verdict.state.value])


def estimate_weight(
    verdict: Verdict | None,
    device_status: DeviceStatus,
    caps: Mapping[str, float],
    pending_weight: float,
) -> float:
    """Weight the filter uses: a reading awaiting its verdict is OBSERVED, not UNKNOWN."""
    if device_status is not DeviceStatus.OK:
        return 0.0
    if verdict is None:
        return pending_weight
    return trust_weight(verdict, device_status, caps)


def evidence_ref(obs: Observation, verdict: Verdict | None, weight: float) -> EvidenceRef:
    if verdict is None:
        kind = StateKind.OBSERVED
    elif weight > 0 and verdict.state in (TrustState.TRUSTED, TrustState.DOUBTFUL):
        kind = StateKind.VALIDATED
    else:
        kind = StateKind.UNKNOWN
    return EvidenceRef(
        reading_id=obs.reading_id,
        sig_hash=obs.sig_hash,
        device_id=obs.device_id,
        value=obs.value,
        sim_time=obs.sim_time,
        state_kind=kind,
        verdict_id=verdict.verdict_id if verdict is not None else None,
        trust_state=verdict.state if verdict is not None else TrustState.UNKNOWN,
        weight=weight,
    )


def meets_requirement(evidence: Sequence[EvidenceRef], risk: RiskClass) -> tuple[bool, tuple[str, ...]]:
    """R0 always passes; R1 needs all TRUSTED/DOUBTFUL with one TRUSTED; R2 and R3 need all TRUSTED.

    A ref with zero weight (contained device, spec 7.2/8.1) never counts as acceptable.
    R3's approval rule belongs to P5; here it is treated as all-TRUSTED (spec 8.6).
    """
    if risk is RiskClass.R0:
        return True, ()
    allowed = (
        (TrustState.TRUSTED, TrustState.DOUBTFUL) if risk is RiskClass.R1 else (TrustState.TRUSTED,)
    )
    pending = [e for e in evidence if e.verdict_id is None]
    failing = [
        e for e in evidence if e.verdict_id is not None and (e.weight <= 0 or e.trust_state not in allowed)
    ]
    has_trusted = any(e.trust_state is TrustState.TRUSTED and e.weight > 0 for e in evidence)
    if not pending and not failing and has_trusted:
        return True, ()
    reasons = []
    if failing or (not pending and not has_trusted):
        reasons.append("TRUST_INSUFFICIENT")
    if pending:
        reasons.append("VERDICT_MISSING")
    return False, tuple(reasons)
