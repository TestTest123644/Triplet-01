"""Deterministic identifiers."""

import hashlib


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sig_hash(sig_b64: str) -> str:
    return _sha256_hex(sig_b64)[:16]


def action_id(incident_id: str, capability: str, target: str, state: str, plan_version: int) -> str:
    parts = (incident_id, capability, target, state, str(plan_version))
    return "act-" + _sha256_hex("|".join(parts))[:16]


def idempotency_key(epoch: int, action_id: str) -> str:
    return f"brain-{epoch}-{action_id}"
