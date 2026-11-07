"""What the Brain needs from a partner, as checks a partner can run (spec 10.4).

Each check returns a `Finding`: the rule, whether it held, and what was seen. A partner that
passes every check can be plugged in; one that fails a check gets a sentence saying exactly
what the Brain could not accept, which is more useful than a stack trace from our side.

The module is importable on its own (`conformance_report`) and is also a pytest module, so
`pytest conformance/` runs it against whatever `conformance_targets` provides.
"""

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from city_brain.domain.ids import sig_hash
from city_brain.edge.guard import Accepted, AcceptedVerdict, ReadingGuard, Rejected, VerdictGuard
from city_brain.edge.profiles import GuardianProfile, TwinProfile
from city_brain.edge.signatures import KeyRing, verify

BRAIN = Path(__file__).resolve().parents[1]
REQUIRED_TWIN_TOOLS = (
    "list_devices", "get_device_key", "get_city_state", "set_valve", "set_signal_plan",
    "switch_grid", "dispatch", "get_command_result",
)


@dataclass(frozen=True)
class Finding:
    rule: str
    ok: bool
    detail: str = ""

    def __str__(self) -> str:
        return f"{'PASS' if self.ok else 'FAIL'} {self.rule}: {self.detail}" if self.detail else \
            f"{'PASS' if self.ok else 'FAIL'} {self.rule}"


def _ok(rule: str, detail: str = "") -> Finding:
    return Finding(rule, True, detail)


def _no(rule: str, detail: str) -> Finding:
    return Finding(rule, False, detail)


# ----- the Twin -----


def check_twin(twin: Any, profile: TwinProfile, ticks: int = 3) -> list[Finding]:
    """Everything the Brain needs from a Twin: a registry, keys, signed readings, tools."""
    findings: list[Finding] = []
    devices = twin.list_devices()
    findings.append(
        _ok("registry is a non-empty list", f"{len(devices)} devices") if devices
        else _no("registry is a non-empty list", "list_devices returned nothing"))

    registry = {}
    for raw in devices:
        try:
            device = profile.map_device(raw)
        except Exception as exc:
            findings.append(_no("every device maps to a canonical profile",
                                f"{raw.get('device_id')}: {exc}"))
            continue
        registry[device.device_id] = device
    if len(registry) == len(devices):
        findings.append(_ok("every device maps to a canonical profile"))

    keyring = KeyRing()
    missing = []
    for device_id in registry:
        key = twin.get_device_key(device_id)
        if isinstance(key, str) and key:
            keyring.set(device_id, key)
        else:
            missing.append(device_id)
    findings.append(
        _ok("every device has a public key") if not missing
        else _no("every device has a public key", f"missing for {sorted(missing)[:5]}"))

    findings.extend(_check_readings(twin, profile, keyring, registry, ticks))
    findings.extend(_check_tools(twin))
    return findings


def _check_readings(
    twin: Any, profile: TwinProfile, keyring: KeyRing, registry: Mapping, ticks: int
) -> list[Finding]:
    guard = ReadingGuard(profile, keyring, registry)
    findings: list[Finding] = []
    rejected: list[str] = []
    seqs: dict[str, list[int]] = {}
    seen = 0
    for _ in range(ticks):
        for reading in twin.tick():
            seen += 1
            raw = json.dumps(reading, separators=(",", ":")).encode("utf-8")
            result = guard.check(raw)
            if isinstance(result, Rejected):
                rejected.append(f"{reading.get('device_id')}: {result.code} {result.detail}")
                continue
            assert isinstance(result, Accepted)
            seqs.setdefault(result.observation.device_id, []).append(result.observation.seq)
    findings.append(
        _ok("every reading passes the edge guard", f"{seen} readings") if not rejected
        else _no("every reading passes the edge guard", "; ".join(rejected[:3])))
    unsorted = [d for d, values in seqs.items() if values != sorted(set(values))]
    findings.append(
        _ok("seq is strictly monotonic per device") if not unsorted
        else _no("seq is strictly monotonic per device", f"{sorted(unsorted)[:5]}"))
    return findings


def _check_tools(twin: Any) -> list[Finding]:
    offered = _tool_names(twin)
    if offered is None:
        return [_ok("the Twin's tools are reachable", "not an MCP target, tools not listed")]
    missing = [tool for tool in REQUIRED_TWIN_TOOLS if tool not in offered]
    return [
        _ok("every tool the Brain calls is offered", f"{len(offered)} tools") if not missing
        else _no("every tool the Brain calls is offered", f"missing {missing}")
    ]


def _tool_names(twin: Any) -> set[str] | None:
    """What this Twin offers: its own list, or what can be seen on the object."""
    names = getattr(twin, "tool_names", None)
    if names is not None:
        return set(names)
    tools = getattr(twin, "_tools", None)  # an in-process Twin keeps a tool -> capability map
    if isinstance(tools, Mapping):
        offered = set(tools) | {"get_command_result"}
        return offered | {name for name in REQUIRED_TWIN_TOOLS if callable(getattr(twin, name, None))}
    return None


# ----- the Guardian -----


def check_guardian(
    guardian: Any, profile: GuardianProfile, readings: Sequence[Mapping], public_key: str
) -> list[Finding]:
    """Everything the Brain needs from a Guardian: signed verdicts bound to the readings."""
    guard = VerdictGuard(profile)
    findings: list[Finding] = []
    verdicts = []
    for reading in readings:
        verdicts.extend(guardian.on_reading(dict(reading)))
    findings.append(
        _ok("a verdict is issued for a reading", f"{len(verdicts)} verdicts") if verdicts
        else _no("a verdict is issued for a reading", "the Guardian said nothing"))

    rejected, unsigned, unbound = [], [], []
    by_reading = {r["reading_id"]: r for r in readings}
    for raw in verdicts:
        payload = json.loads(raw)
        if not verify(payload, public_key):
            unsigned.append(payload.get("verdict_id"))
        result = guard.check(raw)
        if isinstance(result, Rejected):
            rejected.append(f"{result.code} {result.detail}")
            continue
        assert isinstance(result, AcceptedVerdict)
        reading = by_reading.get(result.verdict.reading_id)
        if reading is None or result.verdict.sig_hash != sig_hash(reading["sig"]):
            unbound.append(result.verdict.verdict_id)
    findings.append(
        _ok("every verdict passes the edge guard") if not rejected
        else _no("every verdict passes the edge guard", "; ".join(rejected[:3])))
    findings.append(
        _ok("every verdict is signed by the Guardian") if not unsigned
        else _no("every verdict is signed by the Guardian", f"{unsigned[:3]}"))
    findings.append(
        _ok("every verdict names the reading it judges") if not unbound
        else _no("every verdict names the reading it judges", f"{unbound[:3]}"))
    findings.extend(_check_vocabulary(profile))
    findings.extend(_check_events(guardian, profile))
    return findings


def _check_vocabulary(profile: GuardianProfile) -> list[Finding]:
    missing = [
        state for state in ("TRUSTED", "UNTRUSTED") if state not in set(profile.vocabulary.values())
    ]
    return [
        _ok("the verdict vocabulary covers trust and distrust") if not missing
        else _no("the verdict vocabulary covers trust and distrust", f"no mapping to {missing}")
    ]


def _check_events(guardian: Any, profile: GuardianProfile) -> list[Finding]:
    from city_brain.edge.guard import AcceptedGuardianEvent, GuardianEventGuard

    drain = getattr(guardian, "drain_events", None)
    if drain is None:
        return [_ok("guardian.events is optional", "this Guardian publishes none")]
    guard = GuardianEventGuard(profile)
    bad = []
    for raw in drain():
        result = guard.check(raw)
        if not isinstance(result, AcceptedGuardianEvent):
            bad.append(f"{result.code} {result.detail}")
    return [
        _ok("every guardian event passes the edge guard") if not bad
        else _no("every guardian event passes the edge guard", "; ".join(bad[:3]))
    ]


# ----- the report -----


def conformance_report(findings: Iterable[Finding]) -> str:
    findings = list(findings)
    failed = [f for f in findings if not f.ok]
    lines = [str(f) for f in findings]
    lines.append(
        f"{len(findings) - len(failed)}/{len(findings)} checks passed"
        + ("" if not failed else f"; {len(failed)} FAILED"))
    return "\n".join(lines)


def assert_conformant(findings: Iterable[Finding]) -> None:
    findings = list(findings)
    failed = [str(f) for f in findings if not f.ok]
    assert not failed, "\n".join(failed)


# ----- as a pytest module, against whatever the caller provides -----


def test_twin_conformance(conformance_twin, conformance_twin_profile):
    assert_conformant(check_twin(conformance_twin, conformance_twin_profile))


def test_guardian_conformance(
    conformance_guardian, conformance_guardian_profile, conformance_readings,
    conformance_guardian_key,
):
    assert_conformant(check_guardian(
        conformance_guardian, conformance_guardian_profile, conformance_readings,
        conformance_guardian_key))


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
