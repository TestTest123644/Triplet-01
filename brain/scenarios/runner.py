"""Scenario runner: the Brain against its mocks in one process, on simulated time."""

import argparse
import asyncio
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import yaml

from city_brain.adapters.files.jsonl_log import JsonlLog, verify_chain
from city_brain.adapters.inproc_twin import InProcTwin
from city_brain.adapters.memory.bus import MemoryBus
from city_brain.config import BrainConfig, load_config
from city_brain.core.events import OperatorCommand
from city_brain.core.state import BrainState
from city_brain.domain.types import SensorKind
from city_brain.edge.guard import GuardianEventGuard, ReadingGuard, VerdictGuard
from city_brain.edge.profiles import GuardianProfile
from city_brain.edge.signatures import KeyRing
from city_brain.runtime.app import BrainApp
from city_brain.runtime.clocks import SimClock
from mocks.guardian.mock_guardian import GuardianPolicy, MockGuardian
from mocks.keys import derive_keypair
from mocks.twin import MockTwin

BRAIN_DIR = Path(__file__).resolve().parents[1]
SCENARIOS_DIR = BRAIN_DIR / "scenarios"
STANDALONE_CONFIG = BRAIN_DIR / "config" / "standalone.yaml"
GUARDIAN_PROFILE = BRAIN_DIR / "profiles" / "guardian-triplet07.yaml"
SEED = 42
SCENARIO_KEYS = {
    "duration_s", "guardian_policy", "inject", "expect", "planner", "restart_twin_at_s",
    "operator_actions",
}
REQUIRED_KEYS = {"duration_s", "guardian_policy", "inject", "expect"}
INJECT_KEYS = {"at_s", "capability", "target", "params"}
OPERATOR_KEYS = {"at_s", "action", "target_device"}
# Edge-guard rejection codes, so a summary can show what never reached the core.
REJECT_CODES = (
    "MALFORMED_JSON", "SCHEMA_VIOLATION", "BAD_SIGNATURE", "UNKNOWN_DEVICE", "TYPE_MISMATCH",
    "OVERSIZED", "UNKNOWN_VERDICT_STATE",
)


@dataclass(frozen=True)
class Injection:
    at_s: float
    capability: str
    target: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OperatorAction:
    at_s: float
    action: Literal["approve", "deny", "pause", "resume"]
    target_device: str | None = None


@dataclass(frozen=True)
class Scenario:
    name: str
    duration_s: float
    guardian_policy: GuardianPolicy
    inject: tuple[Injection, ...]
    expect: dict[str, Any]
    planner: str | None = None
    restart_twin_at_s: tuple[float, ...] = ()
    operator_actions: tuple[OperatorAction, ...] = ()


def _check_keys(what: str, found: dict, allowed: set[str], required: set[str]) -> None:
    unknown, missing = sorted(set(found) - allowed), sorted(required - set(found))
    if unknown or missing:
        raise ValueError(f"{what}: unknown keys {unknown}, missing keys {missing}")


def guardian_policy(spec: Any) -> GuardianPolicy:
    """A scenario's Guardian behaviour: a bare kind, or a kind with its arguments."""
    if isinstance(spec, str):
        spec = {"kind": spec}
    if not isinstance(spec, dict) or "kind" not in spec:
        raise ValueError(f"guardian_policy must be a kind or a mapping with one: {spec!r}")
    return GuardianPolicy(
        kind=str(spec["kind"]),
        device=spec.get("device"),
        seconds=float(spec.get("seconds", 0.0)),
        count=int(spec.get("count", 0)),
        until=float(spec.get("until", 0.0)),
        reasons=tuple(spec.get("reasons", ())),
    )


def load_scenario(name: str) -> Scenario:
    data = yaml.safe_load((SCENARIOS_DIR / f"{name}.yaml").read_text(encoding="utf-8"))
    _check_keys(name, data, SCENARIO_KEYS, REQUIRED_KEYS)
    for entry in data["inject"]:
        _check_keys(f"{name} inject", entry, INJECT_KEYS, INJECT_KEYS - {"params"})
    for entry in data.get("operator_actions") or ():
        _check_keys(f"{name} operator_actions", entry, OPERATOR_KEYS, {"at_s", "action"})
    planner = data.get("planner")
    if planner is not None and planner not in ("mpc", "t0"):
        raise ValueError(f"{name}: unknown planner '{planner}'")
    return Scenario(
        name=name,
        duration_s=float(data["duration_s"]),
        guardian_policy=guardian_policy(data["guardian_policy"]),
        inject=tuple(Injection(**entry) for entry in data["inject"]),
        expect=data["expect"],
        planner=planner,
        restart_twin_at_s=tuple(float(t) for t in (data.get("restart_twin_at_s") or ())),
        operator_actions=tuple(
            OperatorAction(**entry) for entry in (data.get("operator_actions") or ())),
    )


def _with_planner(config: BrainConfig, planner: str | None) -> BrainConfig:
    if planner is None:
        return config
    policy = {**config.policy, "water": {**config.policy["water"], "planner": planner}}
    return config.model_copy(update={"policy": policy})


def _pending_for(state: BrainState, target_device: str | None) -> str | None:
    """The action id waiting for an operator on this target, if any."""
    for action_id in sorted(state.pending_approvals):
        record = state.actions.get(action_id)
        if record is None:
            continue
        if target_device is None or record.intent.target == target_device:
            return action_id
    return None


async def _operate(app: BrainApp, action: OperatorAction) -> None:
    action_id = None
    if action.action in ("approve", "deny"):
        action_id = _pending_for(app.state, action.target_device)
        if action_id is None:
            return  # nothing is waiting, so the operator has nothing to answer
    await app.feed(OperatorCommand(action=action.action, action_id=action_id))


def _note_levels(state: BrainState, twin: MockTwin, max_levels: dict[str, float]) -> None:
    """The highest a tank reached, as reported and as estimated."""
    for device_id, device in state.registry.items():
        if device.kind is not SensorKind.WATER_LEVEL:
            continue
        channel = state.filters.get(device_id)
        seen = [twin.devices[device_id].value] if device_id in twin.devices else []
        if channel is not None and channel.initialised:
            seen.append(channel.level)
        for value in seen:
            max_levels[device_id] = max(max_levels.get(device_id, float(value)), float(value))


async def _run_standalone(
    scenario: Scenario, config: BrainConfig, log_path: Path, restart_at_s: float | None,
    duration_s: float,
) -> dict[str, Any]:
    dt = float(config.params["filter"]["sample_period_s"])
    twin = MockTwin(seed=SEED)
    brain_private, brain_public = (
        (config.signing_key, None) if config.signing_key else derive_keypair(f"{SEED}:brain"))
    if brain_public is not None:  # so a reader can check the signatures without the Brain
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.with_suffix(".pub").write_text(brain_public, encoding="ascii")
    private_key, public_key = derive_keypair(f"{SEED}:guardian")
    guardian = MockGuardian(private_key, policy=scenario.guardian_policy)
    guardian_profile = GuardianProfile.load(GUARDIAN_PROFILE, public_key_b64=public_key)
    keyring = KeyRing()
    registry: dict = {}
    clock = SimClock()
    bus = MemoryBus()
    tick = 0
    app = BrainApp(
        config, InProcTwin(twin), clock, JsonlLog(log_path, private_key_b64=brain_private),
        ReadingGuard(twin.profile, keyring, registry), VerdictGuard(guardian_profile),
        event_guard=GuardianEventGuard(guardian_profile), profile=twin.profile,
        keyring=keyring, registry=registry, wall_now=lambda: tick * dt)
    await app.start()

    restarts = {round(t / dt) for t in scenario.restart_twin_at_s}
    if restart_at_s is not None:
        restarts.add(round(restart_at_s / dt))
    injections: dict[int, list[Injection]] = {}
    for injection in scenario.inject:
        injections.setdefault(round(injection.at_s / dt), []).append(injection)
    operator: dict[int, list[OperatorAction]] = {}
    for action in scenario.operator_actions:
        operator.setdefault(round(action.at_s / dt), []).append(action)
    max_levels: dict[str, float] = {}

    for tick in range(round(duration_s / dt)):
        if tick in restarts:
            twin.restart()
        for injection in injections.get(tick, ()):
            twin.inject(injection.capability, injection.target, injection.params)
        for reading in twin.tick():
            bus.publish("readings", json.dumps(reading, separators=(",", ":")).encode("utf-8"))
            for verdict in guardian.on_reading(reading):
                bus.publish("verdicts", verdict)
        for event in guardian.drain_events():
            bus.publish("events", event)
        for stream, raw in bus.drain():
            await app.submit_raw(stream, raw)
        for fired in clock.advance(dt):
            await app.feed(fired)
        for action in operator.get(tick, ()):
            await _operate(app, action)
        _note_levels(app.state, twin, max_levels)
    return {
        "counters": dict(app.state.counters),
        "max_water_level_by_device": max_levels,
        "epoch_count": app.state.epoch + 1,
    }


# ----- run summaries (spec 12.2) -----


def _event_kind(event: dict) -> str:
    return str(event.get("type", "")).removeprefix("tn.tsyp.brain.").removesuffix(".v1")


def summary_path(log_path: Path) -> Path:
    return log_path.with_name(f"{log_path.stem}-summary.json")


def write_summary(log_path: Path, scenario: str, run_state: dict[str, Any]) -> Path:
    """Write `<log>-summary.json` beside the log: what the run did, in one page (spec 12.2)."""
    events = [json.loads(line) for line in log_path.read_bytes().splitlines()]
    counters = run_state.get("counters", {})
    counts_by_type: Counter[str] = Counter()
    commands: Counter[str] = Counter()
    refusals: Counter[str] = Counter()
    verifications: Counter[str] = Counter()
    mode_changes: list[str] = []
    for event in events:
        kind = _event_kind(event)
        counts_by_type[kind] += 1
        data = event.get("data", {})
        if kind == "command":
            if data.get("status") == "DISPATCHED":
                commands[str(data.get("capability"))] += 1
            for code in data.get("shield", {}).get("refusals", ()) or ():
                refusals[str(code)] += 1
        elif kind == "verification":
            verifications[str(data.get("verdict"))] += 1
        elif kind == "status" and data.get("reason") == "mode_changed":
            mode_changes.append(str(data.get("mode")))
    summary = {
        "scenario": scenario,
        "log": log_path.name,
        "epoch_count": run_state.get("epoch_count", 1),
        "event_count": len(events),
        "counts_by_type": dict(sorted(counts_by_type.items())),
        "commands_by_capability": dict(sorted(commands.items())),
        "refusals_by_code": dict(sorted(refusals.items())),
        "verifications_by_verdict": dict(sorted(verifications.items())),
        "mode_changes": mode_changes,
        "dedupe_count": int(counters.get("DUPLICATE", 0)),
        "rejected_by_code": {code: int(counters[code]) for code in REJECT_CODES if counters.get(code)},
        "counters": dict(sorted(counters.items())),
        "max_water_level_by_device": {
            device_id: round(level, 4)
            for device_id, level in sorted(run_state.get("max_water_level_by_device", {}).items())
        },
        "final_hash": events[-1]["hash"] if events else None,
        "chain_valid": verify_chain(log_path),
    }
    path = summary_path(log_path)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def run(
    name: str,
    profile: Literal["standalone", "triplet"],
    logs_dir: Path | None = None,
    restart_at_s: float | None = None,
    duration_s: float | None = None,
    config: BrainConfig | None = None,
) -> Path:
    """Run one scenario and return its log path. `duration_s` overrides the scenario's."""
    if profile == "triplet":
        raise NotImplementedError("triplet wiring lands in P8")
    if profile != "standalone":
        raise ValueError(f"unknown profile '{profile}'")
    scenario = load_scenario(name)
    config = config if config is not None else load_config(STANDALONE_CONFIG)
    config = _with_planner(config, scenario.planner)
    started = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    log_path = Path(logs_dir if logs_dir is not None else config.logs_dir) / f"{name}-e0-{started}.jsonl"
    duration = scenario.duration_s if duration_s is None else duration_s
    run_state = asyncio.run(_run_standalone(scenario, config, log_path, restart_at_s, duration))
    write_summary(log_path, name, run_state)
    return log_path


def main() -> None:
    parser = argparse.ArgumentParser(prog="scenarios.runner")
    parser.add_argument("--name", required=True)
    parser.add_argument("--profile", required=True, choices=("standalone", "triplet"))
    parser.add_argument("--duration-s", type=float, default=None)
    args = parser.parse_args()
    print(run(args.name, args.profile, duration_s=args.duration_s))


if __name__ == "__main__":
    main()
