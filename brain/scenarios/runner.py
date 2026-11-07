"""Scenario runner: the Brain against its mocks in one process, on simulated time."""

import argparse
import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import yaml

from city_brain.adapters.files.jsonl_log import JsonlLog
from city_brain.adapters.inproc_twin import InProcTwin
from city_brain.adapters.memory.bus import MemoryBus
from city_brain.config import BrainConfig, load_config
from city_brain.edge.guard import GuardianEventGuard, ReadingGuard, VerdictGuard
from city_brain.edge.profiles import GuardianProfile
from city_brain.edge.signatures import KeyRing
from city_brain.runtime.app import BrainApp
from city_brain.runtime.clocks import SimClock
from mocks.guardian.mock_guardian import MockGuardian, trust_all
from mocks.keys import derive_keypair
from mocks.twin import MockTwin

BRAIN_DIR = Path(__file__).resolve().parents[1]
SCENARIOS_DIR = BRAIN_DIR / "scenarios"
STANDALONE_CONFIG = BRAIN_DIR / "config" / "standalone.yaml"
GUARDIAN_PROFILE = BRAIN_DIR / "profiles" / "guardian-triplet07.yaml"
SEED = 42
GUARDIAN_POLICIES = {"trust_all": trust_all}
SCENARIO_KEYS = {"duration_s", "guardian_policy", "inject", "expect"}
INJECT_KEYS = {"at_s", "capability", "target", "params"}


@dataclass(frozen=True)
class Injection:
    at_s: float
    capability: str
    target: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Scenario:
    name: str
    duration_s: float
    guardian_policy: str
    inject: tuple[Injection, ...]
    expect: dict[str, Any]


def _check_keys(what: str, found: dict, allowed: set[str], required: set[str]) -> None:
    unknown, missing = sorted(set(found) - allowed), sorted(required - set(found))
    if unknown or missing:
        raise ValueError(f"{what}: unknown keys {unknown}, missing keys {missing}")


def load_scenario(name: str) -> Scenario:
    data = yaml.safe_load((SCENARIOS_DIR / f"{name}.yaml").read_text(encoding="utf-8"))
    _check_keys(name, data, SCENARIO_KEYS, SCENARIO_KEYS)
    if data["guardian_policy"] not in GUARDIAN_POLICIES:
        raise ValueError(f"{name}: unknown guardian_policy '{data['guardian_policy']}'")
    for entry in data["inject"]:
        _check_keys(f"{name} inject", entry, INJECT_KEYS, INJECT_KEYS - {"params"})
    return Scenario(name, float(data["duration_s"]), data["guardian_policy"],
                    tuple(Injection(**entry) for entry in data["inject"]), data["expect"])


async def _run_standalone(
    scenario: Scenario, config: BrainConfig, log_path: Path, restart_at_s: float | None, duration_s: float
) -> None:
    dt = float(config.params["filter"]["sample_period_s"])
    twin = MockTwin(seed=SEED)
    brain_private, brain_public = (
        (config.signing_key, None) if config.signing_key else derive_keypair(f"{SEED}:brain"))
    if brain_public is not None:  # so a reader can check the signatures without the Brain
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.with_suffix(".pub").write_text(brain_public, encoding="ascii")
    private_key, public_key = derive_keypair(f"{SEED}:guardian")
    guardian = MockGuardian(private_key, policy=GUARDIAN_POLICIES[scenario.guardian_policy]())
    guardian_profile = GuardianProfile.load(GUARDIAN_PROFILE, public_key_b64=public_key)
    keyring = KeyRing()
    registry: dict = {}
    clock = SimClock()
    bus = MemoryBus()
    tick = 0
    app = BrainApp(
        config, InProcTwin(twin), clock, JsonlLog(log_path, private_key_b64=brain_private),
        ReadingGuard(twin.profile, keyring, registry), VerdictGuard(guardian_profile),
        event_guard=GuardianEventGuard(guardian_profile), profile=twin.profile, keyring=keyring, registry=registry, wall_now=lambda: tick * dt)
    await app.start()

    restart_tick = None if restart_at_s is None else round(restart_at_s / dt)
    injections: dict[int, list[Injection]] = {}
    for injection in scenario.inject:
        injections.setdefault(round(injection.at_s / dt), []).append(injection)
    for tick in range(round(duration_s / dt)):
        if tick == restart_tick:
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
        raise NotImplementedError("triplet wiring lands in P4")
    if profile != "standalone":
        raise ValueError(f"unknown profile '{profile}'")
    scenario = load_scenario(name)
    config = config if config is not None else load_config(STANDALONE_CONFIG)
    started = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    log_path = Path(logs_dir if logs_dir is not None else config.logs_dir) / f"{name}-e0-{started}.jsonl"
    duration = scenario.duration_s if duration_s is None else duration_s
    asyncio.run(_run_standalone(scenario, config, log_path, restart_at_s, duration))
    return log_path


def main() -> None:
    parser = argparse.ArgumentParser(prog="scenarios.runner")
    parser.add_argument("--name", required=True)
    parser.add_argument("--profile", required=True, choices=("standalone", "triplet"))
    args = parser.parse_args()
    print(run(args.name, args.profile))


if __name__ == "__main__":
    main()
