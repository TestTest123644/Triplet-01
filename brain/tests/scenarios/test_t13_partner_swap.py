"""T13: swap the partner, keep the decisions (spec 10.1, 16).

The same city is fed to the Brain twice: once in the standard Twin's wire format and once in
the alternative partner's, which calls a tank a `flood_sensor`, measures in centimetres and
names its fields differently. Only the profile changes. If the anti-corruption layer does
its job, the canonical observations and the decisions, commands and verifications that
follow from them are the same.

The run goes in over HTTP (`POST /v1/ingest/readings`), so the proof needs no broker.
"""

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from city_brain.adapters.files.jsonl_log import JsonlLog
from city_brain.adapters.inproc_twin import InProcTwin
from city_brain.adapters.rest.api import build_rest_app
from city_brain.config import load_config
from city_brain.core.events import ReadingReceived
from city_brain.core.step import step as core_step
from city_brain.domain.types import CanonicalUnit, SensorKind
from city_brain.edge.guard import Accepted, ReadingGuard, VerdictGuard
from city_brain.edge.profiles import GuardianProfile, TwinProfile
from city_brain.edge.signatures import KeyRing
from city_brain.runtime.app import BrainApp
from city_brain.runtime.clocks import SimClock
from mocks.guardian.mock_guardian import MockGuardian
from mocks.keys import derive_keypair
from mocks.twin import MockTwin
from mocks.twin.alt import ALT_PROFILE, AltTwin

BRAIN = Path(__file__).resolve().parents[2]
CONFIG = load_config(BRAIN / "config" / "standalone.yaml")
TOKEN = "t13-operator"
DEVICE = "z1-wl1"
TICKS = 300  # 30 s: long enough for a rising tank to cross the high band


class Run:
    """One Brain, one partner, fed through the HTTP ingest endpoint."""

    def __init__(self, tmp_path, name, twin, profile):
        self.twin, self.profile = twin, profile
        priv, pub = derive_keypair("t13:guardian")
        self.guardian = MockGuardian(priv)
        guardian_profile = GuardianProfile.load(
            BRAIN / "profiles" / "guardian-triplet07.yaml", public_key_b64=pub)
        self.keyring, self.registry = KeyRing(), {}
        self.log = tmp_path / f"{name}.jsonl"
        self.app = BrainApp(
            CONFIG, InProcTwin(twin), SimClock(), JsonlLog(self.log),
            ReadingGuard(profile, self.keyring, self.registry), VerdictGuard(guardian_profile),
            profile=profile, keyring=self.keyring, registry=self.registry, wall_now=lambda: 0.0)

    def events(self):
        return [json.loads(line) for line in self.log.read_bytes().splitlines()]


def rising(twin: MockTwin, ticks: int, start: float = 5.5, rate: float = 0.02) -> None:
    """Drive one tank up through the high band, leaving the rest of the city alone."""
    device = twin.devices[DEVICE]
    device.value = start
    device.actuator_state["valve_position"] = "closed"
    device.actuator_state["valve_ratio"] = 0.0


async def feed(run: Run, readings, client: TestClient) -> None:
    body = b"\n".join(readings)
    reply = client.post("/v1/ingest/readings", content=body,
                        headers={"Authorization": f"Bearer {TOKEN}"})
    assert reply.status_code == 200, reply.text
    assert reply.json()["accepted"] == len(readings)


def canonical_decisions(events):
    """What the Brain decided, with nothing partner-specific left in it."""
    out = []
    for event in events:
        kind = event["type"]
        data = event["data"]
        if kind == "tn.tsyp.brain.command.v1":
            out.append(("command", data["status"], data["capability"], data["target"], data["state"]))
        elif kind == "tn.tsyp.brain.decision.v1":
            out.append(("decision", data["outcome"], data["incident"].get("hold_reason")))
        elif kind == "tn.tsyp.brain.verification.v1":
            out.append(("verification", data["verdict"]))
    return out


async def both_runs(tmp_path):
    """The same rising tank, once per partner."""
    standard_twin = MockTwin()
    alt_twin = AltTwin(MockTwin())
    rising(standard_twin, TICKS)
    rising(alt_twin.twin, TICKS)

    standard = Run(tmp_path, "standard", standard_twin, standard_twin.profile)
    alternative = Run(tmp_path, "alternative", alt_twin, alt_twin.profile)
    await standard.app.start()
    await alternative.app.start()

    standard_client = TestClient(build_rest_app(standard.app, TOKEN))
    alt_client = TestClient(build_rest_app(alternative.app, TOKEN))
    with standard_client, alt_client:
        for _ in range(TICKS):
            readings = standard_twin.tick()
            translated = [alt_twin.translate(r) for r in readings]
            await feed(standard, [json.dumps(r, separators=(",", ":")).encode() for r in readings],
                       standard_client)
            await feed(alternative, [alt_twin.raw(r) for r in translated], alt_client)
            # Each Guardian judges the stream it was shown: a verdict binds to the signature
            # of the reading it saw, and the two partners sign different bytes.
            for reading, other in zip(readings, translated):
                for verdict in standard.guardian.on_reading(reading):
                    await standard.app.submit_raw("verdicts", verdict)
                for verdict in alternative.guardian.on_reading(other):
                    await alternative.app.submit_raw("verdicts", verdict)
    return standard, alternative


_RUNS: dict[str, object] = {}


async def runs_once(tmp_path_factory):
    """The pair of runs, built once and shared: 300 ticks twice over is not worth repeating."""
    if "runs" not in _RUNS:
        _RUNS["runs"] = await both_runs(tmp_path_factory.mktemp("t13"))
    return _RUNS["runs"]


# ----- the wire format is the only difference -----


def test_the_two_profiles_disagree_about_everything_on_the_wire():
    standard = MockTwin().profile
    alternative = TwinProfile.load(ALT_PROFILE)
    assert standard.fields != alternative.fields
    assert standard.kinds != alternative.kinds
    assert standard.units != alternative.units


def test_a_translated_reading_maps_to_the_same_canonical_observation():
    twin = MockTwin()
    alt = AltTwin(twin)
    reading = next(r for r in twin.tick() if r["device_id"] == DEVICE)
    translated = alt.translate(reading)
    assert translated["device_category"] == "flood_sensor" and translated["unit"] == "cm"
    assert translated["reading"] == pytest.approx(reading["value"] * 100.0)

    standard_fields = twin.profile.map_reading(reading)
    alt_fields = alt.profile.map_reading(translated)
    assert standard_fields["kind"] is SensorKind.WATER_LEVEL
    assert alt_fields["kind"] is SensorKind.WATER_LEVEL
    assert alt_fields["unit"] is CanonicalUnit.M
    assert alt_fields["value"] == pytest.approx(standard_fields["value"])
    assert alt_fields["device_id"] == standard_fields["device_id"]
    assert alt_fields["sim_time"] == standard_fields["sim_time"]


def test_the_alternative_registry_maps_onto_the_same_devices():
    twin = MockTwin()
    alt = AltTwin(twin)
    standard = {d.device_id: d for d in (twin.profile.map_device(r) for r in twin.list_devices())}
    alternative = {
        d.device_id: d for d in (alt.profile.map_device(r) for r in alt.list_devices())
    }
    assert set(standard) == set(alternative)
    for device_id, device in standard.items():
        other = alternative[device_id]
        assert (other.kind, other.unit, other.zone_id) == (device.kind, device.unit, device.zone_id)
        assert other.min_val == pytest.approx(device.min_val)
        assert other.max_val == pytest.approx(device.max_val)


def test_the_alternative_reading_still_has_to_be_signed():
    twin = MockTwin()
    alt = AltTwin(twin)
    reading = next(r for r in twin.tick() if r["device_id"] == DEVICE)
    translated = alt.translate(reading)
    registry = {p.device_id: p for p in (alt.profile.map_device(d) for d in alt.list_devices())}
    keyring = KeyRing()
    for device_id in registry:
        keyring.set(device_id, twin.get_device_key(device_id))
    guard = ReadingGuard(alt.profile, keyring, registry)
    assert isinstance(guard.check(alt.raw(translated)), Accepted)
    forged = {**translated, "reading": 999.0}
    assert guard.check(alt.raw(forged)).code == "BAD_SIGNATURE"


# ----- the same city, decided the same way -----


async def test_both_partners_produce_the_same_canonical_decisions(tmp_path_factory):
    standard, alternative = await runs_once(tmp_path_factory)
    assert canonical_decisions(standard.events()) == canonical_decisions(alternative.events())


async def test_both_partners_actuate_the_same_valve(tmp_path_factory):
    standard, alternative = await runs_once(tmp_path_factory)
    sent = [
        (e["data"]["capability"], e["data"]["target"], e["data"]["state"])
        for run in (standard, alternative)
        for e in run.events()
        if e["type"] == "tn.tsyp.brain.command.v1" and e["data"]["status"] == "DISPATCHED"
    ]
    assert sent, "the rising tank must have been actuated"
    half = len(sent) // 2
    assert sent[:half] == sent[half:]


async def test_both_partners_see_the_same_levels(tmp_path_factory):
    standard, alternative = await runs_once(tmp_path_factory)
    for device_id, channel in standard.app.state.filters.items():
        other = alternative.app.state.filters[device_id]
        assert other.level == pytest.approx(channel.level, abs=1e-6)


# ----- the ingest endpoint itself -----


async def test_ingest_needs_the_operator_token(tmp_path):
    twin = MockTwin()
    run = Run(tmp_path, "auth", twin, twin.profile)
    await run.app.start()
    with TestClient(build_rest_app(run.app, TOKEN)) as client:
        assert client.post("/v1/ingest/readings", content=b"{}").status_code == 401


async def test_an_unknown_stream_is_a_404(tmp_path):
    twin = MockTwin()
    run = Run(tmp_path, "stream", twin, twin.profile)
    await run.app.start()
    with TestClient(build_rest_app(run.app, TOKEN)) as client:
        reply = client.post("/v1/ingest/gossip", content=b"{}",
                            headers={"Authorization": f"Bearer {TOKEN}"})
        assert reply.status_code == 404


async def test_an_empty_body_is_a_400(tmp_path):
    twin = MockTwin()
    run = Run(tmp_path, "empty", twin, twin.profile)
    await run.app.start()
    with TestClient(build_rest_app(run.app, TOKEN)) as client:
        reply = client.post("/v1/ingest/readings", content=b"",
                            headers={"Authorization": f"Bearer {TOKEN}"})
        assert reply.status_code == 400


async def test_ingested_rubbish_is_rejected_by_the_edge_not_the_core(tmp_path):
    twin = MockTwin()
    run = Run(tmp_path, "rubbish", twin, twin.profile)
    await run.app.start()
    with TestClient(build_rest_app(run.app, TOKEN)) as client:
        reply = client.post("/v1/ingest/readings", content=b"{not json}",
                            headers={"Authorization": f"Bearer {TOKEN}"})
        assert reply.status_code == 200
        assert reply.json()["counters_changed"] == {"MALFORMED_JSON": 1}
    assert run.app.state.cursors == {}, "nothing reached the core"
