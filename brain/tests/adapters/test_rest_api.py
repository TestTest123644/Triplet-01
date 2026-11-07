"""The query and operator API (spec 5.6, 12.4)."""

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from city_brain.adapters.files.jsonl_log import JsonlLog
from city_brain.adapters.inproc_twin import InProcTwin
from city_brain.adapters.rest.api import build_rest_app
from city_brain.config import load_config
from city_brain.core.events import OperatorCommand
from city_brain.domain.types import ActionStatus, BrainMode
from city_brain.edge.guard import GuardianEventGuard, ReadingGuard, VerdictGuard
from city_brain.edge.profiles import GuardianProfile
from city_brain.edge.signatures import KeyRing
from city_brain.runtime.app import BrainApp
from city_brain.runtime.clocks import SimClock
from mocks.guardian.mock_guardian import MockGuardian, quarantine_at
from mocks.keys import derive_keypair
from mocks.twin import MockTwin

BRAIN = Path(__file__).resolve().parents[2]
TOKEN = "s3cret-operator-token"


class Rig:
    """A BrainApp fed by the mocks, with its API in front of it."""

    def __init__(self, tmp_path, guardian_policy=None, ticks=10):
        self.twin = MockTwin()
        self.clock = SimClock()
        priv, pub = derive_keypair("rest:guardian")
        self.guardian = MockGuardian(priv, policy=guardian_policy) if guardian_policy else \
            MockGuardian(priv)
        profile = GuardianProfile.load(BRAIN / "profiles" / "guardian-triplet07.yaml",
                                       public_key_b64=pub)
        self.keyring, self.registry = KeyRing(), {}
        self.app = BrainApp(
            load_config(BRAIN / "config" / "standalone.yaml"), InProcTwin(self.twin), self.clock,
            JsonlLog(tmp_path / "api.jsonl"),
            ReadingGuard(self.twin.profile, self.keyring, self.registry),
            VerdictGuard(profile), event_guard=GuardianEventGuard(profile),
            profile=self.twin.profile, keyring=self.keyring, registry=self.registry,
            wall_now=lambda: 0.0)
        self.decisions: list[dict] = []
        self.ticks = ticks

    async def start(self):
        await self.app.start()
        for _ in range(self.ticks):
            for reading in self.twin.tick():
                await self.app.submit_raw("readings", json.dumps(reading).encode())
                for verdict in self.guardian.on_reading(reading):
                    await self.app.submit_raw("verdicts", verdict)
            for event in self.guardian.drain_events():
                await self.app.submit_raw("events", event)
        self.decisions = [
            json.loads(line) for line in Path(self.app._log.path).read_bytes().splitlines()
            if b"decision" in line
        ]
        return self

    def client(self):
        return TestClient(build_rest_app(self.app, TOKEN, self.decisions))


@pytest.fixture
async def rig(tmp_path):
    return await Rig(tmp_path).start()


@pytest.fixture
def client(rig):
    with rig.client() as client:
        yield client


def auth():
    return {"Authorization": f"Bearer {TOKEN}"}


# ----- liveness -----


def test_health_is_always_ok(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_ready_reports_the_registry(client):
    body = client.get("/ready").json()
    assert body["ready"] is True and body["devices"] == 42


# ----- status -----


def test_status_reports_the_mode_and_the_tier(client):
    body = client.get("/v1/status").json()
    assert body["mode"] == BrainMode.NORMAL
    assert body["capability_tier"] == "T2"
    assert body["epoch"] == 0 and body["devices"] == 42
    assert body["zones"] == sorted(body["zones"]) and len(body["zones"]) == 6
    assert body["versions"]["schema"] == "1"


def test_status_counts_what_the_edge_rejected(client, rig):
    rig.app.state.counters["BAD_SIGNATURE"] += 3
    assert client.get("/v1/status").json()["counters"]["BAD_SIGNATURE"] == 3


# ----- zones -----


def test_a_zone_reports_its_channels_and_bands(client):
    body = client.get("/v1/zones/zone-1").json()
    assert body["zone_id"] == "zone-1" and len(body["channels"]) == 7
    kinds = {c["kind"] for c in body["channels"]}
    assert kinds == {"WATER_LEVEL", "TRAFFIC_FLOW", "POWER_LOAD", "AIR_QUALITY", "CALL_RATE"}
    assert body["water_bands"] == {"low": 3.0, "high": 6.0, "critical": 9.0}


def test_a_channel_carries_its_estimate_and_trust(client):
    channels = {c["device_id"]: c for c in client.get("/v1/zones/zone-1").json()["channels"]}
    water = channels["z1-wl1"]
    assert water["estimate"]["value"] > 0 and water["estimate"]["stale"] is False
    assert water["trust_state"] == "TRUSTED" and water["state_kind"] in ("VALIDATED", "OBSERVED")
    assert water["actuator"]["capability"] == "valve"


def test_an_unknown_zone_is_a_404(client):
    assert client.get("/v1/zones/zone-99").status_code == 404


# ----- incidents, actions and explanations -----


def test_incidents_are_listed(client):
    assert "incidents" in client.get("/v1/incidents").json()


def test_an_unknown_incident_is_a_404(client):
    assert client.get("/v1/incidents/nope").status_code == 404


def test_an_unknown_action_is_a_404(client):
    assert client.get("/v1/actions/nope").status_code == 404


def test_a_decision_answers_the_nine_questions(client, rig):
    assert rig.decisions, "the run must have decided something"
    decision_id = rig.decisions[0]["id"]
    body = client.get(f"/v1/explain/{decision_id}").json()
    assert body["decision_id"] == decision_id
    assert set(body["answers"]) == {
        "what_happened", "how_sure", "why_this_action", "what_was_rejected", "what_it_rests_on",
        "what_would_change_it", "what_the_effect_was", "who_may_undo_it", "what_the_risk_was",
    }
    assert body["answers"]["why_this_action"]


def test_an_unknown_decision_is_a_404(client):
    assert client.get("/v1/explain/0-9999").status_code == 404


# ----- the operator's levers -----


def test_pausing_needs_the_token(client):
    assert client.post("/v1/control/pause").status_code == 401
    assert client.post("/v1/control/pause", headers={"Authorization": "Bearer nope"}).status_code == 401


def test_pausing_and_resuming_moves_the_mode(client):
    assert client.post("/v1/control/pause", headers=auth()).json()["mode"] == "PAUSED"
    assert client.get("/v1/status").json()["mode"] == "PAUSED"
    assert client.post("/v1/control/resume", headers=auth()).json()["mode"] == "NORMAL"


def test_approving_needs_the_token(client):
    response = client.post("/v1/approvals/act-1", json={"decision": "approve"})
    assert response.status_code == 401


def test_approving_an_action_that_is_not_waiting_is_a_404(client):
    response = client.post("/v1/approvals/act-1", json={"decision": "approve"}, headers=auth())
    assert response.status_code == 404


def test_a_bad_decision_is_rejected(client):
    response = client.post("/v1/approvals/act-1", json={"decision": "maybe"}, headers=auth())
    assert response.status_code == 422


def test_the_approval_list_is_empty_when_nothing_waits(client):
    assert client.get("/v1/approvals").json() == {"approvals": []}


async def test_a_waiting_action_can_be_approved_over_http(tmp_path):
    """A quarantined valve with rising water asks the operator; the API answers for them."""
    rig = await Rig(tmp_path, guardian_policy=quarantine_at("z1-wl1", 1.0), ticks=3).start()
    state = rig.app.state
    state.mode = BrainMode.NORMAL
    _plant_approval(state)
    with rig.client() as client:
        listed = client.get("/v1/approvals").json()["approvals"]
        assert [a["action_id"] for a in listed] == ["act-waiting"]
        assert listed[0]["risk_class"] == "R3"
        body = client.post("/v1/approvals/act-waiting", json={"decision": "deny"}, headers=auth())
        assert body.status_code == 200
    assert state.actions["act-waiting"].status is ActionStatus.BLOCKED
    assert "OPERATOR_DENIED" in state.actions["act-waiting"].reasons


def _plant_approval(state):
    """One R3 action waiting for an operator, as the containment exception leaves it."""
    from city_brain.core.shield import ShieldResult
    from city_brain.core.state import ActionRecord
    from city_brain.domain.types import ActionIntent, ExpectedEffect, RiskClass

    intent = ActionIntent(
        action_id="act-waiting", incident_id="flood-risk:z1-wl1:1", plan_version=1,
        capability="valve", target="z1-wl1", state="open", risk_class=RiskClass.R3,
        expected_effect=ExpectedEffect(channel="z1-wl1", metric="rate", comparator="<=",
                                       threshold=-0.01, window_s=3.0),
        evidence=(), rationale="the tank is filling and the valve is quarantined",
        alternatives=(), containment_override=True,
    )
    state.actions["act-waiting"] = ActionRecord(
        intent=intent, status=ActionStatus.AWAITING_APPROVAL, key="k", dispatched_at=None,
        reasons=(), shield=ShieldResult(passed=True, refusals=(), checks=()),
    )
    state.pending_approvals["act-waiting"] = state.sim_now


# ----- metrics -----


def test_metrics_are_prometheus_text(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    body = response.text
    assert "# TYPE city_brain_mode gauge" in body
    assert 'city_brain_mode{mode="NORMAL"} 1' in body
    assert "city_brain_events_total " in body


def test_metrics_count_the_counters(client, rig):
    rig.app.state.counters["DUPLICATE"] += 2
    assert 'city_brain_counter_total{code="DUPLICATE"} 2' in client.get("/metrics").text


# ----- the contract -----


def test_the_openapi_document_describes_every_route(client):
    paths = client.get("/openapi.json").json()["paths"]
    for route in (
        "/health", "/ready", "/v1/status", "/v1/zones/{zone}", "/v1/incidents",
        "/v1/incidents/{incident_id}", "/v1/actions/{action_id}", "/v1/explain/{decision_id}",
        "/v1/approvals", "/v1/approvals/{action_id}", "/v1/control/pause",
        "/v1/control/resume", "/metrics",
    ):
        assert route in paths, route


def test_a_query_never_changes_what_the_brain_would_do(client, rig):
    before = (rig.app.state.event_seq, dict(rig.app.state.counters), str(rig.app.state.mode))
    for route in ("/v1/status", "/v1/zones/zone-1", "/v1/incidents", "/metrics", "/ready"):
        client.get(route)
    assert (rig.app.state.event_seq, dict(rig.app.state.counters), str(rig.app.state.mode)) == before
