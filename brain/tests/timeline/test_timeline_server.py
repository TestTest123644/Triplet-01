"""The Timeline server: runs, events and the operator's levers (spec 12.3)."""

import json

import pytest
from fastapi.testclient import TestClient

from timeline.server import build_timeline_app

EVENTS = [
    {"specversion": "1.0", "id": "0-0", "source": "urn:tsyp14:triplet-07:brain",
     "type": "tn.tsyp.brain.status.v1", "subject": "city", "simtime": None, "epoch": 0,
     "correlationid": None, "actionid": None, "schemaversion": "1",
     "datacontenttype": "application/json", "data": {"reason": "startup"},
     "time": "2026-10-17T12:00:00Z", "prev_hash": "0" * 64, "hash": "a" * 64},
    {"specversion": "1.0", "id": "0-1", "source": "urn:tsyp14:triplet-07:brain",
     "type": "tn.tsyp.brain.decision.v1", "subject": "zone-1",
     "simtime": "2026-10-17T12:00:01.000000Z", "epoch": 0, "correlationid": "i-1",
     "actionid": None, "schemaversion": "1", "datacontenttype": "application/json",
     "data": {"outcome": "hold", "incident": {"zone": "zone-1", "device_id": "z1-wl1",
                                              "hold_reason": "STALE"},
              "rule_or_plan": "water_t0", "evidence": [], "alternatives": [],
              "rationale": "no readings", "would_change_if": ["readings arrive"]},
     "time": "2026-10-17T12:00:01Z", "prev_hash": "a" * 64, "hash": "b" * 64},
]
SUMMARY = {"scenario": "T01", "event_count": 2, "chain_valid": True,
           "commands_by_capability": {}, "max_water_level_by_device": {"z1-wl1": 3.4}}


@pytest.fixture
def logs(tmp_path):
    log = tmp_path / "T01-e0-20261017T120000000000Z.jsonl"
    log.write_bytes(b"".join(
        json.dumps(e, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
        for e in EVENTS))
    log.with_name(f"{log.stem}-summary.json").write_text(json.dumps(SUMMARY), encoding="utf-8")
    (tmp_path / "notes.txt").write_text("not a run", encoding="utf-8")
    return tmp_path


@pytest.fixture
def client(logs):
    with TestClient(build_timeline_app(logs, brain_api_url="http://brain.invalid:8100")) as client:
        yield client


RUN = "T01-e0-20261017T120000000000Z.jsonl"


# ----- the page -----


def test_the_page_is_served_whole(client):
    response = client.get("/")
    assert response.status_code == 200 and response.headers["content-type"].startswith("text/html")
    body = response.text
    assert "City Brain" in body and "Timeline" in body


def test_the_page_needs_nothing_from_the_network(client):
    body = client.get("/").text
    assert "http://" not in body.replace("http://brain.invalid", "")
    assert "<script src" not in body and "<link rel=\"stylesheet\"" not in body


def test_the_page_has_the_three_lanes_and_the_operator_controls(client):
    body = client.get("/").text
    for needed in ("Twin", "Guardian", "Brain", "Pause", "Resume", "operator token", "inspector"):
        assert needed in body, needed


def test_health_names_the_brain_api(client, logs):
    body = client.get("/health").json()
    assert body["brain_api"] == "http://brain.invalid:8100" and body["log_dir"] == str(logs)


# ----- runs -----


def test_runs_lists_the_logs_with_their_summaries(client):
    body = client.get("/api/runs").json()
    assert [r["name"] for r in body["runs"]] == [RUN]
    assert body["runs"][0]["scenario"] == "T01"
    assert body["runs"][0]["summary"]["scenario"] == "T01"
    assert body["runs"][0]["bytes"] > 0


def test_runs_ignores_anything_that_is_not_a_run(client):
    assert all(r["name"].endswith(".jsonl") for r in client.get("/api/runs").json()["runs"])


def test_an_empty_log_directory_is_not_an_error(tmp_path):
    with TestClient(build_timeline_app(tmp_path / "nothing-here")) as client:
        assert client.get("/api/runs").json()["runs"] == []


# ----- events -----


def test_events_are_returned_in_order(client):
    body = client.get(f"/api/events?run={RUN}").json()
    assert [e["id"] for e in body["events"]] == ["0-0", "0-1"]


def test_events_can_be_streamed(client):
    with client.stream("GET", f"/api/events?run={RUN}&stream=true") as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        text = "".join(response.iter_text())
    assert text.count("event: brain") == 2 and text.endswith("event: end\ndata: {}\n\n")


def test_an_unknown_run_is_a_404(client):
    assert client.get("/api/events?run=T99-e0-x.jsonl").status_code == 404


@pytest.mark.parametrize("name", ["../secrets.jsonl", "notes.txt", "sub/dir.jsonl"])
def test_a_run_outside_the_log_directory_is_refused(client, name):
    assert client.get(f"/api/events?run={name}").status_code in (400, 404)


def test_the_summary_is_served(client):
    assert client.get(f"/api/summary?run={RUN}").json()["scenario"] == "T01"


def test_a_missing_summary_is_a_404(logs):
    (logs / f"{RUN[:-6]}-summary.json").unlink()
    with TestClient(build_timeline_app(logs)) as client:
        assert client.get(f"/api/summary?run={RUN}").status_code == 404


# ----- the operator's levers -----


def test_a_control_without_a_token_is_refused(client):
    assert client.post("/api/control/pause").status_code == 401


def test_an_unknown_control_is_a_404(client):
    response = client.post("/api/control/explode", headers={"Authorization": "Bearer t"})
    assert response.status_code == 404


def test_an_unreachable_brain_is_reported_as_such(client):
    response = client.post("/api/control/pause", headers={"Authorization": "Bearer t"})
    assert response.status_code == 502


def test_an_approval_is_forwarded_to_the_brain(logs, monkeypatch):
    """The page talks to one origin; the Timeline carries the token to the Brain."""
    seen = {}

    class FakeReply:
        status_code = 200

        def json(self):
            return {"action_id": "act-1", "status": "DISPATCHED"}

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def request(self, method, url, content=None, headers=None):
            seen.update(method=method, url=url, content=content, headers=headers)
            return FakeReply()

    import timeline.server as server

    monkeypatch.setattr(server.httpx, "AsyncClient", FakeClient)
    with TestClient(build_timeline_app(logs, "http://brain:8100")) as client:
        body = client.post("/api/approvals/act-1", json={"decision": "approve"},
                           headers={"Authorization": "Bearer t0ken"})
    assert body.json()["status"] == "DISPATCHED"
    assert seen["url"] == "http://brain:8100/v1/approvals/act-1"
    assert seen["headers"]["Authorization"] == "Bearer t0ken"
    assert json.loads(seen["content"]) == {"decision": "approve"}
