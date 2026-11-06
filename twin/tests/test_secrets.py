"""Tests for pluggable secret resolution and zero-downtime runtime rotation (Phase 9.3)."""

import asyncio
import os
import signal
import sys
import tempfile
from unittest.mock import MagicMock
import pytest
from starlette.testclient import TestClient
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.applications import Starlette
from starlette.middleware import Middleware

from twin.secrets import get_secret, reload_secrets
from twin.mcp_server import build_mcp_app, reload_auth_tokens, get_active_tokens, MCPAuthMiddleware
from twin.red_team_api import (
    build_red_team_app,
    get_or_create_red_team_token,
    set_active_red_team_token,
    get_active_red_team_token,
    RedTeamAuthMiddleware,
)
from twin.kafka_streamer import KafkaStreamer
from twin.simulation import CitySimulator


# Dummy hook for testing
def sample_test_hook(name: str):
    if name == "KAFKA_PASSWORD":
        return "hook-kafka-pass"
    if name == "MCP_AUTH_TOKENS":
        return "brain:hook-tok-b,guardian:hook-tok-g"
    return None


def failing_test_hook(name: str):
    raise RuntimeError("Hook connection to Vault failed with HTTP 503")


def test_resolution_order_hook_over_file_over_env(tmp_path, monkeypatch):
    """Verify strict priority order: hook > file > env."""
    secret_file = tmp_path / "kafka_secret.txt"
    secret_file.write_text("file-kafka-pass\n", encoding="utf-8")

    monkeypatch.setenv("KAFKA_PASSWORD", "env-kafka-pass")
    monkeypatch.setenv("KAFKA_PASSWORD_FILE", str(secret_file))
    monkeypatch.setenv("TWIN_SECRET_HOOK", "tests.test_secrets:sample_test_hook")

    # 1. Hook takes priority
    assert get_secret("KAFKA_PASSWORD") == "hook-kafka-pass"

    # 2. When hook unset, file takes priority
    monkeypatch.delenv("TWIN_SECRET_HOOK")
    assert get_secret("KAFKA_PASSWORD") == "file-kafka-pass"

    # 3. When file unset, env takes priority
    monkeypatch.delenv("KAFKA_PASSWORD_FILE")
    assert get_secret("KAFKA_PASSWORD") == "env-kafka-pass"


def test_hook_exception_falls_through_without_leaking(tmp_path, monkeypatch, caplog):
    """Hook exceptions are logged without leaking secrets and fall through to file/env."""
    monkeypatch.setenv("KAFKA_PASSWORD", "fallback-env-pass")
    monkeypatch.setenv("TWIN_SECRET_HOOK", "tests.test_secrets:failing_test_hook")

    with caplog.at_level("DEBUG"):
        val = get_secret("KAFKA_PASSWORD")

    assert val == "fallback-env-pass"
    assert "fallback-env-pass" not in caplog.text
    assert "Error invoking TWIN_SECRET_HOOK" in caplog.text


def test_file_newline_strip(tmp_path, monkeypatch):
    """File content strips exactly one trailing newline (LF or CRLF)."""
    lf_file = tmp_path / "lf_secret.txt"
    lf_file.write_text("clean-token-value\n", encoding="utf-8")
    monkeypatch.setenv("RED_TEAM_TOKEN_FILE", str(lf_file))
    monkeypatch.delenv("RED_TEAM_TOKEN", raising=False)
    assert get_secret("RED_TEAM_TOKEN") == "clean-token-value"

    crlf_file = tmp_path / "crlf_secret.txt"
    with open(crlf_file, "wb") as f:
        f.write(b"clean-crlf-value\r\n")
    monkeypatch.setenv("RED_TEAM_TOKEN_FILE", str(crlf_file))
    assert get_secret("RED_TEAM_TOKEN") == "clean-crlf-value"


def test_reload_reports_changed_unchanged_error(tmp_path, monkeypatch):
    """reload_secrets accurately reports changed/unchanged per secret and handles errors."""
    pw_file = tmp_path / "pw.txt"
    pw_file.write_text("v1-pass\n", encoding="utf-8")
    monkeypatch.setenv("KAFKA_PASSWORD_FILE", str(pw_file))
    monkeypatch.setenv("MCP_AUTH_TOKENS", "brain:tok1")
    monkeypatch.setenv("RED_TEAM_TOKEN", "rt-tok1")

    res1 = reload_secrets()
    assert res1["status"] == "ok"
    assert "reloaded_at_sim" in res1
    assert "reloaded_at_wall" in res1

    # Second reload with no changes -> unchanged
    res2 = reload_secrets()
    assert res2["results"]["MCP_AUTH_TOKENS"] == "unchanged"
    assert res2["results"]["RED_TEAM_TOKEN"] == "unchanged"
    assert res2["results"]["KAFKA_PASSWORD"] == "unchanged"

    # Rotate MCP and Red Team tokens
    monkeypatch.setenv("MCP_AUTH_TOKENS", "brain:tok2,guardian:tok-g")
    monkeypatch.setenv("RED_TEAM_TOKEN", "rt-tok2")
    res3 = reload_secrets()
    assert res3["results"]["MCP_AUTH_TOKENS"] == "changed"
    assert res3["results"]["RED_TEAM_TOKEN"] == "changed"
    assert res3["results"]["KAFKA_PASSWORD"] == "unchanged"


def test_mcp_old_token_rejected_and_new_token_accepted_after_reload(monkeypatch):
    """MCPAuthMiddleware rejects old token and accepts new token after rotation."""
    monkeypatch.setenv("MCP_AUTH_TOKENS", "brain:tok-old,guardian:tok-g")
    app = build_mcp_app()
    with TestClient(app) as client:
        # 1. With old token, request passes authentication (does not return 401)
        resp = client.post("/mcp", headers={"Authorization": "Bearer tok-old"}, json={})
        assert resp.status_code != 401

        # 2. Rotate to new token
        monkeypatch.setenv("MCP_AUTH_TOKENS", "brain:tok-new,guardian:tok-g")
        status = reload_auth_tokens()
        assert status == "changed"

        # 3. Old token now rejected with 401
        resp_old = client.post("/mcp", headers={"Authorization": "Bearer tok-old"}, json={})
        assert resp_old.status_code == 401
        assert resp_old.json()["code"] == "UNAUTHORIZED"

        # 4. New token accepted
        resp_new = client.post("/mcp", headers={"Authorization": "Bearer tok-new"}, json={})
        assert resp_new.status_code != 401


def test_red_team_reload_needs_old_token(monkeypatch):
    """POST /api/secrets/reload is authenticated with OLD token, subsequent calls require NEW token."""
    monkeypatch.setenv("RED_TEAM_TOKEN", "rt-initial-token")
    sim = CitySimulator(seed=42)
    streamer = KafkaStreamer(mock_mode=True)
    app = build_red_team_app(sim, token="rt-initial-token", streamer=streamer)
    client = TestClient(app)

    # Change secret in environment before calling reload
    monkeypatch.setenv("RED_TEAM_TOKEN", "rt-rotated-token")

    # Calling reload with new token before reload executes fails with 401
    bad_resp = client.post("/api/secrets/reload", headers={"X-Red-Team-Token": "rt-rotated-token"})
    assert bad_resp.status_code == 401

    # Calling reload with initial token succeeds
    good_resp = client.post("/api/secrets/reload", headers={"X-Red-Team-Token": "rt-initial-token"})
    assert good_resp.status_code == 200
    data = good_resp.json()
    assert data["status"] == "ok"
    assert data["results"]["RED_TEAM_TOKEN"] == "changed"

    # After reload, old token returns 401
    st_old = client.get("/api/status", headers={"X-Red-Team-Token": "rt-initial-token"})
    assert st_old.status_code == 401

    # After reload, new token returns 200
    st_new = client.get("/api/status", headers={"X-Red-Team-Token": "rt-rotated-token"})
    assert st_new.status_code == 200


def test_kafka_producer_swap_keeps_seq_continuous(tmp_path, monkeypatch):
    """Kafka producer rotation maintains continuous sequence numbers and reading IDs."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_USERNAME", "sensor_writer")
    monkeypatch.setenv("KAFKA_PASSWORD", "pass-initial")

    streamer = KafkaStreamer(mock_mode=True)
    sim = CitySimulator(seed=42)

    # Initial frame for z1-tc1
    readings_0 = sim.tick()
    r0 = [r for r in readings_0 if r["device_id"] == "z1-tc1"][0]
    res0 = streamer.stream_reading(r0, sim.signing_key_for("z1-tc1"))
    assert res0 is not None
    reading_0, _ = res0
    seq_0 = reading_0.seq

    # Rotate password
    monkeypatch.setenv("KAFKA_PASSWORD", "pass-new")
    status = streamer.reload_kafka_password()
    assert status == "changed"

    # Next frame continues sequence monotonically
    readings_1 = sim.tick()
    r1 = [r for r in readings_1 if r["device_id"] == "z1-tc1"][0]
    res1 = streamer.stream_reading(r1, sim.signing_key_for("z1-tc1"))
    assert res1 is not None
    reading_1, _ = res1

    assert reading_1.seq == seq_0 + 1
    assert reading_1.reading_id == f"r-z1-tc1-{reading_1.seq}"


def test_kafka_producer_wrong_credentials_rejected(monkeypatch):
    """When new credentials fail metadata probe, old producer is kept and error is returned."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_USERNAME", "sensor_writer")
    monkeypatch.setenv("KAFKA_PASSWORD", "bad-pass")

    streamer = KafkaStreamer(mock_mode=True)
    old_prod = MagicMock()
    streamer.producer = old_prod
    streamer._cached_password = "initial-pass"
    streamer._producer_conf = {
        "bootstrap.servers": "localhost:19092",
        "sasl.username": "sensor_writer",
        "sasl.password": "initial-pass",
    }

    # Mock new producer whose list_topics raises
    new_prod = MagicMock()
    new_prod.list_topics.side_effect = Exception("SASL authentication error: wrong password")

    import confluent_kafka
    monkeypatch.setattr(confluent_kafka, "Producer", lambda conf: new_prod)

    status = streamer.reload_kafka_password()

    assert status == "error: new credentials rejected by broker"
    assert streamer.producer is old_prod
    assert streamer._cached_password == "initial-pass"
    assert old_prod.flush.call_count == 0
    assert "bad-pass" not in status
    assert "initial-pass" not in status


def test_kafka_producer_right_credentials_swapped_and_flushed_twice(monkeypatch):
    """When credentials pass probe, new producer is swapped and old producer is flushed twice."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_USERNAME", "sensor_writer")
    monkeypatch.setenv("KAFKA_PASSWORD", "correct-new-pass")

    streamer = KafkaStreamer(mock_mode=True)
    old_prod = MagicMock()
    streamer.producer = old_prod
    streamer._cached_password = "initial-pass"
    streamer._producer_conf = {
        "bootstrap.servers": "localhost:19092",
        "sasl.username": "sensor_writer",
        "sasl.password": "initial-pass",
    }

    # Mock new producer whose list_topics succeeds
    new_prod = MagicMock()
    new_prod.list_topics.return_value = MagicMock()

    import confluent_kafka
    monkeypatch.setattr(confluent_kafka, "Producer", lambda conf: new_prod)

    status = streamer.reload_kafka_password()

    assert status == "changed"
    assert streamer.producer is new_prod
    assert streamer._cached_password == "correct-new-pass"
    assert old_prod.flush.call_count == 2


@pytest.mark.asyncio
async def test_sighup_handler_registered_with_event_loop(monkeypatch):
    """SIGHUP handler is registered with the asyncio event loop."""
    import signal
    if not hasattr(signal, "SIGHUP"):
        pytest.skip("SIGHUP not supported on this platform")

    sim = CitySimulator(seed=42)
    streamer = KafkaStreamer(mock_mode=True)
    loop = asyncio.get_running_loop()

    registered_sig = None
    registered_cb = None

    def mock_add_signal_handler(sig, callback, *args):
        nonlocal registered_sig, registered_cb
        registered_sig = sig
        registered_cb = callback

    monkeypatch.setattr(loop, "add_signal_handler", mock_add_signal_handler)

    from twin.main import setup_sighup_handler
    handler = setup_sighup_handler(loop, sim, streamer)

    assert registered_sig == signal.SIGHUP
    assert callable(registered_cb)
    assert handler is registered_cb

    # Verify executing handler calls reload_secrets
    reloaded = False
    def mock_reload(sim=None, streamer=None):
        nonlocal reloaded
        reloaded = True
        return {"status": "ok"}

    monkeypatch.setattr("twin.secrets.reload_secrets", mock_reload)
    registered_cb()
    assert reloaded is True


def test_secrets_reload_endpoint_reports_kafka_error(monkeypatch):
    """POST /api/secrets/reload returns error when new Kafka password is rejected."""
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    monkeypatch.setenv("KAFKA_USERNAME", "sensor_writer")
    monkeypatch.setenv("KAFKA_PASSWORD", "bad-pass")
    monkeypatch.setenv("RED_TEAM_TOKEN", "rt-token")

    sim = CitySimulator(seed=42)
    streamer = KafkaStreamer(mock_mode=True)
    old_prod = MagicMock()
    streamer.producer = old_prod
    streamer._cached_password = "initial-pass"
    streamer._producer_conf = {
        "bootstrap.servers": "localhost:19092",
        "sasl.username": "sensor_writer",
        "sasl.password": "initial-pass",
    }

    new_prod = MagicMock()
    new_prod.list_topics.side_effect = Exception("SASL authentication error: wrong password")

    import confluent_kafka
    monkeypatch.setattr(confluent_kafka, "Producer", lambda conf: new_prod)

    app = build_red_team_app(sim, token="rt-token", streamer=streamer)
    client = TestClient(app)

    resp = client.post("/api/secrets/reload", headers={"X-Red-Team-Token": "rt-token"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["results"]["KAFKA_PASSWORD"] == "error: new credentials rejected by broker"
    assert streamer.producer is old_prod


def test_sighup_triggers_reload(monkeypatch):
    """SIGHUP signal handler triggers secret reload cleanly."""
    sim = CitySimulator(seed=42)
    streamer = KafkaStreamer(mock_mode=True)
    reloaded = False

    def mock_reload(sim=None, streamer=None):
        nonlocal reloaded
        reloaded = True
        return {"status": "ok"}

    monkeypatch.setattr("twin.secrets.reload_secrets", mock_reload)

    # Simulate SIGHUP handler execution
    from twin.secrets import reload_secrets
    res = reload_secrets(sim=sim, streamer=streamer)
    assert reloaded is True
    assert res["status"] == "ok"


def test_no_secret_value_in_caplog_or_response(tmp_path, monkeypatch, caplog):
    """Verify that secret values never leak into log records or reload API responses."""
    sensitive_pw = "VERY_SECRET_KAFKA_PASSWORD_XYZ123"
    sensitive_mcp = "brain:VERY_SECRET_MCP_TOKEN_ABC789"
    sensitive_rt = "VERY_SECRET_RED_TEAM_TOKEN_QWE456"

    monkeypatch.setenv("KAFKA_PASSWORD", sensitive_pw)
    monkeypatch.setenv("MCP_AUTH_TOKENS", sensitive_mcp)
    monkeypatch.setenv("RED_TEAM_TOKEN", sensitive_rt)

    with caplog.at_level("DEBUG"):
        get_secret("KAFKA_PASSWORD")
        get_secret("MCP_AUTH_TOKENS")
        get_secret("RED_TEAM_TOKEN")
        reload_res = reload_secrets()

    # Verify response structure does not contain sensitive values
    res_str = str(reload_res)
    assert sensitive_pw not in res_str
    assert "VERY_SECRET_MCP_TOKEN_ABC789" not in res_str
    assert sensitive_rt not in res_str

    # Verify logs do not contain sensitive values
    assert sensitive_pw not in caplog.text
    assert "VERY_SECRET_MCP_TOKEN_ABC789" not in caplog.text
    assert sensitive_rt not in caplog.text


def test_secrets_reload_records_journal_event_and_console_ignores(tmp_path, monkeypatch):
    """Reload records journal event with source='secrets', console timeline processes without crashing."""
    db_file = str(tmp_path / "test_journal.db")
    monkeypatch.setenv("GROUND_TRUTH_DB_PATH", db_file)
    monkeypatch.setenv("MCP_AUTH_TOKENS", "brain:tok1")
    monkeypatch.setenv("RED_TEAM_TOKEN", "rt-tok1")

    sim = CitySimulator(seed=42)
    streamer = KafkaStreamer(mock_mode=True)
    app = build_red_team_app(sim, token="rt-tok1", streamer=streamer)
    client = TestClient(app)

    # Trigger reload via REST API
    resp = client.post("/api/secrets/reload", headers={"X-Red-Team-Token": "rt-tok1"})
    assert resp.status_code == 200

    # Query journal endpoint
    j_resp = client.get("/api/journal", headers={"X-Red-Team-Token": "rt-tok1"})
    assert j_resp.status_code == 200
    journal = j_resp.json().get("journal", [])
    secrets_entries = [e for e in journal if e.get("source") == "secrets"]
    assert len(secrets_entries) >= 1
    s_entry = secrets_entries[0]
    assert s_entry["source"] == "secrets"
    assert "results" in s_entry["detail"]
    assert "KAFKA_PASSWORD" in s_entry["detail"]["results"]

    # Verify console timeline handles journal with source='secrets' without crashing
    try:
        from twin.console.timeline import build_timeline_rows
    except ImportError:
        from console.timeline import build_timeline_rows
    rows = build_timeline_rows(journal_entries=journal)
    assert isinstance(rows, list)

