"""Tests for fail-fast Red Team preflight checks in scenarios and master runner."""

import json
import os
import sys
import time
from pathlib import Path
import pytest

REPO_ROOT = str(Path(__file__).resolve().parents[2])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scenarios._twin_common import ScenarioContext, TwinRestClient
import scenarios.run_all as run_all


def test_run_all_preflight_wrong_token_exits_2_fast(monkeypatch, capsys):
    """Wrong Red Team token causes run_all to exit with code 2 in under 5 seconds."""
    def mock_get_status(self):
        return {"status_code": 401, "error": {"code": "UNAUTHORIZED", "message": "Missing or invalid token"}}

    monkeypatch.setattr(run_all.TwinRestClient, "get_status", mock_get_status)
    monkeypatch.setattr(sys, "argv", ["run_all.py", "--red-team-token", "wrong_token"])

    start = time.monotonic()
    code = run_all.main()
    elapsed = time.monotonic() - start

    assert code == 2
    assert elapsed < 5.0, f"run_all took {elapsed:.2f}s (expected < 5s)"
    captured = capsys.readouterr()
    assert "Invalid or missing RED_TEAM_TOKEN / --red-team-token" in captured.err
    assert "docker exec twin_layer cat /app/data/red_team.token" in captured.err


def test_run_all_preflight_connection_refused_exits_2_fast(monkeypatch, capsys):
    """Connection refused causes run_all to exit with code 2 in under 5 seconds."""
    def mock_get_status(self):
        return {"status_code": 0, "error": "[Errno 61] Connection refused"}

    monkeypatch.setattr(run_all.TwinRestClient, "get_status", mock_get_status)
    monkeypatch.setattr(sys, "argv", ["run_all.py"])

    start = time.monotonic()
    code = run_all.main()
    elapsed = time.monotonic() - start

    assert code == 2
    assert elapsed < 5.0, f"run_all took {elapsed:.2f}s (expected < 5s)"
    captured = capsys.readouterr()
    assert "Twin is not running at" in captured.err
    assert "connection refused" in captured.err


def test_scenario_context_preflight_401_logs_and_exits_without_subprocess(tmp_path, monkeypatch):
    """ScenarioContext.__enter__ on 401 logs preflight_failed, writes result, and exits without starting mocks."""
    ctx = ScenarioContext("t_mock", "test_mock", log_dir=str(tmp_path), with_mocks=True)

    def mock_get_status(self):
        return {"status_code": 401, "error": {"code": "UNAUTHORIZED", "message": "Unauthorized access"}}

    monkeypatch.setattr(ctx.rest, "get_status", mock_get_status.__get__(ctx.rest, TwinRestClient))

    # Track if _start_mocks is ever called
    mocks_started = False
    orig_start_mocks = ctx._start_mocks

    def instrumented_start_mocks():
        nonlocal mocks_started
        mocks_started = True
        orig_start_mocks()

    ctx._start_mocks = instrumented_start_mocks

    with pytest.raises(SystemExit) as excinfo:
        with ctx:
            pass

    assert excinfo.value.code != 0
    assert not mocks_started, "Mocks must NOT be started when preflight fails"
    assert ctx._guardian_proc is None
    assert ctx._brain_proc is None

    # Verify log output
    assert os.path.exists(ctx.log_file)
    with open(ctx.log_file, "r", encoding="utf-8") as f:
        lines = [json.loads(line.strip()) for line in f if line.strip()]

    events = [entry.get("event") for entry in lines]
    assert "preflight_failed" in events

    preflight_entry = next(entry for entry in lines if entry.get("event") == "preflight_failed")
    assert preflight_entry.get("details", {}).get("status_code") == 401

    result_entry = lines[-1]
    assert result_entry.get("event") == "result"
    assert result_entry.get("pass") is False
    checks = {c["name"]: c for c in result_entry.get("checks", [])}
    assert "twin_reachable" in checks
    assert checks["twin_reachable"]["pass"] is False


def test_scenario_context_missing_sim_speed_fails_preflight(tmp_path, monkeypatch):
    """ScenarioContext.__enter__ when sim_speed is absent logs preflight_failed and exits without mocks."""
    ctx = ScenarioContext("t_mock2", "test_mock2", log_dir=str(tmp_path), with_mocks=True)

    def mock_get_status(self):
        return {"status": "ok"}  # Missing sim_speed

    monkeypatch.setattr(ctx.rest, "get_status", mock_get_status.__get__(ctx.rest, TwinRestClient))

    mocks_started = False

    def instrumented_start_mocks():
        nonlocal mocks_started
        mocks_started = True

    ctx._start_mocks = instrumented_start_mocks

    with pytest.raises(SystemExit) as excinfo:
        with ctx:
            pass

    assert excinfo.value.code != 0
    assert not mocks_started
    assert os.path.exists(ctx.log_file)
    with open(ctx.log_file, "r", encoding="utf-8") as f:
        lines = [json.loads(line.strip()) for line in f if line.strip()]

    events = [entry.get("event") for entry in lines]
    assert "preflight_failed" in events
    result_entry = lines[-1]
    assert result_entry.get("event") == "result"
    assert result_entry.get("pass") is False
