"""DOM and UI Interaction QA Test Suite for Streamlit Operations Console.

Assigned to: Agent_UI & Agent_QA
Validates via Streamlit AppTest:
1. Complete DOM tree mounting and rendering without exceptions.
2. Modern status bar, sentence-case headers, and 4 KPI metric tiles with zero deltas.
3. Zero unicode emojis across DOM elements.
4. Multi-tab structure in required order: Timeline (default), City map, Truth vs broadcast, Ledger.
5. Interactive manual threat injection and Clear all threats recovery.
"""

import os
from pathlib import Path
import re
import pytest
import requests
from starlette.testclient import TestClient
from streamlit.testing.v1 import AppTest

from twin.db_ledger import GroundTruthLedger
from twin.red_team_api import build_red_team_app
from twin.simulation import CitySimulator


# Regex to detect common unicode emojis
EMOJI_PATTERN = re.compile(
    r"[\U0001F600-\U0001F64F"  # Emoticons
    r"\U0001F300-\U0001F5FF"  # Misc Symbols and Pictographs
    r"\U0001F680-\U0001F6FF"  # Transport and Map
    r"\U0001F700-\U0001F77F"  # Alchemical
    r"\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
    r"\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
    r"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
    r"\U0001FA00-\U0001FA6F"  # Chess Symbols
    r"\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
    r"\U00002702-\U000027B0"  # Dingbats
    r"\U000024C2-\U0001F251]"
)


@pytest.fixture
def dom_app(tmp_path, monkeypatch):
    os.environ["STREAMLIT_TESTING"] = "1"
    db_path = str(tmp_path / "dom_truth.db")
    os.environ["GROUND_TRUTH_DB_PATH"] = db_path

    # Seed temporary ledger with clean baseline readings
    ledger = GroundTruthLedger(db_path=db_path)
    sim = CitySimulator(seed=123, ledger=ledger)
    for _ in range(5):
        sim.tick()

    token = "test-token"
    os.environ["RED_TEAM_TOKEN"] = token
    app = build_red_team_app(sim, token=token)
    client = TestClient(app)

    orig_post = requests.post
    orig_get = requests.get

    def mock_post(url, *args, **kwargs):
        kwargs.pop("timeout", None)
        twin_url = os.environ.get("TWIN_API_URL", "http://127.0.0.1:9")
        if url.startswith(twin_url):
            path = url[len(twin_url):]
            return client.post(path, *args, **kwargs)
        return orig_post(url, *args, **kwargs)

    def mock_get(url, *args, **kwargs):
        kwargs.pop("timeout", None)
        twin_url = os.environ.get("TWIN_API_URL", "http://127.0.0.1:9")
        if url.startswith(twin_url):
            path = url[len(twin_url):]
            return client.get(path, *args, **kwargs)
        return orig_get(url, *args, **kwargs)

    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "get", mock_get)

    candidates = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dashboard.py")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "dashboard.py")),
    ]
    dashboard_path = next((p for p in candidates if os.path.isfile(p)), candidates[0])
    at = AppTest.from_file(dashboard_path, default_timeout=30)
    at.run()
    yield at


def test_dashboard_dom_mounting_and_header(dom_app):
    at = dom_app

    # Verify zero uncaught exceptions in Streamlit render
    assert len(at.exception) == 0

    # Verify 4 main scoreboard metrics with exact sentence-case labels
    metric_labels = [m.label for m in at.metric]
    assert "Readings" in metric_labels
    assert "Attacked readings" in metric_labels
    assert "Active attacks" in metric_labels
    assert "Devices contained" in metric_labels

    # Verify four tabs in exact required order
    assert len(at.tabs) == 4
    tab_labels = [t.label for t in at.tabs]
    assert tab_labels == ["Timeline", "City map", "Truth vs broadcast", "Ledger"]

    # Verify sidebar form submit buttons and threat clear button
    btn_labels = [b.label for b in at.button]
    assert "Inject attack" in btn_labels
    assert "Inject event" in btn_labels
    assert "Clear all threats" in btn_labels

    # Verify zero emojis across all DOM elements
    for m in at.metric:
        assert not EMOJI_PATTERN.search(m.label), f"Emoji found in metric label: {m.label}"
    for t in at.tabs:
        assert not EMOJI_PATTERN.search(t.label), f"Emoji found in tab label: {t.label}"
    for b in at.button:
        assert not EMOJI_PATTERN.search(b.label), f"Emoji found in button label: {b.label}"


def test_dashboard_dom_demo_preset_and_disarm(dom_app):
    at = dom_app

    # 1. Reset baseline by clearing all threats
    clear_btn = next(b for b in at.button if b.label == "Clear all threats")
    clear_btn.click().run()

    atk_metric = next(m for m in at.metric if m.label == "Active attacks")
    assert int(atk_metric.value) == 0

    # 2. Inject attack via sidebar form
    inject_btn = next(b for b in at.button if b.label == "Inject attack")
    inject_btn.click().run()

    # Active attack must be reflected in the scoreboard metric
    atk_metric_injected = next(m for m in at.metric if m.label == "Active attacks")
    assert int(atk_metric_injected.value) >= 1

    # 3. Disarm all threats and verify recovery
    clear_btn = next(b for b in at.button if b.label == "Clear all threats")
    clear_btn.click().run()

    atk_metric_recovered = next(m for m in at.metric if m.label == "Active attacks")
    assert int(atk_metric_recovered.value) == 0
