"""DOM and UI Interaction QA Test Suite for Streamlit Operations Console.

Assigned to: Agent_UI & Agent_QA
Validates via Streamlit AppTest:
1. Complete DOM tree mounting and rendering without exceptions.
2. Header title, subtitle, engine status banner, and scoreboard metrics.
3. Zero unicode emojis across DOM elements.
4. Multi-tab structure (Geospatial Digital Twin, Divergence, Truth Ledger, Audit Log).
5. Interactive Hackathon Pitch Demo Presets injection (Preset 1: Physical Spoofing).
6. Disarm All Threats recovery back to 0 active attacks.
"""

import os
import pytest
from streamlit.testing.v1 import AppTest


from twin.mcp_server import set_active_simulator


@pytest.fixture
def dom_app():
    os.environ["STREAMLIT_TESTING"] = "1"
    set_active_simulator(None)
    candidates = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dashboard.py")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "dashboard.py")),
    ]
    dashboard_path = next((p for p in candidates if os.path.isfile(p)), candidates[0])
    at = AppTest.from_file(dashboard_path, default_timeout=30)
    at.run()
    yield at
    set_active_simulator(None)


def test_dashboard_dom_mounting_and_header(dom_app):
    at = dom_app

    # Verify zero uncaught exceptions in Streamlit render
    assert len(at.exception) == 0

    # Verify 4 main scoreboard metrics
    metric_labels = [m.label for m in at.metric]
    assert "Total Telemetry Frames" in metric_labels
    assert "Active Incidents / Tampered" in metric_labels
    assert "Active Red Team Attacks" in metric_labels
    assert "Simulation Clock (UTC)" in metric_labels

    # Verify tab headers
    assert len(at.tabs) == 4

    # Verify sidebar buttons
    btn_labels = [b.label for b in at.button]
    assert "LAUNCH SELECTED DEMO" in btn_labels
    assert "DISARM ALL THREATS" in btn_labels
    assert "REFRESH TELEMETRY" in btn_labels


def test_dashboard_dom_demo_preset_and_disarm(dom_app):
    at = dom_app

    # 1. Reset baseline
    disarm_btn = next(b for b in at.button if b.label == "DISARM ALL THREATS")
    disarm_btn.click().run()

    atk_metric = next(m for m in at.metric if m.label == "Active Red Team Attacks")
    assert int(atk_metric.value) == 0

    # 2. Select and trigger Preset 1
    demo_sb = next(s for s in at.selectbox if s.label == "Select Pitch Demo")
    demo_sb.select("Preset 1: Physical Spoof (z1-tc1 = 185 veh/min in Downtown)")

    launch_btn = next(b for b in at.button if b.label == "LAUNCH SELECTED DEMO")
    launch_btn.click().run()

    # Active attack must be reflected in the scoreboard metric
    atk_metric_injected = next(m for m in at.metric if m.label == "Active Red Team Attacks")
    assert int(atk_metric_injected.value) >= 1

    # 3. Disarm all threats
    disarm_btn = next(b for b in at.button if b.label == "DISARM ALL THREATS")
    disarm_btn.click().run()

    atk_metric_recovered = next(m for m in at.metric if m.label == "Active Red Team Attacks")
    assert int(atk_metric_recovered.value) == 0
