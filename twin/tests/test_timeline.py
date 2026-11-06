"""Unit tests for the Timeline pure functions (Challenge Rule R3).

Tests:
1. Real entry parsing into 5 distinct lanes (Twin, Guardian, Brain, Actuator, Test harness).
2. Chain linking between attack/event and subsequent Guardian containment and Brain commands.
3. Reaction time calculation in simulation seconds.
4. Refused command representation in the Actuator lane.
5. Physical event escalation and resolution representation in the Twin lane.
6. First attacked reading per attack linked with reading_id and true vs broadcast values.
7. Partner log loading from TIMELINE_LOG_DIR.
8. Time window, zone, and device filtering.
"""

import json
import pytest

try:
    from twin.console.timeline import (
        TimelineRow,
        build_timeline_rows,
        filter_timeline_rows,
        format_reaction_delta,
        load_partner_log_entries,
        parse_rfc3339_to_seconds,
    )
except ImportError:
    from console.timeline import (
        TimelineRow,
        build_timeline_rows,
        filter_timeline_rows,
        format_reaction_delta,
        load_partner_log_entries,
        parse_rfc3339_to_seconds,
    )


def test_chain_linking_and_reaction_times():
    """Verify linking of attack -> Guardian containment -> Brain command with reaction times."""
    device_zone_map = {"z2-wl1": "zone-2", "z1-tc1": "zone-1"}

    # Exact shapes from specification
    journal_entries = [
        {
            "key": "attack_inject:atk-17a54038",
            "sim_ts": "2026-10-17T13:10:26Z",
            "source": "attack",
            "detail": {
                "attack_id": "atk-17a54038",
                "action": "inject",
                "capability": "device_physical",
                "target_device_id": "z2-wl1",
                "target_zone_id": None,
                "params": {"value": 9.5},
                "description": "Test water level spoofing",
            },
        },
        {
            "key": "containment:1",
            "sim_ts": "2026-10-17T13:10:46Z",
            "source": "containment",
            "detail": {
                "caller": "guardian",
                "tool": "isolate_device",
                "target": "z2-wl1",
                "from_status": "ok",
                "to_status": "isolated",
            },
        },
    ]

    attacked_readings = [
        {
            "reading_id": "r-z2-wl1-101",
            "device_id": "z2-wl1",
            "ts": "2026-10-17T13:10:28Z",
            "true_value": 7.05,
            "broadcast_value": 9.50,
            "unit": "meters",
            "is_attacked": 1,
            "attack_type": "device_physical",
        }
    ]

    rows = build_timeline_rows(journal_entries, attacked_readings=attacked_readings, device_zone_map=device_zone_map)

    # Must contain: attack row, attacked reading row, and guardian containment row
    assert len(rows) == 3

    atk_row = next(r for r in rows if r.event_type == "attack_inject")
    assert atk_row.lane == "Twin"
    assert atk_row.chain_id is not None
    assert atk_row.reaction_time_s == 0.0

    reading_row = next(r for r in rows if r.event_type == "attacked_reading")
    assert reading_row.lane == "Twin"
    assert reading_row.chain_id == atk_row.chain_id
    assert reading_row.reaction_time_s == 2.0  # 13:10:28 - 13:10:26
    assert "r-z2-wl1-101" in reading_row.summary
    assert "7.05" in reading_row.summary
    assert "9.50" in reading_row.summary

    guardian_row = next(r for r in rows if r.event_type == "containment")
    assert guardian_row.lane == "Guardian"
    assert guardian_row.chain_id == atk_row.chain_id
    assert guardian_row.reaction_time_s == 20.0  # 13:10:46 - 13:10:26

    # Verify chain summary matches expected format
    assert "z2-wl1 spoofed to 9.5" in atk_row.chain_summary
    assert "Guardian isolate_device +20 s" in atk_row.chain_summary
    assert "no Brain action needed" in atk_row.chain_summary


def test_brain_command_and_actuator_lanes():
    """Verify that Brain command splits into Brain decision and Actuator execution lanes."""
    journal_entries = [
        {
            "key": "event_start:ev-edc05b68",
            "sim_ts": "2026-10-17T12:11:30Z",
            "source": "event",
            "detail": {
                "event_id": "ev-edc05b68",
                "action": "start",
                "type": "traffic_accident",
                "zone_id": "zone-1",
                "device_id": "z1-tc1",
                "params": {},
            },
        },
        {
            "key": "command:1",
            "sim_ts": "2026-10-17T12:11:48Z",
            "source": "command",
            "detail": {
                "caller": "brain",
                "tool": "dispatch",
                "args": {"incident_type": "medical_emergency", "zone_id": "zone-1"},
                "executed": True,
                "refused_code": None,
            },
        },
    ]

    rows = build_timeline_rows(journal_entries)
    assert len(rows) == 3  # 1 event, 1 brain command, 1 actuator result

    event_row = next(r for r in rows if r.event_type == "event_start")
    assert event_row.lane == "Twin"

    brain_row = next(r for r in rows if r.lane == "Brain")
    assert brain_row.event_type == "command"
    assert "dispatch" in brain_row.summary
    assert brain_row.reaction_time_s == 18.0  # 12:11:48 - 12:11:30

    actuator_row = next(r for r in rows if r.lane == "Actuator")
    assert actuator_row.event_type == "actuation_result"
    assert "Actuator executed" in actuator_row.summary
    assert actuator_row.reaction_time_s == 18.0

    assert "traffic_accident in zone-1" in event_row.chain_summary
    assert "Brain dispatch +18 s" in event_row.chain_summary
    assert "Actuator executed" in event_row.chain_summary


def test_actuator_refused_command():
    """Verify that a refused command displays the refused code in the Actuator lane."""
    journal_entries = [
        {
            "key": "command:42",
            "sim_ts": "2026-10-17T14:00:00Z",
            "source": "command",
            "detail": {
                "caller": "brain",
                "tool": "set_valve",
                "args": {"device_id": "z2-wl1", "target_state": "open"},
                "executed": False,
                "refused_code": "ACTUATOR_ATTACK",
            },
        }
    ]

    rows = build_timeline_rows(journal_entries)
    act_row = next(r for r in rows if r.lane == "Actuator")
    assert "Actuator refused: set_valve blocked (ACTUATOR_ATTACK)" in act_row.summary


def test_scenario_lane_muted_test_harness():
    """Verify that entries called by 'scenario' are placed in the 'Test harness' lane."""
    journal_entries = [
        {
            "key": "containment:99",
            "sim_ts": "2026-10-17T15:00:00Z",
            "source": "containment",
            "detail": {
                "caller": "scenario",
                "tool": "reinstate_device",
                "target": "z1-tc1",
                "from_status": "isolated",
                "to_status": "ok",
            },
        },
        {
            "key": "command:100",
            "sim_ts": "2026-10-17T15:00:05Z",
            "source": "command",
            "detail": {
                "caller": "scenario",
                "tool": "dispatch",
                "args": {"zone_id": "zone-1"},
                "executed": True,
                "refused_code": None,
            },
        },
    ]

    rows = build_timeline_rows(journal_entries)
    assert len(rows) == 2
    for r in rows:
        assert r.lane == "Test harness"


def test_physical_event_escalation_and_resolution():
    """Verify that physical event start, escalation, and resolution are mapped into the Twin lane."""
    journal_entries = [
        {
            "key": "event_start:ev-1",
            "sim_ts": "2026-10-17T16:00:00Z",
            "source": "event",
            "detail": {
                "event_id": "ev-1",
                "action": "start",
                "type": "grid_overload",
                "zone_id": "zone-3",
            },
        },
        {
            "key": "event_escalated:ev-1",
            "sim_ts": "2026-10-17T16:01:00Z",
            "source": "event",
            "detail": {
                "event_id": "ev-1",
                "action": "escalated",
                "type": "grid_overload",
                "to": "blackout",
                "zone_id": "zone-3",
            },
        },
        {
            "key": "event_resolved:ev-1",
            "sim_ts": "2026-10-17T16:05:00Z",
            "source": "event",
            "detail": {
                "event_id": "ev-1",
                "action": "resolved",
                "type": "grid_overload",
                "zone_id": "zone-3",
                "resolved_by": "switch_grid:sw-1",
            },
        },
    ]

    rows = build_timeline_rows(journal_entries)
    assert len(rows) == 3
    for r in rows:
        assert r.lane == "Twin"

    assert "grid_overload to blackout in zone-3" in rows[1].summary
    assert "by switch_grid:sw-1" in rows[2].summary


def test_partner_log_entries_loading(tmp_path):
    """Verify loading partner verdict lines from TIMELINE_LOG_DIR."""
    log_file = tmp_path / "t9-sample.log"
    lines = [
        json.dumps({
            "wall_ts": "2026-11-06T09:36:44.623153+00:00",
            "sim_ts": "2026-10-17T13:56:09Z",
            "layer": "brain",
            "event": "implausible_reading_ignored",
            "details": {"device_id": "z1-pl1", "value": 180.0},
        }) + "\n",
        json.dumps({
            "wall_ts": "2026-11-06T09:36:44.634114+00:00",
            "sim_ts": "2026-10-17T13:56:09Z",
            "layer": "guardian",
            "event": "quarantine_device",
            "details": {"device_id": "z1-pl1", "reason": "physical limit violation"},
        }) + "\n",
    ]
    log_file.write_text("".join(lines), encoding="utf-8")

    entries = load_partner_log_entries(str(tmp_path))
    assert len(entries) == 2

    rows = build_timeline_rows([], log_entries=entries)
    assert len(rows) == 2
    guardian_row = next(r for r in rows if r.lane == "Guardian")
    assert "physical limit violation" in guardian_row.summary

    brain_row = next(r for r in rows if r.lane == "Brain")
    assert "implausible_reading_ignored" in brain_row.summary


def test_filter_timeline_rows():
    """Verify time window, zone, and device filtering."""
    rows = [
        TimelineRow(
            row_id="1",
            sim_ts="2026-10-17T12:00:00Z",
            lane="Twin",
            event_type="attack_inject",
            device_id="z1-tc1",
            zone_id="zone-1",
            summary="Attack 1",
        ),
        TimelineRow(
            row_id="2",
            sim_ts="2026-10-17T12:10:00Z",
            lane="Twin",
            event_type="attack_inject",
            device_id="z2-wl1",
            zone_id="zone-2",
            summary="Attack 2",
        ),
    ]

    # Filter by zone
    z1_rows = filter_timeline_rows(rows, zone_id="zone-1")
    assert len(z1_rows) == 1
    assert z1_rows[0].device_id == "z1-tc1"

    # Filter by device
    dev_rows = filter_timeline_rows(rows, device_id="z2-wl1")
    assert len(dev_rows) == 1
    assert dev_rows[0].zone_id == "zone-2"

    # Filter by 5-minute window from 12:12:00
    w_rows = filter_timeline_rows(rows, window_sim_minutes=5, current_sim_ts="2026-10-17T12:12:00Z")
    assert len(w_rows) == 1
    assert w_rows[0].row_id == "2"
