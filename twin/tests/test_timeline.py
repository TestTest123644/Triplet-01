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
9. Reviewer live cases: Port flood chain with (+1 more) Brain command, and Stolen key chain with earliest reading and rollback correlation.
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
            "id": 101,
            "seq": 10,
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

    # Verify chain summary matches expected format without 'needed'
    assert "z2-wl1 device_physical spoofed to 9.5" in atk_row.chain_summary
    assert "Guardian isolate_device +20 s" in atk_row.chain_summary
    assert "no Brain action" in atk_row.chain_summary


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
    assert "no Guardian action" in event_row.chain_summary
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


def test_reviewer_live_port_flood_chain():
    """Finding 5: Rebuild exact reviewer live case for rain_storm in zone-2 with multiple Brain commands."""
    device_zone_map = {"z2-wl1": "zone-2", "z2-wl2": "zone-2"}

    # Storm started 12:02:54, Brain commands at 12:17:16 (+862s), resolved 12:32:52
    journal_entries = [
        {
            "key": "event_start:ev-storm-1",
            "sim_ts": "2026-10-17T12:02:54Z",
            "source": "event",
            "detail": {
                "event_id": "ev-storm-1",
                "action": "start",
                "type": "rain_storm",
                "zone_id": "zone-2",
            },
        },
        {
            "key": "command:1",
            "sim_ts": "2026-10-17T12:17:16Z",
            "source": "command",
            "detail": {
                "caller": "brain",
                "tool": "set_valve",
                "args": {"device_id": "z2-wl1", "target_state": "open"},
                "executed": True,
                "refused_code": None,
            },
        },
        {
            "key": "command:2",
            "sim_ts": "2026-10-17T12:17:16Z",
            "source": "command",
            "detail": {
                "caller": "brain",
                "tool": "set_valve",
                "args": {"device_id": "z2-wl2", "target_state": "open"},
                "executed": True,
                "refused_code": None,
            },
        },
        {
            "key": "event_resolved:ev-storm-1",
            "sim_ts": "2026-10-17T12:32:52Z",
            "source": "event",
            "detail": {
                "event_id": "ev-storm-1",
                "action": "resolved",
                "type": "rain_storm",
                "zone_id": "zone-2",
                "resolved_by": "expired",
            },
        },
    ]

    rows = build_timeline_rows(journal_entries, device_zone_map=device_zone_map)

    # 1 event_start, 2 brain commands, 2 actuator executions, 1 event_resolved = 6 rows
    assert len(rows) == 6

    start_row = next(r for r in rows if r.event_type == "event_start")
    assert start_row.chain_id == "chain-1"

    brain_rows = [r for r in rows if r.lane == "Brain"]
    assert len(brain_rows) == 2
    for b in brain_rows:
        assert b.chain_id == "chain-1"
        assert b.reaction_time_s == 862.0

    actuator_rows = [r for r in rows if r.lane == "Actuator"]
    assert len(actuator_rows) == 2
    for a in actuator_rows:
        assert a.chain_id == "chain-1"
        assert a.reaction_time_s == 862.0

    resolved_row = next(r for r in rows if r.event_type == "event_resolved")
    assert resolved_row.chain_id == "chain-1"

    # Exactly matching reviewer expected chain summary:
    expected_summary = "rain_storm in zone-2 -> no Guardian action -> Brain set_valve +862 s (+1 more) -> Actuator executed -> resolved by expired"
    assert start_row.chain_summary == expected_summary
    for b in brain_rows:
        assert b.chain_summary == expected_summary


def test_reviewer_live_stolen_key_chain():
    """Finding 5: Rebuild exact reviewer live case for stolen key on z1-pl1 with DESC ledger readings,

    earliest reading picked, placed before Guardian row, and rollback carrying the chain id.
    """
    device_zone_map = {"z1-pl1": "zone-1"}

    # Attack injected 12:38:00Z, Guardian quarantine +2s (12:38:02Z), rollback +2s (12:38:02Z), clear at 12:39:06Z
    journal_entries = [
        {
            "key": "attack_inject:atk-key-1",
            "sim_ts": "2026-10-17T12:38:00Z",
            "source": "attack",
            "detail": {
                "attack_id": "atk-key-1",
                "action": "inject",
                "capability": "device_key",
                "target_device_id": "z1-pl1",
                "params": {"spoofed_value": 180.0},
            },
        },
        {
            "key": "containment:1",
            "sim_ts": "2026-10-17T12:38:02Z",
            "source": "containment",
            "detail": {
                "caller": "guardian",
                "tool": "quarantine_device",
                "target": "z1-pl1",
                "from_status": "ok",
                "to_status": "quarantined",
            },
        },
        {
            "key": "containment:2",
            "sim_ts": "2026-10-17T12:38:02Z",
            "source": "containment",
            "detail": {
                "caller": "guardian",
                "tool": "rollback_reading",
                "target": "r-z1-pl1-1128",
                "from_status": "ok",
                "to_status": "rolled_back",
            },
        },
        {
            "key": "attack_clear:atk-key-1",
            "sim_ts": "2026-10-17T12:39:06Z",
            "source": "attack",
            "detail": {
                "attack_id": "atk-key-1",
                "action": "clear",
                "target_device_id": "z1-pl1",
            },
        },
    ]

    # Ledger rows provided in DESC order as the old fetch returned them:
    # 1142 (+28s), 1130 (+5s), 1128 (+1s)
    ledger_readings_desc = [
        {
            "id": 1142,
            "seq": 1142,
            "reading_id": "r-z1-pl1-1142",
            "device_id": "z1-pl1",
            "ts": "2026-10-17T12:38:28Z",
            "true_value": 103.22,
            "broadcast_value": 180.00,
            "unit": "MW",
            "is_attacked": 1,
            "attack_type": "device_key",
        },
        {
            "id": 1130,
            "seq": 1130,
            "reading_id": "r-z1-pl1-1130",
            "device_id": "z1-pl1",
            "ts": "2026-10-17T12:38:05Z",
            "true_value": 102.50,
            "broadcast_value": 180.00,
            "unit": "MW",
            "is_attacked": 1,
            "attack_type": "device_key",
        },
        {
            "id": 1128,
            "seq": 1128,
            "reading_id": "r-z1-pl1-1128",
            "device_id": "z1-pl1",
            "ts": "2026-10-17T12:38:01Z",
            "true_value": 102.10,
            "broadcast_value": 180.00,
            "unit": "MW",
            "is_attacked": 1,
            "attack_type": "device_key",
        },
    ]

    rows = build_timeline_rows(journal_entries, attacked_readings=ledger_readings_desc, device_zone_map=device_zone_map)

    # Must pick earliest reading r-z1-pl1-1128
    reading_row = next(r for r in rows if r.event_type == "attacked_reading")
    assert "r-z1-pl1-1128" in reading_row.summary
    assert reading_row.chain_id == "chain-1"
    assert reading_row.reaction_time_s == 1.0  # 12:38:01 - 12:38:00

    # Ensure reading row is placed BEFORE the Guardian quarantine row in sorted rows
    reading_idx = rows.index(reading_row)
    quarantine_row = next(r for r in rows if r.event_type == "containment" and "quarantine_device" in r.summary)
    quarantine_idx = rows.index(quarantine_row)
    assert reading_idx < quarantine_idx, "Attacked reading must appear before Guardian quarantine in row ordering"

    # Ensure rollback row carries the chain id
    rollback_row = next(r for r in rows if r.event_type == "containment" and "rollback_reading" in r.summary)
    assert rollback_row.chain_id == "chain-1"
    assert rollback_row.reaction_time_s == 2.0  # 12:38:02 - 12:38:00

    # Verify chain summary matches expected format exactly:
    expected_summary = "z1-pl1 device_key spoofed to 180.0 -> Guardian quarantine_device +2 s (+1 more) -> no Brain action"
    atk_row = next(r for r in rows if r.event_type == "attack_inject")
    assert atk_row.chain_summary == expected_summary
    assert rollback_row.chain_summary == expected_summary


def test_chain_attribution_five_back_to_back_attacks():
    """Verify that 5 overlapping back-to-back attacks on one device each get their own action (Finding 3)."""
    device_zone_map = {"z3-tc1": "zone-3"}
    journal_entries = []

    # 5 back-to-back attacks: inject at T, action at T+2, clear at T+30, next inject at T+40
    for i in range(5):
        t_base = i * 40
        aid = f"atk-link-{i+1}"
        cid = f"chain-{i+1}"

        # Inject
        t_inj = f"2026-10-17T12:{t_base//60:02d}:{t_base%60:02d}Z"
        journal_entries.append({
            "key": f"attack_inject:{aid}",
            "sim_ts": t_inj,
            "source": "attack",
            "detail": {
                "attack_id": aid,
                "action": "inject",
                "capability": "link",
                "target_device_id": "z3-tc1",
                "params": {"mode": "corrupt"},
            },
        })

        # Guardian quarantine at +2s
        t_act = t_base + 2
        t_act_str = f"2026-10-17T12:{t_act//60:02d}:{t_act%60:02d}Z"
        journal_entries.append({
            "key": f"containment:{i+1}",
            "sim_ts": t_act_str,
            "source": "containment",
            "detail": {
                "caller": "guardian",
                "tool": "quarantine_device",
                "target": "z3-tc1",
                "from_status": "ok",
                "to_status": "quarantined",
            },
        })

        # Clear at +30s
        t_clr = t_base + 30
        t_clr_str = f"2026-10-17T12:{t_clr//60:02d}:{t_clr%60:02d}Z"
        journal_entries.append({
            "key": f"attack_clear:{aid}",
            "sim_ts": t_clr_str,
            "source": "attack",
            "detail": {
                "attack_id": aid,
                "action": "clear",
                "target_device_id": "z3-tc1",
            },
        })

    rows = build_timeline_rows(journal_entries, attacked_readings=[], device_zone_map=device_zone_map)

    # Every chain chain-1 to chain-5 must exist and contain exactly its own Guardian action at +2 s
    for i in range(1, 6):
        cid = f"chain-{i}"
        c_rows = [r for r in rows if r.chain_id == cid]
        assert len(c_rows) >= 3, f"Chain {cid} missing rows: {len(c_rows)}"

        g_rows = [r for r in c_rows if r.lane == "Guardian"]
        assert len(g_rows) == 1, f"Chain {cid} should have exactly 1 Guardian action, got {len(g_rows)}"
        assert g_rows[0].reaction_time_s == 2.0
        assert g_rows[0].formatted_reaction == "+2 s"

        atk_row = next(r for r in c_rows if r.event_type == "attack_inject")
        assert atk_row.formatted_reaction == ""
        assert "Guardian quarantine_device +2 s -> no Brain action" in atk_row.chain_summary


def test_rogue_device_chain_attribution():
    """Verify rogue device attack linking, naming, and rollback matching (Finding 4)."""
    device_zone_map = {"z1-pl1": "zone-1"}
    journal_entries = [
        {
            "key": "attack_inject:atk-rogue-1",
            "sim_ts": "2026-10-17T12:00:00Z",
            "source": "attack",
            "detail": {
                "attack_id": "atk-rogue-1",
                "action": "inject",
                "capability": "rogue",
                "target_device_id": None,
                "target_zone_id": None,
                "params": {
                    "rogue_device_id": "rg-p1",
                    "zone_id": "zone-2",
                    "device_type": "power_load",
                    "value": 88.88,
                },
            },
        },
        {
            "key": "containment:1",
            "sim_ts": "2026-10-17T12:00:04Z",
            "source": "containment",
            "detail": {
                "caller": "guardian",
                "tool": "quarantine_device",
                "target": "rg-p1",
                "from_status": "ok",
                "to_status": "quarantined",
            },
        },
        {
            "key": "containment:2",
            "sim_ts": "2026-10-17T12:00:06Z",
            "source": "containment",
            "detail": {
                "caller": "guardian",
                "tool": "rollback_reading",
                "target": "r-rg-p1-1",
                "from_status": "ok",
                "to_status": "rolled_back",
            },
        },
    ]

    rows = build_timeline_rows(journal_entries, attacked_readings=[], device_zone_map=device_zone_map)

    atk_row = next(r for r in rows if r.event_type == "attack_inject")
    assert atk_row.device_id == "rg-p1"
    assert atk_row.zone_id == "zone-2"
    assert atk_row.formatted_reaction == ""

    expected_summary = "rg-p1 rogue spoofed to 88.88 -> Guardian quarantine_device +4 s (+1 more) -> no Brain action"
    assert atk_row.chain_summary == expected_summary

    g_quarantine = next(r for r in rows if r.event_type == "containment" and "quarantine_device" in r.summary)
    assert g_quarantine.chain_id == "chain-1"
    assert g_quarantine.reaction_time_s == 4.0
    assert g_quarantine.formatted_reaction == "+4 s"

    g_rollback = next(r for r in rows if r.event_type == "containment" and "rollback_reading" in r.summary)
    assert g_rollback.chain_id == "chain-1"
    assert g_rollback.reaction_time_s == 6.0
    assert g_rollback.formatted_reaction == "+6 s"


def test_filter_timeline_rows_with_current_sim_ts():
    """Verify rows older than now - 5 min are excluded even when they are the newest rows (Finding 2)."""
    rows = [
        TimelineRow(
            row_id="e1",
            sim_ts="2026-10-17T12:36:00Z",
            lane="Twin",
            event_type="event_start",
            device_id=None,
            zone_id="zone-1",
            summary="Test event 10 minutes ago",
        )
    ]

    # At sim 12:46:00Z, Last 5 sim-min window cuts off at 12:41:00Z
    filtered = filter_timeline_rows(
        rows,
        window_sim_minutes=5,
        current_sim_ts="2026-10-17T12:46:00Z",
    )
    assert len(filtered) == 0, "Row at 12:36:00Z must be excluded under 5-min window when current time is 12:46:00Z"

    # With 15-min window, cutoff is 12:31:00Z, so 12:36:00Z is included
    filtered_15 = filter_timeline_rows(
        rows,
        window_sim_minutes=15,
        current_sim_ts="2026-10-17T12:46:00Z",
    )
    assert len(filtered_15) == 1


def test_event_escalated_chain_summary():
    """Verify chain summary includes '-> escalated to <to>' after Brain part."""
    journal_entries = [
        {
            "key": "event_start:ev-1",
            "sim_ts": "2026-10-17T12:00:00Z",
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
            "sim_ts": "2026-10-17T12:02:00Z",
            "source": "event",
            "detail": {
                "event_id": "ev-1",
                "action": "escalated",
                "type": "grid_overload",
                "zone_id": "zone-3",
                "to": "blackout",
            },
        },
    ]
    rows = build_timeline_rows(journal_entries)
    start_row = next(r for r in rows if r.event_type == "event_start")
    expected = "grid_overload in zone-3 -> no Guardian action -> no Brain action -> escalated to blackout"
    assert start_row.chain_summary == expected


def test_event_resolved_chain_summary():
    """Verify chain summary includes '-> resolved by <resolved_by>' at the end."""
    journal_entries = [
        {
            "key": "event_start:ev-2",
            "sim_ts": "2026-10-17T12:00:00Z",
            "source": "event",
            "detail": {
                "event_id": "ev-2",
                "action": "start",
                "type": "flood",
                "zone_id": "zone-1",
            },
        },
        {
            "key": "event_resolved:ev-2",
            "sim_ts": "2026-10-17T12:05:00Z",
            "source": "event",
            "detail": {
                "event_id": "ev-2",
                "action": "resolved",
                "type": "flood",
                "zone_id": "zone-1",
                "resolved_by": "set_valve:v1",
            },
        },
    ]
    rows = build_timeline_rows(journal_entries)
    start_row = next(r for r in rows if r.event_type == "event_start")
    expected = "flood in zone-1 -> no Guardian action -> no Brain action -> resolved by set_valve:v1"
    assert start_row.chain_summary == expected
