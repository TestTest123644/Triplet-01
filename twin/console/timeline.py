"""Pure timeline processing logic for the Twin operations console.

Fulfills Challenge Rule R3:
A live timeline screen showing reading -> Guardian verdict -> Brain decision -> actuator action,
with timestamps and correlated reaction chains.

This module contains pure data-transformation functions with zero Streamlit dependencies,
making it completely unit-testable.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import glob
import json
import os
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class TimelineRow:
    """Represents a single entry across the five console timeline lanes."""
    row_id: str
    sim_ts: str
    lane: str  # "Twin", "Guardian", "Brain", "Actuator", or "Test harness"
    event_type: str
    device_id: Optional[str]
    zone_id: Optional[str]
    summary: str
    chain_id: Optional[str] = None
    reaction_time_s: Optional[float] = None
    chain_summary: Optional[str] = None
    raw_json: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def parse_rfc3339_to_seconds(ts_str: str) -> float:
    """Convert RFC 3339 timestamp (e.g. 2026-10-17T13:10:26Z) to epoch seconds."""
    s = ts_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return dt.timestamp()


def format_seconds_to_rfc3339(seconds: float) -> str:
    """Convert epoch seconds to RFC 3339 UTC string with Z suffix."""
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def format_reaction_delta(delta_s: float) -> str:
    """Format reaction time delta in seconds (e.g. '+20 s')."""
    return f"+{int(round(delta_s))} s"


def extract_zone_from_device(device_id: Optional[str], device_zone_map: Dict[str, str]) -> Optional[str]:
    """Look up zone for a device ID, either from the map or by convention."""
    if not device_id:
        return None
    if device_id in device_zone_map:
        return device_zone_map[device_id]
    if device_id.startswith("z") and "-" in device_id:
        prefix = device_id.split("-")[0]
        if len(prefix) == 2 and prefix[1].isdigit():
            return f"zone-{prefix[1]}"
    return None


def load_partner_log_entries(log_dir: str) -> List[Dict[str, Any]]:
    """Read partner log lines from JSON-lines files in TIMELINE_LOG_DIR."""
    entries: List[Dict[str, Any]] = []
    if not log_dir or not os.path.isdir(log_dir):
        return entries

    log_files = glob.glob(os.path.join(log_dir, "*.log"))
    for file_path in log_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if "sim_ts" in record and record.get("layer") in ("guardian", "brain", "scenario"):
                        entries.append(record)
        except OSError:
            continue

    entries.sort(key=lambda x: str(x.get("sim_ts", "")))
    return entries


def build_timeline_rows(
    journal_entries: List[Dict[str, Any]],
    attacked_readings: Optional[List[Dict[str, Any]]] = None,
    device_zone_map: Optional[Dict[str, str]] = None,
    log_entries: Optional[List[Dict[str, Any]]] = None,
) -> List[TimelineRow]:
    """Transform journal entries, attacked ledger readings, and logs into correlated timeline rows.

    Lanes:
    - Twin: Attack injection/clearing, physical events, and first attacked reading per attack
    - Guardian: Containment actions called by guardian
    - Brain: Command decisions issued by brain
    - Actuator: Command execution results (executed true or refused with code)
    - Test harness: Containment and command actions called by scenario runner
    """
    if device_zone_map is None:
        device_zone_map = {}
    if attacked_readings is None:
        attacked_readings = []
    if log_entries is None:
        log_entries = []

    rows: List[TimelineRow] = []

    # 1. Process Journal Entries
    for idx, entry in enumerate(journal_entries):
        key = entry.get("key", f"entry:{idx}")
        sim_ts = str(entry.get("sim_ts", ""))
        source = entry.get("source", "")
        detail = entry.get("detail", {})

        if source == "attack":
            action = detail.get("action", "")
            aid = detail.get("attack_id", "")
            target_dev = detail.get("target_device_id")
            target_zone = detail.get("target_zone_id") or extract_zone_from_device(target_dev, device_zone_map)
            cap = detail.get("capability", "")
            params = detail.get("params", {})

            if action == "inject":
                val = params.get("value") or params.get("spoofed_value")
                val_txt = f" (spoofed to {val})" if val is not None else ""
                summary = f"Attack injected: {cap} on {target_dev or target_zone}{val_txt}"
                rows.append(
                    TimelineRow(
                        row_id=key,
                        sim_ts=sim_ts,
                        lane="Twin",
                        event_type="attack_inject",
                        device_id=target_dev,
                        zone_id=target_zone,
                        summary=summary,
                        raw_json=entry,
                    )
                )
            elif action == "clear":
                summary = f"Attack cleared: {aid} on {target_dev or target_zone or 'target'}"
                rows.append(
                    TimelineRow(
                        row_id=key,
                        sim_ts=sim_ts,
                        lane="Twin",
                        event_type="attack_clear",
                        device_id=target_dev,
                        zone_id=target_zone,
                        summary=summary,
                        raw_json=entry,
                    )
                )

        elif source == "event":
            action = detail.get("action", "")
            eid = detail.get("event_id", "")
            etype = detail.get("type", "incident")
            zone_id = detail.get("zone_id")
            device_id = detail.get("device_id") or (extract_zone_from_device(zone_id, device_zone_map) if not zone_id else None)
            if not zone_id and device_id:
                zone_id = extract_zone_from_device(device_id, device_zone_map)

            if action == "start":
                summary = f"Physical event started: {etype} in {zone_id or 'city'}"
                rows.append(
                    TimelineRow(
                        row_id=key,
                        sim_ts=sim_ts,
                        lane="Twin",
                        event_type="event_start",
                        device_id=device_id,
                        zone_id=zone_id,
                        summary=summary,
                        raw_json=entry,
                    )
                )
            elif action == "escalated":
                to_state = detail.get("to", "escalated")
                summary = f"Physical event escalated: {etype} to {to_state} in {zone_id or 'city'}"
                rows.append(
                    TimelineRow(
                        row_id=key,
                        sim_ts=sim_ts,
                        lane="Twin",
                        event_type="event_escalated",
                        device_id=device_id,
                        zone_id=zone_id,
                        summary=summary,
                        raw_json=entry,
                    )
                )
            elif action == "resolved":
                resolved_by = detail.get("resolved_by", "mitigation")
                summary = f"Physical event resolved: {etype} by {resolved_by}"
                rows.append(
                    TimelineRow(
                        row_id=key,
                        sim_ts=sim_ts,
                        lane="Twin",
                        event_type="event_resolved",
                        device_id=device_id,
                        zone_id=zone_id,
                        summary=summary,
                        raw_json=entry,
                    )
                )

        elif source == "containment":
            caller = detail.get("caller", "anonymous")
            tool = detail.get("tool", "")
            target = detail.get("target", "")
            from_st = detail.get("from_status", "")
            to_st = detail.get("to_status", "")

            dev_id = target if not target.startswith("r-") else None
            zone_id = extract_zone_from_device(dev_id, device_zone_map)

            lane = "Test harness" if caller == "scenario" else "Guardian"
            summary = f"{tool} on {target} ({from_st} -> {to_st})" if from_st and to_st else f"{tool} on {target}"

            rows.append(
                TimelineRow(
                    row_id=key,
                    sim_ts=sim_ts,
                    lane=lane,
                    event_type="containment",
                    device_id=dev_id,
                    zone_id=zone_id,
                    summary=summary,
                    raw_json=entry,
                )
            )

        elif source == "command":
            caller = detail.get("caller", "anonymous")
            tool = detail.get("tool", "")
            args = detail.get("args", {})
            executed = bool(detail.get("executed", True))
            refused_code = detail.get("refused_code")

            dev_id = args.get("device_id")
            zone_id = args.get("zone_id") or extract_zone_from_device(dev_id, device_zone_map)

            if caller == "scenario":
                summary = f"Scenario command: {tool} (executed={executed})"
                rows.append(
                    TimelineRow(
                        row_id=key,
                        sim_ts=sim_ts,
                        lane="Test harness",
                        event_type="command",
                        device_id=dev_id,
                        zone_id=zone_id,
                        summary=summary,
                        raw_json=entry,
                    )
                )
            else:
                # Brain decision row
                arg_desc = ", ".join(f"{k}={v}" for k, v in args.items()) if args else "no args"
                brain_summary = f"Command decision: {tool}({arg_desc})"
                rows.append(
                    TimelineRow(
                        row_id=f"{key}:brain",
                        sim_ts=sim_ts,
                        lane="Brain",
                        event_type="command",
                        device_id=dev_id,
                        zone_id=zone_id,
                        summary=brain_summary,
                        raw_json=entry,
                    )
                )

                # Actuator execution row
                if executed:
                    act_summary = f"Actuator executed: {tool} completed successfully"
                else:
                    code_msg = f" ({refused_code})" if refused_code else ""
                    act_summary = f"Actuator refused: {tool} blocked{code_msg}"

                rows.append(
                    TimelineRow(
                        row_id=f"{key}:actuator",
                        sim_ts=sim_ts,
                        lane="Actuator",
                        event_type="actuation_result",
                        device_id=dev_id,
                        zone_id=zone_id,
                        summary=act_summary,
                        raw_json=entry,
                    )
                )

    # 2. Correlate first attacked reading per attack
    # An attack in Twin lane (attack_inject) can have a matching reading in attacked_readings
    attacks = [r for r in rows if r.event_type == "attack_inject" and r.device_id]
    used_reading_ids: Set[str] = set()

    for atk in attacks:
        atk_ts_sec = parse_rfc3339_to_seconds(atk.sim_ts)
        matched_reading = None
        for r_truth in attacked_readings:
            rid = str(r_truth.get("reading_id", ""))
            if rid in used_reading_ids:
                continue
            r_dev = str(r_truth.get("device_id", ""))
            r_ts = str(r_truth.get("ts", ""))
            if r_dev == atk.device_id and r_ts:
                try:
                    r_ts_sec = parse_rfc3339_to_seconds(r_ts)
                except Exception:
                    continue
                if r_ts_sec >= atk_ts_sec:
                    matched_reading = r_truth
                    used_reading_ids.add(rid)
                    break

        if matched_reading:
            r_id = matched_reading.get("reading_id")
            d_id = matched_reading.get("device_id")
            t_val = matched_reading.get("true_value", 0.0)
            b_val = matched_reading.get("broadcast_value", 0.0)
            unit = matched_reading.get("unit", "")
            r_ts = matched_reading.get("ts", atk.sim_ts)
            r_zone = extract_zone_from_device(d_id, device_zone_map)

            rows.append(
                TimelineRow(
                    row_id=f"reading:{r_id}",
                    sim_ts=r_ts,
                    lane="Twin",
                    event_type="attacked_reading",
                    device_id=d_id,
                    zone_id=r_zone,
                    summary=f"Attacked reading {r_id}: true {t_val:.2f} {unit} vs broadcast {b_val:.2f} {unit}",
                    raw_json=matched_reading,
                )
            )

    # 3. Process optional partner log entries (TIMELINE_LOG_DIR)
    for l_idx, log_record in enumerate(log_entries):
        layer = log_record.get("layer", "")
        sim_ts = str(log_record.get("sim_ts", ""))
        event = log_record.get("event", "")
        details = log_record.get("details", {})
        dev_id = details.get("device_id")
        zone_id = details.get("zone_id") or extract_zone_from_device(dev_id, device_zone_map)

        if layer == "guardian" and event not in ("mock_started", "mock_stopped"):
            rows.append(
                TimelineRow(
                    row_id=f"log:guardian:{l_idx}",
                    sim_ts=sim_ts,
                    lane="Guardian",
                    event_type="verdict",
                    device_id=dev_id,
                    zone_id=zone_id,
                    summary=f"Guardian verdict: {event} ({details.get('reason') or json.dumps(details)})",
                    raw_json=log_record,
                )
            )
        elif layer == "brain" and event not in ("mock_started", "mock_stopped"):
            rows.append(
                TimelineRow(
                    row_id=f"log:brain:{l_idx}",
                    sim_ts=sim_ts,
                    lane="Brain",
                    event_type="verdict",
                    device_id=dev_id,
                    zone_id=zone_id,
                    summary=f"Brain verdict: {event} ({json.dumps(details)})",
                    raw_json=log_record,
                )
            )

    # 4. Form Chains (Link Guardian actions and Brain commands to attacks or physical events)
    # Origin events are attack_inject and event_start in the Twin lane
    origin_rows = [r for r in rows if r.lane == "Twin" and r.event_type in ("attack_inject", "event_start")]
    
    # Sort all rows by sim_ts first to guarantee temporal order
    rows.sort(key=lambda r: parse_rfc3339_to_seconds(r.sim_ts) if r.sim_ts else 0.0)

    for chain_num, origin in enumerate(origin_rows, start=1):
        chain_id = f"chain-{chain_num}"
        origin.chain_id = chain_id
        origin.reaction_time_s = 0.0

        origin_ts_sec = parse_rfc3339_to_seconds(origin.sim_ts)
        target_dev = origin.device_id
        target_zone = origin.zone_id

        # Also link associated attacked_reading if one exists
        for r in rows:
            if r.event_type == "attacked_reading" and r.device_id == target_dev and r.chain_id is None:
                r_ts_sec = parse_rfc3339_to_seconds(r.sim_ts)
                if origin_ts_sec <= r_ts_sec <= origin_ts_sec + 600.0:
                    r.chain_id = chain_id
                    r.reaction_time_s = r_ts_sec - origin_ts_sec
                    break

        # Search for Guardian action in [origin_ts, origin_ts + 600s]
        linked_guardian: Optional[TimelineRow] = None
        for r in rows:
            if r.lane == "Guardian" and r.chain_id is None:
                r_ts_sec = parse_rfc3339_to_seconds(r.sim_ts)
                if origin_ts_sec <= r_ts_sec <= origin_ts_sec + 600.0:
                    dev_match = target_dev and (r.device_id == target_dev or (r.raw_json and target_dev in str(r.raw_json.get("detail", {}).get("target", ""))))
                    zone_match = target_zone and (r.zone_id == target_zone)
                    if dev_match or zone_match:
                        linked_guardian = r
                        r.chain_id = chain_id
                        r.reaction_time_s = r_ts_sec - origin_ts_sec
                        break

        # Search for Brain command in [origin_ts, origin_ts + 600s]
        linked_brain: Optional[TimelineRow] = None
        linked_actuator: Optional[TimelineRow] = None
        for r in rows:
            if r.lane == "Brain" and r.chain_id is None:
                r_ts_sec = parse_rfc3339_to_seconds(r.sim_ts)
                if origin_ts_sec <= r_ts_sec <= origin_ts_sec + 600.0:
                    dev_match = target_dev and (r.device_id == target_dev)
                    zone_match = target_zone and (r.zone_id == target_zone)
                    if dev_match or zone_match:
                        linked_brain = r
                        r.chain_id = chain_id
                        r.reaction_time_s = r_ts_sec - origin_ts_sec

                        # Find corresponding Actuator row
                        pair_key = r.row_id.replace(":brain", ":actuator")
                        for act_r in rows:
                            if act_r.row_id == pair_key:
                                act_r.chain_id = chain_id
                                act_r.reaction_time_s = r.reaction_time_s
                                linked_actuator = act_r
                                break
                        break

        # Build human-readable chain summary
        # Example: "z2-wl1 spoofed to 9.5 m -> Guardian isolate_device +20 s -> no Brain action needed"
        parts = []
        if origin.event_type == "attack_inject":
            val = origin.raw_json.get("detail", {}).get("params", {}).get("value") if origin.raw_json else None
            val_txt = f" spoofed to {val}" if val is not None else f" {origin.raw_json.get('detail', {}).get('capability', 'attacked')}" if origin.raw_json else ""
            parts.append(f"{target_dev or target_zone}{val_txt}")
        else:
            etype = origin.raw_json.get("detail", {}).get("type", "incident") if origin.raw_json else "incident"
            parts.append(f"{etype} in {target_zone or 'city'}")

        if linked_guardian:
            g_tool = linked_guardian.raw_json.get("detail", {}).get("tool", "containment") if linked_guardian.raw_json else "containment"
            parts.append(f"Guardian {g_tool} {format_reaction_delta(linked_guardian.reaction_time_s or 0.0)}")
        else:
            parts.append("no Guardian action needed")

        if linked_brain:
            b_tool = linked_brain.raw_json.get("detail", {}).get("tool", "command") if linked_brain.raw_json else "command"
            parts.append(f"Brain {b_tool} {format_reaction_delta(linked_brain.reaction_time_s or 0.0)}")
            if linked_actuator:
                is_exec = linked_actuator.raw_json.get("detail", {}).get("executed", True) if linked_actuator.raw_json else True
                ref_code = linked_actuator.raw_json.get("detail", {}).get("refused_code") if linked_actuator.raw_json else None
                act_txt = "executed" if is_exec else f"refused {ref_code}"
                parts.append(f"Actuator {act_txt}")
        else:
            parts.append("no Brain action needed")

        chain_str = " -> ".join(parts)
        origin.chain_summary = chain_str
        if linked_guardian:
            linked_guardian.chain_summary = chain_str
        if linked_brain:
            linked_brain.chain_summary = chain_str
        if linked_actuator:
            linked_actuator.chain_summary = chain_str

    # Final sort: sim_ts in ascending order, preserving original index on ties
    rows.sort(key=lambda r: parse_rfc3339_to_seconds(r.sim_ts) if r.sim_ts else 0.0)
    return rows


def filter_timeline_rows(
    rows: List[TimelineRow],
    window_sim_minutes: Optional[int] = None,
    since_sim_ts: Optional[str] = None,
    zone_id: Optional[str] = None,
    device_id: Optional[str] = None,
    current_sim_ts: Optional[str] = None,
) -> List[TimelineRow]:
    """Filter timeline rows by time window, zone, and device."""
    filtered = rows

    # 1. Filter by time
    if since_sim_ts:
        since_sec = parse_rfc3339_to_seconds(since_sim_ts)
        filtered = [r for r in filtered if r.sim_ts and parse_rfc3339_to_seconds(r.sim_ts) >= since_sec]
    elif window_sim_minutes is not None and window_sim_minutes > 0:
        ref_ts_str = current_sim_ts or (filtered[-1].sim_ts if filtered else None)
        if ref_ts_str:
            ref_sec = parse_rfc3339_to_seconds(ref_ts_str)
            cutoff_sec = ref_sec - (window_sim_minutes * 60.0)
            filtered = [r for r in filtered if r.sim_ts and parse_rfc3339_to_seconds(r.sim_ts) >= cutoff_sec]

    # 2. Filter by zone
    if zone_id and zone_id != "All zones":
        filtered = [r for r in filtered if r.zone_id == zone_id or (r.zone_id is None and zone_id in str(r.summary))]

    # 3. Filter by device
    if device_id and device_id != "All devices":
        filtered = [r for r in filtered if r.device_id == device_id or (r.device_id is None and device_id in str(r.summary))]

    return filtered

