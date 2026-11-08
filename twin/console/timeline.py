"""Correlated incident timeline processor for Twin Operations Console.

Transforms raw journal entries from the Twin red-team REST API (/api/journal),
attacked readings from the ground truth ledger, and optional partner logs into
a 5-lane timeline with correlated incident chains and reaction times.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
import re
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

    @property
    def target(self) -> str:
        return self.device_id or self.zone_id or "-"

    @property
    def formatted_reaction(self) -> str:
        if self.reaction_time_s is None or (self.reaction_time_s == 0.0 and self.event_type in ("attack_inject", "event_start")):
            return ""
        return f"+{int(round(self.reaction_time_s))} s"

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["target"] = self.target
        d["formatted_reaction"] = self.formatted_reaction
        return d


def parse_rfc3339_to_seconds(ts_str: str) -> float:
    """Convert RFC 3339 timestamp (e.g. 2026-10-17T13:10:26Z) to epoch seconds."""
    s = str(ts_str).strip()
    if not s:
        return 0.0
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def format_reaction_delta(delta_s: float) -> str:
    """Format simulation reaction time delta (e.g. +20 s)."""
    return f"+{int(round(delta_s))} s"


def format_display_val(val: Any) -> str:
    """Format numeric value rounded to 2 decimal places."""
    if val is None:
        return ""
    if isinstance(val, int) and not isinstance(val, bool):
        return str(val)
    try:
        fval = float(val)
        return str(round(fval, 2))
    except (ValueError, TypeError):
        return str(val)


def extract_zone_from_device(device_id: Optional[str], device_zone_map: Dict[str, str]) -> Optional[str]:
    """Resolve zone_id from device_id using lookup map or standard naming prefix."""
    if not device_id:
        return None
    if device_id in device_zone_map:
        return device_zone_map[device_id]
    m = re.match(r"^z([1-6])-", device_id)
    if m:
        return f"zone-{m.group(1)}"
    return None


def load_partner_log_entries(log_dir: Optional[str]) -> List[Dict[str, Any]]:
    """Load JSON-lines partner logs from directory (TIMELINE_LOG_DIR)."""
    if not log_dir or not os.path.isdir(log_dir):
        return []

    entries: List[Dict[str, Any]] = []
    try:
        files = sorted(os.listdir(log_dir))
    except OSError:
        return []

    log_files = [os.path.join(log_dir, f) for f in files if f.endswith(".log") or f.endswith(".jsonl")]

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

    entries.sort(key=lambda x: parse_rfc3339_to_seconds(str(x.get("sim_ts", ""))))
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

    attack_meta_map: Dict[str, Dict[str, Any]] = {}

    # 1. Process Journal Entries
    for idx, entry in enumerate(journal_entries):
        key = entry.get("key", f"entry:{idx}")
        sim_ts = str(entry.get("sim_ts", ""))
        source = entry.get("source", "")
        detail = entry.get("detail", {})

        if source == "attack":
            action = detail.get("action", "")
            aid = detail.get("attack_id", "")
            cap = detail.get("capability", "")
            params = detail.get("params", {})
            target_dev = detail.get("target_device_id")

            # Handle rogue capability device and zone resolution (Reviewer finding 4)
            if not target_dev and cap == "rogue":
                target_dev = params.get("rogue_device_id")
            target_zone = detail.get("target_zone_id") or (params.get("zone_id") if cap == "rogue" else None) or extract_zone_from_device(target_dev, device_zone_map)

            if action == "inject":
                if aid:
                    attack_meta_map[aid] = {
                        "capability": cap,
                        "target_dev": target_dev,
                        "target_zone": target_zone,
                        "params": params,
                    }
                val = params.get("value") if params.get("value") is not None else params.get("spoofed_value")
                disp_val = format_display_val(val) if val is not None else None
                val_txt = f" (spoofed to {disp_val})" if disp_val else ""
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
                if aid in attack_meta_map:
                    if not target_dev:
                        target_dev = attack_meta_map[aid]["target_dev"]
                    if not target_zone:
                        target_zone = attack_meta_map[aid]["target_zone"]
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
                resolved_by = detail.get("resolved_by", "expired")
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

    # 2. Correlate first attacked reading per attack (Finding 1)
    attacks = [r for r in rows if r.event_type == "attack_inject" and r.device_id]
    used_reading_ids: Set[str] = set()

    for atk in attacks:
        atk_ts_sec = parse_rfc3339_to_seconds(atk.sim_ts)
        # Find candidates matching this device, attacked flag, and ts >= injection ts
        candidates = []
        for r_truth in attacked_readings:
            rid = str(r_truth.get("reading_id", ""))
            if rid in used_reading_ids:
                continue
            r_dev = str(r_truth.get("device_id", ""))
            if r_dev != atk.device_id:
                continue
            r_ts = str(r_truth.get("ts", ""))
            r_sec = parse_rfc3339_to_seconds(r_ts)
            if r_sec >= atk_ts_sec:
                # Store tuple for deterministic earliest sorting: (timestamp, id, seq, record)
                r_id_num = r_truth.get("id") or 0
                r_seq = r_truth.get("seq") or 0
                candidates.append((r_sec, r_id_num, r_seq, r_truth))

        if candidates:
            # Sort ascending: earliest reading first (finding 1)
            candidates.sort(key=lambda c: (c[0], c[1], c[2]))
            matched_reading = candidates[0][3]
            r_id = matched_reading.get("reading_id")
            used_reading_ids.add(r_id)

            d_id = matched_reading.get("device_id")
            t_val = float(matched_reading.get("true_value", 0.0))
            b_val = float(matched_reading.get("broadcast_value", 0.0))
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
            raw_reason = details.get("reason")
            reason_str = str(raw_reason).strip() if (raw_reason is not None and str(raw_reason).strip()) else ""
            src_tag = ""
            if details.get("sim_ts_source") == "reading_id":
                src_tag = " (sim time from reading_id)"
            elif details.get("sim_ts_source") == "device_latest":
                src_tag = " (sim time approximate)"
            if reason_str:
                summary = f"Guardian verdict: {event} ({reason_str}){src_tag}"
            else:
                summary = f"Guardian verdict: {event}{src_tag}"
            rows.append(
                TimelineRow(
                    row_id=f"log:guardian:{l_idx}",
                    sim_ts=sim_ts,
                    lane="Guardian",
                    event_type="verdict",
                    device_id=dev_id,
                    zone_id=zone_id,
                    summary=summary,
                    raw_json=log_record,
                )
            )
        elif layer == "brain" and event not in ("mock_started", "mock_stopped"):
            raw_reason = details.get("reason")
            reason_str = str(raw_reason).strip() if (raw_reason is not None and str(raw_reason).strip()) else ""
            src_tag = ""
            if details.get("sim_ts_source") == "reading_id":
                src_tag = " (sim time from reading_id)"
            elif details.get("sim_ts_source") == "device_latest":
                src_tag = " (sim time approximate)"
            if reason_str:
                summary = f"Brain verdict: {event} ({reason_str}){src_tag}"
            else:
                summary = f"Brain verdict: {event}{src_tag}"
            rows.append(
                TimelineRow(
                    row_id=f"log:brain:{l_idx}",
                    sim_ts=sim_ts,
                    lane="Brain",
                    event_type="verdict",
                    device_id=dev_id,
                    zone_id=zone_id,
                    summary=summary,
                    raw_json=log_record,
                )
            )

    # 4. Form Chains (Findings 2, 3, 4)
    # Origins are attack_inject and event_start in the Twin lane
    origin_rows = [r for r in rows if r.lane == "Twin" and r.event_type in ("attack_inject", "event_start")]
    origin_rows.sort(key=lambda r: parse_rfc3339_to_seconds(r.sim_ts) if r.sim_ts else 0.0)

    # Pre-calculate maximum timestamp across all rows for open-ended chains
    max_sim_sec = max((parse_rfc3339_to_seconds(r.sim_ts) for r in rows), default=0.0)

    # 4a. Initialize origins metadata and link direct clearance/resolution rows
    origins_meta: List[Dict[str, Any]] = []
    for chain_num, origin in enumerate(origin_rows, start=1):
        chain_id = f"chain-{chain_num}"
        origin.chain_id = chain_id
        origin.reaction_time_s = 0.0

        origin_ts_sec = parse_rfc3339_to_seconds(origin.sim_ts)
        target_dev = origin.device_id
        target_zone = origin.zone_id

        # Determine end of incident window: attack_clear for attack_id, event_resolved for event_id + 120s (finding 2)
        end_ts_sec = max_sim_sec + 120.0
        if origin.event_type == "attack_inject":
            aid = origin.raw_json.get("detail", {}).get("attack_id") if origin.raw_json else None
            if aid:
                for r in rows:
                    if r.event_type == "attack_clear" and r.raw_json:
                        if r.raw_json.get("detail", {}).get("attack_id") == aid:
                            c_sec = parse_rfc3339_to_seconds(r.sim_ts)
                            end_ts_sec = c_sec + 120.0
                            if r.chain_id is None:
                                r.chain_id = chain_id
                                r.reaction_time_s = max(0.0, c_sec - origin_ts_sec)
                            break
        elif origin.event_type == "event_start":
            eid = origin.raw_json.get("detail", {}).get("event_id") if origin.raw_json else None
            if eid:
                for r in rows:
                    if r.raw_json and r.raw_json.get("detail", {}).get("event_id") == eid:
                        if r.event_type == "event_escalated" and r.chain_id is None:
                            r.chain_id = chain_id
                            r.reaction_time_s = max(0.0, parse_rfc3339_to_seconds(r.sim_ts) - origin_ts_sec)
                        elif r.event_type == "event_resolved":
                            res_sec = parse_rfc3339_to_seconds(r.sim_ts)
                            end_ts_sec = res_sec + 120.0
                            if r.chain_id is None:
                                r.chain_id = chain_id
                                r.reaction_time_s = max(0.0, res_sec - origin_ts_sec)

        origins_meta.append({
            "chain_id": chain_id,
            "origin": origin,
            "origin_ts_sec": origin_ts_sec,
            "end_ts_sec": end_ts_sec,
            "target_dev": target_dev,
            "target_zone": target_zone,
        })

    # 4b. Link first attacked reading associated with each attack (finding 1)
    for meta in origins_meta:
        origin = meta["origin"]
        target_dev = meta["target_dev"]
        if origin.event_type == "attack_inject" and target_dev:
            for r in rows:
                if r.event_type == "attacked_reading" and r.device_id == target_dev and r.chain_id is None:
                    r_sec = parse_rfc3339_to_seconds(r.sim_ts)
                    if meta["origin_ts_sec"] <= r_sec <= meta["end_ts_sec"]:
                        r.chain_id = meta["chain_id"]
                        r.reaction_time_s = max(0.0, r_sec - meta["origin_ts_sec"])
                        break

    # 4c. Link Guardian rows (Reviewer finding 3):
    # Rule: a Guardian or Brain row belongs to the matching origin with the LATEST start time at or before the row whose window still contains it.
    for r in rows:
        if r.lane == "Guardian" and r.chain_id is None:
            r_sec = parse_rfc3339_to_seconds(r.sim_ts)
            g_target = str(r.raw_json.get("detail", {}).get("target", "")) if r.raw_json else ""

            candidate_origins = []
            for meta in origins_meta:
                t_dev = meta["target_dev"]
                t_zone = meta["target_zone"]
                dev_match = bool(t_dev and (r.device_id == t_dev or g_target == t_dev or g_target.startswith(f"r-{t_dev}-")))
                zone_match = bool(t_zone and (r.zone_id == t_zone or (not t_dev and t_zone in g_target)))
                if (dev_match or zone_match) and (meta["origin_ts_sec"] <= r_sec <= meta["end_ts_sec"]):
                    candidate_origins.append(meta)

            if candidate_origins:
                best_origin = max(candidate_origins, key=lambda m: m["origin_ts_sec"])
                r.chain_id = best_origin["chain_id"]
                r.reaction_time_s = max(0.0, r_sec - best_origin["origin_ts_sec"])

    # 4d. Link Brain rows and their Actuator pairs (Reviewer finding 3):
    for r in rows:
        if r.lane == "Brain" and r.chain_id is None:
            r_sec = parse_rfc3339_to_seconds(r.sim_ts)

            candidate_origins = []
            for meta in origins_meta:
                t_dev = meta["target_dev"]
                t_zone = meta["target_zone"]
                dev_match = bool(t_dev and r.device_id == t_dev)
                zone_match = bool(t_zone and r.zone_id == t_zone)
                if (dev_match or zone_match) and (meta["origin_ts_sec"] <= r_sec <= meta["end_ts_sec"]):
                    candidate_origins.append(meta)

            if candidate_origins:
                best_origin = max(candidate_origins, key=lambda m: m["origin_ts_sec"])
                r.chain_id = best_origin["chain_id"]
                r.reaction_time_s = max(0.0, r_sec - best_origin["origin_ts_sec"])

                # Pair Actuator row follows its Brain row
                pair_key = r.row_id.replace(":brain", ":actuator")
                for act_r in rows:
                    if act_r.row_id == pair_key and act_r.chain_id is None:
                        act_r.chain_id = best_origin["chain_id"]
                        act_r.reaction_time_s = r.reaction_time_s
                        break

    # 4e. Build formatted chain summary strings for all origins (Reviewer finding 4)
    for meta in origins_meta:
        chain_id = meta["chain_id"]
        origin = meta["origin"]
        target_dev = meta["target_dev"]
        target_zone = meta["target_zone"]

        if origin.event_type == "attack_inject":
            params = origin.raw_json.get("detail", {}).get("params", {}) if origin.raw_json else {}
            val = params.get("value") if params.get("value") is not None else params.get("spoofed_value")
            disp_val = format_display_val(val) if val is not None else None
            cap = origin.raw_json.get("detail", {}).get("capability", "attack") if origin.raw_json else "attack"
            spoofed_txt = f" spoofed to {disp_val}" if disp_val else ""
            dev_or_zone = target_dev or target_zone
            origin_txt = f"{dev_or_zone} {cap}{spoofed_txt}"
        else:
            etype = origin.raw_json.get("detail", {}).get("type", "incident") if origin.raw_json else "incident"
            origin_txt = f"{etype} in {target_zone or 'city'}"

        g_chain_rows = [r for r in rows if r.chain_id == chain_id and r.lane == "Guardian"]
        if g_chain_rows:
            g_chain_rows.sort(key=lambda x: parse_rfc3339_to_seconds(x.sim_ts))
            first_g = g_chain_rows[0]
            g_tool = first_g.raw_json.get("detail", {}).get("tool", first_g.event_type) if first_g.raw_json else first_g.event_type
            g_delta = format_reaction_delta(first_g.reaction_time_s or 0.0)
            g_more = f" (+{len(g_chain_rows) - 1} more)" if len(g_chain_rows) > 1 else ""
            guardian_txt = f"Guardian {g_tool} {g_delta}{g_more}"
        else:
            guardian_txt = "no Guardian action"

        b_chain_rows = [r for r in rows if r.chain_id == chain_id and r.lane == "Brain"]
        if b_chain_rows:
            b_chain_rows.sort(key=lambda x: parse_rfc3339_to_seconds(x.sim_ts))
            first_b = b_chain_rows[0]
            b_tool = first_b.raw_json.get("detail", {}).get("tool", first_b.event_type) if first_b.raw_json else first_b.event_type
            b_delta = format_reaction_delta(first_b.reaction_time_s or 0.0)
            b_more = f" (+{len(b_chain_rows) - 1} more)" if len(b_chain_rows) > 1 else ""
            brain_txt = f"Brain {b_tool} {b_delta}{b_more}"

            # Actuator result follows Brain commands
            a_chain_rows = [r for r in rows if r.chain_id == chain_id and r.lane == "Actuator"]
            any_refused = any(not r.raw_json.get("detail", {}).get("executed", True) for r in a_chain_rows if r.raw_json)
            if any_refused:
                first_ref = next(r for r in a_chain_rows if not r.raw_json.get("detail", {}).get("executed", True))
                ref_code = first_ref.raw_json.get("detail", {}).get("refused_code") if first_ref.raw_json else None
                actuator_txt = f"Actuator refused {ref_code}" if ref_code else "Actuator refused"
            else:
                actuator_txt = "Actuator executed"
        else:
            brain_txt = "no Brain action"
            actuator_txt = None

        # Check for event escalation in this chain
        esc_rows = [r for r in rows if r.chain_id == chain_id and r.event_type == "event_escalated"]
        if esc_rows:
            esc_rows.sort(key=lambda x: parse_rfc3339_to_seconds(x.sim_ts))
            first_esc = esc_rows[0]
            to_val = first_esc.raw_json.get("detail", {}).get("to", "escalated") if first_esc.raw_json else "escalated"
            escalated_txt = f"escalated to {to_val}"
        else:
            escalated_txt = None

        # Check for event resolution in this chain
        res_rows = [r for r in rows if r.chain_id == chain_id and r.event_type == "event_resolved"]
        if res_rows:
            res_rows.sort(key=lambda x: parse_rfc3339_to_seconds(x.sim_ts))
            first_res = res_rows[0]
            resolved_by = first_res.raw_json.get("detail", {}).get("resolved_by", "expired") if first_res.raw_json else "expired"
            resolved_txt = f"resolved by {resolved_by}"
        else:
            resolved_txt = None

        chain_parts = [origin_txt, guardian_txt, brain_txt]
        if actuator_txt:
            chain_parts.append(actuator_txt)
        if escalated_txt:
            chain_parts.append(escalated_txt)
        if resolved_txt:
            chain_parts.append(resolved_txt)
        chain_summary_str = " -> ".join(chain_parts)

        # Distribute chain_summary string to all rows in this chain
        for r in rows:
            if r.chain_id == chain_id:
                r.chain_summary = chain_summary_str

    # Deterministic sorting:
    # 1. sim_ts in ascending order
    # 2. Within identical sim_ts, order lanes: Twin attack_inject -> attacked_reading -> Guardian -> Brain -> Actuator -> others
    lane_rank = {
        ("Twin", "attack_inject"): 0,
        ("Twin", "event_start"): 0,
        ("Twin", "attacked_reading"): 1,
        ("Guardian", "containment"): 2,
        ("Guardian", "verdict"): 2,
        ("Brain", "command"): 3,
        ("Brain", "verdict"): 3,
        ("Actuator", "actuation_result"): 4,
        ("Twin", "attack_clear"): 5,
        ("Twin", "event_escalated"): 5,
        ("Twin", "event_resolved"): 5,
        ("Test harness", "command"): 6,
        ("Test harness", "containment"): 6,
    }

    rows.sort(
        key=lambda r: (
            parse_rfc3339_to_seconds(r.sim_ts) if r.sim_ts else 0.0,
            lane_rank.get((r.lane, r.event_type), 9),
        )
    )
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
