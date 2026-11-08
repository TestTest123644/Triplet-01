"""Mock Brain Layer (Layer 2) Service.

Simulates the City Brain supervisory intelligence and autonomous actuation:
1. Consumes raw telemetry concurrently with Guardian directly from Twin.
2. Holds telemetry in a zero-trust pending state awaiting Guardian trust verification.
3. Subscribes to Guardian's signed trust messages on 'telemetry-confirmation':
   - If TRUSTED (trusted=True): Executes normal municipal operational decisions.
   - If UNTRUSTED (trusted=False): Rejects corrupt telemetry and triggers
    defensive actuations.
4. Maintains an active decision registry and audit history.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from guardian_layer.consumer_producer.action_taker.schema import AnomalyRecord
from guardian_layer.utils.schema import FlushedBatch, Sensor


@dataclass
class IncidentReport:
    """Record of a city incident managed by the City Brain."""

    incident_id: str
    device_id: str
    anomaly_label: str
    detected_at: datetime
    zone_id: str
    device_type: str
    actuations: list[dict[str, Any]] = field(default_factory=list)
    status: str = "OPEN"


@dataclass
class OperationalDecision:
    """Record of an autonomous operational decision executed by the Brain."""

    decision_id: str
    device_id: str
    decision_type: str
    trust_score: float
    timestamp: datetime
    zone_id: str
    actuations: list[dict[str, Any]] = field(default_factory=list)
    status: str = "EXECUTED"


class MockBrainService:
    """Mock City Brain (Layer 2) supervisory intelligence."""

    def __init__(self, twin_service: Any):
        self.twin = twin_service
        self.pending_telemetry: dict[str, dict[str, Any]] = {}
        self.incidents: list[IncidentReport] = []
        self.operational_decisions: list[OperationalDecision] = []
        self.actuation_history: list[dict[str, Any]] = []

    # -------------------------------------------------------------------------
    # Telemetry Consumption (Concurrent with Guardian)
    # -------------------------------------------------------------------------

    def consume_telemetry(
        self,
        batch_or_readings: FlushedBatch | list[Sensor] | tuple[Sensor, ...],
    ) -> dict[str, str]:
        """Brain ingests raw sensor telemetry from Twin.

        In accordance with zero-trust architecture, Brain does NOT act immediately.
        It buffers readings in a 'PENDING_GUARDIAN_TRUST' state
         awaiting Guardian validation.
        """
        readings = (
            batch_or_readings.readings
            if isinstance(batch_or_readings, FlushedBatch)
            else list(batch_or_readings)
        )

        buffered_status: dict[str, str] = {}
        for r in readings:
            sensor_id = r.sensor_id
            if sensor_id not in self.pending_telemetry:
                self.pending_telemetry[sensor_id] = {
                    "readings": [],
                    "received_at": datetime.now(UTC),
                    "status": "WAITING_FOR_GUARDIAN_TRUST",
                }
            self.pending_telemetry[sensor_id]["readings"].append(r)
            buffered_status[sensor_id] = "WAITING_FOR_GUARDIAN_TRUST"

        return buffered_status

    # -------------------------------------------------------------------------
    # Guardian Trust Message Listener & Decision Execution
    # -------------------------------------------------------------------------

    def on_trust_message(self, trust_msg: dict[str, Any]) -> dict[str, Any]:
        """Brain consumes signed trust verdict from Guardian
        (topic: telemetry-confirmation).

        Schema:
        {
          "verdict_id": "v-...",
          "reading_id": "r-...",
          "sig_hash": "...",
          "device_id": "...",
          "state": "TRUSTED" | "UNTRUSTED",
          "trusted": bool,
          "trust": 0.97,
          "reasons": [...],
          "issued_at": "...",
          "expires_at": "...",
          "sig": "..."
        }
        """
        device_id = trust_msg.get("device_id", "unknown")
        is_trusted = bool(trust_msg.get("trusted", trust_msg.get("state") == "TRUSTED"))
        trust_score = float(trust_msg.get("trust", 1.0 if is_trusted else 0.0))
        reasons = trust_msg.get("reasons", [])

        # Fetch device info from Twin
        device_state = self.twin.get_device_state(device_id) or {}
        zone_id = device_state.get("zone_id", "zone-1")
        device_type = device_state.get("device_type", "unknown")

        _ = self.pending_telemetry.pop(device_id, None)

        # ---------------------------------------------------------------------
        # Case A: Telemetry is TRUSTED by Guardian
        # ---------------------------------------------------------------------
        if is_trusted:
            decision_id = f"dec-{uuid.uuid4().hex[:8]}"
            actuations: list[dict[str, Any]] = []

            # Execute normal municipal operational optimizations
            if "water" in device_type or "wl" in device_id:
                act = self.twin.set_valve(device_id, "optimal_flow")
                actuations.append(act)
                dec_type = "OPTIMIZE_WATER_FLOW"
            elif "traffic" in device_type or "tc" in device_id:
                act = self.twin.set_signal_plan(device_id, "green_wave")
                actuations.append(act)
                dec_type = "ADAPTIVE_TRAFFIC_SPLIT"
            elif "power" in device_type:
                act = self.twin.switch_grid(device_id, "balanced")
                actuations.append(act)
                dec_type = "BALANCE_POWER_GRID"
            else:
                dec_type = "NOMINAL_MONITORING"

            decision = OperationalDecision(
                decision_id=decision_id,
                device_id=device_id,
                decision_type=dec_type,
                trust_score=trust_score,
                timestamp=datetime.now(UTC),
                zone_id=zone_id,
                actuations=actuations,
                status="OPERATIONAL_DECISION_EXECUTED",
            )
            self.operational_decisions.append(decision)
            self.actuation_history.extend(actuations)

            return {
                "decision_id": decision_id,
                "device_id": device_id,
                "status": "OPERATIONAL_DECISION_EXECUTED",
                "action": dec_type,
                "trust": trust_score,
                "actuations": actuations,
            }

        # ---------------------------------------------------------------------
        # Case B: Telemetry is UNTRUSTED / ANOMALOUS (Defensive Containment)
        # ---------------------------------------------------------------------
        incident_id = f"inc-{uuid.uuid4().hex[:8]}"
        primary_reason = str(reasons[0]) if reasons else "unknown_anomaly"

        report = IncidentReport(
            incident_id=incident_id,
            device_id=device_id,
            anomaly_label=primary_reason,
            detected_at=datetime.now(UTC),
            zone_id=zone_id,
            device_type=device_type,
        )

        # Brain rejects corrupt telemetry: zero-trust protection
        # Now executes defensive containment & emergency actuations:
        if "out_of_range" in primary_reason:
            valve_res = self.twin.set_valve(device_id, "open")
            disp_res = self.twin.dispatch("flood_warning", zone_id)
            report.actuations.extend([valve_res, disp_res])
            report.status = "FLOOD_RELIEF_ACTUATED"

        elif "sensor_is_down" in primary_reason:
            disp_res = self.twin.dispatch("sensor_inspection", zone_id)
            report.actuations.append(disp_res)
            report.status = "MAINTENANCE_DISPATCHED"

        elif any(k in primary_reason for k in ("spoofed", "not_sensor_data", "cyber")):
            disp_res = self.twin.dispatch("cyber_incident", zone_id)
            report.actuations.append(disp_res)
            report.status = "SECURITY_CONTAINMENT_VERIFIED"

        elif "sensor_malfunction" in primary_reason:
            disp_res = self.twin.dispatch("sensor_repair", zone_id)
            report.actuations.append(disp_res)
            report.status = "REPAIR_DISPATCHED"

        else:
            disp_res = self.twin.dispatch("general_incident", zone_id)
            report.actuations.append(disp_res)
            report.status = "DEFENSIVE_CONTAINMENT_ACTUATED"

        self.incidents.append(report)
        self.actuation_history.extend(report.actuations)

        return {
            "incident_id": incident_id,
            "device_id": device_id,
            "status": report.status,
            "reasons": reasons,
            "actuations": report.actuations,
        }

    # -------------------------------------------------------------------------
    # Legacy Direct Anomaly Handler (For backward compatibility)
    # -------------------------------------------------------------------------

    def handle_anomaly(self, anomaly: AnomalyRecord) -> IncidentReport:
        """Process an anomaly alert directly emitted by Guardian Layer."""
        label_str = (
            anomaly.label.value  # pyright: ignore[reportOptionalMemberAccess]
            if hasattr(anomaly.label, "value")
            else str(anomaly.label)
        )
        fake_msg = {
            "device_id": anomaly.device_id,
            "state": "UNTRUSTED",
            "trusted": False,
            "trust": 0.05,
            "reasons": [label_str],
            "reading_id": anomaly.reading_id,
        }
        _ = self.on_trust_message(fake_msg)
        return self.incidents[-1]

    def get_zone_summary(self, zone_id: str) -> dict[str, Any]:
        """Aggregate all active incidents and operational decisions for a zone."""
        zone_incidents = [inc for inc in self.incidents if inc.zone_id == zone_id]
        zone_decisions = [d for d in self.operational_decisions if d.zone_id == zone_id]
        return {
            "zone_id": zone_id,
            "incident_count": len(zone_incidents),
            "decision_count": len(zone_decisions),
            "incidents": zone_incidents,
            "decisions": zone_decisions,
        }
