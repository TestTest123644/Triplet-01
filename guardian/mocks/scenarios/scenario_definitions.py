"""Scenario Definitions for Guardian Layer Verification.

Defines 8 comprehensive test scenarios covering all static checks,
machine learning detectors, containment actions, and supervisory actuations.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from guardian_layer.utils.schema import Anomalies, FlushedBatch


@dataclass
class Scenario:
    """Definition of an end-to-end integration scenario."""

    id: str
    title: str
    category: str
    target_device_id: str
    description: str
    expected_anomaly: Anomalies | None
    expected_twin_status: str
    expected_brain_status: str
    batch_generator: Callable[[Any], FlushedBatch]


def _gen_s1(twin: Any) -> FlushedBatch:
    return twin.generate_normal_batch(
        batch_id="s1-normal", sensor_id="z1-wl1", count=100
    )


def _gen_s2(twin: Any) -> FlushedBatch:
    return twin.generate_out_of_range_batch(
        batch_id="s2-oor", sensor_id="z1-wl1", count=100
    )


def _gen_s3(twin: Any) -> FlushedBatch:
    return twin.generate_stale_down_batch(
        batch_id="s3-down", sensor_id="z2-wl2", count=20
    )


def _gen_s4(twin: Any) -> FlushedBatch:
    return twin.generate_spoofed_spatial_batch(
        batch_id="s4-spoof", target_sensor_id="z1-wl1", count=20
    )


def _gen_s5(twin: Any) -> FlushedBatch:
    return twin.generate_jitter_malfunction_batch(
        batch_id="s5-malfunction", sensor_id="z3-tc1", count=100
    )


def _gen_s6(twin: Any) -> FlushedBatch:
    return twin.generate_corrupted_not_sensor_batch(
        batch_id="s6-notsensor", sensor_id="z4-tc1", count=100
    )


ALL_SCENARIOS: list[Scenario] = [
    Scenario(
        id="scenario_1_normal",
        title="Nominal Stream Operation",
        category="BASELINE",
        target_device_id="z1-wl1",
        description="Clean, regular sensor telemetry within normal bounds. "
        + "Verifies normal ML prediction and dual DB persistence.",
        expected_anomaly=None,
        expected_twin_status="ok",
        expected_brain_status="OPERATIONAL_DECISION_EXECUTED",
        batch_generator=_gen_s1,
    ),
    Scenario(
        id="scenario_2_out_of_range",
        title="Physical Range Breach (Static Check)",
        category="STATIC_CHECK",
        target_device_id="z1-wl1",
        description="Water level sensor breaches physical upper limit (15.5m > 12.0m)."
        + "Triggers quarantine and Brain flood valve actuation.",
        expected_anomaly=Anomalies.OUT_OF_RANGE,
        expected_twin_status="quarantined",
        expected_brain_status="FLOOD_RELIEF_ACTUATED",
        batch_generator=_gen_s2,
    ),
    Scenario(
        id="scenario_3_sensor_is_down",
        title="Sensor Silence / Loss of Signal (Static Check)",
        category="STATIC_CHECK",
        target_device_id="z2-wl2",
        description="Sensor telemetry goes silent for >60s. "
        + "Triggers sensor_is_down anomaly, "
        + "device quarantine, and field dispatch.",
        expected_anomaly=Anomalies.SENSOR_IS_DOWN,
        expected_twin_status="quarantined",
        expected_brain_status="MAINTENANCE_DISPATCHED",
        batch_generator=_gen_s3,
    ),
    Scenario(
        id="scenario_4_spoofed_data",
        title="Spatial Cluster Spoofing (Spatial Check)",
        category="SPATIAL_CHECK",
        target_device_id="z1-wl1",
        description="Rogue sensor deviates significantly from peer cluster. "
        + "Triggers spoofed_data anomaly, device isolation, and rollback.",
        expected_anomaly=Anomalies.SPOOFED_DATA,
        expected_twin_status="isolated",
        expected_brain_status="SECURITY_CONTAINMENT_VERIFIED",
        batch_generator=_gen_s4,
    ),
    Scenario(
        id="scenario_5_sensor_malfunction",
        title="Hardware Malfunction & Noise Burst (AI Short-Window)",
        category="AI_INFERENCE",
        target_device_id="z3-tc1",
        description="High-frequency erratic jitter triggers Haar wavelet energy"
        + " subbands. Evaluated by XGBoost AnomalyDetector as malfunction.",
        expected_anomaly=Anomalies.SENSOR_MALFUNCTION,
        expected_twin_status="quarantined",
        expected_brain_status="REPAIR_DISPATCHED",
        batch_generator=_gen_s5,
    ),
    Scenario(
        id="scenario_6_not_sensor_data",
        title="Non-Physical Bitstream Inversion (AI Short-Window)",
        category="AI_INFERENCE",
        target_device_id="z4-tc1",
        description="Corrupted synthetic rectangular frequency waveforms classified "
        + "by XGBoost as not_sensor_data. Triggers isolation.",
        expected_anomaly=Anomalies.NOT_SENSOR_DATA,
        expected_twin_status="isolated",
        expected_brain_status="SECURITY_CONTAINMENT_VERIFIED",
        batch_generator=_gen_s6,
    ),
]
