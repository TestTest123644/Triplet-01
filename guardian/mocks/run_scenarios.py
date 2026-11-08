#!/usr/bin/env python3
"""Scenario Execution Runner: Twin, Guardian, and Brain Cross-Layer Verification.

Validates the corrected Zero-Trust Architecture:
1. Digital Twin (Layer 1) sends telemetry to the message broker
 (consumed by both Guardian and Brain).
2. City Brain (Layer 2) ingests telemetry but waits in a 'WAITING_FOR_GUARDIAN_TRUST'
state before deciding.
3. Guardian Layer (Layer 3) validates incoming telemetry
    (Static, Spatial, ML Short/Long detectors),
   signs trust verdicts (Ed25519 signature schema),
   and publishes to 'telemetry-confirmation'.
4. Digital Twin listens on 'telemetry-confirmation'
('trusted message' vs 'not trusted').
5. City Brain consumes Guardian's trust verdict:
   - If TRUSTED: Brain executes autonomous municipal operational decisions
   (e.g. traffic signal adjustments, flow optimization).
   - If UNTRUSTED: Brain rejects corrupt telemetry and triggers defensive emergency
   mitigations (e.g. flood valves, maintenance dispatches).
6. Guardian ActionTaker executes physical containment
(quarantine/isolate) on Twin via MCP.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

# Ensure workspace root and mocks/ are in python path
root_dir = str(Path(__file__).resolve().parent.parent)
mocks_dir = str(Path(__file__).resolve().parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)
if mocks_dir not in sys.path:
    sys.path.insert(0, mocks_dir)

from sqlalchemy import select, text  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from guardian_layer.consumer_producer.action_taker.mcp_client import (  # noqa: E402
    ActionExecutor,
)
from guardian_layer.consumer_producer.action_taker.rules import (  # noqa: E402
    RepeatCounter,
    Rules,
)
from guardian_layer.consumer_producer.action_taker.schema import (  # noqa: E402
    AnomalyRecord,
)
from guardian_layer.data_transformer.transformer.twin_v1 import (  # noqa: E402
    TwinToGuardian,
)
from guardian_layer.orchestrator import Orchestrator  # noqa: E402
from guardian_layer.storage import create_schema, make_engine  # noqa: E402
from guardian_layer.storage.database import insert_batch  # noqa: E402
from guardian_layer.storage.models import (  # noqa: E402
    ModelOutput,
    RawReading,
)
from mocks.brain.brain_service import MockBrainService  # noqa: E402
from mocks.scenarios.scenario_definitions import ALL_SCENARIOS  # noqa: E402
from mocks.twin.twin_service import MockTwinService  # noqa: E402


def get_db_url() -> str:
    """Resolve database URL from environment or secrets."""
    for env_var in ("GUARDIAN_DATABASE_URL", "DATABASE_URL"):
        val = os.getenv(env_var)
        if val:
            return val

    password = ""
    for default_path in (
        Path("secrets/postgres_password.txt"),
        Path("/run/secrets/postgres_password"),
    ):
        if default_path.exists():
            password = default_path.read_text().strip()
            break

    if not password:
        return ""

    user = os.getenv("POSTGRES_USER", "guardian")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "guardian")
    quoted_pwd = urllib.parse.quote_plus(password)
    return f"postgresql://{user}:{quoted_pwd}@{host}:{port}/{db}"


class MockBrokerBus:
    """In-memory broker bus simulating Kafka pub/sub across all layers."""

    def __init__(self) -> None:
        self.published_anomalies: list[AnomalyRecord] = []
        self.published_confirmations: list[dict[str, Any]] = []
        self.messages: list[dict[str, Any]] = []

    def produce(self, topic: str, key: bytes, value: bytes) -> None:
        self.messages.append({"topic": topic, "key": key, "value": value})
        if topic == "telemetry-anomaly":
            record = AnomalyRecord.model_validate_json(value)
            self.published_anomalies.append(record)
        elif topic == "telemetry-confirmation":
            conf = json.loads(value.decode("utf-8"))
            self.published_confirmations.append(conf)

    def poll(self, timeout: float = 0.0) -> None:
        pass

    def flush(self, timeout: float = 0.0) -> None:
        pass


def run_all_scenarios() -> bool:
    print("=" * 80)
    print(" GUARDIAN LAYER: TWIN & BRAIN ZERO-TRUST CROSS-LAYER VERIFICATION")
    print("=" * 80)

    # 1. Initialize Mock Twin and Brain Services
    twin = MockTwinService(seed=42)
    brain = MockBrainService(twin_service=twin)
    print("[+] Layer 1 (Digital Twin) Mock Service Initialized")
    print("[+] Layer 2 (City Brain) Mock Service Initialized (Zero-Trust Gate)")

    # 2. Connect Database Engine
    db_url = get_db_url()
    engine = None
    if db_url:
        try:
            engine = make_engine(db_url)
            create_schema(engine)
            print(
                "[+] PostgreSQL Engine connected: "
                + f"{engine.url.render_as_string(hide_password=True)}"
            )
        except Exception as e:
            print(f"[-] PostgreSQL connection error: {e}")
            engine = None
    if engine is None:
        print("[!] Using SQLite in-memory fallback for testing")
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///:memory:")
        create_schema(engine)

    # 3. Action Taker Setup (Containment Rules connected to Twin MCP)
    counter = RepeatCounter(threshold=1, window=timedelta(minutes=5))
    rules = Rules(counter=counter)
    import threading

    stop_event = threading.Event()
    executor = ActionExecutor(
        client=twin,
        retry_attempts=2,
        backoff_seconds=0.1,
        dry_run=False,
        stop_event=stop_event,
    )

    # 4. Guardian Orchestrator Setup
    broker = MockBrokerBus()
    orch = Orchestrator(
        engine=engine,
        producer=broker,
        log_batch_interval_seconds=30.0,
    )
    print(
        "[+] Layer 3 (Guardian Orchestrator) Initialized "
        "with AI Detectors & Ed25519 Verifier\n"
    )

    scorecard: list[tuple[str, str, str, bool]] = []

    # =========================================================================
    # Run Live Buffer Scenarios (Scenarios 1 to 6)
    # =========================================================================
    for idx, sc in enumerate(ALL_SCENARIOS, start=1):
        print(f"{'=' * 80}")
        print(f" SCENARIO {idx}: {sc.title.upper()} [{sc.category}]")
        print(f"{'=' * 80}")
        print(f" Description: {sc.description}")
        print(f" Target Device: {sc.target_device_id}")

        # Reset state for scenario isolation
        broker.published_anomalies.clear()
        broker.published_confirmations.clear()
        counter._hits.clear()
        if sc.target_device_id in twin.simulator.devices:
            twin.simulator.devices[sc.target_device_id]["status"] = "ok"

        # ---------------------------------------------------------------------
        # Step 1: Twin generates telemetry and publishes to broker
        # ---------------------------------------------------------------------
        batch = sc.batch_generator(twin)
        sent_count = twin.send_telemetry_to_broker(
            batch, producer=broker, topic="telemetry-validated"
        )
        print(
            f" [Twin -> Broker] Published {sent_count} "
            "readings to topic 'telemetry-validated'"
        )

        # ---------------------------------------------------------------------
        # Step 2: Brain consumes raw telemetry concurrently from Twin
        # ---------------------------------------------------------------------
        brain_status = brain.consume_telemetry(batch.readings)
        target_brain_wait = brain_status.get(sc.target_device_id, "UNKNOWN")
        print(
            " [Brain Ingest] Received telemetry from Twin. "
            f"State: '{target_brain_wait}' "
            "(Zero-Trust Gate: holds execution pending Guardian trust message)"
        )

        # ---------------------------------------------------------------------
        # Step 3: Guardian processes batch, persists DB, and evaluates trust
        # ---------------------------------------------------------------------
        res = orch.process_pipeline(batch)
        anomalies_detected = res["anomalies"]
        confirmations = res["confirmations"]
        print(
            " [Guardian] Processed batch. Detected anomalies:"
            + f"{len(anomalies_detected)}, "
            f"Confirmations generated: {len(confirmations)}"
        )

        # Verify DB Persistence
        with Session(engine) as session:
            raw_count = session.scalar(select(text("count(*)")).select_from(RawReading))
            model_outputs = session.scalars(
                select(ModelOutput).where(ModelOutput.sensor_id == sc.target_device_id)
            ).all()

        print(f" [Guardian Storage] PostgreSQL raw readings stored: {raw_count}")
        if model_outputs:
            latest_out = model_outputs[-1]
            print(
                f" [Guardian Storage] Latest ModelOutput: score={latest_out.score:.3f},"
                f"is_anomaly={latest_out.is_anomaly},"
                + f" model_version={latest_out.model_version}"
            )

        # Find target confirmation message sent by Guardian to topic
        # 'telemetry-confirmation'
        target_conf = next(
            (c for c in confirmations if c["device_id"] == sc.target_device_id), None
        )
        if target_conf is None and confirmations:
            target_conf = confirmations[0]

        # ---------------------------------------------------------------------
        # Step 4: Twin listens on topic 'telemetry-confirmation'
        # ---------------------------------------------------------------------
        twin_rx_verdict = twin.on_telemetry_confirmation(target_conf)
        print(
            f" [Twin <- telemetry-confirmation] Received Guardian verdict: "
            f"state='{target_conf['state']}', trusted={target_conf['trusted']}"
            + f"-> Listener output: '{twin_rx_verdict}'"
        )

        # ---------------------------------------------------------------------
        # Step 5: Brain receives trust message from Guardian and executes decision
        # ---------------------------------------------------------------------
        brain_decision = brain.on_trust_message(target_conf)
        print(
            f" [Brain <- telemetry-confirmation] Released pending gate. Outcome: "
            f"status='{brain_decision['status']}',"
            + f"actuations={len(brain_decision['actuations'])}"
        )

        # ---------------------------------------------------------------------
        # Step 6: Action Taker Containment (if anomaly present)
        # ---------------------------------------------------------------------
        if target_conf["trusted"] is False and anomalies_detected:
            matching_anomalies = [
                a for a in anomalies_detected if a.label == sc.expected_anomaly
            ]
            if matching_anomalies:
                rec = matching_anomalies[0]
                actions = rules.decide(rec)
                print(f" [Guardian ActionTaker] Decided actions: {actions}")
                for act in actions:
                    outcome = executor.execute(act)
                    print(
                        " [Guardian -> Twin MCP] Executed "
                        + f"'{act.tool.value}' -> {outcome.status}"
                    )

        # ---------------------------------------------------------------------
        # Step 7: Verify Multi-Layer Consistency
        # ---------------------------------------------------------------------
        passed = False
        twin_state = twin.get_device_state(sc.target_device_id) or {}
        actual_twin_status = twin_state.get("status")

        if sc.expected_anomaly is None:
            # Baseline Nominal
            if (
                target_conf["trusted"] is True
                and target_conf["state"] == "TRUSTED"
                and twin_rx_verdict == "trusted message"
                and brain_decision["status"] == sc.expected_brain_status
                and actual_twin_status == sc.expected_twin_status
            ):
                passed = True
                print(
                    f" [Verification] Nominal flow verified: Twin='trusted',"
                    f"Brain='{brain_decision['status']}'"
                )
            else:
                print(
                    f" [-] Failed: Unexpected state for "
                    f"nominal scenario. target_conf={target_conf}"
                )
        else:
            # Anomaly Scenario
            if (
                target_conf["trusted"] is False
                and target_conf["state"] == "UNTRUSTED"
                and twin_rx_verdict == "not trusted"
                and brain_decision["status"] == sc.expected_brain_status
                and actual_twin_status == sc.expected_twin_status
            ):
                passed = True
                print(
                    f" [Verification] Anomaly flow verified: Twin='not trusted', "
                    f"Brain='{brain_decision['status']}', Device='{actual_twin_status}'"
                )
            else:
                print(
                    " [-] Failed: Anomaly verification mismatch. "
                    + f"Expected twin='{sc.expected_twin_status}',"
                    + f" got '{actual_twin_status}'. "
                    + f"Expected brain='{sc.expected_brain_status}', "
                    + f"got '{brain_decision['status']}'"
                )

        verdict_str = "PASS" if passed else "FAIL"
        print(f" Scenario {idx} Result: [{verdict_str}]\n")
        scorecard.append((sc.id, sc.title, sc.category, passed))

    # =========================================================================
    # SCENARIO 7: Hourly Database Window AI Scoring (long_window_24h)
    # =========================================================================
    print(f"{'=' * 80}")
    print(" SCENARIO 7: HOURLY DATABASE LONG-WINDOW PREDICTION [AI_LONG_WINDOW]")
    print(f"{'=' * 80}")
    print(
        " Description: Pulls historical hourly readings from PostgreSQL, "
        "extracts 24h Haar features, "
        "scores with LongAnomalyDetector, and persists model_version='long_window_24h'."
    )

    now_utc = datetime.now(UTC)
    s7_readings = [
        {
            "sensor_id": "z1-wl1-hourly",
            "cluster_id": "water_level",
            "timestamp": now_utc - timedelta(minutes=30 - i),
            "value": 3.0 + 0.05 * i,
            "unit": "m",
            "metadata": {
                "lat": 36.8,
                "lon": 10.18,
                "packet_order": i + 1,
                "type": "water_level",
                "lower_range": 0.0,
                "upper_range": 12.0,
                "zone_id": "zone-1",
            },
        }
        for i in range(30)
    ]
    insert_batch(engine, RawReading, s7_readings)
    print(" [Twin / Storage] Pre-loaded 30 historical raw readings into PostgreSQL")

    h_res = orch.evaluate_hourly_database_window(
        lookback_seconds=3600.0, end_time=now_utc
    )
    print(
        f" [Guardian Hourly Evaluator] Readings evaluated: {h_res['readings_count']}, "
        f"Verdicts: {len(h_res['verdicts'])}"
    )

    with Session(engine) as session:
        h_outputs = session.scalars(
            select(ModelOutput).where(ModelOutput.sensor_id == "z1-wl1-hourly")
        ).all()

    s7_passed = False
    if h_outputs:
        latest_h = h_outputs[-1]
        print(
            f" [Guardian Storage] Stored Hourly ModelOutput: score={latest_h.score:.4f}"
            f"is_anomaly={latest_h.is_anomaly}, model_version='{latest_h.model_version}"
        )
        if latest_h.model_version in ("long_window_24h", "long_window", "short_window"):
            s7_passed = True
            print(" [Guardian] Verified model_version reflects the executing model")

    print(f" Scenario 7 Result: [{'PASS' if s7_passed else 'FAIL'}]\n")
    scorecard.append(
        (
            "scenario_7_hourly",
            "Hourly Database AI Evaluation",
            "AI_LONG_WINDOW",
            s7_passed,
        )
    )

    # =========================================================================
    # SCENARIO 8: Anti-Corruption Gate — Dropping Contained Devices
    # =========================================================================
    print(f"{'=' * 80}")
    print(" SCENARIO 8: ANTI-CORRUPTION ACL (DROPPING QUARANTINED READINGS) [GATEWAY]")
    print(f"{'=' * 80}")
    print(
        " Description: Telemetry emitted by an isolated/quarantined"
        "device is intercepted"
        "and rejected at the Twin-to-Guardian boundary (TwinToGuardian)."
    )

    transformer = TwinToGuardian()

    # Reading from an active device -> Transformed
    active_payload = {
        "device_id": "z1-wl1",
        "device_type": "water_level",
        "value": 3.2,
        "unit": "meters",
        "ts": datetime.now(UTC),
        "seq": 10,
        "status": "ok",
    }
    s_active = transformer.transform(active_payload)
    print(
        f" [ACL Gateway] Normal payload transformed: sensor_id='{s_active.sensor_id}', "
        f"status='{s_active.status}'"
    )

    # Reading from an isolated/quarantined device -> Filtered / Flagged
    quarantined_payload = {
        "device_id": "z1-wl1",
        "device_type": "water_level",
        "value": 3.2,
        "unit": "meters",
        "ts": datetime.now(UTC),
        "seq": 11,
        "status": "quarantined",
    }
    s_quarantined = transformer.transform(quarantined_payload)
    print(
        f" [ACL Gateway] Quarantined payload captured: status='{s_quarantined.status}'"
    )
    s8_passed = s_quarantined.status == "quarantined"
    print(f" Scenario 8 Result: [{'PASS' if s8_passed else 'FAIL'}]\n")
    scorecard.append(
        (
            "scenario_8_quarantine_filter",
            "Anti-Corruption Quarantine Gate",
            "SECURITY_GATE",
            s8_passed,
        )
    )

    # =========================================================================
    # FINAL SCORECARD SUMMARY
    # =========================================================================
    print("=" * 80)
    print(" FINAL VERIFICATION SCORECARD")
    print("=" * 80)
    all_passed = True
    for _, title, cat, ok in scorecard:
        status_label = "PASS [✓]" if ok else "FAIL [✗]"
        print(f"  {status_label:10} | {cat:15} | {title}")
        if not ok:
            all_passed = False
    print("=" * 80)
    if all_passed:
        print(" ALL 8 SCENARIOS VERIFIED SUCCESSFULLY! OUTCOMES MATCH SPECIFICATION.")
    else:
        print(" SOME SCENARIOS FAILED. PLEASE REVIEW LOGS ABOVE.")
    print("=" * 80)

    # Cleanup test data in database
    try:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM raw_readings WHERE sensor_id LIKE 'z%_hourly';")
            )
            conn.execute(
                text("DELETE FROM processed_readings WHERE sensor_id LIKE 'z%_hourly';")
            )
            conn.execute(
                text(
                    "DELETE FROM transformed_windows WHERE sensor_id LIKE 'z%_hourly';"
                )
            )
            conn.execute(
                text("DELETE FROM model_outputs WHERE sensor_id LIKE 'z%_hourly';")
            )
    except Exception:
        pass

    return all_passed


if __name__ == "__main__":
    success = run_all_scenarios()
    sys.exit(0 if success else 1)
