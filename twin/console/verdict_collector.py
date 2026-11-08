"""Verdict collector service for Layer 1 Twin operations console.

Consumes partner verdicts (Guardian or Brain) from a Kafka topic, normalizes them,
resolves their simulation timestamp against the Twin's SQLite ground-truth ledger,
and appends correlated JSON lines to TIMELINE_LOG_DIR/verdicts-<layer>.jsonl.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sqlite3
import sys
import time
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger("twin.verdict_collector")


def get_verdict_config(env: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Parse and validate verdict collector configuration from environment.

    Exits with code 2 on missing required variables (VERDICT_TOPIC, TIMELINE_LOG_DIR)
    or invalid configuration values.
    """
    _env = os.environ if env is None else env

    def _resolve(base_name: str) -> Tuple[Optional[str], str]:
        v_name = f"VERDICT_{base_name}"
        v_val = _env.get(v_name)
        if v_val is not None and v_val.strip() != "":
            return v_val.strip(), v_name
        fb_val = _env.get(base_name)
        if fb_val is not None and fb_val.strip() != "":
            return fb_val.strip(), base_name
        return None, v_name

    # 1. VERDICT_TOPIC (required)
    topic = (_env.get("VERDICT_TOPIC") or "").strip()
    if not topic:
        print("Missing required environment variable: VERDICT_TOPIC", file=sys.stderr)
        sys.exit(2)

    # 2. TIMELINE_LOG_DIR (required output dir)
    log_dir = (_env.get("TIMELINE_LOG_DIR") or "").strip()
    if not log_dir:
        print("Missing required environment variable: TIMELINE_LOG_DIR", file=sys.stderr)
        sys.exit(2)

    # 3. VERDICT_LAYER (guardian default | brain)
    layer = (_env.get("VERDICT_LAYER") or "guardian").strip().lower()
    if layer not in ("guardian", "brain"):
        print(f"Invalid VERDICT_LAYER: '{layer}'. Allowed: guardian, brain", file=sys.stderr)
        sys.exit(2)

    # 4. GROUND_TRUTH_DB_PATH
    db_path = (_env.get("GROUND_TRUTH_DB_PATH") or "ground_truth.db").strip()

    # 5. Kafka connection & security settings
    raw_boot, _ = _resolve("KAFKA_BOOTSTRAP_SERVERS")
    bootstrap = raw_boot or "localhost:19092"

    raw_proto, proto_var = _resolve("KAFKA_SECURITY_PROTOCOL")
    protocol = raw_proto if raw_proto is not None else "PLAINTEXT"
    allowed_protocols = ("PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL")
    if protocol not in allowed_protocols:
        print(f"Invalid {proto_var}: '{protocol}'. Allowed: {', '.join(allowed_protocols)}", file=sys.stderr)
        sys.exit(2)

    group_id = (_env.get("VERDICT_GROUP_ID") or "twin-verdict-collector").strip() or "twin-verdict-collector"
    offset_reset = (_env.get("VERDICT_OFFSET_RESET") or "latest").strip().lower()
    if offset_reset not in ("latest", "earliest"):
        print(f"Invalid VERDICT_OFFSET_RESET: '{offset_reset}'. Allowed: latest, earliest", file=sys.stderr)
        sys.exit(2)

    raw_fam, fam_var = _resolve("KAFKA_BROKER_ADDRESS_FAMILY")
    family = raw_fam.lower() if raw_fam is not None else "any"
    if family not in ("any", "v4", "v6"):
        print(f"Invalid {fam_var}: '{raw_fam}'. Allowed: any, v4, v6", file=sys.stderr)
        sys.exit(2)

    consumer_conf: Dict[str, Any] = {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": True,
    }
    if family != "any":
        consumer_conf["broker.address.family"] = family

    if protocol != "PLAINTEXT":
        consumer_conf["security.protocol"] = protocol

        raw_mech, mech_var = _resolve("KAFKA_SASL_MECHANISM")
        mechanism = raw_mech if raw_mech is not None else "PLAIN"
        allowed_mechanisms = ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
        if mechanism not in allowed_mechanisms:
            print(f"Invalid {mech_var}: '{mechanism}'. Allowed: {', '.join(allowed_mechanisms)}", file=sys.stderr)
            sys.exit(2)
        consumer_conf["sasl.mechanism"] = mechanism

        username, user_var = _resolve("KAFKA_USERNAME")
        if not username:
            print(f"Missing required environment variable for SASL authentication: {user_var}", file=sys.stderr)
            sys.exit(2)
        consumer_conf["sasl.username"] = username

        # Password resolution: file > literal
        pw_file = None
        pw_file_var = ""
        if _env.get("VERDICT_KAFKA_PASSWORD_FILE") is not None and _env.get("VERDICT_KAFKA_PASSWORD_FILE").strip() != "":
            pw_file = _env.get("VERDICT_KAFKA_PASSWORD_FILE").strip()
            pw_file_var = "VERDICT_KAFKA_PASSWORD_FILE"
        elif _env.get("KAFKA_PASSWORD_FILE") is not None and _env.get("KAFKA_PASSWORD_FILE").strip() != "":
            pw_file = _env.get("KAFKA_PASSWORD_FILE").strip()
            pw_file_var = "KAFKA_PASSWORD_FILE"

        if pw_file is not None:
            if not os.path.exists(pw_file):
                print(f"Password file specified by {pw_file_var} does not exist: '{pw_file}'", file=sys.stderr)
                sys.exit(2)
            if not os.path.isfile(pw_file):
                print(f"Password path specified by {pw_file_var} is not a regular file: '{pw_file}'", file=sys.stderr)
                sys.exit(2)
            try:
                with open(pw_file, "r", encoding="utf-8") as f:
                    password = f.read().rstrip("\r\n")
            except Exception as e:
                print(f"Could not read password file {pw_file_var} at '{pw_file}': {type(e).__name__}", file=sys.stderr)
                sys.exit(2)
            if not password.strip():
                print(f"Password file specified by {pw_file_var} is empty: '{pw_file}'", file=sys.stderr)
                sys.exit(2)
            consumer_conf["sasl.password"] = password
        else:
            raw_pw, pw_var = _resolve("KAFKA_PASSWORD")
            if not raw_pw:
                print(
                    f"Missing required environment variable for SASL authentication: "
                    f"VERDICT_KAFKA_PASSWORD_FILE, VERDICT_KAFKA_PASSWORD, KAFKA_PASSWORD_FILE or KAFKA_PASSWORD",
                    file=sys.stderr,
                )
                sys.exit(2)
            consumer_conf["sasl.password"] = raw_pw

        if protocol == "SASL_SSL":
            ca_loc, ca_var = _resolve("KAFKA_SSL_CA_LOCATION")
            if ca_loc:
                if not os.path.exists(ca_loc):
                    print(f"CA file specified by {ca_var} does not exist: '{ca_loc}'", file=sys.stderr)
                    sys.exit(2)
                consumer_conf["ssl.ca.location"] = ca_loc

    return {
        "topic": topic,
        "layer": layer,
        "log_dir": log_dir,
        "db_path": db_path,
        "bootstrap": bootstrap,
        "consumer_conf": consumer_conf,
    }


def normalize_verdict_payload(raw_bytes: bytes) -> Optional[Dict[str, Any]]:
    """Parse and normalize partner verdict payload.

    Returns dict with extracted fields or None if invalid.
    Skip criteria:
    - Payload length > 64 KB (65536 bytes)
    - Non-JSON or non-object payload
    - Missing both device and reading identifiers
    """
    if len(raw_bytes) > 65536:
        return None

    try:
        data = json.loads(raw_bytes.decode("utf-8"))
    except Exception:
        return None

    if not isinstance(data, dict):
        return None

    # Device identifier candidates
    device_id = None
    for k in ("device_id", "deviceId", "device", "target", "target_device_id"):
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            device_id = v.strip()
            break

    # Reading identifier candidates
    reading_id = None
    for k in ("reading_id", "readingId"):
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            reading_id = v.strip()
            break

    if not device_id and not reading_id:
        return None

    # Event / label candidates
    event = "verdict"
    for k in ("label", "verdict", "event", "type", "classification", "action"):
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            event = v.strip()
            break

    # Reason candidates
    reason = None
    for k in ("reason", "details", "message", "explanation"):
        if k in data:
            reason = data[k]
            break

    # Wall timestamp candidates
    wall_ts = None
    for k in ("detected_at", "timestamp", "ts", "time"):
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            wall_ts = v.strip()
            break

    # Raw representation (truncated to 2 KB string if larger)
    raw_serialized = json.dumps(data)
    if len(raw_serialized.encode("utf-8")) > 2048:
        raw_repr: Any = raw_serialized[:2048]
    else:
        raw_repr = data

    return {
        "device_id": device_id,
        "reading_id": reading_id,
        "event": event,
        "reason": reason,
        "wall_ts": wall_ts,
        "raw": raw_repr,
    }


class VerdictCollector:
    """Consumes Kafka topic, correlates simulation timestamps, and appends partner logs."""

    def __init__(
        self,
        config: Dict[str, Any],
        consumer: Optional[Any] = None,
        db_conn: Optional[sqlite3.Connection] = None,
    ):
        self.config = config
        self.topic = config["topic"]
        self.layer = config["layer"]
        self.log_dir = config["log_dir"]
        self.db_path = config["db_path"]
        self.bootstrap = config.get("bootstrap", "localhost:19092")

        os.makedirs(self.log_dir, exist_ok=True)
        self.output_path = os.path.join(self.log_dir, f"verdicts-{self.layer}.jsonl")

        self.consumer = consumer
        self._owned_consumer = False
        self.db_conn = db_conn
        self._owned_db = False

        # Counters
        self.consumed = 0
        self.written = 0
        self.skipped_invalid = 0
        self.unresolved = 0
        self.by_source: Dict[str, int] = {"reading_id": 0, "device_latest": 0}

        self.running = False
        self._out_file = None

    def _ensure_db(self) -> sqlite3.Connection:
        if self.db_conn is None:
            if not os.path.exists(self.db_path):
                raise FileNotFoundError(f"Ground truth DB not found at: {self.db_path}")
            abs_path = os.path.abspath(self.db_path)
            self.db_conn = sqlite3.connect(f"file:{abs_path}?mode=ro", uri=True)
            self._owned_db = True
        return self.db_conn

    def _ensure_consumer(self) -> Any:
        if self.consumer is None:
            import confluent_kafka
            self.consumer = confluent_kafka.Consumer(self.config["consumer_conf"])
            self.consumer.subscribe([self.topic])
            self._owned_consumer = True
        return self.consumer

    def resolve_sim_ts(
        self, reading_id: Optional[str], device_id: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """Resolve simulation timestamp from local SQLite ground-truth ledger.

        Precedence:
        1. reading_id in readings_truth -> (ts, 'reading_id')
        2. latest reading for device_id in readings_truth -> (ts, 'device_latest')
        3. neither -> (None, None)
        """
        conn = self._ensure_db()
        cursor = conn.cursor()

        # 1. reading_id lookup
        if reading_id:
            cursor.execute("SELECT ts FROM readings_truth WHERE reading_id = ? LIMIT 1", (reading_id,))
            row = cursor.fetchone()
            if row and row[0]:
                return str(row[0]), "reading_id"

        # 2. device_id latest lookup
        if device_id:
            cursor.execute(
                "SELECT ts FROM readings_truth WHERE device_id = ? ORDER BY ts DESC LIMIT 1",
                (device_id,),
            )
            row = cursor.fetchone()
            if row and row[0]:
                return str(row[0]), "device_latest"

        return None, None

    def process_message_bytes(self, raw_bytes: bytes) -> bool:
        """Process a single message payload. Returns True if written, False otherwise."""
        self.consumed += 1

        norm = normalize_verdict_payload(raw_bytes)
        if norm is None:
            self.skipped_invalid += 1
            return False

        sim_ts, source = self.resolve_sim_ts(norm["reading_id"], norm["device_id"])
        if not sim_ts or not source:
            self.unresolved += 1
            return False

        self.by_source[source] = self.by_source.get(source, 0) + 1

        entry = {
            "layer": self.layer,
            "sim_ts": sim_ts,
            "event": norm["event"],
            "details": {
                "device_id": norm["device_id"],
                "reading_id": norm["reading_id"],
                "reason": norm["reason"],
                "wall_ts": norm["wall_ts"],
                "sim_ts_source": source,
                "raw": norm["raw"],
            },
        }

        line = json.dumps(entry) + "\n"
        if self._out_file is None:
            self._out_file = open(self.output_path, "a", encoding="utf-8")
        self._out_file.write(line)
        self._out_file.flush()
        self.written += 1
        return True

    def print_stats(self) -> None:
        """Print current operational metrics to stdout."""
        by_src_str = f"{{reading_id: {self.by_source.get('reading_id', 0)}, device_latest: {self.by_source.get('device_latest', 0)}}}"
        print(
            f"[VERDICT COLLECTOR] Stats: consumed={self.consumed}, written={self.written}, "
            f"skipped_invalid={self.skipped_invalid}, unresolved={self.unresolved}, "
            f"by_source={by_src_str}",
            flush=True,
        )

    def run(self, max_seconds: Optional[float] = None) -> None:
        """Run consumer loop until interrupted or deadline expires."""
        self.running = True
        consumer = self._ensure_consumer()
        self._out_file = open(self.output_path, "a", encoding="utf-8")

        start_time = time.monotonic()
        last_stats_time = start_time
        warned_20s = False

        print(
            f"[VERDICT COLLECTOR] Starting verdict collector for layer '{self.layer}' "
            f"on topic '{self.topic}' (logging to {self.output_path})",
            flush=True,
        )

        def _sig_handler(signum, frame):
            print(f"\n[VERDICT COLLECTOR] Received signal {signum}, stopping cleanly...", flush=True)
            self.running = False

        prev_term = signal.signal(signal.SIGTERM, _sig_handler)
        prev_int = signal.signal(signal.SIGINT, _sig_handler)

        try:
            while self.running:
                now = time.monotonic()
                if max_seconds is not None and (now - start_time) >= max_seconds:
                    break

                # 20-second warning if no messages received
                if not warned_20s and (now - start_time) >= 20.0:
                    warned_20s = True
                    if self.consumed == 0:
                        print(
                            f"[VERDICT COLLECTOR] Warning: No message received on topic '{self.topic}' "
                            f"at '{self.bootstrap}' within 20s. Likely causes: wrong topic, "
                            f"invalid credentials, missing Read ACL on topic or group.",
                            file=sys.stderr,
                            flush=True,
                        )

                # Periodic 10-second stats
                if (now - last_stats_time) >= 10.0:
                    self.print_stats()
                    last_stats_time = now

                # Poll Kafka
                msg = None
                try:
                    msg = consumer.poll(0.5)
                except Exception as e:
                    logger.warning(f"Error polling Kafka: {e}")
                    time.sleep(0.5)
                    continue

                if msg is None:
                    continue

                if hasattr(msg, "error") and msg.error():
                    continue

                val = msg.value() if hasattr(msg, "value") else msg
                if isinstance(val, (bytes, bytearray)):
                    self.process_message_bytes(bytes(val))

        finally:
            signal.signal(signal.SIGTERM, prev_term)
            signal.signal(signal.SIGINT, prev_int)
            self.close()

    def close(self) -> None:
        """Close consumer, flush file, and close DB connection."""
        if self._out_file is not None:
            try:
                self._out_file.flush()
                self._out_file.close()
            except Exception:
                pass
            self._out_file = None

        if self._owned_consumer and self.consumer is not None:
            try:
                self.consumer.close()
            except Exception:
                pass
            self.consumer = None

        if self._owned_db and self.db_conn is not None:
            try:
                self.db_conn.close()
            except Exception:
                pass
            self.db_conn = None

        self.print_stats()


def main() -> int:
    config = get_verdict_config()
    max_sec_env = os.getenv("VERDICT_MAX_SECONDS")
    max_seconds = float(max_sec_env) if max_sec_env else None
    collector = VerdictCollector(config)
    collector.run(max_seconds=max_seconds)
    return 0


if __name__ == "__main__":
    sys.exit(main())

