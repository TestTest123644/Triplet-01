"""Common scenario runner utilities for Layer 1 Digital Twin.

Strict architectural constraints:
- Zero imports from twin.* internal packages.
- Red Team API communication over HTTP /api endpoints.
- Read-only checks via Model Context Protocol (MCP) tool queries.
- Wire checks via confluent-kafka or injected QueueSource.
- Merged JSON-lines logging with final result line.
- Always clear injected events and shut down subprocesses on exit.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
from datetime import datetime, timezone
import json
import logging
import os
import signal
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import urllib.error
import urllib.parse
import urllib.request

from cryptography.hazmat.primitives.asymmetric import ed25519

logger = logging.getLogger("twin_scenarios")


# ==============================================================================
# 1. Cryptographic Wire Verification (Self-Contained)
# ==============================================================================

def verify_payload_signature(payload: Dict[str, Any], pub_key_b64: str) -> bool:
    """Verify Ed25519 signature of wire payload against canonical JSON bytes."""
    sig_b64 = payload.get("sig")
    if not sig_b64 or not pub_key_b64:
        return False
    clean = {k: v for k, v in payload.items() if k != "sig"}
    canonical_bytes = json.dumps(clean, sort_keys=True, separators=(",", ":")).encode("utf-8")
    try:
        pub_bytes = base64.b64decode(pub_key_b64)
        public_key = ed25519.Ed25519PublicKey.from_public_bytes(pub_bytes)
        public_key.verify(base64.b64decode(sig_b64), canonical_bytes)
        return True
    except Exception:
        return False


def parse_sim_timestamp(ts_str: str) -> float:
    """Parse ISO8601 or sim timestamp to float seconds."""
    if not ts_str:
        return 0.0
    try:
        s = ts_str.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return 0.0


# ==============================================================================
# 2. HTTP Red-Team REST Client
# ==============================================================================

class TwinRestClient:
    """HTTP client for Twin Red-Team API and Journal."""

    def __init__(self, base_url: str = "http://localhost:8100", token: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.token = token or os.getenv("RED_TEAM_TOKEN")

    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.token:
            h["X-Red-Team-Token"] = self.token
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(url, data=data, headers=self._headers(), method=method)
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8")
            try:
                err_data = json.loads(raw)
                return {"status_code": e.code, "error": err_data}
            except Exception:
                return {"status_code": e.code, "error": raw}
        except Exception as e:
            return {"status_code": 500, "error": str(e)}

    def get_status(self) -> Dict[str, Any]:
        return self._request("GET", "/api/status")

    def get_journal(self, since_ts: Optional[str] = None, limit: int = 1000) -> List[Dict[str, Any]]:
        query = f"?limit={limit}"
        if since_ts:
            query += f"&since_sim_ts={urllib.parse.quote(since_ts)}"
        res = self._request("GET", f"/api/journal{query}")
        if isinstance(res, dict) and "journal" in res:
            return res["journal"]
        return []

    def inject_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        if "capability" in event_data:
            return self._request("POST", "/api/attack/inject", event_data)
        return self._request("POST", "/api/event/inject", event_data)

    def inject_attack(self, attack_data: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/api/attack/inject", attack_data)

    def clear_event(self, event_id: str = "all") -> Dict[str, Any]:
        res_ev = self._request("POST", "/api/event/clear", {"event_id": event_id})
        res_atk = self._request("POST", "/api/attack/clear", {"attack_id": event_id})
        return {"event": res_ev, "attack": res_atk}

    def clear_attack(self, attack_id: str = "all") -> Dict[str, Any]:
        return self._request("POST", "/api/attack/clear", {"attack_id": attack_id})


# ==============================================================================
# 3. Model Context Protocol (MCP) Client
# ==============================================================================

class TwinMcpClient:
    """Official MCP client over SSE transport for read-only queries."""

    def __init__(self, base_url: str = "http://localhost:8000", token: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.token = token or os.getenv("MCP_AUTH_TOKEN")
        self._key_cache: Dict[str, Optional[str]] = {}

    async def call_tool_async(self, tool_name: str, args: Dict[str, Any]) -> Any:
        from mcp.client.session import ClientSession
        from mcp.client.sse import sse_client

        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        async with sse_client(f"{self.base_url}/sse", headers=headers) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                res = await session.call_tool(tool_name, arguments=args)
                if hasattr(res, "content") and res.content:
                    if len(res.content) == 1:
                        first = res.content[0]
                        text = getattr(first, "text", "")
                        try:
                            return json.loads(text)
                        except Exception:
                            return text
                    else:
                        items = []
                        for c in res.content:
                            t = getattr(c, "text", "")
                            try:
                                items.append(json.loads(t))
                            except Exception:
                                items.append(t)
                        return items
                return res

    def call_tool(self, tool_name: str, args: Dict[str, Any]) -> Any:
        """Synchronously execute an MCP tool."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is None:
            return asyncio.run(self.call_tool_async(tool_name, args))
        else:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, self.call_tool_async(tool_name, args)).result()

    def get_device_key(self, device_id: str) -> Optional[str]:
        if device_id in self._key_cache:
            return self._key_cache[device_id]
        res = self.call_tool("get_device_key", {"device_id": device_id})
        key_val = None
        if isinstance(res, dict) and "public_key" in res:
            key_val = res.get("public_key")
        elif isinstance(res, str):
            key_val = res
        self._key_cache[device_id] = key_val
        return key_val

    def list_devices(self, zone_id: Optional[str] = None) -> List[Dict[str, Any]]:
        args = {"zone_id": zone_id} if zone_id else {}
        res = self.call_tool("list_devices", args)
        return res if isinstance(res, list) else []

    def get_actuator_state(self, device_id: str) -> Dict[str, Any]:
        res = self.call_tool("get_device_state", {"device_id": device_id})
        return res if isinstance(res, dict) else {}

    def get_device_state(self, device_id: str) -> Dict[str, Any]:
        res = self.call_tool("get_device_state", {"device_id": device_id})
        return res if isinstance(res, dict) else {}


# ==============================================================================
# 4. Wire Telemetry Consumer
# ==============================================================================

class WireConsumer:
    """Consumes wire frames from Kafka topic or an in-memory queue."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:19092",
        topic: str = "twin.sensors",
        queue_source: Optional[Any] = None,
    ):
        self.queue_source = queue_source
        self.consumer = None
        if not self.queue_source:
            try:
                import confluent_kafka
                self.consumer = confluent_kafka.Consumer({
                    "bootstrap.servers": bootstrap_servers,
                    "group.id": f"scenario-runner-{int(time.time()*1000)}",
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": False,
                })
                self.consumer.subscribe([topic])
            except Exception as e:
                logger.warning(f"Kafka consumer could not be created: {e}")
                self.consumer = None

    def poll(self, timeout: float = 1.0) -> Optional[Tuple[bytes, Dict[str, Any]]]:
        """Returns (raw_bytes, payload_dict) or None."""
        if self.queue_source:
            raw = self.queue_source.get_reading(timeout=timeout)
            if raw is None:
                return None
            if isinstance(raw, dict):
                raw_bytes = json.dumps(raw).encode("utf-8")
                return raw_bytes, raw
            elif isinstance(raw, bytes):
                try:
                    return raw, json.loads(raw.decode("utf-8"))
                except Exception:
                    return raw, {"_corrupted": True}

        if self.consumer is not None:
            msg = self.consumer.poll(timeout)
            if msg is None or msg.error():
                return None
            val_bytes = msg.value()
            if not val_bytes:
                return None
            try:
                payload = json.loads(val_bytes.decode("utf-8"))
                return val_bytes, payload
            except Exception:
                return val_bytes, {"_corrupted": True}
        return None

    def drain(self, max_seconds: float = 2.0, max_items: Optional[int] = None) -> List[Tuple[bytes, Dict[str, Any]]]:
        """Drain available frames until consumer is empty or deadline expires."""
        res = []
        start = time.time()
        # Initial poll to wait for next message if consumer is briefly quiet
        item = self.poll(timeout=0.05)
        if not item:
            return res
        res.append(item)
        while True:
            if max_items is not None and len(res) >= max_items:
                break
            if time.time() - start >= max_seconds:
                break
            item = self.poll(timeout=0.001)
            if item is None:
                break
            res.append(item)
        return res

    def close(self) -> None:
        if self.consumer is not None:
            try:
                self.consumer.close()
            except Exception:
                pass


# ==============================================================================
# 5. Scenario Execution Context
# ==============================================================================

def resolve_tokens(
    mcp_token: Optional[str] = None,
    brain_token: Optional[str] = None,
    guardian_token: Optional[str] = None,
    scenario_token: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Resolve distinct bearer tokens for brain, guardian, and scenario."""
    env_tokens: Dict[str, str] = {}
    raw = os.getenv("MCP_AUTH_TOKENS", "").strip()
    if raw:
        for part in raw.split(","):
            part = part.strip()
            if ":" in part:
                principal, token = part.split(":", 1)
                env_tokens[principal.strip().lower()] = token.strip()

    b_tok = brain_token or os.getenv("MCP_BRAIN_TOKEN") or env_tokens.get("brain") or mcp_token
    g_tok = guardian_token or os.getenv("MCP_GUARDIAN_TOKEN") or env_tokens.get("guardian") or mcp_token
    s_tok = scenario_token or os.getenv("MCP_SCENARIO_TOKEN") or env_tokens.get("scenario") or mcp_token
    return b_tok, g_tok, s_tok


class ScenarioContext:
    """Manages scenario lifecycle, live mock subprocesses, checks, and logs."""

    def __init__(
        self,
        scenario_id: str,
        name: str,
        twin_host: str = "http://localhost:8100",
        mcp_url: str = "http://localhost:8000",
        red_team_token: Optional[str] = None,
        mcp_token: Optional[str] = None,
        brain_token: Optional[str] = None,
        guardian_token: Optional[str] = None,
        scenario_token: Optional[str] = None,
        kafka_bootstrap: str = "localhost:19092",
        kafka_topic: str = "twin.sensors",
        with_mocks: bool = False,
        partners: bool = False,
        no_brain: bool = False,
        log_dir: str = "logs",
        queue_source: Optional[Any] = None,
    ):
        self.scenario_id = scenario_id
        self.name = name
        self.twin_host = twin_host
        self.mcp_url = mcp_url
        self.red_team_token = red_team_token
        self.mcp_token = mcp_token
        self.kafka_bootstrap = kafka_bootstrap
        self.kafka_topic = kafka_topic
        self.with_mocks = with_mocks
        self.partners = partners
        self.no_brain = no_brain
        self.log_dir = log_dir
        self.queue_source = queue_source

        b_tok, g_tok, s_tok = resolve_tokens(
            mcp_token=mcp_token,
            brain_token=brain_token,
            guardian_token=guardian_token,
            scenario_token=scenario_token,
        )
        self.brain_token = b_tok
        self.guardian_token = g_tok
        self.scenario_token = s_tok

        self.rest = TwinRestClient(base_url=twin_host, token=red_team_token)
        self.mcp = TwinMcpClient(base_url=mcp_url, token=s_tok)
        self.wire = WireConsumer(
            bootstrap_servers=kafka_bootstrap,
            topic=kafka_topic,
            queue_source=queue_source,
        )

        self.start_wall_time = datetime.now(timezone.utc)
        self.utc_stamp = self.start_wall_time.strftime("%Y%m%dT%H%M%SZ")
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, f"{self.scenario_id}-{self.utc_stamp}.log")

        self.checks: List[Dict[str, Any]] = []
        self.injected_ids: List[str] = []
        self.sim_speed = 1.0
        self.start_sim_time: str = ""

        # Subprocesses
        self._guardian_proc: Optional[subprocess.Popen] = None
        self._brain_proc: Optional[subprocess.Popen] = None
        self._guardian_log: Optional[str] = None
        self._brain_log: Optional[str] = None

        self._finished = False

    @classmethod
    def from_args(cls, args: argparse.Namespace, scenario_id: str, name: str) -> "ScenarioContext":
        return cls(
            scenario_id=scenario_id,
            name=name,
            twin_host=getattr(args, "twin_host", "http://localhost:8100"),
            mcp_url=getattr(args, "mcp_url", "http://localhost:8000"),
            red_team_token=getattr(args, "red_team_token", None),
            mcp_token=getattr(args, "mcp_token", None),
            brain_token=getattr(args, "brain_token", None),
            guardian_token=getattr(args, "guardian_token", None),
            scenario_token=getattr(args, "scenario_token", None),
            kafka_bootstrap=getattr(args, "kafka_bootstrap", "localhost:19092"),
            kafka_topic=getattr(args, "kafka_topic", "twin.sensors"),
            with_mocks=getattr(args, "with_mocks", False),
            partners=getattr(args, "partners", False),
            no_brain=getattr(args, "no_brain", False),
            log_dir=getattr(args, "log_dir", "logs"),
        )

    def get_sim_ts_now(self) -> str:
        """Fetch current sim_ts from red team status if available."""
        try:
            st = self.rest.get_status()
            return str(st.get("sim_time", ""))
        except Exception:
            return ""

    def get_sim_time_seconds(self) -> float:
        """Fetch current sim_time from /api/status as float seconds."""
        ts_str = self.get_sim_ts_now()
        if ts_str:
            parsed = parse_sim_timestamp(ts_str)
            if parsed > 0.0:
                return parsed
        return time.time()

    def log_event(self, sim_ts: str, layer: str, event: str, details: Dict[str, Any]) -> None:
        """Write single JSON line event to scenario log."""
        if not sim_ts:
            sim_ts = self.get_sim_ts_now()
        entry = {
            "wall_ts": datetime.now(timezone.utc).isoformat(),
            "sim_ts": sim_ts,
            "layer": layer,
            "event": event,
            "details": details,
        }
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

    def record_check(self, name: str, passed: bool, detail: Any) -> None:
        """Record check result."""
        chk = {"name": name, "pass": bool(passed), "detail": detail}
        self.checks.append(chk)
        self.log_event("", "scenario", "check", chk)
        status_str = "PASS" if passed else "FAIL"
        print(f"[{self.scenario_id}] Check {name}: {status_str} ({detail})", flush=True)

    def sim_sleep(self, sim_seconds: float) -> None:
        """Convert sim-seconds to wall time based on twin sim_speed and sleep."""
        speed = max(0.1, self.sim_speed)
        wall_seconds = sim_seconds / speed
        time.sleep(wall_seconds)

    def __enter__(self) -> "ScenarioContext":
        print(f"=== Starting Scenario {self.scenario_id}: {self.name} ===", flush=True)
        # Ensure clean state between sequential scenarios
        if not self.partners:
            try:
                self.rest.clear_event("all")
                self.rest.clear_attack("all")
                dev_list = self.mcp.list_devices()
                for d in dev_list:
                    did = d.get("device_id")
                    if did and d.get("status") in ("isolated", "quarantined"):
                        self.mcp.call_tool("reinstate_device", {"device_id": did})
                    act = d.get("actuator_state", {})
                    dtype = d.get("device_type")
                    if dtype == "water_level" and act.get("valve_position") != "half":
                        self.mcp.call_tool("set_valve", {"device_id": did, "target_state": "half"})
                    elif dtype == "power_load" and act.get("grid_state") != "grid_a":
                        self.mcp.call_tool("switch_grid", {"device_id": did, "target_state": "grid_a"})
                    elif dtype == "traffic_camera" and act.get("signal_plan") != "normal":
                        self.mcp.call_tool("set_signal_plan", {"device_id": did, "target_state": "normal"})
            except Exception as e:
                logger.debug(f"Pre-scenario reset notice: {e}")

        # 1. Fetch initial status to get sim_speed and sim_time
        st = self.rest.get_status()
        self.sim_speed = float(st.get("sim_speed", 1.0))
        sim_time = st.get("sim_time", "")
        self.start_sim_time = sim_time
        self.log_event(sim_time, "scenario", "scenario_start", {
            "scenario": self.name,
            "sim_speed": self.sim_speed,
            "with_mocks": self.with_mocks,
            "partners": self.partners,
            "no_brain": self.no_brain,
        })

        # 2. Start mock subprocesses if requested
        if self.with_mocks:
            self._start_mocks()

        return self

    def _start_mocks(self) -> None:
        env = os.environ.copy()
        pythonpath = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f".:twin:twin/src:{pythonpath}".strip(":")

        self._guardian_log = os.path.join(self.log_dir, f"{self.scenario_id}-guardian.tmp.log")
        self._brain_log = os.path.join(self.log_dir, f"{self.scenario_id}-brain.tmp.log")

        # Start Guardian mock
        g_cmd = [
            sys.executable,
            "-m", "mocks.mock_guardian",
            "--live",
            "--exit-with-parent",
            "--mcp-url", self.mcp_url,
            "--kafka-bootstrap", self.kafka_bootstrap,
            "--log-file", self._guardian_log,
        ]
        if self.guardian_token:
            g_cmd.extend(["--mcp-token", self.guardian_token])

        self._guardian_proc = subprocess.Popen(g_cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.log_event("", "scenario", "mock_started", {"component": "guardian", "pid": self._guardian_proc.pid})

        # Start Brain mock (unless --no-brain is requested)
        if not self.no_brain:
            b_cmd = [
                sys.executable,
                "-m", "mocks.mock_brain",
                "--live",
                "--exit-with-parent",
                "--mcp-url", self.mcp_url,
                "--kafka-bootstrap", self.kafka_bootstrap,
                "--log-file", self._brain_log,
            ]
            if self.brain_token:
                b_cmd.extend(["--mcp-token", self.brain_token])

            self._brain_proc = subprocess.Popen(b_cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.log_event("", "scenario", "mock_started", {"component": "brain", "pid": self._brain_proc.pid})

        # Register termination handlers to clean up mock subprocesses on exit or interrupt
        import atexit
        atexit.register(self._stop_mocks_processes)

        def _sig_handler(signum, frame):
            self._stop_mocks_processes()
            if signum == signal.SIGINT:
                raise KeyboardInterrupt
            sys.exit(128 + signum)

        for s in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(s, _sig_handler)
            except (ValueError, AttributeError):
                pass

        # Allow mocks 2 seconds to connect and establish baseline stream
        time.sleep(2.0)

    def _stop_mocks_processes(self) -> None:
        for name, proc, log_path in (
            ("guardian", self._guardian_proc, self._guardian_log),
            ("brain", self._brain_proc, self._brain_log),
        ):
            if proc is not None and proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=3.0)
                except Exception:
                    try:
                        proc.kill()
                        proc.wait(timeout=1.0)
                    except Exception:
                        pass
                self.log_event("", "scenario", "mock_stopped", {"component": name})
        self._guardian_proc = None
        self._brain_proc = None

    def _stop_mocks(self) -> None:
        self._stop_mocks_processes()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if not self._finished:
                self.finish()
        finally:
            self._stop_mocks_processes()

    def wait_for_journal(
        self,
        predicate: Callable[[Dict[str, Any]], bool],
        timeout: float = 30.0,
        timeout_sim_seconds: Optional[float] = None,
        poll_interval: float = 0.5,
    ) -> Optional[Dict[str, Any]]:
        """Poll /api/journal until predicate matches an entry or deadline expires."""
        if timeout_sim_seconds is not None:
            start_sim = self.get_sim_time_seconds()
            deadline_sim = start_sim + timeout_sim_seconds
            wall_safety = time.time() + (timeout_sim_seconds / max(0.1, self.sim_speed)) + 30.0
            def is_expired() -> bool:
                return self.get_sim_time_seconds() >= deadline_sim or time.time() >= wall_safety
        else:
            deadline = time.time() + timeout
            def is_expired() -> bool:
                return time.time() >= deadline

        while not is_expired():
            journal = self.rest.get_journal(since_ts=self.start_sim_time, limit=1000)
            for entry in journal:
                if self.start_sim_time and entry.get("sim_ts") and entry.get("sim_ts") < self.start_sim_time:
                    continue
                if predicate(entry):
                    return entry
            time.sleep(poll_interval)
        return None

    def verify_false_positive_guard(
        self,
        allowed_devices: Set[str],
        allowed_zones: Set[str],
    ) -> Tuple[bool, str]:
        """Verify no containment on other devices and no commands in other zones."""
        journal = self.rest.get_journal(since_ts=self.start_sim_time, limit=1000)
        violating_containments = []
        violating_commands = []

        for e in journal:
            if self.start_sim_time and e.get("sim_ts") and e.get("sim_ts") < self.start_sim_time:
                continue
            src = e.get("source")
            dt = e.get("detail", {})
            if src == "containment":
                dev = dt.get("device_id") or dt.get("target") or dt.get("target_device_id")
                # Strip reading prefix if rollback target
                if dev and dev.startswith("r-"):
                    dev = "-".join(dev.split("-")[1:3])
                if dev and dev not in allowed_devices:
                    violating_containments.append((dev, dt))
            elif src == "command":
                tool = dt.get("tool", "")
                args = dt.get("args", {})
                zone = args.get("zone_id")
                dev = args.get("device_id")
                if zone and zone not in allowed_zones:
                    violating_commands.append((tool, zone, args))
                elif dev:
                    # Infer zone from device
                    prefix = dev.split("-")[0]
                    if prefix.startswith("z"):
                        z_inferred = f"zone-{prefix[1:]}"
                        if z_inferred not in allowed_zones:
                            violating_commands.append((tool, z_inferred, args))

        if violating_containments or violating_commands:
            msg = f"False positive violation: containments={violating_containments}, commands={violating_commands}"
            return False, msg
        return True, "nominal"

    def reinstate_if_needed(self, device_id: str) -> bool:
        """Reinstate device only if currently isolated or quarantined (no-op if already ok)."""
        try:
            dev_list = self.mcp.list_devices()
            dev = next((d for d in dev_list if d.get("device_id") == device_id), None)
            if dev and dev.get("status") in ("isolated", "quarantined"):
                self.mcp.call_tool("reinstate_device", {"device_id": device_id})
                return True
        except Exception as e:
            self.log_event("", "scenario", "reinstate_if_needed_error", {"device_id": device_id, "error": str(e)})
        return False

    def finish(self) -> bool:
        """Evaluate all recorded checks, write final result line, and print summary."""
        if self._finished:
            return all(c["pass"] for c in self.checks) and len(self.checks) > 0

        # Guarantee cleanup: clear all events and attacks before writing final line
        try:
            self.rest.clear_event("all")
            self.log_event("", "scenario", "clear_event", {"event_id": "all"})
        except Exception as e:
            self.log_event("", "scenario", "clear_event_error", {"error": str(e)})

        # Reinstate and reset devices ONLY if not in --partners mode
        if self.partners:
            # Under --partners, scenario sends NO actuation or containment;
            # report what is still contained
            try:
                dev_list = self.mcp.list_devices()
                still_contained = [
                    d.get("device_id") for d in dev_list if d.get("status") in ("isolated", "quarantined")
                ]
                if still_contained:
                    print(f"[{self.scenario_id}] [PARTNERS] Devices still contained: {still_contained}")
                    self.log_event("", "scenario", "partners_contained_report", {"still_contained": still_contained})
            except Exception as e:
                logger.debug(f"Partner containment report check: {e}")
        else:
            try:
                # 1. Identify devices contained during THIS scenario from the journal
                contained_during_scenario = set()
                journal_entries = self.rest.get_journal(since_ts=self.start_sim_time, limit=1000)
                for je in journal_entries:
                    if je.get("source") == "containment":
                        dt = je.get("detail", {})
                        to_st = dt.get("to_status", "")
                        tgt = dt.get("target") or dt.get("device_id")
                        if to_st in ("isolated", "quarantined") and tgt:
                            if not tgt.startswith("r-"):
                                contained_during_scenario.add(tgt)

                # 2. Inspect devices: reinstate only if contained during this scenario,
                # and restore actuators only if differing from default
                dev_list = self.mcp.list_devices()
                for d in dev_list:
                    did = d.get("device_id")
                    if not did:
                        continue
                    if d.get("status") in ("isolated", "quarantined"):
                        self.mcp.call_tool("reinstate_device", {"device_id": did})

                    act = d.get("actuator_state", {})
                    dtype = d.get("device_type")
                    if dtype == "water_level" and act.get("valve_position") != "half":
                        self.mcp.call_tool("set_valve", {"device_id": did, "target_state": "half"})
                    elif dtype == "power_load" and act.get("grid_state") != "grid_a":
                        self.mcp.call_tool("switch_grid", {"device_id": did, "target_state": "grid_a"})
                    elif dtype == "traffic_camera" and act.get("signal_plan") != "normal":
                        self.mcp.call_tool("set_signal_plan", {"device_id": did, "target_state": "normal"})
            except Exception as e:
                logger.debug(f"Device restoration notice: {e}")

        # Stop mock subprocesses before reading their log files
        if self.with_mocks:
            self._stop_mocks_processes()

        self.wire.close()
        self._finished = True

        # Collect and sort all log lines strictly by wall_ts
        all_lines = []
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line_s = line.strip()
                        if line_s:
                            try:
                                obj = json.loads(line_s)
                                if obj.get("event") != "result":
                                    all_lines.append(obj)
                            except Exception:
                                pass
            except Exception as e:
                logger.warning(f"Error reading scenario log {self.log_file}: {e}")

        for log_path in (self._guardian_log, self._brain_log):
            if log_path and os.path.exists(log_path):
                try:
                    with open(log_path, "r", encoding="utf-8") as f:
                        for line in f:
                            line_s = line.strip()
                            if line_s:
                                try:
                                    obj = json.loads(line_s)
                                    if obj.get("event") != "result":
                                        all_lines.append(obj)
                                except Exception:
                                    pass
                    os.remove(log_path)
                except Exception as e:
                    logger.warning(f"Error reading mock log {log_path}: {e}")

        all_lines.sort(key=lambda x: str(x.get("wall_ts", "")))

        overall_pass = all(c["pass"] for c in self.checks) and len(self.checks) > 0
        final_sim_ts = self.get_sim_ts_now()
        res_entry = {
            "wall_ts": datetime.now(timezone.utc).isoformat(),
            "sim_ts": final_sim_ts,
            "layer": "scenario",
            "event": "result",
            "pass": overall_pass,
            "checks": self.checks,
        }

        try:
            with open(self.log_file, "w", encoding="utf-8") as f:
                for item in all_lines:
                    f.write(json.dumps(item) + "\n")
                f.write(json.dumps(res_entry) + "\n")
        except Exception as e:
            logger.warning(f"Error writing sorted log {self.log_file}: {e}")

        print(f"=== Scenario {self.scenario_id} Finished: {'PASS' if overall_pass else 'FAIL'} ===", flush=True)
        print(f"Log written to: {self.log_file}", flush=True)
        return overall_pass


def make_common_parser(description: str) -> argparse.ArgumentParser:
    """Create standard argument parser for all scenario CLI scripts."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--twin-host", default=os.getenv("TWIN_API_URL", "http://localhost:8100"), help="Twin Red Team API URL")
    parser.add_argument("--mcp-url", default=os.getenv("TWIN_MCP_URL", "http://localhost:8000"), help="Twin MCP Server URL")
    parser.add_argument("--red-team-token", default=os.getenv("RED_TEAM_TOKEN"), help="Bearer token for Red Team API")
    parser.add_argument("--mcp-token", default=os.getenv("MCP_AUTH_TOKEN"), help="Bearer token for MCP")
    parser.add_argument("--brain-token", default=os.getenv("MCP_BRAIN_TOKEN"), help="Bearer token for Mock Brain")
    parser.add_argument("--guardian-token", default=os.getenv("MCP_GUARDIAN_TOKEN"), help="Bearer token for Mock Guardian")
    parser.add_argument("--scenario-token", default=os.getenv("MCP_SCENARIO_TOKEN"), help="Bearer token for Scenario")
    parser.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"), help="Kafka broker")
    parser.add_argument("--kafka-topic", default=os.getenv("KAFKA_TOPIC", "twin.sensors"), help="Sensor telemetry topic")
    parser.add_argument("--with-mocks", action="store_true", help="Start live mock Guardian and Brain as subprocesses")
    parser.add_argument("--partners", action="store_true", help="Observe external Brain and Guardian via journal")
    parser.add_argument("--no-brain", action="store_true", help="Disable mock Brain (for unmitigated incident tests)")
    parser.add_argument("--log-dir", default="logs", help="Directory for JSON lines event log")
    parser.add_argument("--ci", action="store_true", help="Run in CI test mode")
    return parser

