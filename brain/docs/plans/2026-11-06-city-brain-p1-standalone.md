# City Brain P1 — Standalone Core Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

This is `2026-11-05-city-brain-p1-vertical-slice.md` with the deltas of the final plan §4.1 applied and the defects in those deltas corrected. It replaces that file. Tasks are numbered in execution order.

**Goal:** A Brain that reads signed telemetry, matches readings to Guardian verdicts, keeps every water channel under control with the valve, refuses unsafe commands, verifies each action's physical effect, and writes a hash-chained JSONL log — runnable alone with mocks, with no Twin, broker or network.

**Architecture:** A pure core `step(state, event) -> (state, effects)` fed by adapters through one queue (spec §6). Partner formats are translated at the edge by YAML profiles (§10.1). In standalone mode the mock Twin, mock Guardian and Brain run in one process on a memory bus with a simulated clock, so scenario runs are deterministic. Triplet mode (Kafka and MCP) is P4; here it is a clear stub.

**Tech Stack:** Python 3.13 (`>=3.12`), pydantic v2, jsonschema + rfc3339-validator, cryptography (Ed25519), numpy, PyYAML, pytest, pytest-asyncio, hypothesis.

**Spec:** `brain/docs/ARCHITECTURE.md` (= `CITY_BRAIN_ARCHITECTURE.md` v2.1). Rules G1–G9 of `brain/docs/plans/2026-11-06-city-brain-final-implementation-plan.md` bind every task. Sections implemented here: §2, §5.1, §5.2, §5.3 (verdict message only), §5.4 (decision, command, verification, alert, status), §6, §7, §8.1, §8.2 (water), §8.5 (T0 water fallback only), §8.6 (checks 1–8 and 10), §8.8, §9.1–9.3, §12.2, §16 T02 and T05.

**Out of scope for P1 (later plans, final-plan numbering):** timeline and scenario runner for every T-scenario (P2); Guardian events, heartbeat and modes (P3); Kafka, MCP, REST, conformance kit (P4); approvals and signed Brain events (P5); non-water domains and evidence model (P6); journal and outbox (P7); MPC and effect learning (P8).

## Corrections to the final-plan deltas

| # | Defect in §4.1 as written | Correction here |
|---|---|---|
| C1 | Mocks moved ahead of the profiles (G3 needs them) and the guard (its tests need it); Task 4b needs both; Task 4c belongs in the profile schema | Order is scaffold, skip machinery, types, signatures, profiles + `observed`, mocks, guard, conformance, then the core |
| C2 | Two kept tests call `load_fixture`, which only dropped Task 4 defined | Rewritten against mock output (Tasks 7, 10) |
| C3 | Dropping old Task 15 also dropped `ports.py`, which `InProcTwin` and `BrainApp` need | New Task 16 keeps `ports.py` and `InProcTwin` |
| C4 | The `observed` start value lacks the required `by` | `by: city-brain-team` |
| C5 | `schema.json` is not a repo file | Copied from the JSON block in `INTERFACE.md` (Task 5) |
| C6 | Mock keys were random, so `sig_hash` in logged evidence differs between runs and the determinism exit test cannot pass | All mock keys derive from the seed (Task 6). This also settles open item 9.1: nothing is committed |
| C7 | `trust_weight(None) = 0` starves the filter, because a reading arrives before its verdict | `estimate_weight` gives an unjudged reading `pending_weight`; the trust gate still counts it for nothing (Tasks 11, 15) |
| C8 | `submit_raw` was sync but awaits `twin.call`; `WaterFilter.update` had no way to receive `AT_CEILING`; epoch reset omitted filters and ledger (spec §9.3) | Fixed in Tasks 17, 10, 10–15 |
| C9 | `mcp_server.py`, `publisher.py`, `confluent-kafka` and `mcp` have no consumer until P4 | Moved to P4 |


## Global Constraints

- All files under `brain/` in the `triplet-07` repo; commit nothing outside it. Branch: `brain/p1-vertical-slice`. Commit locally; never push.
- Python `>=3.12`; package `city_brain` in `brain/src/city_brain/`; tests in `brain/tests/`; run from `brain/` with `.venv/Scripts/python -m pytest`.
- Dependencies (pyproject): `pydantic>=2.7`, `jsonschema>=4.21`, `rfc3339-validator>=0.1.4`, `cryptography>=42`, `numpy>=1.26`, `pyyaml>=6`, dev: `pytest>=8`, `pytest-asyncio>=0.23`, `hypothesis>=6`.
- `src/city_brain/core/` imports nothing from `adapters/`, `runtime/`, `asyncio`, `time`, `random`, `datetime.now`, `uuid`, and contains no Twin-specific literals (`twin.sensors`, `set_valve`, `z1-`, `zone-1`, `meters`). Enforced by `tests/test_core_purity.py` (Task 9).
- Every threshold below comes from `policy/default.yaml` or `params/twin-triplet07-v1.yaml`, never a literal in `core/`:
  - water: `high_band: 6.0`, `low_band: 3.0`, `close_sustain_s: 5.0`, `evidence_count: 3`
  - verification: `open_rate_max: -0.01`, `open_window_s: 3.0`, `close_rate_min: 0.005`, `close_window_s: 5.0`
  - shield: `cooldown_s.valve: 5.0`, `rate_limit: {burst: 8, per_s: 2.0}`
  - trust: `verdict_wait_s: 2.0` (wall), `pending_weight: 0.5`, caps `TRUSTED 1.0, DOUBTFUL 0.5, UNTRUSTED 0.0, UNKNOWN 0.0`
  - filter: `sample_period_s: 0.1`, `r0: 2.5e-5`, `q_level: 1.0e-8`, `q_inflow: 1.0e-8`, `b_prior: 0.045`, `jump_sigma: 6.0`, `stuck_ticks: 30`, `stale_s: 2.0`
  - epochs: `seq_drop: 100`, `ts_back_s: 60`
- Risk classes (spec §8.6): valve `open` or ratio increase = R1; valve `closed` or ratio decrease = R2. R1 needs every cited reading TRUSTED or DOUBTFUL and at least one TRUSTED; R2 needs every cited reading TRUSTED.
- Brain events are CloudEvents 1.0 dicts; the core never sets `time` (wall clock); the log sink adds it. Determinism comparisons ignore `time`, `hash` and `prev_hash`.
- `sig_hash(sig)` = first 16 hex chars of SHA-256 over the UTF-8 bytes of the base64 `sig` string.
- Idempotency key = `brain-<epoch>-<action_id>`. Never call a Twin tool twice with the same key.

## Review Focus

1. **Twin restart mid-run** (ids, `seq`, `ts` **and signing keys** restart): expected the Brain reloads keys after a burst of bad signatures, starts a new epoch, raises no false duplicates, and control continues → Task 9 (`test_twin_restart_starts_new_epoch`) and Task 17 (`test_twin_restart_reloads_keys_and_continues`).
2. **Verdict arrives before its reading**: expected the verdict is parked and applied when the reading lands → Task 11 (`test_verdict_before_reading_is_applied`).
3. **Channel pinned at the 12 m ceiling with valve closed** (true level unknown, slope 0): expected the planner still opens the valve → Task 12 (`test_ceiling_with_closed_valve_opens`).
4. **Command outcome unknown**: expected `UNKNOWN`, an alert, and no resend with the same key → Task 15 (`test_timeout_is_unknown_and_never_resent`) and Task 16 (`test_transport_error_is_unknown`).
5. **Readings from a quarantined device** (Twin keeps publishing them): expected zero evidence weight and the Shield refuses to actuate it → Task 11 (`test_contained_device_has_zero_weight`) and Task 13 (`test_contained_target_refused`).

---

## File Structure

```
brain/
├── pyproject.toml, Makefile, README.md, INTERFACE.md, docs/ARCHITECTURE.md, docs/plans/
├── config/standalone.yaml, config/triplet.yaml
├── profiles/twin-triplet07.yaml, profiles/guardian-triplet07.yaml, profiles/twin-alt-example.yaml
├── policy/default.yaml
├── params/twin-triplet07-v1.yaml
├── schemas/partners/twin-triplet07.reading.schema.json   # from the JSON block in INTERFACE.md
├── schemas/guardian-verdict.v1.schema.json
├── schemas/brain-event.v1.schema.json
├── src/city_brain/
│   ├── config.py                  # load config + profiles + policy + params
│   ├── domain/types.py, ids.py    # canonical types, deterministic ids
│   ├── edge/signatures.py         # canonical bytes, Ed25519 verify, KeyRing
│   ├── edge/profiles.py           # TwinProfile, GuardianProfile, ToolCall mapping
│   ├── edge/guard.py              # ReadingGuard, VerdictGuard
│   ├── core/events.py, state.py, epochs.py, estimator.py, trust.py, water.py, shield.py, records.py, coordinator.py, step.py
│   ├── ports.py                   # Protocols for adapters
│   ├── adapters/memory/bus.py, adapters/files/jsonl_log.py, adapters/inproc_twin.py
│   ├── runtime/clocks.py, runtime/app.py, runtime/main.py
├── mocks/keys.py, mocks/twin/physics.py, mocks/guardian/mock_guardian.py
├── scenarios/runner.py, scenarios/T02.yaml, scenarios/T05.yaml
└── tests/ (mirrors src; plus conftest.py, partners.py, factories.py, helpers_mock.py, test_core_purity.py, scenarios/)
```

---

### Task 1: Scaffold, configuration and spec copy

**Files:**
- Create: `brain/pyproject.toml`, `brain/Makefile`, `brain/docs/ARCHITECTURE.md`, `brain/config/standalone.yaml`, `brain/config/triplet.yaml`, `brain/policy/default.yaml`, `brain/params/twin-triplet07-v1.yaml`, `brain/src/city_brain/__init__.py`, `brain/src/city_brain/config.py`
- Test: `brain/tests/test_config.py`

**Interfaces:**
- Produces: `load_config(path: str | Path, env: Mapping[str, str] | None = None) -> BrainConfig`. `BrainConfig` (pydantic) has `mode: Literal["standalone", "triplet"]`, `kafka: KafkaConfig(bootstrap, group, topics: dict[str, str])`, `twin: TwinConfig(profile, mcp_url, call_timeout_s)`, `guardian: GuardianConfig(profile, public_key_file, verdict_wait_s)`, `policy: dict`, `params: dict`, `logs_dir: Path`. Env override syntax `BRAIN__KAFKA__BOOTSTRAP=...` (double underscore = nesting). Policy and params files are loaded into `policy` and `params` from paths named in the config.

**Environment:** the interpreter is `brain/.venv` (Python 3.13, already created; use `.venv/Scripts/python` on this Windows machine). `pyproject.toml` must set `[tool.pytest.ini_options] pythonpath = [".", "src"]`, `addopts = "--import-mode=importlib"`, `asyncio_mode = "auto"`, so `mocks.*`, `scenarios.*` and `tests.*` import from `brain/`. `Makefile` recipes call only the interpreter found by `PY := $(firstword $(wildcard .venv/Scripts/python.exe .venv/bin/python) python)`, so they run under GNU make and `mingw32-make`. Target `test` runs `$(PY) -m pytest`. `brain/docs/ARCHITECTURE.md` already exists: leave it.

- [ ] **Step 1: Copy the spec** to `brain/docs/ARCHITECTURE.md` and create the files above with the values from Global Constraints. `config/standalone.yaml` sets `mode: standalone`, `logs_dir: ../logs`; `config/triplet.yaml` sets `kafka.bootstrap: kafka:9092`, `kafka.group: city-brain`, `kafka.topics: {readings: twin.sensors, verdicts: guardian.verdicts}`, `twin.mcp_url: http://twin_layer:8000/sse`, `twin.call_timeout_s: 2`.

- [ ] **Step 2: Write the failing tests**

```python
def test_loads_standalone_with_policy_and_params():
    cfg = load_config("config/standalone.yaml")
    assert cfg.mode == "standalone"
    assert cfg.policy["water"]["high_band"] == 6.0
    assert cfg.params["filter"]["b_prior"] == 0.045

def test_env_override_nested():
    cfg = load_config("config/triplet.yaml", env={"BRAIN__KAFKA__BOOTSTRAP": "localhost:19092"})
    assert cfg.kafka.bootstrap == "localhost:19092"

def test_unknown_key_is_a_clear_error():
    with pytest.raises(ValueError, match="kafka.bootstrapp"):
        load_config_from_dict({"mode": "triplet", "kafka": {"bootstrapp": "x"}})
```

- [ ] **Step 3: Run** `pytest tests/test_config.py -v` → FAIL (module missing).
- [ ] **Step 4: Implement `load_config` and `load_config_from_dict(data: dict) -> BrainConfig`** in `config.py` (pydantic models with `extra="forbid"`; the error message names the dotted path).
- [ ] **Step 5: Run** `.venv/Scripts/python -m pip install -e .[dev] && .venv/Scripts/python -m pytest tests/test_config.py -v` → PASS.
- [ ] **Step 6: Commit** `git add brain && git commit -m "brain: scaffold, config loader, spec copy"`

---

### Task 2: Partner-test skip machinery

**Files:**
- Create: `brain/tests/conftest.py`, `brain/tests/partners.py`
- Modify: `brain/pyproject.toml` (register markers `triplet`, `kafka`, `twin_live`; dev dependency none new)
- Test: `brain/tests/test_skip_machinery.py`

**Interfaces:**
- Produces `tests/partners.py`: `PartnerEndpoint(marker: str, env_var: str, default: str)`, `ENDPOINTS` for `twin_live` and `triplet` (`BRAIN_TWIN_MCP`, default `http://localhost:8000/sse`) and `kafka` (`BRAIN_KAFKA_BOOTSTRAP`, default `localhost:19092`); `reachable(url_or_host_port: str, timeout_s: float = 0.5) -> bool` (a TCP connect, never raises); `skip_reason(ep, value) -> str` returning e.g. `Twin not reachable at http://localhost:8000/sse ($BRAIN_TWIN_MCP)`.
- `conftest.py`: `pytest_collection_modifyitems` probes each marker used by collected tests once per session and adds `pytest.mark.skip(reason=skip_reason(...))` when unreachable. `pytest_terminal_summary` prints one line, `partner tests skipped: kafka=N triplet=N twin_live=N`, whenever any were skipped (G7: loud, never a failure). Nothing is probed when no collected test carries a marker.

- [ ] **Step 1: Write the failing tests**

```python
def test_reachable_is_false_for_closed_port():
    assert reachable("localhost:1", timeout_s=0.1) is False

def test_reachable_accepts_a_url():
    assert reachable("http://localhost:1/sse", timeout_s=0.1) is False

def test_skip_reason_names_env_var_and_endpoint():
    ep = ENDPOINTS["twin_live"]
    assert skip_reason(ep, "http://x:1/sse") == "Twin not reachable at http://x:1/sse ($BRAIN_TWIN_MCP)"

@pytest.mark.twin_live
def test_marker_skips_when_twin_is_down():
    raise AssertionError("must be skipped with nothing running")
```

Run the last test with `BRAIN_TWIN_MCP` unset and nothing on port 8000. It carries the marker on purpose: it proves the skip and shows the summary line.

- [ ] **Step 2: Run** `.venv/Scripts/python -m pytest tests/test_skip_machinery.py -v` → FAIL.
- [ ] **Step 3: Implement.** **Step 4: Run** → 3 passed, 1 skipped, and the summary line printed.
- [ ] **Step 5: Commit** `git commit -m "brain: partner-test skip machinery"`

---

### Task 3: Canonical types and deterministic ids

**Files:**
- Create: `brain/src/city_brain/domain/types.py`, `brain/src/city_brain/domain/ids.py`
- Test: `brain/tests/domain/test_ids.py`, `brain/tests/domain/test_types.py`

**Interfaces:**
- Produces (all pydantic, `frozen=True`):
  - Enums (`StrEnum`): `SensorKind {WATER_LEVEL, TRAFFIC_FLOW, POWER_LOAD, AIR_QUALITY, CALL_RATE}`, `CanonicalUnit {M, VEH_PER_MIN, MW, AQI, CALLS_PER_MIN}`, `DeviceStatus {OK, ISOLATED, QUARANTINED, ROLLED_BACK, MODELLED}`, `TrustState {TRUSTED, DOUBTFUL, UNTRUSTED, UNKNOWN}`, `StateKind {OBSERVED, VALIDATED, ESTIMATED, UNKNOWN}`, `RiskClass {R0, R1, R2, R3}`, `ActionStatus {PROPOSED, REFUSED, AWAITING_TRUST, BLOCKED, DISPATCHED, REJECTED, UNKNOWN, VERIFYING, VERIFIED, UNVERIFIED, SUPERSEDED}`.
  - `Observation(reading_id, sig_hash, device_id, zone_id, kind, seq: int, sim_time: datetime, value: float, unit, device_status, flags: frozenset[str])`
  - `Verdict(verdict_id, reading_id, sig_hash, device_id, state: TrustState, trust: float | None, reasons: frozenset[str], issued_at: datetime, expires_at: datetime)`
  - `ActuatorCapability(capability: Literal["valve","signal","grid","dispatch"], target: str, states: frozenset[str], current_state: str | None)`
  - `DeviceProfile(device_id, zone_id, kind, unit, min_val: float, max_val: float, status: DeviceStatus, actuator: ActuatorCapability | None)`
  - `EvidenceRef(reading_id, sig_hash, device_id, value: float, sim_time: datetime, state_kind: StateKind, verdict_id: str | None, trust_state: TrustState, weight: float)`
  - `ExpectedEffect(channel: str, metric: Literal["rate","level"], comparator: Literal["<=",">="], threshold: float, window_s: float)`
  - `Alternative(option: str, rejected_because: str)`
  - `ActionIntent(action_id, incident_id, plan_version: int, capability, target, state, risk_class: RiskClass, expected_effect: ExpectedEffect, evidence: tuple[EvidenceRef, ...], rationale: str, alternatives: tuple[Alternative, ...])`
- Produces (`ids.py`): `sig_hash(sig_b64: str) -> str`; `action_id(incident_id: str, capability: str, target: str, state: str, plan_version: int) -> str` returning `"act-" + sha256("|".join(parts))[:16]`; `idempotency_key(epoch: int, action_id: str) -> str`.

- [ ] **Step 1: Write the failing tests**

```python
def test_sig_hash_is_first_16_hex_of_sha256():
    assert sig_hash("abc") == hashlib.sha256(b"abc").hexdigest()[:16]

def test_action_id_is_deterministic_and_sensitive():
    a = action_id("inc-1", "valve", "z1-wl1", "open", 1)
    assert a == action_id("inc-1", "valve", "z1-wl1", "open", 1)
    assert a != action_id("inc-1", "valve", "z1-wl1", "open", 2)
    assert a.startswith("act-") and len(a) == 20

def test_idempotency_key_format():
    assert idempotency_key(3, "act-0123456789abcdef") == "brain-3-act-0123456789abcdef"

def test_types_are_frozen():
    with pytest.raises(ValidationError):
        make_observation().value = 1.0   # helper builds a valid Observation
```

- [ ] **Step 2: Run** `pytest tests/domain -v` → FAIL.
- [ ] **Step 3: Implement** the types and ids. Put a `make_observation(**overrides)` and `make_verdict(**overrides)` factory in `tests/factories.py` for all later tasks (defaults: `z1-wl1`, `zone-1`, `WATER_LEVEL`, value 5.0, `OK`, `TRUSTED`, sim time `2026-10-17T12:00:00Z`).
- [ ] **Step 4: Run** `pytest tests/domain -v` → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: canonical types and deterministic ids"`

---

### Task 4: Signatures and key ring

**Files:**
- Create: `brain/src/city_brain/edge/signatures.py`
- Test: `brain/tests/edge/test_signatures.py`

**Interfaces:**
- Produces: `canonical_bytes(payload: Mapping[str, Any]) -> bytes` (drop `sig`, `json.dumps(..., sort_keys=True, separators=(",", ":"))`, UTF-8 — identical to the Twin's `crypto_utils.canonicalize_for_signing`); `verify(payload: Mapping[str, Any], public_key_b64: str) -> bool` (never raises); `sign(payload: Mapping[str, Any], private_key_b64: str) -> str` (used by mocks); `generate_keypair() -> tuple[str, str]`; `class KeyRing: set(device_id, pub_b64) / get(device_id) -> str | None / clear()`.

- [ ] **Step 1: Write the failing tests**

```python
PAYLOAD = {"reading_id": "r-z1-wl1-7", "device_id": "z1-wl1", "device_type": "water_level",
           "ts": "2026-10-17T12:00:00Z", "seq": 7, "value": 12.0, "unit": "meters",
           "location": "zone-1", "status": "ok"}

def test_roundtrip():
    priv, pub = generate_keypair()
    signed = {**PAYLOAD, "sig": sign(PAYLOAD, priv)}
    assert verify(signed, pub)

def test_roundtrip_survives_json_reparse():          # float 12.0 and 0.0 must re-serialize identically
    priv, pub = generate_keypair()
    for v in (12.0, 0.0, 5.42):
        p = {**PAYLOAD, "value": v}
        wire = json.dumps({**p, "sig": sign(p, priv)})
        assert verify(json.loads(wire), pub)

def test_one_changed_byte_fails():
    priv, pub = generate_keypair()
    signed = {**PAYLOAD, "sig": sign(PAYLOAD, priv)}
    assert not verify({**signed, "value": 12.01}, pub)

def test_garbage_signature_or_key_returns_false():
    assert not verify({**PAYLOAD, "sig": "AAAA"}, generate_keypair()[1])
    assert not verify({**PAYLOAD, "sig": "AAAA"}, "not-base64!")
```

- [ ] **Step 2: Run** `pytest tests/edge/test_signatures.py -v` → FAIL.
- [ ] **Step 3: Implement** with `cryptography.hazmat.primitives.asymmetric.ed25519`; catch `InvalidSignature`, `ValueError`, `binascii.Error`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: Ed25519 verification matching Twin canonical form"`

---

### Task 5: Partner profiles and mapping

**Files:**
- Create: `brain/src/city_brain/edge/profiles.py`, `brain/profiles/twin-triplet07.yaml`, `brain/profiles/guardian-triplet07.yaml`, `brain/profiles/twin-alt-example.yaml`, `brain/schemas/partners/twin-triplet07.reading.schema.json`, `brain/schemas/guardian-verdict.v1.schema.json`
- Test: `brain/tests/edge/test_profiles.py`

**Interfaces:**
- Produces:
  - `TwinProfile.load(path) -> TwinProfile` with `schema: dict`, `map_reading(raw: Mapping) -> dict` (canonical field names and enum values, `value` converted to the canonical unit), `map_device(raw: Mapping) -> DeviceProfile`, `to_tool_call(capability: str, target: str, state: str) -> ToolCall`, `canonical_actuator_state(device_raw: Mapping) -> str | None`.
  - `ToolCall(tool: str, args: dict[str, Any])` (frozen).
  - `GuardianProfile.load(path) -> GuardianProfile` with `map_state(partner_state: str) -> TrustState` (raises `UnknownVerdictState`), `public_key_b64: str`.
- Twin profile content: the field, kind and unit maps and the actuator section from spec §5.2 and §10.1. Canonical valve states: `open`, `closed`, `ratio:<int 0..100>`; `to_tool_call("valve", "z1-wl1", "ratio:33") == ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "33%"})`. `canonical_actuator_state` maps `valve_position` `"open"→"open"`, `"closed"→"closed"`, `"NN%"→"ratio:NN"`, anything else → `"closed"` with flag (the Twin treats unknown states as closed).
- Guardian profile: vocabulary table from spec §7.3; `public_key_file` path.
- Alt profile: fields `reading`→value, `device_category: flood_sensor`→WATER_LEVEL, unit `cm`→`m` (×0.01).
- **`schema.json` is not a file in the repo.** The reading schema exists only as the JSON block in the root `INTERFACE.md` (section with `additionalProperties`). Copy that block verbatim into `schemas/partners/twin-triplet07.reading.schema.json`.
- **`location`:** the schema allows a string or `{zone_id, lat, lon}`. `map_reading` accepts both and yields the zone id string (open item 9.3 settled).
- **`observed` block (final-plan Task 4c, G2).** Both profile kinds require `observed: {commit, date, by, method}`, plus optional `assumed_groups: [<top-level profile key>, ...]`; `method` is one of `code_read`, `recording`, `assumed`. A missing key or unknown method raises `ProfileError` naming the key. `TwinProfile.observed` and `GuardianProfile.observed` expose it; `unverified_groups -> tuple[str, ...]` returns `assumed_groups`, or every group when `method` is `assumed`. `twin-triplet07.yaml` starts as `{commit: 418a024, date: 2026-11-04, by: city-brain-team, method: code_read}`. The Guardian and alt profiles use `method: assumed`.
- **Mock-facing profile data (G3).** `twin-triplet07.yaml` also carries `inventory: {zones: 6, device_id: "z{zone}-{suffix}", zone_id: "zone-{zone}", devices: [{suffix, type, unit, min, max, init}, ...]}` copied from `twin/simulation.py` lines 111–117, and `registry: {source: mcp:list_devices, fields: [device_id, zone_id, device_type, unit, lat, lon, min_val, max_val, status, actuator_state]}`. The Brain never reads `inventory`; the mock does.
- `TwinProfile` also exposes `allowlist -> dict[str, frozenset[str]]` of canonical states per capability (`ratio` stands for `ratio:<0..100>`), consumed by the Shield, and `accepts_partner_state(capability: str, partner_state: str) -> bool`, consumed by the mock.
- `GuardianProfile.load(path, public_key_b64: str | None = None)`: the argument overrides `public_key_file`, and the file may then be absent (standalone runs hold the mock's key in memory; no key material is committed).

- [ ] **Step 1: Write the failing tests**

```python
def test_twin_reading_maps_to_canonical():
    p = TwinProfile.load("profiles/twin-triplet07.yaml")
    m = p.map_reading({**TWIN_RAW})          # TWIN_RAW = water reading, value 5.42, unit "meters", location "zone-1"
    assert m["kind"] == SensorKind.WATER_LEVEL and m["unit"] == CanonicalUnit.M
    assert m["zone_id"] == "zone-1" and m["value"] == 5.42

def test_alt_profile_gives_same_canonical_value():
    alt = TwinProfile.load("profiles/twin-alt-example.yaml")
    m = alt.map_reading({**ALT_RAW})         # reading 542, unit "cm", device_category "flood_sensor"
    assert m["kind"] == SensorKind.WATER_LEVEL and m["value"] == pytest.approx(5.42)

def test_valve_ratio_tool_call():
    p = TwinProfile.load("profiles/twin-triplet07.yaml")
    assert p.to_tool_call("valve", "z1-wl1", "ratio:33") == ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "33%"})
    with pytest.raises(StateNotAllowed):
        p.to_tool_call("valve", "z1-wl1", "half")

def test_guardian_vocabulary():
    g = GuardianProfile.load("profiles/guardian-triplet07.yaml")
    assert g.map_state("SUSPECT") is TrustState.DOUBTFUL
    assert g.map_state("MALICIOUS") is TrustState.UNTRUSTED
    with pytest.raises(UnknownVerdictState):
        g.map_state("MAYBE")

def test_location_object_form_maps_to_zone():
    p = TwinProfile.load("profiles/twin-triplet07.yaml")
    assert p.map_reading({**TWIN_RAW, "location": {"zone_id": "zone-3", "lat": 1.0, "lon": 2.0}})["zone_id"] == "zone-3"

def test_observed_block_is_required(tmp_path):
    bad = tmp_path / "p.yaml"; bad.write_text(yaml_without_observed())
    with pytest.raises(ProfileError, match="observed"):
        TwinProfile.load(bad)

def test_assumed_groups_are_listed():
    assert "actuators" in TwinProfile.load("profiles/twin-alt-example.yaml").unverified_groups
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: partner profiles with observed blocks"`

---

### Task 6: Mock Twin and mock Guardian

R1 evidence. Built from the profile (G3), so a profile edit changes the mock for free. Read `twin/simulation.py` and `twin/kafka_streamer.py` first: the wire format, reply shapes and physics must match them. Do not import from `twin/`.

**Files:**
- Create: `brain/mocks/__init__.py`, `brain/mocks/twin/__init__.py`, `brain/mocks/twin/physics.py`, `brain/mocks/guardian/__init__.py`, `brain/mocks/guardian/mock_guardian.py`, `brain/mocks/keys.py`
- Test: `brain/tests/mocks/test_mock_twin.py`, `brain/tests/mocks/test_mock_guardian.py`

`mocks/twin/mcp_server.py` and `mocks/twin/publisher.py` move to P4 with the transports that use them.

**Interfaces:**
- Produces `mocks/keys.py derive_keypair(label: str) -> tuple[str, str]`: Ed25519 private key = SHA-256 of `label`. **Every mock key is derived from the seed**, because `sig_hash` appears in logged evidence and two runs must be identical (P1 exit).
- Produces `MockTwin(profile: TwinProfile | None = None, seed: int = 42, start: datetime = 2026-10-17T12:00:00Z, defects: MockDefects = MockDefects())`; `None` loads `profiles/twin-triplet07.yaml`. Device ids, zones, units, ranges and initial values come from `profile.inventory`; wire field names, kind names and units come from inverting `profile.fields`, `kinds`, `units`; accepted states from `profile.accepts_partner_state`. Device keys are `derive_keypair(f"{seed}:{restarts}:{device_id}")`.
- Attributes and methods: `devices: dict[str, MockDevice]` (`MockDevice` has `value`, `seq`, `status`, `valve_ratio`, `signal_plan`, `grid_state`, `private_key`, `public_key`), `tick() -> list[dict]` (signed readings in the Twin's exact wire format, 42 devices, `seq` from 1, whole-second `ts`, values rounded as the Twin rounds them, water physics identical to spec §2.2 with `random.Random(seed)`, other channels from the same equations), `list_devices() -> list[dict]`, `get_device_key(device_id) -> str | None`, `get_city_state() -> dict`, `handle_tool(tool: str, args: dict) -> dict` (same replies as the Twin's `set_valve`/`set_signal_plan`/`switch_grid`/`dispatch`/`get_command_result`), `inject(capability: str, target: str, params: dict)` supporting `device_physical`, `device_key`, `sensor_fault`, `link` and `actuator` as in `twin/simulation.py`, `restart()` (new keys, `seq` and clock reset).
- `MockDefects(accept_unknown_states: bool = True, reexecute_repeated_keys: bool = True, actuate_contained: bool = True)` — defaults reproduce the real Twin's behaviour (spec §2.4).
- Produces `MockGuardian(private_key_b64: str, policy: GuardianPolicy = trust_all(), ttl_s: float = 30.0)` with `on_reading(raw: dict) -> list[bytes]` (verdict JSON due now, usually one; spec §5.3 format, `issued_at` = reading `ts`, `expires_at` = `issued_at + ttl_s`, `verdict_id` = `v-<NNNNNN>` from a counter, `sig_hash` per Global Constraints) and `drain_events() -> list[bytes]` (signed `guardian.events` containment messages).
- Policies (factories returning frozen objects): `trust_all()`; `delay(n_s)` holds each verdict until a later reading's `ts` is ≥ the held reading's `ts + n_s`; `silent()` emits nothing; `flip_after(n)` emits TRUSTED for the first `n` verdicts, then UNTRUSTED; `untrusted(device)` emits UNTRUSTED for that device and TRUSTED otherwise; `quarantine_at(device, t)` from `t` seconds after the first reading emits UNTRUSTED for the device and queues one containment event; `expired()` sets `expires_at` before `issued_at`; `unsigned()` omits `sig`. P1 wires only `trust_all`; P3 consumes the rest.

- [ ] **Step 1: Write the failing tests**

```python
def test_mock_water_rises_like_twin():
    t = MockTwin(); vals = [r["value"] for _ in range(100) for r in t.tick() if r["device_id"] == "z1-wl1"]
    assert vals[-1] - vals[0] == pytest.approx(0.015 * 9.9, abs=0.03)

def test_mock_reading_verifies_with_published_key():
    t = MockTwin(); r = t.tick()[0]
    assert verify(r, t.get_device_key(r["device_id"]))

def test_same_seed_gives_identical_signed_stream():
    a, b = MockTwin(seed=7), MockTwin(seed=7)
    assert [a.tick() for _ in range(5)] == [b.tick() for _ in range(5)]

def test_mock_reproduces_half_closes_valve():
    t = MockTwin(); t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "half"})
    assert t.devices["z1-wl1"].valve_ratio == 0.0

def test_actuator_attack_replies_ok_but_noop():
    t = MockTwin(); t.inject("actuator", "z4-wl1", {})
    assert t.handle_tool("set_valve", {"device_id": "z4-wl1", "target_state": "open"})["status"] == "ok"
    assert t.devices["z4-wl1"].valve_ratio == 0.0

def test_restart_changes_keys_and_resets_seq():
    t = MockTwin(); old = t.get_device_key("z1-wl1"); t.tick(); t.restart()
    assert t.get_device_key("z1-wl1") != old and t.tick()[0]["seq"] == 1

def test_trust_all_verdict_binds_reading():
    v = json.loads(MockGuardian(PRIV).on_reading(reading)[0])
    assert v["reading_id"] == reading["reading_id"] and v["sig_hash"] == sig_hash(reading["sig"])
    assert verify(v, PUB)

def test_silent_emits_nothing():
    assert MockGuardian(PRIV, silent()).on_reading(reading) == []

def test_delay_holds_until_sim_time_passes():
    g = MockGuardian(PRIV, delay(2.0))
    assert g.on_reading(at_ts(0)) == [] and g.on_reading(at_ts(1)) == []
    assert len(g.on_reading(at_ts(2))) >= 1

def test_flip_after_turns_untrusted():
    g = MockGuardian(PRIV, flip_after(1))
    states = [json.loads(g.on_reading(at_ts(i))[0])["state"] for i in range(2)]
    assert states == ["TRUSTED", "UNTRUSTED"]

def test_untrusted_applies_to_one_device_only():
    g = MockGuardian(PRIV, untrusted("z1-wl1"))
    assert json.loads(g.on_reading(reading_for("z1-wl1"))[0])["state"] == "UNTRUSTED"
    assert json.loads(g.on_reading(reading_for("z2-wl1"))[0])["state"] == "TRUSTED"

def test_quarantine_at_queues_one_event():
    g = MockGuardian(PRIV, quarantine_at("z1-wl1", 1.0)); g.on_reading(at_ts(0)); g.on_reading(at_ts(2)); g.on_reading(at_ts(3))
    events = [json.loads(e) for e in g.drain_events()]
    assert [e["action"] for e in events] == ["quarantined"] and events[0]["device_id"] == "z1-wl1"

def test_expired_verdict_is_already_past():
    v = json.loads(MockGuardian(PRIV, expired()).on_reading(reading)[0])
    assert v["expires_at"] < v["issued_at"]

def test_unsigned_has_no_sig():
    assert "sig" not in json.loads(MockGuardian(PRIV, unsigned()).on_reading(reading)[0])
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: profile-built mock Twin and scripted mock Guardian"`

---

### Task 7: Edge guard for readings and verdicts

**Files:**
- Create: `brain/src/city_brain/edge/guard.py`
- Test: `brain/tests/edge/test_guard.py`

**Interfaces:**
- Consumes: `TwinProfile`, `GuardianProfile`, `KeyRing`, `verify`, `sig_hash`, `DeviceProfile`.
- Produces: `Accepted(observation: Observation)`, `Rejected(code: str, detail: str, device_id: str | None)`; `ReadingGuard(profile: TwinProfile, keyring: KeyRing, registry: Mapping[str, DeviceProfile]).check(raw: bytes) -> Accepted | Rejected`; `VerdictGuard(profile: GuardianProfile).check(raw: bytes) -> AcceptedVerdict(verdict: Verdict) | Rejected`.
- Reading checks in order with codes from spec §5.1: `MALFORMED_JSON`, `SCHEMA_VIOLATION` (jsonschema `Draft7Validator` with `FormatChecker()`; detail names the JSON path), `UNKNOWN_DEVICE`, `TYPE_MISMATCH`, `BAD_SIGNATURE`; then `OUT_OF_RANGE` and `AT_CEILING` are added as flags on an accepted observation (`value >= max_val` → `AT_CEILING`; outside `[min_val, max_val]` → `OUT_OF_RANGE`). Size limit 4096 bytes → `OVERSIZED`.
- Verdict checks: `MALFORMED_JSON`, `SCHEMA_VIOLATION`, `UNKNOWN_VERDICT_STATE`, `BAD_SIGNATURE` (Guardian public key; canonical bytes as Task 4).

- [ ] **Step 1: Write the failing tests** — one per code, written against mock output (`MockTwin` readings, `MockGuardian` verdicts), plus:

```python
def test_mock_clean_stream_has_zero_rejects():
    twin = MockTwin(); guard = guard_for_mock(twin)   # tests/helpers_mock.py: registry + keyring from the mock
    results = [guard.check(json.dumps(r).encode()) for _ in range(500) for r in twin.tick()]
    assert len(results) == 21_000 and [r for r in results if isinstance(r, Rejected)] == []

def test_bad_date_time_is_schema_violation():
    r = guard.check(json.dumps({**signed_reading(), "ts": "yesterday"}).encode())
    assert isinstance(r, Rejected) and r.code == "SCHEMA_VIOLATION" and "ts" in r.detail

def test_stuck_42_on_water_is_accepted_with_out_of_range_flag():
    r = guard.check(signed_bytes(value=42.0))
    assert isinstance(r, Accepted) and "OUT_OF_RANGE" in r.observation.flags

def test_unsigned_verdict_rejected():
    assert vguard.check(json.dumps(verdict_without_sig()).encode()).code == "BAD_SIGNATURE"
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: edge guard with reason-coded rejects"`

---

### Task 8: Profile and mock conformance

The test that makes a profile edit propagate (G3).

**Files:**
- Test: `brain/tests/mocks/test_mock_matches_profile.py`
- Modify: mock or profile code only where this test exposes a real mismatch

**Interfaces:**
- Consumes `MockTwin`, `MockDefects`, `TwinProfile`, `ReadingGuard`, `tests/helpers_mock.py::guard_for_mock`.

- [ ] **Step 1: Write the tests**

```python
STRICT = MockDefects(accept_unknown_states=False)

@pytest.mark.parametrize("capability", ["valve", "signal", "grid"])
def test_mock_accepts_exactly_the_profile_states(capability):
    p = load(); t = MockTwin(p, defects=STRICT)
    tool, target = tool_and_target(p, capability, t)
    for partner_state in listed_partner_states(p, capability):
        assert t.handle_tool(tool, {"device_id": target, "target_state": partner_state})["status"] == "ok"
    assert t.handle_tool(tool, {"device_id": target, "target_state": "no-such-state"})["status"] == "error"

def test_default_defects_keep_the_real_twin_behaviour():
    assert MockTwin().handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "no-such-state"})["status"] == "ok"

def test_every_mock_reading_passes_the_guard():
    t = MockTwin(); g = guard_for_mock(t)
    assert not [r for _ in range(50) for r in t.tick() if isinstance(g.check(json.dumps(r).encode()), Rejected)]

def test_inventory_matches_registry_shape():
    p = load(); devices = MockTwin(p).list_devices()
    assert len(devices) == 42 and all(set(d) == set(p.registry["fields"]) for d in devices)
    assert all(p.map_device(d).device_id == d["device_id"] for d in devices)

def test_profile_edit_changes_the_mock(tmp_path):
    p = load_with(tmp_path, lambda doc: doc["actuators"]["valve"]["states"].update(closed="shut"))
    t = MockTwin(p, defects=STRICT)
    assert t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "shut"})["status"] == "ok"
    assert t.handle_tool("set_valve", {"device_id": "z1-wl1", "target_state": "closed"})["status"] == "error"
```

- [ ] **Step 2: Run** `.venv/Scripts/python -m pytest tests/mocks -v` and fix any mismatch in the mock, or in the profile if the profile is wrong against `twin/`. Never in `core/`.
- [ ] **Step 3: Commit** `git commit -m "brain: profile and mock conformance test"`

---

### Task 9: Core events, state, step skeleton, epochs and dedupe

**Files:**
- Create: `brain/src/city_brain/core/events.py`, `core/state.py`, `core/epochs.py`, `core/step.py`
- Test: `brain/tests/core/test_epochs.py`, `brain/tests/core/test_step_ingest.py`, `brain/tests/test_core_purity.py`

**Interfaces:**
- Produces events (frozen dataclasses): `RegistryLoaded(devices: tuple[DeviceProfile, ...])`, `ReadingReceived(obs: Observation)`, `ReadingRejected(code: str, device_id: str | None)`, `VerdictReceived(verdict: Verdict)`, `CommandResult(action_id: str, outcome: Literal["ACCEPTED","REJECTED","UNKNOWN"], reply: dict)`, `TimerFired(timer_id: str, kind: str)`.
- Produces effects: `Publish(event: dict)`, `SendCommand(action_id: str, idempotency_key: str, capability: str, target: str, state: str)`, `StartTimer(timer_id: str, kind: str, wall_seconds: float)`, `CancelTimer(timer_id: str)`.
- Produces `BrainState` (dataclass, owned and mutated only by `step`): `epoch: int`, `registry: dict[str, DeviceProfile]`, `cursors: dict[str, DeviceCursor(seq, sim_time)]`, `seen: set[tuple[str, str]]`, `global_tick: int`, `epoch_origin: tuple[datetime, int] | None` (first `(sim_time, seq)` of the epoch), `counters: Counter[str]`, `event_seq: int`, plus fields added by later tasks. Property `sim_now -> datetime` = `origin_time + (global_tick - origin_seq) * sample_period_s` (sub-second precision, because `ts` has whole seconds). `BrainState.new(policy: dict, params: dict) -> BrainState`.
- Produces `is_new_epoch(cursor: DeviceCursor | None, obs: Observation, seq_drop: int, ts_back_s: float) -> bool`.
- Produces `step(state: BrainState, event: Event) -> tuple[BrainState, list[Effect]]`. In this task it handles `RegistryLoaded` (store registry, emit `status` event `startup` — see Task 14 for the builder; until then emit a plain dict `{"type": "status"}` and replace it in Task 14), `ReadingReceived` (epoch check → on new epoch: `epoch += 1`, clear `seen`, `cursors`, emit status `epoch_changed`; dedupe on `(reading_id, sig_hash)` → `counters["DUPLICATE"] += 1` and stop; `seq <= cursor.seq` → `counters["OUT_OF_ORDER"] += 1` and stop; else update cursor and `global_tick = max(global_tick, obs.seq)`), `ReadingRejected` (count by code).

- [ ] **Step 1: Write the failing tests**

```python
def test_duplicate_reading_counted_not_processed():
    s = fresh_state(); r = make_observation(seq=5)
    s, _ = step(s, ReadingReceived(r)); s, fx = step(s, ReadingReceived(r))
    assert s.counters["DUPLICATE"] == 1 and fx == []

def test_twin_restart_starts_new_epoch():
    s = fresh_state()
    s, _ = step(s, ReadingReceived(make_observation(seq=5000, sim_time=T0 + 500)))
    s, fx = step(s, ReadingReceived(make_observation(seq=3, sim_time=T0, reading_id="r-z1-wl1-3", sig_hash="new")))
    assert s.epoch == 1 and s.counters["DUPLICATE"] == 0
    assert any(e.event.get("data", {}).get("reason") == "epoch_changed" for e in fx if isinstance(e, Publish))

def test_small_reorder_is_out_of_order_not_new_epoch():
    ...  # seq 500 then seq 450 → OUT_OF_ORDER, epoch unchanged
```

`tests/test_core_purity.py`: parse every file in `src/city_brain/core/` with `ast`; fail on imports of `asyncio`, `time`, `random`, `uuid`, `city_brain.adapters`, `city_brain.runtime`, on calls to `datetime.now`/`utcnow`, and on string literals matching `twin\.sensors|set_valve|set_signal_plan|switch_grid|\bz\d-|zone-\d|meters`.

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** `pytest tests/core tests/test_core_purity.py -v` → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: core step skeleton with epochs and dedupe"`

---

### Task 10: Water filter (estimator)

**Files:**
- Create: `brain/src/city_brain/core/estimator.py`
- Modify: `core/state.py` (add `filters: dict[str, WaterFilter]`), `core/step.py` (update the filter for `WATER_LEVEL` observations after dedupe)
- Test: `brain/tests/core/test_estimator.py`

**Interfaces:**
- Produces `WaterFilter(params: Mapping[str, float])` with `update(value: float, tick: int, weight: float, u: float, at_ceiling: bool = False) -> None`, read-only properties `level`, `inflow`, `rate` (= `inflow - b * u_current`), `var_level`, `var_rate`, `last_tick`, `flags: frozenset[str]` (`STUCK`, `IMPLAUSIBLE_JUMP`, `MODEL_MISMATCH`, `CEILING` mirrored from the observation flag `AT_CEILING`), `is_stale(global_tick: int) -> bool` (no weighted update for `stale_s / sample_period_s` ticks), and `observed_slope(since_tick: int) -> float | None` — least-squares slope (m/s) over the **measurements** with weight > 0 since `since_tick` (ring buffer of the last 600 accepted `(tick, value)`), `None` if fewer than 10 points. Verification uses `observed_slope`, never the model's `rate`, so a valve that ignores commands cannot be "verified" by the model's own prediction.
- `step` wiring in this task: weight 1.0 when `device_status` is `OK`, else 0.0; Task 15 replaces this with `estimate_weight`. On a new epoch clear `filters` (spec §9.3).
- Gating reset: after 5 consecutive gated innovations, re-initialise `level` to the latest measurement and `inflow` to `observed_slope + b * u`, and set `MODEL_MISMATCH` (cleared after 50 ungated updates). A single spike is ignored; a sustained disagreement means the model (or the actuator) is wrong.
- Algorithm (state `x = [level, inflow]`, `dt = sample_period_s * (tick - last_tick)`):

```
predict: level += (inflow - b*u) * dt ; P = F P F^T + Q·(tick-last_tick),  F = [[1, dt],[0, 1]], Q = diag(q_level, q_inflow)
if weight > 0:
    S = P[0,0] + r0 / weight**2 ; innovation = value - level
    if |innovation| > jump_sigma * sqrt(S): add IMPLAUSIBLE_JUMP, skip update
    else: K = P[:,0] / S ; x += K*innovation ; P = (I - K e0^T) P
STUCK: value identical to previous for >= stuck_ticks consecutive readings while |rate| > 0.005
```

Initialise `level` from the first weighted reading, `inflow = 0`, `P = diag(1e-2, 1e-4)`.

- [ ] **Step 1: Write the failing tests** (synthetic series generated in the test with the Twin's water equations and `random.Random(1)`, noise σ 0.002 m per tick, values rounded to 2 dp):

```python
def test_closed_valve_rate_converges_to_inflow():
    f = run_series(seconds=20, ratio=0.0)        # true inflow 0.015 m/s
    assert f.rate == pytest.approx(0.015, abs=0.003)

def test_open_valve_rate_is_negative_with_known_u():
    f = run_series(seconds=20, ratio=0.0, then_ratio=1.0, switch_at_s=10)
    assert f.rate == pytest.approx(-0.030, abs=0.004)

def test_zero_weight_does_not_update_and_variance_grows():
    f = run_series(seconds=5, ratio=0.0); v0 = f.var_level; level0 = f.level
    f.update(value=11.0, tick=f.last_tick + 1, weight=0.0, u=0.0)
    assert f.var_level > v0 and f.level == pytest.approx(level0 + 0.015 * 0.1, abs=0.002)

def test_jump_flagged_and_ignored():
    f = run_series(seconds=10, ratio=0.0); lvl = f.level
    f.update(value=lvl + 1.0, tick=f.last_tick + 1, weight=1.0, u=0.0)
    assert "IMPLAUSIBLE_JUMP" in f.flags and f.level == pytest.approx(lvl, abs=0.01)

def test_model_mismatch_resets_to_measurements():
    # filter told u=1.0 (valve open) but the true valve stays closed (actuator attack)
    f = run_series(seconds=10, ratio=0.0, commanded_u_from_s=5, commanded_u=1.0)
    assert "MODEL_MISMATCH" in f.flags
    assert f.observed_slope(since_tick=f.last_tick - 30) == pytest.approx(0.015, abs=0.005)

def test_mock_stream_rate_matches_twin():
    f = filter_from_mock("z1-wl1", seconds=60)    # MockTwin readings fed to a WaterFilter, weight 1.0, u 0.0
    assert f.rate == pytest.approx(0.015, abs=0.004)
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** with numpy 2×2 arrays. **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: water Kalman filter with control input and fault flags"`

---

### Task 11: Verdict ledger and trust requirements

**Files:**
- Create: `brain/src/city_brain/core/trust.py`
- Modify: `core/state.py` (add `ledger: VerdictLedger`), `core/step.py` (handle `VerdictReceived`)
- Test: `brain/tests/core/test_trust.py`

**Interfaces:**
- Produces `VerdictLedger` with `add(verdict: Verdict) -> list[tuple[str, str]]` (returns reading keys whose trust changed), `lookup(reading_id: str, sig_hash: str, now: datetime) -> Verdict | None` (None if absent or `now > expires_at`), `prune(now: datetime, keep_s: float = 120.0)`, `clear()`. Keeps the latest verdict per key by `issued_at`. Verdicts for unseen readings are kept (parked) until pruned.
- Produces `trust_weight(verdict: Verdict | None, device_status: DeviceStatus, caps: Mapping[str, float]) -> float` = 0 if device status is not `OK`, else `min(verdict.trust if verdict.trust is not None else 1.0, caps[state])`, 0 when no verdict.
- Produces `estimate_weight(verdict: Verdict | None, device_status: DeviceStatus, caps: Mapping[str, float], pending_weight: float) -> float`, the weight the **filter** uses: 0 if device status is not `OK`; `pending_weight` when no verdict has arrived yet (the reading is OBSERVED, not UNKNOWN, spec §7.2); otherwise `trust_weight`. `trust_weight` stays the weight recorded on evidence, so a reading without a verdict still counts for nothing at the trust gate. See correction C7.
- Produces `evidence_ref(obs: Observation, verdict: Verdict | None, weight: float) -> EvidenceRef` with `state_kind = VALIDATED if verdict and state in (TRUSTED, DOUBTFUL) else OBSERVED if verdict is None else UNKNOWN`.
- Produces `meets_requirement(evidence: Sequence[EvidenceRef], risk: RiskClass) -> tuple[bool, tuple[str, ...]]`, reasons from `TRUST_INSUFFICIENT`, `VERDICT_MISSING`. Rules in Global Constraints. R0 always passes.
- On a new epoch `step` calls `ledger.clear()` (spec §9.3).
- `step` on `VerdictReceived`: `ledger.add`; if `verdict.reading_id`'s observation already arrived, nothing else here (Task 15 re-evaluates pending actions).

- [ ] **Step 1: Write the failing tests**

```python
def test_verdict_before_reading_is_applied():
    led = VerdictLedger(); v = make_verdict(reading_id="r-1", sig_hash="h")
    led.add(v)
    assert led.lookup("r-1", "h", now=v.issued_at) == v

def test_wrong_sig_hash_does_not_match():
    led = VerdictLedger(); led.add(make_verdict(reading_id="r-1", sig_hash="h"))
    assert led.lookup("r-1", "other", now=T0) is None

def test_expired_verdict_is_none():
    v = make_verdict(expires_at=T0 + timedelta(seconds=30)); led = VerdictLedger(); led.add(v)
    assert led.lookup(v.reading_id, v.sig_hash, now=T0 + timedelta(seconds=31)) is None

def test_contained_device_has_zero_weight():
    assert trust_weight(make_verdict(state=TrustState.TRUSTED), DeviceStatus.QUARANTINED, CAPS) == 0.0

def test_filter_weight_while_verdict_pending():
    assert estimate_weight(None, DeviceStatus.OK, CAPS, 0.5) == 0.5
    assert estimate_weight(None, DeviceStatus.QUARANTINED, CAPS, 0.5) == 0.0
    assert estimate_weight(make_verdict(state=TrustState.UNTRUSTED), DeviceStatus.OK, CAPS, 0.5) == 0.0

def test_r1_accepts_doubtful_with_one_trusted_r2_does_not():
    ev = [ref(TrustState.TRUSTED), ref(TrustState.DOUBTFUL)]
    assert meets_requirement(ev, RiskClass.R1)[0] is True
    assert meets_requirement(ev, RiskClass.R2) == (False, ("TRUST_INSUFFICIENT",))

@given(st.lists(st.sampled_from(list(TrustState)), min_size=1, max_size=5))
def test_property_no_r2_with_untrusted_or_unknown(states):
    ok, _ = meets_requirement([ref(s) for s in states], RiskClass.R2)
    assert not ok or all(s is TrustState.TRUSTED for s in states)
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: verdict ledger and risk-class trust requirements"`

---

### Task 12: T0 water planner

**Files:**
- Create: `brain/src/city_brain/core/water.py`
- Test: `brain/tests/core/test_water.py`

**Interfaces:**
- Consumes: `WaterFilter`, `EvidenceRef`, `ActionIntent`, `action_id`.
- Produces `WaterContext(device_id: str, zone_id: str, filter: WaterFilter, valve_state: str, recent: Sequence[EvidenceRef], below_low_since: datetime | None, now: datetime, plan_version: int)` and `plan_water_t0(ctx: WaterContext, policy: Mapping) -> ActionIntent | None`.
- Rules: if `valve_state` is `closed` and (`filter.level >= high_band` or `CEILING` in flags) → intent `open`, R1, expected effect `ExpectedEffect(channel=device_id, metric="rate", comparator="<=", threshold=open_rate_max, window_s=open_window_s)`. If `valve_state` is not `closed` and `filter.level <= low_band` and `now - below_low_since >= close_sustain_s` → intent `closed`, R2, expected `rate >= close_rate_min` within `close_window_s`. Never plan on a channel whose filter is stale, `STUCK`, or whose last reading has `IMPLAUSIBLE_JUMP`. `incident_id = f"flood-risk:{device_id}:{plan_version}"`. Evidence = last `evidence_count` refs. Alternatives: for open, `Alternative("hold", "level at or above high band and rising")`; for close, `Alternative("keep open", "level below low band for close_sustain_s")`.
- Also produces `water_hold_reason(ctx, policy) -> str | None` (`STALE`, `STUCK`, `IMPLAUSIBLE_JUMP`) used by Task 15 for hold records.

- [ ] **Step 1: Write the failing tests**

```python
def test_opens_at_high_band():
    i = plan_water_t0(ctx(level=6.01, valve="closed"), POLICY)
    assert i.state == "open" and i.risk_class is RiskClass.R1 and i.expected_effect.threshold == -0.01

def test_no_open_below_high_band():
    assert plan_water_t0(ctx(level=5.99, valve="closed"), POLICY) is None

def test_ceiling_with_closed_valve_opens():
    assert plan_water_t0(ctx(level=12.0, valve="closed", flags={"CEILING"}), POLICY).state == "open"

def test_close_needs_sustained_low():
    assert plan_water_t0(ctx(level=2.9, valve="open", below_for_s=4.9), POLICY) is None
    assert plan_water_t0(ctx(level=2.9, valve="open", below_for_s=5.0), POLICY).risk_class is RiskClass.R2

def test_stuck_channel_never_planned():
    assert plan_water_t0(ctx(level=8.0, valve="closed", flags={"STUCK"}), POLICY) is None
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: T0 water planner with hysteresis"`

---

### Task 13: Shield (static checks)

**Files:**
- Create: `brain/src/city_brain/core/shield.py`
- Test: `brain/tests/core/test_shield.py`

**Interfaces:**
- Produces `ShieldContext(registry: Mapping[str, DeviceProfile], in_flight: set[str], cycle_intents: Sequence[ActionIntent], last_command_at: Mapping[str, datetime], bucket: TokenBucket, suspect: set[str], now: datetime)`, `CheckOutcome(name: str, passed: bool, code: str | None)`, `ShieldResult(passed: bool, refusals: tuple[str, ...], checks: tuple[CheckOutcome, ...])`, `evaluate(intent: ActionIntent, ctx: ShieldContext, allowlist: Mapping[str, frozenset[str]], policy: Mapping) -> ShieldResult`.
- Produces `TokenBucket(burst: int, per_s: float)` with `take(now: datetime) -> bool` and `peek(now) -> bool`, refilled on sim time (deterministic). `evaluate` only peeks; the coordinator takes a token at dispatch.
- Checks, all run, in order (codes from spec §8.6): 1 `STATE_NOT_ALLOWED` (canonical state not in the profile allowlist; `ratio:N` allowed for 0 ≤ N ≤ 100), 2 `UNKNOWN_TARGET`, 2b `ACTUATOR_SUSPECT` (target in `suspect`), 3 `TARGET_CONTAINED` (registry status ≠ `OK`), 4 `NO_OP` (target's `current_state == intent.state`), 5 `IN_FLIGHT`, 6 `CONFLICT` (another intent this cycle on the same target with a different state), 7 `COOLDOWN` (`now - last_command_at[target] < cooldown_s[capability]`), 8 `RATE_LIMIT`.

- [ ] **Step 1: Write the failing tests** — one test per code, each asserting `passed is False` and the code present, plus:

```python
def test_contained_target_refused():
    r = evaluate(open_intent("z2-wl1"), ctx(status={"z2-wl1": DeviceStatus.QUARANTINED}), ALLOW, POLICY)
    assert not r.passed and "TARGET_CONTAINED" in r.refusals

def test_all_checks_reported_even_after_failure():
    r = evaluate(intent(state="half"), ctx(in_flight={"z1-wl1"}), ALLOW, POLICY)
    assert {"STATE_NOT_ALLOWED", "IN_FLIGHT"} <= set(r.refusals) and len(r.checks) == 9

def test_burst_of_eight_then_rate_limited():
    b = TokenBucket(burst=8, per_s=2.0)
    assert [b.take(T0) for _ in range(9)] == [True] * 8 + [False]
    assert b.take(T0 + timedelta(seconds=0.5))
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: shield static checks and sim-time token bucket"`

---

### Task 14: Brain event records

**Files:**
- Create: `brain/src/city_brain/core/records.py`, `brain/schemas/brain-event.v1.schema.json`
- Modify: `core/step.py` (replace the placeholder status dict from Task 9)
- Test: `brain/tests/core/test_records.py`

**Interfaces:**
- Produces builders returning CloudEvents dicts (no `time` field): `status_event(state, reason: str, data: dict) -> dict`, `decision_event(state, intent: ActionIntent | None, outcome: Literal["act","hold","notify","dismiss"], incident: dict, rule_or_plan: str, evidence, alternatives, rationale: str, would_change_if: Sequence[str]) -> dict`, `command_event(state, intent, shield: ShieldResult, status: ActionStatus, tool_call: dict | None, twin_reply: dict | None, reasons: Sequence[str]) -> dict`, `verification_event(state, action_id: str, expected: ExpectedEffect, observed: dict, verdict: Literal["verified","unverified","inconclusive"]) -> dict`, `alert_event(state, audience: Literal["operator","guardian"], severity: str, code: str, message: str, refs: dict) -> dict`.
- Envelope: `specversion "1.0"`, `id f"{epoch}-{event_seq}"` (increment `state.event_seq`), `source "urn:tsyp14:triplet-07:brain"`, `type "tn.tsyp.brain.<kind>.v1"`, `subject` (decision and status: zone id or `city`; command, verification and alert: target device id), extensions `simtime`, `epoch`, `correlationid`, `actionid`, `schemaversion "1"`. Field names in `data` exactly as the spec §5.4 table and the nine-question table.

- [ ] **Step 1: Write the failing tests**

```python
def test_every_builder_validates_against_schema():
    for ev in sample_events():              # one of each builder
        jsonschema.validate(ev, BRAIN_EVENT_SCHEMA)
        assert "time" not in ev

NINE = ["incident", "evidence", "rule_or_plan", "alternatives", "rationale"]
def test_decision_answers_explainability_questions():
    d = sample_decision()["data"]
    for f in NINE: assert d[f] not in (None, "", [], {})
    for e in d["evidence"]: assert {"reading_id", "sig_hash", "state_kind", "verdict_id", "trust_state", "weight"} <= e.keys()
    for a in d["alternatives"]: assert a["rejected_because"]

def test_event_ids_are_sequential_per_epoch():
    s = fresh_state(); a = status_event(s, "startup", {}); b = status_event(s, "x", {})
    assert (a["id"], b["id"]) == ("0-0", "0-1")
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement** builders and the JSON Schema (2020-12; `data` schema per type with `oneOf` on `type`). **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: brain-event v1 builders and schema"`

---

### Task 15: Coordinator, lifecycle and verification

**Files:**
- Create: `brain/src/city_brain/core/coordinator.py`
- Modify: `core/state.py` (add `actions: dict[str, ActionRecord]`, `in_flight: set[str]`, `suspect: set[str]`, `last_command_at`, `bucket`, `below_low_since: dict[str, datetime | None]`, `last_hold: dict[str, str | None]`, `plan_version: dict[str, int]`), `core/step.py` (wire planner → shield → trust → effects; route `VerdictReceived`, `CommandResult`, `TimerFired`)
- Test: `brain/tests/core/test_coordinator.py`, `brain/tests/core/test_step_water_loop.py`

**Interfaces:**
- Produces `ActionRecord(intent: ActionIntent, status: ActionStatus, key: str, dispatched_at: datetime | None, reasons: tuple[str, ...])` and functions used by `step`:
  - `propose(state, intent) -> list[Effect]`: Shield; refused → `REFUSED` + `command_event`; else re-check trust via ledger for each evidence ref → if every ref has a verdict: `meets_requirement` → `dispatch` or `BLOCKED`; else `AWAITING_TRUST` + `StartTimer(f"verdict:{action_id}", "verdict_wait", verdict_wait_s)`. Always also a `decision_event` (outcome `act`).
  - `on_verdict(state, reading_key) -> list[Effect]`: re-evaluate `AWAITING_TRUST` actions citing that reading; if decided, `CancelTimer`.
  - `on_timer(state, TimerFired) -> list[Effect]`: verdict wait expired → refs without verdict become UNKNOWN → `BLOCKED` with `VERDICT_TIMEOUT`.
  - `dispatch(state, record) -> list[Effect]`: re-run `meets_requirement` (dispatch-time check), take a bucket token, set `in_flight`, `last_command_at`, emit `SendCommand` and `command_event(status=DISPATCHED)`.
  - `on_result(state, CommandResult) -> list[Effect]`: `ACCEPTED` → `VERIFYING`, set registry `current_state = intent.state`, `dispatched_at = state.sim_now`; `REJECTED` → `REJECTED`, clear in-flight; `UNKNOWN` → `UNKNOWN`, clear in-flight, `alert_event(operator, "COMMAND_RESULT_UNKNOWN")`. No resend in any branch.
  - `on_estimate(state, device_id) -> list[Effect]`: for `VERIFYING` actions on that channel with `sim_now - dispatched_at >= window_s`: compare `filter.observed_slope(since_tick=<tick at dispatch + 5>)` (skip the first 0.5 s while the valve takes effect) with the expected effect → `VERIFIED` or `UNVERIFIED` (`inconclusive` if the slope is `None`, treated as `UNVERIFIED` for alerts); put both expected and observed slope in the `verification` event; `UNVERIFIED` adds target to `suspect` and emits `alert_event(operator, "ACTUATOR_NO_EFFECT")` and `alert_event(guardian, "ACTUATOR_NO_EFFECT")`. Clear in-flight either way.
- `step` on each `WATER_LEVEL` observation: update the filter with `estimate_weight` (`pending_weight` from `policy.trust`), so readings are estimated on arrival even though their verdict usually lands just after them; then build `WaterContext` (evidence refs via ledger + `trust_weight`; `u` = commanded ratio of the registry's current state: open 1.0, closed 0.0, `ratio:N` N/100), call `on_estimate`, then `plan_water_t0`; if an intent → `propose`; if no intent and `water_hold_reason` changed for that device → `decision_event(outcome="hold", would_change_if=[...])`.

- On a new epoch also clear `in_flight`, `below_low_since`, `last_hold` and the recent-evidence buffers; `actions` keeps its history.

- [ ] **Step 1: Write the failing tests** (drive `step` with factories; assert on emitted effects and record status)

```python
def test_happy_path_open_then_verified():
    s = state_with_registry()
    s = feed_rising_channel(s, "z1-wl1", until_level=6.05, verdict=TrustState.TRUSTED)
    send = only(SendCommand, s.last_effects)
    assert (send.target, send.state) == ("z1-wl1", "open")
    s, _ = step(s, CommandResult(send.action_id, "ACCEPTED", {"status": "ok"}))
    s = feed_falling_channel(s, "z1-wl1", seconds=3.5)
    assert s.actions[send.action_id].status is ActionStatus.VERIFIED

def test_actuator_noop_is_unverified_with_two_alerts_and_no_resend():
    ... # ACCEPTED but level keeps rising 3.5 s → UNVERIFIED, alerts to operator and guardian,
        # target in suspect, and no further SendCommand for z1-wl1 over the next 30 s

def test_missing_verdict_times_out_to_blocked():
    ... # no verdicts → AWAITING_TRUST + StartTimer; TimerFired → BLOCKED with VERDICT_TIMEOUT; no SendCommand

def test_timeout_is_unknown_and_never_resent():
    ... # CommandResult UNKNOWN → status UNKNOWN, alert, and no SendCommand with the same key ever again

def test_downgrade_between_decision_and_dispatch_blocks():
    ... # verdicts arrive TRUSTED for 2 refs, then a newer DOUBTFUL for one before the 3rd arrives (R2 close) → BLOCKED

def test_estimate_tracks_when_each_verdict_follows_its_reading():
    ... # reading, then its verdict, every tick → level crosses the band, intent goes AWAITING_TRUST on the
        # newest cited reading, its verdict arrives, SendCommand follows

def test_hold_record_emitted_once_per_reason_change():
    ... # STUCK channel above high band → exactly one hold decision with reason STUCK over 10 s of readings
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** `pytest tests/core -v` → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: action lifecycle, trust gate and effect verification"`

---

### Task 16: Ports and in-process Twin

**Files:**
- Create: `brain/src/city_brain/ports.py`, `brain/src/city_brain/adapters/__init__.py`, `brain/src/city_brain/adapters/inproc_twin.py`
- Test: `brain/tests/adapters/test_inproc_twin.py`

`McpTwinClient` moves to P4. `ports.py` stays here: `InProcTwin` and `BrainApp` both need it.

**Interfaces:**
- Produces in `ports.py` (`typing.Protocol`): `TwinPort` with `async load_registry() -> tuple[list[dict], dict[str, str], dict]` (devices, keys, city_state), `async call(tool_call: ToolCall, idempotency_key: str) -> CommandOutcome`, `async get_result(idempotency_key: str) -> dict | None`. `CommandOutcome(outcome: Literal["ACCEPTED","REJECTED","UNKNOWN"], reply: dict)`.
- Produces `InProcTwin(twin: MockTwin)` implementing `TwinPort` by calling `twin.handle_tool` directly. `call` adds `idempotency_key` to args; reply `status == "ok"` → ACCEPTED, `"error"` → REJECTED, any exception from the twin → UNKNOWN with `{"error": "<type>"}`.

- [ ] **Step 1: Write the failing tests**

```python
async def test_load_registry_returns_42_devices_and_keys():
    devices, keys, city = await InProcTwin(MockTwin()).load_registry()
    assert len(devices) == 42 and set(keys) == {d["device_id"] for d in devices}

async def test_set_valve_accepted():
    out = await InProcTwin(MockTwin()).call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"}), "brain-0-act-x")
    assert out.outcome == "ACCEPTED"

async def test_unknown_device_is_rejected():
    out = await InProcTwin(MockTwin()).call(ToolCall("set_valve", {"device_id": "nope", "target_state": "open"}), "k2")
    assert out.outcome == "REJECTED"

async def test_transport_error_is_unknown():
    out = await InProcTwin(ExplodingTwin()).call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"}), "k3")
    assert out.outcome == "UNKNOWN" and out.reply == {"error": "RuntimeError"}

async def test_result_is_cached_by_key():
    p = InProcTwin(MockTwin()); await p.call(ToolCall("set_valve", {"device_id": "z1-wl1", "target_state": "open"}), "k4")
    assert (await p.get_result("k4"))["status"] == "ok"
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -m "brain: Twin port and in-process adapter"`

---

### Task 17: Runtime, clocks, JSONL log and standalone scenarios

**Files:**
- Create: `brain/src/city_brain/adapters/memory/bus.py`, `brain/src/city_brain/adapters/files/jsonl_log.py`, `brain/src/city_brain/runtime/clocks.py`, `brain/src/city_brain/runtime/app.py`, `brain/src/city_brain/runtime/main.py`, `brain/scenarios/__init__.py`, `brain/scenarios/runner.py`, `brain/scenarios/T02.yaml`, `brain/scenarios/T05.yaml`, `brain/tests/scenarios/helpers.py`
- Modify: `brain/Makefile` (targets `test`, `scenario`, `standalone`)
- Test: `brain/tests/adapters/test_jsonl_log.py`, `brain/tests/scenarios/test_t02_t05_standalone.py`, `brain/tests/scenarios/test_determinism.py`

**Interfaces:**
- Produces `JsonlLog(path: Path)` with `append(event: dict) -> dict` (adds `time` = wall clock ISO-8601, `prev_hash`, `hash` = SHA-256 of the canonical JSON of the event without `hash`), and `verify_chain(path: Path) -> bool`.
- Produces clocks implementing `Clock` protocol (`start(timer_id, kind, wall_seconds)`, `cancel(timer_id)`): `RealClock(queue)` (asyncio `call_later` → `TimerFired` into the queue) and `SimClock()` with `advance(seconds) -> list[TimerFired]` (deterministic).
- Produces `BrainApp(config, twin: TwinPort, clock: Clock, log: JsonlLog, reading_guard, verdict_guard)` with `async start()` (load registry via `twin.load_registry`, fill the key ring, feed `RegistryLoaded`), `async reload_registry()` (same, called when `BAD_SIGNATURE` rejects come from ≥ 5 distinct devices within one second of wall time, at most once every 10 s — this is how a Twin restart with new keys is recovered; after reload the core's epoch check takes over), `async submit_raw(stream: Literal["readings", "verdicts"], raw: bytes) -> list[Effect]` (async because `twin.call` is; `stream` is a logical name, topic names belong to the P4 Kafka adapter; guard → event → `step` → effects handled: `Publish` → log; `SendCommand` → map with `profile.to_tool_call` → `twin.call` → `CommandResult` fed back; `StartTimer`/`CancelTimer` → clock).
- Produces `scenarios/runner.py run(name: str, profile: Literal["standalone","triplet"], logs_dir: Path | None = None) -> Path` (returns the log path; `None` uses `config.logs_dir`; tests always pass `tmp_path`, so nothing lands in the root `logs/`). Standalone: one process, `MockTwin` + `MockGuardian(policy=trust_all())` + `InProcTwin` + `SimClock`, all keys derived from the seed, the Guardian public key passed through `GuardianProfile.load(..., public_key_b64=...)`; on each mock tick submit each reading followed by its verdict, then `SimClock.advance(0.1)` and feed the returned `TimerFired`. `profile="triplet"` raises `NotImplementedError("triplet wiring lands in P4")`. Scenario YAML: `duration_s`, `inject: [{at_s, capability, target, params}]`, `expect` block used by tests. Log name `logs/<ID>-e<epoch>-<UTC start>.jsonl`.
- `T02.yaml`: 300 s, no injection. `T05.yaml`: 300 s, `inject: [{at_s: 0, capability: actuator, target: z4-wl1}]`.
- `python -m city_brain --config config/standalone.yaml --scenario T02` runs a scenario; `--config config/triplet.yaml` exits with code 2 and `triplet mode lands in P4`.
- `Makefile`: `make scenario name=T02 profile=standalone` calls `$(PY) -m scenarios.runner --name $(name) --profile $(profile)` and prints the log path.

- [ ] **Step 1: Write the failing tests**

```python
def test_chain_detects_tampering(tmp_path):
    log = JsonlLog(tmp_path / "a.jsonl"); [log.append({"n": i}) for i in range(3)]
    assert verify_chain(tmp_path / "a.jsonl")
    edit_line(tmp_path / "a.jsonl", 1, {"n": 99})
    assert not verify_chain(tmp_path / "a.jsonl")

def test_t02_valve_opened_before_6m_and_verified():
    events = read_log(run("T02", "standalone"))
    cmd = first(events, type="tn.tsyp.brain.command.v1", target="z1-wl1", state="open", status="DISPATCHED")
    assert max_level_before(events, "z1-wl1", cmd["simtime"]) < 6.05
    assert first(events, type="tn.tsyp.brain.verification.v1", actionid=cmd["actionid"])["data"]["verdict"] == "verified"
    assert all(e["data"]["status"] != "REFUSED" or e["data"]["reasons"] for e in commands(events))

def test_t05_faulty_valve_unverified_alerted_not_resent():
    events = read_log(run("T05", "standalone"))
    assert verdicts_for(events, "z4-wl1") == ["unverified"]
    assert count(events, type="tn.tsyp.brain.alert.v1", code="ACTUATOR_NO_EFFECT") == 2
    assert count(events, type="tn.tsyp.brain.command.v1", target="z4-wl1", status="DISPATCHED") == 1

def test_twin_restart_reloads_keys_and_continues():
    # scenario helper: run 200 s, call twin.restart() (new keys, seq/ts reset), run 250 s more
    events = read_log(run_with_restart("T02", restart_at_s=200, total_s=450))
    assert any(e["type"].endswith("status.v1") and e["data"]["reason"] == "epoch_changed" for e in events)
    later = [e for e in events if e["epoch"] == 1]
    assert any(e["type"].endswith("command.v1") and e["data"]["status"] == "DISPATCHED" for e in later)

def test_triplet_profile_is_a_clear_stub():
    with pytest.raises(NotImplementedError, match="P4"):
        run("T02", "triplet")

def test_standalone_run_is_deterministic():
    a, b = read_log(run("T02", "standalone")), read_log(run("T02", "standalone"))
    strip = lambda es: [{k: v for k, v in e.items() if k not in ("time", "hash", "prev_hash")} for e in es]
    assert strip(a) == strip(b)
```

- [ ] **Step 2: Run** → FAIL. **Step 3: Implement.** **Step 4: Run** `.venv/Scripts/python -m pytest -v` (whole suite) → PASS; `mingw32-make scenario name=T02 profile=standalone` prints the log path.
- [ ] **Step 5: Commit** `git commit -m "brain: runtime, clocks, hash-chained log, standalone T02 and T05"`

---

## P1 exit check

- `.venv/Scripts/python -m pytest` (unit, property, contract, scenario) green with no Twin, broker or network; partner-marked tests skip with the summary line.
- `mingw32-make scenario name=T02 profile=standalone` writes a log where the valve opens before the high band and the action is `verified`; `name=T05` gives `unverified`, 2 alerts and no resend; two T02 runs are byte-identical apart from `time` and hash fields.
- `git diff main --stat` touches nothing outside `brain/`.
