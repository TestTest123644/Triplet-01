# City Brain — Final Implementation Plan (Twin-in-development edition)

TSYP14 · Triplet 07 · City Brain team · 6 November 2026

Supersedes `2026-11-05-city-brain-roadmap.md`. Keeps `CITY_BRAIN_ARCHITECTURE.md` v2.1 as the design of record. The P1 task list is `brain/docs/plans/2026-11-06-city-brain-p1-standalone.md`, which applies §4.1 and corrects its ordering defects.

---

## 0. Why this plan differs from the roadmap

The previous roadmap treated the Twin as a fixed thing: it pinned facts to commit `418a024`, made recorded Twin fixtures a prerequisite of Task 4, and put interoperability proof at P7 and the timeline at P3.

The Twin is still being developed. Field names, tool arguments, accepted actuator states, physics constants, topic names and key handling can all move under us, and the Guardian folder is still empty. Three consequences:

1. **Nothing may be a prerequisite that only the live Twin can supply.** Anything blocking on `docker compose up` blocks the whole team on another team's schedule.
2. **A recorded Twin stream is evidence of what the Twin did on a date, never a contract.** The contract is the profile file, and the profile has to be *checkable* against whatever the Twin is today.
3. **The scored requirements are all things we own.** R1 (runs alone with mocks), R2 (one trigger, one log), R3 (live timeline), the Interface Card, the Test Book, the security measures — none of them need a working partner. They need our mocks to be good. So they come first.

The reordering below means the Brain reaches full spec compliance and its full scoring surface **before the Twin is ever required**, and that connecting to the real Twin (or a different one in Phase 2) becomes a half-hour certification procedure rather than a milestone.

---

## 1. Governing rules under a moving partner

These hold for every plan below. They are the engineering argument we make in the report and the pitch.

| # | Rule | Enforced by |
|---|---|---|
| G1 | **The profile is the contract; the Twin is an observation of it.** Every partner-specific fact lives in `profiles/*.yaml`. The core never learns a Twin fact. | `tests/test_core_purity.py` (P1 Task 7) |
| G2 | **Every profile records when and against what it was observed.** `observed: {commit, date, by}` block, required by the profile schema. | `tests/edge/test_profiles.py` |
| G3 | **The mock is built from the profile, not from the Twin.** If the profile says a valve accepts `open/closed/NN%`, the mock accepts exactly that. A profile change changes the mock for free. | `tests/mocks/test_mock_matches_profile.py` (P1) |
| G4 | **Partner drift is detected by a kit, not by a human reading a diff.** `make certify-twin` / `make certify-guardian` produce a dated report naming every mismatch. | `conformance/` (P4) |
| G5 | **Drift is fixed in the profile and the mock. Never in `core/`.** If a Twin change appears to require a core change, stop and report it — it means a design assumption was wrong. | Review rule; P-plan exit checks |
| G6 | **Every scenario runs in both profiles.** `make scenario name=T03 profile=standalone` always works; `profile=triplet` works when the Twin is up. The Test Book is complete either way. | P2 scenario runner |
| G7 | **Twin-dependent tests skip loudly, never fail silently.** `@pytest.mark.triplet` skips with "Twin not reachable at $BRAIN_TWIN_MCP" and the suite stays green. | `conftest.py` (P1) |
| G8 | **Recordings are dated artefacts, not fixtures.** `recordings/<date>-<commit>/` is git-ignored. One small gzipped `fixtures/sample-60s/` (< 5 MB) is committed for contract tests. | `.gitignore`, P4 |
| G9 | **The Guardian gets identical treatment.** It is a profile, a mock with scripted policies, and a conformance suite — not an assumption. | P3, P4 |

---

## 2. Requirement coverage

### 2.1 Spec rules R1–R4

| Req | What the spec asks | Where it is built | Needs the Twin? |
|---|---|---|---|
| **R1** | Layer runs alone with mocks of both partners; one-page `INTERFACE.md` with what it sends/receives, one example of each message, how to point it at a different partner through configuration (never hard-coded), and how it authenticates | Mocks: P1. Config and profiles: P1. `INTERFACE.md` v1: P2, final: P4 | No |
| **R2** | Every scenario starts with one trigger and leaves a log file in the repository | Scenario runner + hash-chained JSONL + `scenarios/*.sh` at repo root: P2 | No |
| **R3** | Live timeline screen showing, with timestamps, reading → Guardian verdict → Brain decision → actuator action across the three layers | Timeline service (FastAPI + SSE + one HTML page): P2 | No (reads mocks in standalone) |
| **R4** | Document how the triplet coordinated, how a partner failure or change was handled, evidence of a successful joint run | `COLLABORATION.md` kept from P1 onward; certification reports from the C-loop are the evidence of handling partner change | Joint run only |

R4 is the one place the real Twin is non-optional, and the certification loop (§5) produces exactly the artefact it asks for: a dated record of a partner change and how we absorbed it.

### 2.2 Scoring surface (50 pts, per team)

| Pts | Criterion | What earns it | Plan |
|---|---|---|---|
| 10 | Individual prototype functionality | Water control works end to end, verified, with explainable holds | P1 |
| 5 | Technical implementation & features | Kalman estimator, risk classes, effect verification, epochs, determinism | P1, P3 |
| **15** | Interoperability, connectivity & collaboration | Profile-driven anti-corruption layer, two transports with identical decisions (T13), conformance kit others can run, published AsyncAPI/OpenAPI, Interface Card | **P4** |
| **10** | Security & trust mechanisms | Ed25519 edge verification, signed verdicts checked, trust gate by risk class, Shield allowlists, signed Brain events, hash-chained logs, kill switch, R3 approvals | **P5** |
| 5 | Documentation & code quality | README, INTERFACE.md, ADRs, test pyramid, report part 2 | Throughout |
| 5 | Demonstration & presence | Standalone demo that cannot fail on venue Wi-Fi; timeline tells the story in 8 minutes | P2 + demo runbook §7 |

The two heaviest categories (15 + 10 = half the score) are P4 and P5. Both are entirely ours. That is why they moved ahead of the evidence model, persistence and MPC.

---

## 3. Plan sequence

| Plan | Builds | Exit test | Twin needed |
|---|---|---|---|
| **P1** Core slice, standalone | Everything in the existing P1 plan **minus** Kafka/MCP/real-Twin tasks, **plus** mock Guardian policies and the `conftest` skip machinery | `make scenario name=T02 profile=standalone` → valve opened before the high band, action `verified`; T05 → `unverified` + 2 alerts + no resend; two runs byte-identical apart from `time`/hashes | No |
| **P2** R2 + R3 evidence | Scenario runner for every T-scenario in both profiles; run summaries; hash-chain verifier; timeline service; root `scenarios/` triggers and `logs/`; `INTERFACE.md` v1; `TESTBOOK.md` | Every T-scenario runnable with one command in `standalone`; timeline shows the full chain live; `verify-chain` detects a tampered log | No |
| **P3** Trust, Guardian contract, modes | Verdict adapter behind a port; signed verdicts; heartbeat + watermark; containment events; expiry and revocation; modes WARMUP/NORMAL/GUARDIAN_DEGRADED/TWIN_DEGRADED/PAUSED; no-retroactive-trust; scripted Guardian mock (trust-all, delay, silent, flip, quarantine, expired, unsigned) | T03, T06, T10 pass on mocks; T09 reaches the refusal and the approval request (the approval itself is P5); property test: no R2 command ever cites non-TRUSTED evidence, over random event orders | No |
| **P4** Transports, profiles, conformance | Kafka source; MCP Twin client; REST ingest adapter; `twin-alt-example` profile; **conformance kit** for Twin and Guardian; AsyncAPI + OpenAPI; `INTERFACE.md` final | **T13**: the same scenario through Kafka, REST and the alt profile produces identical decision logs apart from transport metadata; conformance kit passes against our own mocks | No (kit runs against mocks; real Twin is the C-loop) |
| **P5** Security and operator controls | Ed25519-signed Brain events; Guardian signature verification enforced; operator approvals (R3) and kill switch; bearer-token auth on the query API; containment exception; log tamper test; secrets out of the repo | T09 passes with an approval; forged verdict rejected; tampered log detected; unauthenticated approval refused | No |
| **C** Partner certification loop | *Procedure, not a plan.* Record, certify, fix profile + mock, re-run everything, file a report | `make certify-twin` green; `pytest -m triplet` green; report filed | **Yes** |
| **P6** Evidence model and other domains | Generic channel filters for traffic/power/AQ/calls; detectors; log-odds evidence; couplings; labelled correlations; incident lifecycle; response catalog | T04, T07, T08, T12 pass; zero non-water commands in T01 | No |
| **P7** Persistence and recovery | SQLite journal (WAL), outbox, snapshots, restart replay, reconciliation via `get_command_result` / `get_device_state`; offsets committed after journalling | `kill -9` mid-run: no lost decision, no duplicate command | No |
| **P8** Depth | Effect-gain identification (RLS), horizon forecasts, enumerative water MPC, model-based verification; offline calibration; CP-SAT allocation | Valve changes per channel in T01 ≥ 50% fewer than bang-bang; levels in band | No |

**P1–P5 meets R1–R3 and is fully scoreable without the Twin ever existing.** The Test Book is complete only after P6: T04, T07, T08 and T12 run from P2 but pass from P6. The C-loop then connects it, repeatedly, as the Twin lands. P6–P8 deepen it without touching interfaces.

---

## 4. Plan detail

### 4.1 P1 — Core slice, standalone

**Applied in `brain/docs/plans/2026-11-06-city-brain-p1-standalone.md`, which also corrects these deltas (its table C1–C9) and is the version to execute.** The deltas as first written:

**Removed from P1** (they move to P4 and the C-loop):
- Task 4 (record real Twin fixtures) → P4 Step "sample fixture", and the C-loop
- Task 15 (MCP Twin client) → P4. `InProcTwin` from Task 15 **stays** in P1.
- Task 16 (Kafka source) → P4
- Task 18 (real-Twin run) → C-loop

**Reordered:** old Task 14 (mock Twin + mock Guardian) becomes **Task 4**, before the edge guard. The guard, the filter and every later test are then written against mock output, which is generated from the profile (G3). Renumber the rest.

**Added to P1:**

- **Task 4b — profile↔mock conformance.** `tests/mocks/test_mock_matches_profile.py`: for every actuator in `profiles/twin-triplet07.yaml`, the mock accepts exactly the listed states and rejects everything else except where `MockDefects` says otherwise; for every field mapping, a mock reading passes `ReadingGuard` with zero rejects; the mock's device inventory matches the profile's `registry` expectations in shape. This is the test that makes a profile edit propagate.
- **Task 4c — `observed` block.** Profile schema requires `observed: {commit, date, by, method}`. `method` is one of `code_read`, `recording`, `assumed`. Any field group marked `assumed` is listed by `make certify-twin` as unverified. The current `twin-triplet07.yaml` starts as `{commit: 418a024, date: 2026-11-04, by: city-brain-team, method: code_read}` — and §2 of the architecture is tagged accordingly.
- **Task 0 — `conftest.py` skip machinery.** Markers `triplet`, `kafka`, `twin_live`. Each skips with a message naming the env var and the endpoint it tried. `pytest` with nothing running must be green and must print a summary line of what was skipped.
- **Mock Guardian policies** (lifted from P2 into P1, since the mock is R1 evidence): `trust_all`, `delay(n_s)`, `silent`, `flip_after(n)`, `untrusted(device)`, `quarantine_at(device, t)`, `expired`, `unsigned`. P1 only needs `trust_all` wired into scenarios; the rest are implemented and unit-tested here so P3 only has to consume them.

**Unchanged:** Tasks 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 17 (standalone parts), including every test body already written.

**P1 exit:** as written in the existing plan, minus the Kafka and triplet markers.

### 4.2 P2 — R2 and R3 evidence

The point of this plan is that a jury can see cause and effect, and reproduce it, with nothing but our folder.

- **Scenario runner, both profiles.** `make scenario name=<ID> profile=<standalone|triplet>`. Standalone wires `MockTwin` + scripted `MockGuardian` + `InProcTwin` + `SimClock` in one process on the memory bus. Triplet wiring is stubbed and lands in P4. Scenario YAML: `duration_s`, `guardian_policy`, `inject: [{at_s, capability, target, params}]`, `expect` block consumed by tests.
- **Root triggers.** `scenarios/t01.sh` … one per Test Book entry, each a one-liner calling the Brain's make target, so R2's "one command or button" is satisfied at the triplet level and the other teams can call ours.
- **Logs.** `logs/<ID>-e<epoch>-<UTC start>.jsonl`, hash-chained, plus `…-summary.json` (counts per event type, refusals by code, verifications, mode changes, final hash, config hash, profile hash, parameter version). `make verify-chain file=…`.
- **Timeline service** (`brain/timeline/`): FastAPI + SSE + one static HTML page, own process, own consumer group. Three lanes (Twin, Guardian, Brain), one row per correlation chain, sim-time and wall-time stamps, click a decision for evidence/alternatives/"would change if", per-zone water panel with band and valve ratio, mode indicator. **In standalone it tails the JSONL log**, so R3 is demonstrable with no broker and no partner; in triplet mode it subscribes to Kafka. Same page either way.
- **`INTERFACE.md` v1** — the one-page Interface Card: what the Brain consumes and emits, one worked example of each message, the config keys that repoint it at a different partner (`twin.profile`, `twin.mcp_url`, `kafka.*`, `guardian.profile`, `guardian.public_key_file`), and how it authenticates (Ed25519 reading and verdict verification, signed Brain events, bearer token on the query API).
- **`TESTBOOK.md`** at the repo root in the spec's field format (§6 below).

**Exit:** every T-scenario in §6 runs standalone with one command and leaves a log; timeline shows reading → verdict → decision → command → verification live during a run; editing a log line makes `verify-chain` fail.

### 4.3 P3 — Trust, Guardian contract, modes

The Guardian folder is empty, so this plan is written against our proposed contract (architecture §5.3) and our scripted mock, with the adapter behind a port so a different transport is a new adapter and nothing else.

Contents as in the old P2, plus:

- **`VerdictPort`** with three interchangeable adapters: Kafka (P4), REST `POST /v1/verdicts` + `/v1/guardian-events`, and MCP client calling a Guardian `get_verdict(reading_id)` tool. P3 implements the REST and in-process ones; Kafka lands in P4. All three land on the same core event.
- **Guardian profile vocabulary mapping** (architecture §7.3) with `UNKNOWN_VERDICT_STATE` rejection at the edge, and an `observed` block like the Twin profile.
- **No-retroactive-trust**: readings received during a Guardian outage stay UNKNOWN unless explicitly verdicted later; beliefs built during the outage are re-scored at zero weight; NORMAL is re-entered only on fresh verdict-backed evidence.

**Exit:** T03, T06, T10 pass on mocks, T09 up to the approval request; the hypothesis property "no R2 or R3 command ever cites evidence that is not TRUSTED at both decision time and dispatch time" holds over randomised event orders including out-of-order verdicts, expiries and revocations.

### 4.4 P4 — Transports, profiles, conformance kit

This is the 15-point plan and the Twin-drift defence. All of it runs against our own mocks.

- **Kafka source** (manual commits, `auto.offset.reset=latest`, pause/resume backpressure) and **MCP Twin client** (single SSE session, serialised calls, 2 s timeout, backoff 0.5→8 s, `get_command_result` before any resend, `ok`→ACCEPTED / `error`→REJECTED / timeout→UNKNOWN).
- **REST ingest adapter** so readings can arrive over HTTP from a partner that has no broker.
- **`profiles/twin-alt-example.yaml`** — different field names (`reading`, `device_category: flood_sensor`), centimetres, a different actuator vocabulary — with a mock built from it (G3), so the alternative partner is real and testable, not hypothetical.
- **Conformance kit** (`brain/conformance/`), a standalone pytest package another team can run against their own layer:
  - *Twin suite*: reading schema; signature verifies with published keys; `seq` monotonic per device; `ts` cadence consistent with `seq`; tool list, argument names and accepted states; command result semantics; behaviour on an invalid target state; containment status values; idempotency-key behaviour.
  - *Guardian suite*: verdict schema and signature; binding to a real `reading_id` + `sig_hash`; heartbeat interval; containment event format; expiry expressed in sim time.
  - *Brain self-conformance*: both suites run against our own mocks in CI, which is what proves our mocks are honest — the R1 claim with evidence behind it.
- **Published contracts**: `contracts/asyncapi.yaml`, `contracts/openapi.json`, `schemas/*.schema.json`, and `INTERFACE.md` final linking to them.
- **Sample fixture**: `fixtures/sample-60s/` (gzipped, < 5 MB) from whatever the Twin is at the time, or from the mock if it is not up, clearly labelled which.

**Exit — T13, the interoperability proof:** run the same scenario four ways — Kafka + triplet profile, REST + triplet profile, in-process + triplet profile, Kafka + alt profile — and diff the decision logs. Identical apart from transport metadata. One core build, four partners. That is the claim the 15 points are for, and it is checked by a test rather than asserted in the report.

### 4.5 P5 — Security and operator controls

- Ed25519-signed Brain events; timeline verifies signatures and shows a badge.
- Guardian verdict signature verification enforced; unsigned or wrongly-signed verdicts → UNKNOWN, never TRUSTED. Forged-verdict test.
- Operator approvals for R3 and the containment exception (architecture §8.7), with the approval recorded and attributed.
- Kill switch: `POST /v1/control/pause` / `/resume`, token-authenticated; PAUSED allows R0 only.
- Bearer token from env on the query API; unauthenticated approval attempt refused and logged.
- Secrets: keys from env or mounted files, `.env` git-ignored, `.env.example` only; a test asserts no private key material under `brain/`.
- Log tamper test as the acceptance for the hash chain.
- Size limit 4 KB and fuzzed-input dead-lettering as a property test.

**Exit:** the security table in architecture §11.1 has a passing test per row, or an explicit "not applicable on this partner" line in `INTERFACE.md`.

---

## 5. The certification loop (C)

Run this the first time the Twin is reachable, and again **every time the Twin team changes anything**. It is also exactly the Phase 2 onboarding procedure for a partner we have never met, and it produces the R4 artefact.

```
make certify-twin TWIN_MCP=http://localhost:8000/sse KAFKA=localhost:19092
```

1. **Sample.** Record 60 s of telemetry plus `list_devices`, every `get_device_key`, `get_city_state` into `recordings/<date>-<commit>/` (git-ignored).
2. **Certify.** Run the Twin conformance suite against the live Twin and the recording. Output `conformance/reports/twin-<date>-<commit>.md`: one line per check, PASS / FAIL / UNVERIFIED, with the observed value next to the profile's value.
3. **Reconcile.** Every FAIL is fixed in `profiles/twin-triplet07.yaml` and, where needed, `mocks/twin/`. Bump the profile's `observed` block. **No file under `src/city_brain/core/` may change** (G5); if one seems to need to, stop and write the reason into the report — that is a design defect and the team decides, not the agent.
4. **Re-run everything.** `pytest` (green with no partner), then `pytest -m triplet` and `make scenario name=T02 profile=triplet`.
5. **File.** Commit the report and append a line to `COLLABORATION.md`: date, what changed on the Twin side, how many profile lines changed, how many core lines changed (target: zero), time to green.

That last line is the collaboration evidence R4 asks for, and "the Twin changed X and our layer absorbed it in N minutes with zero core changes" is the single strongest thing we can say in the pitch.

The same procedure with `make certify-guardian` applies the moment the Guardian ships anything.

---

## 6. Test Book

Every scenario runs `profile=standalone` from P2 onward and `profile=triplet` once the Twin is up. The spec requires the Test Book to cover all three layers and every interaction between them; the coverage column shows that it does.

| ID | Name | Covers | Trigger | Expected: Twin | Expected: Guardian | Expected: Brain | How to verify |
|---|---|---|---|---|---|---|---|
| T01 | Calm city, valves managed | Twin→Brain | `scenarios/t01.sh` | Normal physics, 15 min | All TRUSTED, heartbeat 1 s | Every water channel held in band; zero traffic/power/call commands; ≤ 2 valve changes per channel after settling | Summary: commands by capability; per-channel max level |
| T02 | Rising water controlled | Twin→Brain | `scenarios/t02.sh` | `z1-wl1` rises | TRUSTED | Valve commanded before the high band; `verification: verified`; level returns to band | Timeline chain reading→verdict→decision→command→verification |
| T03 | Fake flood (physical tamper) | all three | `scenarios/t03.sh` | Broadcasts value × 2.5 | DOUBTFUL/UNTRUSTED within 10 s | No command caused by the false level; `decision: hold` with `IMPLAUSIBLE_JUMP`; sibling channel still controlled | Log: no command citing that device; hold record present |
| T04 | Stolen key on power | all three | `scenarios/t04.sh` | Valid signature, implausible value | DOUBTFUL, `RATE_OF_CHANGE` | Zero `grid` commands; hold with reason; belief stays low | Log: zero grid commands |
| T05 | Faulty valve | Twin→Brain, Brain→Guardian | `scenarios/t05.sh` | Replies `ok`, valve unchanged | Receives the advisory | `verification: unverified`; operator alert; `ACTUATOR_NO_EFFECT` advisory to the Guardian; no automatic resend | Log: unverified + 2 alerts + exactly one dispatch |
| T06 | Corrupted link | Twin→Brain, Twin→Guardian | `scenarios/t06.sh` | Signatures broken | Marks the device, may quarantine | Dead-lettered `BAD_SIGNATURE`; channel STALE after 2 s; no action; alert if critical predicted | DLQ counts; status/alert events |
| T07 | Replay storm | Twin→Brain | `scenarios/t07.sh` | Re-sends identical readings | Flags `REPLAY` | Duplicates counted, not processed; no decision change | Summary: dedupe count > 0 |
| T08 | Stuck sensor | all three | `scenarios/t08.sh` | Constant out-of-range value | Flags `STUCK` | `OUT_OF_RANGE` + channel `STUCK`; no command based on it; alert | Log |
| T09 | Quarantined drain valve | **Brain→Guardian**, all three | `scenarios/t09.sh` | Publishes with `status: quarantined` | Quarantine event | Shield refuses `TARGET_CONTAINED`; R3 approval request when critical predicted; acts only on operator approval | Console approval; log shows the dilemma and the decider |
| T10 | Guardian outage | Brain→Guardian | `scenarios/t10.sh` | Normal | Silent | `GUARDIAN_DEGRADED` within 5 s; no R1/R2; alerts continue; no retroactive trust on return | Status events with timestamps |
| T11 | Twin restart mid-run | Twin→Brain | `scenarios/t11.sh` | Ids, clock and keys restart | — | New epoch; keys reloaded; no false duplicates; control resumes | Status event `epoch_changed`; decisions continue |
| T12 | Real incident during an attack | all three | `scenarios/t12.sh` | Both at once | DOUBTFUL on one, TRUSTED on the other | Hold on the attacked channel; the real incident controlled and verified | Timeline shows both chains side by side |
| T13 | Partner swap | Brain interop | `scenarios/t13.sh` | — | — | Identical decision log across Kafka, REST, in-process and the alt profile | Four-way diff |
| **T14** | **Forged verdict** | Brain→Guardian, security | `scenarios/t14.sh` | Normal | A `TRUSTED` verdict with a bad signature | Verdict → UNKNOWN; no R1/R2 command cites it; alert | Log: rejected verdict, no command |
| **T15** | **Partner drift** | interop, R4 | `make certify-twin` against a profile deliberately one field stale | — | — | Certification report names the mismatched field; after the profile fix, every scenario passes with zero core changes | The dated report |

T14 and T15 are new. T14 is the single clearest demonstration of the 10 security points; T15 is the single clearest demonstration of the 15 interoperability points, and it is the one that only makes sense because the Twin is still moving.

---

## 7. Deliverables and demo

**Repository** (`brain/` only; commit nothing outside it):

```
brain/
├── README.md, INTERFACE.md, Makefile, pyproject.toml, Dockerfile, docker-compose.yml
├── docs/ARCHITECTURE.md, docs/ADR.md
├── config/, profiles/, policy/, params/, schemas/, contracts/
├── src/city_brain/{domain,core,ports,edge,adapters,runtime,persistence,learn}/
├── timeline/
├── mocks/twin/, mocks/guardian/
├── conformance/{twin,guardian,reports}/
├── scenarios/, fixtures/, tests/
```

Root items to agree with the triplet: `.github/CODEOWNERS`, root `docker-compose.yml` with `include:` per team, `scenarios/`, `logs/`, `TESTBOOK.md`, `INTEGRATION.md`, `COLLABORATION.md`.

**Report, Brain part (5 pages, the six mandated subheadings):** Role and goals → architecture §1 · Architecture → §§4, 6–8 · Tech stack and why → §13 · Interfaces in/out → §5 + `INTERFACE.md` · Security measures → §11 + P5 test table · Known limitations → §20, with the Twin-in-development posture stated as a design choice rather than an excuse.

**Pitch deck (~5 slides):** problem · three-layer split and where we sit · the trust gate and effect verification in one diagram · the partner-swap claim with the T13 diff · what makes it reliable (fail-closed, deterministic replay, hash-chained logs).

**Demo runbook:** run `profile=standalone` as the default and switch to `triplet` only if the live connection is healthy at the readiness check. Standalone needs no broker, no partner and no network, so the 8 minutes cannot be lost to someone else's container. Bring the hotspot anyway. Have `logs/` from a prior triplet run open in a second tab so the joint-run evidence is on screen even if the live link dies.

---

## 8. Hand-off prompts

Start each plan in the `triplet-07` clone. Work stays local on the plan's branch; push only when the team says so.

**P1:**

```
Read brain/docs/ARCHITECTURE.md and brain/docs/plans/2026-11-06-city-brain-p1-standalone.md.
Execute task by task on branch brain/p1-vertical-slice using superpowers:subagent-driven-development.
The Twin may be unavailable — that must never block you; mark Twin-dependent tests with the
skip machinery from Task 2. Stop after each task's commit if a test outside the task fails.
```

**Each later plan:**

```
Plan <N> is merged. Using brain/docs/ARCHITECTURE.md, §4.<N+1> of the final implementation plan,
and the current code in brain/, write the implementation plan for Plan <N+1> with
superpowers:writing-plans. Honour rules G1-G9 of the final plan.
```

**Certification, whenever the Twin moves:**

```
The Twin has changed. Run the certification loop in §5 of the final implementation plan:
sample, certify, reconcile in profiles/ and mocks/ only, re-run everything, file the report
and the COLLABORATION.md line. If a change under src/city_brain/core/ appears necessary,
stop and explain why instead of making it.
```

---

## 9. Open items

**Settle before P2 ships** (they are ours alone):

1. ~~Guardian public key in standalone~~ — settled in P1: mock keys derive from the seed and the runner passes the public key in memory, so no key material is committed.
2. Recording retention — `recordings/` git-ignored, one gzipped `fixtures/sample-60s/` committed. Lower the gzip threshold to 5 MB so preset recordings never land raw in a repo the jury clones.
3. ~~`location` as object~~ — settled in P1: `map_reading` handles both forms.

**Ask the Twin team** (and record the answers in the profile's `observed` block):

1. Will the wire format in `schema.json` be frozen, and from when? If not, what is the notification channel when it changes?
2. Can a run id or start time be added to readings, so epochs are explicit rather than inferred from key rotation?
3. Will commands to isolated or quarantined devices be refused, or is that the Brain's job permanently?
4. Will unknown `target_state` values keep returning `ok`? If so, we keep the allowlist and say so in the report; if not, tell us when it changes.
5. Can the Twin inject real (non-attack) incidents per zone? Without it, the non-water domains can only ever be tested for correct non-reaction.

**Ask the Guardian team:**

1. Accept `guardian.verdicts` and `guardian.events` as in architecture §5.3, or name another transport? Any of the three adapters works; we need to know which.
2. One verdict per reading, or per device window? Both are supported if each verdict names the readings it covers.
3. Will verdicts be signed, and where is the public key published?
4. Will containment be announced as events with sim-time effective times?
5. Will you consume our `ACTUATOR_NO_EFFECT` advisories?

Every one of these has a default in the profile already, so none of them blocks a single line of P1 through P5.
