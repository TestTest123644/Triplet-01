# SDD ledger — plan: brain/docs/plans/2026-11-06-city-brain-p1-standalone.md

Spec: brain/docs/ARCHITECTURE.md. Rules G1-G9: brain/docs/plans/2026-11-06-city-brain-final-implementation-plan.md §1.
Local only: never push. Branch brain/p1-vertical-slice. Interpreter brain/.venv (3.13). make = mingw32-make.

## Pre-flight scan

| Pair / task | Produces vs consumes | Finding |
|---|---|---|
| T1 → all | BrainConfig, policy, params | ok; pending_weight added to policy (C7) |
| T2 self | markers, skip | ok; marker test deliberately never runs its body |
| T3 → T5,7,9-15 | canonical types | ok; Alternative is (option, rejected_because), spec §7.1 says str; plan kept, spec §5.4 Q7 needs rejected_because |
| T4 → T6,T7 | sign/verify/KeyRing | ok |
| T5 → T6 | inventory, fields, accepts_partner_state | was missing, added (C1) |
| T5 → T7 | map_reading, GuardianProfile | ok |
| T5 → T13/T17 | allowlist | was unspecified, added |
| T6 → T7 | MockTwin output | order fixed (C1) |
| T6 → T17 | deterministic keys, MockGuardian | fixed (C6) |
| T7 → T8 | guard_for_mock | ok |
| T9 → T10,11,15 | epoch reset | extended to filters, ledger, in-flight (C8) |
| T10 → T15 | observed_slope, flags, at_ceiling | at_ceiling added (C8) |
| T11 → T15 | estimate_weight vs trust_weight | added (C7) |
| T12 → T15 | WaterContext | ok |
| T13 → T15 | evaluate, TokenBucket | ok |
| T14 → T9 | status_event replaces placeholder | ok |
| T16 → T17 | TwinPort, InProcTwin | fixed (C3) |
| T15 → T17 | effects, SendCommand | ok; submit_raw async (C8) |
| T1 self | Makefile vs Windows | PY detection, mingw32-make |
| T3-T5, T7, T9-T14 self | tests vs interfaces | ok on reading |
| T6 self | old TrustAllGuardian vs policies | rewritten as MockGuardian |
| T15 self | test_missing_verdict needs filter to rise without verdicts | resolved by C7 pending_weight |
| T17 self | run() writes ../logs from tests | logs_dir param; tests use tmp_path |

## Rulings

Ruling: task order is scaffold, skip machinery, types, signatures, profiles+observed, mocks, guard, conformance, core — mocks need profiles (G3), guard tests need mocks — if wrong, two tasks swap.
Ruling: the two fixture-based tests are rewritten against MockTwin output — no recordings may be a prerequisite (final plan §0) — real-Twin fit is checked later by the C-loop.
Ruling: ports.py kept with InProcTwin as new Task 16; McpTwinClient to P4 — Task 17 needs TwinPort — none.
Ruling: observed.by = city-brain-team — schema requires by, start value omitted it — cosmetic.
Ruling: reading schema copied from the INTERFACE.md JSON block — no schema.json exists in the repo at 418a024 — if Twin team adds one, the C-loop picks it up.
Ruling: all mock keys derive from the seed — sig_hash is in logged evidence, determinism exit test needs it — mock keys are not secrets; settles open item 9.1 with nothing committed.
Ruling: estimate_weight gives a reading with no verdict yet pending_weight 0.5 for the filter only — readings arrive before verdicts, spec §7.2 calls these OBSERVED not UNKNOWN, and §8.11 proposes before verdicts land — if wrong, a fake value can nudge the estimate during verdict lag, though no command can cite it past the trust gate; P3 must revisit once real Guardian latency is known.
Ruling: location object form accepted in map_reading — schema allows it — none (settles open item 9.3).
Ruling: mcp, confluent-kafka, mcp_server.py, publisher.py moved to P4 — no consumer in P1 — P4 adds them back.
Ruling: BrainApp.submit_raw is async and takes a logical stream name — it awaits twin.call; topic names belong to the Kafka adapter — none.
Ruling: tests pass logs_dir=tmp_path — commit nothing outside brain/ — none.
Ruling: final plan copy patched in brain/docs/plans (T09 P3 exit, P1-P5 claim, hand-off paths); originals in Downloads untouched — plan said spec-complete while four scenarios pass only at P6 — wording only.

## Progress
Task 1: implementer done (330d6bc), awaiting review
Task 1: complete (commits 066858f..330d6bc, review clean)
Task 1: minor (deferred): _apply_env silently replaces a non-dict node on collision; BRAIN__POLICY__ overrides have no effect because policy_file loads after
Task 1: minor (deferred): no test for env string coercion
Ruling: brain/.gitignore gets *.egg-info/ in Task 2 — root .gitignore is outside brain/ — none.
Ruling: subagent commit trailers follow each subagent's own attribution reminder — they name the model that wrote the commit — cosmetic.
Task 2: fix round 1/5 (1 addressed pending re-review — reachable positive test; commits 081537f..6be6ee4)
Task 2: fix round 1/5 (2 addressed, 0 open; commits 081537f..6be6ee4)
Task 2: complete (commits 330d6bc..6be6ee4, review clean)
Task 2: minor (deferred): _skipped in conftest is module-level, never reset; a test with both triplet and twin_live double-probes and double-counts; port 0 becomes 80; terminal summary line untested
Ruling: canonical enums use uppercase values equal to member names; CanonicalUnit uses the spec's m, veh_per_min, MW, AQI, calls_per_min; DeviceStatus is canonical uppercase and the profile maps the Twin's lowercase at the edge — spec §7.1, §10.1 and Task 17 tests — if wrong, one enum changes value.
Task 3: fix round 1/5 (2 addressed, 0 open; commits ae088c0..00c1ec7)
Task 3: complete (commits 6be6ee4..00c1ec7, review clean)
Task 3: minor (deferred): Observation omits spec §7.1 epoch and tick, brief-compliant; note for P3
Task 4: complete (commits 00c1ec7..65108aa, review clean)
Task 4: minor (deferred): test_canonical_bytes_matches_twin_form is tautological, no non-ASCII literal; inline import in test body
Ruling: valve state parsing mirrors handle_set_valve, overriding the plan's "anything else closed" — the plan's rationale is false for % strings; the spec ranks Twin code first — none.
Ruling: accepts_partner_state stays the contract (strict); MockDefects.accept_unknown_states=True reproduces the Twin's looser behaviour — keeps G3 and §2.4 both true — if wrong, one method changes.
Ruling: inventory gains initial_actuator and zone_coords — otherwise the mock hardcodes Twin facts, against G1 and G3 — none.
Task 5: minor (deferred): map_reading missing seq/ts surfaces as KeyError, non-numeric value raises raw error; _states rebuilds 101 strings per call; test_toolcall_is_frozen uses Exception; GuardianProfile ignores verdict_schema (fixing the last one is Task 7's concern)
Task 5: fix round 1/5 (4 addressed, 0 open; commits cc6592d..95f5710)
Task 5: complete (commits 65108aa..95f5710, review clean)
Task 5: minor (deferred): nan% is closed/flagged in the profile but opens fully on the real Twin; deliberate safe-side divergence, conformance kit must not assume parity
Task 6: minor (deferred): wrong-device-type error text differs from Twin for water_level and power_load; invented Unknown tool text; fractional ttl_s/delay truncated by whole-second ts; bare KeyError on missing args; test reaches into p._data; no test for inject on unknown target
Ruling: mocks/twin/mock_twin.py added beyond the brief's file list — MockTwin needs a home separate from physics — none.
Task 6: fix round 1/5 (7 addressed, 0 open; commits 0d5f107..bb988a1)
Task 6: complete (commits 95f5710..bb988a1, review clean)
Ruling: non-finite reading values are rejected as SCHEMA_VIOLATION rather than flagged — NaN defeats every later comparison and is not valid JSON — if wrong, a flag replaces a reject.
Ruling: OVERSIZED applies to verdicts too — spec §11.1 — none.
Task 7: minor (deferred): module-level guard and twin in test_guard.py are shared state
Task 7: fix round 1/5 (4 addressed, 0 open, follow-up found: huge int overflow; commits b1b3dc6..13dd94b)
Task 7: fix round 2/5 (3 addressed, 0 open; commits 13dd94b..34df5ee)
Task 7: complete (commits bb988a1..34df5ee, review clean)
Task 8: complete (commits 34df5ee..2c7f8c6, review clean)
Task 8: minor (deferred): edit-propagation test only for valve; accepted states checked by status only, not stored state; ratio boundary not exercised
Task 9: minor (deferred): seen unbounded per epoch (comment added, bound in P7); sim_now carries up to 1 s constant bias from whole-second ts; restarts before seq 100 and within 60 s read as OUT_OF_ORDER until seq catches up; missing epochs key is a bare KeyError
Task 9: fix round 1/5 (3 addressed, 0 open; commits 2850627..ef7d45d)
Task 9: complete (commits 2c7f8c6..ef7d45d, review clean)
Ruling: purity test need not defeat star imports of datetime, rebinding or dynamic imports — it guards against accidents, not deliberate evasion; review catches those — if wrong, a small AST addition.
Ruling: planner holds on MODEL_MISMATCH (add reason to water_hold_reason, Task 12) and OUT_OF_RANGE readings give the filter weight 0 (Task 15) — after a gating reset a stuck or spoofed sensor sits at the fake level for up to 25 ticks with no hold flag, and spec §1 forbids acting on it — if wrong, control resumes up to 5 s later after a real model mismatch.
Ruling: estimator module constants (ring 600, 10 points, 5 gated, 50 ungated, 0.005, P0) stay in code — the brief writes them and they are structural, not partner thresholds — moves to params if calibration (P8) needs them.
Task 10: minor (deferred): MODEL_MISMATCH clears after 50 updates even if the actuator still lies, so trust logic must not read a cleared flag as fixed; RESET_SLOPE_WINDOW = 30 is unspecified; missing filter params are fatal at ingest; step.py import order
Task 10: fix round 1/5 (2 addressed, 0 open; commits fe3edc5..4f34cf2)
Task 10: complete (commits ef7d45d..4f34cf2, review clean)
Task 11: minor (deferred): add returns [] when a fresh verdict replaces an expired identical one, so Task 15 re-evaluation must not rely on it alone; issued_at tie allows replay re-upgrade, note for Guardian conformance; test depends on cwd and config file
Task 11: fix round 1/5 (2 addressed, 0 open; commits 240913a..0000067)
Task 11: complete (commits 4f34cf2..0000067, review clean)
Ruling: WaterContext gains global_tick and staleness is filter.is_stale only — the filter owns it and a skippable check is unsafe — changes the brief's signature; Task 15 must pass it.
Ruling: ratio:0 counts as closed, unknown valve states hold with UNKNOWN_VALVE_STATE — spec §1 never act blind — if wrong, one hold reason goes away.
Task 12: fix round 1/5 (3 addressed, 0 open; commits 22d46c4..de9b986)
Task 12: complete (commits 0000067..de9b986, review clean)
Task 13: complete (commits de9b986..7bec5c4, review clean)
Task 13: minor (deferred): no test for current_state None or ratio:0 vs closed (treated as different, so opening or closing into the equivalent state is not NO_OP); NO_OP does not check actuator capability matches; unknown-target test does not assert later checks record pass; peek test would pass if peek committed a refill
Ruling: verification_event gains keyword-only target and incident_id — one action's events must share subject and correlation (spec §12.1) — changes the brief's signature; Task 15 must pass them.
Ruling: _native raises on non-finite floats and naive datetimes — a record may not carry them (§7.2, §9.2) — a bad value crashes the loop loudly instead of logging; revisit in P7 with the journal.
Task 14: minor (deferred): schema does not require incident keys or evidence minItems; no test of wrong value types inside data; time format not checked by validate; builders do not validate Literal args
Task 14: fix round 1/5 (5 addressed, 0 open; commits bf38a90..8030391)
Task 14: complete (commits 7bec5c4..8030391, review clean)
Ruling: Task 15 adds what the brief leaves open: allowlist on RegistryLoaded, one active action per target, refusal repeats not re-logged, blocked retry after cooldown, reading status updates registry, non-terminal actions go UNKNOWN on epoch change — without these the loop spams or double-proposes, and old actions dangle — behaviour limited to Task 15.
Ruling: UNKNOWN joins the active set, so the target freezes until reconciliation (P7) — spec §2.4 and §8.8 forbid resending on unreconciled state; operator is alerted — if wrong, a frozen channel needs operator action during a lost reply.
Ruling: verification counts only readings whose verdict is TRUSTED or DOUBTFUL when the window closes — spec §8.8 — fewer than 10 points gives inconclusive.
Ruling: suspect clears on a new epoch — a new Twin run resets actuators — if wrong, a bad valve gets one more try and one more alert.
Ruling: RATE_LIMIT at dispatch stays BLOCKED with the F cooldown — burst sizing should not reach it — if wrong, a valve waits 5 s.
Ruling: planning needs a full evidence_count of readings — one reading must not trigger R1 and also initialise the filter — if wrong, the first command comes 0.2 s later.
Ruling: EvidenceRef gains device_status, weight is zero if arrival or current status is not OK — spec §8.1 — extends the Task 3 type.
Ruling: epoch-caused UNKNOWN does not block planning; only lost replies freeze a target — a Twin restart must not freeze the city (T11).
Task 15: minor (deferred): false UNVERIFIED and suspect on a clamped sensor at the ceiling; SETTLE_TICKS encodes 0.5 s only at 0.1 s period; REJECTED re-plans once per cooldown unbounded; dispatch re-checks containment only via evidence, matters when a capability cites other devices; PROPOSED never published; _verify bypasses _transition; absolute slope threshold can verify a no-op valve while level falls for another reason (spec criterion)
Ruling: a lost-reply UNKNOWN is released by a new epoch — a Twin restart resets actuators, so the old unknown no longer describes anything — if wrong, one command follows a restart without reconciliation.
Task 15: fix round 1/5 (8 addressed, 0 open; commits 1af6f67..a9dffee)
Task 15: complete (commits 8030391..a9dffee, review clean)
Task 15: minor (deferred): EPOCH_CHANGED reasons accumulate across restarts; inconclusive alert wording when points were excluded; commanded_ratio accepts leading zeros while Shield does not; INSUFFICIENT_EVIDENCE hold per channel at startup; expired TRUSTED seen first gets full filter weight (harmless, cannot actuate)
Task 16: complete (commits a9dffee..a078709, review clean)
Task 16: minor (deferred): load_registry may put None keys in the dict; get_result does not catch exceptions, the P7 reconcile caller must; UnexpectedReply branch untested
Ruling: BrainApp fills data.tool on DISPATCHED command events from profile.to_tool_call before logging — core may hold no Twin tool names, spec §5.4 wants tool — none.
Ruling: burst-of-bad-signatures reload reads an injected wall_now so standalone stays deterministic; run() gains restart_at_s for the restart test — none.
Ruling: logs_dir is the repo-root logs/ per final plan §4.2; generated logs stay uncommitted — R2 expects a shared location; generated files show untracked at root — a careless git add is the risk.
Task 17: minor (deferred): max_level_before reads frozen evidence, not level at dispatch; verify_chain cannot detect tail truncation (needs §12.2 final hash, P2); submit_raw drops reload effects and feed returns unenriched events; SimClock delivers a timer cancelled within the same advance, core guards it; src reaches mocks transitively via runtime/main -> scenarios.runner for the CLI; test_determinism shares one process hash seed (independently checked with two PYTHONHASHSEED values, identical); suite takes about 13 min
Task 17: fix round 1/5 (8 addressed, 0 open; commits 6f91022..e7a74fc)
Task 17: complete (commits a078709..e7a74fc, review clean)
Task 17: minor (deferred): RESTART_AT_S 192 hand-tuned to T02 physics, fails loudly if physics moves; config/triplet.yaml logs_dir still ../logs; root .gitignore does not cover logs/*.jsonl (outside brain/, needs the team)
Final review: With fixes (2 Critical, 10 Important). Ran on Opus because Fable needs usage credits.
Ruling: dedupe runs before the epoch check, and an epoch needs corroboration (N distinct devices or a key change) — one replay must not wipe safety state; overrides plan Task 9 order; spec §5.1 agrees — first few readings of a real restart are dropped.
Ruling: close_window_s becomes 15 s, inconclusive no longer marks suspect, suspect clears when observed behaviour matches the commanded state — noise must not permanently condemn a healthy valve (spec §8.8 "until verified") — close verification takes longer.
Ruling: verification closes at window_s + verdict_wait_s — legal 2 s Guardian lag must not condemn a valve — verified events arrive 2 s later.
Ruling: I2 gets a hold on UNTRUSTED verdicts now; filter rollback is P3's — full replay is a design change — the estimate can still be nudged, but no command can cite it and a hold is recorded.
Ruling: I10 parked to P3 — Guardian containment does not exist yet — P3 must split the status fields.
Ruling: suspect recovery counting readings awaiting a verdict is parked to P3 with I2 — needs per-reading trust in the filter — forged readings during verdict lag could clear suspect.
