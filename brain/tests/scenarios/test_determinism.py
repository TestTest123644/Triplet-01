from tests.scenarios.helpers import read_log

# Wall-clock `time` differs between two runs by design, and the hash chain and the Ed25519
# signature both cover it, so all three are excluded: what has to be identical is every
# decision the Brain made and everything it rested on.
PER_RUN = ("time", "hash", "prev_hash", "sig")


def strip(events):
    return [{k: v for k, v in e.items() if k not in PER_RUN} for e in events]


def test_standalone_run_is_deterministic(t02_events, t02_rerun_log):
    assert strip(t02_events) == strip(read_log(t02_rerun_log))


def test_both_runs_are_signed_and_chained(t02_log, t02_rerun_log):
    from city_brain.adapters.files.jsonl_log import verify_chain

    for log in (t02_log, t02_rerun_log):
        public_key = log.with_suffix(".pub").read_text(encoding="ascii").strip()
        assert verify_chain(log, public_key)
