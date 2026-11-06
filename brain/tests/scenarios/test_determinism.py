from tests.scenarios.helpers import read_log


def strip(events):
    return [{k: v for k, v in e.items() if k not in ("time", "hash", "prev_hash")} for e in events]


def test_standalone_run_is_deterministic(t02_events, t02_rerun_log):
    assert strip(t02_events) == strip(read_log(t02_rerun_log))
