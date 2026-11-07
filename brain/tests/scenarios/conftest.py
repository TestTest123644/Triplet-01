"""Scenario runs are slow (a 300 s run signs 126,000 readings), so each is run once per session."""

import pytest

from scenarios.runner import run
from tests.scenarios.helpers import read_log

RESTART_AT_S = 192  # z1-wl1 opened at 191.4 s and is still VERIFYING


@pytest.fixture(scope="session")
def t02_log(tmp_path_factory):
    return run("T02", "standalone", tmp_path_factory.mktemp("t02"))


@pytest.fixture(scope="session")
def t02_rerun_log(tmp_path_factory):
    return run("T02", "standalone", tmp_path_factory.mktemp("t02-rerun"))


@pytest.fixture(scope="session")
def t05_log(tmp_path_factory):
    return run("T05", "standalone", tmp_path_factory.mktemp("t05"))


@pytest.fixture(scope="session")
def restart_log(tmp_path_factory):
    return run("T02", "standalone", tmp_path_factory.mktemp("restart"), restart_at_s=RESTART_AT_S, duration_s=450)


@pytest.fixture(scope="session")
def t02_events(t02_log):
    return read_log(t02_log)


@pytest.fixture(scope="session")
def t05_events(t05_log):
    return read_log(t05_log)


@pytest.fixture(scope="session")
def restart_events(restart_log):
    return read_log(restart_log)


# ----- the Test Book (spec 16) -----

BOOK = ("T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12")


# How long each scenario has to run for its own criteria to be visible. The scenario files
# keep the durations the spec names, and `make book` runs those in full; the suite runs the
# shortest window that still contains the attack, the outage or the restart being checked.
BOOK_TEST_DURATION_S = {
    "T01": 150.0,   # the band, the quiet capabilities, and valve changes after 120 s
    "T03": 60.0,    # the spoof starts at 20 s
    "T04": 60.0,    # the stolen key starts at 20 s
    "T06": 40.0,    # the link corrupts from 10 s, and a channel is stale after 2 s
    "T07": 40.0,    # the replay starts at 10 s
    "T08": 60.0,    # the sensor sticks from 10 s
    "T09": None,    # the tank only reaches the high band near 190 s, and the operator answers at 195 s
    "T10": None,    # the outage window is 15 s to 40 s, and recovery follows it
    "T11": 200.0,   # the Twin restarts at 150 s
    "T12": 90.0,    # the stolen key starts at 20 s while the tank fills
}


@pytest.fixture(scope="session")
def book_logs(tmp_path_factory, t02_log, t05_log):
    """Every scenario in the book, run once. T02 and T05 reuse the runs they already have."""
    logs = {"T02": t02_log, "T05": t05_log}
    for name in BOOK:
        if name in logs:
            continue
        logs[name] = run(
            name, "standalone", tmp_path_factory.mktemp(name.lower()),
            duration_s=BOOK_TEST_DURATION_S.get(name),
        )
    return logs


@pytest.fixture(scope="session")
def brain_event_schema():
    import json
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "schemas" / "brain-event.v1.schema.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _book_fixture(name):
    @pytest.fixture(scope="session", name=f"{name.lower()}_log")
    def log_fixture(book_logs):
        return book_logs[name]

    @pytest.fixture(scope="session", name=f"{name.lower()}_events")
    def events_fixture(book_logs):
        return read_log(book_logs[name])

    return log_fixture, events_fixture


# T02 and T05 have their own fixtures above; the rest are generated from the book.
for _name in (n for n in BOOK if n not in ("T02", "T05")):
    globals()[f"{_name.lower()}_log_fixture"], globals()[f"{_name.lower()}_events_fixture"] = (
        _book_fixture(_name))
