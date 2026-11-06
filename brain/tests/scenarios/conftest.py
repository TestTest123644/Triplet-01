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
