"""Pytest configuration and global isolation fixtures.

Sets GROUND_TRUTH_DB_PATH to a temporary database file at conftest import time,
ensuring no test can accidentally read or write to ./ground_truth.db.
"""

import os
import tempfile
import pytest

# Ensure GROUND_TRUTH_DB_PATH is set before any twin module is imported.
# twin.mcp_server.get_active_simulator() lazily builds a CitySimulator on the
# default ledger path if this environment variable is not configured.
_GLOBAL_TEST_TMPDIR = tempfile.mkdtemp(prefix="twin_conftest_")
_GLOBAL_TEST_DB = os.path.join(_GLOBAL_TEST_TMPDIR, "conftest_ground_truth.db")
os.environ["GROUND_TRUTH_DB_PATH"] = _GLOBAL_TEST_DB


@pytest.fixture(autouse=True)
def reset_mcp_active_simulator():
    """Reset twin.mcp_server._ACTIVE_SIMULATOR before and after every test."""
    try:
        import twin.mcp_server

        twin.mcp_server._ACTIVE_SIMULATOR = None
    except ImportError:
        pass

    yield

    try:
        import twin.mcp_server

        twin.mcp_server._ACTIVE_SIMULATOR = None
    except ImportError:
        pass
