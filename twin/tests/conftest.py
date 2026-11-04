"""Pytest configuration and global isolation fixtures.

Sets GROUND_TRUTH_DB_PATH to a temporary database file at conftest import time,
ensuring no test can accidentally read or write to ./ground_truth.db.
Also ensures twin/src and twin are added to sys.path so tests can import
twin, dashboard, and mocks cleanly whether invoked from twin/ or repo root.
"""

import os
from pathlib import Path
import sys
import tempfile
import pytest

# Ensure twin/src and twin root are on sys.path for both local and root test invocations
_twin_dir = Path(__file__).resolve().parent.parent
_src_dir = _twin_dir / "src"
for _path_str in [str(_src_dir), str(_twin_dir)]:
    if _path_str not in sys.path:
        sys.path.insert(0, _path_str)

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
