"""Test strict mock and scenario boundaries.

Assigned to: Agent_Reviewer / Agent_QA.
Mocks and scenarios represent external partner layers (City Brain, Guardian, test harness).
They must not import Twin internals (twin.simulation, twin.mcp_server, twin.kafka_streamer, etc.).
Imports of twin.mocks.* are explicitly permitted if needed when running in-tree, but mocks and scenarios
must never import internal twin modules directly.
"""

from __future__ import annotations

import glob
import os
import re
from pathlib import Path


def test_no_internal_twin_imports_in_mocks_and_scenarios():
    """Verify twin/mocks/*.py and scenarios/*.py have zero direct imports of twin internals."""
    repo_root = Path(__file__).resolve().parent.parent.parent

    # Match lines like "import twin..." or "from twin..."
    # But allow "import twin.mocks" / "from twin.mocks" if ever present.
    import_twin_regex = re.compile(r"^\s*(?:import|from)\s+twin(?:\.([a-zA-Z0-9_]+))?")

    search_patterns = [
        str(repo_root / "twin" / "mocks" / "*.py"),
        str(repo_root / "scenarios" / "*.py"),
    ]

    violations = []
    scanned_files = 0

    for pat in search_patterns:
        for file_path in glob.glob(pat):
            scanned_files += 1
            rel_path = os.path.relpath(file_path, repo_root)
            with open(file_path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    match = import_twin_regex.match(line)
                    if match:
                        submod = match.group(1)
                        # Explicitly permitted: imports of twin.mocks.* (if any)
                        if submod == "mocks":
                            continue
                        violations.append((rel_path, line_no, line.strip()))

    assert scanned_files > 0, "Expected to find mock and scenario python files to scan."
    assert not violations, (
        f"Found forbidden twin internal imports in mock/scenario files: {violations}"
    )

