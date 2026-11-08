#!/usr/bin/env python3
"""Execute Guardian benchmark orchestrator and mock cross-layer scenarios.

Runs:
  1. scripts/benchmark_orchestrator.py
  2. mocks/run_scenarios.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    scenarios_dir = Path(__file__).resolve().parent
    repo_root = scenarios_dir.parent
    guardian_dir = repo_root / "guardian"
    venv_dir = guardian_dir / ".venv"

    # Ensure JAVA_HOME is exported for pemja / Flink
    if "JAVA_HOME" not in os.environ:
        for jvm_path in (
            "/usr/lib/jvm/java-17-openjdk-amd64",
            "/usr/lib/jvm/default-java",
            "/usr/lib/jvm/java-21-openjdk-amd64",
        ):
            if os.path.exists(jvm_path):
                os.environ["JAVA_HOME"] = jvm_path
                break

    # If guardian/.venv exists and current Python is not running from it, re-exec with venv python
    venv_python = venv_dir / "bin" / "python"
    if venv_python.exists() and Path(sys.prefix).resolve() != venv_dir.resolve():
        cmd = [str(venv_python), str(__file__)] + sys.argv[1:]
        return subprocess.run(cmd).returncode

    python_bin = sys.executable

    # Ensure guardian/src is in PYTHONPATH
    src_dir = str(guardian_dir / "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    os.environ["PYTHONPATH"] = src_dir + os.pathsep + os.environ.get("PYTHONPATH", "")

    # 1. Run Benchmark Orchestrator
    print("=" * 72)
    print(">>> [1/2] Executing: scripts/benchmark_orchestrator.py")
    print("=" * 72)
    benchmark_script = guardian_dir / "scripts" / "benchmark_orchestrator.py"
    res1 = subprocess.run([python_bin, str(benchmark_script)], cwd=guardian_dir)
    if res1.returncode != 0:
        return res1.returncode

    # 2. Run Mock Scenarios
    print("\n" + "=" * 72)
    print(">>> [2/2] Executing: mocks/run_scenarios.py")
    print("=" * 72)
    scenarios_script = guardian_dir / "mocks" / "run_scenarios.py"
    res2 = subprocess.run([python_bin, str(scenarios_script)], cwd=guardian_dir)
    return res2.returncode


if __name__ == "__main__":
    sys.exit(main())
