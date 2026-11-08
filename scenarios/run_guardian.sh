#!/usr/bin/env bash
set -euo pipefail

# Determine repository root and guardian directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARDIAN_DIR="${SCRIPT_DIR}/../guardian"

# Ensure JAVA_HOME is set for pemja/flink
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"

cd "${GUARDIAN_DIR}"

echo "========================================================================"
echo ">>> [1/2] Running: uv run python scripts/benchmark_orchestrator.py"
echo "========================================================================"
uv run python scripts/benchmark_orchestrator.py

echo ""
echo "========================================================================"
echo ">>> [2/2] Running: uv run python mocks/run_scenarios.py"
echo "========================================================================"
uv run python mocks/run_scenarios.py
