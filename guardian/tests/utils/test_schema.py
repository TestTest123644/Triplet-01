import subprocess
import sys

import pytest

from guardian_layer.utils.schema import CheckResult, SensorSummary


def test_schema_imports_in_a_fresh_interpreter():
    # in-process the import order of other tests can mask a circular import
    subprocess.run(
        [sys.executable, "-I", "-c", "import guardian_layer.utils.schema"], check=True
    )


def test_sensor_summary_is_in_schema():
    summary = SensorSummary("s1", "water", 36.79, 10.18, 0.5, 0.6)
    assert (summary.sensor_id, summary.cluster, summary.mean, summary.last) == (
        "s1",
        "water",
        0.5,
        0.6,
    )
    with pytest.raises(AttributeError):
        summary.mean = 1.0  # type: ignore[misc]


def test_check_result_pass_is_a_string_enum():
    assert CheckResult.PASS == "pass"
