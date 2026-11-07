import pytest

from tests.partners import ENDPOINTS, reachable, skip_reason

_skipped: dict[str, int] = {name: 0 for name in sorted(ENDPOINTS)}


def pytest_collection_modifyitems(config, items):
    probed: dict[str, str | None] = {}  # marker -> skip reason, None if reachable
    for item in items:
        for name, ep in ENDPOINTS.items():
            if item.get_closest_marker(name) is None:
                continue
            if name not in probed:
                value = ep.value()
                probed[name] = None if reachable(value) else skip_reason(ep, value)
            if probed[name] is not None:
                item.add_marker(pytest.mark.skip(reason=probed[name]))
                _skipped[name] += 1


def pytest_terminal_summary(terminalreporter):
    if any(_skipped.values()):
        counts = " ".join(f"{k}={v}" for k, v in _skipped.items())
        terminalreporter.write_line(f"partner tests skipped: {counts}")
