import ast
import re
from pathlib import Path

import pytest

CORE = Path(__file__).resolve().parent.parent / "src" / "city_brain" / "core"
BANNED_MODULES = {"asyncio", "time", "random", "uuid"}
BANNED_LAYERS = {"adapters", "runtime"}
BANNED_CALLS = {
    "datetime.datetime.now",
    "datetime.datetime.utcnow",
    "datetime.datetime.today",
    "datetime.date.today",
}
LITERALS = re.compile(r"twin\.sensors|set_valve|set_signal_plan|switch_grid|\bz\d-|zone-\d|meters")


def _banned_import(qualified: str, level: int) -> bool:
    parts = qualified.split(".") if qualified else []
    if level:  # relative: from core, `..x` is city_brain.x
        return level >= 2 and bool(parts) and parts[0] in BANNED_LAYERS
    if parts and parts[0] in BANNED_MODULES:
        return True
    return parts[:1] == ["city_brain"] and len(parts) > 1 and parts[1] in BANNED_LAYERS


def _dotted(node: ast.AST, aliases: dict[str, str]) -> str | None:
    chain = []
    while isinstance(node, ast.Attribute):
        chain.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    root = aliases.get(node.id, node.id)
    return ".".join([root, *reversed(chain)])


def _violations(path: Path) -> list[str]:
    out = []
    tree = ast.parse(path.read_text(encoding="utf-8"))
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                aliases[a.asname or a.name.split(".")[0]] = a.name if a.asname else a.name.split(".")[0]
                if _banned_import(a.name, 0):
                    out.append(f"{path.name}:{node.lineno} import {a.name}")
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if _banned_import(base, node.level):
                out.append(f"{path.name}:{node.lineno} import {'.' * node.level}{base}")
            for a in node.names:
                full = f"{base}.{a.name}" if base else a.name
                if node.level == 0:
                    aliases[a.asname or a.name] = full
                if not _banned_import(base, node.level) and _banned_import(full, node.level):
                    out.append(f"{path.name}:{node.lineno} import {'.' * node.level}{full}")
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _dotted(node.func, aliases)
            if name in BANNED_CALLS:
                out.append(f"{path.name}:{node.lineno} call {name}")
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and LITERALS.search(node.value):
            out.append(f"{path.name}:{node.lineno} literal {node.value!r}")
    return sorted(out)


def test_core_is_pure():
    files = sorted(CORE.glob("**/*.py"))
    assert files
    assert [v for f in files for v in _violations(f)] == []


@pytest.mark.parametrize(
    "source, expected",
    [
        ("import time", ["bad.py:1 import time"]),
        ("import time as t", ["bad.py:1 import time"]),
        ("from time import monotonic", ["bad.py:1 import time"]),
        ("import random", ["bad.py:1 import random"]),
        ("from random import choice", ["bad.py:1 import random"]),
        ("import uuid", ["bad.py:1 import uuid"]),
        ("from uuid import uuid4", ["bad.py:1 import uuid"]),
        ("import asyncio", ["bad.py:1 import asyncio"]),
        ("from asyncio import sleep", ["bad.py:1 import asyncio"]),
        ("import city_brain.adapters", ["bad.py:1 import city_brain.adapters"]),
        ("from city_brain.runtime import x", ["bad.py:1 import city_brain.runtime"]),
        ("from city_brain import adapters", ["bad.py:1 import city_brain.adapters"]),
        ("from city_brain import runtime as r", ["bad.py:1 import city_brain.runtime"]),
        ("from ..adapters import x", ["bad.py:1 import ..adapters"]),
        ("from .. import runtime", ["bad.py:1 import ..runtime"]),
        ("from datetime import datetime\nx = datetime.now()", ["bad.py:2 call datetime.datetime.now"]),
        ("from datetime import datetime as dt\nx = dt.now()", ["bad.py:2 call datetime.datetime.now"]),
        ("import datetime\nx = datetime.datetime.utcnow()", ["bad.py:2 call datetime.datetime.utcnow"]),
        ("import datetime as d\nx = d.datetime.today()", ["bad.py:2 call datetime.datetime.today"]),
        ("from datetime import date\nx = date.today()", ["bad.py:2 call datetime.date.today"]),
        ("x = 'twin.sensors'", ["bad.py:1 literal 'twin.sensors'"]),
        ("x = 'set_valve'", ["bad.py:1 literal 'set_valve'"]),
        ("x = 'set_signal_plan'", ["bad.py:1 literal 'set_signal_plan'"]),
        ("x = 'switch_grid'", ["bad.py:1 literal 'switch_grid'"]),
        ("x = 'z1-wl1'", ["bad.py:1 literal 'z1-wl1'"]),
        ("x = 'zone-1'", ["bad.py:1 literal 'zone-1'"]),
        ("x = 'meters'", ["bad.py:1 literal 'meters'"]),
    ],
)
def test_checker_detects_violation(tmp_path, source, expected):
    bad = tmp_path / "bad.py"
    bad.write_text(source + "\n")
    assert _violations(bad) == expected


@pytest.mark.parametrize(
    "source",
    [
        "from datetime import datetime\nx = my_datetime.now()",
        "from city_brain.domain.types import Observation",
        "from .state import BrainState",
        "from datetime import datetime, timedelta\nx = datetime(2026, 1, 1)",
    ],
)
def test_checker_allows_clean_code(tmp_path, source):
    ok = tmp_path / "ok.py"
    ok.write_text(source + "\n")
    assert _violations(ok) == []
