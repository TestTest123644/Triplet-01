import ast
import re
from pathlib import Path

CORE = Path(__file__).resolve().parent.parent / "src" / "city_brain" / "core"
BANNED_MODULES = {"asyncio", "time", "random", "uuid"}
BANNED_PREFIXES = ("city_brain.adapters", "city_brain.runtime")
LITERALS = re.compile(r"twin\.sensors|set_valve|set_signal_plan|switch_grid|\bz\d-|zone-\d|meters")


def _violations(path: Path) -> list[str]:
    out = []
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        for n in names:
            if n.split(".")[0] in BANNED_MODULES or n.startswith(BANNED_PREFIXES):
                out.append(f"{path.name}:{node.lineno} import {n}")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr in ("now", "utcnow") and ast.unparse(node.func.value).endswith("datetime"):
                out.append(f"{path.name}:{node.lineno} datetime.{node.func.attr}")
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and LITERALS.search(node.value):
            out.append(f"{path.name}:{node.lineno} literal {node.value!r}")
    return out


def test_core_is_pure():
    files = sorted(CORE.glob("**/*.py"))
    assert files
    assert [v for f in files for v in _violations(f)] == []


def test_checker_detects_violations(tmp_path):
    bad = tmp_path / "bad.py"
    bad.write_text("import time\nfrom datetime import datetime\nx = datetime.now()\ny = 'set_valve'\n")
    assert len(_violations(bad)) == 3
