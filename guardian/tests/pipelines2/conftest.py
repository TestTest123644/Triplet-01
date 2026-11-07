import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for path in (ROOT, ROOT / "tests" / "pipelines"):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)
