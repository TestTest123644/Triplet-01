"""Tests for the twin/INTERFACE.md interface card (A4 single-page constraints)."""

from pathlib import Path
import re
import pytest


def test_interface_card_constraints():
    """Verify twin/INTERFACE.md meets A4 single-page constraints:

    - At most 70 lines
    - At most 450 words
    - Exactly four required section headings
    - Ends with reference link to docs/INTERFACE_REFERENCE.md
    """
    card_path = Path(__file__).resolve().parents[1] / "INTERFACE.md"
    assert card_path.is_file(), f"Missing interface card: {card_path}"

    content = card_path.read_text(encoding="utf-8")
    lines = content.splitlines()
    words = content.split()

    assert len(lines) <= 70, f"INTERFACE.md has {len(lines)} lines (max allowed: 70)"
    assert len(words) <= 450, f"INTERFACE.md has {len(words)} words (max allowed: 450)"

    required_headings = [
        "## What the Twin sends",
        "## What the Twin accepts",
        "## How to repoint it",
        "## How it authenticates",
    ]

    for heading in required_headings:
        assert heading in content, f"Missing required section heading: '{heading}'"

    last_non_empty = [line.strip() for line in lines if line.strip()][-1]
    assert last_non_empty == "Full reference: docs/INTERFACE_REFERENCE.md", (
        f"Unexpected final line: {last_non_empty}"
    )

