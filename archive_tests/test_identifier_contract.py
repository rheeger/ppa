from __future__ import annotations

import json
from pathlib import Path

from archive_vault.canon import phone as canon_phone

CASES = json.loads((Path(__file__).parent / "fixtures" / "canon" / "cases.json").read_text(encoding="utf-8"))


def test_shared_canon_cases() -> None:
    seen: dict[str, str] = {}
    for case in CASES["cases"]:
        parsed = canon_phone.parse(case["input"])
        assert parsed.canonical == case["canonical"], case
        assert parsed.validity == case["validity"], case
        if case.get("extension"):
            assert parsed.extension == case["extension"]
        if parsed.canonical:
            seen.setdefault(parsed.canonical, case["id"])
    assert canon_phone.parse("alice42").canonical != "42"
    assert canon_phone.parse("alice42").original != canon_phone.parse("bob42").original
    assert canon_phone.canonical("alice42") == ""
    assert canon_phone.canonical("bob42") == ""


def test_extension_not_folded_into_identity() -> None:
    parsed = canon_phone.parse("5551234567 ext 99")
    assert parsed.canonical == "+15551234567"
    assert parsed.extension == "99"
    assert "99" not in parsed.canonical[-2:] or parsed.canonical.endswith("4567")
