"""Tests for staging_report command."""

from __future__ import annotations

from pathlib import Path

from archive_mcp.commands.staging import staging_report


def _write_card(root: Path, rel: str, body: str) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body, encoding="utf-8")


def test_staging_report_counts_by_type(tmp_path):
    root = tmp_path / "stg"
    _write_card(
        root,
        "a.md",
        """---
uid: u1
type: meal_order
restaurant: R
sent_at: "2024-03-01T12:00:00Z"
---
x
""",
    )
    _write_card(
        root,
        "b.md",
        """---
uid: u2
type: meal_order
restaurant: R2
sent_at: "2024-03-02T12:00:00Z"
---
y
""",
    )
    r = staging_report(str(root))
    assert r.total_cards == 2
    types = {t.card_type: t.count for t in r.types}
    assert types.get("meal_order") == 2


def test_volume_within_estimate(tmp_path):
    root = tmp_path / "stg"
    # event_ticket expected 20–50
    for i in range(25):
        _write_card(
            root,
            f"e{i}.md",
            f"""---
uid: et{i}
type: event_ticket
sent_at: "2024-01-01T00:00:00Z"
---
b
""",
        )
    r = staging_report(str(root))
    et = next(t for t in r.types if t.card_type == "event_ticket")
    assert et.within_estimate is True
    assert et.volume_status == "OK"


def test_volume_below_estimate(tmp_path):
    root = tmp_path / "stg"
    _write_card(
        root,
        "x.md",
        """---
uid: u1
type: meal_order
sent_at: "2024-01-01T00:00:00Z"
---
b
""",
    )
    r = staging_report(str(root))
    mo = next(t for t in r.types if t.card_type == "meal_order")
    assert mo.within_estimate is False
    assert "LOW" in mo.volume_status


def test_volume_above_estimate(tmp_path):
    root = tmp_path / "stg"
    for i in range(4000):
        _write_card(
            root,
            f"p{i}.md",
            f"""---
uid: u{i}
type: meal_order
sent_at: "2024-01-01T00:00:00Z"
---
b
""",
        )
    r = staging_report(str(root))
    mo = next(t for t in r.types if t.card_type == "meal_order")
    assert mo.within_estimate is False
    assert "HIGH" in mo.volume_status


def test_empty_staging_directory(tmp_path):
    root = tmp_path / "empty"
    root.mkdir()
    r = staging_report(str(root))
    assert r.total_cards == 0
    assert r.types == []


def test_staging_report_sample_uids(tmp_path):
    root = tmp_path / "stg"
    for i in range(15):
        _write_card(
            root,
            f"c{i}.md",
            f"""---
uid: uid-{i}
type: purchase
sent_at: "2024-01-01T00:00:00Z"
---
b
""",
        )
    r = staging_report(str(root))
    pu = next(t for t in r.types if t.card_type == "purchase")
    assert len(pu.sample_uids) <= 10
