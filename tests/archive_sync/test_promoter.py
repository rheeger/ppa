"""Tests for staging -> vault promotion."""

from __future__ import annotations

from pathlib import Path

from archive_sync.extractors.promoter import (PromotionResult,
                                              _vault_rel_path_for_card,
                                              promote_staging)


def _meal_front(uid: str) -> str:
    return f"""---
uid: {uid}
type: meal_order
service: Test
restaurant: Cafe
sent_at: "2024-03-15T12:00:00Z"
items: []
---
# meal
"""


def test_promote_meal_order_to_correct_directory(tmp_path):
    vault = tmp_path / "vault"
    staging = tmp_path / "stg"
    (vault / "Email").mkdir(parents=True)
    staging.mkdir()
    uid = "hfa-meal_order-testpromo"
    (staging / "x.md").write_text(_meal_front(uid), encoding="utf-8")
    rel = _vault_rel_path_for_card("meal_order", uid, "2024-03-15T12:00:00Z")
    assert "Transactions/MealOrders/2024-03/" in rel
    r = promote_staging(str(vault), str(staging))
    assert r.moved == 1
    assert (vault / rel).is_file()
    assert not (staging / "x.md").exists()


def test_promote_place_to_entities_directory(tmp_path):
    vault = tmp_path / "vault"
    staging = tmp_path / "stg"
    (vault / "Entities" / "Places").mkdir(parents=True)
    staging.mkdir()
    uid = "hfa-place-testpromo"
    body = """---
uid: hfa-place-testpromo
type: place
name: Cafe
city: Brooklyn
created: "2024-03-15"
updated: "2024-03-15"
sent_at: "2024-03-15T12:00:00Z"
---
# p
"""
    (staging / "p.md").write_text(body, encoding="utf-8")
    rel = _vault_rel_path_for_card("place", uid, "2024-03-15T12:00:00Z")
    assert rel.startswith("Entities/Places/")
    r = promote_staging(str(vault), str(staging))
    assert r.moved == 1
    assert (vault / rel).is_file()


def test_dry_run_reports_without_moving(tmp_path):
    vault = tmp_path / "vault"
    staging = tmp_path / "stg"
    staging.mkdir()
    (staging / "x.md").write_text(_meal_front("hfa-meal_order-dry"), encoding="utf-8")
    r = promote_staging(str(vault), str(staging), dry_run=True)
    assert r.moved == 1
    assert not list(vault.rglob("*.md"))


def test_idempotent_skip_existing(tmp_path):
    vault = tmp_path / "vault"
    staging = tmp_path / "stg"
    staging.mkdir()
    uid = "hfa-meal_order-idem"
    text = _meal_front(uid)
    (staging / "x.md").write_text(text, encoding="utf-8")
    r1 = promote_staging(str(vault), str(staging))
    assert r1.moved == 1
    (staging / "x.md").write_text(text, encoding="utf-8")
    r2 = promote_staging(str(vault), str(staging))
    assert r2.skipped == 1
    assert r2.moved == 0


def test_overwrite_changed_content(tmp_path):
    vault = tmp_path / "vault"
    staging = tmp_path / "stg"
    staging.mkdir()
    uid = "hfa-meal_order-ow"
    v1 = _meal_front(uid)
    (staging / "x.md").write_text(v1, encoding="utf-8")
    promote_staging(str(vault), str(staging))
    v2 = v1.replace("# meal", "# meal v2")
    (staging / "y.md").write_text(v2, encoding="utf-8")
    r = promote_staging(str(vault), str(staging))
    assert r.moved == 1
    rel = _vault_rel_path_for_card("meal_order", uid, "2024-03-15T12:00:00Z")
    assert "v2" in (vault / rel).read_text(encoding="utf-8")


def test_missing_activity_at_uses_base_dir(tmp_path):
    uid = "hfa-meal_order-noact"
    rel = _vault_rel_path_for_card("meal_order", uid, "")
    assert rel.count("/") >= 2


def test_vault_rel_path_uses_card_contracts(tmp_path):
    rel = _vault_rel_path_for_card("grocery_order", "hfa-grocery_order-x", "2024-05-01T00:00:00Z")
    assert "Groceries" in rel or "Grocery" in rel


def test_promotion_result_counts(tmp_path):
    vault = tmp_path / "vault"
    staging = tmp_path / "stg"
    staging.mkdir()
    (staging / "a.md").write_text(_meal_front("hfa-meal_order-a"), encoding="utf-8")
    (staging / "b.md").write_text(_meal_front("hfa-meal_order-b"), encoding="utf-8")
    r = promote_staging(str(vault), str(staging))
    assert isinstance(r, PromotionResult)
    assert r.moved == 2
    assert r.errors == 0
