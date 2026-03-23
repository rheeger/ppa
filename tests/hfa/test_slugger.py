from hfa.slugger import normalize_for_slug, unique_slug


def test_normalize_for_slug_basic():
    assert normalize_for_slug("Robert A. Heeger III") == "robert-a-heeger-iii"


def test_unique_slug_appends_hash_when_needed(tmp_vault):
    (tmp_vault / "People" / "jane-smith.md").write_text("---\n---\n", encoding="utf-8")
    slug = unique_slug(tmp_vault, "jane-smith", "source-123")
    assert slug.startswith("jane-smith-")
    assert slug != "jane-smith"
