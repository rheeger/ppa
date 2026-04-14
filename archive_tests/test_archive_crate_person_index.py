"""Step 13 — Rust PersonResolutionIndex vs hfa.identity_resolver.PersonIndex."""

from __future__ import annotations

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")


def _python_email_to_wikilink(py_idx) -> dict[str, str]:
    out: dict[str, str] = {}
    for wikilink, data in py_idx.records.items():
        emails = data.get("emails") or []
        if not isinstance(emails, list):
            continue
        for e in emails:
            if not isinstance(e, str) or not e.strip():
                continue
            out[e.strip().lower()] = wikilink
    return out


def test_person_index_email_matches_python(tmp_path):
    """Normalized email → wikilink agrees with Python PersonIndex."""
    import archive_crate
    from archive_vault.identity_resolver import PersonIndex

    (tmp_path / "People").mkdir(parents=True)
    (tmp_path / "People" / "alice-test.md").write_text(
        """---
uid: hfa-person-aliceidx001
type: person
source:
  - manual
source_id: alice@example.com
created: "2025-06-01"
updated: "2025-06-15"
summary: Alice Test
first_name: Alice
last_name: Test
emails:
  - alice@example.com
people: []
orgs: []
tags: []
---

Body.
""",
        encoding="utf-8",
    )

    py_idx = PersonIndex(str(tmp_path), preload=True)
    rust_idx = archive_crate.build_person_index(str(tmp_path))

    assert len(rust_idx) == len(py_idx.records)
    py_map = _python_email_to_wikilink(py_idx)
    for norm_email, wikilink in py_map.items():
        assert rust_idx.wikilink_for_email(norm_email) == wikilink


def test_person_index_last_name_candidates_match_python(tmp_path):
    """by_last_name / first_initial bucket matches Python candidate narrowing keys."""
    import archive_crate
    from archive_vault.identity_resolver import PersonIndex

    (tmp_path / "People").mkdir(parents=True)
    (tmp_path / "People" / "bob-smith.md").write_text(
        """---
uid: hfa-person-bobidx001
type: person
source:
  - manual
source_id: bob@example.com
created: "2025-06-01"
updated: "2025-06-15"
summary: Bob Smith
first_name: Bob
last_name: Smith
people: []
orgs: []
tags: []
---

""",
        encoding="utf-8",
    )

    py_idx = PersonIndex(str(tmp_path), preload=True)
    rust_idx = archive_crate.build_person_index(str(tmp_path))

    wikilink = "[[bob-smith]]"
    assert wikilink in py_idx.records
    assert rust_idx.has_wikilink(wikilink)

    # identity_resolver normalizes "smith" for last-name index
    from archive_vault.identity_resolver import normalize_person_name

    last = normalize_person_name("Smith")
    rust_list = rust_idx.wikilinks_for_last_name(last)
    py_set = py_idx.by_last_name.get(last, set())
    assert set(rust_list) == py_set

    first, _last = "bob", last  # normalized
    fi = first[:1]
    rust_fi = rust_idx.wikilinks_for_first_initial_last(last, fi[0])
    py_fi = py_idx.by_first_initial_last.get((last, fi), set())
    assert set(rust_fi) == py_fi


def test_person_index_from_cache_sqlite(tmp_path):
    """Optional cache_path build matches vault-only when tier-2 cache is populated."""
    import archive_crate

    (tmp_path / "People").mkdir(parents=True)
    (tmp_path / "People" / "carol.md").write_text(
        """---
uid: hfa-person-carolidx001
type: person
source:
  - manual
source_id: carol@zed.example
created: "2025-06-01"
updated: "2025-06-15"
summary: Carol Zed
emails:
  - carol@zed.example
people: []
orgs: []
tags: []
---

""",
        encoding="utf-8",
    )

    cache_db = tmp_path / "cache.sqlite3"
    archive_crate.build_vault_cache(str(tmp_path), str(cache_db), 2)

    from_vault = archive_crate.build_person_index(str(tmp_path))
    from_cache = archive_crate.build_person_index(str(tmp_path), str(cache_db))

    assert len(from_vault) == len(from_cache)
    w = from_vault.wikilink_for_email("carol@zed.example")
    assert w == "[[carol]]"
    assert from_cache.wikilink_for_email("carol@zed.example") == w
