"""Step 14–15: fuzzy token_sort_ratio smoke + slice parity."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")


def test_token_sort_ratio_symmetric():
    import archive_crate

    assert archive_crate.token_sort_ratio("Jane Doe", "Doe Jane") == pytest.approx(100.0, abs=0.01)


def test_resolve_person_batch_rust_matches_python(tmp_path):
    """Step 14 — Rust batch resolve matches Python on fixture vault."""
    import archive_crate
    from archive_tests.fixtures import load_fixture_vault
    from archive_vault.identity_resolver import resolve_person_batch as py_resolve_batch

    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    ids = [
        {"name": "Nobody Zzzunknown123"},
        {"name": "Someone Else Unknown456"},
        {"name": "Jane Smith"},
        {"emails": ["jane@example.com"]},
    ]
    py_out = py_resolve_batch(vault, ids)
    rs_out = archive_crate.resolve_person_batch(str(vault), ids)
    assert len(py_out) == len(rs_out)
    for a, b in zip(py_out, rs_out):
        assert a.action == b.action
        assert abs(a.confidence - b.confidence) <= 1, (
            f"confidence divergence: py={a.confidence} rs={b.confidence}"
        )
        if a.action == "create":
            assert a.wikilink == b.wikilink
        assert sorted(a.reasons) == sorted(b.reasons)


_DEFAULT_SLICE = ".slices/1pct"


def _resolve_slice_path() -> str | None:
    raw = os.environ.get("PPA_RESOLVE_PARITY_SLICE", "").strip()
    if raw:
        return raw
    p = Path(_DEFAULT_SLICE)
    if p.is_dir():
        return str(p)
    return None


def _collect_identifiers_from_slice(slice_path: str) -> list[dict]:
    """Extract person identifiers from derived + finance cards via Rust cache."""
    import archive_crate
    from archive_sync.extractors.entity_resolution import DERIVED_ENTITY_CARD_TYPES, _person_names_from_derived_card

    cache_path = os.path.join(slice_path, "_meta", "vault-scan-cache.sqlite3")
    if not os.path.exists(cache_path):
        pytest.skip(f"No tier-2 cache at {cache_path}")

    derived = archive_crate.frontmatter_dicts_from_cache(
        cache_path, types=list(DERIVED_ENTITY_CARD_TYPES),
    )
    batch_ids: list[dict] = []
    for row in derived:
        fm = row["frontmatter"]
        for raw in _person_names_from_derived_card(fm):
            batch_ids.append({"name": raw})

    finance = archive_crate.frontmatter_dicts_from_cache(cache_path, types=["finance"])
    for row in finance:
        cp = row["frontmatter"].get("counterparty")
        if cp and isinstance(cp, str) and cp.strip():
            batch_ids.append({"name": cp.strip()})

    return batch_ids


@pytest.mark.integration
@pytest.mark.slow
def test_resolve_person_batch_rust_matches_python_on_slice():
    """Step 15 — Rust vs Python resolve on ≥100 identifiers from a real vault slice.

    Uses ``PPA_RESOLVE_PARITY_SLICE`` env var or defaults to ``.slices/1pct``.
    """
    import archive_crate
    from archive_vault.identity_resolver import resolve_person_batch as py_resolve_batch

    slice_path = _resolve_slice_path()
    if slice_path is None:
        pytest.skip("No slice available (set PPA_RESOLVE_PARITY_SLICE or place .slices/1pct)")

    slice_path = str(Path(slice_path).resolve())
    batch_ids = _collect_identifiers_from_slice(slice_path)
    if len(batch_ids) < 100:
        pytest.skip(f"Only {len(batch_ids)} identifiers in slice (need ≥100)")

    py_results = py_resolve_batch(slice_path, batch_ids)
    rs_results = archive_crate.resolve_person_batch(slice_path, batch_ids)

    assert len(py_results) == len(rs_results), (
        f"Length mismatch: Python={len(py_results)}, Rust={len(rs_results)}"
    )

    hard_mismatches = []
    for i, (py, rs) in enumerate(zip(py_results, rs_results)):
        if py.action != rs.action or py.wikilink != rs.wikilink:
            hard_mismatches.append(
                f"[{i}] ids={batch_ids[i]} "
                f"py(action={py.action}, wiki={py.wikilink}, conf={py.confidence}) "
                f"rs(action={rs.action}, wiki={rs.wikilink}, conf={rs.confidence})"
            )
        else:
            assert abs(py.confidence - rs.confidence) <= 1, (
                f"[{i}] confidence divergence >1: py={py.confidence} rs={rs.confidence} "
                f"ids={batch_ids[i]}"
            )

    assert not hard_mismatches, (
        f"{len(hard_mismatches)} action/wikilink mismatches:\n" + "\n".join(hard_mismatches[:20])
    )
