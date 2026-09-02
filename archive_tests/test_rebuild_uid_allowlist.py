"""Dirty-UID rebuild must stay incremental and never escalate to full-scan."""

from __future__ import annotations

from pathlib import Path

from archive_cli.loader import resolve_uid_allowlist_rebuild


def test_allowlist_intersection_is_incremental() -> None:
    mode, materialize, missing = resolve_uid_allowlist_rebuild(
        {"hfa-a", "hfa-b", "hfa-missing"},
        {"hfa-a", "hfa-b", "hfa-c"},
    )
    assert mode == "incremental"
    assert materialize == {"hfa-a", "hfa-b"}
    assert missing == {"hfa-missing"}


def test_allowlist_all_missing_is_noop() -> None:
    mode, materialize, missing = resolve_uid_allowlist_rebuild(
        {"hfa-gone"},
        {"hfa-a"},
    )
    assert mode == "noop"
    assert materialize == set()
    assert missing == {"hfa-gone"}


def test_allowlist_does_not_escalate_when_every_present_uid_is_dirty() -> None:
    """Classification used to return full when materialize_uids >= all rows."""
    present = {f"hfa-{i}" for i in range(20)}
    mode, materialize, missing = resolve_uid_allowlist_rebuild(present, present)
    assert mode == "incremental"
    assert materialize == present
    assert missing == set()


def test_allowlist_strips_blank_uids() -> None:
    mode, materialize, missing = resolve_uid_allowlist_rebuild(
        {"hfa-a", "", "  "},
        {"hfa-a"},
    )
    assert mode == "incremental"
    assert materialize == {"hfa-a"}
    assert missing == set()


def test_collect_allowlist_does_not_full_scan_frontmatter(tmp_path: Path) -> None:
    from archive_cli.scanner import _collect_canonical_rows
    from archive_cli.vault_cache import VaultScanCache
    from archive_tests.fixtures import load_fixture_vault

    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    uid_map = cache.uid_to_rel_path()
    wanted = set(list(uid_map.keys())[:2])
    calls = {"n": 0}
    orig = cache.frontmatter_for_rel_path

    def spy(rel_path: str) -> dict:
        calls["n"] += 1
        return orig(rel_path)

    cache.frontmatter_for_rel_path = spy  # type: ignore[method-assign]
    rows, slug_map, dup_count, *_rest = _collect_canonical_rows(
        vault,
        cache=cache,
        uid_allowlist=wanted,
        progress_every=0,
    )
    assert calls["n"] == 0
    assert dup_count == 0
    assert wanted <= {str(row.card.uid).strip() for row in rows}
    assert len(slug_map) == len(cache.all_rel_paths())
