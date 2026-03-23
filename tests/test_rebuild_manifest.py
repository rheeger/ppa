"""Unit tests for manifest classification helpers (no Postgres required)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from archive_mcp.index_store import (NoteManifestRow,  # noqa: E402
                                     _classify_manifest_rebuild_delta,
                                     _vault_paths_and_fingerprint)


def _row(rel_path: str, uid: str, card_type: str = "document") -> MagicMock:
    row = MagicMock()
    row.rel_path = rel_path
    row.card.uid = uid
    row.card.type = card_type
    row.frontmatter = {"uid": uid, "type": card_type}
    return row


def test_classify_duplicate_uid_forces_full() -> None:
    mode, mat, purge, _ = _classify_manifest_rebuild_delta(
        [],
        manifest_by_path={},
        file_stats={},
        versions=(1, 1, 1),
        duplicate_uid_count=1,
    )
    assert mode == "full"
    assert not mat and not purge


def test_classify_all_new_manifest_empty_is_full() -> None:
    rows = [_row("a.md", "u1"), _row("b.md", "u2")]
    stats = {"a.md": (1, 10), "b.md": (2, 20)}
    mode, mat, purge, counters = _classify_manifest_rebuild_delta(
        rows,
        manifest_by_path={},
        file_stats=stats,
        versions=(8, 4, 1),
        duplicate_uid_count=0,
    )
    assert mode == "full"


def test_classify_unchanged_noop() -> None:
    versions = (8, 4, 1)
    m = NoteManifestRow(
        rel_path="a.md",
        card_uid="u1",
        slug="a",
        content_hash="x",
        frontmatter_hash="will_replace",
        file_size=10,
        mtime_ns=1,
        card_type="document",
        typed_projection="",
        people_json="[]",
        orgs_json="[]",
        scan_version=1,
        chunk_schema_version=versions[1],
        projection_registry_version=versions[2],
        index_schema_version=versions[0],
    )
    row = _row("a.md", "u1", "document")
    from archive_mcp.index_store import _frontmatter_hash_stable

    m = NoteManifestRow(
        rel_path=m.rel_path,
        card_uid=m.card_uid,
        slug=m.slug,
        content_hash=m.content_hash,
        frontmatter_hash=_frontmatter_hash_stable(row.frontmatter),
        file_size=m.file_size,
        mtime_ns=m.mtime_ns,
        card_type=m.card_type,
        typed_projection=m.typed_projection,
        people_json=m.people_json,
        orgs_json=m.orgs_json,
        scan_version=m.scan_version,
        chunk_schema_version=m.chunk_schema_version,
        projection_registry_version=m.projection_registry_version,
        index_schema_version=m.index_schema_version,
    )
    mode, mat, purge, counters = _classify_manifest_rebuild_delta(
        [row],
        manifest_by_path={"a.md": m},
        file_stats={"a.md": (1, 10)},
        versions=versions,
        duplicate_uid_count=0,
    )
    assert mode == "noop"
    assert counters["unchanged"] == 1


def test_classify_person_change_escalates_full() -> None:
    versions = (8, 4, 1)
    from archive_mcp.index_store import _frontmatter_hash_stable

    row = _row("People/x.md", "p1", "person")
    fm_hash = _frontmatter_hash_stable(row.frontmatter)
    m = NoteManifestRow(
        rel_path="People/x.md",
        card_uid="p1",
        slug="x",
        content_hash="h",
        frontmatter_hash=fm_hash + "diff",
        file_size=1,
        mtime_ns=1,
        card_type="person",
        typed_projection="people",
        people_json="[]",
        orgs_json="[]",
        scan_version=1,
        chunk_schema_version=versions[1],
        projection_registry_version=versions[2],
        index_schema_version=versions[0],
    )
    mode, _, _, _ = _classify_manifest_rebuild_delta(
        [row],
        manifest_by_path={"People/x.md": m},
        file_stats={"People/x.md": (1, 1)},
        versions=versions,
        duplicate_uid_count=0,
    )
    assert mode == "full"


def test_vault_fingerprint_stable(tmp_path: Path) -> None:
    (tmp_path / "n.md").write_text("hi", encoding="utf-8")
    stats, fp1 = _vault_paths_and_fingerprint(tmp_path, ["n.md"])
    _, fp2 = _vault_paths_and_fingerprint(tmp_path, ["n.md"])
    assert fp1 == fp2
    assert "n.md" in stats
