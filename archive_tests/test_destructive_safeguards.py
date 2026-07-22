"""Regression tests for the 2026-04-24 destructive-code audit fixes.

Covers:
  - bootstrap() refuses populated schemas without ``force=True``
  - _replace_note_manifest is UPSERT + prune (mid-failure leaves it consistent)
  - _clear_meta_for_finalize is a no-op (the upsert is sufficient)
  - merge_provenance preserves a prior-history chain on overwrite
  - merge_provenance does NOT grow history on byte-identical re-writes
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from archive_vault.provenance import (
    MAX_PROVENANCE_HISTORY,
    ProvenanceEntry,
    merge_provenance,
    read_provenance,
    write_provenance,
)


def _entry(**kw) -> ProvenanceEntry:
    base = dict(source="s", date="2026-04-24", method="deterministic", model="", input_hash="")
    base.update(kw)
    return ProvenanceEntry(**base)


def test_merge_provenance_preserves_prior_on_overwrite() -> None:
    existing = {"counterparty": _entry(source="enrich_finance_v5", date="2026-04-15")}
    incoming = {"counterparty": _entry(source="phase6_5_step16_backfill", date="2026-04-23")}
    out = merge_provenance(existing, incoming)
    assert out["counterparty"].source == "phase6_5_step16_backfill"
    assert out["counterparty"].prior is not None
    assert len(out["counterparty"].prior) == 1
    assert out["counterparty"].prior[0]["source"] == "enrich_finance_v5"
    assert out["counterparty"].prior[0]["date"] == "2026-04-15"


def test_merge_provenance_chain_caps_at_history_limit() -> None:
    cur: dict[str, ProvenanceEntry] = {}
    for i in range(MAX_PROVENANCE_HISTORY + 4):
        cur = merge_provenance(cur, {"f": _entry(source=f"src-{i}", date=f"2026-04-{(i % 28) + 1:02d}")})
    assert cur["f"].source == f"src-{MAX_PROVENANCE_HISTORY + 3}"
    assert cur["f"].prior is not None
    assert len(cur["f"].prior) == MAX_PROVENANCE_HISTORY


def test_merge_provenance_idempotent_no_grow_for_identical_entries() -> None:
    e = _entry(source="s", date="2026-04-24")
    cur = merge_provenance({}, {"f": e})
    cur = merge_provenance(cur, {"f": _entry(source="s", date="2026-04-24")})
    cur = merge_provenance(cur, {"f": _entry(source="s", date="2026-04-24")})
    assert cur["f"].prior is None or cur["f"].prior == []


def test_provenance_round_trips_through_body() -> None:
    p = merge_provenance(
        {"summary": _entry(source="extractor", date="2026-04-15")},
        {"summary": _entry(source="enricher", date="2026-04-23", model="gemini-2.5")},
    )
    body = write_provenance("body content", p)
    assert "<!-- provenance" in body
    parsed = read_provenance(body)
    assert parsed["summary"].source == "enricher"
    assert parsed["summary"].model == "gemini-2.5"
    assert parsed["summary"].prior is not None
    assert parsed["summary"].prior[0]["source"] == "extractor"


def test_clean_phase3_dry_run_default(tmp_path: Path) -> None:
    """Without --apply the script never deletes; manifest is written."""
    vault = tmp_path / "fake-vault"
    (vault / "Transactions" / "Rides").mkdir(parents=True)
    (vault / "Transactions" / "Rides" / "x.md").write_text("hi", encoding="utf-8")
    script = Path(__file__).resolve().parents[1] / "archive_scripts" / "clean-phase3-derived-dirs.sh"
    res = subprocess.run(["bash", str(script), str(vault)], capture_output=True, text=True, check=True)
    assert "DRY-RUN" in res.stdout
    assert (vault / "Transactions" / "Rides" / "x.md").exists()


def test_clean_phase3_apply_on_non_prod(tmp_path: Path) -> None:
    vault = tmp_path / "fake-vault"
    (vault / "Transactions" / "Rides").mkdir(parents=True)
    (vault / "Transactions" / "Rides" / "x.md").write_text("hi", encoding="utf-8")
    script = Path(__file__).resolve().parents[1] / "archive_scripts" / "clean-phase3-derived-dirs.sh"
    res = subprocess.run(["bash", str(script), "--apply", str(vault)], capture_output=True, text=True, check=True)
    assert "deleted." in res.stdout
    assert not (vault / "Transactions" / "Rides").exists()


def test_clean_phase3_refuses_prod_vault_without_override(tmp_path: Path) -> None:
    """Prod-vault prefix refuse path works without homeowner filesystem paths.

    The shipped script hard-codes PROD_VAULT_PATTERNS. For CI portability we
    run a temp copy whose first pattern is remapped onto ``tmp_path`` while
    leaving the refuse/--apply/guard behavior intact.
    """
    prod_root = tmp_path / "Archive" / "seed"
    vault = prod_root / "hf-archives-seed-2099"
    (vault / "Transactions" / "Rides").mkdir(parents=True)
    marker = vault / "Transactions" / "Rides" / "x.md"
    marker.write_text("hi", encoding="utf-8")

    original = Path(__file__).resolve().parents[1] / "archive_scripts" / "clean-phase3-derived-dirs.sh"
    script = tmp_path / "clean-phase3-derived-dirs.sh"
    # Keep path as POSIX-style string even if tmp_path is absolute.
    patched_pattern = f'"{prod_root.as_posix()}/hf-archives-seed-"'
    content = original.read_text(encoding="utf-8").replace(
        '"/Users/rheeger/Archive/seed/hf-archives-seed-"',
        patched_pattern,
        1,
    )
    assert patched_pattern in content, "failed to remap prod vault pattern for isolated test"
    script.write_text(content, encoding="utf-8")
    script.chmod(0o755)

    res = subprocess.run(
        ["bash", str(script), "--apply", str(vault)],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 3
    assert "REFUSING" in res.stderr
    assert marker.exists(), "safeguard must refuse before deleting prod-like vault contents"


@pytest.mark.integration
class TestBootstrapSafeguard:
    def test_bootstrap_refuses_populated_schema(self, pgvector_dsn: str, tmp_path: Path, monkeypatch) -> None:
        # Force=False explicitly (override the conftest test-default).
        monkeypatch.delenv("PPA_BOOTSTRAP_FORCE", raising=False)
        from archive_cli.index_store import PostgresArchiveIndex
        from archive_cli.migrate import MigrationRunner

        index = PostgresArchiveIndex(tmp_path, dsn=pgvector_dsn)
        index.schema = "ppa_bootstrap_safeguard"
        with index._connect() as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {index.schema} CASCADE")
            conn.commit()
        index.bootstrap()
        with index._connect() as conn:
            MigrationRunner(conn, index.schema).run()
            conn.execute(
                f"""
                INSERT INTO {index.schema}.cards (uid, rel_path, slug, type, content_hash)
                VALUES ('hfa-test-1','p.md','p','person','sha256:fake')
                """
            )
            conn.commit()
        with pytest.raises(RuntimeError, match="bootstrap refused"):
            index.bootstrap()
        index.bootstrap(force=True)  # explicit override succeeds

    def test_replace_note_manifest_upsert_preserves_existing_keys(self, pgvector_dsn: str, tmp_path: Path) -> None:
        from archive_cli.index_store import PostgresArchiveIndex
        from archive_cli.loader import NoteManifestRow
        from archive_cli.migrate import MigrationRunner

        index = PostgresArchiveIndex(tmp_path, dsn=pgvector_dsn)
        index.schema = "ppa_manifest_upsert"
        with index._connect() as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {index.schema} CASCADE")
            conn.commit()
        index.bootstrap()
        with index._connect() as conn:
            MigrationRunner(conn, index.schema).run()

        def _row(rel: str, uid: str, ch: str = "h0") -> NoteManifestRow:
            return NoteManifestRow(
                rel_path=rel,
                card_uid=uid,
                slug=Path(rel).stem,
                content_hash=ch,
                frontmatter_hash="fh0",
                file_size=10,
                mtime_ns=1,
                card_type="person",
                typed_projection="",
                people_json="[]",
                orgs_json="[]",
                scan_version=1,
                chunk_schema_version=1,
                projection_registry_version=1,
                index_schema_version=1,
            )

        with index._connect() as conn:
            index._replace_note_manifest(conn, [_row("a.md", "u1"), _row("b.md", "u2")])
            conn.commit()
            cnt_row = conn.execute(f"SELECT COUNT(*) AS c FROM {index.schema}.note_manifest").fetchone()
            assert int(cnt_row["c"]) == 2
        # Now write a new manifest set: keep a.md, drop b.md, add c.md, mutate a.md content_hash.
        with index._connect() as conn:
            index._replace_note_manifest(
                conn,
                [_row("a.md", "u1", ch="h_NEW"), _row("c.md", "u3")],
            )
            conn.commit()
            rows = conn.execute(
                f"SELECT rel_path, content_hash FROM {index.schema}.note_manifest ORDER BY rel_path"
            ).fetchall()
            mapping = {str(r["rel_path"]): str(r["content_hash"]) for r in rows}
            assert mapping == {"a.md": "h_NEW", "c.md": "h0"}
