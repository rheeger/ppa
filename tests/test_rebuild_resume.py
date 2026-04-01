"""Checkpoint resume helpers."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from archive_mcp.index_config import CHUNK_SCHEMA_VERSION, INDEX_SCHEMA_VERSION
from archive_mcp.index_store import PostgresArchiveIndex
from archive_mcp.loader import _compute_run_id, _try_resume_checkpoint
from archive_mcp.projections.registry import PROJECTION_REGISTRY_VERSION
from tests.fixtures import load_fixture_vault
from tests.index_snapshot import snapshot_projection_state


def test_compute_run_id_stable() -> None:
    a = _compute_run_id("abc")
    b = _compute_run_id("abc")
    assert a == b


def test_compute_run_id_changes_with_schema_version(monkeypatch: pytest.MonkeyPatch) -> None:
    import archive_mcp.loader as loader

    a = _compute_run_id("x")
    monkeypatch.setattr(loader, "INDEX_SCHEMA_VERSION", INDEX_SCHEMA_VERSION + 99)
    b = _compute_run_id("x")
    assert a != b
    monkeypatch.setattr(loader, "INDEX_SCHEMA_VERSION", INDEX_SCHEMA_VERSION)


@pytest.mark.integration
def test_try_resume_returns_none_for_fresh_checkpoint(pgvector_dsn: str) -> None:
    from pathlib import Path

    from archive_mcp.index_store import PostgresArchiveIndex

    vault = Path(".")
    index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
    index.schema = "archive_resume_test"
    index.bootstrap()
    with index._connect() as conn:
        rid = _compute_run_id("fp")
        assert _try_resume_checkpoint(conn, index.schema, rid) == (None, 0)


@pytest.mark.integration
def test_try_resume_returns_path_for_matching_checkpoint(pgvector_dsn: str) -> None:
    from pathlib import Path

    from archive_mcp.index_store import PostgresArchiveIndex

    vault = Path(".")
    index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
    index.schema = "archive_resume_test2"
    index.bootstrap()
    rid = _compute_run_id("abc")
    with index._connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {index.schema}.rebuild_checkpoint (
                id, run_id, mode, last_committed_rel_path, last_committed_card_uid,
                loaded_card_count, loaded_row_counts_json, loaded_bytes_estimate,
                vault_manifest_hash, index_schema_version, chunk_schema_version,
                projection_registry_version, manifest_schema_version, duplicate_uid_rows_loaded,
                status, updated_at
            ) VALUES (
                1, %s, 'full', 'People/a.md', 'u1', 1, '{{}}', 0, 'fp',
                %s, %s, %s, 1, TRUE, 'in_progress', NOW()
            )
            ON CONFLICT (id) DO UPDATE SET
                run_id = EXCLUDED.run_id,
                last_committed_rel_path = EXCLUDED.last_committed_rel_path,
                loaded_card_count = EXCLUDED.loaded_card_count,
                status = EXCLUDED.status
            """,
            (rid, INDEX_SCHEMA_VERSION, CHUNK_SCHEMA_VERSION, PROJECTION_REGISTRY_VERSION),
        )
        conn.commit()
        path, loaded = _try_resume_checkpoint(conn, index.schema, rid)
        assert path == "People/a.md"
        assert loaded == 1


@pytest.mark.integration
def test_try_resume_returns_none_for_mismatched_run_id(pgvector_dsn: str) -> None:
    from pathlib import Path

    from archive_mcp.index_store import PostgresArchiveIndex

    vault = Path(".")
    index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
    index.schema = "archive_resume_mismatch"
    index.bootstrap()
    rid_ok = _compute_run_id("abc")
    rid_other = _compute_run_id("different_fp")
    with index._connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {index.schema}.rebuild_checkpoint (
                id, run_id, mode, last_committed_rel_path, last_committed_card_uid,
                loaded_card_count, loaded_row_counts_json, loaded_bytes_estimate,
                vault_manifest_hash, index_schema_version, chunk_schema_version,
                projection_registry_version, manifest_schema_version, duplicate_uid_rows_loaded,
                status, updated_at
            ) VALUES (
                1, %s, 'full', 'People/a.md', 'u1', 1, '{{}}', 0, 'fp',
                %s, %s, %s, 1, TRUE, 'in_progress', NOW()
            )
            ON CONFLICT (id) DO UPDATE SET
                run_id = EXCLUDED.run_id,
                last_committed_rel_path = EXCLUDED.last_committed_rel_path,
                status = EXCLUDED.status
            """,
            (rid_other, INDEX_SCHEMA_VERSION, CHUNK_SCHEMA_VERSION, PROJECTION_REGISTRY_VERSION),
        )
        conn.commit()
        assert _try_resume_checkpoint(conn, index.schema, rid_ok) == (None, 0)


@pytest.mark.integration
@pytest.mark.slow
def test_checkpoint_resume_sigterm_matches_fresh_full(
    pgvector_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Interrupt rebuild at ~50% checkpoint, resume, then match uninterrupted full rebuild."""
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    from hfa.vault import iter_note_paths

    total_cards = len(list(iter_note_paths(vault)))
    # Interrupt before the final cards so resume has work left (batch_size=1 checkpoints often).
    target_loaded = max(1, min(total_cards // 2, total_cards - 3))

    schema_resume = "archive_resume_sigterm"
    schema_ref = "archive_resume_sigterm_ref"
    ppa_root = Path(__file__).resolve().parents[1]

    idx = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
    idx.schema = schema_resume
    idx.bootstrap()

    env = os.environ.copy()
    env["PPA_PATH"] = str(vault)
    env["PPA_INDEX_DSN"] = pgvector_dsn
    env["PPA_INDEX_SCHEMA"] = schema_resume
    env["PPA_REBUILD_RESUME"] = "1"
    env["PPA_EMBEDDING_PROVIDER"] = "hash"
    env["PPA_EMBEDDING_MODEL"] = "archive-hash-dev"
    env["PPA_EMBEDDING_VERSION"] = "1"
    cmd = [
        sys.executable,
        "-m",
        "archive_mcp",
        "rebuild-indexes",
        "--workers",
        "1",
        "--batch-size",
        "1",
        "--commit-interval",
        "1",
        "--executor",
        "serial",
    ]
    proc = subprocess.Popen(cmd, cwd=str(ppa_root), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.time() + 180.0
    killed = False
    try:
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            try:
                from psycopg import connect

                with connect(pgvector_dsn) as conn:
                    row = conn.execute(
                        f"SELECT loaded_card_count FROM {schema_resume}.rebuild_checkpoint WHERE id = 1"
                    ).fetchone()
                    lc = int(row[0] if row and row[0] is not None else 0)
            except Exception:
                lc = 0
            if lc >= target_loaded:
                proc.send_signal(signal.SIGTERM)
                killed = True
                break
            time.sleep(0.08)
        proc.wait(timeout=120)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=30)

    assert killed, "did not reach checkpoint threshold before subprocess exit"

    # Complete the same resume path in-process (CLI subprocess can exit mid-finalize; loader must finish).
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema_resume)
    monkeypatch.setenv("PPA_REBUILD_RESUME", "1")
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.setenv("PPA_EMBEDDING_MODEL", "archive-hash-dev")
    monkeypatch.setenv("PPA_EMBEDDING_VERSION", "1")
    idx.rebuild_with_metrics(
        force_full=False,
        workers=1,
        batch_size=1,
        commit_interval=1,
        executor_kind="serial",
    )

    idx_ref = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
    idx_ref.schema = schema_ref
    idx_ref.bootstrap()
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema_ref)
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.setenv("PPA_EMBEDDING_MODEL", "archive-hash-dev")
    monkeypatch.setenv("PPA_EMBEDDING_VERSION", "1")
    monkeypatch.delenv("PPA_REBUILD_RESUME", raising=False)
    idx_ref.rebuild_with_metrics(
        force_full=True,
        workers=1,
        batch_size=50,
        commit_interval=50,
        executor_kind="serial",
    )

    with idx._connect() as conn:
        snap_resume = snapshot_projection_state(conn, schema_resume)
    with idx_ref._connect() as conn:
        snap_ref = snapshot_projection_state(conn, schema_ref)

    assert snap_resume == snap_ref
