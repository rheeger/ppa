"""Archive MCP server tests."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

import archive_mcp.__main__ as archive_main
import archive_mcp.commands._resolve as resolve_mod
import archive_mcp.commands.seed_links as seed_links_cmd
import archive_mcp.server as archive_server
from archive_mcp.commands._resolve import get_index
from archive_mcp.embedding_provider import (
    HashEmbeddingProvider,
    OpenAIEmbeddingProvider,
    _resolve_openai_api_key,
    _resolve_service_account_token,
    get_embedding_provider,
)
from archive_mcp.index_store import PostgresArchiveIndex
from archive_mcp.server import (  # type: ignore[import-not-found]
    archive_bootstrap_postgres,
    archive_duplicate_uids,
    archive_duplicates,
    archive_embed_pending,
    archive_embedding_backlog,
    archive_embedding_status,
    archive_graph,
    archive_hybrid_search,
    archive_index_status,
    archive_link_candidate,
    archive_link_candidates,
    archive_link_quality_gate,
    archive_person,
    archive_query,
    archive_read,
    archive_rebuild_indexes,
    archive_review_link_candidate,
    archive_search,
    archive_seed_link_backfill,
    archive_seed_link_surface,
    archive_stats,
    archive_timeline,
    archive_validate,
    archive_vector_search,
)
from hfa.provenance import ProvenanceEntry
from hfa.schema import FinanceCard, PersonCard
from hfa.vault import write_card


class FakeIndex:
    location = "postgresql://archive:archive@localhost:5432/archive"

    def search(self, query: str, limit: int = 20, **_kwargs):
        return [
            {
                "rel_path": "People/jane-smith.md",
                "summary": "Jane Smith",
                "matched_by": "lexical",
            }
        ]

    def read_path_for_uid(self, uid: str):
        if uid == "hfa-person-aaaabbbbcccc":
            return "People/jane-smith.md"
        return None

    def query_cards(self, **_kwargs):
        return [
            {
                "rel_path": "People/jane-smith.md",
                "summary": "Jane Smith",
                "type": "person",
                "activity_at": "2026-03-06",
            }
        ]

    def graph(self, note_path: str, hops: int = 2):
        if note_path == "People/jane-smith.md":
            return {"People/jane-smith.md": ["People/mary-jones.md"]}
        return None

    def person_path(self, name: str):
        if name.replace(" ", "-").lower() == "jane-smith":
            return "People/jane-smith.md"
        return None

    def timeline(self, **_kwargs):
        return [
            {
                "created": "2026-03-06",
                "rel_path": "People/jane-smith.md",
                "summary": "Jane Smith",
            }
        ]

    def stats(self):
        return 2, [{"type": "person", "count": 2}], [{"source": "linkedin", "count": 1}]

    def rebuild(self):
        return {
            "cards": 3,
            "external_ids": 3,
            "edges": 1,
            "chunks": 4,
            "duplicate_uids": 0,
        }

    def status(self):
        return {
            "card_count": "3",
            "external_id_count": "3",
            "chunk_count": "4",
            "schema_version": "3",
        }

    def duplicate_uid_rows(self, *, limit: int = 20):
        return [
            {
                "uid": "hfa-imessage-thread-duplicate",
                "preferred_rel_path": "IMessageThreads/2015-01/hfa-imessage-thread-duplicate.md",
                "preferred_type": "imessage_thread",
                "preferred_source_id": "chat-1",
                "preferred_summary": "Chat one",
                "duplicate_rel_path": "IMessageThreads/2018-12/hfa-imessage-thread-duplicate.md",
                "duplicate_type": "imessage_thread",
                "duplicate_source_id": "chat-1",
                "duplicate_summary": "Chat one duplicate",
                "duplicate_group_size": 2,
            }
        ][:limit]

    def embedding_status(self, *, embedding_model: str, embedding_version: int):
        return {
            "embedding_model": embedding_model,
            "embedding_version": embedding_version,
            "chunk_schema_version": 4,
            "chunk_count": 4,
            "embedded_chunk_count": 2,
            "pending_chunk_count": 2,
        }

    def embedding_backlog(self, *, embedding_model: str, embedding_version: int, limit: int = 20):
        return [
            {
                "rel_path": "People/jane-smith.md",
                "chunk_type": "summary",
                "chunk_index": 0,
                "content": "Jane Smith",
                "token_count": 2,
            }
        ][:limit]

    def embed_pending(
        self,
        *,
        provider,
        embedding_model: str,
        embedding_version: int,
        limit: int = 20,
        **kwargs,
    ):
        return {
            "provider": getattr(provider, "name", "unknown"),
            "embedding_model": embedding_model,
            "embedding_version": embedding_version,
            "chunk_schema_version": 4,
            "batch_size": 2,
            "failed": 0,
            "embedded": min(limit, 2),
        }

    def vector_search(
        self,
        *,
        query_vector,
        embedding_model: str,
        embedding_version: int,
        limit: int = 20,
        **_kwargs,
    ):
        return [
            {
                "card_uid": "hfa-person-aaaabbbbcccc",
                "rel_path": "People/jane-smith.md",
                "summary": "Jane Smith",
                "type": "person",
                "matched_by": "vector",
                "score": 1.42,
                "chunk_type": "person_profile",
                "chunk_index": 0,
                "preview": "Jane Smith works at Endaoment",
                "matched_chunk_count": 2,
                "provenance_bias": "deterministic",
                "similarity": 0.99,
            }
        ][:limit]

    def hybrid_search(
        self,
        *,
        query: str,
        query_vector,
        embedding_model: str,
        embedding_version: int,
        limit: int = 20,
        **_kwargs,
    ):
        return [
            {
                "card_uid": "hfa-person-aaaabbbbcccc",
                "rel_path": "People/jane-smith.md",
                "summary": "Jane Smith",
                "type": "person",
                "matched_by": "hybrid",
                "score": 4.2,
                "lexical_score": 0.8,
                "vector_similarity": 0.99,
                "exact_match": True,
                "graph_hops": "",
                "chunk_type": "person_profile",
                "chunk_index": 0,
                "provenance_bias": "deterministic",
                "provenance_score": 0.08,
                "preview": "Jane Smith works at Endaoment",
            }
        ][:limit]

    def fetch_hybrid_lexical_vector(
        self,
        *,
        query: str,
        query_vector,
        embedding_model: str,
        embedding_version: int,
        candidate_limit: int = 20,
        **kwargs,
    ):
        rows = self.hybrid_search(
            query=query,
            query_vector=query_vector,
            embedding_model=embedding_model,
            embedding_version=embedding_version,
            limit=candidate_limit,
            **kwargs,
        )
        lexical_rows: list[dict] = []
        vector_rows: list[dict] = []
        for r in rows:
            if r.get("exact_match"):
                lexical_rows.append(
                    {
                        "card_uid": r["card_uid"],
                        "rel_path": r["rel_path"],
                        "summary": r["summary"],
                        "type": r["type"],
                        "activity_at": r.get("activity_at", ""),
                        "slug_exact": 1,
                        "summary_exact": 0,
                        "external_id_exact": 0,
                        "person_exact": 0,
                        "lexical_score": float(r.get("lexical_score", 0.8)),
                    }
                )
            vector_rows.append(
                {
                    "card_uid": r["card_uid"],
                    "rel_path": r["rel_path"],
                    "summary": r["summary"],
                    "type": r["type"],
                    "activity_at": r.get("activity_at", ""),
                    "matched_by": "vector",
                    "similarity": float(r.get("vector_similarity", 0.99)),
                    "preview": r.get("preview", r["summary"]),
                    "chunk_type": r.get("chunk_type", ""),
                    "chunk_index": int(r.get("chunk_index", 0)),
                    "matched_chunk_count": int(r.get("matched_chunk_count", 1)),
                    "provenance_bias": r.get("provenance_bias", "mixed"),
                    "provenance_score": float(r.get("provenance_score", 0.04)),
                    "score": 0.0,
                    "graph_hops": "",
                }
            )
        return lexical_rows, vector_rows

    def fetch_graph_neighbors_for_uids(self, anchor_uids):
        return set()


@pytest.fixture
def tmp_vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance").mkdir()
    (vault / "Attachments").mkdir()
    (vault / "_templates").mkdir()
    (vault / ".obsidian").mkdir()
    meta = vault / "_meta"
    meta.mkdir()
    (meta / "identity-map.json").write_text("{}", encoding="utf-8")
    (meta / "sync-state.json").write_text("{}", encoding="utf-8")
    (meta / "dedup-candidates.json").write_text(
        json.dumps(
            [
                {
                    "incoming": {"summary": "Jane Smyth"},
                    "existing": "[[jane-smith]]",
                    "confidence": 82,
                }
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://archive:archive@localhost:5432/archive")
    return vault


def _person_provenance(source: str) -> dict[str, ProvenanceEntry]:
    return {
        "summary": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "emails": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "company": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "tags": ProvenanceEntry(source, "2026-03-06", "deterministic"),
    }


def _finance_provenance(source: str) -> dict[str, ProvenanceEntry]:
    return {
        "summary": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "tags": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "amount": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "currency": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "counterparty": ProvenanceEntry(source, "2026-03-06", "deterministic"),
        "category": ProvenanceEntry(source, "2026-03-06", "deterministic"),
    }


def _seed_vault(vault: Path) -> PersonCard:
    jane = PersonCard(
        uid="hfa-person-aaaabbbbcccc",
        type="person",
        source=["contacts.apple", "linkedin"],
        source_id="jane@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Jane Smith",
        emails=["jane@example.com"],
        company="Endaoment",
        tags=["linkedin"],
    )
    mary = PersonCard(
        uid="hfa-person-ddddeeeeffff",
        type="person",
        source=["notion"],
        source_id="mary@example.com",
        created="2026-03-07",
        updated="2026-03-07",
        summary="Mary Jones",
        emails=["mary@example.com"],
        company="Acme",
    )
    finance = FinanceCard(
        uid="hfa-finance-111122223333",
        type="finance",
        source=["copilot"],
        source_id="2026-03-01:Flight:-120.0",
        created="2026-03-01",
        updated="2026-03-06",
        summary="Flight -120.00",
        tags=["copilot", "transaction", "travel"],
        amount=-120.0,
        currency="USD",
        counterparty="Flight",
        category="Travel",
    )
    write_card(
        vault,
        "People/jane-smith.md",
        jane,
        body="Connected to [[mary-jones]]",
        provenance=_person_provenance("contacts.apple"),
    )
    write_card(vault, "People/mary-jones.md", mary, provenance=_person_provenance("notion"))
    write_card(
        vault,
        "Finance/2026-03/hfa-finance-111122223333.md",
        finance,
        provenance=_finance_provenance("copilot"),
    )
    return jane


@pytest.fixture
def fake_index(monkeypatch: pytest.MonkeyPatch) -> FakeIndex:
    fake = FakeIndex()
    monkeypatch.setattr(resolve_mod, "get_index", lambda vault=None: fake)
    return fake


def test_archive_search_and_query(tmp_vault, fake_index):
    _seed_vault(tmp_vault)
    assert "jane-smith" in archive_search("Endaoment")
    assert "jane-smith" in archive_query(type_filter="person", source_filter="linkedin")


def test_archive_read_person_and_timeline(tmp_vault, fake_index):
    jane = _seed_vault(tmp_vault)
    assert "Jane Smith" in archive_read(jane.uid)
    assert "Jane Smith" in archive_person("jane-smith")
    timeline = archive_timeline(start_date="2026-03-01", end_date="2026-03-31")
    assert "People/jane-smith.md" in timeline


def test_archive_read_rejects_path_traversal(tmp_vault, fake_index):
    _seed_vault(tmp_vault)
    assert archive_read("../../etc/passwd.md") == "Not found"


def test_archive_graph_stats_and_duplicates(tmp_vault, fake_index):
    _seed_vault(tmp_vault)
    graph = archive_graph("People/jane-smith.md")
    assert "People/mary-jones.md" in graph
    stats = archive_stats()
    assert "person: 2" in stats
    assert "linkedin: 1" in stats
    duplicates = archive_duplicates()
    assert "Jane Smyth" in duplicates
    duplicate_uids = archive_duplicate_uids()
    assert "hfa-imessage-thread-duplicate" in duplicate_uids


def test_archive_validate_clean(tmp_vault):
    _seed_vault(tmp_vault)
    result = archive_validate()
    assert "0 errors" in result


def test_archive_rebuild_indexes_and_status(tmp_vault, fake_index):
    rebuilt = archive_rebuild_indexes()
    assert "cards: 3" in rebuilt
    assert "chunks: 4" in rebuilt
    assert "duplicate_uids: 0" in rebuilt
    status = archive_index_status()
    assert "card_count: 3" in status
    assert "chunk_count: 4" in status


def test_get_index_requires_postgres_dsn(tmp_vault, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    with pytest.raises(RuntimeError, match="PPA_INDEX_DSN is required"):
        get_index(tmp_vault)


def test_get_index_returns_postgres_backend(tmp_vault):
    index = get_index(tmp_vault)
    assert isinstance(index, PostgresArchiveIndex)
    assert index.dsn == "postgresql://archive:archive@localhost:5432/archive"


def test_archive_bootstrap_postgres_requires_dsn(tmp_vault, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    assert archive_bootstrap_postgres() == "PPA_INDEX_DSN is required"


def test_archive_bootstrap_postgres_uses_postgres_backend(tmp_vault, monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []

    def fake_bootstrap(self) -> dict[str, str]:
        calls.append(self.dsn)
        return {"backend": "postgres", "schema": self.schema}

    monkeypatch.setattr(PostgresArchiveIndex, "bootstrap", fake_bootstrap)
    result = archive_bootstrap_postgres()
    assert "backend: postgres" in result
    assert calls == ["postgresql://archive:archive@localhost:5432/archive"]


def test_postgres_bootstrap_command_dispatches(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from archive_mcp.commands import admin as admin_cmd

    vault = tmp_path / "vault"
    vault.mkdir()
    monkeypatch.setattr(archive_main, "resolve_store", lambda: type("S", (), {"vault": vault})())
    monkeypatch.setattr(admin_cmd, "bootstrap_postgres", lambda **kwargs: "bootstrapped")
    monkeypatch.setattr(sys, "argv", ["archive_mcp", "bootstrap-postgres"])
    archive_main.main()
    assert capsys.readouterr().out.strip() == "bootstrapped"


def test_postgres_embed_pending_command_dispatches(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from archive_mcp.commands import admin as admin_cmd

    vault = tmp_path / "vault"
    vault.mkdir()
    monkeypatch.setattr(archive_main, "resolve_store", lambda: type("S", (), {"vault": vault})())
    monkeypatch.setattr(admin_cmd, "embed_pending", lambda **kwargs: "embedded")
    monkeypatch.setattr(sys, "argv", ["archive_mcp", "embed-pending"])
    archive_main.main()
    assert capsys.readouterr().out.strip() == "embedded"


def test_seed_link_commands_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from archive_mcp.commands import seed_links as seed_cmd
    from archive_mcp.commands import status as status_cmd

    vault = tmp_path / "vault"
    vault.mkdir()
    monkeypatch.setattr(archive_main, "resolve_store", lambda: type("S", (), {"vault": vault})())
    monkeypatch.setattr(archive_main, "resolve_index", lambda vault=None: object())
    monkeypatch.setenv("PPA_SEED_LINKS_ENABLED", "1")
    monkeypatch.setattr(seed_cmd, "seed_link_surface", lambda **kwargs: "surface")
    monkeypatch.setattr(sys, "argv", ["archive_mcp", "seed-link-surface"])
    archive_main.main()
    assert capsys.readouterr().out.strip() == "surface"

    monkeypatch.setattr(seed_cmd, "seed_link_enqueue", lambda **kwargs: f"enqueue:{kwargs['job_type']}")
    monkeypatch.setattr(sys, "argv", ["archive_mcp", "seed-link-enqueue", "--job-type", "seed_backfill"])
    archive_main.main()
    assert capsys.readouterr().out.strip() == "enqueue:seed_backfill"

    monkeypatch.setattr(seed_cmd, "seed_link_backfill", lambda **kwargs: f"backfill:{kwargs['workers']}")
    monkeypatch.setattr(sys, "argv", ["archive_mcp", "seed-link-backfill", "--workers", "3"])
    archive_main.main()
    assert capsys.readouterr().out.strip() == "backfill:3"

    monkeypatch.setattr(
        seed_cmd,
        "seed_link_refresh",
        lambda **kwargs: f"refresh:{kwargs['source_uids']}",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["archive_mcp", "seed-link-refresh", "--source-uids", "hfa-person-1"],
    )
    archive_main.main()
    assert capsys.readouterr().out.strip() == "refresh:hfa-person-1"

    monkeypatch.setattr(seed_cmd, "seed_link_worker", lambda **kwargs: f"worker:{kwargs['workers']}")
    monkeypatch.setattr(sys, "argv", ["archive_mcp", "seed-link-worker", "--workers", "2"])
    archive_main.main()
    assert capsys.readouterr().out.strip() == "worker:2"

    monkeypatch.setattr(seed_cmd, "seed_link_promote", lambda **kwargs: f"promote:{kwargs['workers']}")
    monkeypatch.setattr(sys, "argv", ["archive_mcp", "seed-link-promote", "--workers", "2"])
    archive_main.main()
    assert capsys.readouterr().out.strip() == "promote:2"

    monkeypatch.setattr(
        seed_cmd,
        "seed_link_report",
        lambda **kwargs: f"report:{kwargs['rebuild_if_dirty']}",
    )
    monkeypatch.setattr(sys, "argv", ["archive_mcp", "seed-link-report"])
    archive_main.main()
    assert capsys.readouterr().out.strip() == "report:True"

    monkeypatch.setattr(status_cmd, "duplicate_uids", lambda **kwargs: "duplicate-uids")
    monkeypatch.setattr(sys, "argv", ["archive_mcp", "duplicate-uids"])
    archive_main.main()
    assert capsys.readouterr().out.strip() == "duplicate-uids"

    monkeypatch.setattr(seed_cmd, "link_quality_gate", lambda **kwargs: "gate")
    monkeypatch.setattr(sys, "argv", ["archive_mcp", "link-quality-gate"])
    archive_main.main()
    assert capsys.readouterr().out.strip() == "gate"


def test_benchmark_seed_links_command_dispatches(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
):
    monkeypatch.setenv("PPA_SEED_LINKS_ENABLED", "1")
    monkeypatch.setattr(
        archive_main,
        "benchmark_seed_links",
        lambda **kwargs: {"vault": str(kwargs["vault"]), "profile": kwargs["profile"]},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["archive_mcp", "benchmark-seed-links", "--vault", str(tmp_path / "vault")],
    )
    archive_main.main()
    output = capsys.readouterr().out.strip()
    assert '"profile": "local-laptop"' in output


def test_postgres_bootstrap_prepares_pgvector_ready_schema(tmp_vault, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PPA_SEED_LINKS_ENABLED", "1")
    index = PostgresArchiveIndex(tmp_vault, dsn="postgresql://archive:archive@localhost:5432/archive")
    executed: list[str] = []

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            executed.append(" ".join(str(sql).split()))
            return self

        def fetchone(self):
            return {"value": "3"}

        def fetchall(self):
            return []

        def commit(self):
            return None

    index._connect = lambda: FakeConnection()  # type: ignore[method-assign]
    result = index.bootstrap()
    assert result["backend"] == "postgres"
    assert any("CREATE EXTENSION IF NOT EXISTS vector" in sql for sql in executed)
    assert any(f"CREATE TABLE IF NOT EXISTS {index.schema}.chunks" in sql for sql in executed)
    assert any(f"CREATE TABLE IF NOT EXISTS {index.schema}.embeddings" in sql for sql in executed)
    assert any(f"CREATE TABLE IF NOT EXISTS {index.schema}.link_jobs" in sql for sql in executed)
    assert any(f"CREATE TABLE IF NOT EXISTS {index.schema}.link_candidates" in sql for sql in executed)
    assert any(f"CREATE TABLE IF NOT EXISTS {index.schema}.link_decisions" in sql for sql in executed)
    assert any(f"CREATE TABLE IF NOT EXISTS {index.schema}.promotion_queue" in sql for sql in executed)
    assert any(f"CREATE TABLE IF NOT EXISTS {index.schema}.duplicate_uid_rows" in sql for sql in executed)


def test_archive_embedding_status_and_backlog(tmp_vault, fake_index):
    status = archive_embedding_status(embedding_model="test-embed", embedding_version=1)
    assert "embedding_model: test-embed" in status
    assert "chunk_schema_version: 4" in status
    assert "pending_chunk_count: 2" in status

    backlog = archive_embedding_backlog(limit=3, embedding_model="test-embed", embedding_version=1)
    assert "People/jane-smith.md" in backlog
    assert "summary#0" in backlog


def test_archive_embed_pending_and_vector_hybrid_search(tmp_vault, fake_index, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    embed_result = archive_embed_pending(limit=2, embedding_model="test-embed", embedding_version=1)
    assert "Embedded chunks for test-embed v1" in embed_result
    assert "- embedded: 2" in embed_result
    assert "- failed: 0" in embed_result

    vector_result = archive_vector_search("Jane Smith", limit=3, embedding_model="test-embed", embedding_version=1)
    assert "Vector matches for test-embed v1" in vector_result
    assert "People/jane-smith.md" in vector_result
    assert "person_profile#0" in vector_result
    assert "matched_by=vector" in vector_result

    hybrid_result = archive_hybrid_search("Endaoment", limit=5, embedding_model="test-embed", embedding_version=1)
    assert "Hybrid matches for 'Endaoment'" in hybrid_result
    assert "People/jane-smith.md" in hybrid_result
    assert "matched_by=hybrid" in hybrid_result
    assert "exact_match=true" in hybrid_result


def test_archive_tool_profile_remote_read_blocks_sensitive_and_admin(fake_index, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PPA_MCP_TOOL_PROFILE", "remote-read")
    assert archive_search("Jane") != "Tool disabled by PPA_MCP_TOOL_PROFILE=remote-read"
    assert archive_read("People/jane-smith.md") == "Tool disabled by PPA_MCP_TOOL_PROFILE=remote-read"
    assert archive_rebuild_indexes() == "Tool disabled by PPA_MCP_TOOL_PROFILE=remote-read"


def test_archive_seed_link_surface_describes_scope(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PPA_SEED_LINKS_ENABLED", "1")
    surface = archive_seed_link_surface()
    assert "Archive seed link surface" in surface
    assert "message_in_thread" in surface
    assert "identityLinker" in surface


def test_archive_seed_link_wrappers(tmp_vault, monkeypatch: pytest.MonkeyPatch, fake_index):
    monkeypatch.setenv("PPA_SEED_LINKS_ENABLED", "1")

    mock_sl = {
        "run_seed_link_enqueue": lambda index, **kwargs: {
            "prepared": 8,
            "enqueued": 5,
            "existing": 3,
        },
        "run_seed_link_backfill": lambda index, **kwargs: {
            "workers": kwargs.get("max_workers", 0) or 4,
            "jobs_prepared": 8,
            "jobs_enqueued": 5,
            "jobs_existing": 3,
            "jobs_completed": 5,
            "jobs_failed": 0,
            "candidates": 12,
            "needs_review": 3,
            "auto_promoted": 4,
            "canonical_safe": 2,
            "derived_promotions_applied": 4,
            "canonical_applied": 2,
            "llm_judged": 1,
            "promotion_blocked": 0,
            "orphaned_links_before": 5,
            "orphaned_links_after": 1,
        },
        "run_incremental_link_refresh": lambda index, **kwargs: {
            "job_type": "incremental",
            "jobs_enqueued": 1,
            "jobs_completed": 1,
            "jobs_failed": 0,
            "candidates": 3,
            "needs_review": 1,
            "auto_promoted": 1,
            "canonical_safe": 1,
            "derived_promotions_applied": 1,
            "canonical_applied": 1,
        },
        "run_seed_link_workers": lambda index, **kwargs: {
            "workers": kwargs.get("max_workers", 0) or 2,
            "jobs_completed": 4,
            "jobs_failed": 0,
            "candidates": 9,
            "needs_review": 2,
            "auto_promoted": 1,
            "canonical_safe": 1,
            "llm_judged": 0,
            "module_metrics": {},
        },
        "run_seed_link_promotion_workers": lambda index, **kwargs: {
            "derived_edge": 2,
            "canonical_field": 1,
            "blocked": 0,
        },
        "run_seed_link_report": lambda index, **kwargs: {
            "rebuilt": True,
            "passes": True,
            "seed_card_count": 100,
            "reviewable_seed_card_count": 80,
            "total_cards_reviewed": 80,
            "scan_coverage": 1.0,
            "orphaned_links_after": 0,
            "duplicate_uid_count": 0,
            "high_priority_review_backlog": 3,
            "high_risk_precision": 1.0,
        },
        "list_link_candidates": lambda index, **kwargs: [
            {
                "candidate_id": 11,
                "module_name": "communicationLinker",
                "source_rel_path": "Email/board-thread.md",
                "target_rel_path": "Email/board-message.md",
                "proposed_link_type": "thread_has_message",
                "status": "needs_review",
                "final_confidence": 0.77,
                "decision": "review",
                "decision_reason": "borderline",
                "promotion_status": "",
            }
        ],
        "get_link_candidate_details": lambda index, candidate_id: {
            "module_name": "communicationLinker",
            "proposed_link_type": "thread_has_message",
            "source_rel_path": "Email/board-thread.md",
            "target_rel_path": "Email/board-message.md",
            "status": "needs_review",
            "final_confidence": 0.77,
            "decision": "review",
            "decision_reason": "borderline",
            "deterministic_score": 1.0,
            "lexical_score": 0.1,
            "graph_score": 0.8,
            "llm_score": 0.0,
            "risk_penalty": 0.0,
            "promotion_target": "canonical_field",
            "promotion_status": "queued",
            "target_field_name": "messages",
            "blocked_reason": "",
            "llm_model": "",
            "evidence": [
                {
                    "evidence_type": "exact_thread_id",
                    "evidence_source": "frontmatter",
                    "feature_name": "thread_id",
                    "feature_value": "gmail-thread-1",
                    "feature_weight": 1.0,
                }
            ],
            "reviews": [],
        },
        "review_link_candidate": lambda index, **kwargs: {
            "status": "approved",
            "decision": "auto_promote",
            "final_confidence": 0.77,
        },
        "compute_link_quality_gate": lambda index: {
            "passes": True,
            "seed_card_count": 100,
            "total_cards_reviewed": 100,
            "scan_coverage": 1.0,
            "required_scan_coverage": 1.0,
            "orphaned_links_after": 0,
            "duplicate_uid_count": 0,
            "dead_end_count": 4,
            "high_priority_review_backlog": 3,
            "max_high_priority_review_backlog": 50,
            "high_risk_precision": 1.0,
            "required_high_risk_precision": 0.95,
            "candidate_counts": [
                {
                    "module_name": "communicationLinker",
                    "proposed_link_type": "thread_has_message",
                    "count": 2,
                }
            ],
            "auto_promoted_counts": [
                {
                    "module_name": "communicationLinker",
                    "proposed_link_type": "thread_has_message",
                    "count": 1,
                }
            ],
        },
        "get_seed_scope_rows": lambda: [],
        "get_surface_policy_rows": lambda: [],
    }
    monkeypatch.setattr(seed_links_cmd, "default_seed_link_imports", lambda: mock_sl)

    enqueue = archive_server.archive_seed_link_enqueue(job_type="seed_backfill")
    assert "prepared: 8" in enqueue
    assert "existing: 3" in enqueue

    backfill = archive_seed_link_backfill(workers=4)
    assert "jobs_completed: 5" in backfill
    assert "canonical_applied: 2" in backfill

    refresh = archive_server.archive_seed_link_refresh("hfa-person-1,hfa-email-2", workers=2)
    assert "job_type: incremental" in refresh
    assert "jobs_enqueued: 1" in refresh

    worker = archive_server.archive_seed_link_worker(workers=2)
    assert "jobs_completed: 4" in worker
    assert "canonical_safe: 1" in worker

    promote = archive_server.archive_seed_link_promote(workers=2)
    assert "derived_edge: 2" in promote
    assert "canonical_field: 1" in promote

    report = archive_server.archive_seed_link_report()
    assert "rebuilt: True" in report
    assert "scan_coverage: 1.0" in report

    candidates = archive_link_candidates(status="needs_review")
    assert "id=11" in candidates
    assert "thread_has_message" in candidates

    details = archive_link_candidate(11)
    assert "Archive link candidate 11" in details
    assert "deterministic=1.0000" in details

    reviewed = archive_review_link_candidate(11, reviewer="robbie", action="approve")
    assert "decision: auto_promote" in reviewed

    gate = archive_link_quality_gate()
    assert "passes: True" in gate
    assert "scan_coverage: 1.0" in gate


def test_get_embedding_provider_defaults_to_hash(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("PPA_EMBEDDING_PROVIDER", raising=False)
    provider = get_embedding_provider(model="test-model")
    assert isinstance(provider, HashEmbeddingProvider)
    assert provider.model == "test-model"


def test_get_embedding_provider_supports_openai(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    provider = get_embedding_provider(model="text-embedding-3-small")
    assert isinstance(provider, OpenAIEmbeddingProvider)
    assert provider.model == "text-embedding-3-small"


def test_openai_embedding_provider_parses_embedding_response(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    captured: dict[str, object] = {}

    def fake_post_json(url: str, headers: dict[str, str], payload: dict[str, object]):
        captured["url"] = url
        captured["headers"] = headers
        captured["payload"] = payload
        return {"data": [{"embedding": [0.1, 0.2, 0.3]}]}

    monkeypatch.setattr("archive_mcp.embedding_provider._post_json", fake_post_json)
    provider = OpenAIEmbeddingProvider(model="text-embedding-3-small", dimension=3)
    vectors = provider.embed_texts(["hello world"])
    assert vectors == [[0.1, 0.2, 0.3]]
    assert str(captured["url"]).endswith("/embeddings")
    assert captured["payload"] == {
        "model": "text-embedding-3-small",
        "input": ["hello world"],
        "dimensions": 3,
    }


def test_resolve_openai_api_key_from_arnold_vault_toggle(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("PPA_USE_ARNOLD_OPENAI_KEY", "1")
    monkeypatch.setenv("PPA_OPENAI_API_KEY_OP_REF", "op://Arnold/OPENAI_API_KEY/credential")
    monkeypatch.setattr(
        "archive_mcp.embedding_provider._read_1password_secret",
        lambda ref: "vault-key" if ref else None,
    )
    assert _resolve_openai_api_key() == "vault-key"


def test_resolve_openai_api_key_supports_op_reference_env(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("OPENAI_API_KEY", "op://Arnold/OPENAI_API_KEY/credential")
    monkeypatch.setenv("PPA_USE_ARNOLD_OPENAI_KEY", "1")
    monkeypatch.setattr(
        "archive_mcp.embedding_provider._read_1password_secret",
        lambda ref: "resolved-from-op" if ref.startswith("op://") else None,
    )
    assert _resolve_openai_api_key() == "resolved-from-op"


def test_resolve_openai_api_key_requires_toggle_for_op_reference(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("OPENAI_API_KEY", "op://Arnold/OPENAI_API_KEY/credential")
    monkeypatch.delenv("PPA_USE_ARNOLD_OPENAI_KEY", raising=False)
    with pytest.raises(RuntimeError, match="PPA_USE_ARNOLD_OPENAI_KEY=1"):
        _resolve_openai_api_key()


def test_resolve_service_account_token_prefers_passkey_gate_op_ref(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("OP_SERVICE_ACCOUNT_TOKEN", raising=False)
    monkeypatch.delenv("PPA_OP_SERVICE_ACCOUNT_TOKEN_FILE", raising=False)
    monkeypatch.setenv(
        "PPA_OP_SERVICE_ACCOUNT_TOKEN_OP_REF",
        "op://Arnold-Passkey-Gate/Service Account Auth Token: Arnold-Passkey-Gate/credential",
    )
    monkeypatch.setattr(
        "archive_mcp.embedding_provider._run_op_read",
        lambda reference, env: "gate-sa-token" if "Arnold-Passkey-Gate" in reference else None,
    )
    assert _resolve_service_account_token() == "gate-sa-token"


def test_resolve_service_account_token_falls_back_to_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    token_file = tmp_path / "op-tokens-service-account-token"
    token_file.write_text("file-token\n", encoding="utf-8")
    monkeypatch.delenv("OP_SERVICE_ACCOUNT_TOKEN", raising=False)
    monkeypatch.setenv("PPA_OP_SERVICE_ACCOUNT_TOKEN_FILE", str(token_file))
    monkeypatch.setattr("archive_mcp.embedding_provider._run_op_read", lambda reference, env: None)
    assert _resolve_service_account_token() == "file-token"
