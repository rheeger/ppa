"""Unit tests for the OpenAI Batch API embedding path."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock


class TestRenderBatchJsonl:
    def test_render_includes_context_prefix(self, tmp_path: Path):
        from archive_cli.batch_embedder import PendingChunk, _render_batch_jsonl

        chunks = [
            PendingChunk(chunk_key="ck-1", content="hello world", prefix="CTX: person\n---\n"),
            PendingChunk(chunk_key="ck-2", content="second chunk", prefix=""),
        ]
        dest = tmp_path / "in.jsonl"
        _render_batch_jsonl(chunks, model="text-embedding-3-small", dimension=1536, dest_path=dest)
        lines = dest.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 2

        rec0 = json.loads(lines[0])
        assert rec0["custom_id"] == "r-0"
        assert rec0["method"] == "POST"
        assert rec0["url"] == "/v1/embeddings"
        assert rec0["body"]["model"] == "text-embedding-3-small"
        assert rec0["body"]["dimensions"] == 1536
        assert rec0["body"]["input"].startswith("CTX: person")
        assert "hello world" in rec0["body"]["input"]

        rec1 = json.loads(lines[1])
        assert rec1["custom_id"] == "r-1"
        assert rec1["body"]["input"] == "second chunk"


class TestHttpHelpers:
    def test_resolve_api_key_uses_env(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test-123")
        from archive_cli.batch_embedder import _resolve_api_key

        assert _resolve_api_key() == "sk-test-123"

    def test_base_url_default(self, monkeypatch):
        monkeypatch.delenv("PPA_OPENAI_BASE_URL", raising=False)
        monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
        from archive_cli.batch_embedder import _base_url

        assert _base_url() == "https://api.openai.com/v1"

    def test_base_url_respects_env_override(self, monkeypatch):
        monkeypatch.setenv("PPA_OPENAI_BASE_URL", "https://example.test/v1/")
        from archive_cli.batch_embedder import _base_url

        assert _base_url() == "https://example.test/v1"


class TestSubmitBatchesNoWork:
    def test_no_pending_submits_nothing(self, tmp_path, monkeypatch):
        """With zero pending chunks, submit_batches makes no HTTP calls and returns empty."""
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        from archive_cli import batch_embedder as be

        class FakeCursor:
            def __init__(self):
                self._rows: list[tuple] = []

            def fetchone(self):
                return None

            def fetchall(self):
                return self._rows

        class FakeConn:
            def __init__(self):
                self.executed = []

            def execute(self, sql, params=None):
                self.executed.append((sql, params))
                return FakeCursor()

            def commit(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

        index = MagicMock()
        index.schema = "ppa"
        index._connect = MagicMock(side_effect=lambda: FakeConn())
        index._ensure_batch_embed_tables = MagicMock()

        import logging

        result = be.submit_batches(
            index=index,
            logger_=logging.getLogger("ppa.test"),
            embedding_model="text-embedding-3-small",
            embedding_version=1,
            max_batches=3,
            requests_per_batch=10,
            include_context_prefix=True,
            artifact_dir=str(tmp_path),
        )
        assert result["submitted_batches"] == 0
        assert result["total_requests"] == 0
