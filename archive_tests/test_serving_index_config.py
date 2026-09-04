import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from archive_cli.index_config import (
    get_query_embed_cache_max_age_days,
    get_query_embed_cache_max_rows,
    get_query_embed_cache_path,
    get_query_embed_cache_ram_entries,
    get_serving_index_max_rss_mb,
    get_serving_index_path,
)
from archive_cli.query_explain import explain_sql
from archive_cli.serving_index import (
    merge_jsonl_by_key,
    publish_serving_index,
    read_dirty_uids,
)


def test_serving_index_defaults(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("PPA_SERVING_INDEX_PATH", raising=False)
    monkeypatch.delenv("PPA_QUERY_EMBED_CACHE_PATH", raising=False)
    assert get_serving_index_path(tmp_path) == tmp_path / "_meta" / "rust-search-index"
    assert get_query_embed_cache_path(tmp_path) == tmp_path / "_meta" / "query-embed-cache.sqlite"
    assert get_serving_index_max_rss_mb() >= 256
    assert get_query_embed_cache_ram_entries() >= 0
    assert get_query_embed_cache_max_rows() >= 1
    assert get_query_embed_cache_max_age_days() >= 1


def test_explain_sql_captures_failure() -> None:
    class _Boom:
        def execute(self, *_a, **_k):
            raise RuntimeError("canceling statement due to statement timeout")

    out = explain_sql(_Boom(), "SELECT 1")
    assert out["ok"] is False
    assert out["canceled"] is True


def test_merge_jsonl_by_key_replaces_and_appends(tmp_path: Path) -> None:
    src = tmp_path / "src.jsonl"
    dest = tmp_path / "dest.jsonl"
    src.write_text(
        json.dumps({"card_uid": "a", "summary": "old-a"})
        + "\n"
        + json.dumps({"card_uid": "b", "summary": "keep-b"})
        + "\n",
        encoding="utf-8",
    )
    n = merge_jsonl_by_key(
        src,
        dest,
        key="card_uid",
        replacements=[
            {"card_uid": "a", "summary": "new-a"},
            {"card_uid": "c", "summary": "new-c"},
        ],
    )
    assert n == 2
    rows = [json.loads(line) for line in dest.read_text(encoding="utf-8").splitlines() if line.strip()]
    by_uid = {row["card_uid"]: row["summary"] for row in rows}
    assert by_uid == {"a": "new-a", "b": "keep-b", "c": "new-c"}


def test_read_dirty_uids_ignores_empty_vault_written(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "rust-search-index"
    root.mkdir()
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    (root / "DIRTY").write_text(
        json.dumps({"ts": "1", "reason": "vault_written", "uids": []})
        + "\n"
        + json.dumps({"ts": "2", "reason": "vault_written", "uids": ["", "  "]})
        + "\n"
        + json.dumps({"ts": "3", "reason": "processor", "uids": ["uid-real"]})
        + "\n",
        encoding="utf-8",
    )
    assert read_dirty_uids(tmp_path) == ["uid-real"]


def test_publish_serving_index_none_dirty_uids_does_not_skip(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "archive_cli.serving_index.serving_index_status",
        lambda _vault: {
            "serving_index_ready": True,
            "serving_index_generation": "gen-keep",
            "serving_index_dirty_records": 2,
            "serving_index_format": 1,
        },
    )
    store = MagicMock()
    store.vault = tmp_path
    store.index.schema = "ppa"
    store.index._connect.side_effect = RuntimeError("full-export-started")
    with pytest.raises(RuntimeError, match="full-export-started"):
        publish_serving_index(store)


def test_publish_serving_index_skips_when_dirty_without_uids(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "archive_cli.serving_index.serving_index_status",
        lambda _vault: {
            "serving_index_ready": True,
            "serving_index_generation": "gen-keep",
            "serving_index_dirty_records": 2,
            "serving_index_format": 1,
        },
    )
    crate_calls: list[str] = []

    class _Crate:
        @staticmethod
        def serving_index_build(*_a, **_k):
            crate_calls.append("build")
            return {"ok": True}

        @staticmethod
        def serving_index_publish(*_a, **_k):
            crate_calls.append("publish")

        @staticmethod
        def serving_index_truncate_dirty(*_a, **_k):
            crate_calls.append("truncate")

    monkeypatch.setattr("archive_cli.serving_index._crate", lambda: _Crate())
    store = MagicMock()
    store.vault = tmp_path
    store.index.schema = "ppa"
    result = publish_serving_index(store, dirty_uids=[])
    assert result["ok"] is True
    assert result["skipped"] == "dirty_without_uids"
    assert result["generation"] == "gen-keep"
    assert crate_calls == []
    store.index._connect.assert_not_called()


def test_publish_serving_index_incremental_skips_full_export(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "rust-search-index"
    prev = root / "generations" / "gen-prev"
    prev.mkdir(parents=True)
    (prev / "cards.jsonl").write_text(
        json.dumps({"card_uid": "old", "summary": "keep"}) + "\n",
        encoding="utf-8",
    )
    (prev / "chunks.jsonl").write_text(
        json.dumps({"chunk_key": "ck-old", "card_uid": "old", "chunk_type": "body", "chunk_index": 0}) + "\n",
        encoding="utf-8",
    )
    (prev / "edges.jsonl").write_text(
        json.dumps({"source_uid": "old", "target_uid": "nbr", "edge_type": "mentions"}) + "\n",
        encoding="utf-8",
    )
    (prev / "embedding_keys.txt").write_text("ck-old\n", encoding="utf-8")
    (prev / "embeddings.bin").write_bytes(b"abcd")
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    monkeypatch.setattr(
        "archive_cli.serving_index.serving_index_status",
        lambda _vault: {
            "serving_index_ready": True,
            "serving_index_generation": "gen-prev",
            "serving_index_dirty_records": 1,
            "serving_index_format": 1,
        },
    )
    sqls: list[str] = []

    class _Conn:
        def execute(self, sql, _params=None):
            sqls.append(str(sql))
            return []

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    store = MagicMock()
    store.vault = tmp_path
    store.index.schema = "ppa"
    store.index._connect.return_value = _Conn()

    class _Crate:
        @staticmethod
        def serving_index_build(*_a, **_k):
            return {"ok": True, "cards": 1}

        @staticmethod
        def serving_index_publish(*_a, **_k):
            return None

        @staticmethod
        def serving_index_truncate_dirty(*_a, **_k):
            return None

    monkeypatch.setattr("archive_cli.serving_index._crate", lambda: _Crate())
    result = publish_serving_index(store, dirty_uids=["uid-new"], dest_generation="gen-new")
    assert result["ok"] is True
    assert result["generation"] == "gen-new"
    joined = "\n".join(sqls)
    assert "FROM ppa.cards c" in joined
    assert "c.uid = ANY(%s)" in joined
    assert "SELECT COUNT(*) AS c FROM ppa.cards" not in joined
    assert "FROM ppa.embeddings" not in joined
    dest = root / "generations" / "gen-new"
    cards = [json.loads(line) for line in (dest / "cards.jsonl").read_text(encoding="utf-8").splitlines()]
    assert {row["card_uid"] for row in cards} == {"old"}
    assert (dest / "embeddings.bin").stat().st_ino == (prev / "embeddings.bin").stat().st_ino
