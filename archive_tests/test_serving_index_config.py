from archive_cli.index_config import (
    get_query_embed_cache_max_age_days,
    get_query_embed_cache_max_rows,
    get_query_embed_cache_path,
    get_query_embed_cache_ram_entries,
    get_serving_index_max_rss_mb,
    get_serving_index_path,
)
from archive_cli.query_explain import explain_sql


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
