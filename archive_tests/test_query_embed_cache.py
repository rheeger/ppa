from __future__ import annotations

from archive_cli.query_embed_cache import QueryEmbedCache, QueryEmbedSpec, query_embed_cache_key


def _spec(dimension: int = 4) -> QueryEmbedSpec:
    return QueryEmbedSpec(model="archive-hash-dev", version=1, provider="hash", dimension=dimension)


def test_cache_key_stable_after_case_and_whitespace(tmp_path) -> None:
    a = query_embed_cache_key("  Hello   World ", model="m", version=1, provider="p", dimension=8)
    b = query_embed_cache_key("hello world", model="m", version=1, provider="p", dimension=8)
    assert a == b
    c = query_embed_cache_key("hello world", model="M", version=1, provider="p", dimension=8)
    assert a != c


def test_get_put_lru_and_stats(tmp_path) -> None:
    cache = QueryEmbedCache(tmp_path / "q.sqlite", ram_entries=2)
    spec = _spec()
    vec = [0.1, 0.2, 0.3, 0.4]
    assert cache.get("alpha", spec) is None
    cache.put("alpha", spec, vec)
    hit = cache.get("alpha", spec)
    assert hit == vec
    stats = cache.stats()
    assert stats["rows"] == 1
    assert stats["hits"] >= 1
    assert stats["misses"] >= 1
    cache.close()


def test_evict_by_max_rows(tmp_path) -> None:
    cache = QueryEmbedCache(tmp_path / "q.sqlite", ram_entries=8)
    spec = _spec()
    cache.put("one", spec, [1.0, 0.0, 0.0, 0.0])
    cache.put("two", spec, [0.0, 1.0, 0.0, 0.0])
    cache.put("three", spec, [0.0, 0.0, 1.0, 0.0])
    result = cache.evict(max_rows=1, max_age_days=30)
    assert result["deleted_cap"] >= 2
    assert cache.stats()["rows"] == 1
    cache.close()
