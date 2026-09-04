import inspect

from archive_cli.embedder import EmbedderMixin


def test_materialize_embed_context_sql_filters_queue_after_joins() -> None:
    src = inspect.getsource(EmbedderMixin._materialize_embed_context)
    assert "FROM {self.schema}.embed_queue q" in src
    assert "WHERE card.uid IN" in src
    last_join = src.rfind("LEFT JOIN LATERAL")
    where_filter = src.find("WHERE card.uid IN")
    assert last_join != -1
    assert where_filter != -1
    assert last_join < where_filter
    # Filtering the left table before LEFT JOIN LATERAL is invalid SQL.
    prefix = src[:last_join]
    assert "WHERE card.uid IN" not in prefix
