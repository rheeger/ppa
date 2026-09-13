import inspect

from archive_cli.embedder import EmbedderMixin, embedding_spec_matches_index, normalize_chunk_schema_id
from archive_cli.index_config import CHUNK_SCHEMA_VERSION
from archive_engine.contracts import EmbeddingSpec


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


def test_serving_chunk_schema_digit_matches_embedder_id() -> None:
    assert normalize_chunk_schema_id("7") == "chunk_schema_v7"
    spec = EmbeddingSpec(
        provider_namespace="openai",
        model="text-embedding-3-small",
        model_revision="1",
        dimension=1536,
        metric="cosine",
        normalization="l2",
        chunk_schema=str(CHUNK_SCHEMA_VERSION),
    )
    assert embedding_spec_matches_index(
        spec,
        model="text-embedding-3-small",
        version=1,
        dimension=1536,
    ) is True
