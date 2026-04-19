"""Tests for the embed-estimate command."""

from unittest.mock import MagicMock


def test_embed_estimate_calculates_cost():
    """1M pending chunks at ~400 tokens/chunk = ~$8 at $0.02/1M tokens."""
    from archive_cli.commands.status import embedding_estimate

    mock_store = MagicMock()
    mock_store.embedding_status.return_value = {
        "chunk_count": 1_200_000,
        "embedded_chunk_count": 200_000,
        "pending_chunk_count": 1_000_000,
        "embedding_model": "text-embedding-3-small",
        "embedding_version": 1,
    }
    result = embedding_estimate(store=mock_store, logger=MagicMock())
    assert result["pending_chunks"] == 1_000_000
    assert result["estimated_cost_usd"] == 8.0


def test_embed_estimate_zero_pending():
    """No pending chunks = no cost, no time."""
    from archive_cli.commands.status import embedding_estimate

    mock_store = MagicMock()
    mock_store.embedding_status.return_value = {
        "chunk_count": 500_000,
        "embedded_chunk_count": 500_000,
        "pending_chunk_count": 0,
        "embedding_model": "text-embedding-3-small",
        "embedding_version": 1,
    }
    result = embedding_estimate(store=mock_store, logger=MagicMock())
    assert result["pending_chunks"] == 0
    assert result["estimated_cost_usd"] == 0.0
    assert result["estimated_minutes"] == 0.0


def test_embed_estimate_includes_model_info():
    """Result includes model name and concurrency settings."""
    from archive_cli.commands.status import embedding_estimate

    mock_store = MagicMock()
    mock_store.embedding_status.return_value = {
        "chunk_count": 100,
        "embedded_chunk_count": 0,
        "pending_chunk_count": 100,
        "embedding_model": "text-embedding-3-small",
        "embedding_version": 1,
    }
    result = embedding_estimate(store=mock_store, logger=MagicMock())
    assert "embedding_model" in result
    assert "batch_size" in result
    assert "concurrency" in result
