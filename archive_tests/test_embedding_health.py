"""Tests for embedding coverage health checks."""

from unittest.mock import MagicMock


def test_embedding_coverage_reports_complete():
    """100% coverage reports ok=True with 0 pending."""
    from archive_cli.commands.health_check import _check_embedding_coverage

    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.side_effect = [
        {"count": 1000},
        {"count": 1000},
    ]
    result = _check_embedding_coverage(mock_conn, "test_schema")
    assert result["ok"] is True
    assert result["pending_count"] == 0
    assert result["coverage_percent"] == 100.0


def test_embedding_coverage_reports_pending():
    """Partial coverage reports ok=False with correct pending count."""
    from archive_cli.commands.health_check import _check_embedding_coverage

    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.side_effect = [
        {"count": 1000},
        {"count": 800},
    ]
    result = _check_embedding_coverage(mock_conn, "test_schema")
    assert result["ok"] is False
    assert result["pending_count"] == 200
    assert result["coverage_percent"] == 80.0


def test_embedding_coverage_handles_empty():
    """Empty table (0 chunks) reports ok=True."""
    from archive_cli.commands.health_check import _check_embedding_coverage

    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.side_effect = [
        {"count": 0},
        {"count": 0},
    ]
    result = _check_embedding_coverage(mock_conn, "test_schema")
    assert result["ok"] is True
    assert result["pending_count"] == 0


def test_embedding_coverage_handles_db_error():
    """Database error returns ok=False with error message."""
    from archive_cli.commands.health_check import _check_embedding_coverage

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("connection refused")
    result = _check_embedding_coverage(mock_conn, "test_schema")
    assert result["ok"] is False
    assert "error" in result
