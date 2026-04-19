"""Tests for embedding progress calculation."""


def test_calculate_embed_progress_rate_and_eta():
    """Progress correctly calculates chunks/sec and ETA."""
    from archive_cli.embedder import _calculate_embed_progress

    result = _calculate_embed_progress(
        embedded_so_far=500,
        failed_so_far=10,
        total_pending=1000,
        elapsed_seconds=50.0,
    )
    assert result["rate_per_second"] == 10.0
    assert result["remaining"] == 490
    assert result["eta_seconds"] == 49.0


def test_calculate_embed_progress_zero_elapsed():
    """Zero elapsed time produces rate=0 and eta=0."""
    from archive_cli.embedder import _calculate_embed_progress

    result = _calculate_embed_progress(
        embedded_so_far=0,
        failed_so_far=0,
        total_pending=1000,
        elapsed_seconds=0.0,
    )
    assert result["rate_per_second"] == 0.0
    assert result["eta_seconds"] == 0.0


def test_calculate_embed_progress_all_done():
    """When all done, remaining=0."""
    from archive_cli.embedder import _calculate_embed_progress

    result = _calculate_embed_progress(
        embedded_so_far=1000,
        failed_so_far=0,
        total_pending=1000,
        elapsed_seconds=100.0,
    )
    assert result["remaining"] == 0
    assert result["eta_seconds"] == 0.0


def test_format_mss_formats_elapsed_and_eta():
    """Elapsed/ETA should render as M:SS per operational logging convention."""
    from archive_cli.embedder import _format_mss

    assert _format_mss(0) == "0:00"
    assert _format_mss(5) == "0:05"
    assert _format_mss(65) == "1:05"
    assert _format_mss(3600) == "60:00"
    assert _format_mss(3665) == "61:05"
