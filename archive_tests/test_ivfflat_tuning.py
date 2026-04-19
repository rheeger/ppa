"""Tests for IVFFlat index auto-tuning."""


class TestIvfflatListsCalculation:
    def test_auto_lists_small_table(self):
        """Tables with <= 1M rows use rows / 1000, minimum 10."""
        from archive_cli.schema_ddl import _calculate_ivfflat_lists

        assert _calculate_ivfflat_lists(10_000) == 10
        assert _calculate_ivfflat_lists(100_000) == 100
        assert _calculate_ivfflat_lists(1_000_000) == 1000

    def test_auto_lists_large_table(self):
        """Tables with > 1M rows use sqrt(rows)."""
        from archive_cli.schema_ddl import _calculate_ivfflat_lists

        assert _calculate_ivfflat_lists(4_000_000) == 2000
        assert _calculate_ivfflat_lists(9_000_000) == 3000

    def test_auto_lists_minimum_is_10(self):
        """Even tiny tables get at least 10 lists."""
        from archive_cli.schema_ddl import _calculate_ivfflat_lists

        assert _calculate_ivfflat_lists(5) == 10
        assert _calculate_ivfflat_lists(50) == 10

    def test_auto_lists_zero_returns_none(self):
        """Empty table returns None (skip index creation)."""
        from archive_cli.schema_ddl import _calculate_ivfflat_lists

        assert _calculate_ivfflat_lists(0) is None


class TestIvfflatEnvOverride:
    def test_env_override_takes_priority(self, monkeypatch):
        """PPA_IVFFLAT_LISTS overrides auto-calculation."""
        monkeypatch.setenv("PPA_IVFFLAT_LISTS", "500")
        from archive_cli.index_config import get_ivfflat_lists

        assert get_ivfflat_lists() == 500

    def test_env_empty_returns_none(self, monkeypatch):
        """Empty/unset env var returns None (use auto-calculation)."""
        monkeypatch.delenv("PPA_IVFFLAT_LISTS", raising=False)
        from archive_cli.index_config import get_ivfflat_lists

        assert get_ivfflat_lists() is None

    def test_env_invalid_returns_none(self, monkeypatch):
        """Non-integer env var returns None."""
        monkeypatch.setenv("PPA_IVFFLAT_LISTS", "not-a-number")
        from archive_cli.index_config import get_ivfflat_lists

        assert get_ivfflat_lists() is None
