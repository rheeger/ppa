from archive_cli.corpus_hygiene.census import filter_threads_since
from archive_cli.corpus_hygiene.classification_reuse import EmailThreadRecord
from archive_sync.llm_enrichment.thread_date import any_activity_since, iso_day


def test_iso_day_reads_date_prefix() -> None:
    assert iso_day("2026-04-15T12:00:00Z") == "2026-04-15"
    assert iso_day("") == ""
    assert iso_day("not-a-date") == ""


def test_any_activity_since_empty_cutoff_keeps_all() -> None:
    assert any_activity_since(["2020-01-01"], "") is True


def test_any_activity_since_requires_one_day_on_or_after() -> None:
    assert any_activity_since(["2026-02-28", "2026-03-01"], "2026-03-01") is True
    assert any_activity_since(["2026-02-28T23:59:59Z"], "2026-03-01") is False


def test_filter_threads_since_uses_last_or_first_message() -> None:
    older = EmailThreadRecord(
        thread_uid="old",
        gmail_thread_id="g-old",
        first_message_at="2026-01-10",
        last_message_at="2026-02-20",
    )
    newer = EmailThreadRecord(
        thread_uid="new",
        gmail_thread_id="g-new",
        first_message_at="2025-12-01",
        last_message_at="2026-04-02T08:00:00Z",
    )
    kept = filter_threads_since([older, newer], "2026-03-01")
    assert [t.thread_uid for t in kept] == ["new"]
