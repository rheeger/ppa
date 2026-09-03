"""Archive-sync gmail correspondent adapter tests."""

import io
import json
import urllib.error

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.adapters.gmail_correspondents import (
    GmailCorrespondentsAdapter,
    _extract_addresses_from_headers,
    _is_gmail_quota_error,
    _should_keep_correspondent,
    gmail_after_query,
    resolve_correspondents_list_query,
)
from archive_vault.schema import PersonCard
from archive_vault.uid import generate_uid
from archive_vault.vault import read_note, write_card


def test_extract_addresses_from_headers_parses_multiple_fields():
    headers = [
        {"name": "From", "value": "Alice Example <alice@example.com>"},
        {"name": "To", "value": "Bob <bob@example.com>, carol@example.org"},
        {"name": "Cc", "value": '"Dan D" <dan@example.net>'},
        {"name": "Subject", "value": "ignored"},
    ]
    pairs = _extract_addresses_from_headers(headers)
    assert ("Alice Example", "alice@example.com") in pairs
    assert ("Bob", "bob@example.com") in pairs
    assert ("", "carol@example.org") in pairs
    assert ("Dan D", "dan@example.net") in pairs


def test_to_card_returns_correspondent_person():
    card, provenance, _ = GmailCorrespondentsAdapter().to_card(
        {"name": "John Smith", "email": "john@example.com", "count": 8}
    )
    assert isinstance(card, PersonCard)
    assert card.first_name == "John"
    assert card.last_name == "Smith"
    assert card.tags == ["email-correspondent", "gmail-correspondent"]
    assert card.emails_seen_count == 8
    assert provenance["emails_seen_count"].method == "deterministic"


def test_should_keep_correspondent_filters_automated_senders():
    assert _should_keep_correspondent("Marty Messinger", "mmessinger@nb.com") is True
    assert _should_keep_correspondent("Taylor Kimmett", "notifications@github.com") is False
    assert _should_keep_correspondent("American Express", "americanexpress@welcome.americanexpress.com") is False
    assert _should_keep_correspondent("Baruch Piller", "reply-abc123@reply.linkedin.com") is False


def test_fetch_max_messages_is_per_run_not_cumulative(tmp_vault):
    adapter = GmailCorrespondentsAdapter()
    responses = iter(
        [
            {
                "messages": [{"id": "m1"}],
                "nextPageToken": "page-2",
            },
            {
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Marty Messinger <mmessinger@nb.com>"},
                    ]
                }
            },
            {
                "messages": [{"id": "m2"}],
                "nextPageToken": None,
            },
            {
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Navin Ram <navin_ram@yahoo.com>"},
                    ]
                }
            },
        ]
    )
    adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]
    first = adapter.fetch(
        str(tmp_vault), {"page_token": None, "scanned_messages": 0}, account_email="me@example.com", max_messages=1
    )
    second = adapter.fetch(
        str(tmp_vault),
        {"page_token": "page-2", "scanned_messages": 1},
        account_email="me@example.com",
        max_messages=1,
    )
    assert first[0]["email"] == "mmessinger@nb.com"
    assert first[0]["scanned_messages"] == 1
    assert second[0]["email"] == "navin_ram@yahoo.com"
    assert second[0]["scanned_messages"] == 2


def test_fetch_from_local_messages_filters_by_account_email(tmp_vault):
    email_root = tmp_vault / "Email" / "2026-03"
    email_root.mkdir(parents=True, exist_ok=True)
    (email_root / "one.md").write_text(
        """---
type: email_message
account_email: one@example.com
from_name: Alice Example
from_email: alice@example.com
to_emails:
  - one@example.com
reply_to_emails: []
---
""",
        encoding="utf-8",
    )
    (email_root / "two.md").write_text(
        """---
type: email_message
account_email: two@example.com
from_name: Bob Example
from_email: bob@example.com
to_emails:
  - two@example.com
reply_to_emails: []
---
""",
        encoding="utf-8",
    )

    adapter = GmailCorrespondentsAdapter()
    items = adapter._fetch_from_local_messages(str(tmp_vault), {"one@example.com"}, account_emails={"one@example.com"})

    assert [item["email"] for item in items] == ["alice@example.com"]
    assert items[0]["count"] == 1


def test_fetch_resumes_correspondent_counts_from_cursor(tmp_vault):
    adapter = GmailCorrespondentsAdapter()
    responses = iter(
        [
            {
                "messages": [{"id": "m2"}],
                "nextPageToken": None,
            },
            {
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Navin Ram <navin_ram@yahoo.com>"},
                    ]
                }
            },
        ]
    )
    adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]
    items = adapter.fetch(
        str(tmp_vault),
        {
            "page_token": "page-2",
            "scanned_messages": 1,
            "correspondent_counts": {
                "mmessinger@nb.com": {
                    "name": "Marty Messinger",
                    "email": "mmessinger@nb.com",
                    "count": 4,
                }
            },
        },
        account_email="me@example.com",
        dry_run=True,
    )
    by_email = {item["email"]: item["count"] for item in items}
    assert by_email == {"mmessinger@nb.com": 4, "navin_ram@yahoo.com": 1}
    assert items[0]["scanned_messages"] == 2


def test_fetch_skips_failed_precondition_messages(tmp_vault, monkeypatch):
    adapter = GmailCorrespondentsAdapter()
    calls: list[str] = []
    monkeypatch.setattr("archive_sync.adapters.gmail_correspondents.time.sleep", lambda _seconds: None)

    def fake_gws(args):
        calls.append(" ".join(args))
        if "messages" in args and "list" in args:
            return {"messages": [{"id": "bad"}, {"id": "good"}], "nextPageToken": None}
        if '"id": "bad"' in args[-1]:
            raise RuntimeError(
                '{"error":{"code":400,"reason":"failedPrecondition","message":"Precondition check failed."}}'
            )
        return {
            "payload": {
                "headers": [
                    {"name": "From", "value": "Marty Messinger <mmessinger@nb.com>"},
                ]
            }
        }

    adapter._gws = fake_gws  # type: ignore[method-assign]
    items = adapter.fetch(
        str(tmp_vault),
        {"page_token": None, "scanned_messages": 0},
        account_email="me@example.com",
        dry_run=True,
    )
    assert [item["email"] for item in items] == ["mmessinger@nb.com"]
    assert items[0]["scanned_messages"] == 2
    assert sum('"id": "bad"' in call for call in calls) == 3


def test_fetch_skips_backend_error_messages(tmp_vault, monkeypatch):
    adapter = GmailCorrespondentsAdapter()
    monkeypatch.setattr("archive_sync.adapters.gmail_correspondents.time.sleep", lambda _seconds: None)

    def fake_gws(args):
        if "messages" in args and "list" in args:
            return {"messages": [{"id": "flaky"}, {"id": "good"}], "nextPageToken": None}
        if '"id": "flaky"' in args[-1]:
            raise RuntimeError('{"error":{"code":500,"reason":"backendError","message":"Unknown Error."}}')
        return {
            "payload": {
                "headers": [
                    {"name": "From", "value": "Marty Messinger <mmessinger@nb.com>"},
                ]
            }
        }

    adapter._gws = fake_gws  # type: ignore[method-assign]
    items = adapter.fetch(
        str(tmp_vault),
        {"page_token": None, "scanned_messages": 0},
        account_email="me@example.com",
        dry_run=True,
    )
    assert [item["email"] for item in items] == ["mmessinger@nb.com"]
    assert items[0]["scanned_messages"] == 2


def test_fetch_with_account_email_merges_vault_and_api(tmp_vault):
    email_root = tmp_vault / "Email" / "2026-03"
    email_root.mkdir(parents=True, exist_ok=True)
    (email_root / "local.md").write_text(
        """---
type: email_message
account_email: me@example.com
gmail_message_id: m-local
from_name: Local Only
from_email: local@example.com
sent_at: 2026-03-09T12:00:00+00:00
to_emails:
  - me@example.com
---
""",
        encoding="utf-8",
    )
    adapter = GmailCorrespondentsAdapter()
    list_calls: list[dict] = []
    adapter._gws = _empty_list_gws(list_calls)  # type: ignore[method-assign]
    items = adapter.fetch(str(tmp_vault), {}, account_email="me@example.com", max_messages=1, dry_run=True)
    assert list_calls
    assert list_calls[0]["q"] == "after:2026/03/09"
    assert [item["email"] for item in items] == ["local@example.com"]
    assert items[0]["count"] == 1


def _list_params(args: list[str]) -> dict:
    assert args[:4] == ["gmail", "users", "messages", "list"]
    return json.loads(args[-1])


def _empty_list_gws(list_calls: list[dict]):
    def fake_gws(args):
        if "messages" in args and "list" in args:
            list_calls.append(_list_params(args))
            return {"messages": [], "nextPageToken": None}
        raise AssertionError(f"unexpected gws args: {args}")

    return fake_gws


def test_fetch_emits_only_dirty_correspondents(tmp_vault):
    adapter = GmailCorrespondentsAdapter()
    responses = iter(
        [
            {"messages": [{"id": "m1"}], "nextPageToken": None},
            {
                "id": "m1",
                "internalDate": "1700000000000",
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Alice Example <alice@example.com>"},
                    ]
                },
            },
        ]
    )
    adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]
    cursor = {
        "last_sync": "2026-03-09T00:55:05",
        "correspondent_state": {
            "alice@example.com": {
                "name": "Alice Example",
                "email": "alice@example.com",
                "count": 5,
                "last_seen": "2026-03-09T12:00:00+00:00",
            },
            "bob@example.com": {
                "name": "Bob Example",
                "email": "bob@example.com",
                "count": 2,
                "last_seen": "2026-03-08T10:00:00+00:00",
            },
        },
        "vault_scanned_messages": 100,
    }
    items = adapter.fetch(
        str(tmp_vault),
        cursor,
        account_email="me@example.com",
        dry_run=True,
    )
    emails = {item["email"] for item in items}
    assert emails == {"alice@example.com"}
    assert items[0]["count"] == 6
    patch = adapter.finalize_cursor(cursor)
    assert patch is not None
    assert patch["correspondent_state"]["alice@example.com"]["count"] == 6
    assert patch["correspondent_state"]["bob@example.com"]["count"] == 2


def test_fetch_dirty_correspondents_cursor_survives_checkpoint(tmp_vault):
    adapter = GmailCorrespondentsAdapter()
    cursor = {
        "last_sync": "2026-03-09T00:55:05",
        "correspondent_state": {
            "alice@example.com": {
                "name": "Alice Example",
                "email": "alice@example.com",
                "count": 2,
                "last_seen": "2026-03-10T12:00:00+00:00",
            },
        },
        "vault_scanned_messages": 10,
        "page_token": "page-2",
        "scanned_messages": 1,
        "correspondent_counts": {
            "alice@example.com": {
                "name": "Alice Example",
                "email": "alice@example.com",
                "count": 2,
                "last_seen": "2026-03-10T12:00:00+00:00",
            }
        },
    }
    adapter._gws = _empty_list_gws([])  # type: ignore[method-assign]
    items = adapter.fetch(str(tmp_vault), cursor, account_email="me@example.com", dry_run=True)
    assert items == []
    patch = adapter.finalize_cursor(cursor)
    assert patch is not None
    assert patch["correspondent_state"]["alice@example.com"]["count"] == 2
    assert patch["correspondent_state"]["alice@example.com"]["last_seen"] == "2026-03-10T12:00:00+00:00"


def test_gmail_after_query_uses_exclusive_utc_date():
    assert gmail_after_query("2026-03-09T00:55:05") == "after:2026/03/09"
    assert gmail_after_query("2026-03-09T00:55:05+00:00") == "after:2026/03/09"
    assert gmail_after_query("") is None


def test_fetch_with_last_sync_restricts_list_to_after_query(tmp_vault):
    adapter = GmailCorrespondentsAdapter()
    list_calls: list[dict] = []
    adapter._gws = _empty_list_gws(list_calls)  # type: ignore[method-assign]
    adapter.fetch(
        str(tmp_vault),
        {"page_token": None, "scanned_messages": 0, "last_sync": "2026-03-09T00:55:05"},
        account_email="me@example.com",
        dry_run=True,
    )
    assert list_calls
    assert list_calls[0]["q"] == "after:2026/03/09"
    assert "pageToken" not in list_calls[0]


def test_fetch_resume_does_not_add_after_query_from_last_sync(tmp_vault):
    adapter = GmailCorrespondentsAdapter()
    list_calls: list[dict] = []
    adapter._gws = _empty_list_gws(list_calls)  # type: ignore[method-assign]
    adapter.fetch(
        str(tmp_vault),
        {
            "page_token": "page-2",
            "scanned_messages": 500,
            "last_sync": "2026-03-09T00:55:05",
        },
        account_email="me@example.com",
        dry_run=True,
    )
    assert list_calls
    assert "q" not in list_calls[0]
    assert list_calls[0]["pageToken"] == "page-2"


def test_fetch_resume_keeps_in_progress_list_query(tmp_vault):
    adapter = GmailCorrespondentsAdapter()
    list_calls: list[dict] = []
    adapter._gws = _empty_list_gws(list_calls)  # type: ignore[method-assign]
    adapter.fetch(
        str(tmp_vault),
        {
            "page_token": "page-2",
            "scanned_messages": 500,
            "last_sync": "2026-03-09T00:55:05",
            "list_query": "after:2026/01/01",
        },
        account_email="me@example.com",
        dry_run=True,
    )
    assert list_calls[0]["q"] == "after:2026/01/01"
    assert list_calls[0]["pageToken"] == "page-2"


def test_fetch_without_last_sync_walks_full_mailbox(tmp_vault):
    adapter = GmailCorrespondentsAdapter()
    list_calls: list[dict] = []
    adapter._gws = _empty_list_gws(list_calls)  # type: ignore[method-assign]
    adapter.fetch(
        str(tmp_vault),
        {"page_token": None, "scanned_messages": 0},
        account_email="me@example.com",
        dry_run=True,
    )
    assert list_calls
    assert "q" not in list_calls[0]


def test_fetch_persists_list_query_while_in_progress(tmp_vault):
    adapter = GmailCorrespondentsAdapter()
    responses = iter(
        [
            {"messages": [{"id": "m1"}], "nextPageToken": "page-2"},
            {
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Marty Messinger <mmessinger@nb.com>"},
                    ]
                }
            },
        ]
    )
    adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]
    cursor = {"page_token": None, "scanned_messages": 0, "last_sync": "2026-03-09T00:55:05"}
    adapter.fetch(
        str(tmp_vault),
        cursor,
        account_email="me@example.com",
        max_messages=1,
        dry_run=True,
    )
    assert cursor["list_query"] == "after:2026/03/09"
    assert cursor["page_token"] == "page-2"


def test_incremental_merge_does_not_regress_emails_seen_count(tmp_vault):
    existing = PersonCard(
        uid=generate_uid("person", "gmail-correspondents", "john@example.com"),
        type="person",
        source=["gmail-correspondents"],
        source_id="john@example.com",
        created="2026-01-01",
        updated="2026-01-01",
        summary="John Smith",
        first_name="John",
        last_name="Smith",
        emails=["john@example.com"],
        emails_seen_count=5000,
        tags=["email-correspondent", "gmail-correspondent"],
    )
    write_card(
        tmp_vault,
        "People/john-smith.md",
        existing,
        provenance=deterministic_provenance(existing, "gmail-correspondents"),
    )
    adapter = GmailCorrespondentsAdapter()
    adapter.fetch = lambda vault_path, cursor, config=None, **kwargs: [  # type: ignore[method-assign]
        {"name": "John Smith", "email": "john@example.com", "count": 2}
    ]
    result = adapter.ingest(str(tmp_vault), account_email="me@example.com")
    assert result.merged == 1
    frontmatter, _, _ = read_note(tmp_vault, "People/john-smith.md")
    assert frontmatter["emails_seen_count"] == 5000


def test_resolve_correspondents_list_query_modes():
    assert resolve_correspondents_list_query({}, None) == (None, "full")
    assert resolve_correspondents_list_query({"last_sync": "2026-03-09T00:55:05"}, None) == (
        "after:2026/03/09",
        "incremental",
    )
    assert resolve_correspondents_list_query(
        {"page_token": "p2", "last_sync": "2026-03-09T00:55:05"},
        None,
    ) == (None, "resume")
    assert resolve_correspondents_list_query(
        {
            "page_token": "p2",
            "last_sync": "2026-03-09T00:55:05",
            "list_query": "after:2026/01/01",
        },
        None,
    ) == ("after:2026/01/01", "resume")


def test_api_workers_sequential_when_gws_stubbed():
    adapter = GmailCorrespondentsAdapter()
    adapter._gws = lambda args: {}  # type: ignore[method-assign]
    assert adapter._can_parallelize_gws() is False
    assert adapter._can_use_http() is False
    assert adapter._api_workers() == 1


def test_gmail_api_workers_getter_defaults_to_24(monkeypatch):
    monkeypatch.delenv("PPA_GMAIL_API_WORKERS", raising=False)
    from archive_cli.index_config import get_gmail_api_workers

    assert get_gmail_api_workers() >= 24
    adapter = GmailCorrespondentsAdapter()
    monkeypatch.setenv("PPA_GMAIL_API_WORKERS", "32")
    assert adapter._api_workers() == 32


def test_fetch_does_not_double_count_vault_message_returned_by_api(tmp_vault):
    email_root = tmp_vault / "Email" / "2026-03"
    email_root.mkdir(parents=True, exist_ok=True)
    (email_root / "local.md").write_text(
        """---
type: email_message
account_email: me@example.com
gmail_message_id: m1
from_name: Alice Example
from_email: alice@example.com
sent_at: 2026-03-09T12:00:00+00:00
to_emails:
  - me@example.com
---
""",
        encoding="utf-8",
    )
    adapter = GmailCorrespondentsAdapter()
    get_ids: list[str] = []

    def fake_gws(args):
        if "messages" in args and "list" in args:
            return {"messages": [{"id": "m1"}, {"id": "m2"}], "nextPageToken": None}
        get_ids.append(args[-1])
        if '"id": "m1"' in args[-1]:
            raise AssertionError("vault-covered message should not be fetched from Gmail")
        return {
            "id": "m2",
            "payload": {
                "headers": [
                    {"name": "From", "value": "Bob Example <bob@example.com>"},
                ]
            },
        }

    adapter._gws = fake_gws  # type: ignore[method-assign]
    items = adapter.fetch(
        str(tmp_vault),
        {"page_token": None, "scanned_messages": 0},
        account_email="me@example.com",
        dry_run=True,
    )
    by_email = {item["email"]: item["count"] for item in items}
    assert by_email["alice@example.com"] == 1
    assert by_email["bob@example.com"] == 1
    assert get_ids
    assert all('"id": "m1"' not in payload for payload in get_ids)


def test_fetch_http_uses_batch_get_not_gws(tmp_vault, monkeypatch):
    adapter = GmailCorrespondentsAdapter()

    class DummyManager:
        def get_access_token(self, *, force_refresh=False):
            return "token"

        def build_env(self, env=None, *, force_refresh=False):
            return {}

    adapter._token_manager = DummyManager()
    adapter._token_manager_key = ("gmail", "me@example.com")
    adapter._ensure_token_manager = lambda account_email: None  # type: ignore[method-assign]
    batch_ids: list[list[str]] = []

    def fake_http(args):
        if args[:4] == ["gmail", "users", "messages", "list"]:
            return {"messages": [{"id": "m1"}, {"id": "m2"}], "nextPageToken": None}
        raise AssertionError(f"individual get should be batched, got {args}")

    def fake_batch(message_ids):
        batch_ids.append(list(message_ids))
        return [
            {
                "id": message_id,
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Marty Messinger <mmessinger@nb.com>"},
                    ]
                },
            }
            for message_id in message_ids
        ]

    adapter._gmail_http_json = fake_http  # type: ignore[method-assign]
    adapter._gmail_http_batch_get_metadata = fake_batch  # type: ignore[method-assign]

    def fail_gws(*_args, **_kwargs):
        raise AssertionError("gws subprocess should not run on the HTTP hot path")

    monkeypatch.setattr("archive_sync.adapters.gmail_correspondents.subprocess.run", fail_gws)
    items = adapter.fetch(
        str(tmp_vault),
        {"page_token": None, "scanned_messages": 0},
        account_email="me@example.com",
        dry_run=True,
    )
    assert batch_ids == [["m1", "m2"]]
    assert items[0]["email"] == "mmessinger@nb.com"
    assert items[0]["count"] == 2


def test_is_gmail_quota_error_detects_rate_limit():
    assert _is_gmail_quota_error("Quota exceeded for quota metric", 403) is True
    assert _is_gmail_quota_error("rateLimitExceeded") is True
    assert _is_gmail_quota_error("not found", 404) is False
    assert _is_gmail_quota_error("boom", 429) is True


def test_fetch_messages_metadata_does_not_fanout_on_quota():
    adapter = GmailCorrespondentsAdapter()
    adapter._token_manager = object()
    adapter._can_parallelize_gws = lambda: True  # type: ignore[method-assign]
    adapter._gmail_http_batch_get_metadata = lambda _ids: (_ for _ in ()).throw(  # type: ignore[method-assign]
        RuntimeError('{"error":{"code":403,"message":"Quota exceeded","errors":[{"reason":"rateLimitExceeded"}]}}')
    )
    called: list[str] = []
    adapter._fetch_message_metadata = lambda mid: called.append(mid) or {}  # type: ignore[method-assign]
    try:
        adapter._fetch_messages_metadata(["m1", "m2"])
        raise AssertionError("expected quota error to propagate")
    except RuntimeError as exc:
        assert "Quota exceeded" in str(exc)
    assert called == []


def test_gmail_http_retries_quota_then_succeeds(monkeypatch):
    adapter = GmailCorrespondentsAdapter()

    class DummyManager:
        def get_access_token(self, *, force_refresh=False):
            return "token"

    sleeps: list[float] = []
    monkeypatch.setattr("archive_sync.adapters.gmail_correspondents.time.sleep", sleeps.append)
    monkeypatch.setattr("archive_sync.adapters.gmail_correspondents.random.uniform", lambda _a, _b: 0.0)
    attempts = {"n": 0}

    def fake_urlopen(req, timeout=60):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise urllib.error.HTTPError(
                getattr(req, "full_url", "https://example/messages"),
                403,
                "Forbidden",
                hdrs=None,
                fp=io.BytesIO(
                    b'{"error":{"code":403,"message":"Quota exceeded","errors":[{"reason":"rateLimitExceeded"}]}}'
                ),
            )

        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def read(self):
                return b'{"ok": true}'

        return Resp()

    monkeypatch.setattr("archive_sync.adapters.gmail_correspondents.urllib.request.urlopen", fake_urlopen)
    out = adapter._gmail_http_request_json("https://example/messages", token_manager=DummyManager())
    assert out == {"ok": True}
    assert attempts["n"] == 2
    assert sleeps == [5.0]
