from __future__ import annotations

import json
from pathlib import Path

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.adapters.github_history import (GitHubHistoryAdapter, _message_uid,
                                     _repo_uid, _thread_uid)
from hfa.schema import PersonCard
from hfa.vault import read_note, write_card


def _seed_person(tmp_vault: Path) -> None:
    person = PersonCard(
        uid="hfa-person-robbie123",
        type="person",
        source=["contacts.apple"],
        source_id="robbie@endaoment.org",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Robbie Heeger",
        first_name="Robbie",
        last_name="Heeger",
        emails=["robbie@endaoment.org"],
        github="rheeger",
    )
    write_card(
        tmp_vault,
        "People/robbie-heeger.md",
        person,
        provenance=deterministic_provenance(person, "contacts.apple"),
    )
    (tmp_vault / "_meta" / "identity-map.json").write_text(
        json.dumps(
            {
                "_comment": "Alias -> canonical person wikilink",
                "email:robbie@endaoment.org": "[[robbie-heeger]]",
                "github:rheeger": "[[robbie-heeger]]",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _seed_email_only_person(tmp_vault: Path) -> None:
    person = PersonCard(
        uid="hfa-person-robbieemail",
        type="person",
        source=["contacts.apple"],
        source_id="robbie@endaoment.org",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Robbie Heeger",
        first_name="Robbie",
        last_name="Heeger",
        emails=["robbie@endaoment.org"],
    )
    write_card(
        tmp_vault,
        "People/robbie-heeger.md",
        person,
        provenance=deterministic_provenance(person, "contacts.apple"),
    )
    (tmp_vault / "_meta" / "identity-map.json").write_text(
        json.dumps(
            {
                "_comment": "Alias -> canonical person wikilink",
                "email:robbie@endaoment.org": "[[robbie-heeger]]",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _sample_bundle() -> dict[str, object]:
    repo_uid = _repo_uid("rheeger/hey-arnold-hfa")
    thread_uid = _thread_uid("rheeger/hey-arnold-hfa", "pull_request", "12")
    message_uid = _message_uid("rheeger/hey-arnold-hfa", "review_comment", "789")
    return {
        "repo": {
            "kind": "repo",
            "source": ["github.repo"],
            "source_id": "rheeger/hey-arnold-hfa",
            "summary": "rheeger/hey-arnold-hfa",
            "people": ["[[robbie-heeger]]"],
            "orgs": [],
            "github_repo_id": "123",
            "github_node_id": "R_kgDOExample",
            "name_with_owner": "rheeger/hey-arnold-hfa",
            "owner_login": "rheeger",
            "owner_type": "User",
            "html_url": "https://github.com/rheeger/hey-arnold-hfa",
            "api_url": "https://api.github.com/repos/rheeger/hey-arnold-hfa",
            "ssh_url": "git@github.com:rheeger/hey-arnold-hfa.git",
            "default_branch": "main",
            "homepage_url": "",
            "description": "Archive tooling",
            "visibility": "private",
            "is_private": True,
            "is_fork": False,
            "is_archived": False,
            "parent_name_with_owner": "",
            "primary_language": "Python",
            "languages": ["Python", "TypeScript"],
            "topics": ["archive", "mcp"],
            "license_name": "MIT",
            "created_at": "2026-03-01T00:00:00Z",
            "pushed_at": "2026-03-10T12:00:00Z",
            "created": "2026-03-01",
            "updated": "2026-03-10",
            "body": "",
        },
        "commits": [
            {
                "kind": "commit",
                "source": ["github.commit"],
                "source_id": "rheeger/hey-arnold-hfa:deadbeef",
                "summary": "Add GitHub archive ingest",
                "people": ["[[robbie-heeger]]"],
                "orgs": [],
                "github_node_id": "C_kwDOExample",
                "commit_sha": "deadbeef",
                "repository_name_with_owner": "rheeger/hey-arnold-hfa",
                "repository": f"[[{repo_uid}]]",
                "parent_shas": ["abc123"],
                "html_url": "https://github.com/rheeger/hey-arnold-hfa/commit/deadbeef",
                "api_url": "",
                "authored_at": "2026-03-10T11:00:00Z",
                "committed_at": "2026-03-10T11:05:00Z",
                "message_headline": "Add GitHub archive ingest",
                "additions": 10,
                "deletions": 2,
                "changed_files": 3,
                "author_login": "rheeger",
                "author_name": "Robbie Heeger",
                "author_email": "robbie@endaoment.org",
                "committer_login": "rheeger",
                "committer_name": "Robbie Heeger",
                "committer_email": "robbie@endaoment.org",
                "associated_pr_numbers": ["12"],
                "associated_pr_urls": ["https://github.com/rheeger/hey-arnold-hfa/pull/12"],
                "created": "2026-03-10",
                "updated": "2026-03-10",
                "body": "Build the first GitHub archive ingest path.",
            }
        ],
        "threads": [
            {
                "kind": "thread",
                "source": ["github.thread", "github.thread.pull_request"],
                "source_id": "rheeger/hey-arnold-hfa:pull_request:12",
                "summary": "Add GitHub archive ingest",
                "people": ["[[robbie-heeger]]"],
                "orgs": [],
                "github_thread_id": "456",
                "github_node_id": "PR_kwDOExample",
                "repository_name_with_owner": "rheeger/hey-arnold-hfa",
                "repository": f"[[{repo_uid}]]",
                "thread_type": "pull_request",
                "number": "12",
                "html_url": "https://github.com/rheeger/hey-arnold-hfa/pull/12",
                "api_url": "https://api.github.com/repos/rheeger/hey-arnold-hfa/pulls/12",
                "state": "open",
                "is_draft": False,
                "merged_at": "",
                "closed_at": "",
                "title": "Add GitHub archive ingest",
                "labels": ["archive"],
                "assignees": ["rheeger"],
                "milestone": "",
                "base_ref": "main",
                "head_ref": "feature/github-ingest",
                "participant_logins": ["rheeger"],
                "messages": [f"[[{message_uid}]]"],
                "first_message_at": "2026-03-10T12:00:00Z",
                "last_message_at": "2026-03-10T12:00:00Z",
                "message_count": 1,
                "created": "2026-03-10",
                "updated": "2026-03-10",
                "body": "",
            }
        ],
        "messages": [
            {
                "kind": "message",
                "source": ["github.message", "github.message.review_comment"],
                "source_id": "rheeger/hey-arnold-hfa:review_comment:789",
                "summary": "Can we preserve the GitHub ids here?",
                "people": ["[[robbie-heeger]]"],
                "orgs": [],
                "github_message_id": "789",
                "github_node_id": "PRRC_kwDOExample",
                "repository_name_with_owner": "rheeger/hey-arnold-hfa",
                "repository": f"[[{repo_uid}]]",
                "thread": f"[[{thread_uid}]]",
                "message_type": "review_comment",
                "html_url": "https://github.com/rheeger/hey-arnold-hfa/pull/12#discussion_r789",
                "api_url": "https://api.github.com/repos/rheeger/hey-arnold-hfa/pulls/comments/789",
                "actor_login": "rheeger",
                "actor_name": "Robbie Heeger",
                "actor_email": "robbie@endaoment.org",
                "sent_at": "2026-03-10T12:00:00Z",
                "updated_at": "2026-03-10T12:01:00Z",
                "review_state": "COMMENTED",
                "review_commit_sha": "deadbeef",
                "in_reply_to_message_id": "",
                "path": "archive_mcp/index_store.py",
                "position": "12",
                "original_position": "12",
                "original_commit_sha": "deadbeef",
                "diff_hunk": "@@ -1,2 +1,2 @@",
                "created": "2026-03-10",
                "updated": "2026-03-10",
                "body": "Can we preserve the GitHub ids here?",
            }
        ],
    }


def test_stage_history_writes_manifest_and_stage_files(tmp_vault: Path, tmp_path: Path, monkeypatch):
    adapter = GitHubHistoryAdapter()
    stage_dir = tmp_path / "stage"
    bundle = _sample_bundle()

    monkeypatch.setattr(
        adapter,
        "_list_visible_repositories",
        lambda max_repos=None: [{"full_name": "rheeger/hey-arnold-hfa"}],
    )
    monkeypatch.setattr(
        adapter,
        "_fetch_repo_bundle",
        lambda repo_row, *, vault_path, max_commits_per_repo, max_threads_per_repo, max_messages_per_thread: bundle,
    )

    manifest = adapter.stage_history(str(tmp_vault), stage_dir, verbose=False)

    assert manifest["counts"]["repos"] == 1
    assert manifest["counts"]["commits"] == 1
    assert manifest["counts"]["threads"] == 1
    assert manifest["counts"]["messages"] == 1
    assert (stage_dir / "manifest.json").exists()
    assert (stage_dir / "repos.jsonl").exists()
    assert (stage_dir / "_meta" / "extract-state.json").exists()


def test_import_stage_creates_git_cards(tmp_vault: Path, tmp_path: Path):
    _seed_person(tmp_vault)
    stage_dir = tmp_path / "stage"
    stage_dir.mkdir()
    bundle = _sample_bundle()
    for name, key in (("repos.jsonl", "repo"), ("commits.jsonl", "commits"), ("threads.jsonl", "threads"), ("messages.jsonl", "messages")):
        records = bundle[key]
        rows = [records] if isinstance(records, dict) else list(records)
        (stage_dir / name).write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    adapter = GitHubHistoryAdapter()
    result = adapter.ingest(str(tmp_vault), stage_dir=str(stage_dir))

    assert result.created == 4

    repo_rel_path = next((tmp_vault / "GitRepos").rglob("*.md")).relative_to(tmp_vault)
    commit_rel_path = next((tmp_vault / "GitCommits").rglob("*.md")).relative_to(tmp_vault)
    thread_rel_path = next((tmp_vault / "GitThreads").rglob("*.md")).relative_to(tmp_vault)
    message_rel_path = next((tmp_vault / "GitMessages").rglob("*.md")).relative_to(tmp_vault)

    repo_frontmatter, _, _ = read_note(tmp_vault, str(repo_rel_path))
    commit_frontmatter, commit_body, _ = read_note(tmp_vault, str(commit_rel_path))
    thread_frontmatter, _, _ = read_note(tmp_vault, str(thread_rel_path))
    message_frontmatter, message_body, _ = read_note(tmp_vault, str(message_rel_path))

    assert repo_frontmatter["type"] == "git_repository"
    assert repo_frontmatter["name_with_owner"] == "rheeger/hey-arnold-hfa"
    assert repo_frontmatter["people"] == ["[[robbie-heeger]]"]
    assert commit_frontmatter["type"] == "git_commit"
    assert commit_frontmatter["commit_sha"] == "deadbeef"
    assert "Build the first GitHub archive ingest path." in commit_body
    assert thread_frontmatter["type"] == "git_thread"
    assert thread_frontmatter["messages"] == [f"[[{_message_uid('rheeger/hey-arnold-hfa', 'review_comment', '789')}]]"]
    assert message_frontmatter["type"] == "git_message"
    assert message_frontmatter["path"] == "archive_mcp/index_store.py"
    assert "Can we preserve the GitHub ids here?" in message_body


def test_import_stage_enriches_existing_person_with_github_handle(tmp_vault: Path, tmp_path: Path):
    _seed_email_only_person(tmp_vault)
    stage_dir = tmp_path / "stage"
    stage_dir.mkdir()
    bundle = _sample_bundle()
    for name, key in (("repos.jsonl", "repo"), ("commits.jsonl", "commits"), ("threads.jsonl", "threads"), ("messages.jsonl", "messages")):
        records = bundle[key]
        rows = [records] if isinstance(records, dict) else list(records)
        (stage_dir / name).write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    adapter = GitHubHistoryAdapter()
    result = adapter.ingest(str(tmp_vault), stage_dir=str(stage_dir))

    assert result.created == 4
    person_frontmatter, _, _ = read_note(tmp_vault, "People/robbie-heeger.md")
    assert person_frontmatter["github"] == "rheeger"
    assert any(source.startswith("github.") for source in person_frontmatter["source"])


def test_fetch_commits_walks_multiple_refs_and_dedupes(monkeypatch):
    adapter = GitHubHistoryAdapter()
    monkeypatch.setattr(
        adapter,
        "_fetch_ref_names",
        lambda *, owner, repo, ref_prefix: ["main", "feature"] if ref_prefix == "refs/heads/" else ["v1.0.0"],
    )

    calls: list[tuple[str, bool]] = []

    def fake_history(*, owner, repo, qualified_name, max_commits=None, seen_shas=None, stop_when_page_seen=False):
        calls.append((qualified_name, stop_when_page_seen))
        if qualified_name == "refs/heads/main":
            return [{"oid": "aaa"}, {"oid": "bbb"}]
        if qualified_name == "refs/heads/feature":
            return [{"oid": "bbb"}, {"oid": "ccc"}]
        if qualified_name == "refs/tags/v1.0.0":
            return [{"oid": "ddd"}]
        return []

    monkeypatch.setattr(adapter, "_fetch_commit_history_for_ref", fake_history)
    repo_meta, commits = adapter._fetch_commits(owner="rheeger", repo="hey-arnold-hfa", default_branch="main")

    assert repo_meta["nameWithOwner"] == "rheeger/hey-arnold-hfa"
    assert [row["oid"] for row in commits] == ["aaa", "bbb", "ccc", "ddd"]
    assert calls[0] == ("refs/heads/main", False)
    assert calls[1] == ("refs/heads/feature", True)


def test_fetch_commit_history_for_ref_stops_when_page_is_fully_seen(monkeypatch):
    adapter = GitHubHistoryAdapter()
    payloads = iter(
        [
            {
                "data": {
                    "rateLimit": {"remaining": 4000, "cost": 1, "resetAt": "2026-03-11T05:00:00Z"},
                    "repository": {
                        "ref": {
                            "target": {
                                "history": {
                                    "nodes": [{"oid": "aaa"}, {"oid": "bbb"}],
                                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                                }
                            }
                        }
                    },
                }
            },
            {
                "data": {
                    "rateLimit": {"remaining": 4000, "cost": 1, "resetAt": "2026-03-11T05:00:00Z"},
                    "repository": {
                        "ref": {
                            "target": {
                                "history": {
                                    "nodes": [{"oid": "aaa"}, {"oid": "bbb"}],
                                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor-2"},
                                }
                            }
                        }
                    },
                }
            },
        ]
    )
    monkeypatch.setattr(adapter, "_gh_graphql_page", lambda query, variables, max_retries=None: next(payloads))
    commits = adapter._fetch_commit_history_for_ref(
        owner="rheeger",
        repo="hey-arnold-hfa",
        qualified_name="refs/heads/feature",
        seen_shas={"aaa", "bbb"},
        stop_when_page_seen=True,
    )
    assert [row["oid"] for row in commits] == ["aaa", "bbb"]


def test_fetch_commit_history_for_ref_reduces_graphql_page_size_on_retryable_error(monkeypatch):
    adapter = GitHubHistoryAdapter()
    calls: list[int] = []

    def fake_graphql(query, variables, *, max_retries=None):
        page_size = variables["pageSize"]
        calls.append(page_size)
        if page_size == 25:
            raise RuntimeError("gh: HTTP 502")
        return {
            "data": {
                "rateLimit": {"remaining": 4000, "cost": 1, "resetAt": "2026-03-11T05:00:00Z"},
                "repository": {
                    "ref": {
                        "target": {
                            "history": {
                                "nodes": [{"oid": "aaa"}],
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                            }
                        }
                    }
                },
            }
        }

    monkeypatch.setattr(adapter, "_gh_graphql_page", fake_graphql)
    commits = adapter._fetch_commit_history_for_ref(
        owner="rheeger",
        repo="hey-arnold-hfa",
        qualified_name="refs/heads/main",
    )

    assert calls[:2] == [25, 10]
    assert [row["oid"] for row in commits] == ["aaa"]


def test_paged_rest_rows_reduces_page_size_on_retryable_error(monkeypatch):
    adapter = GitHubHistoryAdapter()
    calls: list[int] = []

    def fake_gh_api(endpoint, *, params=None, paginate=False, slurp=False, method="GET", max_retries=None):
        per_page = params["per_page"]
        calls.append(per_page)
        if per_page == 25:
            raise RuntimeError("Get https://api.github.com/example: net/http: TLS handshake timeout")
        return [[{"id": 1}]]

    monkeypatch.setattr(adapter, "_gh_api", fake_gh_api)
    rows = adapter._paged_rest_rows("repos/foo/bar/pulls/1/reviews", base_params={})

    assert calls[:2] == [25, 10]
    assert rows == [{"id": 1}]


def test_retry_delay_seconds_handles_secondary_limit_and_timeouts():
    adapter = GitHubHistoryAdapter()
    assert adapter._retry_delay_seconds("secondary rate limit", 1) == 60
    assert adapter._retry_delay_seconds("secondary rate limit", 10) == 300
    assert adapter._retry_delay_seconds("TLS handshake timeout", 2) == 30
    assert adapter._retry_delay_seconds("gh: HTTP 502", 3) == 30


def test_fetch_repo_bundle_limits_discussion_fetches_when_thread_cap_set(tmp_vault: Path, monkeypatch):
    adapter = GitHubHistoryAdapter()
    monkeypatch.setattr(adapter, "_repo_detail", lambda owner, repo: {"full_name": "rheeger/test"})
    monkeypatch.setattr(
        adapter,
        "_fetch_commits",
        lambda owner, repo, max_commits=None, default_branch="": ({"nameWithOwner": "rheeger/test"}, []),
    )
    monkeypatch.setattr(
        adapter,
        "_repo_issues",
        lambda owner, repo: [
            {"number": 1, "title": "Issue 1", "state": "open", "updated_at": "2026-03-10T00:00:00Z"},
            {"number": 2, "title": "Issue 2", "state": "open", "updated_at": "2026-03-09T00:00:00Z"},
        ],
    )
    monkeypatch.setattr(
        adapter,
        "_repo_pulls",
        lambda owner, repo: [
            {
                "number": 11,
                "id": 11,
                "node_id": "PR11",
                "title": "PR 11",
                "state": "open",
                "updated_at": "2026-03-11T00:00:00Z",
                "html_url": "https://github.com/rheeger/test/pull/11",
                "url": "https://api.github.com/repos/rheeger/test/pulls/11",
                "labels": [],
                "assignees": [],
                "base": {"ref": "main"},
                "head": {"ref": "feature"},
            },
        ],
    )
    captured: dict[str, object] = {}

    def fake_issue_comments(owner, repo, *, issue_numbers=None):
        captured["issue_numbers"] = issue_numbers
        return []

    def fake_review_comments(owner, repo, *, pull_numbers=None):
        captured["pull_numbers"] = pull_numbers
        return []

    def fake_reviews(owner, repo, pull_number):
        captured.setdefault("review_pulls", []).append(pull_number)
        return []

    monkeypatch.setattr(adapter, "_repo_issue_comments", fake_issue_comments)
    monkeypatch.setattr(adapter, "_repo_review_comments", fake_review_comments)
    monkeypatch.setattr(adapter, "_fetch_reviews", fake_reviews)

    bundle = adapter._fetch_repo_bundle(
        {"full_name": "rheeger/test"},
        vault_path=str(tmp_vault),
        max_commits_per_repo=10,
        max_threads_per_repo=1,
        max_messages_per_thread=10,
    )

    assert len(bundle["threads"]) == 1
    assert captured["pull_numbers"] == ["11"]
    assert captured["issue_numbers"] == ["11"]
    assert captured["review_pulls"] == ["11"]
