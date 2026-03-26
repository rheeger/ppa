"""GitHub repository, commit, and discussion archive adapter using the gh CLI."""

from __future__ import annotations

import json
import os
import subprocess
import time
from collections import Counter
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from hfa.identity import IdentityCache
from hfa.identity_resolver import merge_into_existing
from hfa.provenance import ProvenanceEntry
from hfa.schema import (
    GitCommitCard,
    GitMessageCard,
    GitRepositoryCard,
    GitThreadCard,
    validate_card_permissive,
)
from hfa.uid import generate_uid
from hfa.vault import read_note

from .base import BaseAdapter, FetchedBatch, deterministic_provenance

REPO_SOURCE = "github.repo"
COMMIT_SOURCE = "github.commit"
THREAD_SOURCE = "github.thread"
MESSAGE_SOURCE = "github.message"
DEFAULT_BATCH_SIZE = 200
RATE_LIMIT_FLOOR = 250
DEFAULT_STAGE_WORKERS = 2
MAX_RETRIES = 6
GRAPHQL_PAGE_SIZE_CANDIDATES = (25, 10, 5)
REST_PAGE_SIZE_CANDIDATES = (25, 10, 5)
REQUEST_INTERVAL_SECONDS = 0.5
RETRYABLE_MARKERS = (
    "secondary rate limit",
    "you have exceeded a secondary rate limit",
    "rate limit exceeded",
    "api rate limit exceeded",
    "http 502",
    "502 bad gateway",
    "tls handshake timeout",
    "i/o timeout",
    "operation timed out",
    "stream error",
    "connection reset by peer",
    "context deadline exceeded",
    "403",
    "429",
    "abuse detection",
)

REFS_QUERY = """
query($owner: String!, $name: String!, $refPrefix: String!, $endCursor: String) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  repository(owner: $owner, name: $name) {
    refs(refPrefix: $refPrefix, first: 100, after: $endCursor) {
      nodes {
        name
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
""".strip()

ISSUES_QUERY = """
query($owner: String!, $name: String!, $pageSize: Int!, $endCursor: String) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  repository(owner: $owner, name: $name) {
    issues(first: $pageSize, after: $endCursor, orderBy: {field: UPDATED_AT, direction: DESC}, states: [OPEN, CLOSED]) {
      nodes {
        id
        number
        title
        url
        state
        createdAt
        updatedAt
        closedAt
        labels(first: 20) { nodes { name } }
        assignees(first: 20) { nodes { login } }
        milestone { title }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
""".strip()

PULLS_QUERY = """
query($owner: String!, $name: String!, $pageSize: Int!, $endCursor: String) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  repository(owner: $owner, name: $name) {
    pullRequests(first: $pageSize, after: $endCursor, orderBy: {field: UPDATED_AT, direction: DESC}, states: [OPEN, CLOSED, MERGED]) {
      nodes {
        id
        number
        title
        url
        state
        isDraft
        createdAt
        updatedAt
        closedAt
        mergedAt
        labels(first: 20) { nodes { name } }
        assignees(first: 20) { nodes { login } }
        milestone { title }
        baseRefName
        headRefName
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
""".strip()

COMMITS_QUERY = """
query($owner: String!, $name: String!, $qualifiedName: String!, $pageSize: Int!, $endCursor: String) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  repository(owner: $owner, name: $name) {
    ref(qualifiedName: $qualifiedName) {
      name
      target {
        ... on Commit {
          history(first: $pageSize, after: $endCursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              id
              oid
              url
              messageHeadline
              messageBody
              authoredDate
              committedDate
              additions
              deletions
              changedFilesIfAvailable
              parents(first: 10) {
                nodes {
                  oid
                }
              }
              author {
                name
                email
                user {
                  id
                  login
                  url
                }
              }
              committer {
                name
                email
                user {
                  id
                  login
                  url
                }
              }
              associatedPullRequests(first: 10) {
                nodes {
                  number
                  url
                }
              }
            }
          }
        }
        ... on Tag {
          target {
            ... on Commit {
              history(first: $pageSize, after: $endCursor) {
                pageInfo {
                  hasNextPage
                  endCursor
                }
                nodes {
                  id
                  oid
                  url
                  messageHeadline
                  messageBody
                  authoredDate
                  committedDate
                  additions
                  deletions
                  changedFilesIfAvailable
                  parents(first: 10) {
                    nodes {
                      oid
                    }
                  }
                  author {
                    name
                    email
                    user {
                      id
                      login
                      url
                    }
                  }
                  committer {
                    name
                    email
                    user {
                      id
                      login
                      url
                    }
                  }
                  associatedPullRequests(first: 10) {
                    nodes {
                      number
                      url
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
""".strip()


def _clean(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _clean_multiline(value: Any) -> str:
    lines = [line.rstrip() for line in str(value or "").replace("\r\n", "\n").split("\n")]
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)


def _clean_list(values: Iterable[Any], *, lower: bool = False) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean(value)
        if lower:
            text = text.lower()
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return cleaned


def _normalize_login(value: Any) -> str:
    return _clean(value).removeprefix("@").lower()


def _normalize_email(value: Any) -> str:
    text = _clean(value).lower()
    return text if "@" in text else ""


def _date_only(value: Any) -> str:
    text = _clean(value)
    if len(text) >= 10:
        return text[:10]
    return date.today().isoformat()


def _parse_iso(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _summary_from_text(value: str, *, fallback: str, limit: int = 120) -> str:
    text = _clean(value.splitlines()[0] if value else "")
    if not text:
        return fallback
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _repo_uid(name_with_owner: str) -> str:
    return generate_uid("git-repository", REPO_SOURCE, name_with_owner.lower())


def _commit_uid(name_with_owner: str, sha: str) -> str:
    return generate_uid("git-commit", COMMIT_SOURCE, f"{name_with_owner.lower()}:{sha.lower()}")


def _thread_uid(name_with_owner: str, thread_type: str, number: str) -> str:
    return generate_uid("git-thread", THREAD_SOURCE, f"{name_with_owner.lower()}:{thread_type}:{number}")


def _message_uid(name_with_owner: str, message_type: str, message_id: str) -> str:
    return generate_uid(
        "git-message",
        MESSAGE_SOURCE,
        f"{name_with_owner.lower()}:{message_type}:{message_id}",
    )


def _wikilink(uid: str) -> str:
    return f"[[{uid}]]"


def _issue_number_from_url(url: str) -> str:
    parts = [part for part in str(url or "").rstrip("/").split("/") if part]
    return parts[-1] if parts else ""


class GitHubHistoryAdapter(BaseAdapter):
    source_id = "github-history"
    preload_existing_uid_index = False

    def __init__(self) -> None:
        self._last_request_at = 0.0

    def _throttle_request(self) -> None:
        now = time.monotonic()
        delay = REQUEST_INTERVAL_SECONDS - (now - self._last_request_at)
        if delay > 0:
            time.sleep(delay)
        self._last_request_at = time.monotonic()

    def _gh_api(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        paginate: bool = False,
        slurp: bool = False,
        method: str = "GET",
        max_retries: int | None = None,
    ) -> Any:
        command = ["gh", "api", endpoint, "-X", method]
        if paginate:
            command.append("--paginate")
        if slurp:
            command.append("--slurp")
        for key, value in (params or {}).items():
            if value in (None, ""):
                continue
            command.extend(["-f", f"{key}={value}"])
        return self._run_gh_json(command, max_retries=max_retries)

    def _gh_graphql_page(
        self, query: str, variables: dict[str, Any], *, max_retries: int | None = None
    ) -> dict[str, Any]:
        command = ["gh", "api", "graphql", "-f", f"query={query}"]
        for key, value in variables.items():
            if value in (None, ""):
                continue
            command.extend(["-F", f"{key}={value}"])
        payload = self._run_gh_json(command, max_retries=max_retries)
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected GitHub GraphQL payload")
        return payload

    def _is_retryable_error(self, exc: Exception | str) -> bool:
        message = str(exc).lower()
        return any(marker in message for marker in RETRYABLE_MARKERS)

    def _retry_delay_seconds(self, exc: Exception | str, attempt: int) -> int:
        message = str(exc).lower()
        if "secondary rate limit" in message:
            return min(300, 60 * max(1, attempt))
        if "tls handshake timeout" in message or "i/o timeout" in message or "operation timed out" in message:
            return min(120, 15 * max(1, attempt))
        if "http 502" in message or "502 bad gateway" in message or "stream error" in message:
            return min(90, 10 * max(1, attempt))
        if "429" in message or "rate limit exceeded" in message:
            return min(180, 30 * max(1, attempt))
        return min(60, 2**attempt)

    def _run_gh_json(self, command: list[str], *, max_retries: int | None = None) -> Any:
        last_error: RuntimeError | None = None
        command_preview = " ".join(command[:6]) + (" ..." if len(command) > 6 else "")
        attempts_total = max(1, int(max_retries or MAX_RETRIES))
        for attempt in range(1, attempts_total + 1):
            self._throttle_request()
            proc = subprocess.run(command, capture_output=True, text=True, check=False)
            if proc.returncode == 0:
                try:
                    return json.loads(proc.stdout)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"Invalid gh JSON output: {exc}") from exc

            stderr = (proc.stderr or proc.stdout or "").strip()
            message = stderr.lower()
            if attempt < attempts_total and any(marker in message for marker in RETRYABLE_MARKERS):
                sleep_seconds = self._retry_delay_seconds(stderr or "gh command failed", attempt)
                time.sleep(sleep_seconds)
                last_error = RuntimeError(f"{stderr or 'gh command failed'} | command={command_preview}")
                continue
            raise RuntimeError(f"{stderr or 'gh command failed'} | command={command_preview}")
        if last_error is not None:
            raise last_error
        raise RuntimeError("gh command failed")

    def _paged_rest_rows(
        self,
        endpoint: str,
        *,
        base_params: dict[str, Any] | None = None,
        page_size_candidates: tuple[int, ...] = REST_PAGE_SIZE_CANDIDATES,
    ) -> list[dict[str, Any]]:
        last_error: RuntimeError | None = None
        for per_page in page_size_candidates:
            params = dict(base_params or {})
            params["per_page"] = per_page
            try:
                payload = self._gh_api(
                    endpoint,
                    params=params,
                    paginate=True,
                    slurp=True,
                    max_retries=1,
                )
            except RuntimeError as exc:
                last_error = exc
                if self._is_retryable_error(exc):
                    continue
                raise
            pages = payload if isinstance(payload, list) else [payload]
            rows: list[dict[str, Any]] = []
            for page in pages:
                if isinstance(page, list):
                    rows.extend(item for item in page if isinstance(item, dict))
            return rows
        if last_error is not None:
            raise last_error
        return []

    def _respect_rate_limit(self, payload: dict[str, Any]) -> None:
        rate = payload.get("data", {}).get("rateLimit", {}) if isinstance(payload, dict) else {}
        try:
            remaining = int(rate.get("remaining", 0) or 0)
        except (TypeError, ValueError):
            remaining = 0
        reset_at = _clean(rate.get("resetAt", ""))
        if remaining >= RATE_LIMIT_FLOOR or not reset_at:
            return
        reset_dt = _parse_iso(reset_at)
        if reset_dt is None:
            return
        now = datetime.now(timezone.utc)
        sleep_seconds = max(0.0, (reset_dt - now).total_seconds()) + 1.0
        if sleep_seconds > 0:
            time.sleep(min(sleep_seconds, 300.0))

    def _list_visible_repositories(self, *, max_repos: int | None = None) -> list[dict[str, Any]]:
        payload = self._gh_api(
            "user/repos",
            params={
                "affiliation": "owner,collaborator,organization_member",
                "per_page": 100,
                "sort": "updated",
                "direction": "desc",
            },
            paginate=True,
            slurp=True,
        )
        pages = payload if isinstance(payload, list) else [payload]
        repos: list[dict[str, Any]] = []
        seen: set[str] = set()
        for page in pages:
            if not isinstance(page, list):
                continue
            for repo in page:
                if not isinstance(repo, dict):
                    continue
                full_name = _clean(repo.get("full_name", ""))
                if not full_name or full_name in seen:
                    continue
                seen.add(full_name)
                repos.append(repo)
                if max_repos is not None and len(repos) >= max(0, int(max_repos)):
                    return repos
        return repos

    def _fetch_ref_names(self, *, owner: str, repo: str, ref_prefix: str) -> list[str]:
        cursor: str | None = None
        names: list[str] = []
        seen: set[str] = set()
        while True:
            payload = self._gh_graphql_page(
                REFS_QUERY,
                {
                    "owner": owner,
                    "name": repo,
                    "refPrefix": ref_prefix,
                    "endCursor": cursor,
                },
            )
            self._respect_rate_limit(payload)
            refs = payload.get("data", {}).get("repository", {}).get("refs", {})
            nodes = refs.get("nodes") or []
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                name = _clean(node.get("name", ""))
                if not name or name in seen:
                    continue
                seen.add(name)
                names.append(name)
            page_info = refs.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = _clean(page_info.get("endCursor", ""))
            if not cursor:
                break
        return names

    def _paged_graphql_connection(
        self,
        query: str,
        *,
        owner: str,
        repo: str,
        connection_name: str,
        page_size_candidates: tuple[int, ...] = GRAPHQL_PAGE_SIZE_CANDIDATES,
    ) -> list[dict[str, Any]]:
        cursor: str | None = None
        page_size_index = 0
        rows: list[dict[str, Any]] = []
        while True:
            payload: dict[str, Any] | None = None
            last_error: RuntimeError | None = None
            for candidate_index in range(page_size_index, len(page_size_candidates)):
                page_size = page_size_candidates[candidate_index]
                try:
                    payload = self._gh_graphql_page(
                        query,
                        {
                            "owner": owner,
                            "name": repo,
                            "pageSize": page_size,
                            "endCursor": cursor,
                        },
                        max_retries=1,
                    )
                    page_size_index = candidate_index
                    break
                except RuntimeError as exc:
                    last_error = exc
                    if not self._is_retryable_error(exc):
                        raise
            if payload is None:
                assert last_error is not None
                raise last_error
            self._respect_rate_limit(payload)
            connection = payload.get("data", {}).get("repository", {}).get(connection_name, {}) or {}
            nodes = connection.get("nodes") or []
            rows.extend(node for node in nodes if isinstance(node, dict))
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = _clean(page_info.get("endCursor", ""))
            if not cursor:
                break
        return rows

    def _fetch_commit_history_for_ref(
        self,
        *,
        owner: str,
        repo: str,
        qualified_name: str,
        max_commits: int | None = None,
        seen_shas: set[str] | None = None,
        stop_when_page_seen: bool = False,
    ) -> list[dict[str, Any]]:
        cursor: str | None = None
        commits: list[dict[str, Any]] = []
        page_size_index = 0
        while True:
            payload: dict[str, Any] | None = None
            last_error: RuntimeError | None = None
            for candidate_index in range(page_size_index, len(GRAPHQL_PAGE_SIZE_CANDIDATES)):
                page_size = GRAPHQL_PAGE_SIZE_CANDIDATES[candidate_index]
                try:
                    payload = self._gh_graphql_page(
                        COMMITS_QUERY,
                        {
                            "owner": owner,
                            "name": repo,
                            "qualifiedName": qualified_name,
                            "pageSize": page_size,
                            "endCursor": cursor,
                        },
                        max_retries=1,
                    )
                    page_size_index = candidate_index
                    break
                except RuntimeError as exc:
                    last_error = exc
                    if not self._is_retryable_error(exc):
                        raise
            if payload is None:
                assert last_error is not None
                raise last_error
            self._respect_rate_limit(payload)
            ref_payload = payload.get("data", {}).get("repository", {}).get("ref", {}) or {}
            target = ref_payload.get("target", {}) or {}
            history = target.get("history", {}) if isinstance(target, dict) else {}
            if not history and isinstance(target, dict):
                history = (
                    ((target.get("target") or {}).get("history", {})) if isinstance(target.get("target"), dict) else {}
                )
            nodes = history.get("nodes") or []
            page_new_count = 0
            if isinstance(nodes, list):
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    sha = _clean(node.get("oid", "")).lower()
                    if seen_shas is not None and sha and sha in seen_shas:
                        commits.append(node)
                        continue
                    if sha:
                        page_new_count += 1
                    commits.append(node)
            if stop_when_page_seen and seen_shas is not None and nodes and page_new_count == 0:
                break
            if max_commits is not None and len(commits) >= max(0, int(max_commits)):
                return commits[: max(0, int(max_commits))]
            page_info = history.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = _clean(page_info.get("endCursor", ""))
            if not cursor:
                break
        return commits

    def _fetch_commits(
        self,
        *,
        owner: str,
        repo: str,
        max_commits: int | None = None,
        default_branch: str = "",
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        repo_meta = {"nameWithOwner": f"{owner}/{repo}"}
        branch_names = self._fetch_ref_names(owner=owner, repo=repo, ref_prefix="refs/heads/")
        tag_names = self._fetch_ref_names(owner=owner, repo=repo, ref_prefix="refs/tags/")
        ordered_branch_names: list[str] = []
        normalized_default = _clean(default_branch)
        if normalized_default and normalized_default in branch_names:
            ordered_branch_names.append(normalized_default)
        ordered_branch_names.extend(name for name in branch_names if name != normalized_default)
        ref_names = [f"refs/heads/{branch_name}" for branch_name in ordered_branch_names]
        ref_names.extend(f"refs/tags/{tag_name}" for tag_name in tag_names)
        if not ref_names:
            return repo_meta, []
        commits: list[dict[str, Any]] = []
        seen_shas: set[str] = set()
        default_ref = f"refs/heads/{normalized_default}" if normalized_default else ""
        for qualified_name in ref_names:
            branch_commits = self._fetch_commit_history_for_ref(
                owner=owner,
                repo=repo,
                qualified_name=qualified_name,
                max_commits=max_commits,
                seen_shas=seen_shas,
                stop_when_page_seen=bool(default_ref) and qualified_name != default_ref,
            )
            for commit in branch_commits:
                sha = _clean(commit.get("oid", "")).lower()
                if not sha or sha in seen_shas:
                    continue
                seen_shas.add(sha)
                commits.append(commit)
                if max_commits is not None and len(commits) >= max(0, int(max_commits)):
                    return repo_meta, commits[: max(0, int(max_commits))]
        return repo_meta, commits

    def _fetch_reviews(
        self,
        *,
        owner: str,
        repo: str,
        pull_number: str,
    ) -> list[dict[str, Any]]:
        return self._paged_rest_rows(
            f"repos/{owner}/{repo}/pulls/{pull_number}/reviews",
            base_params={},
        )

    def _repo_detail(self, owner: str, repo: str) -> dict[str, Any]:
        payload = self._gh_api(f"repos/{owner}/{repo}")
        return payload if isinstance(payload, dict) else {}

    def _repo_issues(self, owner: str, repo: str) -> list[dict[str, Any]]:
        rows = self._paged_graphql_connection(
            ISSUES_QUERY,
            owner=owner,
            repo=repo,
            connection_name="issues",
        )
        normalized: list[dict[str, Any]] = []
        for row in rows:
            normalized.append(
                {
                    "id": _clean(row.get("id", "")),
                    "node_id": _clean(row.get("id", "")),
                    "number": row.get("number"),
                    "title": _clean(row.get("title", "")),
                    "html_url": _clean(row.get("url", "")),
                    "url": _clean(row.get("url", "")),
                    "state": _clean(row.get("state", "")).lower(),
                    "created_at": _clean(row.get("createdAt", "")),
                    "updated_at": _clean(row.get("updatedAt", "")),
                    "closed_at": _clean(row.get("closedAt", "")),
                    "labels": [
                        {"name": _clean((label or {}).get("name", ""))}
                        for label in ((row.get("labels") or {}).get("nodes") or [])
                    ],
                    "assignees": [
                        {"login": _clean((assignee or {}).get("login", ""))}
                        for assignee in ((row.get("assignees") or {}).get("nodes") or [])
                    ],
                    "milestone": (
                        {"title": _clean(((row.get("milestone") or {}).get("title", "")))}
                        if row.get("milestone")
                        else None
                    ),
                }
            )
        return normalized

    def _repo_pulls(self, owner: str, repo: str) -> list[dict[str, Any]]:
        rows = self._paged_graphql_connection(
            PULLS_QUERY,
            owner=owner,
            repo=repo,
            connection_name="pullRequests",
        )
        normalized: list[dict[str, Any]] = []
        for row in rows:
            normalized.append(
                {
                    "id": _clean(row.get("id", "")),
                    "node_id": _clean(row.get("id", "")),
                    "number": row.get("number"),
                    "title": _clean(row.get("title", "")),
                    "html_url": _clean(row.get("url", "")),
                    "url": _clean(row.get("url", "")),
                    "state": _clean(row.get("state", "")).lower(),
                    "draft": bool(row.get("isDraft", False)),
                    "created_at": _clean(row.get("createdAt", "")),
                    "updated_at": _clean(row.get("updatedAt", "")),
                    "closed_at": _clean(row.get("closedAt", "")),
                    "merged_at": _clean(row.get("mergedAt", "")),
                    "labels": [
                        {"name": _clean((label or {}).get("name", ""))}
                        for label in ((row.get("labels") or {}).get("nodes") or [])
                    ],
                    "assignees": [
                        {"login": _clean((assignee or {}).get("login", ""))}
                        for assignee in ((row.get("assignees") or {}).get("nodes") or [])
                    ],
                    "milestone": (
                        {"title": _clean(((row.get("milestone") or {}).get("title", "")))}
                        if row.get("milestone")
                        else None
                    ),
                    "base": {"ref": _clean(row.get("baseRefName", ""))},
                    "head": {"ref": _clean(row.get("headRefName", ""))},
                }
            )
        return normalized

    def _repo_issue_comments(
        self, owner: str, repo: str, *, issue_numbers: list[str] | None = None
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for issue_number in issue_numbers or []:
            rows.extend(
                self._paged_rest_rows(
                    f"repos/{owner}/{repo}/issues/{issue_number}/comments",
                    base_params={"sort": "created", "direction": "asc"},
                )
            )
        return rows

    def _repo_review_comments(
        self, owner: str, repo: str, *, pull_numbers: list[str] | None = None
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for pull_number in pull_numbers or []:
            rows.extend(
                self._paged_rest_rows(
                    f"repos/{owner}/{repo}/pulls/{pull_number}/comments",
                    base_params={"sort": "created", "direction": "asc"},
                )
            )
        return rows

    def _resolve_people(
        self,
        identity_cache: IdentityCache,
        *,
        logins: Iterable[str] = (),
        emails: Iterable[str] = (),
    ) -> list[str]:
        links: list[str] = []
        for login in logins:
            normalized = _normalize_login(login)
            if not normalized:
                continue
            resolved = identity_cache.resolve("github", normalized)
            if resolved and resolved not in links:
                links.append(resolved)
        for email in emails:
            normalized = _normalize_email(email)
            if not normalized:
                continue
            resolved = identity_cache.resolve("email", normalized)
            if resolved and resolved not in links:
                links.append(resolved)
        return links

    def _provenance_entry(self, source: str) -> ProvenanceEntry:
        return ProvenanceEntry(source=source, date=date.today().isoformat(), method="deterministic")

    def _enhance_person_with_github(
        self,
        vault_path: str | Path,
        *,
        login: str,
        email: str = "",
        source: str,
    ) -> bool:
        normalized_login = _normalize_login(login)
        normalized_email = _normalize_email(email)
        if not normalized_login:
            return False
        identity_cache = IdentityCache(vault_path)
        wikilink = identity_cache.resolve("github", normalized_login)
        if wikilink is None and normalized_email:
            wikilink = identity_cache.resolve("email", normalized_email)
        if wikilink is None:
            return False

        slug = wikilink.removeprefix("[[").removesuffix("]]")
        note = read_note(vault_path, f"People/{slug}.md")
        frontmatter = note[0]
        validate_card_permissive(frontmatter)
        existing_github = _normalize_login(frontmatter.get("github", ""))
        existing_emails = {_normalize_email(item) for item in frontmatter.get("emails", []) if _normalize_email(item)}
        incoming_data: dict[str, Any] = {"source": [source]}
        incoming_provenance: dict[str, ProvenanceEntry] = {}
        should_merge = False

        if not existing_github:
            incoming_data["github"] = normalized_login
            incoming_provenance["github"] = self._provenance_entry(source)
            should_merge = True
        elif existing_github != normalized_login:
            return False

        if normalized_email and normalized_email not in existing_emails:
            incoming_data["emails"] = [normalized_email]
            incoming_provenance["emails"] = self._provenance_entry(source)
            should_merge = True

        if source not in frontmatter.get("source", []):
            should_merge = True

        if not should_merge:
            return False

        merge_into_existing(
            vault_path,
            wikilink,
            incoming_data,
            incoming_provenance,
        )
        return True

    def _select_discussion_threads(
        self,
        *,
        issues: list[dict[str, Any]],
        pulls: list[dict[str, Any]],
        max_threads: int | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        issue_rows = [item for item in issues if not item.get("pull_request") and str(item.get("number", "")).strip()]
        pull_rows = [item for item in pulls if str(item.get("number", "")).strip()]
        if max_threads is None:
            return issue_rows, pull_rows
        combined: list[tuple[str, dict[str, Any]]] = [("issue", item) for item in issue_rows] + [
            ("pull_request", item) for item in pull_rows
        ]
        combined.sort(
            key=lambda pair: (
                _clean((pair[1] or {}).get("updated_at", "")),
                str((pair[1] or {}).get("number", "")),
            ),
            reverse=True,
        )
        selected = combined[: max(0, int(max_threads))]
        selected_issues = [item for kind, item in selected if kind == "issue"]
        selected_pulls = [item for kind, item in selected if kind == "pull_request"]
        return selected_issues, selected_pulls

    def _build_repo_item(
        self,
        repo_detail: dict[str, Any],
        repo_meta: dict[str, Any],
        identity_cache: IdentityCache,
    ) -> dict[str, Any]:
        owner = repo_detail.get("owner") or repo_meta.get("owner") or {}
        owner_login = _normalize_login(owner.get("login", ""))
        owner_type = _clean(owner.get("type") or owner.get("__typename", ""))
        name_with_owner = _clean(repo_detail.get("full_name") or repo_meta.get("nameWithOwner"))
        repo_id = str(repo_detail.get("id") or repo_meta.get("id") or "").strip()
        topics = repo_detail.get("topics")
        if topics in (None, []):
            topics = [
                ((node or {}).get("topic") or {}).get("name", "")
                for node in ((repo_meta.get("repositoryTopics") or {}).get("nodes") or [])
                if isinstance(node, dict)
            ]
        languages = repo_detail.get("languages")
        if isinstance(languages, list):
            language_names = [
                ((item or {}).get("name", "") if isinstance(item, dict) else str(item)) for item in languages
            ]
        else:
            language_names = [
                (node or {}).get("name", "")
                for node in ((repo_meta.get("languages") or {}).get("nodes") or [])
                if isinstance(node, dict)
            ]
        owner_people = self._resolve_people(identity_cache, logins=[owner_login])
        orgs: list[str] = []
        if owner_type.lower() == "organization" and owner_login:
            orgs.append(owner_login)
        return {
            "kind": "repo",
            "source": [REPO_SOURCE],
            "source_id": name_with_owner.lower(),
            "summary": name_with_owner,
            "people": owner_people,
            "orgs": orgs,
            "github_repo_id": repo_id,
            "github_node_id": _clean(repo_meta.get("id", "")),
            "name_with_owner": name_with_owner,
            "owner_login": owner_login,
            "owner_type": owner_type,
            "html_url": _clean(repo_detail.get("html_url") or repo_meta.get("url")),
            "api_url": _clean(repo_detail.get("url", "")),
            "ssh_url": _clean(repo_detail.get("ssh_url") or repo_meta.get("sshUrl")),
            "default_branch": _clean(
                repo_detail.get("default_branch") or ((repo_meta.get("defaultBranchRef") or {}).get("name", ""))
            ),
            "homepage_url": _clean(repo_detail.get("homepage") or repo_meta.get("homepageUrl")),
            "description": _clean(repo_detail.get("description") or repo_meta.get("description")),
            "visibility": _clean(repo_detail.get("visibility") or repo_meta.get("visibility")),
            "is_private": bool(
                repo_detail.get("private") if "private" in repo_detail else repo_meta.get("isPrivate", False)
            ),
            "is_fork": bool(repo_detail.get("fork") if "fork" in repo_detail else repo_meta.get("isFork", False)),
            "is_archived": bool(
                repo_detail.get("archived") if "archived" in repo_detail else repo_meta.get("isArchived", False)
            ),
            "parent_name_with_owner": _clean(
                ((repo_detail.get("parent") or {}).get("full_name", ""))
                or ((repo_meta.get("parent") or {}).get("nameWithOwner", ""))
            ),
            "primary_language": _clean(
                (
                    (repo_detail.get("language") or "")
                    if not isinstance(repo_detail.get("language"), dict)
                    else repo_detail["language"].get("name", "")
                )
            ),
            "languages": _clean_list(language_names),
            "topics": _clean_list(topics or [], lower=True),
            "license_name": _clean(
                ((repo_detail.get("license") or {}).get("name", ""))
                or ((repo_meta.get("licenseInfo") or {}).get("name", ""))
            ),
            "created_at": _clean(repo_detail.get("created_at") or repo_meta.get("createdAt")),
            "pushed_at": _clean(repo_detail.get("pushed_at") or repo_meta.get("pushedAt")),
            "created": _date_only(repo_detail.get("created_at") or repo_meta.get("createdAt")),
            "updated": _date_only(
                repo_detail.get("updated_at")
                or repo_meta.get("updatedAt")
                or repo_detail.get("pushed_at")
                or repo_meta.get("pushedAt")
            ),
            "body": "",
        }

    def _build_commit_item(
        self,
        name_with_owner: str,
        commit: dict[str, Any],
        identity_cache: IdentityCache,
    ) -> dict[str, Any]:
        sha = _clean(commit.get("oid", "")).lower()
        author = commit.get("author") or {}
        committer = commit.get("committer") or {}
        author_user = author.get("user") or {}
        committer_user = committer.get("user") or {}
        author_login = _normalize_login(author_user.get("login", ""))
        committer_login = _normalize_login(committer_user.get("login", ""))
        author_email = _normalize_email(author.get("email", ""))
        committer_email = _normalize_email(committer.get("email", ""))
        people = self._resolve_people(
            identity_cache,
            logins=[author_login, committer_login],
            emails=[author_email, committer_email],
        )
        prs = commit.get("associatedPullRequests") or {}
        pr_nodes = prs.get("nodes") or []
        message_body = _clean_multiline(commit.get("messageBody", ""))
        return {
            "kind": "commit",
            "source": [COMMIT_SOURCE],
            "source_id": f"{name_with_owner.lower()}:{sha}",
            "summary": _clean(commit.get("messageHeadline", "")) or sha[:12],
            "people": people,
            "orgs": [],
            "github_node_id": _clean(commit.get("id", "")),
            "commit_sha": sha,
            "repository_name_with_owner": name_with_owner,
            "repository": _wikilink(_repo_uid(name_with_owner)),
            "parent_shas": _clean_list(
                [((node or {}).get("oid", "")) for node in ((commit.get("parents") or {}).get("nodes") or [])],
                lower=True,
            ),
            "html_url": _clean(commit.get("url", "")),
            "api_url": "",
            "authored_at": _clean(commit.get("authoredDate", "")),
            "committed_at": _clean(commit.get("committedDate", "")),
            "message_headline": _clean(commit.get("messageHeadline", "")),
            "additions": int(commit.get("additions", 0) or 0),
            "deletions": int(commit.get("deletions", 0) or 0),
            "changed_files": int(commit.get("changedFilesIfAvailable", 0) or 0),
            "author_login": author_login,
            "author_name": _clean(author.get("name", "")),
            "author_email": author_email,
            "committer_login": committer_login,
            "committer_name": _clean(committer.get("name", "")),
            "committer_email": committer_email,
            "associated_pr_numbers": _clean_list([str((node or {}).get("number", "")) for node in pr_nodes]),
            "associated_pr_urls": _clean_list([((node or {}).get("url", "")) for node in pr_nodes]),
            "created": _date_only(commit.get("committedDate") or commit.get("authoredDate")),
            "updated": _date_only(commit.get("committedDate") or commit.get("authoredDate")),
            "body": message_body,
        }

    def _thread_base_item(
        self,
        *,
        name_with_owner: str,
        thread_type: str,
        number: str,
        github_thread_id: str,
        github_node_id: str,
        html_url: str,
        api_url: str,
        title: str,
        state: str,
        is_draft: bool,
        merged_at: str,
        closed_at: str,
        labels: list[str],
        assignees: list[str],
        milestone: str,
        base_ref: str,
        head_ref: str,
        created_at: str,
        updated_at: str,
    ) -> dict[str, Any]:
        return {
            "kind": "thread",
            "source": [THREAD_SOURCE, f"{THREAD_SOURCE}.{thread_type}"],
            "source_id": f"{name_with_owner.lower()}:{thread_type}:{number}",
            "summary": _clean(title) or f"{name_with_owner} #{number}",
            "people": [],
            "orgs": [],
            "github_thread_id": github_thread_id,
            "github_node_id": github_node_id,
            "repository_name_with_owner": name_with_owner,
            "repository": _wikilink(_repo_uid(name_with_owner)),
            "thread_type": thread_type,
            "number": number,
            "html_url": html_url,
            "api_url": api_url,
            "state": state,
            "is_draft": is_draft,
            "merged_at": merged_at,
            "closed_at": closed_at,
            "title": title,
            "labels": labels,
            "assignees": assignees,
            "milestone": milestone,
            "base_ref": base_ref,
            "head_ref": head_ref,
            "participant_logins": [],
            "messages": [],
            "first_message_at": "",
            "last_message_at": "",
            "message_count": 0,
            "created": _date_only(created_at or merged_at or closed_at),
            "updated": _date_only(updated_at or merged_at or closed_at or created_at),
            "body": "",
        }

    def _message_item(
        self,
        *,
        name_with_owner: str,
        thread_type: str,
        thread_number: str,
        message_type: str,
        github_message_id: str,
        github_node_id: str,
        html_url: str,
        api_url: str,
        actor_login: str,
        actor_name: str,
        actor_email: str,
        sent_at: str,
        updated_at: str,
        body: str,
        review_state: str = "",
        review_commit_sha: str = "",
        in_reply_to_message_id: str = "",
        path: str = "",
        position: str = "",
        original_position: str = "",
        original_commit_sha: str = "",
        diff_hunk: str = "",
        identity_cache: IdentityCache | None = None,
    ) -> dict[str, Any]:
        people = (
            self._resolve_people(
                identity_cache,
                logins=[actor_login],
                emails=[actor_email],
            )
            if identity_cache is not None
            else []
        )
        thread_uid = _thread_uid(name_with_owner, thread_type, thread_number)
        summary_fallback = (
            f"{message_type} on {path}" if path else f"{message_type} by {actor_login or actor_name or 'unknown'}"
        )
        return {
            "kind": "message",
            "source": [MESSAGE_SOURCE, f"{MESSAGE_SOURCE}.{message_type}"],
            "source_id": f"{name_with_owner.lower()}:{message_type}:{github_message_id}",
            "summary": _summary_from_text(body, fallback=summary_fallback),
            "people": people,
            "orgs": [],
            "github_message_id": github_message_id,
            "github_node_id": github_node_id,
            "repository_name_with_owner": name_with_owner,
            "repository": _wikilink(_repo_uid(name_with_owner)),
            "thread": _wikilink(thread_uid),
            "message_type": message_type,
            "html_url": html_url,
            "api_url": api_url,
            "actor_login": actor_login,
            "actor_name": actor_name,
            "actor_email": actor_email,
            "sent_at": sent_at,
            "updated_at": updated_at,
            "review_state": review_state,
            "review_commit_sha": review_commit_sha,
            "in_reply_to_message_id": in_reply_to_message_id,
            "path": path,
            "position": position,
            "original_position": original_position,
            "original_commit_sha": original_commit_sha,
            "diff_hunk": diff_hunk,
            "created": _date_only(sent_at or updated_at),
            "updated": _date_only(updated_at or sent_at),
            "body": _clean_multiline(body),
        }

    def _build_discussion_items(
        self,
        *,
        name_with_owner: str,
        issues: list[dict[str, Any]],
        pulls: list[dict[str, Any]],
        issue_comments: list[dict[str, Any]],
        review_comments: list[dict[str, Any]],
        reviews_by_pull: dict[str, list[dict[str, Any]]],
        identity_cache: IdentityCache,
        max_threads: int | None = None,
        max_messages_per_thread: int | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        thread_items: dict[str, dict[str, Any]] = {}
        message_items: list[dict[str, Any]] = []

        pulls_by_number: dict[str, dict[str, Any]] = {
            str(item.get("number", "")).strip(): item for item in pulls if str(item.get("number", "")).strip()
        }

        for issue in issues:
            number = str(issue.get("number", "")).strip()
            if not number or issue.get("pull_request"):
                continue
            thread_uid = _thread_uid(name_with_owner, "issue", number)
            thread_items[thread_uid] = self._thread_base_item(
                name_with_owner=name_with_owner,
                thread_type="issue",
                number=number,
                github_thread_id=str(issue.get("id", "")).strip(),
                github_node_id=_clean(issue.get("node_id", "")),
                html_url=_clean(issue.get("html_url", "")),
                api_url=_clean(issue.get("url", "")),
                title=_clean(issue.get("title", "")),
                state=_clean(issue.get("state", "")),
                is_draft=False,
                merged_at="",
                closed_at=_clean(issue.get("closed_at", "")),
                labels=_clean_list(
                    [((label or {}).get("name", "")) for label in issue.get("labels", []) or []],
                    lower=True,
                ),
                assignees=_clean_list(
                    [((assignee or {}).get("login", "")) for assignee in issue.get("assignees", []) or []],
                    lower=True,
                ),
                milestone=_clean(((issue.get("milestone") or {}).get("title", ""))),
                base_ref="",
                head_ref="",
                created_at=_clean(issue.get("created_at", "")),
                updated_at=_clean(issue.get("updated_at", "")),
            )

        for pull in pulls:
            number = str(pull.get("number", "")).strip()
            if not number:
                continue
            thread_uid = _thread_uid(name_with_owner, "pull_request", number)
            thread_items[thread_uid] = self._thread_base_item(
                name_with_owner=name_with_owner,
                thread_type="pull_request",
                number=number,
                github_thread_id=str(pull.get("id", "")).strip(),
                github_node_id=_clean(pull.get("node_id", "")),
                html_url=_clean(pull.get("html_url", "")),
                api_url=_clean(pull.get("url", "")),
                title=_clean(pull.get("title", "")),
                state=_clean(pull.get("state", "")),
                is_draft=bool(pull.get("draft", False)),
                merged_at=_clean(pull.get("merged_at", "")),
                closed_at=_clean(pull.get("closed_at", "")),
                labels=_clean_list(
                    [((label or {}).get("name", "")) for label in pull.get("labels", []) or []],
                    lower=True,
                ),
                assignees=_clean_list(
                    [((assignee or {}).get("login", "")) for assignee in pull.get("assignees", []) or []],
                    lower=True,
                ),
                milestone=_clean(((pull.get("milestone") or {}).get("title", ""))),
                base_ref=_clean(((pull.get("base") or {}).get("ref", ""))),
                head_ref=_clean(((pull.get("head") or {}).get("ref", ""))),
                created_at=_clean(pull.get("created_at", "")),
                updated_at=_clean(pull.get("updated_at", "")),
            )

        ordered_threads = sorted(
            thread_items.values(),
            key=lambda item: (
                _clean(item.get("updated", "")),
                _clean(item.get("number", "")),
            ),
            reverse=True,
        )
        if max_threads is not None:
            ordered_threads = ordered_threads[: max(0, int(max_threads))]
        allowed_thread_keys = {(item["thread_type"], item["number"]) for item in ordered_threads}
        thread_items = {
            _thread_uid(name_with_owner, item["thread_type"], item["number"]): item for item in ordered_threads
        }

        def _append_message(item: dict[str, Any], thread_type: str, thread_number: str) -> None:
            if (thread_type, thread_number) not in allowed_thread_keys:
                return
            message_items.append(item)

        for comment in issue_comments:
            issue_number = _issue_number_from_url(comment.get("issue_url", ""))
            if not issue_number:
                continue
            thread_type = "pull_request" if issue_number in pulls_by_number else "issue"
            user = comment.get("user") or {}
            _append_message(
                self._message_item(
                    name_with_owner=name_with_owner,
                    thread_type=thread_type,
                    thread_number=issue_number,
                    message_type="issue_comment",
                    github_message_id=str(comment.get("id", "")).strip(),
                    github_node_id=_clean(comment.get("node_id", "")),
                    html_url=_clean(comment.get("html_url", "")),
                    api_url=_clean(comment.get("url", "")),
                    actor_login=_normalize_login(user.get("login", "")),
                    actor_name=_clean(user.get("login", "")),
                    actor_email="",
                    sent_at=_clean(comment.get("created_at", "")),
                    updated_at=_clean(comment.get("updated_at", "")),
                    body=str(comment.get("body", "") or ""),
                    identity_cache=identity_cache,
                ),
                thread_type,
                issue_number,
            )

        for pull_number, reviews in reviews_by_pull.items():
            for review in reviews:
                user = review.get("user") or {}
                _append_message(
                    self._message_item(
                        name_with_owner=name_with_owner,
                        thread_type="pull_request",
                        thread_number=pull_number,
                        message_type="review",
                        github_message_id=str(review.get("id", "")).strip(),
                        github_node_id=_clean(review.get("node_id", "")),
                        html_url=_clean(review.get("html_url", "")),
                        api_url=_clean(review.get("url", "")),
                        actor_login=_normalize_login(user.get("login", "")),
                        actor_name=_clean(user.get("login", "")),
                        actor_email="",
                        sent_at=_clean(review.get("submitted_at", "") or review.get("created_at", "")),
                        updated_at=_clean(review.get("submitted_at", "") or review.get("created_at", "")),
                        body=str(review.get("body", "") or ""),
                        review_state=_clean(review.get("state", "")),
                        review_commit_sha=_clean(review.get("commit_id", "")),
                        identity_cache=identity_cache,
                    ),
                    "pull_request",
                    pull_number,
                )

        for comment in review_comments:
            pull_number = _issue_number_from_url(comment.get("pull_request_url", ""))
            if not pull_number:
                continue
            user = comment.get("user") or {}
            _append_message(
                self._message_item(
                    name_with_owner=name_with_owner,
                    thread_type="pull_request",
                    thread_number=pull_number,
                    message_type="review_comment",
                    github_message_id=str(comment.get("id", "")).strip(),
                    github_node_id=_clean(comment.get("node_id", "")),
                    html_url=_clean(comment.get("html_url", "")),
                    api_url=_clean(comment.get("url", "")),
                    actor_login=_normalize_login(user.get("login", "")),
                    actor_name=_clean(user.get("login", "")),
                    actor_email="",
                    sent_at=_clean(comment.get("created_at", "")),
                    updated_at=_clean(comment.get("updated_at", "")),
                    body=str(comment.get("body", "") or ""),
                    review_commit_sha=_clean(comment.get("commit_id", "")),
                    in_reply_to_message_id=_clean(comment.get("in_reply_to_id", "")),
                    path=_clean(comment.get("path", "")),
                    position=_clean(comment.get("position", "")),
                    original_position=_clean(comment.get("original_position", "")),
                    original_commit_sha=_clean(comment.get("original_commit_id", "")),
                    diff_hunk=_clean_multiline(comment.get("diff_hunk", "")),
                    identity_cache=identity_cache,
                ),
                "pull_request",
                pull_number,
            )

        messages_by_thread: dict[str, list[dict[str, Any]]] = {}
        for message in message_items:
            thread_ref = _clean(message.get("thread", ""))
            messages_by_thread.setdefault(thread_ref, []).append(message)

        finalized_threads: list[dict[str, Any]] = []
        for thread in thread_items.values():
            thread_ref = _wikilink(_thread_uid(name_with_owner, thread["thread_type"], thread["number"]))
            thread_messages = sorted(
                messages_by_thread.get(thread_ref, []),
                key=lambda item: (
                    _clean(item.get("sent_at", "")),
                    _clean(item.get("github_message_id", "")),
                ),
            )
            if max_messages_per_thread is not None:
                thread_messages = thread_messages[: max(0, int(max_messages_per_thread))]
            participant_logins = set(thread.get("assignees", []))
            people = list(thread.get("people", []))
            message_refs: list[str] = []
            timestamps: list[str] = []
            for message in thread_messages:
                message_uid = _message_uid(
                    name_with_owner,
                    message["message_type"],
                    message["github_message_id"],
                )
                message_refs.append(_wikilink(message_uid))
                if _clean(message.get("sent_at", "")):
                    timestamps.append(_clean(message.get("sent_at", "")))
                actor_login = _normalize_login(message.get("actor_login", ""))
                if actor_login:
                    participant_logins.add(actor_login)
                for person in message.get("people", []):
                    if person not in people:
                        people.append(person)
            thread["participant_logins"] = _clean_list(participant_logins, lower=True)
            thread["people"] = people
            thread["messages"] = message_refs
            thread["message_count"] = len(thread_messages)
            if timestamps:
                thread["first_message_at"] = min(timestamps)
                thread["last_message_at"] = max(timestamps)
            created_hint = (
                _clean(thread.get("first_message_at", ""))
                or _clean(thread.get("merged_at", ""))
                or _clean(thread.get("closed_at", ""))
            )
            updated_hint = (
                _clean(thread.get("last_message_at", ""))
                or _clean(thread.get("merged_at", ""))
                or _clean(thread.get("closed_at", ""))
            )
            thread["created"] = _date_only(created_hint or thread.get("created", ""))
            thread["updated"] = _date_only(updated_hint or thread.get("updated", ""))
            finalized_threads.append(thread)

        allowed_message_refs = {
            message_ref for thread in finalized_threads for message_ref in thread.get("messages", [])
        }
        filtered_messages: list[dict[str, Any]] = []
        for message in message_items:
            message_ref = _wikilink(
                _message_uid(
                    name_with_owner,
                    _clean(message.get("message_type", "")),
                    _clean(message.get("github_message_id", "")),
                )
            )
            if message_ref not in allowed_message_refs:
                continue
            filtered_messages.append(message)
        return finalized_threads, filtered_messages

    def _fetch_repo_bundle(
        self,
        repo_row: dict[str, Any],
        *,
        vault_path: str,
        max_commits_per_repo: int | None,
        max_threads_per_repo: int | None,
        max_messages_per_thread: int | None,
    ) -> dict[str, Any]:
        full_name = _clean(repo_row.get("full_name", ""))
        owner, repo = full_name.split("/", 1)
        identity_cache = IdentityCache(vault_path)
        repo_detail = self._repo_detail(owner, repo)
        repo_meta, commits = self._fetch_commits(
            owner=owner,
            repo=repo,
            max_commits=max_commits_per_repo,
            default_branch=_clean(repo_detail.get("default_branch", "")),
        )
        issues = self._repo_issues(owner, repo)
        pulls = self._repo_pulls(owner, repo)
        selected_issues, selected_pulls = self._select_discussion_threads(
            issues=issues,
            pulls=pulls,
            max_threads=max_threads_per_repo,
        )
        selected_issue_numbers = [
            str(item.get("number", "")).strip() for item in selected_issues if str(item.get("number", "")).strip()
        ]
        selected_pull_numbers = [
            str(item.get("number", "")).strip() for item in selected_pulls if str(item.get("number", "")).strip()
        ]
        issue_comment_numbers = selected_issue_numbers + [
            number for number in selected_pull_numbers if number not in selected_issue_numbers
        ]
        issue_comments = self._repo_issue_comments(owner, repo, issue_numbers=issue_comment_numbers)
        review_comments = self._repo_review_comments(owner, repo, pull_numbers=selected_pull_numbers)
        reviews_by_pull: dict[str, list[dict[str, Any]]] = {}
        review_pulls = selected_pulls if max_threads_per_repo is not None else pulls
        for pull in review_pulls:
            pull_number = str(pull.get("number", "")).strip()
            if not pull_number:
                continue
            reviews_by_pull[pull_number] = self._fetch_reviews(owner=owner, repo=repo, pull_number=pull_number)

        name_with_owner = _clean(repo_detail.get("full_name") or repo_meta.get("nameWithOwner") or full_name)
        repo_item = self._build_repo_item(repo_detail, repo_meta, identity_cache)
        commit_items = [self._build_commit_item(name_with_owner, commit, identity_cache) for commit in commits]
        thread_items, message_items = self._build_discussion_items(
            name_with_owner=name_with_owner,
            issues=selected_issues if max_threads_per_repo is not None else issues,
            pulls=selected_pulls if max_threads_per_repo is not None else pulls,
            issue_comments=issue_comments,
            review_comments=review_comments,
            reviews_by_pull=reviews_by_pull,
            identity_cache=identity_cache,
            max_threads=max_threads_per_repo,
            max_messages_per_thread=max_messages_per_thread,
        )
        return {
            "repo": repo_item,
            "commits": commit_items,
            "threads": thread_items,
            "messages": message_items,
        }

    def stage_history(
        self,
        vault_path: str,
        stage_dir: str | Path,
        *,
        max_repos: int | None = None,
        max_commits_per_repo: int | None = None,
        max_threads_per_repo: int | None = None,
        max_messages_per_thread: int | None = None,
        workers: int | None = None,
        progress_every: int = 10,
        verbose: bool = False,
    ) -> dict[str, Any]:
        stage_path = Path(stage_dir).expanduser().resolve()
        stage_path.mkdir(parents=True, exist_ok=True)
        meta_dir = stage_path / "_meta"
        meta_dir.mkdir(exist_ok=True)
        state_path = meta_dir / "extract-state.json"
        manifest_path = stage_path / "manifest.json"
        stage_files = {
            "repos": stage_path / "repos.jsonl",
            "commits": stage_path / "commits.jsonl",
            "threads": stage_path / "threads.jsonl",
            "messages": stage_path / "messages.jsonl",
        }
        existing_state = {}
        if state_path.exists():
            try:
                existing_state = json.loads(state_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                existing_state = {}
        completed_repos = set(existing_state.get("completed_repos", [])) if isinstance(existing_state, dict) else set()
        failures: list[dict[str, str]] = []
        counts = Counter()
        started_at = time.perf_counter()
        repos = self._list_visible_repositories(max_repos=max_repos)
        pending_repos = [repo for repo in repos if _clean(repo.get("full_name", "")) not in completed_repos]
        worker_count = max(
            1,
            int(workers or os.environ.get("HFA_GITHUB_STAGE_WORKERS") or DEFAULT_STAGE_WORKERS),
        )

        handles = {name: path.open("a", encoding="utf-8") for name, path in stage_files.items()}

        def _write_records(name: str, records: list[dict[str, Any]]) -> None:
            handle = handles[name]
            for record in records:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
            handle.flush()

        def _write_state(complete: bool = False) -> None:
            payload = {
                "repo_count": len(repos),
                "completed_repos": sorted(completed_repos),
                "failures": failures,
                "counts": dict(counts),
                "complete": complete,
                "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
            state_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

        try:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = {
                    executor.submit(
                        self._fetch_repo_bundle,
                        repo_row,
                        vault_path=vault_path,
                        max_commits_per_repo=max_commits_per_repo,
                        max_threads_per_repo=max_threads_per_repo,
                        max_messages_per_thread=max_messages_per_thread,
                    ): _clean(repo_row.get("full_name", ""))
                    for repo_row in pending_repos
                }
                processed = 0
                for future in as_completed(futures):
                    full_name = futures[future]
                    try:
                        bundle = future.result()
                    except Exception as exc:
                        failures.append({"repo": full_name, "error": str(exc)})
                        _write_state(complete=False)
                        continue
                    _write_records("repos", [bundle["repo"]])
                    _write_records("commits", bundle["commits"])
                    _write_records("threads", bundle["threads"])
                    _write_records("messages", bundle["messages"])
                    counts["repos"] += 1
                    counts["commits"] += len(bundle["commits"])
                    counts["threads"] += len(bundle["threads"])
                    counts["messages"] += len(bundle["messages"])
                    completed_repos.add(full_name)
                    processed += 1
                    _write_state(complete=False)
                    if verbose and progress_every and processed % max(1, int(progress_every)) == 0:
                        elapsed = time.perf_counter() - started_at
                        print(
                            f"[github-history] processed={processed}/{len(pending_repos)} "
                            f"repos={counts['repos']} commits={counts['commits']} "
                            f"threads={counts['threads']} messages={counts['messages']} "
                            f"elapsed_s={elapsed:.1f}"
                        )
        finally:
            for handle in handles.values():
                handle.close()

        elapsed_seconds = round(time.perf_counter() - started_at, 3)
        _write_state(complete=True)
        manifest = {
            "repo_count": len(repos),
            "processed_repos": len(completed_repos),
            "counts": dict(counts),
            "failures": failures,
            "elapsed_seconds": elapsed_seconds,
            "stage_files": {name: str(path) for name, path in stage_files.items()},
        }
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        return manifest

    def _iter_staged_batches(
        self,
        stage_dir: str | Path,
        *,
        batch_size: int,
        max_items: int | None = None,
    ) -> Iterable[FetchedBatch]:
        stage_path = Path(stage_dir).expanduser().resolve()
        sequence = 0
        emitted = 0
        batch_items: list[dict[str, Any]] = []
        stage_files = [
            stage_path / "repos.jsonl",
            stage_path / "commits.jsonl",
            stage_path / "threads.jsonl",
            stage_path / "messages.jsonl",
        ]
        for path in stage_files:
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line:
                        continue
                    batch_items.append(json.loads(line))
                    emitted += 1
                    if max_items is not None and emitted > max(0, int(max_items)):
                        break
                    if len(batch_items) >= batch_size:
                        yield FetchedBatch(items=list(batch_items), sequence=sequence)
                        sequence += 1
                        batch_items = []
                if max_items is not None and emitted > max(0, int(max_items)):
                    break
        if batch_items:
            yield FetchedBatch(items=list(batch_items), sequence=sequence)

    def fetch_batches(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        **kwargs,
    ) -> Iterable[FetchedBatch]:
        stage_dir = kwargs.get("stage_dir")
        if not stage_dir:
            raise ValueError("github-history imports require --stage-dir")
        batch_size = max(
            1,
            int(kwargs.get("batch_size") or os.environ.get("HFA_GITHUB_IMPORT_BATCH_SIZE") or DEFAULT_BATCH_SIZE),
        )
        yield from self._iter_staged_batches(stage_dir, batch_size=batch_size, max_items=kwargs.get("max_items"))

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for batch in self.fetch_batches(vault_path, cursor, config=config, **kwargs):
            items.extend(batch.items)
        return items

    def to_card(self, item: dict[str, Any]):
        kind = _clean(item.get("kind", "")).lower()
        today = date.today().isoformat()
        if kind == "repo":
            card = GitRepositoryCard(
                uid=_repo_uid(_clean(item.get("name_with_owner", ""))),
                type="git_repository",
                source=list(item.get("source", [])) or [REPO_SOURCE],
                source_id=_clean(item.get("source_id", "")),
                created=_clean(item.get("created", "")) or today,
                updated=_clean(item.get("updated", "")) or today,
                summary=_clean(item.get("summary", "")),
                people=list(item.get("people", [])),
                orgs=list(item.get("orgs", [])),
                github_repo_id=_clean(item.get("github_repo_id", "")),
                github_node_id=_clean(item.get("github_node_id", "")),
                name_with_owner=_clean(item.get("name_with_owner", "")),
                owner_login=_clean(item.get("owner_login", "")),
                owner_type=_clean(item.get("owner_type", "")),
                html_url=_clean(item.get("html_url", "")),
                api_url=_clean(item.get("api_url", "")),
                ssh_url=_clean(item.get("ssh_url", "")),
                default_branch=_clean(item.get("default_branch", "")),
                homepage_url=_clean(item.get("homepage_url", "")),
                description=_clean(item.get("description", "")),
                visibility=_clean(item.get("visibility", "")),
                is_private=bool(item.get("is_private", False)),
                is_fork=bool(item.get("is_fork", False)),
                is_archived=bool(item.get("is_archived", False)),
                parent_name_with_owner=_clean(item.get("parent_name_with_owner", "")),
                primary_language=_clean(item.get("primary_language", "")),
                languages=list(item.get("languages", [])),
                topics=list(item.get("topics", [])),
                license_name=_clean(item.get("license_name", "")),
                created_at=_clean(item.get("created_at", "")),
                pushed_at=_clean(item.get("pushed_at", "")),
            )
            provenance = deterministic_provenance(card, REPO_SOURCE)
            return card, provenance, _clean_multiline(item.get("body", ""))

        if kind == "commit":
            card = GitCommitCard(
                uid=_commit_uid(
                    _clean(item.get("repository_name_with_owner", "")),
                    _clean(item.get("commit_sha", "")),
                ),
                type="git_commit",
                source=list(item.get("source", [])) or [COMMIT_SOURCE],
                source_id=_clean(item.get("source_id", "")),
                created=_clean(item.get("created", "")) or today,
                updated=_clean(item.get("updated", "")) or today,
                summary=_clean(item.get("summary", "")),
                people=list(item.get("people", [])),
                orgs=list(item.get("orgs", [])),
                github_node_id=_clean(item.get("github_node_id", "")),
                commit_sha=_clean(item.get("commit_sha", "")),
                repository_name_with_owner=_clean(item.get("repository_name_with_owner", "")),
                repository=_clean(item.get("repository", "")),
                parent_shas=list(item.get("parent_shas", [])),
                html_url=_clean(item.get("html_url", "")),
                api_url=_clean(item.get("api_url", "")),
                authored_at=_clean(item.get("authored_at", "")),
                committed_at=_clean(item.get("committed_at", "")),
                message_headline=_clean(item.get("message_headline", "")),
                additions=int(item.get("additions", 0) or 0),
                deletions=int(item.get("deletions", 0) or 0),
                changed_files=int(item.get("changed_files", 0) or 0),
                author_login=_clean(item.get("author_login", "")),
                author_name=_clean(item.get("author_name", "")),
                author_email=_clean(item.get("author_email", "")),
                committer_login=_clean(item.get("committer_login", "")),
                committer_name=_clean(item.get("committer_name", "")),
                committer_email=_clean(item.get("committer_email", "")),
                associated_pr_numbers=list(item.get("associated_pr_numbers", [])),
                associated_pr_urls=list(item.get("associated_pr_urls", [])),
            )
            provenance = deterministic_provenance(card, COMMIT_SOURCE)
            return card, provenance, _clean_multiline(item.get("body", ""))

        if kind == "thread":
            thread_type = _clean(item.get("thread_type", "")).lower() or "thread"
            card = GitThreadCard(
                uid=_thread_uid(
                    _clean(item.get("repository_name_with_owner", "")),
                    thread_type,
                    _clean(item.get("number", "")),
                ),
                type="git_thread",
                source=list(item.get("source", [])) or [THREAD_SOURCE],
                source_id=_clean(item.get("source_id", "")),
                created=_clean(item.get("created", "")) or today,
                updated=_clean(item.get("updated", "")) or today,
                summary=_clean(item.get("summary", "")),
                people=list(item.get("people", [])),
                orgs=list(item.get("orgs", [])),
                github_thread_id=_clean(item.get("github_thread_id", "")),
                github_node_id=_clean(item.get("github_node_id", "")),
                repository_name_with_owner=_clean(item.get("repository_name_with_owner", "")),
                repository=_clean(item.get("repository", "")),
                thread_type=thread_type,
                number=_clean(item.get("number", "")),
                html_url=_clean(item.get("html_url", "")),
                api_url=_clean(item.get("api_url", "")),
                state=_clean(item.get("state", "")),
                is_draft=bool(item.get("is_draft", False)),
                merged_at=_clean(item.get("merged_at", "")),
                closed_at=_clean(item.get("closed_at", "")),
                title=_clean(item.get("title", "")),
                labels=list(item.get("labels", [])),
                assignees=list(item.get("assignees", [])),
                milestone=_clean(item.get("milestone", "")),
                base_ref=_clean(item.get("base_ref", "")),
                head_ref=_clean(item.get("head_ref", "")),
                participant_logins=list(item.get("participant_logins", [])),
                messages=list(item.get("messages", [])),
                first_message_at=_clean(item.get("first_message_at", "")),
                last_message_at=_clean(item.get("last_message_at", "")),
                message_count=int(item.get("message_count", 0) or 0),
            )
            provenance = deterministic_provenance(card, f"{THREAD_SOURCE}.{thread_type}")
            return card, provenance, _clean_multiline(item.get("body", ""))

        if kind == "message":
            message_type = _clean(item.get("message_type", "")).lower() or "message"
            card = GitMessageCard(
                uid=_message_uid(
                    _clean(item.get("repository_name_with_owner", "")),
                    message_type,
                    _clean(item.get("github_message_id", "")),
                ),
                type="git_message",
                source=list(item.get("source", [])) or [MESSAGE_SOURCE],
                source_id=_clean(item.get("source_id", "")),
                created=_clean(item.get("created", "")) or today,
                updated=_clean(item.get("updated", "")) or today,
                summary=_clean(item.get("summary", "")),
                people=list(item.get("people", [])),
                orgs=list(item.get("orgs", [])),
                github_message_id=_clean(item.get("github_message_id", "")),
                github_node_id=_clean(item.get("github_node_id", "")),
                repository_name_with_owner=_clean(item.get("repository_name_with_owner", "")),
                repository=_clean(item.get("repository", "")),
                thread=_clean(item.get("thread", "")),
                message_type=message_type,
                html_url=_clean(item.get("html_url", "")),
                api_url=_clean(item.get("api_url", "")),
                actor_login=_clean(item.get("actor_login", "")),
                actor_name=_clean(item.get("actor_name", "")),
                actor_email=_clean(item.get("actor_email", "")),
                sent_at=_clean(item.get("sent_at", "")),
                updated_at=_clean(item.get("updated_at", "")),
                review_state=_clean(item.get("review_state", "")),
                review_commit_sha=_clean(item.get("review_commit_sha", "")),
                in_reply_to_message_id=_clean(item.get("in_reply_to_message_id", "")),
                path=_clean(item.get("path", "")),
                position=_clean(item.get("position", "")),
                original_position=_clean(item.get("original_position", "")),
                original_commit_sha=_clean(item.get("original_commit_sha", "")),
                diff_hunk=_clean_multiline(item.get("diff_hunk", "")),
            )
            provenance = deterministic_provenance(card, f"{MESSAGE_SOURCE}.{message_type}")
            return card, provenance, _clean_multiline(item.get("body", ""))

        raise ValueError(f"Unsupported GitHub history record kind: {kind}")

    def merge_card(self, vault_path, rel_path, card, body, provenance) -> None:
        self._replace_generic_card(vault_path, rel_path, card, body, provenance)

    def after_card_write(
        self,
        vault_path: str | Path,
        card,
        rel_path: Path,
        *,
        raw_item: dict[str, Any],
        action: str,
        **kwargs,
    ) -> None:
        source = card.source[0] if getattr(card, "source", None) else self.source_id
        candidate_pairs: list[tuple[str, str]] = []
        if isinstance(card, GitRepositoryCard):
            candidate_pairs.append((card.owner_login, ""))
        elif isinstance(card, GitCommitCard):
            candidate_pairs.append((card.author_login, card.author_email))
            candidate_pairs.append((card.committer_login, card.committer_email))
        elif isinstance(card, GitMessageCard):
            candidate_pairs.append((card.actor_login, card.actor_email))
        elif isinstance(card, GitThreadCard):
            for login in card.participant_logins:
                candidate_pairs.append((login, ""))
        seen: set[tuple[str, str]] = set()
        for login, email in candidate_pairs:
            key = (_normalize_login(login), _normalize_email(email))
            if not key[0] or key in seen:
                continue
            seen.add(key)
            self._enhance_person_with_github(vault_path, login=login, email=email, source=source)
