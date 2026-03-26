"""Otter meeting transcript archive adapter using the direct Otter API."""

from __future__ import annotations

import json
import os
import random
import re
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterator

from hfa.identity import IdentityCache
from hfa.provenance import ProvenanceEntry
from hfa.schema import CalendarEventCard, MeetingTranscriptCard
from hfa.thread_hash import compute_meeting_transcript_body_sha_from_payload
from hfa.uid import generate_uid
from hfa.vault import find_note_by_slug, read_note, write_card

from .base import BaseAdapter, FetchedBatch, deterministic_provenance

MEETING_SOURCE = "otter.meeting"
TITLE_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _meeting_uid(meeting_id: str) -> str:
    return generate_uid("meeting-transcript", MEETING_SOURCE, meeting_id)


def _wikilink_from_uid(uid: str) -> str:
    return f"[[{uid}]]"


def _clean(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def _first_nonempty(*values: Any) -> str:
    for value in values:
        cleaned = _clean(value)
        if cleaned:
            return cleaned
    return ""


def _coerce_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _normalize_iso_datetime(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        parsed = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return parsed.isoformat().replace("+00:00", "Z")
    cleaned = str(value).strip()
    if not cleaned:
        return ""
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        for fmt in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
            try:
                parsed = datetime.strptime(cleaned, fmt)
                if fmt == "%Y/%m/%d":
                    return parsed.date().isoformat()
                parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.isoformat().replace("+00:00", "Z")
            except ValueError:
                continue
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat().replace("+00:00", "Z")


def _format_otter_search_date(value: str | None) -> str:
    normalized = _normalize_iso_datetime(value)
    if len(normalized) >= 10:
        return normalized[:10].replace("-", "/")
    cleaned = _clean(value or "")
    if len(cleaned) >= 10 and cleaned[4] == "-" and cleaned[7] == "-":
        return cleaned[:10].replace("-", "/")
    return ""


def _parse_duration_seconds(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    cleaned = _clean(value)
    if not cleaned:
        return 0
    total = 0
    number = ""
    for char in cleaned:
        if char.isdigit():
            number += char
            continue
        if not number:
            continue
        amount = int(number)
        if char == "h":
            total += amount * 3600
        elif char == "m":
            total += amount * 60
        elif char == "s":
            total += amount
        number = ""
    if total:
        return total
    try:
        return int(cleaned)
    except ValueError:
        return 0


def _normalize_conference_url(value: str) -> str:
    cleaned = _clean(value)
    if not cleaned:
        return ""
    parsed = urllib.parse.urlparse(cleaned)
    if not parsed.scheme or not parsed.netloc:
        return cleaned
    path = parsed.path.rstrip("/")
    return urllib.parse.urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", "", ""))


def _is_otter_url(value: str) -> bool:
    normalized = _normalize_conference_url(value)
    if not normalized:
        return False
    parsed = urllib.parse.urlparse(normalized)
    host = parsed.netloc.lower()
    return host.endswith("otter.ai")


def _normalize_title_key(value: str) -> str:
    cleaned = _clean(value).lower()
    if not cleaned:
        return ""
    return " ".join(TITLE_TOKEN_RE.findall(cleaned))


def _title_tokens(value: str) -> set[str]:
    normalized = _normalize_title_key(value)
    if not normalized:
        return set()
    return {token for token in normalized.split(" ") if token}


def _parse_utc_datetime(value: str) -> datetime | None:
    normalized = _normalize_iso_datetime(value)
    if len(normalized) < 19:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized).astimezone(timezone.utc)
    except ValueError:
        return None


def _time_distance_seconds(left: str, right: str) -> int | None:
    left_dt = _parse_utc_datetime(left)
    right_dt = _parse_utc_datetime(right)
    if left_dt is None or right_dt is None:
        return None
    return int(abs((left_dt - right_dt).total_seconds()))


def _title_similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    return SequenceMatcher(None, left, right).ratio()


def _token_overlap_score(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    overlap = len(left & right)
    if overlap == 0:
        return 0.0
    return overlap / max(len(left), len(right))


def _body_date(value: str) -> str:
    return value[:10] if len(value) >= 10 else date.today().isoformat()


def _slug_from_ref(value: str) -> str:
    cleaned = _clean(value)
    if cleaned.startswith("[[") and cleaned.endswith("]]"):
        return cleaned[2:-2].split("|", 1)[0].strip()
    return cleaned


def _iter_markdown_rel_paths_under(vault_path: str | Path, top_level_dir: str) -> Iterator[Path]:
    root = Path(vault_path).expanduser().resolve() / top_level_dir
    if not root.exists():
        return
    for path in sorted(root.rglob("*.md")):
        if path.name.startswith("."):
            continue
        yield path.relative_to(Path(vault_path).expanduser().resolve())


def _coerce_cli_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _parse_json_from_stdout(stdout: str) -> dict[str, Any]:
    text = stdout.strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            content = payload.get("content")
            if isinstance(content, list):
                for item in content:
                    if not isinstance(item, dict):
                        continue
                    raw_text = item.get("text")
                    if isinstance(raw_text, str) and raw_text.strip():
                        try:
                            nested = json.loads(raw_text)
                            return nested if isinstance(nested, dict) else {"data": nested}
                        except json.JSONDecodeError:
                            continue
        return payload if isinstance(payload, dict) else {"data": payload}
    except json.JSONDecodeError:
        pass
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not line or line[0] not in "{[":
            continue
        try:
            payload = json.loads(line)
            return payload if isinstance(payload, dict) else {"data": payload}
        except json.JSONDecodeError:
            continue
    raise RuntimeError("mcporter output did not contain JSON")


class OtterApiClient:
    """Otter API client supporting API key or OAuth modes."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        api_base_url: str | None = None,
        token_url: str | None = None,
        timeout_seconds: int = 30,
        max_retries: int = 3,
    ) -> None:
        import os

        self.api_key = _clean(api_key or os.environ.get("OTTER_AI_API_KEY", ""))
        self.client_id = _clean(client_id or os.environ.get("OTTER_API_CLIENT_ID", ""))
        self.client_secret = _clean(client_secret or os.environ.get("OTTER_API_CLIENT_SECRET", ""))
        default_base_url = "https://otter.ai/forward/api/v1" if self.api_key else "https://api.otter.ai/v1"
        self.api_base_url = _clean(api_base_url or os.environ.get("OTTER_API_BASE_URL", default_base_url)).rstrip("/")
        self.token_url = _clean(token_url or os.environ.get("OTTER_API_TOKEN_URL", "https://api.otter.ai/oauth/token"))
        self.timeout_seconds = int(timeout_seconds)
        self.max_retries = max(0, int(max_retries))
        self._access_token = ""
        self._access_token_expires_at = 0.0
        self._token_lock = threading.Lock()

    def _require_credentials(self) -> None:
        if self.api_key:
            return
        if not self.client_id or not self.client_secret:
            raise RuntimeError(
                "Otter API credentials missing. Set OTTER_AI_API_KEY or OTTER_API_CLIENT_ID and OTTER_API_CLIENT_SECRET."
            )

    def _auth_mode(self) -> str:
        return "api_key" if self.api_key else "oauth"

    def _fetch_access_token(self, *, force_refresh: bool = False) -> str:
        self._require_credentials()
        if self.api_key:
            return self.api_key
        now = time.time()
        if not force_refresh and self._access_token and now < self._access_token_expires_at - 30:
            return self._access_token
        with self._token_lock:
            now = time.time()
            if not force_refresh and self._access_token and now < self._access_token_expires_at - 30:
                return self._access_token
            payload = urllib.parse.urlencode(
                {
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                }
            ).encode("utf-8")
            req = urllib.request.Request(self.token_url, data=payload, method="POST")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as response:
                body = json.loads(response.read().decode("utf-8"))
            token = _first_nonempty(body.get("access_token"))
            if not token:
                raise RuntimeError("Otter token response missing access_token")
            expires_in = int(body.get("expires_in", 3600) or 3600)
            self._access_token = token
            self._access_token_expires_at = time.time() + max(60, expires_in)
            return self._access_token

    def _request_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        allow_force_refresh: bool = True,
    ) -> dict[str, Any]:
        query = urllib.parse.urlencode({key: value for key, value in (params or {}).items() if value not in (None, "")})
        url = f"{self.api_base_url}{path}"
        if query:
            url = f"{url}?{query}"
        forced_refresh = not allow_force_refresh
        for attempt in range(self.max_retries + 1):
            token = self._fetch_access_token(force_refresh=forced_refresh)
            req = urllib.request.Request(url)
            req.add_header("Authorization", f"Bearer {token}")
            req.add_header("Accept", "application/json")
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_seconds) as response:
                    return json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                if exc.code == 401 and allow_force_refresh and not forced_refresh:
                    forced_refresh = True
                    continue
                if exc.code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    delay = self._retry_delay(attempt, retry_after)
                    time.sleep(delay)
                    continue
                message = exc.read().decode("utf-8") or str(exc)
                raise RuntimeError(f"Otter API request failed ({exc.code}): {message}") from exc
            except urllib.error.URLError as exc:
                if attempt < self.max_retries:
                    time.sleep(self._retry_delay(attempt))
                    continue
                raise RuntimeError(f"Otter API request failed: {exc}") from exc
        raise RuntimeError("Otter API request failed after retries")

    def _retry_delay(self, attempt: int, retry_after: str | None = None) -> float:
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                pass
        base_delay = min(2**attempt, 8)
        return base_delay + random.uniform(0.05, 0.25)

    def list_meetings(
        self,
        *,
        page_size: int = 25,
        page_token: str | None = None,
        updated_after: str | None = None,
        start_after: str | None = None,
        end_before: str | None = None,
    ) -> dict[str, Any]:
        if self._auth_mode() == "api_key":
            params = {
                "page_size": page_size,
                "created_after": _format_otter_search_date(updated_after or start_after),
                "created_before": _format_otter_search_date(end_before),
                "include_shared_meetings": "true",
            }
            try:
                return self._request_json(
                    "/speeches",
                    params=params,
                    allow_force_refresh=False,
                )
            except RuntimeError as exc:
                if "(404)" not in str(exc):
                    raise
                return self._request_json(
                    "/speeches/search",
                    params=params,
                    allow_force_refresh=False,
                )
        return self._request_json(
            "/meetings",
            params={
                "page_size": page_size,
                "page_token": page_token,
                "updated_after": updated_after,
                "start_after": start_after,
                "end_before": end_before,
            },
        )

    def get_meeting_detail(self, meeting_id: str) -> dict[str, Any]:
        if self._auth_mode() == "api_key":
            return self._request_json(f"/speeches/{urllib.parse.quote(meeting_id)}", allow_force_refresh=False)
        return self._request_json(f"/meetings/{urllib.parse.quote(meeting_id)}")

    def get_transcript(self, meeting_id: str) -> dict[str, Any]:
        if self._auth_mode() == "api_key":
            return self.get_meeting_detail(meeting_id)
        return self._request_json(f"/meetings/{urllib.parse.quote(meeting_id)}/transcript")


class OtterMcpClient:
    """Otter MCP client over mcporter."""

    def __init__(
        self,
        *,
        mcporter_bin: str | None = None,
        server_name: str | None = None,
        timeout_seconds: int = 120,
        list_tool: str | None = None,
        detail_tool: str | None = None,
        transcript_tool: str | None = None,
        meeting_id_arg: str | None = None,
        transcript_id_arg: str | None = None,
    ) -> None:
        self.mcporter_bin = _clean(
            mcporter_bin or os.environ.get("MCPORTER_CMD") or os.environ.get("MCPORTER_BIN") or "mcporter"
        )
        self.server_name = _clean(server_name or os.environ.get("OTTER_MCP_SERVER", "otter_meeting_mcp"))
        self.timeout_seconds = int(timeout_seconds)
        self.list_tool = _clean(list_tool or os.environ.get("OTTER_MCP_LIST_TOOL", ""))
        self.detail_tool = _clean(detail_tool or os.environ.get("OTTER_MCP_DETAIL_TOOL", ""))
        self.transcript_tool = _clean(transcript_tool or os.environ.get("OTTER_MCP_TRANSCRIPT_TOOL", ""))
        self.meeting_id_arg = _clean(meeting_id_arg or os.environ.get("OTTER_MCP_MEETING_ID_ARG", ""))
        self.transcript_id_arg = _clean(transcript_id_arg or os.environ.get("OTTER_MCP_TRANSCRIPT_ID_ARG", ""))
        self._discovered_tools: dict[str, str] | None = None
        self._user_info: dict[str, Any] | None = None
        self._tool_lock = threading.Lock()

    def _ensure_mcporter(self) -> str:
        resolved = shutil.which(self.mcporter_bin)
        if resolved:
            return resolved
        if os.path.exists(self.mcporter_bin):
            return self.mcporter_bin
        raise RuntimeError(
            f"mcporter not available. Set MCPORTER_CMD or install mcporter before using {self.server_name}."
        )

    def _run_mcporter(self, *args: str) -> tuple[str, str]:
        cmd = [self._ensure_mcporter(), *args]
        env = {**os.environ, "NPM_CONFIG_CACHE": os.environ.get("NPM_CONFIG_CACHE", "/tmp/npm-cache")}
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=self.timeout_seconds,
            env=env,
        )
        if proc.returncode != 0:
            error = proc.stderr.strip() or proc.stdout.strip() or f"mcporter failed with exit code {proc.returncode}"
            raise RuntimeError(error)
        return proc.stdout, proc.stderr

    def _discover_tools(self) -> dict[str, str]:
        if self._discovered_tools is not None:
            return self._discovered_tools
        with self._tool_lock:
            if self._discovered_tools is not None:
                return self._discovered_tools
            stdout, _ = self._run_mcporter("list", self.server_name)
            candidates: list[str] = []
            for raw_line in stdout.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                if line.startswith("function "):
                    signature = line[len("function ") :]
                    tool_name = signature.split("(", 1)[0].strip()
                    if tool_name:
                        candidates.append(tool_name)
                    continue
                token = line.split()[0].strip(" -*:")
                if token and all(ch.isalnum() or ch in {"_", "-", "."} for ch in token):
                    candidates.append(token)

            def _pick(*keywords: str) -> str:
                lowered = [(tool, tool.lower()) for tool in candidates]
                for tool, lowered_name in lowered:
                    if all(keyword in lowered_name for keyword in keywords):
                        return tool
                return ""

            discovered = {
                "list": self.list_tool
                or _pick("search")
                or _pick("list", "meeting")
                or _pick("list", "speech")
                or _pick("recent", "meeting"),
                "detail": self.detail_tool
                or _pick("fetch")
                or _pick("get", "meeting")
                or _pick("get", "speech")
                or _pick("meeting"),
                "transcript": self.transcript_tool
                or _pick("transcript")
                or _pick("fetch")
                or _pick("get", "transcript"),
            }
            if not discovered["transcript"]:
                discovered["transcript"] = discovered["detail"]
            self._discovered_tools = discovered
            return discovered

    def _call_tool(self, tool_name: str, **kwargs: Any) -> dict[str, Any]:
        args = [f"{key}={_coerce_cli_value(value)}" for key, value in kwargs.items() if value not in (None, [])]
        stdout, _ = self._run_mcporter("call", f"{self.server_name}.{tool_name}", *args)
        return _parse_json_from_stdout(stdout)

    def _get_user_info(self) -> dict[str, Any]:
        if self._user_info is not None:
            return self._user_info
        try:
            info = self._call_tool("get_user_info")
            self._user_info = info if isinstance(info, dict) else {}
            return self._user_info
        except RuntimeError:
            stdout, _ = self._run_mcporter("call", f"{self.server_name}.get_user_info")
            parsed: dict[str, Any] = {}
            for raw_line in stdout.splitlines():
                line = raw_line.strip()
                if line.startswith("Name:"):
                    parsed["name"] = _clean(line.split(":", 1)[1])
                elif line.startswith("Email:"):
                    parsed["email"] = _clean(line.split(":", 1)[1])
                elif line.startswith("Current DateTime:"):
                    parsed["current_datetime"] = _clean(line.split(":", 1)[1])
            self._user_info = parsed
            return self._user_info
        return self._user_info

    def _call_with_id_fallbacks(self, tool_name: str, meeting_id: str, explicit_arg: str = "") -> dict[str, Any]:
        attempted: list[str] = []
        arg_names = [
            name for name in [explicit_arg, "meetingId", "meeting_id", "otid", "id", "speechId", "speech_id"] if name
        ]
        last_error: Exception | None = None
        for arg_name in dict.fromkeys(arg_names):
            try:
                return self._call_tool(tool_name, **{arg_name: meeting_id})
            except Exception as exc:  # noqa: BLE001
                attempted.append(arg_name)
                last_error = exc
                continue
        if last_error is not None:
            raise RuntimeError(f"{tool_name} failed for args {attempted}: {last_error}") from last_error
        raise RuntimeError(f"No valid id arg names available for {tool_name}")

    def list_meetings(
        self,
        *,
        page_size: int = 25,
        page_token: str | None = None,
        updated_after: str | None = None,
        start_after: str | None = None,
        end_before: str | None = None,
    ) -> dict[str, Any]:
        tools = self._discover_tools()
        if not tools.get("list"):
            raise RuntimeError(f"Could not discover a list tool for MCP server {self.server_name}")
        user_info = self._get_user_info()
        params: dict[str, Any] = {
            "query": "",
            "created_after": _format_otter_search_date(updated_after or start_after) or "",
            "created_before": _format_otter_search_date(end_before) or date.today().strftime("%Y/%m/%d"),
            "include_shared_meetings": "true",
        }
        username = _first_nonempty(user_info.get("name"), user_info.get("username"))
        if username:
            params["username"] = username
        params["query"] = ""
        page_size_arg = _clean(os.environ.get("OTTER_MCP_PAGE_SIZE_ARG", ""))
        page_token_arg = _clean(os.environ.get("OTTER_MCP_PAGE_TOKEN_ARG", ""))
        updated_after_arg = _clean(os.environ.get("OTTER_MCP_UPDATED_AFTER_ARG", "created_after"))
        start_after_arg = _clean(os.environ.get("OTTER_MCP_START_AFTER_ARG", "created_after"))
        end_before_arg = _clean(os.environ.get("OTTER_MCP_END_BEFORE_ARG", "created_before"))
        if page_size_arg:
            params[page_size_arg] = page_size
        if page_token_arg and page_token:
            params[page_token_arg] = page_token
        if updated_after_arg and updated_after:
            params[updated_after_arg] = _format_otter_search_date(updated_after)
        if start_after_arg and start_after:
            params[start_after_arg] = _format_otter_search_date(start_after)
        if end_before_arg:
            params[end_before_arg] = _format_otter_search_date(end_before) or params.get(end_before_arg, "")
        return self._call_tool(tools["list"], **params)

    def get_meeting_detail(self, meeting_id: str) -> dict[str, Any]:
        tools = self._discover_tools()
        if not tools.get("detail"):
            raise RuntimeError(f"Could not discover a detail tool for MCP server {self.server_name}")
        return self._call_with_id_fallbacks(tools["detail"], meeting_id, explicit_arg=self.meeting_id_arg)

    def get_transcript(self, meeting_id: str) -> dict[str, Any]:
        tools = self._discover_tools()
        if not tools.get("transcript"):
            raise RuntimeError(f"Could not discover a transcript tool for MCP server {self.server_name}")
        return self._call_with_id_fallbacks(tools["transcript"], meeting_id, explicit_arg=self.transcript_id_arg)


class OtterTranscriptsAdapter(BaseAdapter):
    source_id = "otter-transcripts"
    preload_existing_uid_index = True

    def _build_client(self) -> OtterApiClient | OtterMcpClient:
        fetch_mode = _clean(os.environ.get("OTTER_FETCH_MODE", "auto")).lower()
        if fetch_mode == "mcp":
            return OtterMcpClient()
        if fetch_mode == "api":
            return OtterApiClient()
        mcporter_hint = os.environ.get("MCPORTER_CMD") or os.environ.get("MCPORTER_BIN")
        if mcporter_hint or shutil.which("mcporter"):
            return OtterMcpClient()
        if os.environ.get("OTTER_AI_API_KEY") or (
            os.environ.get("OTTER_API_CLIENT_ID") and os.environ.get("OTTER_API_CLIENT_SECRET")
        ):
            return OtterApiClient()
        return OtterMcpClient()

    def get_cursor_key(self, **kwargs) -> str:
        account_email = _clean(kwargs.get("account_email", "")).lower()
        return f"{self.source_id}:{account_email}" if account_email else self.source_id

    def _list_rows(self, response: dict[str, Any]) -> list[dict[str, Any]]:
        payload = response.get("data")
        if isinstance(payload, dict):
            response = payload
        for key in ("meetings", "items", "conversations", "data", "speeches", "results"):
            candidate = response.get(key)
            if isinstance(candidate, list):
                return [row for row in candidate if isinstance(row, dict)]
        return []

    def _next_page_token(self, response: dict[str, Any]) -> str | None:
        payload = response.get("data")
        if isinstance(payload, dict):
            response = payload
        for key in ("nextPageToken", "next_page_token", "nextCursor", "next_cursor", "cursor"):
            value = _first_nonempty(response.get(key))
            if value:
                return value
        return None

    def _resolve_people(self, cache: IdentityCache, emails: list[str]) -> list[str]:
        links: list[str] = []
        for email_value in emails:
            resolved = cache.resolve("email", email_value)
            if resolved and resolved not in links:
                links.append(resolved)
        return links

    def _load_existing_transcript_state(
        self,
        vault_path: str,
        *,
        account_email: str,
    ) -> dict[str, dict[str, str]]:
        existing: dict[str, dict[str, str]] = {}
        normalized_account = _clean(account_email).lower()
        for rel_path in _iter_markdown_rel_paths_under(vault_path, "MeetingTranscripts"):
            frontmatter, _, _ = read_note(vault_path, str(rel_path))
            if _clean(frontmatter.get("type", "")) != "meeting_transcript":
                continue
            if normalized_account and _clean(frontmatter.get("account_email", "")).lower() != normalized_account:
                continue
            meeting_id = _clean(frontmatter.get("otter_meeting_id", ""))
            if not meeting_id:
                continue
            existing[meeting_id] = {
                "otter_updated_at": _clean(frontmatter.get("otter_updated_at", "")),
                "transcript_body_sha": _clean(frontmatter.get("transcript_body_sha", "")),
            }
        return existing

    def _calendar_lookup(
        self,
        vault_path: str,
    ) -> tuple[
        dict[str, list[dict[str, Any]]],
        dict[str, list[dict[str, Any]]],
        dict[str, list[dict[str, Any]]],
        dict[str, list[dict[str, Any]]],
    ]:
        by_event_id: dict[str, list[dict[str, Any]]] = {}
        by_ical_uid: dict[str, list[dict[str, Any]]] = {}
        by_conference: dict[str, list[dict[str, Any]]] = {}
        by_start_date: dict[str, list[dict[str, Any]]] = {}
        for rel_path in _iter_markdown_rel_paths_under(vault_path, "Calendar"):
            frontmatter, _, _ = read_note(vault_path, str(rel_path))
            if _clean(frontmatter.get("type", "")) != "calendar_event":
                continue
            wikilink = f"[[{Path(rel_path).stem}]]"
            event_id = _clean(frontmatter.get("event_id", ""))
            ical_uid = _clean(frontmatter.get("ical_uid", ""))
            conference_url = _normalize_conference_url(_clean(frontmatter.get("conference_url", "")))
            title = _first_nonempty(frontmatter.get("title"), frontmatter.get("summary"))
            start_at = _clean(frontmatter.get("start_at", ""))
            organizer_email = _clean(frontmatter.get("organizer_email", "")).lower()
            attendee_emails = {
                _clean(email).lower() for email in _coerce_list(frontmatter.get("attendee_emails")) if _clean(email)
            }
            candidate = {
                "wikilink": wikilink,
                "rel_path": str(rel_path),
                "account_email": _clean(frontmatter.get("account_email", "")).lower(),
                "event_id": event_id,
                "ical_uid": ical_uid,
                "start_at": start_at,
                "title_key": _normalize_title_key(title),
                "title_tokens": _title_tokens(title),
                "organizer_email": organizer_email,
                "attendee_emails": attendee_emails,
            }
            if event_id:
                by_event_id.setdefault(event_id, []).append(candidate)
            if ical_uid:
                by_ical_uid.setdefault(ical_uid, []).append(candidate)
            if conference_url:
                by_conference.setdefault(conference_url, []).append(candidate)
            start_date = _normalize_iso_datetime(start_at)[:10]
            if start_date:
                by_start_date.setdefault(start_date, []).append(candidate)
        return by_event_id, by_ical_uid, by_conference, by_start_date

    def _dedupe_candidate_links(self, candidates: list[dict[str, Any]]) -> list[str]:
        links: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            wikilink = _clean(candidate.get("wikilink", ""))
            if not wikilink or wikilink in seen:
                continue
            seen.add(wikilink)
            links.append(wikilink)
        return links

    def _select_exact_calendar_candidates(
        self,
        candidates: list[dict[str, Any]],
        *,
        title: str,
        start_at: str,
    ) -> list[str]:
        if not candidates:
            return []
        selected = list(candidates)
        meeting_start = _normalize_iso_datetime(start_at)
        if meeting_start:
            same_day = [
                candidate
                for candidate in selected
                if not _clean(candidate.get("start_at", ""))
                or _normalize_iso_datetime(candidate.get("start_at", ""))[:10] == meeting_start[:10]
            ]
            if same_day:
                selected = same_day
        meeting_title_key = _normalize_title_key(title)
        meeting_title_tokens = _title_tokens(title)
        if meeting_title_key and len(selected) > 1:
            scored: list[tuple[float, dict[str, Any]]] = []
            for candidate in selected:
                candidate_title_key = _clean(candidate.get("title_key", ""))
                if not candidate_title_key:
                    continue
                candidate_title_tokens = set(candidate.get("title_tokens", set()) or set())
                title_score = max(
                    _title_similarity(meeting_title_key, candidate_title_key),
                    _token_overlap_score(meeting_title_tokens, candidate_title_tokens),
                )
                if title_score < 0.7:
                    continue
                scored.append((title_score, candidate))
            if scored:
                scored.sort(key=lambda item: (-item[0], _clean(item[1].get("wikilink", ""))))
                best_score = scored[0][0]
                selected = [candidate for score, candidate in scored if best_score - score <= 0.05]
        return self._dedupe_candidate_links(selected)

    def _stage_files(self, stage_dir: str | Path) -> dict[str, Path]:
        stage_path = Path(stage_dir).expanduser().resolve()
        meta_dir = stage_path / "_meta"
        stage_path.mkdir(parents=True, exist_ok=True)
        meta_dir.mkdir(exist_ok=True)
        return {
            "stage_path": stage_path,
            "meta_dir": meta_dir,
            "meetings": stage_path / "meetings.jsonl",
            "manifest": stage_path / "manifest.json",
            "state": meta_dir / "extract-state.json",
            "failures": stage_path / "hydration-failures.jsonl",
        }

    def _load_stage_state(self, state_path: Path) -> dict[str, Any]:
        if not state_path.exists():
            return {}
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _write_stage_state(
        self,
        state_path: Path,
        *,
        account_email: str,
        filters: dict[str, Any],
        page_token: str | None,
        next_window_date: str | None = None,
        emitted_meetings: int,
        skipped_unchanged_meetings: int,
        failed_hydrations: int,
        completed_meeting_ids: set[str],
        complete: bool,
    ) -> None:
        payload = {
            "account_email": _clean(account_email).lower(),
            "filters": filters,
            "page_token": page_token or "",
            "next_window_date": next_window_date or "",
            "emitted_meetings": emitted_meetings,
            "skipped_unchanged_meetings": skipped_unchanged_meetings,
            "failed_hydrations": failed_hydrations,
            "completed_meeting_ids": sorted(completed_meeting_ids),
            "complete": complete,
            "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        state_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _coerce_stage_window_date(self, value: str | None, *, fallback: date) -> date:
        cleaned = _clean(value or "")
        if cleaned:
            try:
                return date.fromisoformat(cleaned[:10])
            except ValueError:
                pass
        return fallback

    def _append_stage_row(self, path: Path, row: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    def _append_failure_row(self, path: Path, row: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    def _build_calendar_backlink_items(self, vault_path: str, transcript_item: dict[str, Any]) -> list[dict[str, Any]]:
        transcript_link = _wikilink_from_uid(_meeting_uid(_first_nonempty(transcript_item.get("otter_meeting_id"))))
        items: list[dict[str, Any]] = []
        for event_ref in list(transcript_item.get("calendar_events", []) or []):
            slug = _slug_from_ref(str(event_ref))
            if not slug:
                continue
            note_path = find_note_by_slug(vault_path, slug)
            if note_path is None:
                continue
            frontmatter, _, _ = read_note(vault_path, str(note_path.relative_to(vault_path)))
            if _clean(frontmatter.get("type", "")) != "calendar_event":
                continue
            meeting_transcripts = list(frontmatter.get("meeting_transcripts", []) or [])
            if transcript_link in meeting_transcripts:
                continue
            items.append(
                {
                    "kind": "calendar_backlink",
                    "frontmatter": dict(frontmatter),
                    "meeting_transcript_ref": transcript_link,
                }
            )
        return items

    def _expand_import_items(
        self,
        vault_path: str,
        rows: list[dict[str, Any]],
        *,
        event_lookup_by_id: dict[str, list[dict[str, Any]]] | None = None,
        event_lookup_by_ical_uid: dict[str, list[dict[str, Any]]] | None = None,
        event_lookup_by_conference: dict[str, list[dict[str, Any]]] | None = None,
        event_lookup_by_start_date: dict[str, list[dict[str, Any]]] | None = None,
    ) -> list[dict[str, Any]]:
        expanded: list[dict[str, Any]] = []
        for row in rows:
            transcript_row = dict(row)
            transcript_row.setdefault("kind", "meeting_transcript")
            if not list(transcript_row.get("calendar_events", []) or []):
                all_people_emails = [
                    _clean(email).lower()
                    for email in [
                        _first_nonempty(transcript_row.get("host_email")),
                        *_coerce_list(transcript_row.get("speaker_emails")),
                        *_coerce_list(transcript_row.get("participant_emails")),
                    ]
                    if _clean(email)
                ]
                matched_calendar_events = self._match_calendar_event(
                    title=_first_nonempty(transcript_row.get("title"), transcript_row.get("summary")),
                    event_id_hint=_first_nonempty(transcript_row.get("event_id_hint")),
                    ical_uid=_first_nonempty(transcript_row.get("ical_uid")),
                    conference_url=_first_nonempty(transcript_row.get("conference_url")),
                    start_at=_first_nonempty(transcript_row.get("start_at")),
                    participant_emails=all_people_emails,
                    by_event_id=event_lookup_by_id or {},
                    by_ical_uid=event_lookup_by_ical_uid or {},
                    by_conference=event_lookup_by_conference or {},
                    by_start_date=event_lookup_by_start_date or {},
                )
                if matched_calendar_events:
                    transcript_row["calendar_events"] = matched_calendar_events
            expanded.append(transcript_row)
            expanded.extend(self._build_calendar_backlink_items(vault_path, transcript_row))
        return expanded

    def _extract_people_from_payload(
        self, payloads: list[dict[str, Any]]
    ) -> tuple[list[str], list[str], list[str], list[str], str, str]:
        speaker_names: list[str] = []
        speaker_emails: list[str] = []
        participant_names: list[str] = []
        participant_emails: list[str] = []
        host_name = ""
        host_email = ""
        for payload in payloads:
            for field_name in ("speakers", "speaker_info", "speakerInfos"):
                for speaker in _coerce_list(payload.get(field_name)):
                    if isinstance(speaker, str):
                        cleaned = _clean(speaker)
                        if cleaned and cleaned not in speaker_names:
                            speaker_names.append(cleaned)
                        continue
                    if not isinstance(speaker, dict):
                        continue
                    name = _first_nonempty(
                        speaker.get("name"),
                        speaker.get("speaker_name"),
                        speaker.get("speakerName"),
                        speaker.get("display_name"),
                        speaker.get("displayName"),
                    )
                    email = _first_nonempty(
                        speaker.get("email"), speaker.get("speaker_email"), speaker.get("speakerEmail")
                    )
                    if name and name not in speaker_names:
                        speaker_names.append(name)
                    if email and email.lower() not in speaker_emails:
                        speaker_emails.append(email.lower())
            for field_name in ("participants", "attendees", "participant_info", "participantInfo"):
                for participant in _coerce_list(payload.get(field_name)):
                    if not isinstance(participant, dict):
                        continue
                    name = _first_nonempty(
                        participant.get("name"), participant.get("display_name"), participant.get("displayName")
                    )
                    email = _first_nonempty(participant.get("email"))
                    if name and name not in participant_names:
                        participant_names.append(name)
                    if email and email.lower() not in participant_emails:
                        participant_emails.append(email.lower())
            for raw_participant in _coerce_list(payload.get("calendar_participants")):
                cleaned = _clean(raw_participant)
                if not cleaned:
                    continue
                if ":" in cleaned:
                    name_part, email_part = cleaned.split(":", 1)
                    name = _clean(name_part)
                    email = _clean(email_part).lower()
                    if name and name not in participant_names:
                        participant_names.append(name)
                    if email and email not in participant_emails:
                        participant_emails.append(email)
            host = payload.get("host") or payload.get("owner") or {}
            if isinstance(host, dict):
                host_name = host_name or _first_nonempty(
                    host.get("name"), host.get("display_name"), host.get("displayName")
                )
                host_email = host_email or _first_nonempty(host.get("email"))
        return speaker_names, speaker_emails, participant_names, participant_emails, host_name, host_email.lower()

    def _extract_action_items(self, payloads: list[dict[str, Any]]) -> list[str]:
        items: list[str] = []
        for payload in payloads:
            for field_name in ("action_items", "actionItems", "takeaways"):
                for item in _coerce_list(payload.get(field_name)):
                    if isinstance(item, dict):
                        text = _first_nonempty(item.get("text"), item.get("title"), item.get("description"))
                    else:
                        text = _clean(item)
                    if text and text not in items:
                        items.append(text)
            metadata = payload.get("metadata")
            if isinstance(metadata, dict):
                for item in _coerce_list(metadata.get("action_items")):
                    text = _clean(item)
                    if text and text not in items:
                        items.append(text)
        return items

    def _extract_summary_text(self, payloads: list[dict[str, Any]]) -> str:
        for payload in payloads:
            metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
            summary = _first_nonempty(
                payload.get("summary"),
                payload.get("summary_text"),
                payload.get("summaryText"),
                payload.get("short_summary"),
                metadata.get("short_summary"),
            )
            if summary:
                return summary
        return ""

    def _transcript_lines(self, payloads: list[dict[str, Any]]) -> list[str]:
        lines: list[str] = []
        for payload in payloads:
            for field_name in ("segments", "utterances", "transcript"):
                value = payload.get(field_name)
                if isinstance(value, list):
                    for segment in value:
                        if not isinstance(segment, dict):
                            text = _clean(segment)
                            if text:
                                lines.append(text)
                            continue
                        speaker = _first_nonempty(
                            segment.get("speaker_name"),
                            segment.get("speakerName"),
                            segment.get("speaker"),
                            segment.get("speaker_label"),
                            segment.get("speakerLabel"),
                        )
                        timestamp = _normalize_iso_datetime(
                            segment.get("start_at")
                            or segment.get("startAt")
                            or segment.get("timestamp")
                            or segment.get("start_time")
                            or segment.get("startTime")
                        )
                        if not timestamp and isinstance(segment.get("start_time"), (int, float)):
                            timestamp = str(segment.get("start_time"))
                        text = _first_nonempty(segment.get("text"), segment.get("content"), segment.get("utterance"))
                        pieces = [piece for piece in [timestamp, speaker, text] if piece]
                        if pieces:
                            lines.append(" | ".join(pieces))
                elif isinstance(value, str):
                    text = _clean(value)
                    if text:
                        lines.append(text)
            text = _first_nonempty(payload.get("text"), payload.get("transcript_text"), payload.get("content"))
            if text and text not in lines:
                lines.append(text)
        return lines

    def _match_calendar_event(
        self,
        *,
        title: str,
        event_id_hint: str,
        ical_uid: str,
        conference_url: str,
        start_at: str,
        participant_emails: list[str],
        by_event_id: dict[str, list[dict[str, Any]]],
        by_ical_uid: dict[str, list[dict[str, Any]]],
        by_conference: dict[str, list[dict[str, Any]]],
        by_start_date: dict[str, list[dict[str, Any]]],
    ) -> list[str]:
        if event_id_hint and event_id_hint in by_event_id:
            direct_matches = self._select_exact_calendar_candidates(
                by_event_id[event_id_hint],
                title=title,
                start_at=start_at,
            )
            if direct_matches:
                return direct_matches
        if ical_uid and ical_uid in by_ical_uid:
            direct_matches = self._select_exact_calendar_candidates(
                by_ical_uid[ical_uid],
                title=title,
                start_at=start_at,
            )
            if direct_matches:
                return direct_matches
        normalized_conference = _normalize_conference_url(conference_url)
        meeting_start = _normalize_iso_datetime(start_at)
        if normalized_conference and not _is_otter_url(normalized_conference):
            candidates = by_conference.get(normalized_conference, [])
            if candidates:
                direct_matches = self._select_exact_calendar_candidates(
                    candidates,
                    title=title,
                    start_at=start_at,
                )
                if direct_matches:
                    return direct_matches

        meeting_title_key = _normalize_title_key(title)
        meeting_title_tokens = _title_tokens(title)
        if not meeting_start or not meeting_title_key:
            return []

        meeting_dt = _parse_utc_datetime(meeting_start)
        if meeting_dt is None:
            return []

        meeting_emails = {_clean(email).lower() for email in participant_emails if _clean(email)}
        candidate_pool: list[dict[str, Any]] = []
        for day_offset in (-1, 0, 1):
            day_key = (meeting_dt + timedelta(days=day_offset)).date().isoformat()
            candidate_pool.extend(by_start_date.get(day_key, []))

        if not candidate_pool:
            return []

        scored: list[tuple[float, int, str]] = []
        seen_links: set[str] = set()
        for candidate in candidate_pool:
            wikilink = _clean(candidate.get("wikilink", ""))
            if not wikilink or wikilink in seen_links:
                continue
            seen_links.add(wikilink)
            candidate_title_key = _clean(candidate.get("title_key", ""))
            candidate_title_tokens = set(candidate.get("title_tokens", set()) or set())
            if not candidate_title_key:
                continue
            title_ratio = _title_similarity(meeting_title_key, candidate_title_key)
            token_score = _token_overlap_score(meeting_title_tokens, candidate_title_tokens)
            title_score = max(title_ratio, token_score)
            if title_score < 0.55:
                continue

            delta_seconds = _time_distance_seconds(meeting_start, _clean(candidate.get("start_at", "")))
            if delta_seconds is None:
                delta_seconds = 99_999_999
            if delta_seconds > 8 * 3600:
                continue

            attendee_emails = set(candidate.get("attendee_emails", set()) or set())
            organizer_email = _clean(candidate.get("organizer_email", "")).lower()
            email_overlap = len(meeting_emails & attendee_emails)
            organizer_overlap = 1 if organizer_email and organizer_email in meeting_emails else 0

            # Prefer exact-ish title matches, then attendee overlap, then time proximity.
            score = (
                title_score * 1000.0
                + min(email_overlap, 3) * 50.0
                + organizer_overlap * 25.0
                - min(delta_seconds, 8 * 3600) / 60.0
            )

            if delta_seconds > 2 * 3600 and title_score < 0.85:
                continue
            if title_ratio < 0.7 and token_score < 0.7 and email_overlap == 0 and organizer_overlap == 0:
                continue
            scored.append((score, delta_seconds, wikilink))

        if not scored:
            return []

        scored.sort(key=lambda item: (-item[0], item[1], item[2]))
        best_score, best_delta, best_link = scored[0]
        if len(scored) == 1:
            return [best_link]
        winners = [
            link for score, delta, link in scored if best_score - score <= 10.0 and abs(delta - best_delta) <= 15 * 60
        ]
        if winners:
            return list(dict.fromkeys(winners))
        return [best_link]

    def _render_body(self, *, summary_text: str, action_items: list[str], transcript_lines: list[str]) -> str:
        sections: list[str] = []
        if summary_text:
            sections.append(f"## Summary\n\n{summary_text}")
        if action_items:
            sections.append("## Action Items\n\n" + "\n".join(f"- {item}" for item in action_items))
        if transcript_lines:
            sections.append("## Transcript\n\n" + "\n".join(transcript_lines))
        return "\n\n".join(section for section in sections if section).strip()

    def _hydrate_meeting(
        self,
        row: dict[str, Any],
        *,
        client: OtterApiClient,
        identity_cache: IdentityCache,
        account_email: str,
        event_lookup_by_id: dict[str, list[dict[str, Any]]],
        event_lookup_by_ical_uid: dict[str, list[dict[str, Any]]],
        event_lookup_by_conference: dict[str, list[dict[str, Any]]],
        event_lookup_by_start_date: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any] | None:
        meeting_id = _first_nonempty(row.get("id"), row.get("otid"), row.get("meeting_id"), row.get("meetingId"))
        if not meeting_id:
            return None
        detail = client.get_meeting_detail(meeting_id)
        transcript = client.get_transcript(meeting_id)
        detail_payload = detail.get("data") if isinstance(detail.get("data"), dict) else detail
        transcript_payload = transcript.get("data") if isinstance(transcript.get("data"), dict) else transcript
        title = _first_nonempty(
            detail_payload.get("title"),
            detail_payload.get("name"),
            row.get("title"),
            row.get("name"),
        )
        detail_metadata = detail_payload.get("metadata") if isinstance(detail_payload.get("metadata"), dict) else {}
        transcript_metadata = (
            transcript_payload.get("metadata") if isinstance(transcript_payload.get("metadata"), dict) else {}
        )
        status = _first_nonempty(detail_payload.get("status"), row.get("status"))
        start_at = _normalize_iso_datetime(
            detail_payload.get("start_at")
            or detail_payload.get("startAt")
            or detail_payload.get("started_at")
            or detail_payload.get("startedAt")
            or detail_metadata.get("start_time")
            or transcript_metadata.get("start_time")
            or row.get("start_time")
            or row.get("start_at")
            or row.get("startAt")
        )
        end_at = _normalize_iso_datetime(
            detail_payload.get("end_at")
            or detail_payload.get("endAt")
            or detail_payload.get("ended_at")
            or detail_payload.get("endedAt")
            or detail_metadata.get("end_time")
            or transcript_metadata.get("end_time")
            or row.get("end_at")
            or row.get("endAt")
        )
        otter_updated_at = _normalize_iso_datetime(
            detail_payload.get("updated_at")
            or detail_payload.get("updatedAt")
            or detail_payload.get("modified_at")
            or detail_payload.get("modifiedAt")
            or row.get("updated_at")
            or row.get("updatedAt")
        )
        language = _first_nonempty(detail_payload.get("language"), transcript_payload.get("language"))
        conference_url = _first_nonempty(
            detail_payload.get("conference_url"),
            detail_payload.get("conferenceUrl"),
            detail_payload.get("meeting_url"),
            detail_payload.get("meetingUrl"),
            detail_payload.get("share_url"),
            detail_payload.get("shareUrl"),
            detail_payload.get("url"),
            row.get("conference_url"),
            row.get("conferenceUrl"),
            row.get("url"),
        )
        event_id_hint = _first_nonempty(
            detail_payload.get("event_id"),
            detail_payload.get("eventId"),
            detail_payload.get("calendar_event_id"),
            detail_payload.get("calendarEventId"),
            detail_payload.get("event_id_hint"),
            detail_payload.get("eventIdHint"),
            transcript_payload.get("event_id"),
            transcript_payload.get("eventId"),
        )
        ical_uid = _first_nonempty(
            detail_payload.get("ical_uid"),
            detail_payload.get("iCalUID"),
            transcript_payload.get("ical_uid"),
            transcript_payload.get("iCalUID"),
        )
        speaker_names, speaker_emails, participant_names, participant_emails, host_name, host_email = (
            self._extract_people_from_payload([row, detail_payload, transcript_payload])
        )
        summary_text = self._extract_summary_text([detail_payload, transcript_payload])
        action_items = self._extract_action_items([detail_payload, transcript_payload])
        transcript_lines = self._transcript_lines([transcript_payload, detail_payload])
        if not transcript_lines:
            return None
        body = self._render_body(
            summary_text=summary_text, action_items=action_items, transcript_lines=transcript_lines
        )
        transcript_body_sha = compute_meeting_transcript_body_sha_from_payload(
            {
                "otter_meeting_id": meeting_id,
                "otter_conversation_id": _first_nonempty(
                    detail_payload.get("conversation_id"), detail_payload.get("conversationId")
                ),
                "title": title,
                "status": status,
                "start_at": start_at,
                "end_at": end_at,
                "duration_seconds": _parse_duration_seconds(
                    detail_payload.get("duration_seconds")
                    or detail_payload.get("durationSeconds")
                    or detail_metadata.get("duration")
                    or transcript_metadata.get("duration")
                    or row.get("duration_seconds")
                    or row.get("durationSeconds")
                    or row.get("duration")
                ),
                "speaker_names": speaker_names,
                "speaker_emails": speaker_emails,
                "participant_names": participant_names,
                "participant_emails": participant_emails,
                "host_name": host_name,
                "host_email": host_email,
                "conference_url": conference_url,
                "event_id_hint": event_id_hint,
                "ical_uid": ical_uid,
                "summary_text": summary_text,
                "action_items": action_items,
                "transcript": body,
            }
        )
        all_people_emails = [email for email in [host_email, *speaker_emails, *participant_emails] if email]
        calendar_events = self._match_calendar_event(
            title=title,
            event_id_hint=event_id_hint,
            ical_uid=ical_uid,
            conference_url=conference_url,
            start_at=start_at,
            participant_emails=all_people_emails,
            by_event_id=event_lookup_by_id,
            by_ical_uid=event_lookup_by_ical_uid,
            by_conference=event_lookup_by_conference,
            by_start_date=event_lookup_by_start_date,
        )
        return {
            "otter_meeting_id": meeting_id,
            "otter_conversation_id": _first_nonempty(
                detail_payload.get("conversation_id"), detail_payload.get("conversationId")
            ),
            "account_email": _clean(account_email).lower(),
            "title": title,
            "meeting_url": _first_nonempty(
                detail_payload.get("meeting_url"),
                detail_payload.get("meetingUrl"),
                detail_payload.get("share_url"),
                detail_payload.get("shareUrl"),
                detail_payload.get("url"),
                row.get("meeting_url"),
                row.get("share_url"),
                row.get("url"),
            ),
            "transcript_url": _first_nonempty(
                detail_payload.get("transcript_url"),
                detail_payload.get("transcriptUrl"),
                detail_payload.get("share_url"),
                detail_payload.get("shareUrl"),
                detail_payload.get("url"),
            ),
            "recording_url": _first_nonempty(
                detail_payload.get("recording_url"),
                detail_payload.get("recordingUrl"),
                detail_payload.get("share_url"),
                detail_payload.get("shareUrl"),
                detail_payload.get("url"),
            ),
            "conference_url": conference_url,
            "language": language,
            "status": status,
            "start_at": start_at,
            "end_at": end_at,
            "duration_seconds": _parse_duration_seconds(
                detail_payload.get("duration_seconds")
                or detail_payload.get("durationSeconds")
                or detail_metadata.get("duration")
                or transcript_metadata.get("duration")
                or row.get("duration_seconds")
                or row.get("durationSeconds")
                or row.get("duration")
                or 0
            ),
            "speaker_names": speaker_names,
            "speaker_emails": speaker_emails,
            "participant_names": participant_names,
            "participant_emails": participant_emails,
            "host_name": host_name,
            "host_email": host_email,
            "calendar_events": calendar_events,
            "event_id_hint": event_id_hint,
            "ical_uid": ical_uid,
            "otter_updated_at": otter_updated_at,
            "transcript_body_sha": transcript_body_sha,
            "people": self._resolve_people(identity_cache, all_people_emails),
            "created": _body_date(start_at or otter_updated_at),
            "updated": _body_date(otter_updated_at or start_at),
            "body": body,
        }

    def stage_transcripts(
        self,
        vault_path: str,
        stage_dir: str | Path,
        *,
        account_email: str = "",
        max_meetings: int | None = 100,
        page_size: int = 25,
        workers: int = 2,
        updated_after: str | None = None,
        start_after: str | None = None,
        end_before: str | None = None,
        quick_update: bool = False,
        progress_every: int = 25,
        verbose: bool = False,
        config=None,
    ) -> dict[str, Any]:
        files = self._stage_files(stage_dir)
        state = self._load_stage_state(files["state"])
        completed_meeting_ids = set(state.get("completed_meeting_ids", [])) if isinstance(state, dict) else set()
        page_token = _first_nonempty(state.get("page_token", "")) or None
        emitted_meetings = int(state.get("emitted_meetings", 0) or 0)
        skipped_unchanged_meetings = int(state.get("skipped_unchanged_meetings", 0) or 0)
        failed_hydrations = int(state.get("failed_hydrations", 0) or 0)
        started_at = time.perf_counter()

        def _log(message: str) -> None:
            if verbose:
                print(f"[otter-stage] {message}", flush=True)

        client = self._build_client()
        identity_cache = IdentityCache(vault_path)
        (
            event_lookup_by_id,
            event_lookup_by_ical_uid,
            event_lookup_by_conference,
            event_lookup_by_start_date,
        ) = self._calendar_lookup(vault_path)
        hash_cache_enabled = bool(getattr(config, "otter_transcript_body_sha_cache_enabled", True))
        quick_update_enabled = bool(quick_update and hash_cache_enabled)
        existing_state = (
            self._load_existing_transcript_state(vault_path, account_email=account_email)
            if quick_update_enabled
            else {}
        )
        filters = {
            "account_email": _clean(account_email).lower(),
            "updated_after": updated_after or "",
            "start_after": start_after or "",
            "end_before": end_before or "",
            "page_size": int(page_size),
        }
        hit_cap = False
        next_window_date_text = _first_nonempty(state.get("next_window_date", ""))

        def _process_rows(
            rows: list[dict[str, Any]], *, state_page_token: str | None, next_window_date: str | None
        ) -> None:
            nonlocal emitted_meetings, skipped_unchanged_meetings, failed_hydrations
            pending_rows: list[dict[str, Any]] = []
            for row in rows:
                meeting_id = _first_nonempty(
                    row.get("id"), row.get("otid"), row.get("meeting_id"), row.get("meetingId")
                )
                if not meeting_id or meeting_id in completed_meeting_ids:
                    continue
                if max_meetings is not None and emitted_meetings + len(pending_rows) >= max_meetings:
                    break
                if quick_update_enabled:
                    row_updated_at = _normalize_iso_datetime(
                        row.get("updated_at") or row.get("updatedAt") or row.get("modified_at") or row.get("modifiedAt")
                    )
                    if row_updated_at and row_updated_at == existing_state.get(meeting_id, {}).get(
                        "otter_updated_at", ""
                    ):
                        skipped_unchanged_meetings += 1
                        continue
                pending_rows.append(row)

            max_workers = max(1, min(int(workers or 1), max(1, len(pending_rows))))
            if pending_rows:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            self._hydrate_meeting,
                            row,
                            client=client,
                            identity_cache=identity_cache,
                            account_email=account_email,
                            event_lookup_by_id=event_lookup_by_id,
                            event_lookup_by_ical_uid=event_lookup_by_ical_uid,
                            event_lookup_by_conference=event_lookup_by_conference,
                            event_lookup_by_start_date=event_lookup_by_start_date,
                        ): row
                        for row in pending_rows
                    }
                    processed = 0
                    for future in as_completed(futures):
                        row = futures[future]
                        meeting_id = _first_nonempty(
                            row.get("id"), row.get("otid"), row.get("meeting_id"), row.get("meetingId")
                        )
                        try:
                            item = future.result()
                        except Exception as exc:
                            failed_hydrations += 1
                            self._append_failure_row(files["failures"], {"meeting_id": meeting_id, "error": str(exc)})
                            self._write_stage_state(
                                files["state"],
                                account_email=account_email,
                                filters=filters,
                                page_token=state_page_token,
                                next_window_date=next_window_date,
                                emitted_meetings=emitted_meetings,
                                skipped_unchanged_meetings=skipped_unchanged_meetings,
                                failed_hydrations=failed_hydrations,
                                completed_meeting_ids=completed_meeting_ids,
                                complete=False,
                            )
                            continue
                        if item is None:
                            failed_hydrations += 1
                            self._append_failure_row(
                                files["failures"], {"meeting_id": meeting_id, "error": "Transcript unavailable"}
                            )
                            self._write_stage_state(
                                files["state"],
                                account_email=account_email,
                                filters=filters,
                                page_token=state_page_token,
                                next_window_date=next_window_date,
                                emitted_meetings=emitted_meetings,
                                skipped_unchanged_meetings=skipped_unchanged_meetings,
                                failed_hydrations=failed_hydrations,
                                completed_meeting_ids=completed_meeting_ids,
                                complete=False,
                            )
                            continue
                        item.setdefault("kind", "meeting_transcript")
                        self._append_stage_row(files["meetings"], item)
                        completed_meeting_ids.add(_first_nonempty(item.get("otter_meeting_id")))
                        emitted_meetings += 1
                        processed += 1
                        self._write_stage_state(
                            files["state"],
                            account_email=account_email,
                            filters=filters,
                            page_token=state_page_token,
                            next_window_date=next_window_date,
                            emitted_meetings=emitted_meetings,
                            skipped_unchanged_meetings=skipped_unchanged_meetings,
                            failed_hydrations=failed_hydrations,
                            completed_meeting_ids=completed_meeting_ids,
                            complete=False,
                        )
                        if verbose and progress_every and processed % max(1, int(progress_every)) == 0:
                            _log(
                                f"processed={emitted_meetings} skipped={skipped_unchanged_meetings} "
                                f"failed={failed_hydrations} elapsed_s={time.perf_counter() - started_at:.1f}"
                            )

        if isinstance(client, OtterMcpClient):
            lower_bound_date = self._coerce_stage_window_date(start_after or updated_after, fallback=date(2020, 1, 1))
            upper_bound_date = self._coerce_stage_window_date(end_before, fallback=date.today())
            current_window_date = self._coerce_stage_window_date(next_window_date_text, fallback=upper_bound_date)
            while current_window_date >= lower_bound_date:
                window_text = current_window_date.isoformat()
                response = client.list_meetings(
                    page_size=max(1, page_size),
                    page_token=None,
                    updated_after=window_text,
                    start_after=window_text,
                    end_before=window_text,
                )
                rows = self._list_rows(response)
                next_window_date = (
                    (current_window_date - timedelta(days=1)).isoformat()
                    if current_window_date > lower_bound_date
                    else ""
                )
                _process_rows(rows, state_page_token=None, next_window_date=next_window_date)
                self._write_stage_state(
                    files["state"],
                    account_email=account_email,
                    filters={**filters, "window_strategy": "day_by_day_backward"},
                    page_token=None,
                    next_window_date=next_window_date,
                    emitted_meetings=emitted_meetings,
                    skipped_unchanged_meetings=skipped_unchanged_meetings,
                    failed_hydrations=failed_hydrations,
                    completed_meeting_ids=completed_meeting_ids,
                    complete=False,
                )
                if max_meetings is not None and emitted_meetings >= max_meetings:
                    hit_cap = True
                    break
                current_window_date -= timedelta(days=1)
        else:
            while True:
                response = client.list_meetings(
                    page_size=max(1, page_size),
                    page_token=page_token,
                    updated_after=updated_after,
                    start_after=start_after,
                    end_before=end_before,
                )
                rows = self._list_rows(response)
                if not rows:
                    break
                _process_rows(rows, state_page_token=page_token, next_window_date=None)

                page_token = self._next_page_token(response)
                self._write_stage_state(
                    files["state"],
                    account_email=account_email,
                    filters=filters,
                    page_token=page_token,
                    next_window_date=None,
                    emitted_meetings=emitted_meetings,
                    skipped_unchanged_meetings=skipped_unchanged_meetings,
                    failed_hydrations=failed_hydrations,
                    completed_meeting_ids=completed_meeting_ids,
                    complete=False,
                )
                if max_meetings is not None and emitted_meetings >= max_meetings:
                    hit_cap = True
                    break
                if not page_token:
                    break

        self._write_stage_state(
            files["state"],
            account_email=account_email,
            filters=filters,
            page_token=(None if (not hit_cap and not page_token) else page_token),
            next_window_date=(
                next_window_date_text
                if isinstance(client, OtterApiClient)
                else _first_nonempty(locals().get("next_window_date", ""))
            ),
            emitted_meetings=emitted_meetings,
            skipped_unchanged_meetings=skipped_unchanged_meetings,
            failed_hydrations=failed_hydrations,
            completed_meeting_ids=completed_meeting_ids,
            complete=(not hit_cap and (not page_token) and (not _first_nonempty(locals().get("next_window_date", "")))),
        )
        manifest = {
            "account_email": _clean(account_email).lower(),
            "filters": filters,
            "counts": {
                "meetings": emitted_meetings,
                "skipped_unchanged_meetings": skipped_unchanged_meetings,
                "failed_hydrations": failed_hydrations,
            },
            "elapsed_seconds": round(time.perf_counter() - started_at, 3),
            "stage_files": {
                "meetings": str(files["meetings"]),
                "extract_state": str(files["state"]),
                "hydration_failures": str(files["failures"]),
            },
        }
        files["manifest"].write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        return manifest

    def relink_stage(
        self,
        vault_path: str,
        stage_dir: str | Path,
        *,
        progress_every: int = 100,
        verbose: bool = False,
    ) -> dict[str, int]:
        stage_path = Path(stage_dir).expanduser().resolve()
        meetings_path = stage_path / "meetings.jsonl"
        if not meetings_path.exists():
            return {
                "rows_scanned": 0,
                "matches_found": 0,
                "transcripts_updated": 0,
                "calendar_events_updated": 0,
                "unmatched_rows": 0,
                "missing_transcripts": 0,
            }

        def _log(message: str) -> None:
            if verbose:
                print(f"[otter-relink] {message}", flush=True)

        (
            event_lookup_by_id,
            event_lookup_by_ical_uid,
            event_lookup_by_conference,
            event_lookup_by_start_date,
        ) = self._calendar_lookup(vault_path)

        calendar_rel_by_link: dict[str, str] = {}
        for candidates in event_lookup_by_start_date.values():
            for candidate in candidates:
                wikilink = _clean(candidate.get("wikilink", ""))
                rel_path = _clean(candidate.get("rel_path", ""))
                if wikilink and rel_path:
                    calendar_rel_by_link[wikilink] = rel_path

        transcript_by_meeting_id: dict[str, tuple[str, dict[str, Any], str, dict[str, ProvenanceEntry]]] = {}
        for rel_path in _iter_markdown_rel_paths_under(vault_path, "MeetingTranscripts"):
            frontmatter, body, provenance = read_note(vault_path, str(rel_path))
            if _clean(frontmatter.get("type", "")) != "meeting_transcript":
                continue
            meeting_id = _clean(frontmatter.get("otter_meeting_id", ""))
            if meeting_id:
                transcript_by_meeting_id[meeting_id] = (str(rel_path), frontmatter, body, provenance)

        stats = {
            "rows_scanned": 0,
            "matches_found": 0,
            "transcripts_updated": 0,
            "calendar_events_updated": 0,
            "unmatched_rows": 0,
            "missing_transcripts": 0,
        }
        today = date.today().isoformat()

        with meetings_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                row = json.loads(line)
                stats["rows_scanned"] += 1
                meeting_id = _first_nonempty(row.get("otter_meeting_id"), row.get("id"), row.get("otid"))
                if not meeting_id:
                    continue
                transcript_record = transcript_by_meeting_id.get(meeting_id)
                if transcript_record is None:
                    stats["missing_transcripts"] += 1
                    continue

                rel_path, frontmatter, body, provenance = transcript_record
                all_people_emails = [
                    _clean(email).lower()
                    for email in [
                        _first_nonempty(row.get("host_email"), frontmatter.get("host_email")),
                        *_coerce_list(row.get("speaker_emails") or frontmatter.get("speaker_emails")),
                        *_coerce_list(row.get("participant_emails") or frontmatter.get("participant_emails")),
                    ]
                    if _clean(email)
                ]
                matched_calendar_events = self._match_calendar_event(
                    title=_first_nonempty(
                        row.get("title"), row.get("summary"), frontmatter.get("title"), frontmatter.get("summary")
                    ),
                    event_id_hint=_first_nonempty(row.get("event_id_hint"), frontmatter.get("event_id_hint")),
                    ical_uid=_first_nonempty(row.get("ical_uid"), frontmatter.get("ical_uid")),
                    conference_url=_first_nonempty(row.get("conference_url"), frontmatter.get("conference_url")),
                    start_at=_first_nonempty(row.get("start_at"), frontmatter.get("start_at")),
                    participant_emails=all_people_emails,
                    by_event_id=event_lookup_by_id,
                    by_ical_uid=event_lookup_by_ical_uid,
                    by_conference=event_lookup_by_conference,
                    by_start_date=event_lookup_by_start_date,
                )
                if not matched_calendar_events:
                    stats["unmatched_rows"] += 1
                    continue

                stats["matches_found"] += 1
                updated_transcript_links = list(frontmatter.get("calendar_events", []) or [])
                transcript_changed = False
                for event_ref in matched_calendar_events:
                    if event_ref not in updated_transcript_links:
                        updated_transcript_links.append(event_ref)
                        transcript_changed = True
                transcript_uid = _clean(frontmatter.get("uid", ""))
                transcript_ref = _wikilink_from_uid(transcript_uid) if transcript_uid else ""

                if transcript_changed:
                    transcript_frontmatter = dict(frontmatter)
                    transcript_frontmatter["calendar_events"] = updated_transcript_links
                    transcript_card = MeetingTranscriptCard(**transcript_frontmatter)
                    transcript_provenance = dict(provenance)
                    transcript_provenance["calendar_events"] = ProvenanceEntry(
                        source=MEETING_SOURCE,
                        date=today,
                        method="deterministic",
                    )
                    write_card(vault_path, rel_path, transcript_card, body=body, provenance=transcript_provenance)
                    frontmatter = transcript_frontmatter
                    provenance = transcript_provenance
                    stats["transcripts_updated"] += 1

                for event_ref in matched_calendar_events:
                    event_rel_path = calendar_rel_by_link.get(event_ref)
                    if not event_rel_path or not transcript_ref:
                        continue
                    event_frontmatter, event_body, event_provenance = read_note(vault_path, event_rel_path)
                    existing_refs = list(event_frontmatter.get("meeting_transcripts", []) or [])
                    if transcript_ref in existing_refs:
                        continue
                    existing_refs.append(transcript_ref)
                    updated_event_frontmatter = dict(event_frontmatter)
                    updated_event_frontmatter["meeting_transcripts"] = existing_refs
                    event_card = CalendarEventCard(**updated_event_frontmatter)
                    updated_event_provenance = dict(event_provenance)
                    updated_event_provenance["meeting_transcripts"] = ProvenanceEntry(
                        source=MEETING_SOURCE,
                        date=today,
                        method="deterministic",
                    )
                    write_card(
                        vault_path, event_rel_path, event_card, body=event_body, provenance=updated_event_provenance
                    )
                    stats["calendar_events_updated"] += 1

                if progress_every and stats["rows_scanned"] % max(1, progress_every) == 0:
                    _log(
                        "progress: "
                        f"rows={stats['rows_scanned']} matches={stats['matches_found']} "
                        f"transcripts_updated={stats['transcripts_updated']} "
                        f"calendar_events_updated={stats['calendar_events_updated']}"
                    )

        return stats

    def relink_existing(
        self,
        vault_path: str,
        *,
        progress_every: int = 100,
        verbose: bool = False,
    ) -> dict[str, int]:
        def _log(message: str) -> None:
            if verbose:
                print(f"[otter-relink-existing] {message}", flush=True)

        (
            event_lookup_by_id,
            event_lookup_by_ical_uid,
            event_lookup_by_conference,
            event_lookup_by_start_date,
        ) = self._calendar_lookup(vault_path)

        calendar_rel_by_link: dict[str, str] = {}
        for candidates in event_lookup_by_start_date.values():
            for candidate in candidates:
                wikilink = _clean(candidate.get("wikilink", ""))
                rel_path = _clean(candidate.get("rel_path", ""))
                if wikilink and rel_path:
                    calendar_rel_by_link[wikilink] = rel_path

        stats = {
            "rows_scanned": 0,
            "matches_found": 0,
            "transcripts_updated": 0,
            "calendar_events_updated": 0,
            "unmatched_rows": 0,
            "missing_transcripts": 0,
        }
        today = date.today().isoformat()

        for rel_path in _iter_markdown_rel_paths_under(vault_path, "MeetingTranscripts"):
            frontmatter, body, provenance = read_note(vault_path, str(rel_path))
            if _clean(frontmatter.get("type", "")) != "meeting_transcript":
                continue
            stats["rows_scanned"] += 1
            all_people_emails = [
                _clean(email).lower()
                for email in [
                    _first_nonempty(frontmatter.get("host_email")),
                    *_coerce_list(frontmatter.get("speaker_emails")),
                    *_coerce_list(frontmatter.get("participant_emails")),
                ]
                if _clean(email)
            ]
            matched_calendar_events = self._match_calendar_event(
                title=_first_nonempty(frontmatter.get("title"), frontmatter.get("summary")),
                event_id_hint=_first_nonempty(frontmatter.get("event_id_hint")),
                ical_uid=_first_nonempty(frontmatter.get("ical_uid")),
                conference_url=_first_nonempty(frontmatter.get("conference_url")),
                start_at=_first_nonempty(frontmatter.get("start_at")),
                participant_emails=all_people_emails,
                by_event_id=event_lookup_by_id,
                by_ical_uid=event_lookup_by_ical_uid,
                by_conference=event_lookup_by_conference,
                by_start_date=event_lookup_by_start_date,
            )
            if not matched_calendar_events:
                stats["unmatched_rows"] += 1
                continue

            stats["matches_found"] += 1
            updated_transcript_links = list(frontmatter.get("calendar_events", []) or [])
            transcript_changed = False
            for event_ref in matched_calendar_events:
                if event_ref not in updated_transcript_links:
                    updated_transcript_links.append(event_ref)
                    transcript_changed = True

            transcript_uid = _clean(frontmatter.get("uid", ""))
            transcript_ref = _wikilink_from_uid(transcript_uid) if transcript_uid else ""

            if transcript_changed:
                transcript_frontmatter = dict(frontmatter)
                transcript_frontmatter["calendar_events"] = updated_transcript_links
                transcript_card = MeetingTranscriptCard(**transcript_frontmatter)
                transcript_provenance = dict(provenance)
                transcript_provenance["calendar_events"] = ProvenanceEntry(
                    source=MEETING_SOURCE,
                    date=today,
                    method="deterministic",
                )
                write_card(vault_path, str(rel_path), transcript_card, body=body, provenance=transcript_provenance)
                frontmatter = transcript_frontmatter
                provenance = transcript_provenance
                stats["transcripts_updated"] += 1

            for event_ref in matched_calendar_events:
                event_rel_path = calendar_rel_by_link.get(event_ref)
                if not event_rel_path or not transcript_ref:
                    continue
                event_frontmatter, event_body, event_provenance = read_note(vault_path, event_rel_path)
                existing_refs = list(event_frontmatter.get("meeting_transcripts", []) or [])
                if transcript_ref in existing_refs:
                    continue
                existing_refs.append(transcript_ref)
                updated_event_frontmatter = dict(event_frontmatter)
                updated_event_frontmatter["meeting_transcripts"] = existing_refs
                event_card = CalendarEventCard(**updated_event_frontmatter)
                updated_event_provenance = dict(event_provenance)
                updated_event_provenance["meeting_transcripts"] = ProvenanceEntry(
                    source=MEETING_SOURCE,
                    date=today,
                    method="deterministic",
                )
                write_card(vault_path, event_rel_path, event_card, body=event_body, provenance=updated_event_provenance)
                stats["calendar_events_updated"] += 1

            if progress_every and stats["rows_scanned"] % max(1, progress_every) == 0:
                _log(
                    "progress: "
                    f"rows={stats['rows_scanned']} matches={stats['matches_found']} "
                    f"transcripts_updated={stats['transcripts_updated']} "
                    f"calendar_events_updated={stats['calendar_events_updated']}"
                )

        return stats

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

    def _iter_staged_batches(
        self,
        vault_path: str,
        stage_dir: str | Path,
        *,
        batch_size: int,
        max_items: int | None = None,
    ):
        stage_path = Path(stage_dir).expanduser().resolve()
        sequence = 0
        emitted = 0
        batch_items: list[dict[str, Any]] = []
        (
            event_lookup_by_id,
            event_lookup_by_ical_uid,
            event_lookup_by_conference,
            event_lookup_by_start_date,
        ) = self._calendar_lookup(vault_path)
        meetings_path = stage_path / "meetings.jsonl"
        if not meetings_path.exists():
            return
        with meetings_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                row = json.loads(line)
                expanded_rows = self._expand_import_items(
                    vault_path,
                    [row],
                    event_lookup_by_id=event_lookup_by_id,
                    event_lookup_by_ical_uid=event_lookup_by_ical_uid,
                    event_lookup_by_conference=event_lookup_by_conference,
                    event_lookup_by_start_date=event_lookup_by_start_date,
                )
                for expanded_row in expanded_rows:
                    batch_items.append(expanded_row)
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

    def _live_fetch_batches(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        account_email: str = "",
        max_meetings: int | None = 100,
        page_size: int = 25,
        workers: int = 2,
        updated_after: str | None = None,
        start_after: str | None = None,
        end_before: str | None = None,
        quick_update: bool = False,
        **kwargs,
    ):
        client = self._build_client()
        identity_cache = IdentityCache(vault_path)
        (
            event_lookup_by_id,
            event_lookup_by_ical_uid,
            event_lookup_by_conference,
            event_lookup_by_start_date,
        ) = self._calendar_lookup(vault_path)
        hash_cache_enabled = bool(getattr(config, "otter_transcript_body_sha_cache_enabled", True))
        quick_update_enabled = bool(quick_update and hash_cache_enabled)
        existing_state = (
            self._load_existing_transcript_state(vault_path, account_email=account_email)
            if quick_update_enabled
            else {}
        )
        page_token = cursor.get("page_token")
        emitted_meetings = int(cursor.get("emitted_meetings", 0) or 0)
        sequence = int(cursor.get("batch_sequence", 0) or 0)
        total_skipped = int(cursor.get("skipped_unchanged_meetings", 0) or 0)
        while True:
            response = client.list_meetings(
                page_size=max(1, page_size),
                page_token=page_token,
                updated_after=updated_after,
                start_after=start_after,
                end_before=end_before,
            )
            rows = self._list_rows(response)
            if not rows:
                break
            batch_skipped = 0
            pending_rows: list[dict[str, Any]] = []
            for row in rows:
                meeting_id = _first_nonempty(
                    row.get("id"), row.get("otid"), row.get("meeting_id"), row.get("meetingId")
                )
                if not meeting_id:
                    continue
                if max_meetings is not None and emitted_meetings + len(pending_rows) >= max_meetings:
                    break
                if quick_update_enabled:
                    row_updated_at = _normalize_iso_datetime(
                        row.get("updated_at") or row.get("updatedAt") or row.get("modified_at") or row.get("modifiedAt")
                    )
                    if row_updated_at and row_updated_at == existing_state.get(meeting_id, {}).get(
                        "otter_updated_at", ""
                    ):
                        batch_skipped += 1
                        total_skipped += 1
                        continue
                pending_rows.append(row)

            hydrated_items: list[dict[str, Any]] = []
            failed_hydrations = 0
            max_workers = max(1, min(int(workers or 1), max(len(pending_rows), 1)))
            if pending_rows:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            self._hydrate_meeting,
                            row,
                            client=client,
                            identity_cache=identity_cache,
                            account_email=account_email,
                            event_lookup_by_id=event_lookup_by_id,
                            event_lookup_by_ical_uid=event_lookup_by_ical_uid,
                            event_lookup_by_conference=event_lookup_by_conference,
                            event_lookup_by_start_date=event_lookup_by_start_date,
                        ): row
                        for row in pending_rows
                    }
                    for future in as_completed(futures):
                        try:
                            item = future.result()
                        except Exception:
                            failed_hydrations += 1
                            continue
                        if item:
                            hydrated_items.append(item)
            hydrated_items.sort(key=lambda item: (item.get("start_at", ""), item.get("otter_meeting_id", "")))
            emitted_meetings += len(hydrated_items)
            next_page_token = self._next_page_token(response)
            expanded_items = self._expand_import_items(vault_path, hydrated_items)
            yield FetchedBatch(
                items=expanded_items,
                cursor_patch={
                    "page_token": next_page_token,
                    "emitted_meetings": emitted_meetings,
                    "batch_sequence": sequence + 1,
                    "skipped_unchanged_meetings": total_skipped,
                },
                sequence=sequence,
                skipped_count=batch_skipped + failed_hydrations,
                skip_details={
                    "skipped_unchanged_meetings": batch_skipped,
                    "failed_hydrations": failed_hydrations,
                },
            )
            sequence += 1
            page_token = next_page_token
            if max_meetings is not None and emitted_meetings >= max_meetings:
                break
            if not page_token:
                break

    def fetch_batches(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        **kwargs,
    ):
        stage_dir = kwargs.get("stage_dir")
        if stage_dir:
            batch_size = max(1, int(kwargs.get("batch_size") or 100))
            yield from self._iter_staged_batches(
                vault_path, stage_dir, batch_size=batch_size, max_items=kwargs.get("max_items")
            )
            return
        yield from self._live_fetch_batches(vault_path, cursor, config=config, **kwargs)

    def finalize_cursor(self, cursor: dict[str, Any], **kwargs) -> dict[str, Any] | None:
        return {
            "page_token": None,
            "last_sync": datetime.now().isoformat(),
        }

    def to_card(self, item: dict[str, Any]):
        kind = _clean(item.get("kind", "")).lower()
        if kind == "calendar_backlink":
            frontmatter = dict(item.get("frontmatter", {}) or {})
            meeting_ref = _clean(item.get("meeting_transcript_ref", ""))
            existing_refs = list(frontmatter.get("meeting_transcripts", []) or [])
            if meeting_ref and meeting_ref not in existing_refs:
                existing_refs.append(meeting_ref)
            card = CalendarEventCard(
                **{
                    key: value
                    for key, value in {
                        **frontmatter,
                        "meeting_transcripts": existing_refs,
                    }.items()
                    if key in CalendarEventCard.model_fields
                }
            )
            today = date.today().isoformat()
            provenance = {}
            if meeting_ref:
                provenance["meeting_transcripts"] = ProvenanceEntry(
                    source=MEETING_SOURCE, date=today, method="deterministic"
                )
            return card, provenance, ""

        meeting_id = _first_nonempty(item.get("otter_meeting_id"))
        today = date.today().isoformat()
        card = MeetingTranscriptCard(
            uid=_meeting_uid(meeting_id),
            type="meeting_transcript",
            source=[MEETING_SOURCE],
            source_id=meeting_id,
            created=_first_nonempty(item.get("created")) or today,
            updated=today,
            summary=_first_nonempty(item.get("title")) or meeting_id,
            people=list(item.get("people", [])),
            account_email=_first_nonempty(item.get("account_email")).lower(),
            otter_meeting_id=meeting_id,
            otter_conversation_id=_first_nonempty(item.get("otter_conversation_id")),
            title=_first_nonempty(item.get("title")),
            meeting_url=_first_nonempty(item.get("meeting_url")),
            transcript_url=_first_nonempty(item.get("transcript_url")),
            recording_url=_first_nonempty(item.get("recording_url")),
            conference_url=_first_nonempty(item.get("conference_url")),
            language=_first_nonempty(item.get("language")),
            status=_first_nonempty(item.get("status")),
            start_at=_first_nonempty(item.get("start_at")),
            end_at=_first_nonempty(item.get("end_at")),
            duration_seconds=int(item.get("duration_seconds", 0) or 0),
            speaker_names=list(item.get("speaker_names", [])),
            speaker_emails=list(item.get("speaker_emails", [])),
            participant_names=list(item.get("participant_names", [])),
            participant_emails=list(item.get("participant_emails", [])),
            host_name=_first_nonempty(item.get("host_name")),
            host_email=_first_nonempty(item.get("host_email")).lower(),
            calendar_events=list(item.get("calendar_events", [])),
            event_id_hint=_first_nonempty(item.get("event_id_hint")),
            ical_uid=_first_nonempty(item.get("ical_uid")),
            otter_updated_at=_first_nonempty(item.get("otter_updated_at")),
            transcript_body_sha=_first_nonempty(item.get("transcript_body_sha")),
        )
        provenance = deterministic_provenance(card, MEETING_SOURCE)
        return card, provenance, str(item.get("body", "")).strip()

    def merge_card(self, vault_path, rel_path, card, body, provenance) -> None:
        self._replace_generic_card(vault_path, rel_path, card, body, provenance)
