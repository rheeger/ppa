"""Gmail correspondents adapter using the gws CLI."""

from __future__ import annotations

import json
import os
import re
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from email.utils import getaddresses
from pathlib import Path
from typing import Any

from .base import BaseAdapter, deterministic_provenance
from ppa_google_auth import ACCOUNTS, build_google_cli_token_manager
from hfa.schema import PersonCard
from hfa.uid import generate_uid

AUTOMATED_LOCAL_PREFIXES = {
    "alert",
    "alerts",
    "billing",
    "comment",
    "community",
    "contact",
    "donotreply",
    "do-not-reply",
    "hello",
    "info",
    "mail",
    "mailer-daemon",
    "newsletter",
    "no-reply",
    "noreply",
    "notification",
    "notifications",
    "push",
    "receipt",
    "receipts",
    "reply",
    "security",
    "subscribed",
    "support",
    "update",
    "updates",
}
AUTOMATED_DOMAINS = {
    "facebookmail.com",
    "googlegroups.com",
    "linkedin.com",
    "noreply.github.com",
    "reply.github.com",
    "reply.linkedin.com",
    "replies.uber.com",
    "substack.com",
}
AUTOMATED_DOMAIN_PREFIXES = {
    "about",
    "e",
    "email",
    "lists",
    "mail",
    "news",
    "notification",
    "notifications",
    "o",
    "promotion",
    "promotions",
    "reply",
    "replies",
    "welcome",
}
NON_PERSON_NAME_TOKENS = {
    "advisors",
    "alliance",
    "american",
    "animal",
    "air",
    "buy",
    "capital",
    "club",
    "community",
    "company",
    "cooking",
    "daily",
    "express",
    "facebook",
    "foundation",
    "from",
    "fund",
    "geographic",
    "group",
    "hospital",
    "hotels",
    "information",
    "institute",
    "lines",
    "linkedin",
    "mail",
    "management",
    "making",
    "national",
    "news",
    "on",
    "partners",
    "porter",
    "resident",
    "residents",
    "running",
    "team",
    "the",
    "university",
    "via",
    "moves",
}


def _split_display_name(name: str) -> tuple[str, str]:
    cleaned = " ".join(name.strip().split())
    if not cleaned:
        return "", ""
    parts = cleaned.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _looks_like_person_name(name: str) -> bool:
    cleaned = " ".join(name.strip().split())
    if not cleaned or any(char in cleaned for char in "@/"):
        return False
    if cleaned.isupper():
        return False
    tokens = [re.sub(r"[^A-Za-z'-]", "", token) for token in cleaned.split()]
    tokens = [token for token in tokens if token]
    if len(tokens) < 2 or len(tokens) > 4:
        return False
    lowered = {token.lower() for token in tokens}
    if lowered & NON_PERSON_NAME_TOKENS:
        return False
    return all(len(token) >= 2 and token.replace("-", "").replace("'", "").isalpha() for token in tokens)


def _looks_like_person_local_part(local: str) -> bool:
    cleaned = local.lower().strip()
    if not cleaned:
        return False
    base = cleaned.split("+", 1)[0]
    tokens = [token for token in re.split(r"[._+-]+", base) if token]
    if base in AUTOMATED_LOCAL_PREFIXES or any(token in AUTOMATED_LOCAL_PREFIXES for token in tokens):
        return False
    if any(base.startswith(f"{prefix}-") or base.startswith(f"{prefix}_") for prefix in AUTOMATED_LOCAL_PREFIXES):
        return False
    if base.startswith("reply+") or base.startswith("reply-"):
        return False
    if len(tokens) >= 2 and all(token.isalpha() and len(token) >= 2 for token in tokens[:3]):
        return True
    return False


def _is_automated_local(local: str) -> bool:
    cleaned = local.lower().strip()
    if not cleaned:
        return True
    base = cleaned.split("+", 1)[0]
    tokens = [token for token in re.split(r"[._+-]+", base) if token]
    if base in AUTOMATED_LOCAL_PREFIXES or any(token in AUTOMATED_LOCAL_PREFIXES for token in tokens):
        return True
    if any(base.startswith(f"{prefix}-") or base.startswith(f"{prefix}_") for prefix in AUTOMATED_LOCAL_PREFIXES):
        return True
    return base.startswith("reply+") or base.startswith("reply-")


def _should_keep_correspondent(name: str, email: str) -> bool:
    local, _, domain = email.partition("@")
    local = local.lower().strip()
    domain = domain.lower().strip()
    first_label = domain.split(".", 1)[0]
    if domain in AUTOMATED_DOMAINS:
        return False
    if first_label in AUTOMATED_DOMAIN_PREFIXES:
        return False
    if _is_automated_local(local):
        return False
    if _looks_like_person_name(name):
        return True
    return _looks_like_person_local_part(local)


def _extract_addresses_from_headers(headers: list[dict[str, str]]) -> list[tuple[str, str]]:
    values: list[str] = []
    keep = {"from", "to", "cc", "bcc", "reply-to"}
    for h in headers:
        name = (h.get("name") or "").lower()
        if name in keep:
            values.append(h.get("value", ""))
    pairs = getaddresses(values)
    out: list[tuple[str, str]] = []
    for n, e in pairs:
        em = (e or "").strip().lower()
        if not em or "@" not in em:
            continue
        out.append(((n or "").strip(), em))
    return out


def _strip_yaml_scalar(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _parse_inline_list(value: str) -> list[str]:
    raw = value.strip()
    if not (raw.startswith("[") and raw.endswith("]")):
        return []
    inner = raw[1:-1].strip()
    if not inner:
        return []
    return [_strip_yaml_scalar(part) for part in inner.split(",") if _strip_yaml_scalar(part)]


def _managed_account_emails() -> set[str]:
    return {
        str(account.get("email", "")).strip().lower()
        for account in ACCOUNTS.values()
        if str(account.get("email", "")).strip()
    }


class GmailCorrespondentsAdapter(BaseAdapter):
    source_id = "gmail-correspondents"
    preload_existing_uid_index = False

    def _ensure_token_manager(self, account_email: str) -> None:
        account = account_email.strip().lower()
        token_key = ("gmail", account)
        if getattr(self, "_token_manager_key", None) == token_key:
            return
        try:
            self._token_manager = build_google_cli_token_manager(
                account_email=account,
                services=["gmail"],
            )
        except RuntimeError:
            self._token_manager = None
        self._token_manager_key = token_key

    def get_cursor_key(self, **kwargs) -> str:
        account_emails = sorted(
            {
                str(value).strip().lower()
                for value in (kwargs.get("account_emails") or [])
                if str(value).strip()
            }
        )
        if account_emails:
            return f"{self.source_id}:aggregate:{'+'.join(account_emails)}"
        account_email = str(kwargs.get("account_email", "")).strip().lower()
        return f"{self.source_id}:{account_email}" if account_email else self.source_id

    def _gws(self, args: list[str]) -> dict[str, Any]:
        env = None
        token_manager = getattr(self, "_token_manager", None)
        if token_manager is not None:
            env = token_manager.build_env()
        proc = subprocess.run(["gws", *args], capture_output=True, text=True, check=False, env=env)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "gws command failed")
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid gws JSON output: {e}") from e

    def _read_local_message_fields(self, path: Path) -> dict[str, Any]:
        values: dict[str, Any] = {
            "account_email": "",
            "from_name": "",
            "from_email": "",
            "to_emails": [],
            "reply_to_emails": [],
        }
        current_list_key: str | None = None
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as handle:
                if handle.readline().strip() != "---":
                    return values
                for raw_line in handle:
                    line = raw_line.rstrip("\n")
                    stripped = line.strip()
                    if stripped == "---":
                        break
                    if current_list_key is not None:
                        if stripped.startswith("- "):
                            cast_list = values[current_list_key]
                            if isinstance(cast_list, list):
                                item = _strip_yaml_scalar(stripped[2:])
                                if item:
                                    cast_list.append(item)
                            continue
                        current_list_key = None
                    if ":" not in line:
                        continue
                    key, raw_value = line.split(":", 1)
                    key = key.strip()
                    raw_value = raw_value.strip()
                    if key not in values:
                        continue
                    if key in {"to_emails", "reply_to_emails"}:
                        if raw_value.startswith("["):
                            values[key] = _parse_inline_list(raw_value)
                        elif not raw_value:
                            values[key] = []
                            current_list_key = key
                    else:
                        values[key] = _strip_yaml_scalar(raw_value)
        except FileNotFoundError:
            return values
        return values

    def _fetch_from_local_messages(
        self,
        vault_path: str,
        own: set[str],
        *,
        account_emails: set[str] | None = None,
        log=None,
        progress_every: int | None = None,
    ) -> list[dict[str, Any]]:
        email_root = Path(vault_path) / "Email"
        counts: dict[str, dict[str, Any]] = defaultdict(lambda: {"name": "", "email": "", "count": 0})
        scanned = 0
        normalized_account_filters = {value.strip().lower() for value in (account_emails or set()) if value.strip()}
        paths = list(email_root.rglob("*.md"))
        max_workers = min(32, max(4, (os.cpu_count() or 8) * 2))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            iterator = executor.map(self._read_local_message_fields, paths, chunksize=128)
            for payload in iterator:
                payload_account = str(payload.get("account_email", "")).strip().lower()
                if normalized_account_filters and payload_account not in normalized_account_filters:
                    continue
                per_message: dict[str, str] = {}
                from_email = str(payload.get("from_email", "")).strip().lower()
                from_name = str(payload.get("from_name", "")).strip()
                if from_email:
                    per_message[from_email] = from_name
                for key in ("to_emails", "reply_to_emails"):
                    for email in payload.get(key, []) or []:
                        normalized = str(email).strip().lower()
                        if normalized and normalized not in per_message:
                            per_message[normalized] = ""
                for email, name in per_message.items():
                    if email in own:
                        continue
                    if not _should_keep_correspondent(name, email):
                        continue
                    row = counts[email]
                    if name and not row["name"]:
                        row["name"] = name
                    row["email"] = email
                    row["count"] += 1
                scanned += 1
                if log and progress_every and scanned % max(1, int(progress_every)) == 0:
                    log(
                        f"local message scan progress: scanned={scanned} unique_correspondents={len(counts)} "
                        f"account_filters={sorted(normalized_account_filters) if normalized_account_filters else ['all']}"
                    )
        items = sorted(counts.values(), key=lambda x: (-x["count"], x["email"]))
        for item in items:
            item["scanned_messages"] = scanned
            item["next_page_token"] = None
        return items

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        account_email: str = "",
        max_messages: int | None = None,
        query: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        self._ensure_token_manager(account_email)
        verbose = self.ingest_verbose(**kwargs)
        progress_every = self.ingest_progress_every(**kwargs)

        def _log(message: str) -> None:
            if verbose:
                print(f"{self.source_id}: {message}", flush=True)

        requested_account_filters = {
            str(value).strip().lower()
            for value in [account_email, *(kwargs.get("account_emails") or [])]
            if str(value).strip()
        }
        own = set(requested_account_filters)
        own.update(_managed_account_emails())
        own_map = load_own_aliases(vault_path)
        own.update(a.lower() for a in own_map if "@" in a)

        if not query and (Path(vault_path) / "Email").exists():
            items = self._fetch_from_local_messages(
                vault_path,
                own,
                account_emails=requested_account_filters or None,
                log=_log,
                progress_every=progress_every,
            )
            scanned = items[0]["scanned_messages"] if items else 0
            cursor["page_token"] = None
            cursor["scanned_messages"] = scanned
            return items

        page_token = cursor.get("page_token")
        scanned = int(cursor.get("scanned_messages", 0) or 0)
        batch_scanned = 0
        counts: dict[str, dict[str, Any]] = defaultdict(lambda: {"name": "", "email": "", "count": 0})

        while True:
            params: dict[str, Any] = {"userId": "me", "maxResults": 500}
            if page_token:
                params["pageToken"] = page_token
            if query:
                params["q"] = query
            list_data = self._gws(["gmail", "users", "messages", "list", "--params", json.dumps(params)])
            msgs = list_data.get("messages", [])
            if not msgs:
                page_token = None
                break

            for m in msgs:
                mid = m.get("id")
                if not mid:
                    continue
                get_params = {
                    "userId": "me",
                    "id": mid,
                    "format": "metadata",
                }
                msg = self._gws(["gmail", "users", "messages", "get", "--params", json.dumps(get_params)])
                headers = msg.get("payload", {}).get("headers", [])
                for name, email in _extract_addresses_from_headers(headers):
                    if email in own:
                        continue
                    if not _should_keep_correspondent(name, email):
                        continue
                    row = counts[email]
                    if name and not row["name"]:
                        row["name"] = name
                    row["email"] = email
                    row["count"] += 1
                scanned += 1
                batch_scanned += 1
                if max_messages and batch_scanned >= max_messages:
                    next_token = list_data.get("nextPageToken")
                    cursor["page_token"] = next_token
                    cursor["scanned_messages"] = scanned
                    items = sorted(counts.values(), key=lambda x: (-x["count"], x["email"]))
                    for i in items:
                        i["scanned_messages"] = scanned
                        i["next_page_token"] = next_token
                    return items

            page_token = list_data.get("nextPageToken")
            if not page_token:
                break

        cursor["page_token"] = None
        cursor["scanned_messages"] = scanned
        items = sorted(counts.values(), key=lambda x: (-x["count"], x["email"]))
        for i in items:
            i["scanned_messages"] = scanned
            i["next_page_token"] = None
        return items

    def to_card(self, item: dict[str, Any]):
        today = date.today().isoformat()
        email = str(item.get("email", "")).strip().lower()
        name = str(item.get("name", "")).strip()
        first_name, last_name = _split_display_name(name)
        source_id = email or str(item.get("name", "")).strip() or "gmail-correspondent-unknown"
        card = PersonCard(
            uid=generate_uid("person", self.source_id, source_id),
            type="person",
            source=[self.source_id],
            source_id=source_id,
            created=today,
            updated=today,
            summary=name or email or "unknown",
            first_name=first_name,
            last_name=last_name,
            emails=[email] if email else [],
            tags=["email-correspondent", "gmail-correspondent"],
            emails_seen_count=int(item.get("count", 0) or 0),
        )
        provenance = deterministic_provenance(card, self.source_id)
        return card, provenance, ""


def load_own_aliases(vault_path: str) -> set[str]:
    path = os.path.join(vault_path, "_meta", "own-emails.json")
    if not os.path.exists(path):
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {str(x).strip().lower() for x in data if isinstance(x, str)}
    except (OSError, json.JSONDecodeError):
        pass
    return set()
