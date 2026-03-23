"""Google OAuth token management for PPA — extracted from arnoldlib.google_cli_auth."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
import webbrowser
from collections.abc import Sequence
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

from .accounts import ACCOUNTS

TOKEN_ENV_VAR = "GOOGLE_WORKSPACE_CLI_TOKEN"
DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token"
DEFAULT_GWS_CLIENT_PATH = Path.home() / ".config" / "gws" / "client_secret.json"
DEFAULT_LOCAL_OAUTH_KEYS = Path.home() / ".gmail-mcp" / "gcp-oauth.keys.json"
DEFAULT_LOCAL_CREDENTIALS = Path.home() / ".gmail-mcp" / "credentials.json"
TOKEN_CACHE_DIR = Path(os.environ.get("OPENCLAW_GOOGLE_TOKEN_CACHE_DIR", "/tmp/openclaw/google-cli-tokens"))
REFRESH_TOKEN_CACHE_DIR = Path(
    os.environ.get("OPENCLAW_GOOGLE_REFRESH_TOKEN_CACHE_DIR", "/tmp/openclaw/google-cli-refresh-tokens")
)
TOKEN_REFRESH_MARGIN_SECONDS = 300

OP_TOKEN_FILE = os.environ.get(
    "PPA_OP_SERVICE_ACCOUNT_TOKEN_FILE",
    "/home/arnold/.openclaw/credentials/op-service-account-token",
)

MINIMAL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
]

GMAIL_READONLY_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
]

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.modify",
]

CALENDAR_READONLY_SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
]

CALENDAR_SCOPES = [
    "https://www.googleapis.com/auth/calendar",
]

DRIVE_READONLY_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
]

DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
]

SHEETS_READONLY_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

TASKS_READONLY_SCOPES = [
    "https://www.googleapis.com/auth/tasks.readonly",
]

TASKS_SCOPES = [
    "https://www.googleapis.com/auth/tasks",
]

CONTACTS_READONLY_SCOPES = [
    "https://www.googleapis.com/auth/contacts.readonly",
]

CONTACTS_SCOPES = [
    "https://www.googleapis.com/auth/contacts",
]

DIRECTORY_READONLY_SCOPES = [
    "https://www.googleapis.com/auth/directory.readonly",
]

STANDARD_SCOPES = [
    *GMAIL_SCOPES,
    "https://www.googleapis.com/auth/calendar.events",
    *CALENDAR_READONLY_SCOPES,
    *DRIVE_READONLY_SCOPES,
    *SHEETS_READONLY_SCOPES,
    *TASKS_READONLY_SCOPES,
    *CONTACTS_READONLY_SCOPES,
    *DIRECTORY_READONLY_SCOPES,
]

READ_ONLY_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/calendar.events",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/tasks.readonly",
    "https://www.googleapis.com/auth/contacts.readonly",
    "https://www.googleapis.com/auth/directory.readonly",
]

DRAFTER_PLUS_SCOPES = list(STANDARD_SCOPES)

FULL_SCOPES = [
    *GMAIL_SCOPES,
    *CALENDAR_SCOPES,
    *DRIVE_SCOPES,
    *SHEETS_SCOPES,
    *TASKS_SCOPES,
    *CONTACTS_SCOPES,
    *DIRECTORY_READONLY_SCOPES,
]

SERVICE_SCOPES: dict[str, list[str]] = {
    "gmail": GMAIL_SCOPES,
    "calendar": CALENDAR_SCOPES,
    "drive": DRIVE_SCOPES,
    "sheets": SHEETS_SCOPES,
    "tasks": TASKS_SCOPES,
    "contacts": CONTACTS_SCOPES,
    "directory": DIRECTORY_READONLY_SCOPES,
}

SCOPE_PROFILES: dict[str, list[str]] = {
    "minimal": MINIMAL_SCOPES,
    "gmail-readonly": GMAIL_READONLY_SCOPES,
    "gmail": GMAIL_SCOPES,
    "calendar-readonly": CALENDAR_READONLY_SCOPES,
    "calendar": CALENDAR_SCOPES,
    "readonly": READ_ONLY_SCOPES,
    "standard": STANDARD_SCOPES,
    "full": FULL_SCOPES,
}


def _dedupe_preserve_order(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        cleaned = str(value).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
    return deduped


def account_name_from_email(account_email: str) -> str | None:
    target = account_email.strip().lower()
    if not target:
        return None
    for account_name, account in ACCOUNTS.items():
        if str(account.get("email", "")).strip().lower() == target:
            return account_name
    return None


def default_scopes_for_account(account_name: str | None) -> list[str]:
    if not account_name or account_name not in ACCOUNTS:
        return list(FULL_SCOPES)
    account = ACCOUNTS[account_name]
    profile = account.get("scope_profile")
    if account.get("read_only"):
        return list(READ_ONLY_SCOPES)
    if profile == "standard":
        return list(DRAFTER_PLUS_SCOPES)
    return list(FULL_SCOPES)


def scopes_for_services(services: Sequence[str]) -> list[str]:
    scopes: list[str] = []
    for service in services:
        key = str(service).strip().lower()
        if not key:
            continue
        if key not in SERVICE_SCOPES:
            raise ValueError(f"Unknown Google service '{service}'")
        scopes.extend(SERVICE_SCOPES[key])
    return _dedupe_preserve_order(scopes)


def resolve_scopes(
    *,
    account_name: str | None = None,
    scope_profile: str | None = None,
    services: Sequence[str] | None = None,
    scopes: Sequence[str] | None = None,
) -> list[str]:
    if scopes:
        return _dedupe_preserve_order(scopes)
    if services:
        return scopes_for_services(services)
    if scope_profile:
        normalized = scope_profile.strip().lower()
        if normalized not in SCOPE_PROFILES:
            raise ValueError(f"Unknown scope profile '{scope_profile}'")
        return list(SCOPE_PROFILES[normalized])
    return default_scopes_for_account(account_name)


def _normalize_client_config(raw: dict) -> dict[str, str]:
    if "installed" in raw:
        raw = raw["installed"]
    elif "web" in raw:
        raw = raw["web"]
    if not isinstance(raw, dict):
        raise RuntimeError("Invalid OAuth client config format")
    client_id = str(raw.get("client_id", "")).strip()
    client_secret = str(raw.get("client_secret", "")).strip()
    if not client_id or not client_secret:
        raise RuntimeError("OAuth client config missing client_id or client_secret")
    token_uri = str(raw.get("token_uri", DEFAULT_TOKEN_URI)).strip() or DEFAULT_TOKEN_URI
    redirect_uris = [str(uri).strip() for uri in raw.get("redirect_uris", []) if str(uri).strip()]
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "token_uri": token_uri,
        "redirect_uri": redirect_uris[0] if redirect_uris else "",
    }


def load_google_oauth_client_config(
    *,
    client_json: str | None = None,
    client_json_path: str | Path | None = None,
) -> dict[str, str]:
    candidates: list[str] = []
    if client_json:
        candidates.append(client_json)
    env_json = os.environ.get("GOOGLE_OAUTH_CLIENT_JSON")
    if env_json:
        candidates.append(env_json)
    for candidate in candidates:
        try:
            return _normalize_client_config(json.loads(candidate))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid OAuth client JSON: {exc}") from exc

    paths: list[Path] = []
    if client_json_path is not None:
        paths.append(Path(client_json_path))
    paths.extend([DEFAULT_LOCAL_OAUTH_KEYS, DEFAULT_GWS_CLIENT_PATH])
    for path in paths:
        if not path.exists():
            continue
        try:
            return _normalize_client_config(json.loads(path.read_text(encoding="utf-8")))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid OAuth client JSON in {path}: {exc}") from exc
    raise RuntimeError("No usable Google OAuth client config found")


def load_local_refresh_token(credentials_path: str | Path | None = None) -> str | None:
    path = Path(credentials_path) if credentials_path is not None else DEFAULT_LOCAL_CREDENTIALS
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid local credentials JSON in {path}: {exc}") from exc
    refresh_token = str(payload.get("refresh_token", "")).strip()
    return refresh_token or None


def _refresh_token_cache_path(*, account_name: str | None = None, token_ref: str = "") -> Path:
    cache_material = json.dumps(
        {
            "account_name": account_name or "",
            "token_ref": token_ref,
        },
        sort_keys=True,
    )
    cache_key = hashlib.sha256(cache_material.encode("utf-8")).hexdigest()[:16]
    return REFRESH_TOKEN_CACHE_DIR / f"{cache_key}.json"


def _read_cached_refresh_token(*, account_name: str | None = None, token_ref: str = "") -> str | None:
    path = _refresh_token_cache_path(account_name=account_name, token_ref=token_ref)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    refresh_token = str(payload.get("refresh_token", "")).strip()
    return refresh_token or None


def _write_cached_refresh_token(refresh_token: str, *, account_name: str | None = None, token_ref: str = "") -> None:
    cleaned = refresh_token.strip()
    if not cleaned:
        return
    _write_json_atomic(
        _refresh_token_cache_path(account_name=account_name, token_ref=token_ref),
        {"refresh_token": cleaned},
    )


def _safe_op_read(reference: str) -> str | None:
    token_file = Path(OP_TOKEN_FILE)
    op_env = dict(os.environ)
    if token_file.exists():
        service_token = token_file.read_text(encoding="utf-8").strip()
        if service_token:
            op_env["OP_SERVICE_ACCOUNT_TOKEN"] = service_token
    for attempt in range(1, 4):
        try:
            proc = subprocess.run(
                ["op", "read", reference],
                capture_output=True,
                text=True,
                check=False,
                timeout=20 * attempt,
                env=op_env,
            )
        except subprocess.TimeoutExpired:
            if attempt >= 3:
                return None
            time.sleep(min(5 * attempt, 15))
            continue
        if proc.returncode == 0:
            return proc.stdout.strip() or None
        stderr = (proc.stderr or "").strip()
        if attempt >= 3 or not stderr:
            return None
        if any(marker in stderr.lower() for marker in ("timed out", "timeout", "temporarily unavailable")):
            time.sleep(min(5 * attempt, 15))
            continue
        return None
    return None


def load_refresh_token(
    *,
    account_name: str | None = None,
    credentials_path: str | Path | None = None,
) -> str | None:
    use_local_credentials = credentials_path is not None or account_name in (None, "rheeger")
    if use_local_credentials:
        local_token = load_local_refresh_token(credentials_path)
        if local_token:
            return local_token
    if not account_name or account_name not in ACCOUNTS:
        return None

    account = ACCOUNTS[account_name]
    token_env = str(account.get("token_env", "")).strip()
    if token_env:
        value = os.environ.get(token_env, "").strip()
        if value:
            _write_cached_refresh_token(value, account_name=account_name)
            return value

    token_ref = str(account.get("token_op_ref", "")).strip()
    if token_ref:
        cached_value = _read_cached_refresh_token(account_name=account_name, token_ref=token_ref)
        if cached_value:
            return cached_value
        op_value = _safe_op_read(token_ref)
        if op_value:
            _write_cached_refresh_token(op_value, account_name=account_name, token_ref=token_ref)
            return op_value
    return None


def mint_access_token(
    *,
    refresh_token: str,
    client_config: dict[str, str],
    scopes: Sequence[str] | None = None,
) -> dict:
    data: dict[str, str] = {
        "client_id": client_config["client_id"],
        "client_secret": client_config["client_secret"],
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    resolved_scopes = _dedupe_preserve_order(scopes or [])
    if resolved_scopes:
        data["scope"] = " ".join(resolved_scopes)
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(client_config.get("token_uri", DEFAULT_TOKEN_URI), data=encoded)
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if "error" in payload:
        description = payload.get("error_description", payload["error"])
        raise RuntimeError(f"Token refresh failed: {description}")
    return payload


class _OAuthCallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        if "code" in params:
            self.server.auth_code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h2>Authorization successful.</h2><p>You can close this tab.</p></body></html>"
            )
        elif "error" in params:
            self.server.auth_code = None
            self.server.auth_error = params.get("error", ["unknown"])[0]
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                f"<html><body><h2>Authorization failed: {self.server.auth_error}</h2></body></html>".encode()
            )
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.server.got_callback = True

    def log_message(self, format, *args):  # noqa: A003
        pass


def exchange_authorization_code(
    *,
    code: str,
    client_config: dict[str, str],
    redirect_uri: str,
) -> dict:
    data = urllib.parse.urlencode(
        {
            "code": code,
            "client_id": client_config["client_id"],
            "client_secret": client_config["client_secret"],
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
    ).encode("utf-8")
    req = urllib.request.Request(client_config.get("token_uri", DEFAULT_TOKEN_URI), data=data)
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if "error" in payload:
        description = payload.get("error_description", payload["error"])
        raise RuntimeError(f"Authorization-code exchange failed: {description}")
    return payload


def write_local_credentials(payload: dict, credentials_path: str | Path | None = None) -> Path:
    path = Path(credentials_path) if credentials_path is not None else DEFAULT_LOCAL_CREDENTIALS
    _write_json_atomic(path, payload)
    return path


def mint_refresh_token_via_browser(
    *,
    client_config: dict[str, str],
    scopes: Sequence[str],
    login_hint: str | None = None,
    open_browser: bool = True,
) -> dict:
    redirect_uri = str(client_config.get("redirect_uri", "")).strip()
    if not redirect_uri:
        raise RuntimeError("OAuth client config is missing redirect_uris")
    parsed_redirect = urlparse(redirect_uri)
    port = parsed_redirect.port or 3000

    auth_params = {
        "client_id": client_config["client_id"],
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(_dedupe_preserve_order(scopes)),
        "access_type": "offline",
        "prompt": "consent",
    }
    if login_hint:
        auth_params["login_hint"] = login_hint
    auth_url = f"https://accounts.google.com/o/oauth2/auth?{urlencode(auth_params)}"

    server = HTTPServer(("localhost", port), _OAuthCallbackHandler)
    server.auth_code = None
    server.auth_error = None
    server.got_callback = False
    print(auth_url, flush=True)
    if open_browser:
        try:
            webbrowser.open(auth_url)
        except Exception:
            pass

    try:
        while not server.got_callback:
            server.handle_request()
    finally:
        server.server_close()

    if not server.auth_code:
        raise RuntimeError(f"Authorization failed: {getattr(server, 'auth_error', 'unknown')}")
    payload = exchange_authorization_code(
        code=server.auth_code,
        client_config=client_config,
        redirect_uri=redirect_uri,
    )
    if not payload.get("refresh_token"):
        raise RuntimeError(
            "No refresh token returned. Revoke prior access for this OAuth client and retry with prompt=consent."
        )
    return payload


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{path.stem}-", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":")))
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


@dataclass
class GoogleCliTokenManager:
    refresh_token: str
    client_config: dict[str, str]
    scopes: list[str]
    cache_key: str
    refresh_margin_seconds: int = TOKEN_REFRESH_MARGIN_SECONDS
    token_env_var: str = TOKEN_ENV_VAR
    _access_token: str = ""
    _expires_at: float = 0.0
    _cache_loaded: bool = False

    @property
    def cache_path(self) -> Path:
        return TOKEN_CACHE_DIR / f"{self.cache_key}.json"

    def _load_cache(self) -> None:
        if self._cache_loaded or not self.cache_path.exists():
            self._cache_loaded = True
            return
        try:
            payload = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            self._cache_loaded = True
            return
        if payload.get("scopes") != self.scopes:
            self._cache_loaded = True
            return
        self._access_token = str(payload.get("access_token", "")).strip()
        self._expires_at = float(payload.get("expires_at", 0) or 0)
        self._cache_loaded = True

    def _write_cache(self) -> None:
        _write_json_atomic(
            self.cache_path,
            {
                "access_token": self._access_token,
                "expires_at": self._expires_at,
                "scopes": self.scopes,
            },
        )

    def get_access_token(self, *, force_refresh: bool = False) -> str:
        self._load_cache()
        now = time.time()
        if (
            not force_refresh
            and self._access_token
            and now < (self._expires_at - self.refresh_margin_seconds)
        ):
            return self._access_token

        payload = mint_access_token(
            refresh_token=self.refresh_token,
            client_config=self.client_config,
            scopes=self.scopes,
        )
        self._access_token = str(payload.get("access_token", "")).strip()
        expires_in = int(payload.get("expires_in", 3600) or 3600)
        self._expires_at = time.time() + expires_in
        self._write_cache()
        return self._access_token

    def build_env(self, env: dict[str, str] | None = None, *, force_refresh: bool = False) -> dict[str, str]:
        built = dict(env or os.environ)
        built[self.token_env_var] = self.get_access_token(force_refresh=force_refresh)
        return built

    def ensure_env(self, *, force_refresh: bool = False) -> str:
        token = self.get_access_token(force_refresh=force_refresh)
        os.environ[self.token_env_var] = token
        return token


def build_google_cli_token_manager(
    *,
    account_name: str | None = None,
    account_email: str | None = None,
    scope_profile: str | None = None,
    services: Sequence[str] | None = None,
    scopes: Sequence[str] | None = None,
    client_json_path: str | Path | None = None,
    credentials_path: str | Path | None = None,
    refresh_margin_seconds: int = TOKEN_REFRESH_MARGIN_SECONDS,
) -> GoogleCliTokenManager | None:
    if credentials_path is None:
        env_credentials_path = os.environ.get("GOOGLE_WORKSPACE_CREDENTIALS_PATH", "").strip()
        if env_credentials_path:
            credentials_path = env_credentials_path
    resolved_account_name = account_name or account_name_from_email(account_email or "")
    refresh_token = load_refresh_token(account_name=resolved_account_name, credentials_path=credentials_path)
    if not refresh_token:
        return None
    client_config = load_google_oauth_client_config(client_json_path=client_json_path)
    resolved_scopes = resolve_scopes(
        account_name=resolved_account_name,
        scope_profile=scope_profile,
        services=services,
        scopes=scopes,
    )
    cache_material = json.dumps(
        {
            "account_name": resolved_account_name,
            "account_email": account_email or "",
            "scopes": resolved_scopes,
            "credentials_path": str(credentials_path or DEFAULT_LOCAL_CREDENTIALS),
            "client_id": client_config.get("client_id", ""),
            "token_uri": client_config.get("token_uri", ""),
        },
        sort_keys=True,
    )
    cache_key = hashlib.sha256(cache_material.encode("utf-8")).hexdigest()[:16]
    return GoogleCliTokenManager(
        refresh_token=refresh_token,
        client_config=client_config,
        scopes=resolved_scopes,
        cache_key=cache_key,
        refresh_margin_seconds=refresh_margin_seconds,
    )
