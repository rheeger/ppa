"""Authenticated streamable-HTTP MCP for Tailscale clients (Arnold).

Binds loopback by default. Tailscale Serve publishes it on the personal
tailnet. HTTP serve refuses to start without a bearer token.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable, MutableMapping
from pathlib import Path
from typing import Any

from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse

from archive_engine.access import is_restricted, policy_identity, resolve_access_context
from archive_engine.contracts import AccessContext

_log = logging.getLogger("ppa.http")

DEFAULT_HTTP_HOST = "127.0.0.1"
DEFAULT_HTTP_PORT = 8765
DEFAULT_TOKEN_FILE = Path.home() / ".ppa" / "mcp-http-token"
DEFAULT_OWNER_FILE = Path.home() / ".ppa" / "mcp-http-owner.json"
HEALTH_PATHS = frozenset({"/health", "/health/"})

Scope = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Any]]
Send = Callable[[Any], Awaitable[None]]


def resolve_http_auth_token() -> str:
    """Return the configured bearer token, or empty if unset."""
    direct = os.environ.get("PPA_MCP_AUTH_TOKEN", "").strip()
    if direct:
        return direct
    file_path = os.environ.get("PPA_MCP_AUTH_TOKEN_FILE", "").strip()
    path = Path(file_path) if file_path else DEFAULT_TOKEN_FILE
    if path.is_file():
        return path.read_text(encoding="utf-8").strip()
    return ""


def http_owner_file() -> Path:
    raw = os.environ.get("PPA_MCP_HTTP_OWNER_FILE", "").strip()
    return Path(raw) if raw else DEFAULT_OWNER_FILE


def write_http_owner(*, host: str, port: int, pid: int | None = None) -> Path:
    """Record the live HTTP MCP so stdio copies can refuse to mmap the index."""

    dest = http_owner_file()
    dest.parent.mkdir(parents=True, exist_ok=True)
    payload = {"pid": int(pid or os.getpid()), "host": str(host).strip(), "port": int(port)}
    dest.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    dest.chmod(0o600)
    return dest


def clear_http_owner() -> None:
    dest = http_owner_file()
    try:
        dest.unlink()
    except FileNotFoundError:
        return


def read_http_owner() -> dict[str, Any] | None:
    dest = http_owner_file()
    if not dest.is_file():
        return None
    try:
        payload = json.loads(dest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    host = str(payload.get("host") or "").strip()
    try:
        port = int(payload.get("port") or 0)
    except (TypeError, ValueError):
        port = 0
    if not host or port <= 0:
        return None
    return payload


def http_owner_health_url(owner: dict[str, Any] | None = None) -> str:
    payload = owner if owner is not None else read_http_owner()
    if payload:
        return f"http://{payload['host']}:{int(payload['port'])}/health"
    env_url = os.environ.get("PPA_MCP_HTTP_URL", "").strip()
    if env_url:
        return env_url.rstrip("/").removesuffix("/mcp") + "/health"
    host = os.environ.get("PPA_MCP_HTTP_HOST", "").strip()
    if not host:
        return ""
    port = int(os.environ.get("PPA_MCP_HTTP_PORT") or DEFAULT_HTTP_PORT)
    return f"http://{host}:{port}/health"


def http_serving_owner_reachable(*, timeout: float = 0.4) -> str:
    """Return ``host:port`` when the dedicated HTTP MCP answers /health."""

    url = http_owner_health_url()
    if not url:
        return ""
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if int(getattr(resp, "status", 0) or 0) != 200:
                return ""
    except (OSError, urllib.error.URLError, TimeoutError, ValueError):
        return ""
    owner = read_http_owner()
    if owner:
        return f"{owner['host']}:{int(owner['port'])}"
    host = os.environ.get("PPA_MCP_HTTP_HOST", "").strip()
    port = os.environ.get("PPA_MCP_HTTP_PORT") or str(DEFAULT_HTTP_PORT)
    if host:
        return f"{host}:{port}"
    return url


def write_http_auth_token(token: str, path: Path | None = None) -> Path:
    """Persist a token with 0600 perms. Used by the install helper."""
    dest = path or DEFAULT_TOKEN_FILE
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(token.strip() + "\n", encoding="utf-8")
    dest.chmod(0o600)
    return dest


def bearer_authorized(authorization_header: str, expected: str) -> bool:
    """Constant-time compare of ``Authorization: Bearer <token>``."""
    if not expected:
        return False
    header = authorization_header.strip()
    prefix = "Bearer "
    if not header.startswith(prefix):
        return False
    provided = header[len(prefix) :].strip()
    if not provided or len(provided) != len(expected):
        return False
    return hmac.compare_digest(provided, expected)


class BearerAuthASGI:
    """ASGI wrapper that requires a bearer token except on ``/health``."""

    def __init__(self, app: Any, token: str, public_paths: frozenset[str] = HEALTH_PATHS) -> None:
        self.app = app
        self._token = token
        self._public = public_paths

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self.app(scope, receive, send)
            return
        if scope["type"] == "http":
            path = str(scope.get("path") or "")
            if path in self._public:
                await self.app(scope, receive, send)
                return
            headers = {
                key.decode("latin-1").lower(): value.decode("latin-1") for key, value in scope.get("headers") or []
            }
            if not bearer_authorized(headers.get("authorization", ""), self._token):
                response = JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


def configure_http_transport(mcp: Any, *, host: str, port: int) -> None:
    """Point the existing FastMCP instance at loopback HTTP without DNS-rebinding."""
    mcp.settings.host = host
    mcp.settings.port = port
    # Tailscale Serve forwards Host: <magicdns>. Bearer auth is the control.
    mcp.settings.transport_security = TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    )


def bind_http_access_context(*, archive_id: str | None = None) -> AccessContext:
    """Resolve one AccessContext for the HTTP process. Invalid policy fails closed."""

    aid = (archive_id or os.environ.get("PPA_ARCHIVE_ID") or "http").strip() or "http"
    access = resolve_access_context(aid)
    if access.deny and access.deny_reason:
        raise RuntimeError(access.deny_reason)
    _log.info(
        "http_mcp_access policy=%s principal=%s profile=%s restricted=%s",
        policy_identity(access),
        access.principal,
        access.profile,
        is_restricted(access),
    )
    return access


def run_http(mcp: Any, *, host: str, port: int, token: str) -> None:
    """Serve streamable HTTP with bearer auth. Blocks."""
    if not token:
        raise RuntimeError(
            f"HTTP MCP requires PPA_MCP_AUTH_TOKEN or a token file at {DEFAULT_TOKEN_FILE} (or PPA_MCP_AUTH_TOKEN_FILE)"
        )
    bind_http_access_context()

    import anyio
    import uvicorn
    from starlette.requests import Request
    from starlette.responses import JSONResponse as StarletteJSON

    @mcp.custom_route("/health", methods=["GET"])
    async def health(_request: Request) -> StarletteJSON:
        return StarletteJSON({"ok": True, "service": "ppa-mcp"})

    configure_http_transport(mcp, host=host, port=port)
    inner = mcp.streamable_http_app()
    app = BearerAuthASGI(inner, token=token)
    owner_path = write_http_owner(host=host, port=port)
    _log.info(
        "http_mcp_listen host=%s port=%d path=/mcp health=/health owner=%s",
        host,
        port,
        owner_path,
    )

    async def _serve() -> None:
        config = uvicorn.Config(app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)
        try:
            await server.serve()
        finally:
            clear_http_owner()

    try:
        anyio.run(_serve)
    finally:
        clear_http_owner()
