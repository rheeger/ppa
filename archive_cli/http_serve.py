"""Authenticated streamable-HTTP MCP for Tailscale clients (Arnold).

Binds loopback by default. Tailscale Serve publishes it on the personal
tailnet. HTTP serve refuses to start without a bearer token.
"""

from __future__ import annotations

import hmac
import logging
import os
from collections.abc import Awaitable, Callable, MutableMapping
from pathlib import Path
from typing import Any

from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse

_log = logging.getLogger("ppa.http")

DEFAULT_HTTP_HOST = "127.0.0.1"
DEFAULT_HTTP_PORT = 8765
DEFAULT_TOKEN_FILE = Path.home() / ".ppa" / "mcp-http-token"
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


def run_http(mcp: Any, *, host: str, port: int, token: str) -> None:
    """Serve streamable HTTP with bearer auth. Blocks."""
    if not token:
        raise RuntimeError(
            f"HTTP MCP requires PPA_MCP_AUTH_TOKEN or a token file at {DEFAULT_TOKEN_FILE} (or PPA_MCP_AUTH_TOKEN_FILE)"
        )

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
    _log.info("http_mcp_listen host=%s port=%d path=/mcp health=/health", host, port)

    async def _serve() -> None:
        config = uvicorn.Config(app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()

    anyio.run(_serve)
