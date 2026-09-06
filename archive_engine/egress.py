"""Provider egress policy attached to ``runtime.providers``.

Destination *capabilities* live here. Provider *construction* stays in the
existing registries (``archive_cli.providers.PROVIDER_REGISTRY``,
``archive_vault.llm_provider.PROVIDER_REGISTRY``, ``get_embedding_provider``).
This module does not invent a second provider registry.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator
from urllib import request
from urllib.parse import urljoin, urlparse

from archive_engine.access import is_restricted, source_allowed
from archive_engine.contracts import AccessContext
from archive_engine.errors import EgressDeniedError

LOCAL_ONLY = "local-only"
RESTRICTED = "restricted"
UNRESTRICTED = "unrestricted"
EGRESS_MODES = frozenset({LOCAL_ONLY, RESTRICTED, UNRESTRICTED})

LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0", "[::1]"})
FIRECRAWL_CANONICAL_URL = "https://api.firecrawl.dev/"


@dataclass(frozen=True, slots=True)
class Destination:
    """Capability record for a named provider destination."""

    name: str
    transport: str  # none | http
    locality: str  # local | remote


# Capabilities for names already used by embedding / LLM / OCR ports.
DESTINATIONS: dict[str, Destination] = {
    "hash": Destination("hash", transport="none", locality="local"),
    "ollama": Destination("ollama", transport="http", locality="local"),
    "openai": Destination("openai", transport="http", locality="remote"),
    "gemini": Destination("gemini", transport="http", locality="remote"),
    "firecrawl": Destination("firecrawl", transport="http", locality="remote"),
    "openclaw": Destination("openclaw", transport="none", locality="local"),
}

_HOST_DESTINATIONS: tuple[tuple[str, str], ...] = (
    ("api.openai.com", "openai"),
    ("openai.com", "openai"),
    ("generativelanguage.googleapis.com", "gemini"),
    ("googleapis.com", "gemini"),
    ("api.firecrawl.dev", "firecrawl"),
    ("firecrawl.dev", "firecrawl"),
)


@dataclass(frozen=True, slots=True)
class EgressPolicy:
    mode: str
    revision: str
    allowed_sources: tuple[str, ...] = ()
    allowed_domains: tuple[str, ...] = ()
    deny: bool = False


@dataclass(frozen=True, slots=True)
class EgressEvent:
    phase: str
    destination: str
    url: str
    allowed: bool
    mode: str
    sources: tuple[str, ...] = ()
    domains: tuple[str, ...] = ()
    body: str = ""
    reason: str = ""


_ACCESS: ContextVar[AccessContext | None] = ContextVar("ppa_egress_access", default=None)
_SOURCES: ContextVar[tuple[str, ...]] = ContextVar("ppa_egress_sources", default=())
_DOMAINS: ContextVar[tuple[str, ...]] = ContextVar("ppa_egress_domains", default=())
_CAPTURE: ContextVar[list[EgressEvent] | None] = ContextVar("ppa_egress_capture", default=None)


def current_access() -> AccessContext | None:
    return _ACCESS.get()


def current_payload() -> tuple[tuple[str, ...], tuple[str, ...]]:
    return _SOURCES.get(), _DOMAINS.get()


@contextmanager
def egress_scope(
    access: AccessContext | None,
    *,
    sources: tuple[str, ...] | list[str] = (),
    domains: tuple[str, ...] | list[str] = (),
) -> Iterator[None]:
    """Bind access + payload labels for nested HTTP helpers."""

    tok_a = _ACCESS.set(access)
    tok_s = _SOURCES.set(tuple(str(item) for item in sources if str(item).strip()))
    tok_d = _DOMAINS.set(tuple(str(item) for item in domains if str(item).strip()))
    try:
        yield
    finally:
        _ACCESS.reset(tok_a)
        _SOURCES.reset(tok_s)
        _DOMAINS.reset(tok_d)


@contextmanager
def capture_egress() -> Iterator[list[EgressEvent]]:
    """Record authorize / transport / redirect events for tests."""

    events: list[EgressEvent] = []
    token = _CAPTURE.set(events)
    try:
        yield events
    finally:
        _CAPTURE.reset(token)


def _record(event: EgressEvent) -> None:
    bucket = _CAPTURE.get()
    if bucket is not None:
        bucket.append(event)


def destination_spec(name: str) -> Destination | None:
    key = (name or "").strip().lower()
    if not key:
        return None
    return DESTINATIONS.get(key)


def is_loopback_host(host: str) -> bool:
    cleaned = (host or "").strip().lower().rstrip(".")
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    return cleaned in LOOPBACK_HOSTS or cleaned.startswith("127.")


def _with_scheme(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        return f"http://{raw}"
    return raw


def hostname_of(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(_with_scheme(url))
    return (parsed.hostname or "").strip().lower()


def is_remote_url(url: str) -> bool:
    host = hostname_of(url)
    if not host:
        return True
    return not is_loopback_host(host)


def destination_from_url(url: str) -> str:
    host = hostname_of(url)
    if is_loopback_host(host):
        return "ollama"
    for suffix, name in _HOST_DESTINATIONS:
        if host == suffix or host.endswith(f".{suffix}"):
            return name
    return "unknown"


def resolve_egress_policy(
    access: AccessContext | None = None,
    *,
    environ: dict[str, str] | None = None,
) -> EgressPolicy:
    """Resolve mode from AccessContext + operator env. Entry-point only."""

    env = os.environ if environ is None else environ
    ctx = access if access is not None else current_access()
    revision = ""
    if ctx is not None:
        revision = (ctx.egress_policy_revision or "").strip()
    mode_env = (env.get("PPA_EGRESS_MODE") or "").strip().lower().replace("_", "-")
    rev = revision.lower()
    if mode_env in EGRESS_MODES:
        mode = mode_env
    elif "local-only" in rev or "local_only" in rev:
        mode = LOCAL_ONLY
    elif "unrestricted" in rev:
        mode = UNRESTRICTED
    elif rev == RESTRICTED or rev.endswith("-restricted"):
        mode = RESTRICTED
    elif ctx is not None and is_restricted(ctx):
        mode = RESTRICTED
    else:
        mode = UNRESTRICTED
    allowed_sources = tuple(ctx.allowed_sources) if ctx is not None else ()
    allowed_domains = tuple(ctx.allowed_domains) if ctx is not None else ()
    deny = bool(ctx.deny) if ctx is not None else False
    return EgressPolicy(
        mode=mode,
        revision=revision or "unspecified",
        allowed_sources=allowed_sources,
        allowed_domains=allowed_domains,
        deny=deny,
    )


def _deny(policy: EgressPolicy, destination: str, url: str, reason: str, *, sources: tuple[str, ...] = (), domains: tuple[str, ...] = ()) -> None:
    _record(
        EgressEvent(
            phase="authorize",
            destination=destination,
            url=url,
            allowed=False,
            mode=policy.mode,
            sources=sources,
            domains=domains,
            reason=reason,
        )
    )
    raise EgressDeniedError(reason)


def authorize_destination(
    name: str,
    *,
    access: AccessContext | None = None,
    sources: tuple[str, ...] | list[str] = (),
    domains: tuple[str, ...] | list[str] = (),
    url: str = "",
) -> Destination:
    """Permit a named destination before any provider work."""

    policy = resolve_egress_policy(access)
    payload_sources = tuple(str(item) for item in sources if str(item).strip()) or current_payload()[0]
    payload_domains = tuple(str(item) for item in domains if str(item).strip()) or current_payload()[1]
    dest = destination_spec(name)
    if dest is None:
        _deny(policy, name or "unknown", url, f"unknown provider destination {name or 'unknown'!r}", sources=payload_sources, domains=payload_domains)
    assert dest is not None
    if policy.deny:
        _deny(policy, dest.name, url, policy.revision or "access denied", sources=payload_sources, domains=payload_domains)
    if dest.transport == "none":
        if dest.name != "hash":
            _deny(
                policy,
                dest.name,
                url,
                f"destination {dest.name} has no authorized transport",
                sources=payload_sources,
                domains=payload_domains,
            )
        if url:
            _deny(policy, dest.name, url, f"destination {dest.name} does not use HTTP", sources=payload_sources, domains=payload_domains)
        _record(
            EgressEvent(
                phase="authorize",
                destination=dest.name,
                url="",
                allowed=True,
                mode=policy.mode,
                sources=payload_sources,
                domains=payload_domains,
            )
        )
        return dest
    if dest.locality == "remote" and policy.mode == LOCAL_ONLY:
        _deny(policy, dest.name, url, f"local-only policy blocks remote destination {dest.name}", sources=payload_sources, domains=payload_domains)
    if dest.locality == "local" and url and is_remote_url(url):
        _deny(policy, dest.name, url, f"local destination {dest.name} cannot use remote URL", sources=payload_sources, domains=payload_domains)
    if policy.mode == LOCAL_ONLY and url and is_remote_url(url):
        _deny(policy, dest.name, url, "local-only policy blocks remote URL", sources=payload_sources, domains=payload_domains)
    if dest.locality == "remote" and (policy.mode == RESTRICTED or (access is not None and is_restricted(access))):
        ctx = access if access is not None else current_access()
        if not payload_sources:
            _deny(
                policy,
                dest.name,
                url,
                f"restricted egress requires explicit payload sources for remote destination {dest.name}",
                sources=payload_sources,
                domains=payload_domains,
            )
        if payload_sources:
            if ctx is None:
                _deny(policy, dest.name, url, "restricted egress missing access context", sources=payload_sources, domains=payload_domains)
            for source in payload_sources:
                if not source_allowed(ctx.allowed_sources, source):
                    _deny(
                        policy,
                        dest.name,
                        url,
                        f"source {source!r} is not permitted for destination {dest.name}",
                        sources=payload_sources,
                        domains=payload_domains,
                    )
        if payload_domains and ctx is not None and ctx.allowed_domains:
            allowed = {item.lower() for item in ctx.allowed_domains}
            for domain in payload_domains:
                if domain.lower() not in allowed:
                    _deny(
                        policy,
                        dest.name,
                        url,
                        f"domain {domain!r} is not permitted for destination {dest.name}",
                        sources=payload_sources,
                        domains=payload_domains,
                    )
    _record(
        EgressEvent(
            phase="authorize",
            destination=dest.name,
            url=url,
            allowed=True,
            mode=policy.mode,
            sources=payload_sources,
            domains=payload_domains,
        )
    )
    return dest


def authorize_request(
    *,
    destination: str,
    url: str,
    access: AccessContext | None = None,
    sources: tuple[str, ...] | list[str] = (),
    domains: tuple[str, ...] | list[str] = (),
) -> Destination:
    """Authorize destination + URL before ``urlopen``."""

    return authorize_destination(destination, access=access, sources=sources, domains=domains, url=url)


def authorize_redirect(
    from_url: str,
    location: str,
    *,
    destination: str,
    access: AccessContext | None = None,
    sources: tuple[str, ...] | list[str] = (),
    domains: tuple[str, ...] | list[str] = (),
) -> None:
    """Refuse local-only / local destinations following a remote Location."""

    policy = resolve_egress_policy(access)
    dest = destination_spec(destination)
    target = urljoin(from_url, location)
    payload_sources = tuple(sources) or current_payload()[0]
    payload_domains = tuple(domains) or current_payload()[1]
    remote = is_remote_url(target)
    if dest is not None and dest.locality == "local" and remote:
        _record(
            EgressEvent(
                phase="redirect",
                destination=destination,
                url=target,
                allowed=False,
                mode=policy.mode,
                sources=payload_sources,
                domains=payload_domains,
                reason="local destination cannot follow remote redirect",
            )
        )
        raise EgressDeniedError("local destination cannot follow remote redirect")
    if policy.mode == LOCAL_ONLY and remote:
        _record(
            EgressEvent(
                phase="redirect",
                destination=destination,
                url=target,
                allowed=False,
                mode=policy.mode,
                sources=payload_sources,
                domains=payload_domains,
                reason="local-only policy cannot follow remote redirect",
            )
        )
        raise EgressDeniedError("local-only policy cannot follow remote redirect")
    authorize_request(destination=destination, url=target, access=access, sources=sources, domains=domains)
    _record(
        EgressEvent(
            phase="redirect",
            destination=destination,
            url=target,
            allowed=True,
            mode=policy.mode,
            sources=payload_sources,
            domains=payload_domains,
        )
    )


def _request_url(req: request.Request | str) -> str:
    if isinstance(req, request.Request):
        return req.full_url
    return str(req)


class _PolicyRedirectHandler(request.HTTPRedirectHandler):
    def __init__(self, *, destination: str, access: AccessContext | None, sources: tuple[str, ...], domains: tuple[str, ...]):
        super().__init__()
        self._destination = destination
        self._access = access
        self._sources = sources
        self._domains = domains

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        authorize_redirect(
            req.full_url,
            newurl,
            destination=self._destination,
            access=self._access,
            sources=self._sources,
            domains=self._domains,
        )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _transport_body(req: request.Request | str) -> str:
    if not isinstance(req, request.Request) or req.data is None:
        return ""
    raw = req.data if isinstance(req.data, bytes) else str(req.data).encode("utf-8")
    return raw.decode("utf-8", errors="replace")


def policy_urlopen(
    req: request.Request | str,
    timeout: float = 60.0,
    *,
    destination: str,
    access: AccessContext | None = None,
    sources: tuple[str, ...] = (),
    domains: tuple[str, ...] = (),
) -> Any:
    """``urlopen`` that re-checks policy on every redirect."""

    payload_sources = sources or current_payload()[0]
    payload_domains = domains or current_payload()[1]
    ctx = access if access is not None else current_access()
    url = _request_url(req)
    _record(
        EgressEvent(
            phase="transport",
            destination=destination,
            url=url,
            allowed=True,
            mode=resolve_egress_policy(ctx).mode,
            sources=payload_sources,
            domains=payload_domains,
            body=_transport_body(req),
        )
    )
    opener = request.build_opener(
        _PolicyRedirectHandler(destination=destination, access=ctx, sources=payload_sources, domains=payload_domains)
    )
    return opener.open(req, timeout=timeout)


def guarded_urlopen(
    req: request.Request | str,
    timeout: float = 60.0,
    *,
    destination: str,
    urlopen: Any = None,
    access: AccessContext | None = None,
    sources: tuple[str, ...] | list[str] = (),
    domains: tuple[str, ...] | list[str] = (),
) -> Any:
    """Authorize, then open. Injected ``urlopen`` keeps existing test patches working."""

    url = _request_url(req)
    payload_sources = tuple(sources) or current_payload()[0]
    payload_domains = tuple(domains) or current_payload()[1]
    ctx = access if access is not None else current_access()
    authorize_request(destination=destination, url=url, access=ctx, sources=payload_sources, domains=payload_domains)
    if urlopen is not None:
        _record(
            EgressEvent(
                phase="transport",
                destination=destination,
                url=url,
                allowed=True,
                mode=resolve_egress_policy(ctx).mode,
                sources=payload_sources,
                domains=payload_domains,
                body=_transport_body(req),
            )
        )
        return urlopen(req, timeout=timeout)
    return policy_urlopen(
        req,
        timeout=timeout,
        destination=destination,
        access=ctx,
        sources=payload_sources,
        domains=payload_domains,
    )
