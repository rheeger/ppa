"""Versioned instance configuration (P09-B).

Precedence is CLI override > environment > explicit instance file > defaults.
CWD discovery is a documented legacy adapter and is skipped when an instance
directory or config path is bound. Secret values are never stored on the
serializable model.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Literal
from urllib.parse import urlparse

from archive_engine.contracts import ArchiveIdentity, EmbeddingSpec
from archive_engine.errors import ConfigError

CONFIG_SCHEMA_VERSION = 1
INSTANCE_CONFIG_FILENAMES = ("ppa.yml", "ppa.yaml", "ppa.json")
INSTANCE_META_FILENAME = "ppa-instance.json"
LEGACY_VAULT_CONFIG = "ppa-config.json"
CANDIDATE_INSTANCE_FILENAME = "ppa-instance.v1.candidate.json"

Origin = Literal["cli", "env", "file", "legacy", "default"]

CRITICAL_TOP_LEVEL = frozenset(
    {
        "schema_version",
        "identity",
        "storage",
        "engine",
        "embeddings",
        "access",
        "egress",
    }
)
KNOWN_TOP_LEVEL = CRITICAL_TOP_LEVEL | frozenset(
    {
        "entity",
        "sources",
        "enrichment",
        "rerank",
        "maintenance",
        "retrieval",
        "scopes",
        "secret_refs",
        "vault_tuning",
    }
)
VALID_ENTITY_TYPES = frozenset({"person", "organization", "household", "admin", "other"})
_SECRET_KEY_RE = re.compile(r"(secret|password|token|api[_-]?key|credential|dsn)$", re.I)
_SECRET_VALUE_RE = re.compile(r"(sk-|ghp_|xox[baprs]-|postgresql://\S+:\S+@)", re.I)


@dataclass(frozen=True)
class FieldOrigin:
    field: str
    origin: Origin
    source: str


@dataclass(frozen=True)
class SecretRef:
    name: str
    provider: str = "env"

    def to_payload(self) -> dict[str, str]:
        return {"name": self.name, "provider": self.provider}


@dataclass(frozen=True)
class EntitySpec:
    name: str = ""
    entity_type: str = "person"


@dataclass(frozen=True)
class StorageBindings:
    vault_path: str
    index_schema: str
    index_dsn_ref: SecretRef
    serving_index_path: str
    query_embed_cache_path: str


@dataclass(frozen=True)
class EngineCompat:
    native: str = "rust"
    format_compatibility: str = "serving-index-v1"


@dataclass(frozen=True)
class SourceAccount:
    source_key: str
    account_scope: str = ""
    enabled: bool = True


@dataclass(frozen=True)
class AccessPolicySpec:
    principal: str
    profile: str
    allowed_tools: tuple[str, ...] = ()
    allowed_sources: tuple[str, ...] = ()
    allowed_domains: tuple[str, ...] = ()
    egress_policy_revision: str = "p05b-unspecified"


@dataclass(frozen=True)
class MaintenanceBudgets:
    rebuild_workers: int | None = None
    rebuild_batch_size: int | None = None


@dataclass(frozen=True)
class RetrievalSettings:
    default_limit: int = 20
    default_hops: int = 2
    default_mode: str = "hybrid"


@dataclass(frozen=True)
class VaultTuning:
    merge_threshold: int = 90
    conflict_threshold: int = 75
    fuzzy_name_threshold: float = 85.0
    finance_min_amount: float = 20.0


@dataclass(frozen=True)
class InstanceConfig:
    schema_version: int
    identity: ArchiveIdentity
    entity: EntitySpec
    storage: StorageBindings
    engine: EngineCompat
    embeddings: EmbeddingSpec
    access: AccessPolicySpec
    sources: tuple[SourceAccount, ...] = ()
    enrichment: dict[str, Any] = field(default_factory=dict)
    rerank: dict[str, Any] = field(default_factory=dict)
    maintenance: MaintenanceBudgets = field(default_factory=MaintenanceBudgets)
    retrieval: RetrievalSettings = field(default_factory=RetrievalSettings)
    vault_tuning: VaultTuning = field(default_factory=VaultTuning)
    scopes: tuple[dict[str, Any], ...] = ()
    secret_refs: tuple[SecretRef, ...] = ()
    origins: tuple[FieldOrigin, ...] = ()

    def fingerprint(self) -> str:
        material = json.dumps(
            {
                "schema_version": self.schema_version,
                "archive_id": self.identity.archive_id,
                "root": self.identity.canonical_root,
                "binding": self.identity.schema_binding,
                "entity": [self.entity.name, self.entity.entity_type],
                "schema": self.storage.index_schema,
                "serving": self.storage.serving_index_path,
                "embed_cache": self.storage.query_embed_cache_path,
                "engine": [self.engine.native, self.engine.format_compatibility],
                "embeddings": self.embeddings.to_payload(),
                "access": [
                    self.access.principal,
                    self.access.profile,
                    list(self.access.allowed_tools),
                    list(self.access.allowed_sources),
                    list(self.access.allowed_domains),
                    self.access.egress_policy_revision,
                ],
                "retrieval": [self.retrieval.default_limit, self.retrieval.default_hops, self.retrieval.default_mode],
                "scopes": list(self.scopes),
            },
            sort_keys=True,
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    def origin_map(self) -> dict[str, FieldOrigin]:
        return {item.field: item for item in self.origins}


_BOUND: ContextVar[InstanceConfig | None] = ContextVar("ppa_instance_config", default=None)
_SECRETS: ContextVar[dict[str, str]] = ContextVar("ppa_instance_secrets", default={})


def current_instance_config() -> InstanceConfig | None:
    return _BOUND.get()


def current_secret_values() -> dict[str, str]:
    return dict(_SECRETS.get())


@contextmanager
def bind_instance_config(
    config: InstanceConfig, *, secrets: Mapping[str, str] | None = None
) -> Iterator[InstanceConfig]:
    token = _BOUND.set(config)
    secret_token = _SECRETS.set(dict(secrets or {}))
    try:
        yield config
    finally:
        _BOUND.reset(token)
        _SECRETS.reset(secret_token)


def _as_mapping(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConfigError(f"{field_name} must be an object")
    return dict(value)


def _as_str(value: object) -> str:
    return str(value).strip() if value is not None else ""


def _env(environ: Mapping[str, str], name: str) -> str:
    return str(environ.get(name, "") or "").strip()


def _load_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")
    text = path.read_text(encoding="utf-8")
    if path.suffix in {".yml", ".yaml"}:
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover
            raise ConfigError("YAML support is unavailable") from exc
        payload = yaml.safe_load(text) or {}
    else:
        payload = json.loads(text or "{}")
    if not isinstance(payload, dict):
        raise ConfigError(f"Archive config must be an object: {path}")
    return payload


def _discover_legacy_cwd() -> Path | None:
    cwd = Path.cwd()
    for filename in INSTANCE_CONFIG_FILENAMES:
        candidate = cwd / filename
        if candidate.exists():
            return candidate.resolve()
    return None


def _instance_file(instance_dir: Path) -> Path | None:
    for filename in (INSTANCE_META_FILENAME, *INSTANCE_CONFIG_FILENAMES):
        for candidate in (instance_dir / filename, instance_dir / "_meta" / filename):
            if candidate.exists():
                return candidate.resolve()
    return None


def _reject_unknown_critical(payload: Mapping[str, Any], *, path: Path) -> None:
    unknown = [key for key in payload if key not in KNOWN_TOP_LEVEL]
    critical = [key for key in unknown if key in CRITICAL_TOP_LEVEL or _SECRET_KEY_RE.search(str(key))]
    if critical:
        raise ConfigError(f"unknown critical keys in {path}: {', '.join(sorted(critical))}")
    unsafe = [key for key in unknown if str(key) in {"dsn", "index_dsn", "openai_api_key", "api_key"}]
    if unsafe:
        raise ConfigError(f"unknown critical keys in {path}: {', '.join(sorted(unsafe))}")
    if unknown:
        raise ConfigError(f"unknown keys in {path}: {', '.join(sorted(unknown))}")


def _pick(
    field_name: str,
    *,
    cli: object | None,
    env_value: str,
    env_name: str,
    file_value: object | None,
    default: object,
    origins: list[FieldOrigin],
    file_label: str,
) -> object:
    if cli is not None and cli != "":
        origins.append(FieldOrigin(field_name, "cli", "cli_overrides"))
        return cli
    if env_value:
        origins.append(FieldOrigin(field_name, "env", env_name))
        return env_value
    if file_value is not None and file_value != "":
        origins.append(FieldOrigin(field_name, "file", file_label))
        return file_value
    origins.append(FieldOrigin(field_name, "default", "defaults"))
    return default


def dsn_descriptor(index_dsn: str | None) -> str:
    raw = (index_dsn or "").strip()
    if not raw:
        return "no-dsn"
    parsed = urlparse(raw)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    db = (parsed.path or "/").lstrip("/") or "postgres"
    return f"{host}:{port}/{db}"


def redact_value(field_name: str, value: object) -> object:
    if isinstance(value, SecretRef):
        return value.to_payload()
    if isinstance(value, Mapping):
        return {str(key): redact_value(str(key), item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_value(field_name, item) for item in value]
    text = "" if value is None else str(value)
    if _SECRET_KEY_RE.search(field_name) or _SECRET_VALUE_RE.search(text):
        if "postgresql://" in text.lower() or text.startswith("postgres://"):
            return f"redacted:{dsn_descriptor(text)}"
        return "redacted"
    return value


def _parse_entity_type(raw: object) -> str:
    value = _as_str(raw).lower() or "person"
    if value not in VALID_ENTITY_TYPES:
        raise ConfigError(f"invalid entity_type {value!r}")
    return value


def _csv(raw: object) -> tuple[str, ...]:
    if raw is None or raw == "":
        return ()
    if isinstance(raw, (list, tuple)):
        return tuple(str(item).strip() for item in raw if str(item).strip())
    return tuple(part.strip() for part in str(raw).split(",") if part.strip())


def _validate_profile(profile: str) -> str:
    from archive_engine.access import VALID_TOOL_PROFILES

    cleaned = profile.strip().lower() or "full"
    if cleaned in {"trusted-local", "full"} or cleaned in VALID_TOOL_PROFILES:
        return cleaned
    raise ConfigError(f"invalid access profile {profile!r}")


def _embedding_spec(
    *,
    provider: str,
    model: str,
    revision: str,
    dimension: int,
    metric: str,
    normalization: str,
    chunk_schema: str,
) -> EmbeddingSpec:
    if dimension <= 0:
        raise ConfigError("embeddings.dimension must be a positive integer")
    if not provider or not model:
        raise ConfigError("embeddings provider and model are required")
    return EmbeddingSpec(
        provider_namespace=provider,
        model=model,
        model_revision=str(revision),
        dimension=int(dimension),
        metric=metric or "cosine",
        normalization=normalization or "none",
        chunk_schema=chunk_schema,
    )


def _schema_binding(index_schema: str, schema_version: int) -> str:
    schema = (index_schema or "ppa").strip() or "ppa"
    return f"warehouse:{schema}+index_schema_v{schema_version}"


def archive_identity_for(
    vault: Path | str,
    *,
    schema_binding: str,
    archive_id: str = "",
) -> ArchiveIdentity:
    """Same identity formula as ``archive_cli.engine_factory.resolve_archive_identity``."""

    root = Path(vault).expanduser().resolve()
    aid = archive_id.strip()
    if not aid:
        material = f"{root.as_posix()}\n{schema_binding}".encode()
        aid = hashlib.sha256(material).hexdigest()
    return ArchiveIdentity(archive_id=aid, canonical_root=str(root), schema_binding=schema_binding)


def _instance_cache_path(vault: Path, archive_id: str, filename: str) -> str:
    suffix = hashlib.sha256(archive_id.encode("utf-8")).hexdigest()[:12]
    stem, ext = filename.split(".", 1)
    return str(vault / "_meta" / f"{stem}-{suffix}.{ext}")


def load_legacy_vault_tuning(vault: Path) -> tuple[VaultTuning, Path | None]:
    from archive_vault.config import as_vault_tuning, load_config

    path = Path(vault) / "_meta" / LEGACY_VAULT_CONFIG
    if not path.exists():
        return VaultTuning(), None
    mapped = as_vault_tuning(load_config(vault))
    return VaultTuning(**mapped), path


def write_instance_candidate(vault: Path, payload: Mapping[str, Any]) -> Path:
    meta = Path(vault) / "_meta"
    meta.mkdir(parents=True, exist_ok=True)
    candidate = meta / CANDIDATE_INSTANCE_FILENAME
    candidate.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return candidate


def rollback_instance_candidate(vault: Path) -> bool:
    candidate = Path(vault) / "_meta" / CANDIDATE_INSTANCE_FILENAME
    if not candidate.exists():
        return False
    candidate.unlink()
    return True


def resolve_instance_config(
    *,
    cli_overrides: Mapping[str, Any] | None = None,
    environ: Mapping[str, str] | None = None,
    config_path: str | Path | None = None,
    instance_dir: str | Path | None = None,
    allow_cwd_discovery: bool = True,
    schema_version_hint: int = 9,
    chunk_schema_hint: int = 6,
    vector_dimension_hint: int = 1536,
) -> InstanceConfig:
    """Resolve one instance. Explicit path/dir binding disables cwd discovery."""

    env = os.environ if environ is None else environ
    cli = dict(cli_overrides or {})
    origins: list[FieldOrigin] = []
    secrets: dict[str, str] = {}

    bound_dir = Path(instance_dir).expanduser().resolve() if instance_dir else None
    explicit = config_path or cli.get("config_path") or _env(env, "PPA_CONFIG_PATH")
    file_path: Path | None = Path(explicit).expanduser().resolve() if explicit else None
    if file_path is None and bound_dir is not None:
        file_path = _instance_file(bound_dir)
    cwd_used = False
    if file_path is None and bound_dir is None and allow_cwd_discovery:
        file_path = _discover_legacy_cwd()
        cwd_used = file_path is not None
        if cwd_used:
            origins.append(FieldOrigin("config_path", "legacy", "cwd"))

    payload: dict[str, Any] = {}
    file_label = "defaults"
    if file_path is not None:
        payload = _load_payload(file_path)
        file_label = str(file_path)
        origins.append(FieldOrigin("config_path", "file" if not cwd_used else "legacy", file_label))

    identity_block = _as_mapping(payload.get("identity"), field_name="identity")
    storage_block = _as_mapping(payload.get("storage"), field_name="storage")
    entity_block = _as_mapping(payload.get("entity"), field_name="entity")
    engine_block = _as_mapping(payload.get("engine"), field_name="engine")
    embed_block = _as_mapping(payload.get("embeddings"), field_name="embeddings")
    access_block = _as_mapping(payload.get("access"), field_name="access")
    retrieval_block = _as_mapping(payload.get("retrieval"), field_name="retrieval")
    maintenance_block = _as_mapping(payload.get("maintenance"), field_name="maintenance")

    if file_path is not None and int(payload.get("schema_version") or 0) >= 1:
        _reject_unknown_critical(payload, path=file_path)
        if (
            storage_block.get("index_dsn")
            or payload.get("index_dsn")
            or embed_block.get("api_key")
            or embed_block.get("openai_api_key")
        ):
            raise ConfigError("plaintext secrets are rejected; use secret_refs")

    vault_default = str(bound_dir) if bound_dir is not None else str(Path.home() / "Archive" / "vault")
    if cli.get("vault_path"):
        vault = Path(str(cli.get("vault_path"))).expanduser()
        origins.append(FieldOrigin("storage.vault_path", "cli", "cli_overrides"))
    elif bound_dir is not None:
        vault = bound_dir
        origins.append(FieldOrigin("storage.vault_path", "cli", "instance_dir"))
    else:
        vault = Path(
            str(
                _pick(
                    "storage.vault_path",
                    cli=None,
                    env_value=_env(env, "PPA_PATH"),
                    env_name="PPA_PATH",
                    file_value=storage_block.get("vault_path")
                    or payload.get("vault_path")
                    or identity_block.get("canonical_root"),
                    default=vault_default,
                    origins=origins,
                    file_label=file_label,
                )
            )
        ).expanduser()

    index_schema = str(
        _pick(
            "storage.index_schema",
            cli=cli.get("index_schema"),
            env_value=_env(env, "PPA_INDEX_SCHEMA"),
            env_name="PPA_INDEX_SCHEMA",
            file_value=storage_block.get("index_schema") or payload.get("index_schema"),
            default="ppa",
            origins=origins,
            file_label=file_label,
        )
    )

    dsn_env = _env(env, "PPA_INDEX_DSN")
    dsn_cli = _as_str(cli.get("index_dsn"))
    if dsn_cli:
        secrets["index_dsn"] = dsn_cli
        origins.append(FieldOrigin("storage.index_dsn", "cli", "cli_overrides"))
        dsn_ref = SecretRef("PPA_INDEX_DSN", provider="cli")
    elif dsn_env:
        secrets["index_dsn"] = dsn_env
        origins.append(FieldOrigin("storage.index_dsn", "env", "PPA_INDEX_DSN"))
        dsn_ref = SecretRef("PPA_INDEX_DSN", provider="env")
    elif storage_block.get("index_dsn_ref") or payload.get("index_dsn_ref"):
        raw_ref = storage_block.get("index_dsn_ref") or payload.get("index_dsn_ref")
        if isinstance(raw_ref, Mapping):
            dsn_ref = SecretRef(
                name=_as_str(raw_ref.get("name")) or "PPA_INDEX_DSN", provider=_as_str(raw_ref.get("provider")) or "env"
            )
        else:
            dsn_ref = SecretRef(name=_as_str(raw_ref) or "PPA_INDEX_DSN")
        origins.append(FieldOrigin("storage.index_dsn", "file", file_label))
    else:
        dsn_ref = SecretRef("PPA_INDEX_DSN")
        origins.append(FieldOrigin("storage.index_dsn", "default", "defaults"))

    archive_id = str(
        _pick(
            "identity.archive_id",
            cli=cli.get("archive_id"),
            env_value=_env(env, "PPA_ARCHIVE_ID"),
            env_name="PPA_ARCHIVE_ID",
            file_value=identity_block.get("archive_id"),
            default="",
            origins=origins,
            file_label=file_label,
        )
    )
    binding = _schema_binding(index_schema, schema_version_hint)
    identity = archive_identity_for(vault, schema_binding=binding, archive_id=archive_id)
    if identity_block.get("canonical_root") and Path(
        str(identity_block["canonical_root"])
    ).expanduser().resolve() != Path(identity.canonical_root):
        if not cli.get("vault_path") and bound_dir is None:
            raise ConfigError("identity.canonical_root does not match storage.vault_path")

    entity = EntitySpec(
        name=str(
            _pick(
                "entity.name",
                cli=cli.get("entity_name"),
                env_value=_env(env, "PPA_ENTITY_NAME"),
                env_name="PPA_ENTITY_NAME",
                file_value=entity_block.get("name"),
                default="",
                origins=origins,
                file_label=file_label,
            )
        ),
        entity_type=_parse_entity_type(
            _pick(
                "entity.entity_type",
                cli=cli.get("entity_type"),
                env_value=_env(env, "PPA_ENTITY_TYPE"),
                env_name="PPA_ENTITY_TYPE",
                file_value=entity_block.get("entity_type") or entity_block.get("type"),
                default="person",
                origins=origins,
                file_label=file_label,
            )
        ),
    )

    serving_default = str(Path(identity.canonical_root) / "_meta" / "rust-search-index")
    embed_cache_default = _instance_cache_path(
        Path(identity.canonical_root), identity.archive_id, "query-embed-cache.sqlite"
    )
    serving_path = str(
        _pick(
            "storage.serving_index_path",
            cli=cli.get("serving_index_path"),
            env_value=_env(env, "PPA_SERVING_INDEX_PATH"),
            env_name="PPA_SERVING_INDEX_PATH",
            file_value=storage_block.get("serving_index_path"),
            default=serving_default,
            origins=origins,
            file_label=file_label,
        )
    )
    embed_cache = str(
        _pick(
            "storage.query_embed_cache_path",
            cli=cli.get("query_embed_cache_path"),
            env_value=_env(env, "PPA_QUERY_EMBED_CACHE_PATH"),
            env_name="PPA_QUERY_EMBED_CACHE_PATH",
            file_value=storage_block.get("query_embed_cache_path"),
            default=embed_cache_default,
            origins=origins,
            file_label=file_label,
        )
    )

    native = (
        str(
            _pick(
                "engine.native",
                cli=cli.get("engine"),
                env_value=_env(env, "PPA_ENGINE"),
                env_name="PPA_ENGINE",
                file_value=engine_block.get("native"),
                default="rust",
                origins=origins,
                file_label=file_label,
            )
        ).lower()
        or "rust"
    )
    if native not in {"rust", "python"}:
        raise ConfigError(f"incompatible engine spec {native!r}")

    provider = str(
        _pick(
            "embeddings.provider",
            cli=cli.get("embedding_provider"),
            env_value=_env(env, "PPA_EMBEDDING_PROVIDER"),
            env_name="PPA_EMBEDDING_PROVIDER",
            file_value=embed_block.get("provider") or embed_block.get("provider_namespace"),
            default="hash",
            origins=origins,
            file_label=file_label,
        )
    )
    model = str(
        _pick(
            "embeddings.model",
            cli=cli.get("embedding_model"),
            env_value=_env(env, "PPA_EMBEDDING_MODEL"),
            env_name="PPA_EMBEDDING_MODEL",
            file_value=embed_block.get("model"),
            default="archive-hash-dev",
            origins=origins,
            file_label=file_label,
        )
    )
    revision = str(
        _pick(
            "embeddings.model_revision",
            cli=cli.get("embedding_version"),
            env_value=_env(env, "PPA_EMBEDDING_VERSION"),
            env_name="PPA_EMBEDDING_VERSION",
            file_value=embed_block.get("model_revision") or embed_block.get("version"),
            default="1",
            origins=origins,
            file_label=file_label,
        )
    )
    dimension_raw = _pick(
        "embeddings.dimension",
        cli=cli.get("vector_dimension"),
        env_value=_env(env, "PPA_VECTOR_DIMENSION"),
        env_name="PPA_VECTOR_DIMENSION",
        file_value=embed_block.get("dimension"),
        default=vector_dimension_hint,
        origins=origins,
        file_label=file_label,
    )
    try:
        dimension = int(dimension_raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError("incompatible embeddings.dimension") from exc
    embeddings = _embedding_spec(
        provider=provider,
        model=model,
        revision=revision,
        dimension=dimension,
        metric=_as_str(embed_block.get("metric")) or "cosine",
        normalization=_as_str(embed_block.get("normalization")) or "none",
        chunk_schema=_as_str(embed_block.get("chunk_schema")) or f"chunk_schema_v{chunk_schema_hint}",
    )

    profile = _validate_profile(
        str(
            _pick(
                "access.profile",
                cli=cli.get("profile"),
                env_value=_env(env, "PPA_ACCESS_PROFILE") or _env(env, "PPA_MCP_TOOL_PROFILE"),
                env_name="PPA_ACCESS_PROFILE",
                file_value=access_block.get("profile"),
                default="trusted-local",
                origins=origins,
                file_label=file_label,
            )
        )
    )
    access = AccessPolicySpec(
        principal=str(
            _pick(
                "access.principal",
                cli=cli.get("principal"),
                env_value=_env(env, "PPA_ACCESS_PRINCIPAL"),
                env_name="PPA_ACCESS_PRINCIPAL",
                file_value=access_block.get("principal"),
                default="local-operator",
                origins=origins,
                file_label=file_label,
            )
        ),
        profile=profile,
        allowed_tools=_csv(
            cli.get("allowed_tools")
            if cli.get("allowed_tools") is not None
            else (_env(env, "PPA_ACCESS_ALLOWED_TOOLS") or access_block.get("allowed_tools"))
        ),
        allowed_sources=_csv(
            cli.get("allowed_sources")
            if cli.get("allowed_sources") is not None
            else (_env(env, "PPA_ACCESS_ALLOWED_SOURCES") or access_block.get("allowed_sources"))
        ),
        allowed_domains=_csv(
            cli.get("allowed_domains")
            if cli.get("allowed_domains") is not None
            else (_env(env, "PPA_ACCESS_ALLOWED_DOMAINS") or access_block.get("allowed_domains"))
        ),
        egress_policy_revision=str(
            _pick(
                "access.egress_policy_revision",
                cli=cli.get("egress_policy_revision"),
                env_value=_env(env, "PPA_ACCESS_EGRESS_POLICY_REVISION"),
                env_name="PPA_ACCESS_EGRESS_POLICY_REVISION",
                file_value=access_block.get("egress_policy_revision"),
                default="p05b-unspecified",
                origins=origins,
                file_label=file_label,
            )
        ),
    )
    origins.append(
        FieldOrigin(
            "access.allowed_sources",
            "env"
            if _env(env, "PPA_ACCESS_ALLOWED_SOURCES")
            else "file"
            if access_block.get("allowed_sources")
            else "default",
            "access",
        )
    )
    origins.append(
        FieldOrigin(
            "access.allowed_domains",
            "env"
            if _env(env, "PPA_ACCESS_ALLOWED_DOMAINS")
            else "file"
            if access_block.get("allowed_domains")
            else "default",
            "access",
        )
    )

    retrieval = RetrievalSettings(
        default_limit=int(
            _pick(
                "retrieval.default_limit",
                cli=cli.get("default_limit"),
                env_value=_env(env, "PPA_RETRIEVAL_DEFAULT_LIMIT"),
                env_name="PPA_RETRIEVAL_DEFAULT_LIMIT",
                file_value=(retrieval_block.get("defaults") or {}).get("default_limit")
                if isinstance(retrieval_block.get("defaults"), Mapping)
                else retrieval_block.get("default_limit"),
                default=20,
                origins=origins,
                file_label=file_label,
            )
        ),
        default_hops=int(
            _pick(
                "retrieval.default_hops",
                cli=cli.get("default_hops"),
                env_value="",
                env_name="",
                file_value=retrieval_block.get("default_hops"),
                default=2,
                origins=origins,
                file_label=file_label,
            )
        ),
        default_mode=str(
            _pick(
                "retrieval.default_mode",
                cli=cli.get("default_mode"),
                env_value="",
                env_name="",
                file_value=retrieval_block.get("default_mode") or retrieval_block.get("mode"),
                default="hybrid",
                origins=origins,
                file_label=file_label,
            )
        ),
    )

    sources_raw = payload.get("sources") or []
    sources: list[SourceAccount] = []
    if isinstance(sources_raw, list):
        for item in sources_raw:
            if not isinstance(item, Mapping):
                raise ConfigError("sources entries must be objects")
            key = _as_str(item.get("source_key") or item.get("source"))
            if not key:
                raise ConfigError("source_key is required")
            sources.append(
                SourceAccount(
                    source_key=key,
                    account_scope=_as_str(item.get("account_scope") or item.get("account")),
                    enabled=bool(item.get("enabled", True)),
                )
            )

    scopes_raw = payload.get("scopes") or retrieval_block.get("scopes") or []
    if scopes_raw and not isinstance(scopes_raw, list):
        raise ConfigError("scopes must be a list")
    scopes = tuple(dict(item) for item in scopes_raw or [] if isinstance(item, Mapping))

    tuning_block = _as_mapping(payload.get("vault_tuning"), field_name="vault_tuning")
    if tuning_block:
        vault_tuning = VaultTuning(
            merge_threshold=int(tuning_block.get("merge_threshold", 90) or 90),
            conflict_threshold=int(tuning_block.get("conflict_threshold", 75) or 75),
            fuzzy_name_threshold=float(tuning_block.get("fuzzy_name_threshold", 85.0) or 85.0),
            finance_min_amount=float(tuning_block.get("finance_min_amount", 20.0) or 20.0),
        )
        origins.append(FieldOrigin("vault_tuning", "file", file_label))
    else:
        vault_tuning, legacy_path = load_legacy_vault_tuning(Path(identity.canonical_root))
        if legacy_path is not None:
            origins.append(FieldOrigin("vault_tuning", "legacy", str(legacy_path)))

    if "openai_api_key" in embed_block or "api_key" in embed_block:
        raise ConfigError("embeddings must use secret_refs; plaintext api_key is rejected")

    config = InstanceConfig(
        schema_version=CONFIG_SCHEMA_VERSION,
        identity=identity,
        entity=entity,
        storage=StorageBindings(
            vault_path=identity.canonical_root,
            index_schema=index_schema,
            index_dsn_ref=dsn_ref,
            serving_index_path=str(Path(serving_path).expanduser()),
            query_embed_cache_path=str(Path(embed_cache).expanduser()),
        ),
        engine=EngineCompat(
            native=native, format_compatibility=_as_str(engine_block.get("format_compatibility")) or "serving-index-v1"
        ),
        embeddings=embeddings,
        access=access,
        sources=tuple(sources),
        enrichment=_as_mapping(payload.get("enrichment"), field_name="enrichment"),
        rerank=_as_mapping(payload.get("rerank"), field_name="rerank"),
        maintenance=MaintenanceBudgets(
            rebuild_workers=int(maintenance_block["rebuild_workers"])
            if maintenance_block.get("rebuild_workers")
            else None,
            rebuild_batch_size=int(maintenance_block["rebuild_batch_size"])
            if maintenance_block.get("rebuild_batch_size")
            else None,
        ),
        retrieval=retrieval,
        vault_tuning=vault_tuning,
        scopes=scopes,
        secret_refs=(dsn_ref,),
        origins=tuple(origins),
    )
    _SECRETS.set({**current_secret_values(), **secrets})
    return config


def explain_instance_config(config: InstanceConfig, *, secrets: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Redacted effective config with per-field origins. Never includes secret values."""

    blob = {
        "schema_version": config.schema_version,
        "identity": config.identity.to_payload(),
        "entity": {"name": config.entity.name, "entity_type": config.entity.entity_type},
        "storage": {
            "vault_path": config.storage.vault_path,
            "index_schema": config.storage.index_schema,
            "index_dsn": config.storage.index_dsn_ref.to_payload(),
            "serving_index_path": config.storage.serving_index_path,
            "query_embed_cache_path": config.storage.query_embed_cache_path,
        },
        "engine": {"native": config.engine.native, "format_compatibility": config.engine.format_compatibility},
        "embeddings": config.embeddings.to_payload(),
        "access": {
            "principal": config.access.principal,
            "profile": config.access.profile,
            "allowed_tools": list(config.access.allowed_tools),
            "allowed_sources": list(config.access.allowed_sources),
            "allowed_domains": list(config.access.allowed_domains),
            "egress_policy_revision": config.access.egress_policy_revision,
        },
        "retrieval": {
            "default_limit": config.retrieval.default_limit,
            "default_hops": config.retrieval.default_hops,
            "default_mode": config.retrieval.default_mode,
        },
        "sources": [item.__dict__ for item in config.sources],
        "scopes": list(config.scopes),
        "secret_refs": [ref.to_payload() for ref in config.secret_refs],
        "fingerprint": config.fingerprint(),
        "origins": [{"field": item.field, "origin": item.origin, "source": item.source} for item in config.origins],
    }
    redacted = redact_value("root", blob)
    if not isinstance(redacted, dict):
        raise ConfigError("explain produced a non-object payload")
    text = json.dumps(redacted)
    for value in (secrets or current_secret_values()).values():
        if value and value in text:
            raise ConfigError("explain leaked a secret value")
    if "sk-" in text or "postgresql://" in text.lower():
        raise ConfigError("explain leaked a secret value")
    return redacted
