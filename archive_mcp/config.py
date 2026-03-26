"""Typed config loading for ppa."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .contracts import ArchiveConfig
from .index_config import _ppa_env

try:  # pragma: no cover - optional dependency
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]


DEFAULT_CONFIG_FILENAMES = ("ppa.yml", "ppa.yaml", "ppa.json")


def _load_config_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        payload = json.loads(text or "{}")
    else:
        if yaml is None:
            raise RuntimeError("YAML support is unavailable but a ppa YAML config was requested")
        payload = yaml.safe_load(text) or {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"Archive config must be an object: {path}")
    return payload


def _discover_config_path(explicit_path: str | None = None) -> Path | None:
    raw = explicit_path or _ppa_env("PPA_CONFIG_PATH")
    if raw:
        return Path(raw).expanduser().resolve()
    cwd = Path.cwd()
    for filename in DEFAULT_CONFIG_FILENAMES:
        candidate = cwd / filename
        if candidate.exists():
            return candidate.resolve()
    return None


def _merge_dict(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_archive_config(explicit_path: str | None = None) -> ArchiveConfig:
    payload: dict[str, Any] = {}
    config_path = _discover_config_path(explicit_path)
    if config_path is not None:
        payload = _load_config_payload(config_path)

    retrieval_defaults = dict(payload.get("retrieval_defaults", {}))
    retrieval = dict(payload.get("retrieval", {}))
    runtime = dict(payload.get("runtime", {}))
    embeddings = dict(payload.get("embeddings", {}))
    seed_links = dict(payload.get("seed_links", {}))

    runtime = _merge_dict(
        {
            "mode": _ppa_env("PPA_RUNTIME_MODE", default=runtime.get("mode", "stdio") or "stdio"),
            "tool_profile": _ppa_env("PPA_MCP_TOOL_PROFILE", default=runtime.get("tool_profile", "full") or "full"),
            "enable_long_lived_runtime": str(runtime.get("enable_long_lived_runtime", "0")).strip().lower()
            in {"1", "true", "yes", "on"},
        },
        runtime,
    )
    embeddings = _merge_dict(
        {
            "provider": _ppa_env("PPA_EMBEDDING_PROVIDER", default=embeddings.get("provider", "hash") or "hash"),
            "model": _ppa_env(
                "PPA_EMBEDDING_MODEL", default=embeddings.get("model", "archive-hash-dev") or "archive-hash-dev"
            ),
            "version": int(_ppa_env("PPA_EMBEDDING_VERSION", default=str(embeddings.get("version", 1) or 1))),
        },
        embeddings,
    )
    retrieval_defaults = _merge_dict(
        {
            "default_limit": int(retrieval_defaults.get("default_limit", 20) or 20),
            "default_hops": int(retrieval_defaults.get("default_hops", 2) or 2),
            "default_mode": str(retrieval_defaults.get("default_mode", "hybrid") or "hybrid"),
        },
        retrieval_defaults,
    )
    seed_links_enabled_raw = os.environ.get("PPA_SEED_LINKS_ENABLED", "")
    seed_links_enabled_env = seed_links_enabled_raw.strip().lower() if seed_links_enabled_raw else None
    seed_links = _merge_dict(
        {
            "enabled": (
                seed_links_enabled_env in {"1", "true", "yes", "on"}
                if seed_links_enabled_env is not None
                else str(seed_links.get("enabled", "0")).strip().lower() in {"1", "true", "yes", "on"}
            ),
            "include_llm": str(seed_links.get("include_llm", "1")).strip().lower() in {"1", "true", "yes", "on"},
            "apply_promotions": str(seed_links.get("apply_promotions", "1")).strip().lower()
            in {"1", "true", "yes", "on"},
        },
        seed_links,
    )
    retrieval = _merge_dict(
        {
            "mode": str(retrieval.get("mode", "hybrid") or "hybrid"),
            "limit_default": int(retrieval.get("limit_default", retrieval_defaults.get("default_limit", 20)) or 20),
            "candidate_multiplier": int(retrieval.get("candidate_multiplier", 8) or 8),
            "preserve_exact_match_bias": str(retrieval.get("preserve_exact_match_bias", "1")).strip().lower()
            in {"1", "true", "yes", "on"},
            "explain": _merge_dict(
                {
                    "enabled": True,
                    "include_candidate_generation": True,
                    "include_score_breakdown": True,
                    "include_context": True,
                },
                dict(retrieval.get("explain") or {}),
            ),
            "query_planner": _merge_dict(
                {
                    "enabled": True,
                    "provider": "deterministic",
                    "model": "",
                    "max_variants": 2,
                    "allow_filter_inference": True,
                    "allow_alias_expansion": True,
                    "max_inferred_types": 3,
                    "max_inferred_sources": 3,
                },
                dict(retrieval.get("query_planner") or {}),
            ),
            "reranker": _merge_dict(
                {
                    "enabled": False,
                    "provider": "none",
                    "model": "",
                    "top_k": 30,
                    "blend": {
                        "top_1_3_retrieval_weight": 0.75,
                        "top_4_10_retrieval_weight": 0.60,
                        "rest_retrieval_weight": 0.40,
                    },
                    "preserve_exact_match_floor": True,
                },
                dict(retrieval.get("reranker") or {}),
            ),
            "context": _merge_dict(
                {
                    "include_in_embeddings": True,
                    "include_in_reranker_input": True,
                    "include_in_result_payloads": True,
                },
                dict(retrieval.get("context") or {}),
            ),
        },
        retrieval,
    )
    runtime = _merge_dict(
        {
            "local_model_runtime": _merge_dict(
                {
                    "enabled": False,
                    "base_url": "",
                    "provider_kind": "openai_compatible",
                    "healthcheck_path": "/health",
                },
                dict(runtime.get("local_model_runtime") or {}),
            ),
        },
        runtime,
    )

    vault_default = payload.get("vault_path", str(Path.home() / "Archive" / "vault"))
    return ArchiveConfig(
        vault_path=_ppa_env("PPA_PATH", default=str(vault_default)),
        index_dsn=_ppa_env("PPA_INDEX_DSN", default=payload.get("index_dsn") or "") or None,
        index_schema=_ppa_env("PPA_INDEX_SCHEMA", default=payload.get("index_schema", "archive_mcp") or "archive_mcp"),
        retrieval_defaults=retrieval_defaults,
        retrieval=retrieval,
        runtime=runtime,
        embeddings=embeddings,
        seed_links=seed_links,
    )
