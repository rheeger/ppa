"""Typed config loading for ppa.

``load_archive_config`` is a compatibility facade over
``archive_engine.config.resolve_instance_config``. Precedence is CLI override >
environment > explicit instance file > defaults. Bound instance directories are
CLI-equivalent for ``storage.vault_path``.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from archive_engine.config import current_instance_config, current_secret_values, resolve_instance_config

from .contracts import ArchiveConfig
from .index_config import (
    CHUNK_SCHEMA_VERSION,
    DEFAULT_VECTOR_DIMENSION,
    INDEX_SCHEMA_VERSION,
    _ppa_env,
)

DEFAULT_CONFIG_FILENAMES = ("ppa.yml", "ppa.yaml", "ppa.json")


def _archive_config_from_instance(config) -> ArchiveConfig:
    secrets = current_secret_values()
    version = config.embeddings.model_revision
    try:
        embed_version = int(version)
    except (TypeError, ValueError):
        embed_version = 1
    seed_links_enabled_raw = os.environ.get("PPA_SEED_LINKS_ENABLED", "")
    seed_enabled = seed_links_enabled_raw.strip().lower() in {"1", "true", "yes", "on"}
    return ArchiveConfig(
        vault_path=config.storage.vault_path,
        index_dsn=secrets.get("index_dsn") or None,
        index_schema=config.storage.index_schema,
        retrieval_defaults={
            "default_limit": config.retrieval.default_limit,
            "default_hops": config.retrieval.default_hops,
            "default_mode": config.retrieval.default_mode,
        },
        retrieval={
            "mode": config.retrieval.default_mode,
            "limit_default": config.retrieval.default_limit,
            "candidate_multiplier": 8,
            "preserve_exact_match_bias": True,
            "explain": {
                "enabled": True,
                "include_candidate_generation": True,
                "include_score_breakdown": True,
                "include_context": True,
            },
            "query_planner": {
                "enabled": True,
                "provider": "deterministic",
                "model": "",
                "max_variants": 2,
                "allow_filter_inference": True,
                "allow_alias_expansion": True,
                "max_inferred_types": 3,
                "max_inferred_sources": 3,
            },
            "reranker": {
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
            "context": {
                "include_in_embeddings": True,
                "include_in_reranker_input": True,
                "include_in_result_payloads": True,
            },
        },
        runtime={
            "mode": _ppa_env("PPA_RUNTIME_MODE", default="stdio") or "stdio",
            "tool_profile": config.access.profile,
            "enable_long_lived_runtime": False,
            "local_model_runtime": {
                "enabled": False,
                "base_url": "",
                "provider_kind": "openai_compatible",
                "healthcheck_path": "/health",
            },
        },
        embeddings={
            "provider": config.embeddings.provider_namespace,
            "model": config.embeddings.model,
            "version": embed_version,
        },
        seed_links={
            "enabled": seed_enabled,
            "include_llm": True,
            "apply_promotions": True,
        },
    )


def load_archive_config(
    explicit_path: str | None = None,
    *,
    instance_dir: str | Path | None = None,
    cli_overrides: Mapping[str, Any] | None = None,
    environ: Mapping[str, str] | None = None,
    allow_cwd_discovery: bool | None = None,
) -> ArchiveConfig:
    bound = current_instance_config()
    if (
        bound is not None
        and explicit_path is None
        and instance_dir is None
        and not cli_overrides
        and environ is None
    ):
        return _archive_config_from_instance(bound)

    discover = True if allow_cwd_discovery is None else allow_cwd_discovery
    if instance_dir is not None and allow_cwd_discovery is None:
        discover = False

    instance = resolve_instance_config(
        cli_overrides=cli_overrides,
        environ=environ,
        config_path=explicit_path,
        instance_dir=instance_dir,
        allow_cwd_discovery=discover,
        schema_version_hint=INDEX_SCHEMA_VERSION,
        chunk_schema_hint=CHUNK_SCHEMA_VERSION,
        vector_dimension_hint=DEFAULT_VECTOR_DIMENSION,
    )
    return _archive_config_from_instance(instance)
