# Archive Runtime And Config

## Config Precedence

Runtime behavior should resolve in this order:

1. explicit config object or config file
2. environment variables
3. code defaults

## Supported Config Inputs

- `ARCHIVE_CONFIG_PATH`
- repo-local `archive-mcp.yml`
- repo-local `archive-mcp.yaml`
- repo-local `archive-mcp.json`

## What Belongs In Config

Non-secret behavior should prefer config:

- retrieval defaults
- runtime mode
- tool/profile defaults
- seed-link default behavior
- other stable non-host-specific archive behavior

## What Stays In Environment Variables

Environment variables remain the right place for:

- secrets
- host-specific DSNs
- API keys
- ephemeral local overrides

## Runtime Modes

The archive platform should support:

- normal stdio MCP/runtime mode
- a long-lived runtime mode for expensive retrieval/model-backed operations

The long-lived runtime mode exists to avoid repeated cold starts for:

- vector search preparation
- embedding operations
- future reranking or model-backed retrieval steps

## Status Surface

Runtime status should expose:

- backend
- schema
- schema versions
- projection registry version
- card, edge, and chunk counts
- projection coverage counts
- runtime mode
- rebuild timing metadata

This status surface should be usable by both humans and agents.

`archive_status_json` also returns resolved `retrieval` and `local_model_runtime` blocks from config (see below).

## Retrieval config (`retrieval`)

Loaded from the top-level `retrieval` key in `archive-mcp.yml` (merged with code defaults in `archive_mcp.config.load_archive_config`).

- `mode`, `limit_default`, `candidate_multiplier`, `preserve_exact_match_bias`
- `explain.*` — toggles for explain payload richness
- `query_planner.*` — deterministic vs model provider, `max_variants`, filter inference
- `reranker.*` — `enabled`, `provider` (`none` | `heuristic` | `model`), `top_k`, `blend` weights, `preserve_exact_match_floor`
- `context.*` — `include_in_embeddings`, `include_in_reranker_input`, `include_in_result_payloads`

See `docs/RETRIEVAL_EXPLAIN_SCHEMA.md` for the explain JSON shape.

## Local model runtime (`runtime.local_model_runtime`)

Optional OpenAI-compatible base URL for future planner/rerank/embedding hosts (CPU-first; not required).

- `enabled`, `base_url`, `provider_kind` (default `openai_compatible`), `healthcheck_path`

Status exposes this block so agents can detect configured local inference without starting it.
