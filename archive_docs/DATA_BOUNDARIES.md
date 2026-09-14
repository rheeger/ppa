# Where PPA stores and sends data

A local archive gives its owner a copy of records across services. Keeping control of that copy requires knowing where content is stored, which providers process it, and what a client receives. This inventory describes those boundaries for contributors and operators.

PPA does not encrypt live vault files, attachments, Postgres, journals, caches, serving generations, or temporary files at rest. The backup workflow uses external encryption tooling. Host permissions and disk encryption are operator choices, not engine guarantees.

Code running as the same operating-system user can read what that user can read. Retrieval policy and provider checks do not create an operating-system sandbox. See the [security model](SECURITY_MODEL.md) for the full scope.

## Durable store

| Location | Kind | Typical contents | PPA encryption | Notes |
| --- | --- | --- | --- | --- |
| Vault markdown (`*.md`) | source of truth | frontmatter + card bodies | none | Readable records protected by host permissions and any host encryption. |
| Vault attachments / binaries | source of truth | original files, OCR inputs | none | Extracted text is written back onto attachment cards, not into email `message_body`. |
| Postgres warehouse | derived index | cards, chunks, edges, embeddings, checkpoints | none | DSN may contain a password. Never log the DSN. |
| Publication journal | decision state | change batches / receipts | none | Can name UIDs and sources. |
| Serving generations | derived | JSONL / native serving export | none | May include titles, snippets, neighbor lists. Treat as sensitive. |
| `_meta/` next to the vault | derived / config | scan cache, query-embed cache, LLM config | none | Not a secret store. |

## Caches and temp

| Location | Kind | Typical contents | PPA encryption | Notes |
| --- | --- | --- | --- | --- |
| `vault/_meta/` vault scan cache (SQLite WAL) | cache | frontmatter; tier-2 may hold compressed bodies | none | Rebuilds from the vault. |
| `vault/_meta/query-embed-cache.sqlite` | cache | query text keys + vectors | none | Keys include `policy_identity` so principals do not share entries. |
| Inference cache (SQLite) | cache | prompt/response JSON keyed by content hash + model + versions | none | May contain card excerpts sent to a provider. |
| Extract / anydoc cache | cache | attachment markdown | none | Local OCR first; hosted Firecrawl is remote egress. |
| Process temp / contained work dirs | temp | extract staging, COPY buffers | none | Must stay inside directories controlled by the operation. |
| Backup artifacts | backup | vault bundle and recovery metadata | external OpenSSL for the encrypted bundle | The recovery manifest is a separate metadata file. Other backup copies depend on their backup tool. |

## Secrets on disk (not archive content)

| Location | Mode | Notes |
| --- | --- | --- |
| `~/.ppa/mcp-http-token` | `0600` | HTTP MCP bearer. Not a vault path. |
| `~/.ppa/gemini_key.txt` | operator | Gemini key fallback. Never log. |
| `~/.ppa/firecrawl_key.txt` | operator | Hosted OCR key. Never log. |
| Env / 1Password refs | process | `OPENAI_API_KEY`, `PPA_TEST_PG_DSN`, `FIRECRAWL_API_KEY`, `OP_SERVICE_ACCOUNT_TOKEN`. Redact from logs. |

## Provider egress

Sending archive text to a provider is a different act from reading a
source to ingest it.

| Destination | Transport | Locality | Registry (do not duplicate) |
| --- | --- | --- | --- |
| `hash` | none | local | `get_embedding_provider` |
| `ollama` | HTTP | loopback only | `archive_cli.providers` / `archive_vault.llm_provider` |
| `openai` | HTTP | remote | same |
| `gemini` | HTTP | remote | `archive_vault.llm_provider` |
| `firecrawl` | HTTP | remote | `archive_sync.anydoc_ocr` hosted retry |
| `openclaw` | none | n/a | stub; no transport |
| unknown | unspecified | unspecified | deny |

Policy lives in `archive_engine.egress` and is attached to
`runtime.providers`. `PPA_EGRESS_MODE=local-only` cannot use remote
destinations and cannot follow a remote redirect. Restricted
`AccessContext` cannot send a denied source to a remote destination.
A failed authorized call does not fall back to another destination.

Other outbound HTTP (batch embedder, geocoder, Gmail ingest) is
**source ingest or operator tooling**, not enrichment egress. Those
paths still must not log tokens or DSNs.

## Diagnostics

`ppa.*` log lines pass through `archive_engine.redaction.RedactingFormatter`.
Bearer tokens, `sk-` keys, Gemini `AIza…` keys, Postgres passwords, query
`key=` params, Sentry-style DSNs, and raw frontmatter/card bodies are
replaced with `[REDACTED]`. Correlation IDs are `safe_diagnostic_id`
hashes.

Evidence under `logs/plans/` must not contain real secrets or seed
vault paths. Tests use synthetic sentinels only.

## Threat boundary

These checks apply at the engine, MCP, HTTP, and routed provider interfaces. Code with independent access to the files remains subject to host permissions. Returning content to a remote client is a separate disclosure from storing it locally.

## Configuration requirements

Instance configuration should make these choices explicit without adding another provider registry:

1. Select the provider policy: `unrestricted` for trusted local access, `local-only`, or a source-restricted `AccessContext`.
2. Use secret references (environment, 1Password, or `0600` files) and keep plaintext keys out of committed config.
3. Make hosted OCR and cloud embedding choices explicit; a local failure must not silently change the destination.
4. Describe live storage as plaintext unless the operator has configured host encryption.
