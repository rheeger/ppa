# Data boundaries

P05-C inventory of where archive data lives and what PPA actually does
with it. This is not an encryption specification.

**PPA does not encrypt vault files, attachments, Postgres, journals,
caches, serving generations, temp files, or backups.** File permissions
and operator-managed disk encryption (FileVault, LUKS, a host volume)
are outside this engine. Arnold / Hey Arnold runbooks and vision docs
that describe LUKS volumes, FileVault, or `archive_cli/encryption.py`
are host procedures or future product ideas — they are not implemented
capabilities of this tree and must not be cited as PPA at-rest
encryption.

Same-user process compromise is out of scope: an attacker running as
the PPA OS user can read whatever that user can read.

## Durable store

| Location | Kind | Typical contents | PPA encryption | Notes |
| --- | --- | --- | --- | --- |
| Vault markdown (`*.md`) | source of truth | frontmatter + card bodies | none | Canonical archive. OS perms only. |
| Vault attachments / binaries | source of truth | original files, OCR inputs | none | Extracted text is written back onto attachment cards, not into email `message_body`. |
| Postgres warehouse | derived index | cards, chunks, edges, embeddings, checkpoints | none | DSN may contain a password. Never log the DSN. |
| Publication journal | derived | change batches / receipts | none | Can name UIDs and sources. |
| Serving generations | derived | JSONL / native serving export | none | May include titles, snippets, neighbor lists. Treat as sensitive. |
| `_meta/` next to the vault | derived / config | scan cache, query-embed cache, LLM config | none | Not a secret store. |

## Caches and temp

| Location | Kind | Typical contents | PPA encryption | Notes |
| --- | --- | --- | --- | --- |
| `vault/_meta/` vault scan cache (SQLite WAL) | cache | frontmatter; tier-2 may hold compressed bodies | none | Rebuilds from the vault. |
| `vault/_meta/query-embed-cache.sqlite` | cache | query text keys + vectors | none | Keys include `policy_identity` so principals do not share entries. |
| Inference cache (SQLite) | cache | prompt/response JSON keyed by content hash + model + versions | none | May contain card excerpts sent to a provider. |
| Extract / anydoc cache | cache | attachment markdown | none | Local OCR first; hosted Firecrawl is remote egress. |
| Process temp / contained work dirs | temp | extract staging, COPY buffers | none | Must stay inside owned directories (P05-A). |
| Operator backups | operator | vault and/or PGDATA copies | none by PPA | Encryption, if any, is the backup tool's job. |

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
| unknown | — | — | **deny** |

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

P05 defends public engine, MCP, HTTP, and provider interfaces. It does
not claim protection against arbitrary code running as the same OS
user, and it does not claim encrypted-at-rest deployment.

## P09 configuration requirements

P09-B should expose, without inventing a second provider registry:

1. Explicit egress mode: `unrestricted` (trusted-local) vs `local-only`
   vs source-restricted `AccessContext`.
2. Secret references (env / 1Password / `0600` files) — never plaintext
   keys in committed config.
3. Confirmation that hosted OCR / cloud embeddings are a consent step,
   not a silent fallback from a local failure.
4. Honest at-rest language: PPA stores plaintext; host encryption is
   optional and operator-owned.
