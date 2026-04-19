# PPA v2 Phase 5: Full embedding pass runbook

## Pre-flight

1. Verify Phase 4 complete: `ppa index-status` shows expected card counts.
2. Health check: `ppa health-check` passes.
3. Cost estimate: `make embed-estimate` — review pending count and projected cost.
4. OpenAI key: `PPA_USE_ARNOLD_OPENAI_KEY=1 ppa health` — embeddings check passes.
5. Disk space: embeddings table grows on the order of ~6 KB per row. At 3M chunks, plan for tens of GB. Ensure Postgres data volume has headroom.
6. Rate limits: Tier 3 (150K tokens/min) implies roughly hundreds of chunks per minute; Tier 4 is much faster. Adjust concurrency if you hit 429s.

## Where Phase 5 runs

Phase 5 is intended to run on a **developer machine** with network access to OpenAI and a reachable `PPA_INDEX_DSN`, not on the Arnold VM. Arnold (8 GB RAM, 4 vCPUs) is relevant for **Phase 9** restore and index rebuild times, not for the bulk API embedding call itself.

## Execution

1. Run: `make embed-production` (sets `PPA_EMBED_DEFER_VECTOR_INDEX=1` so IVFFlat is dropped before bulk load and rebuilt after).
2. Monitor: `make embed-verify` in another terminal (pending chunk count).
3. If interrupted: re-run `make embed-production` — work is idempotent and resumes on pending chunks.
4. If rate-limited (429): lower concurrency, e.g. `PPA_EMBED_CONCURRENCY=2 make embed-production`.

## CRITICAL: Do not stop after embedding completes

After chunks are embedded, `embed-pending` may still run a **single-threaded IVFFlat index build** when defer is enabled. That step:

- Is logged as rebuild step 6 (vector index).
- Can take **15–40+ minutes** on large corpora.
- Leaves `embedding-status` at zero pending while the index is still building.

**Do not kill the process** until logs show the vector index rebuild complete. Stopping early can leave queries without a usable vector index (sequential scans).

## Post-embedding verification

1. Wait for the vector index rebuild to finish in logs.
2. `make embed-verify` — pending should be 0.
3. `ppa health-check` — embedding coverage should be complete.
4. Spot-check hybrid/vector search for representative queries.
5. Tests: `python -m pytest archive_tests/test_embedding_verification.py -v`

## Rollback

To clear embeddings and rebuild:

```sql
TRUNCATE {schema}.embeddings;
DROP INDEX IF EXISTS {schema}.idx_embeddings_vector;
```

Then re-run `make embed-production`.
