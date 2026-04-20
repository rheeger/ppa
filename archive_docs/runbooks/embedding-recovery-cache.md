# Embedding recovery cache

**Why it exists.** On 2026-04-23 a routine `make rebuild-indexes` wiped 6.77M
production embeddings via a CASCADE FK on `embeddings.chunk_key`. Recovery
cost ~3 hours of OpenAI download bandwidth (no LLM cost) because all 122
prior batches had output files still retained on OpenAI's side (~30-day
retention window). Migration 004 + the `clean-phase3` and `bootstrap`
safeguards make a future wipe extremely unlikely, but a local copy of the
last embedding run is defense in depth that costs only disk space.

**The cache.** Once `embed-batch-ingest` finishes, the downloaded
`*-out.jsonl` files in `_artifacts/_embedding-runs/batches/` are no longer
used by the embedder pipeline. The `ppa embed-cache-rotate` command moves
them into `_artifacts/_embedding-recovery-cache/run-{UTC-timestamp}/` and
prunes older runs (default keeps only the most recent run).

**Disk footprint.** Each completed `text-embedding-3-small` batch is roughly
1.5 GB (50,000 × 1536-dim float vectors as JSONL). A full-vault embed of
~6.77M chunks generates ~135 batches → ~200 GB. Plan accordingly.

## Workflow

### Automatic rotation after `embed-batch-loop`

`archive_scripts/ppa-embed-batch-loop.sh` runs `embed-cache-rotate` as its
exit step when there are no pending chunks and no in-flight batches. No
manual action required for the standard production embedding workflow.

### Manual rotation after a one-shot ingest

```bash
# After ppa embed-batch-ingest (or the recovery flow described in the
# 2026-04-24 postmortem) finishes, move output files into the cache:
make embed-cache-rotate

# Or with explicit retention:
KEEP=2 make embed-cache-rotate

# Or dry-run first:
DRY_RUN=1 make embed-cache-rotate
```

### Re-ingesting from the cache (skipping OpenAI download)

If embeddings are ever wiped again (and the cache holds the prior run's
files), point `embed-batch-ingest` at the cache directory:

```bash
PPA_INDEX_DSN=... PPA_INDEX_SCHEMA=ppa \
  .venv/bin/python -m archive_cli embed-batch-ingest \
  --artifact-dir _artifacts/_embedding-recovery-cache/run-YYYYMMDD-HHMMSSZ \
  --workers 4
```

You'll need to first re-eligible the batches that match those files:

```sql
UPDATE ppa.embed_batches SET ingested_at = NULL WHERE openai_batch_id IN (
  SELECT openai_batch_id FROM ppa.embed_batches WHERE status = 'completed'
);
```

(`embed-batch-ingest` won't touch already-ingested batches, so this UPDATE
is the trigger.)

The local file path matches the OpenAI batch ID format
(`batch_{openai_id}-out.jsonl`), so `_ingest_one_batch` will find them on
disk before falling back to a network download.

## Retention policy

`embed-cache-rotate` keeps only the most recent run by default. To retain
more (e.g. for cross-version comparison):

```bash
KEEP=3 make embed-cache-rotate
```

Each run directory holds a `MANIFEST.txt` with the rotation timestamp and
file count for forensic traceability.

## Cleanup scripts: don't touch

- `archive_scripts/clean-ppa-machine-artifacts.sh` only removes paths under
  `/tmp` and (optionally) `.slices/`. It does NOT touch
  `_artifacts/_embedding-recovery-cache/`.
- `archive_scripts/clean-phase3-derived-dirs.sh` operates on vault content,
  not artifact directories.

If a future cleanup utility is added, it MUST explicitly opt-in to the
recovery cache via a flag like `--include-embedding-recovery-cache` and
should be reviewed against this runbook.
