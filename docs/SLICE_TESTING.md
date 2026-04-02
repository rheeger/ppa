# Stratified seed slice — configuration, generation, and verification

The PPA test infrastructure uses a **stratified transitive-closure slicer** to produce a
structurally complete subset of the real seed vault for integration and behavioral testing.

## Slice configuration files

| File                            | Purpose                                      | Typical use                                                                      |
| ------------------------------- | -------------------------------------------- | -------------------------------------------------------------------------------- |
| `tests/slice_config.json`       | Full slice (~5% of seed, `cluster_cap` 200)  | `make test-slice` + `make test-slice-verify`                                     |
| `tests/slice_config.smoke.json` | Tiny slice (~0.5% of seed, `cluster_cap` 60) | `make test-slice-smoke` + `make test-slice-verify-smoke`; fast agent/CI feedback |

### Config fields

```json
{
  "vault_commit": "",
  "snapshot_date": "2026-03-31",
  "seed_uids_by_type": {},
  "cluster_cap": 200,
  "min_cards_per_type": 1,
  "target_percent": 5,
  "primary_user_uid": "hfa-person-9c9dbd68e803"
}
```

- **`target_percent`** — percentage of each card type to seed into the slice.
- **`cluster_cap`** — if a single seed's transitive closure exceeds this many cards, that seed is dropped and an alternative is chosen (prevents hub explosion).
- **`min_cards_per_type`** — guarantee at least this many cards of every type.
- **`seed_uids_by_type`** — optional: pin specific UIDs per type for reproducibility across re-forks.
- **`vault_commit` / `snapshot_date`** — metadata for provenance tracking.
- **`primary_user_uid`** — optional PersonCard UID always included as an anchor (overridable with `PPA_PRIMARY_USER_UID`).

## Behavioral manifests

| File                                | Purpose                                                                                                    |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `tests/slice_manifest.json`         | Full behavioral + structural checks against a **real** seed slice (`make test-slice-verify`, bench verify) |
| `tests/slice_manifest.smoke.json`   | Lighter behavioral + structural checks for `make test-slice-verify-smoke`                                  |
| `tests/slice_manifest_fixture.json` | Queries grounded in `tests/fixtures` — CI `slice-verify` job and `test_rebuild_incremental`                |

Health-check reads the manifest and asserts every check passes after a rebuild.

## Where slices are saved

| Make target        | Output directory                | Notes                             |
| ------------------ | ------------------------------- | --------------------------------- |
| `test-slice`       | `/tmp/ppa-test-slice`           | Full 5% slice from real seed      |
| `test-slice-smoke` | `/tmp/ppa-test-slice-smoke`     | Tiny 0.5% slice for fast feedback |
| Manual CLI         | Whatever you pass to `--output` | e.g. `/tmp/ppa-test-slice-seed`   |

Slice output is a **flat directory of vault-format `.md` files** — it can be pointed at by
`PPA_PATH` for any PPA command (`rebuild-indexes`, `health-check`, `benchmark`, etc.).

## Generating a slice

### Via Makefile (recommended)

```bash
# Full slice from real seed vault (tens of minutes for ~1.85M notes):
make test-slice

# Smoke slice (same vault scan, smaller output — still tens of minutes):
make test-slice-smoke
```

Both use `PPA_BENCHMARK_SOURCE_VAULT` (defaults to `~/Archive/seed/hf-archives-seed-…`
in the Makefile). Override with:

```bash
PPA_BENCHMARK_SOURCE_VAULT=/path/to/seed make test-slice
```

### Via CLI directly

```bash
ppa --log-file logs/ppa-slice-seed.log slice-seed \
    --config tests/slice_config.smoke.json \
    --output /tmp/my-slice \
    --source-vault ~/Archive/seed/hf-archives-seed-20260307-235127 \
    --progress-every 10000
```

Key flags:

- **`--log-file`** must come **before** `slice-seed` (it's a global flag).
- **`--target-percent`** / **`--cluster-cap`** override the config file values.
- **`--build-image`** builds a Docker image for CI distribution.

## Verifying a slice (rebuild + health-check)

### Via Makefile

```bash
# After test-slice:
make test-slice-verify

# After test-slice-smoke:
make test-slice-verify-smoke
```

These run: `bootstrap-postgres` → `rebuild-indexes` → `health-check --manifest <manifest>`.
Output reports land in `logs/health-report.json` and `logs/health-report.md`.

### Manual verification against any slice

```bash
export PPA_PATH=/tmp/ppa-test-slice-seed
export PPA_INDEX_DSN=postgresql://archive:archive@127.0.0.1:50051/archive
export PPA_INDEX_SCHEMA=archive_test_slice_seed
export PPA_EMBEDDING_PROVIDER=hash PPA_EMBEDDING_MODEL=archive-hash-dev PPA_EMBEDDING_VERSION=1

ppa bootstrap-postgres
ppa --log-file logs/rebuild.log rebuild-indexes --workers 4
ppa health-check --manifest tests/slice_manifest.smoke.json --report-format both --report-dir logs
```

## What slice-verify checks

1. **Structural:** zero duplicate UIDs, card counts by type populated.
2. **Behavioral:** FTS queries (if present in manifest) — precision, recall, top-3 type match.
3. **Graph / temporal:** entries from manifest (currently empty; expanded in Phase 1+).

## Logging

All long-running slice operations follow [`ppa/.cursor/rules/ppa-long-running-jobs.mdc`](.cursor/rules/ppa-long-running-jobs.mdc):

- `ppa.slice` logger: walk count, read-pass progress with ETA (`M:SS`), copy progress, final metrics.
- `ppa.loader` logger: 6-step rebuild progress with rate/ETA.
- Logs go to **stderr**; stdout is reserved for JSON summaries.
- Use `--log-file` for `tail -f` visibility on long runs.

## Vault scan cache

PPA persists an expensive full-vault scan to **`<vault>/_meta/vault-scan-cache.sqlite3`** (SQLite, WAL mode). Tier 1 caches frontmatter-only fields; tier 2 adds zlib-compressed bodies, manifest `content_hash`, wikilinks, and raw-file SHA-256 for seed-link sketches.

- **Invalidation:** walk-only fingerprint (sorted `rel_path`, `mtime_ns`, `size` per note). Any mtime/size change anywhere in the vault forces a full cache rebuild (no partial invalidation).
- **`--no-cache`** (global flag, same level as `--log-file`): skip reading/writing the cache file; scan still runs into an in-memory SQLite DB (`slice-seed`, `rebuild-indexes`).
- **Rough sizes on very large vaults:** tier 1 often ~100–150MB; tier 2 often ~2–3GB (zlib-compressed bodies vs raw markdown on disk).
- **Delete the cache:** `rm <vault>/_meta/vault-scan-cache.sqlite3` (safe; next run rebuilds).

## CI behavior

CI (`slice-verify` job in `.github/workflows/test.yml`) uses `--source-vault tests/fixtures`
(synthetic fixtures, not the real seed) so the job completes in seconds without vault access.
Real-seed verification is a **local / nightly** operation.
