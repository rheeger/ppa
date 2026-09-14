# Test a subset of an archive

A small synthetic fixture is the starting point for a contribution. Some retrieval and linking changes also need a larger sample with connected records. PPA's slicer selects a subset and follows relationships so related cards remain available for behavioral checks.

Use an archive you are authorized to test. A slice can contain private records, so keep it out of public pull requests. New contributors can use [synthetic fixtures](../archive_tests/EXTENSIBILITY_CONTRACT.md) without the maintainer's vault. Slice creation still reads the input archive; a small output does not imply a short job.

## Slice configuration files

| File                                      | Purpose                                      | Typical use                                                                      |
| ----------------------------------------- | -------------------------------------------- | -------------------------------------------------------------------------------- |
| `archive_tests/slice_config.json` | Full slice (~5% of seed, `cluster_cap` 200)  | `make test-slice` + `make test-slice-verify`                                     |
| `archive_tests/slice_config.smoke.json`           | Tiny slice (~0.5% of seed, `cluster_cap` 60) | `make test-slice-smoke` + `make test-slice-verify-smoke`; smaller output for inspection |

### Config fields

```json
{
  "vault_commit": "",
  "snapshot_date": "2026-03-31",
  "seed_uids_by_type": {},
  "cluster_cap": 200,
  "min_cards_per_type": 1,
  "target_percent": 5,
  "primary_user_uid": ""
}
```

- **`target_percent`**: percentage of each card type to seed into the slice.
- **`cluster_cap`**: if a single seed's transitive closure exceeds this many cards, that seed is dropped and an alternative is chosen (prevents hub explosion).
- **`min_cards_per_type`**: minimum requested cards per type.
- **`seed_uids_by_type`**: optional: pin specific UIDs per type for reproducibility across re-forks.
- **`vault_commit` / `snapshot_date`**: metadata for provenance tracking.
- **`primary_user_uid`**: optional PersonCard UID always included as an anchor (overridable with `PPA_PRIMARY_USER_UID`).

## Behavioral manifests

| File                                | Purpose                                                                                                     |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `archive_tests/slice_manifest.json`         | Full behavioral + structural checks against a **real** seed slice (`make test-slice-verify`, bench verify)  |
| `archive_tests/slice_manifest.smoke.json`   | Lighter behavioral + structural checks for `make test-slice-verify-smoke`                                   |
| `archive_tests/slice_manifest_fixture.json` | Queries grounded in `archive_tests/fixtures` for CI `slice-verify` job and `test_rebuild_incremental` |

Health-check reads the manifest and asserts every check passes after a rebuild.

## Where slices are saved

| Make target                           | Output directory                  | Notes                                           |
| ------------------------------------- | --------------------------------- | ----------------------------------------------- |
| `test-slice`                          | `/tmp/ppa-test-slice`             | Full 5% slice from real seed                    |
| `test-slice-smoke`                    | `/tmp/ppa-test-slice-smoke`       | Tiny 0.5% slice for fast feedback               |
| `slice-local-1pct` / `5pct` / `10pct` | `ppa/.slices/1pct` … (gitignored) | Persisted local copies; see `.slices/README.md` |
| `slice-local-all`                     | all three under `.slices/`        | Runs 1% then 5% then 10% (long)                 |
| Manual CLI                            | Whatever you pass to `--output`   | e.g. `/tmp/ppa-test-slice-seed`                 |

Configs for local 1/5/10%: `archive_tests/slice_config.1pct.json`, `slice_config.5pct.json`, `slice_config.10pct.json` (`cluster_cap` matches `archive_scripts/run_slice_cache_bench.sh`).

Slice output is a flat directory of vault-format `.md` files that can be used by
`PPA_PATH` for any PPA command (`rebuild-indexes`, `health-check`, `benchmark`, etc.).

## Generating a slice

### Via Makefile (recommended)

```bash
# Select the source archive explicitly.
PPA_BENCHMARK_SOURCE_VAULT=/path/to/source-vault make test-slice

# A smaller output still requires scanning that source.
PPA_BENCHMARK_SOURCE_VAULT=/path/to/source-vault make test-slice-smoke
```

Both use `PPA_BENCHMARK_SOURCE_VAULT`. The Makefile has a maintainer-specific default, so set this variable to the intended source archive:

```bash
PPA_BENCHMARK_SOURCE_VAULT=/path/to/seed make test-slice
```

### Via CLI directly

```bash
ppa --log-file logs/ppa-slice-seed.log slice-seed \
    --config archive_tests/slice_config.smoke.json \
    --output /tmp/my-slice \
    --source-vault /path/to/source-vault \
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
ppa health-check --manifest archive_tests/slice_manifest.smoke.json --report-format both --report-dir logs
```

## What slice-verify checks

1. **Structural:** zero duplicate UIDs, card counts by type populated.
2. **Behavioral:** FTS queries (if present in manifest): precision, recall, top-3 type match.
3. **Graph and temporal:** the relationship and date queries supplied by the chosen manifest. An empty section adds no checks.

## Logging

All long-running slice operations follow [detached-job instructions](../.cursor/skills/long-running-jobs/SKILL.md):

- `ppa.slice` logger: walk count, read-pass progress with ETA (`M:SS`), copy progress, final metrics.
- `ppa.loader` logger: 6-step rebuild progress with rate/ETA.
- Logs go to **stderr**; stdout is reserved for JSON summaries.
- Use `--log-file` for `tail -f` visibility on long runs.

## Vault scan cache

PPA persists an expensive full-vault scan to **`<vault>/_meta/vault-scan-cache.sqlite3`** (SQLite, WAL mode). Tier 1 caches frontmatter-only fields; tier 2 adds zlib-compressed bodies, manifest `content_hash`, wikilinks, and raw-file SHA-256 for seed-link sketches.

- **Invalidation:** walk-only fingerprint (sorted `rel_path`, `mtime_ns`, `size` per note). On fingerprint mismatch, only notes whose `mtime_ns` or `file_size` changed are re-parsed (incremental rebuild). Deleted notes are purged; unchanged rows are kept. A cache version derived from source-file fingerprints triggers a full rebuild when parsing or hashing logic changes.
- **`--no-cache`** (global flag, same level as `--log-file`): skip reading/writing the cache file; scan still runs into an in-memory SQLite DB (`slice-seed`, `rebuild-indexes`).
- **Delete the cache:** `rm <vault>/_meta/vault-scan-cache.sqlite3` (safe; next run rebuilds).

## CI behavior

CI (`slice-verify` job in `.github/workflows/test.yml`) uses `--source-vault archive_tests/fixtures`
with synthetic records. Checks against an existing personal archive run separately in the environment that owns it.
