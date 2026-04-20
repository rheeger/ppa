# PPA v2 — Phase 4 full rebuild (operational runbook)

This runbook covers the **one** full index rebuild plus post-rebuild geocoding and verification. It assumes Phases 0–3 are complete and the repo is at `/Users/rheeger/Code/rheeger/ppa` (adjust paths as needed).

## Environment

- `PPA_PATH` — production vault root
- `PPA_INDEX_DSN` — Postgres connection string (pgvector)
- `PPA_INDEX_SCHEMA` — typically `ppa`
- `PPA_DEFAULT_TIMEZONE` — e.g. `America/Los_Angeles`
- Rebuild safety: optional `PPA_REBUILD_RESUME=1` for checkpoint resume

Rust engine (default): `PPA_ENGINE=rust` — expect ~15–25 minutes for a full rebuild vs ~2 hours with `PPA_ENGINE=python`.

### `ppa validate`

After a rebuild (or any time `_meta/vault-scan-cache.sqlite3` exists), `ppa validate` calls **`archive_crate.validate_vault_from_cache`** — parallel Rust validation via rayon that checks uid/type/source/dates **and** full provenance coverage from the `provenance_json` column in cache. This is **authoritative and fast** (~2s on 164k notes).

The tier-2 cache stores `provenance_json` (extracted from the raw body before stripping) so the validator has exactly the same provenance data that the Python `validate_provenance` function uses. No body decompression, no Python multiprocessing, no fallback engines — one behavior.

If the cache is missing or `archive_crate` is unavailable, CLI falls back to per-file Python (`read_note` + Pydantic + `validate_provenance`).

## Pre-rebuild

1. Record rollback anchor (optional): `git rev-parse HEAD > /tmp/ppa-rebuild-anchor.txt`
2. Run automated gates: `make pre-rebuild-check`
3. Confirm Rust extension loads: `python -c "import archive_crate; print('ok')"` (optional; falls back to Python if missing)

## Execute rebuild

```bash
export PPA_REBUILD_RESUME=1
time ppa --log-file logs/phase4-rebuild.log rebuild-indexes --force-full-rebuild --workers N
```

Internals (high level): tier-2 vault cache → scan/fingerprint → schema + migrations → `archive_crate.materialize_row_batch` (or Python) → COPY flush → indexes → manifest.

### Monitor

```bash
psql "$PPA_INDEX_DSN" -c "SELECT loaded_card_count, status FROM ppa.rebuild_checkpoint WHERE id = 1;"
```

(Adjust schema if `PPA_INDEX_SCHEMA` is not `ppa`.)

## Post-rebuild geocoding

```bash
ppa geocode-places
```

Runs Nominatim (rate-limited), writes `latitude`/`longitude` on PlaceCard markdown, then **incremental** rebuild to refresh Postgres.

## Post-rebuild verification

```bash
make post-rebuild-check
```

## Spot-check expectations

| Check                                   | Expected                                                                  |
| --------------------------------------- | ------------------------------------------------------------------------- |
| `ppa index-status`                      | Card count ~matches vault note count (within ~0.1%)                       |
| `ppa validate`                          | Zero errors                                                               |
| `ppa temporal-neighbors --timestamp …`  | Sensible ordering for known fixtures                                      |
| `ppa query --type meal_order --limit 5` | Rows with items / metadata where applicable                               |
| `ppa query --type place --limit 5`      | Places with lat/lng after geocoding                                       |
| `ppa quality-report`                    | Per-type stats; derived types with content score higher than empty shells |
| Ingestion log                           | Row count matches `cards` after full rebuild                              |
| Edges                                   | Non-zero counts for key edge types where data exists                      |

## Rollback

1. Vault markdown is source of truth — unchanged by index-only operations.
2. To roll back **git-tracked** code: `git checkout "$(cat /tmp/ppa-rebuild-anchor.txt)"` (if anchor was saved).
3. Rebuild index again: `ppa rebuild-indexes --force-full-rebuild --workers N`.

## Phase handoff

After this runbook completes successfully, Phase 5 (embedding pass) may proceed.
