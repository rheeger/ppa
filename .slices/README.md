# Local stratified slices (gitignored)

Generated vault trees from your seed live here — **not committed** (see repo `.gitignore`).

| Directory | Config                          | Approx. role                   |
| --------- | ------------------------------- | ------------------------------ |
| `1pct/`   | `tests/slice_config.1pct.json`  | Small slice, `cluster_cap` 400 |
| `5pct/`   | `tests/slice_config.5pct.json`  | Medium, `cluster_cap` 1000     |
| `10pct/`  | `tests/slice_config.10pct.json` | Large, `cluster_cap` 2500      |

Build from repo root (`ppa/`):

```bash
make slice-local-1pct    # or slice-local-5pct, slice-local-10pct, slice-local-all
```

Override seed vault:

```bash
PPA_BENCHMARK_SOURCE_VAULT=/path/to/seed make slice-local-5pct
```

Then point tools at e.g. `PPA_PATH=$(pwd)/.slices/5pct`.

**Smoke tests (sender-census, template-sampler):** The full seed vault can take **many minutes** to scan (every note is read). For EDL smoke checks, use a **local stratified slice** (you likely already have `10pct/` from `make slice-local-10pct` or `slice-local-all`):

```bash
# From ppa/ — align with extract-emails-10pct-slice / quality reports
PPA_PATH="$(pwd)/.slices/10pct" .venv/bin/python -m archive_mcp sender-census --domain doordash.com --sample 50
PPA_PATH="$(pwd)/.slices/10pct" .venv/bin/python -m archive_mcp template-sampler --domain doordash.com --category receipt --per-year 2 --out-dir /tmp/dd-era-samples
```

Or: `make sender-census-slice-smoke DOMAIN=doordash.com` and `make template-sampler-slice-smoke DOMAIN=doordash.com` (see repo `Makefile` — both use `.slices/10pct`). Census only reads `Email/**/*.md` (not the whole vault tree).

Clean Phase-3 derived cards inside these vaults only:

```bash
make clean-phase3-derived-local-slices
```
