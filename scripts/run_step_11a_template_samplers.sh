#!/usr/bin/env bash
# Step 11a: template-sampler batch (one Email/ walk) for Tier 1–3 providers.
#
# Defaults: 10pct slice → archive_sync/extractors/specs/samples/<provider>/
#
# Env:
#   PER_YEAR       samples per calendar year per job (default: 5)
#   BATCH          path to job JSON (default: scripts/step_11a_template_sampler_jobs.json)
#   VAULT          override vault path (default: .slices/10pct)
#   RUN_ON_SEED=1  use full seed vault + write under specs/samples_seed/ (requires SEED_VAULT or PPA_SEED_VAULT)
#                  Evidence tree documented at archive_sync/extractors/specs/samples_seed/README.md
#   SEED_VAULT     path to hf-archives seed tree (e.g. .../hf-archives-seed-...)
#
# Examples:
#   ./scripts/run_step_11a_template_samplers.sh
#   PER_YEAR=15 ./scripts/run_step_11a_template_samplers.sh
#   RUN_ON_SEED=1 SEED_VAULT="$HOME/Archive/seed/hf-archives-seed-20260307-235127" PER_YEAR=12 ./scripts/run_step_11a_template_samplers.sh
#
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="${ROOT}/.venv/bin/python"
BATCH_SRC="${BATCH:-${ROOT}/scripts/step_11a_template_sampler_jobs.json}"
PER_YEAR="${PER_YEAR:-5}"
VAULT="${VAULT:-${ROOT}/.slices/10pct}"
RUN_ON_SEED="${RUN_ON_SEED:-0}"

CLEAN_BATCH=""
cleanup() {
  if [[ -n "${CLEAN_BATCH}" ]]; then
    rm -f "${CLEAN_BATCH}"
  fi
}
trap cleanup EXIT

if [[ "${RUN_ON_SEED}" == "1" ]]; then
  SV="${SEED_VAULT:-${PPA_SEED_VAULT:-}}"
  if [[ -z "${SV}" || ! -d "${SV}" ]]; then
    echo "RUN_ON_SEED=1 requires SEED_VAULT or PPA_SEED_VAULT pointing at the seed hf-archives vault." >&2
    exit 1
  fi
  VAULT="${SV}"
  CLEAN_BATCH="$(mktemp)"
  sed 's|archive_sync/extractors/specs/samples/|archive_sync/extractors/specs/samples_seed/|g' "${BATCH_SRC}" > "${CLEAN_BATCH}"
  BATCH="${CLEAN_BATCH}"
  echo "Seed vault: ${VAULT}"
  echo "Output:     archive_sync/extractors/specs/samples_seed/<provider>/"
else
  BATCH="${BATCH_SRC}"
  if [[ ! -d "${VAULT}" ]]; then
    echo "Missing ${VAULT} — set VAULT= or run: make slice-local-10pct" >&2
    exit 1
  fi
fi

echo "Running template-sampler batch — vault=${VAULT} per-year=${PER_YEAR}"
"${PY}" -m archive_mcp template-sampler \
  --vault "${VAULT}" \
  --batch "${BATCH}" \
  --per-year "${PER_YEAR}"

echo "Done."
