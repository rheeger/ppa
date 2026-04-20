#!/usr/bin/env bash
# Remove email-extraction derived transaction + entity cards from a vault (seed or slice).
#
# Safety:
#   - Default mode is DRY-RUN. Pass --apply to actually delete.
#   - Refuses to operate on the canonical seed vault unless PPA_ALLOW_PROD_VAULT_DELETE=1
#     is set in the environment AND --apply is passed.
#   - Always emits a manifest of paths it would remove (or did remove) to
#     _artifacts/_phase3-revert/{vault-basename}-{date}.txt for forensics.
#
# This guard exists because of the 2026-04-23 incident where a routine rebuild
# wiped 6.5M production embeddings. See .cursor/plans/phase_6_5_cross_derived_card_linkers_*.plan.md
# (section "Destructive-code audit, 2026-04-24") for the full postmortem.
set -euo pipefail

PROD_VAULT_PATTERNS=(
  "/Users/rheeger/Archive/seed/hf-archives-seed-"
  "/srv/hfa-secure/vault"
)

APPLY=0
ARGS=()
for arg in "$@"; do
  case "$arg" in
    --apply) APPLY=1 ;;
    --dry-run) APPLY=0 ;;
    -h|--help)
      cat <<'EOF'
Usage: clean-phase3-derived-dirs.sh [--apply | --dry-run] <vault> [<vault> ...]

Removes derived transaction + entity directories from each vault.

Default is --dry-run: prints what would be removed and writes a manifest, no deletion.
Pass --apply to actually delete.

Refuses to operate on the canonical seed vault unless PPA_ALLOW_PROD_VAULT_DELETE=1.
EOF
      exit 0
      ;;
    *) ARGS+=("$arg") ;;
  esac
done

if [[ ${#ARGS[@]} -eq 0 ]]; then
  echo "usage: clean-phase3-derived-dirs.sh [--apply | --dry-run] <vault> [<vault> ...]" >&2
  exit 2
fi

is_prod_vault() {
  local vault="$1"
  for pattern in "${PROD_VAULT_PATTERNS[@]}"; do
    if [[ "$vault" == "$pattern"* ]]; then
      return 0
    fi
  done
  return 1
}

# Manifest dir relative to repo root (script lives in ppa/archive_scripts/).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_PPA="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST_DIR="$REPO_PPA/_artifacts/_phase3-revert"
mkdir -p "$MANIFEST_DIR"

clean_one() {
  local root="${1:?vault root}"
  if [[ ! -d "$root" ]]; then
    echo "skip (not a directory): $root"
    return 0
  fi
  if is_prod_vault "$root"; then
    if [[ "$APPLY" -eq 1 && "${PPA_ALLOW_PROD_VAULT_DELETE:-0}" != "1" ]]; then
      echo "REFUSING: $root looks like a production vault." >&2
      echo "  To override: PPA_ALLOW_PROD_VAULT_DELETE=1 $(basename "$0") --apply $root" >&2
      echo "  Strongly recommended: snapshot first via:" >&2
      echo "    cp -a \"$root\" \"$root.pre-phase3-revert-\$(date +%Y%m%d-%H%M%S)\"" >&2
      exit 3
    fi
  fi

  local timestamp
  timestamp="$(date +%Y%m%d-%H%M%S)"
  local manifest_path
  manifest_path="$MANIFEST_DIR/$(basename "$root")-$timestamp.txt"

  echo "vault: $root  apply=$APPLY  manifest: $manifest_path"
  {
    echo "# clean-phase3-derived-dirs.sh"
    echo "# vault: $root"
    echo "# apply: $APPLY"
    echo "# date:  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "# user:  ${USER:-unknown}@$(hostname)"
    echo
  } >"$manifest_path"

  local subdirs=(
    "$root/Transactions/MealOrders"
    "$root/Transactions/Rides"
    "$root/Transactions/Flights"
    "$root/Transactions/Accommodations"
    "$root/Transactions/CarRentals"
    "$root/Transactions/Groceries"
    "$root/Transactions/Shipments"
    "$root/Entities/Places"
    "$root/Entities/Organizations"
  )
  local total_files=0
  for d in "${subdirs[@]}"; do
    if [[ -d "$d" ]]; then
      local count
      count=$(find "$d" -type f 2>/dev/null | wc -l | tr -d ' ')
      total_files=$((total_files + count))
      echo "$d  files=$count" >>"$manifest_path"
      find "$d" -type f -print >>"$manifest_path" 2>/dev/null || true
    else
      echo "$d  (missing)" >>"$manifest_path"
    fi
  done

  echo "  paths considered: ${#subdirs[@]}  files: $total_files"
  echo "  manifest written: $manifest_path"

  if [[ "$APPLY" -ne 1 ]]; then
    echo "  DRY-RUN — pass --apply to delete."
    return 0
  fi

  for d in "${subdirs[@]}"; do
    rm -rf -- "$d"
  done
  echo "  deleted."
}

for vault in "${ARGS[@]}"; do
  clean_one "$vault"
done
