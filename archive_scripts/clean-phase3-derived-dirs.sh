#!/usr/bin/env bash
# Remove email-extraction derived transaction + entity cards from a vault (seed or slice).
# Safe to run on missing dirs. Same tree as Phase 3 revert on the full seed vault.
set -euo pipefail

clean_one() {
  local root="${1:?vault root}"
  if [[ ! -d "$root" ]]; then
    echo "skip (not a directory): $root"
    return 0
  fi
  echo "Cleaning Phase 3 derived dirs under: $root"
  rm -rf \
    "$root/Transactions/MealOrders" \
    "$root/Transactions/Rides" \
    "$root/Transactions/Flights" \
    "$root/Transactions/Accommodations" \
    "$root/Transactions/CarRentals" \
    "$root/Transactions/Groceries" \
    "$root/Transactions/Shipments" \
    "$root/Entities/Places" \
    "$root/Entities/Organizations"
  echo "  done."
}

for vault in "$@"; do
  clean_one "$vault"
done
