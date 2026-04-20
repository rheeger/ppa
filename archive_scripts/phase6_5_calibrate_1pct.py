"""Phase 6.5 Step 12 -- per-module 1pct calibration driver.

Reads the 1pct vault via VaultScanCache, builds the catalog (which triggers
every registered linker's post_build_hook), runs each Phase 6.5 new module's
generator against every matching source card, and writes the results to
_artifacts/_linkers/{module}/calibration/candidates-{date}.jsonl plus a
human-readable report.

Usage:

    .venv/bin/python archive_scripts/phase6_5_calibrate_1pct.py \
        --vault .slices/1pct \
        --module meetingArtifactLinker \
        --module tripClusterLinker \
        --module financeReconcileLinker

Each --module is optional; omitting the flag calibrates all active Phase 6.5
linkers.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from archive_cli.linker_calibration import run_vault_calibration

PHASE_6_5_MODULES = ("meetingArtifactLinker", "tripClusterLinker", "financeReconcileLinker")


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault", required=True)
    parser.add_argument(
        "--module",
        action="append",
        default=[],
        help="Repeatable. Defaults to all Phase 6.5 new modules.",
    )
    parser.add_argument(
        "--artifact-root",
        default="_artifacts/_linkers",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log = logging.getLogger("ppa.calibrate")

    modules = tuple(args.module) or PHASE_6_5_MODULES

    vault_path = Path(args.vault).resolve()
    if not vault_path.exists():
        print(f"Vault not found: {vault_path}", file=sys.stderr)
        return 2

    log.info("Running vault calibration over %s...", vault_path)
    run_vault_calibration(
        vault_path,
        modules,
        artifact_root=Path(args.artifact_root),
        write_phase_summary=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
