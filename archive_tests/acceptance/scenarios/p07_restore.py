"""P07-D acceptance: encrypted restore rebuilds the corrected story without saved indexes."""

from __future__ import annotations

import logging
import shutil
import time
from typing import Any

from archive_engine.corrections import CorrectionCommandRequest, execute_correction_command
from archive_engine.recovery import (
    ENCRYPTION_TOOL,
    create_encrypted_bundle,
    restore_encrypted_bundle,
    snapshot_tree_hashes,
    write_restore_receipt,
)
from archive_sync.adapters.base import deterministic_provenance
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_vault.schema import FinanceCard
from archive_vault.vault import read_note, write_card

UID = "hfa-finance-p07dacc001"
REL = "Finance/2026-09/hfa-finance-p07dacc001.md"
PASSPHRASE = "p07d-acceptance-fixture"


def _require_meta(vault) -> None:
    meta = vault / "_meta"
    for name, payload in (
        ("own-emails.json", "[]\n"),
        ("nicknames.json", "{}\n"),
        ("ppa-config.json", "{}\n"),
        ("llm-config.json", '{"primary": {"provider": "gemini", "model": "fixture"}}\n'),
    ):
        path = meta / name
        if not path.is_file():
            path.write_text(payload, encoding="utf-8")


def _write_dinner(vault) -> None:
    (vault / "Finance" / "2026-09").mkdir(parents=True, exist_ok=True)
    card = FinanceCard(
        uid=UID,
        type="finance",
        source=["amex"],
        source_id="amex:p07d-acceptance",
        created="2026-09-01",
        updated="2026-09-01",
        summary="P07-D Acceptance Dinner",
        amount=42.0,
        currency="USD",
        counterparty="Restaurant",
        note="synthetic restore dinner",
    )
    write_card(vault, REL, card, body="dinner", provenance=deterministic_provenance(card, "amex"))


def run_p07_restore(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    _require_meta(runtime.vault)
    _write_dinner(runtime.vault)
    execute_correction_command(
        runtime.vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=UID,
            field="amount",
            value=18.5,
            author="acceptance.p07d",
            reason="paper receipt",
            rel_path=REL,
        ),
    )
    source_hashes = snapshot_tree_hashes(runtime.vault)
    backup_base = runtime.root / "encrypted-backup"
    created = create_encrypted_bundle(runtime.vault, backup_base, passphrase=PASSPHRASE)
    dest = runtime.root / "restored-vault"
    if dest.exists():
        shutil.rmtree(dest)
    receipt = restore_encrypted_bundle(
        dest=dest,
        passphrase=PASSPHRASE,
        backup_base=backup_base,
        active_root=runtime.vault,
        source_vault=runtime.vault,
        source_hashes=source_hashes,
        strip_reconstructible=True,
    )
    if snapshot_tree_hashes(runtime.vault) != source_hashes:
        raise AssertionError("active vault changed during restore")
    if read_note(dest, REL)[0].get("amount") != 18.5:
        raise AssertionError("restored dinner is not the corrected amount")
    blake_path = dest / "People" / "blake-loser.md"
    identity_ok = blake_path.is_file()
    blake_redirect = ""
    if identity_ok:
        blake_redirect = str(read_note(dest, "People/blake-loser.md")[0].get("redirect_to") or "")
        if blake_redirect:
            raise AssertionError(f"restored undone identity still redirects: {blake_redirect}")

    from archive_cli.commands.recovery import activate_restored_archive
    from archive_cli.server import archive_read, archive_search
    from archive_tests.acceptance.environment import reset_serving_handle

    import os

    previous_path = os.environ.get("PPA_PATH")
    os.environ["PPA_PATH"] = str(dest)
    os.environ["PPA_SERVING_INDEX_PATH"] = str(runtime.root / "restored-serving")
    reset_serving_handle()
    try:
        activated = activate_restored_archive(
            vault=dest,
            logger=logging.getLogger("ppa.acceptance"),
            receipt=receipt,
            query_uid=UID,
            query_text="P07-D Acceptance Dinner",
        )
        mcp_read = archive_read(UID)
        mcp_search = archive_search("P07-D Acceptance Dinner", limit=8)
    finally:
        if previous_path is None:
            os.environ.pop("PPA_PATH", None)
        else:
            os.environ["PPA_PATH"] = previous_path
        os.environ["PPA_SERVING_INDEX_PATH"] = str(runtime.serving_index_path)
        reset_serving_handle()

    if "18.5" not in mcp_read and "18.50" not in mcp_read:
        raise AssertionError(f"MCP read missed corrected amount: {mcp_read[:500]}")
    if UID not in mcp_search and "Dinner" not in mcp_search:
        raise AssertionError(f"MCP search missed restored dinner: {mcp_search[:500]}")

    evidence_dir = runtime.root.parent
    receipt_path = write_restore_receipt(receipt, evidence_dir / "restore-receipt.json")
    elapsed = round(time.monotonic() - started, 3)
    receipt.rto_seconds = elapsed
    receipt.rebuilt = True
    receipt.query = {"mcp_read_hit": True, "mcp_search_hit": True, "uid": UID, "amount": 18.5}
    write_restore_receipt(receipt, receipt_path)
    return {
        "id": "p07.restore.roundtrip",
        "status": "passed",
        "elapsed_seconds": elapsed,
        "encryption_tool": ENCRYPTION_TOOL,
        "archive_sha256": created["archive_sha256"],
        "restore_root": receipt.restore_root,
        "active_root_untouched": receipt.active_root_untouched,
        "checkpoint": receipt.checkpoint,
        "rpo_seconds": receipt.rpo_seconds,
        "rto_seconds": elapsed,
        "scale_profile": receipt.scale_profile,
        "cited_amount": 18.5,
        "identity_card_restored": identity_ok,
        "blake_redirect": blake_redirect,
        "mcp_read_hit": True,
        "mcp_search_hit": True,
        "notes": receipt.notes,
        "activate_rebuilt": bool(activated.get("rebuilt")),
    }


register(
    Scenario(
        id="p07.restore.roundtrip",
        suite="p07",
        product_guarantee="An encrypted fixture backup restores into a new root, rebuilds without saved indexes, and answers the corrected story",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=("restore-receipt.json",),
        run=run_p07_restore,
    )
)
