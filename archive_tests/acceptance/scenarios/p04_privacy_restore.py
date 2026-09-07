"""P04-C: deny leakage plus encrypted restore of the same corrected story."""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
from typing import Any

from archive_cli.serving_index import get_serving_handle
from archive_engine.access import access_request_fields
from archive_engine.contracts import AccessContext
from archive_engine.corrections import CorrectionCommandRequest, execute_correction_command
from archive_engine.egress import authorize_destination, capture_egress, egress_scope
from archive_engine.errors import EgressDeniedError
from archive_engine.recovery import (
    ENCRYPTION_TOOL,
    create_encrypted_bundle,
    restore_encrypted_bundle,
    snapshot_tree_hashes,
    write_restore_receipt,
)
from archive_sync.adapters.base import deterministic_provenance
from archive_tests.acceptance.environment import IsolatedRuntime, reset_serving_handle
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_retrieval_privacy import (
    DENIED_TOKEN,
    GMAIL_UID,
    MEDICAL_UID,
    MIXED_UID,
    _publish_privacy,
)
from archive_vault.schema import FinanceCard
from archive_vault.vault import read_note, write_card

UID = "hfa-finance-p04crestore01"
REL = "Finance/2026-09/hfa-finance-p04crestore01.md"
PASSPHRASE = "p04c-restore-fixture"
QUERY = "P04-C Restore Dinner"


def _require_meta(vault) -> None:
    meta = vault / "_meta"
    meta.mkdir(parents=True, exist_ok=True)
    for name, payload in (
        ("own-emails.json", "[]\n"),
        ("nicknames.json", "{}\n"),
        ("ppa-config.json", "{}\n"),
        ("llm-config.json", '{"primary": {"provider": "gemini", "model": "fixture"}}\n'),
        ("identity-map.json", "{}\n"),
        ("sync-state.json", "{}\n"),
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
        source_id="amex:p04c-restore",
        created="2026-09-01",
        updated="2026-09-01",
        summary=QUERY,
        amount=42.0,
        currency="USD",
        counterparty="Harbor Kitchen",
        note="synthetic restore dinner",
    )
    write_card(vault, REL, card, body="dinner 42.00", provenance=deterministic_provenance(card, "amex"))


def _deny_matrix(runtime: IsolatedRuntime) -> dict[str, Any]:
    reset_serving_handle()
    gid = _publish_privacy(runtime.root / "privacy-index")
    os.environ["PPA_SERVING_INDEX_PATH"] = str(runtime.root / "privacy-index")
    reset_serving_handle()
    handle = get_serving_handle(runtime.vault)
    alice = AccessContext(archive_id="p04c", principal="alice", profile="read-only", allowed_sources=("gmail",))
    policy = access_request_fields(alice)
    listed = handle.query(limit=20, **policy)
    uids = {str(row.get("card_uid") or "") for row in listed if row.get("card_uid")}
    graph = handle.graph("Email/gmail.md", hops=2, **policy)
    blob = json.dumps({"listed": listed, "graph": graph})
    if MEDICAL_UID in uids or MIXED_UID in uids:
        raise AssertionError(f"gmail principal leaked denied UIDs: {sorted(uids)}")
    if GMAIL_UID not in uids:
        raise AssertionError(f"gmail principal lost allowed source: {sorted(uids)}")
    if DENIED_TOKEN in blob:
        raise AssertionError("denied sentinel leaked through metadata/graph")
    handle.close()
    reset_serving_handle()
    os.environ["PPA_SERVING_INDEX_PATH"] = str(runtime.serving_index_path)
    return {
        "generation": gid,
        "allowed_uid": GMAIL_UID,
        "denied_uids_absent": True,
        "denied_token_leaked": False,
        "graph_keys": sorted(graph.keys()),
    }


def _egress_deny() -> dict[str, Any]:
    access = AccessContext(
        archive_id="p04c",
        principal="local-operator",
        profile="trusted-local",
        egress_policy_revision="local-only",
    )
    os.environ["PPA_EGRESS_MODE"] = "local-only"
    with capture_egress() as events:
        with egress_scope(access):
            try:
                authorize_destination(
                    "openai",
                    access=access,
                    url="https://api.openai.com/v1/embeddings",
                )
                raise AssertionError("local-only authorized a remote destination")
            except EgressDeniedError:
                pass
    if any(event.allowed and event.destination == "openai" for event in events):
        raise AssertionError("remote destination recorded as allowed")
    if any(DENIED_TOKEN in (event.body or "") for event in events):
        raise AssertionError("egress capture contained denied archive text")
    return {
        "denied": True,
        "events": [
            {"destination": event.destination, "allowed": event.allowed, "reason": event.reason} for event in events
        ],
    }


def run_p04_privacy_restore(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    _require_meta(runtime.vault)
    deny = _deny_matrix(runtime)
    egress = _egress_deny()
    _write_dinner(runtime.vault)
    execute_correction_command(
        runtime.vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=UID,
            field="amount",
            value=18.5,
            author="acceptance.p04c",
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

    cache = dest / "_meta" / "vault-scan-cache.sqlite3"
    if cache.exists():
        cache.write_bytes(b"corrupted-reconstructible")
        cache.unlink()

    from archive_cli.commands.recovery import activate_restored_archive
    from archive_cli.server import archive_read, archive_search

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
            query_text=QUERY,
        )
        mcp_read = archive_read(UID)
        mcp_search = archive_search(QUERY, limit=8)
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
    if DENIED_TOKEN in mcp_read or DENIED_TOKEN in mcp_search:
        raise AssertionError("denied sentinel appeared in restore MCP output")

    receipt_path = write_restore_receipt(receipt, runtime.root.parent / "restore-receipt.json")
    elapsed = round(time.monotonic() - started, 3)
    return {
        "id": "p04.privacy_restore",
        "status": "passed",
        "elapsed_seconds": elapsed,
        "deny": deny,
        "egress": egress,
        "encryption_tool": ENCRYPTION_TOOL,
        "archive_sha256": created["archive_sha256"],
        "restore_root": receipt.restore_root,
        "active_root_untouched": receipt.active_root_untouched,
        "cited_amount": 18.5,
        "canonical_uid": UID,
        "mcp_read_hit": True,
        "mcp_search_hit": True,
        "denied_token_leaked": False,
        "activate_rebuilt": bool(activated.get("rebuilt")),
        "receipt_path": str(receipt_path),
        "production_proven": False,
    }


register(
    Scenario(
        id="p04.privacy_restore",
        suite="p04",
        product_guarantee="Denied sources cannot leak through search/graph/egress, and encrypted restore returns the corrected canonical IDs",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("docker", "rust_engine", "hash_embeddings"),
        expected_artifacts=("restore-receipt.json",),
        run=run_p04_privacy_restore,
    )
)
