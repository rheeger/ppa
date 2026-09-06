"""Recovery command functions (P07-D).

Engine backup/restore plus an explicit activation step that rebuilds derived
indexes. P09 owns central CLI/parser registration — this module is callable
but is not wired into ``archive_cli.__main__``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from archive_engine.recovery import (
    RestoreReceipt,
    create_encrypted_bundle,
    restore_encrypted_bundle,
    snapshot_tree_hashes,
    verify_encrypted_bundle,
)


def backup_archive(
    *,
    vault: Path,
    backup_base: Path,
    passphrase: str,
    logger: logging.Logger,
    include_reconstructible: bool = False,
) -> dict[str, Any]:
    """Create an encrypted vault bundle using the existing encrypt script."""

    logger.info("recovery_backup_start vault=%s backup_base=%s", vault, backup_base)
    result = create_encrypted_bundle(
        vault,
        backup_base,
        passphrase=passphrase,
        include_reconstructible=include_reconstructible,
    )
    logger.info("recovery_backup_done archive=%s sha256=%s", result.get("archive"), result.get("archive_sha256"))
    return result


def verify_backup(
    *,
    logger: logging.Logger,
    backup_base: Path | None = None,
    archive_file: Path | None = None,
    passphrase: str | None = None,
) -> dict[str, Any]:
    """Verify checksum and optional decrypt of an encrypted bundle."""

    logger.info("recovery_verify_start backup_base=%s archive=%s", backup_base, archive_file)
    result = verify_encrypted_bundle(
        backup_base=backup_base,
        archive_file=archive_file,
        passphrase=passphrase,
    )
    logger.info("recovery_verify_done sha256=%s", result.get("archive_sha256"))
    return result


def restore_archive(
    *,
    dest: Path,
    passphrase: str,
    logger: logging.Logger,
    backup_base: Path | None = None,
    archive_file: Path | None = None,
    active_root: Path | None = None,
    source_vault: Path | None = None,
) -> dict[str, Any]:
    """Restore into a new root. Refuses to overwrite the active vault."""

    logger.info("recovery_restore_start dest=%s active_root=%s", dest, active_root)
    source_hashes = snapshot_tree_hashes(source_vault) if source_vault else None
    receipt = restore_encrypted_bundle(
        dest=dest,
        passphrase=passphrase,
        backup_base=backup_base,
        archive_file=archive_file,
        active_root=active_root,
        source_vault=source_vault,
        source_hashes=source_hashes,
        strip_reconstructible=True,
    )
    logger.info(
        "recovery_restore_done dest=%s files=%s untouched=%s",
        receipt.restore_root,
        receipt.extracted_files,
        receipt.active_root_untouched,
    )
    return receipt.to_payload()


def activate_restored_archive(
    *,
    vault: Path,
    logger: logging.Logger,
    receipt: dict[str, Any] | RestoreReceipt | None = None,
    query_uid: str = "",
    query_text: str = "",
) -> dict[str, Any]:
    """Rebuild reconstructible indexes and query the restored vault.

    Derived Postgres / serving state is created here. Restore itself must not
    depend on a saved generation.
    """

    from archive_cli.commands.admin import rebuild_indexes
    from archive_cli.store import DefaultArchiveStore

    logger.info("recovery_activate_start vault=%s", vault)
    store = DefaultArchiveStore(vault=vault)
    bootstrap = store.bootstrap()
    rebuilt = rebuild_indexes(store=store, logger=logger, force_full=True, no_cache=True)
    query: dict[str, Any] = {}
    if query_uid:
        from archive_cli.commands.read import read as read_card

        query["read"] = read_card(query_uid, store=store, logger=logger)
    if query_text:
        from archive_cli.commands.search import search as search_cmd

        query["search"] = search_cmd(query_text, limit=8, store=store, logger=logger)
    payload = receipt.to_payload() if isinstance(receipt, RestoreReceipt) else dict(receipt or {})
    payload["status"] = "activated"
    payload["rebuilt"] = True
    payload["bootstrap"] = {key: bootstrap.get(key) for key in ("schema", "ok") if key in bootstrap} or {"ok": True}
    payload["rebuild"] = {
        "cards": rebuilt.get("cards") if isinstance(rebuilt, dict) else rebuilt,
        "serving_index_ok": (rebuilt.get("serving_index") or {}).get("ok") if isinstance(rebuilt, dict) else None,
    }
    payload["query"] = query
    logger.info("recovery_activate_done rebuilt=%s", True)
    return payload
