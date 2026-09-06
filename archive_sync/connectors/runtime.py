"""Connector runtime: validate, fetch, normalize, persist, then propose a cursor.

Durable writes emit P02 ``ChangeRecord``s and P03 ``OutputReceipt``s.
The sample writer still skip-rewrites existing files; Gmail/calendar use the
correction-aware adapter writer so P07 overrides survive replay.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping
from pathlib import Path

from archive_engine.contracts import AccessContext, ArchiveIdentity, ChangeRecord, OutputReceipt, OutputRevision
from archive_engine.errors import IncompatibleContractError
from archive_sync.connectors.contracts import (
    CanonicalProposal,
    CanonicalWriter,
    ConnectorManifest,
    ConnectorRunResult,
    PersistResult,
    parse_manifest,
    require_access,
    validate_manifest,
    validate_proposal,
)
from archive_sync.connectors.registry import get_connector_factory
from archive_vault.paths import resolve_contained_path
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import validate_card_strict
from archive_vault.vault import write_card

logger = logging.getLogger("ppa.connectors")


def _revision_for(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _provenance_entries(raw: Mapping[str, object]) -> dict[str, ProvenanceEntry]:
    out: dict[str, ProvenanceEntry] = {}
    for field, value in raw.items():
        if isinstance(value, ProvenanceEntry):
            out[field] = value
            continue
        if not isinstance(value, Mapping):
            raise IncompatibleContractError(f"provenance.{field} must be an object")
        method = str(value.get("method") or "deterministic")
        if method != "deterministic" and field != "summary":
            # Connectors emit sourced facts; inferred methods stay labeled, never relabeled.
            pass
        out[field] = ProvenanceEntry(
            source=str(value.get("source") or ""),
            date=str(value.get("date") or ""),
            method=method,
        )
    return out


class ContainedVaultWriter:
    """Canonical writer that persists through ``write_card`` + contained paths."""

    def __init__(self, vault: Path) -> None:
        self.vault = Path(vault)
        self.write_attempts = 0
        self.uid_to_rel: dict[str, str] = {}

    def write_canonical(
        self,
        proposal: CanonicalProposal,
        *,
        identity: ArchiveIdentity,
        access: AccessContext,
    ) -> PersistResult:
        require_access(identity=identity, access=access)
        self.write_attempts += 1
        uid = str(proposal.card.get("uid") or "")
        target = resolve_contained_path(self.vault, proposal.rel_path, purpose="write", create_parents=True)
        if target.is_file():
            self.uid_to_rel[uid] = proposal.rel_path
            return PersistResult(
                uid=uid,
                rel_path=proposal.rel_path,
                revision=_revision_for(target),
                created=False,
                duplicate=True,
            )
        card = validate_card_strict(dict(proposal.card))
        write_card(
            self.vault,
            proposal.rel_path,
            card,
            body=proposal.body,
            provenance=_provenance_entries(proposal.provenance),
        )
        written = resolve_contained_path(self.vault, proposal.rel_path, purpose="read")
        self.uid_to_rel[uid] = proposal.rel_path
        return PersistResult(
            uid=uid,
            rel_path=proposal.rel_path,
            revision=_revision_for(written),
            created=True,
            duplicate=False,
        )


def execute_from_manifest(
    payload: Mapping[str, object],
    *,
    identity: ArchiveIdentity,
    access: AccessContext,
    writer: CanonicalWriter,
    cursor: Mapping[str, object] | None = None,
    run_id: str = "p08a",
) -> ConnectorRunResult:
    """Validate ``payload`` before resolving a factory or touching the writer."""

    manifest = parse_manifest(payload)
    return execute_connector(
        manifest.connector_id,
        identity=identity,
        access=access,
        writer=writer,
        cursor=cursor,
        run_id=run_id,
        manifest=manifest,
    )


def execute_connector(
    connector_id: str,
    *,
    identity: ArchiveIdentity,
    access: AccessContext,
    writer: CanonicalWriter,
    cursor: Mapping[str, object] | None = None,
    run_id: str = "p08a",
    manifest: ConnectorManifest | None = None,
    context: Mapping[str, object] | None = None,
    pending_p02_wiring: str = "pending",
    pending_p03_wiring: str = "pending",
) -> ConnectorRunResult:
    require_access(identity=identity, access=access)
    factory = get_connector_factory(connector_id)
    connector = factory()
    bind = getattr(connector, "bind_context", None)
    if callable(bind) and context:
        bind(context)
    resolved = validate_manifest(manifest or connector.manifest())
    if resolved.connector_id != connector_id:
        raise IncompatibleContractError("manifest connector_id does not match requested connector")
    batch = connector.fetch(cursor=dict(cursor or {}), identity=identity, access=access)
    if len(batch.records) > resolved.batch_limit:
        raise IncompatibleContractError(f"batch {batch.batch_id} exceeds batch_limit {resolved.batch_limit}")
    proposals = connector.normalize(batch, identity=identity, access=access)
    persists: list[PersistResult] = []
    changes: list[ChangeRecord] = []
    receipts: list[OutputReceipt] = []
    sequence = 1
    for proposal in proposals:
        validate_proposal(proposal, resolved)
        persist = writer.write_canonical(proposal, identity=identity, access=access)
        persists.append(persist)
        if persist.created or not persist.duplicate:
            changes.append(
                ChangeRecord(
                    archive_id=identity.archive_id,
                    sequence=sequence,
                    mutation_id=f"{run_id}:{persist.uid}:{persist.revision[:12]}",
                    uid=persist.uid,
                    operation="upsert",
                    before_revision="",
                    after_revision=persist.revision,
                    source=proposal.identity.source,
                    account=proposal.identity.account_scope,
                    run_id=run_id,
                    committed=True,
                )
            )
            receipts.append(
                OutputReceipt(
                    processor=f"connector.{resolved.connector_id}",
                    processor_version=resolved.connector_version,
                    input_uid=persist.uid,
                    input_revision=persist.revision,
                    status="completed",
                    outputs=(OutputRevision(uid=persist.uid, revision=persist.revision),),
                )
            )
            sequence += 1
            logger.info(
                "connector persist connector_id=%s uid=%s account=%s created=%s",
                resolved.connector_id,
                persist.uid,
                proposal.identity.account_scope,
                persist.created,
            )
        else:
            logger.info(
                "connector replay connector_id=%s uid=%s account=%s duplicate=true",
                resolved.connector_id,
                persist.uid,
                proposal.identity.account_scope,
            )
    return ConnectorRunResult(
        connector_id=resolved.connector_id,
        manifest=resolved,
        batch_id=batch.batch_id,
        proposals=tuple(proposals),
        persists=tuple(persists),
        changes=tuple(changes),
        committed_cursor=dict(batch.cursor_after_candidate),
        proposed_cursor=dict(batch.cursor_after_candidate),
        receipts=tuple(receipts),
        pending_p02_wiring=pending_p02_wiring,
        pending_p03_wiring=pending_p03_wiring,
    )
