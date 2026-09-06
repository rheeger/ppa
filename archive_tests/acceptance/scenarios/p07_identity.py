"""P07-C acceptance: mistaken merge undo plus maintain receipts."""

from __future__ import annotations

import logging
import time
from typing import Any

from archive_engine.corrections import CorrectionCommandRequest, execute_correction_command, identity_receipts
from archive_sync.adapters.base import deterministic_provenance
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_vault.identity import redirect_target_uid, resolve_any, upsert_identity_map
from archive_vault.schema import PersonCard
from archive_vault.vault import read_note, read_note_by_uid, write_card

WINNER_UID = "hfa-person-p07cwin0001"
LOSER_UID = "hfa-person-p07close0001"
OTHER_UID = "hfa-person-p07coth0001"


def _person(uid: str, first: str, last: str, email: str) -> PersonCard:
    return PersonCard(
        uid=uid,
        type="person",
        source=["acceptance.p07c"],
        source_id=email,
        created="2026-09-06",
        updated="2026-09-06",
        summary=f"{first} {last}",
        first_name=first,
        last_name=last,
        emails=[email],
        tags=["p07-identity", "synthetic"],
    )


def _write(vault, rel: str, card: PersonCard, people: list[str] | None = None) -> None:
    data = card.model_dump(mode="python")
    if people:
        data["people"] = people
        card = PersonCard.model_validate(data)
    write_card(vault, rel, card, body=card.summary, provenance=deterministic_provenance(card, "acceptance.p07c"))
    upsert_identity_map(
        vault, f"[[{rel.split('/')[-1].removesuffix('.md')}]]", {"name": card.summary, "emails": card.emails}
    )


def run_p07_identity(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    _write(runtime.vault, "People/alex-winner.md", _person(WINNER_UID, "Alex", "Winner", "alex@p07c.test"))
    _write(runtime.vault, "People/blake-loser.md", _person(LOSER_UID, "Blake", "Loser", "blake@p07c.test"))
    _write(
        runtime.vault,
        "People/casey-other.md",
        _person(OTHER_UID, "Casey", "Other", "casey@p07c.test"),
        people=["[[blake-loser]]"],
    )
    before_graph = {
        "blake_email": resolve_any(runtime.vault, "email", "blake@p07c.test"),
        "casey_people": read_note(runtime.vault, "People/casey-other.md")[0].get("people"),
        "blake_redirect": redirect_target_uid(runtime.vault, LOSER_UID),
    }
    merged = execute_correction_command(
        runtime.vault,
        CorrectionCommandRequest(
            action="merge_identities",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="acceptance.p07c",
            reason="mistaken duplicate",
        ),
    )
    if merged.status != "active":
        raise AssertionError(f"merge was not active: {merged.status}")
    if read_note_by_uid(runtime.vault, LOSER_UID)[1].get("redirect_to") != WINNER_UID:
        raise AssertionError("historical loser UID is not resolvable as a redirect")
    if resolve_any(runtime.vault, "email", "blake@p07c.test") != "[[alex-winner]]":
        raise AssertionError("loser alias did not move to winner")

    winner_fm, winner_body, winner_prov = read_note(runtime.vault, "People/alex-winner.md")
    winner_fm = dict(winner_fm)
    winner_fm["title"] = "Later Title"
    write_card(
        runtime.vault,
        "People/alex-winner.md",
        PersonCard.model_validate(winner_fm),
        body=winner_body,
        provenance={**winner_prov, **deterministic_provenance(PersonCard.model_validate(winner_fm), "acceptance.p07c")},
    )

    undone = execute_correction_command(
        runtime.vault,
        CorrectionCommandRequest(
            action="undo_identity",
            decision_id=merged.decision_id,
            author="acceptance.p07c",
            reason="not the same person",
        ),
    )
    if undone.status == "blocked":
        raise AssertionError(f"undo blocked unexpectedly: {undone.identity_conflicts}")
    loser = read_note_by_uid(runtime.vault, LOSER_UID)
    if loser is None or loser[1].get("redirect_to"):
        raise AssertionError("undo did not restore loser card")
    if read_note(runtime.vault, "People/alex-winner.md")[0].get("title") != "Later Title":
        raise AssertionError("later unrelated winner edit was lost")
    if read_note(runtime.vault, "People/casey-other.md")[0].get("people") != ["[[blake-loser]]"]:
        raise AssertionError("reference rewrite was not inverted")

    receipts = identity_receipts(
        runtime.vault,
        {
            "winner_uid": WINNER_UID,
            "loser_uid": LOSER_UID,
            "winner_rel_path": "People/alex-winner.md",
            "loser_rel_path": "People/blake-loser.md",
            "winner_revision": undone.after_revision,
            "references": [{"uid": OTHER_UID, "rel_path": "People/casey-other.md"}],
        },
    )
    maintain_payload: dict[str, Any] = {}
    try:
        from archive_cli.commands.maintain import run_maintenance
        from archive_cli.store import DefaultArchiveStore

        store = DefaultArchiveStore(vault=runtime.vault)
        store.bootstrap()
        dirty = runtime.vault / "_meta" / "p07c-dirty-uids.txt"
        dirty.write_text("\n".join([WINNER_UID, LOSER_UID, OTHER_UID]) + "\n", encoding="utf-8")
        report = run_maintenance(
            store=store,
            logger=logging.getLogger("ppa.acceptance"),
            dry_run=False,
            run_processors=True,
            apply_processors=True,
            dirty_uids_path=str(dirty),
        )
        maintain_payload = {
            "publication_ok": report.publication.get("ok"),
            "failed_revision_uids": list(report.failed_revision_uids or []),
            "processor_reports": len(report.processor_reports or []),
        }
        if WINNER_UID in (report.failed_revision_uids or []) or LOSER_UID in (report.failed_revision_uids or []):
            raise AssertionError(f"maintain failed identity UIDs: {report.failed_revision_uids}")
    except Exception as exc:
        maintain_payload = {
            "error": f"{type(exc).__name__}: {exc}",
            "receipts": [item.to_payload() for item in receipts],
        }

    after_graph = {
        "blake_email": resolve_any(runtime.vault, "email", "blake@p07c.test"),
        "casey_people": read_note(runtime.vault, "People/casey-other.md")[0].get("people"),
        "blake_redirect": redirect_target_uid(runtime.vault, LOSER_UID),
        "winner_title": read_note(runtime.vault, "People/alex-winner.md")[0].get("title"),
    }
    return {
        "id": "p07.identity.undo",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "merge_decision_id": merged.decision_id,
        "undo_decision_id": undone.decision_id,
        "before_graph": before_graph,
        "after_graph": after_graph,
        "receipt_output_uids": [out.uid for receipt in merged.receipts for out in receipt.outputs],
        "maintain": maintain_payload,
        "cited_people": after_graph["casey_people"],
    }


register(
    Scenario(
        id="p07.identity.undo",
        suite="p07",
        product_guarantee="A mistaken person merge can be undone with later unrelated edits and references preserved",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=("p07-identity.json",),
        run=run_p07_identity,
    )
)
