"""P07-B acceptance: a corrected dinner amount survives source replay."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from archive_engine.corrections import CorrectionCommandRequest, execute_correction_command
from archive_engine.recovery_manifest import generate_manifest
from archive_sync.adapters.base import BaseAdapter, deterministic_provenance
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_vault.decisions import open_conflicts_for
from archive_vault.provenance import PROVENANCE_METHOD_HUMAN
from archive_vault.schema import FinanceCard
from archive_vault.vault import read_note, write_card

UID = "hfa-finance-p07accep01"
REL = "Finance/2026-09/hfa-finance-p07accep01.md"


class _Replay(BaseAdapter):
    source_id = "amex"

    def fetch(self, vault_path, cursor, config=None, **kwargs):
        return []

    def to_card(self, item):
        raise AssertionError("acceptance replay is write-path only")


def _card(amount: float, *, source_id: str = "amex:p07-dinner") -> FinanceCard:
    return FinanceCard(
        uid=UID,
        type="finance",
        source=["amex"],
        source_id=source_id,
        created="2026-09-01",
        updated="2026-09-01",
        summary="P07 Acceptance Dinner",
        amount=amount,
        currency="USD",
        counterparty="Restaurant",
        note="synthetic dinner for correction replay",
    )


def _replay(vault: Path, amount: float, *, source_id: str) -> None:
    card = _card(amount, source_id=source_id)
    _Replay()._replace_generic_card(
        vault,
        Path(REL),
        card,
        "dinner",
        deterministic_provenance(card, "amex"),
    )


def run_p07_corrections(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    (runtime.vault / "Finance" / "2026-09").mkdir(parents=True, exist_ok=True)
    meta = runtime.vault / "_meta"
    if not (meta / "own-emails.json").is_file():
        (meta / "own-emails.json").write_text("[]\n", encoding="utf-8")
    if not (meta / "nicknames.json").is_file():
        (meta / "nicknames.json").write_text("{}\n", encoding="utf-8")
    if not (meta / "ppa-config.json").is_file():
        (meta / "ppa-config.json").write_text("{}\n", encoding="utf-8")
    if not (meta / "llm-config.json").is_file():
        (meta / "llm-config.json").write_text(
            '{"primary": {"provider": "gemini", "model": "fixture"}}\n',
            encoding="utf-8",
        )
    card = _card(42.0)
    write_card(
        runtime.vault,
        REL,
        card,
        body="dinner",
        provenance=deterministic_provenance(card, "amex"),
    )
    applied = execute_correction_command(
        runtime.vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=UID,
            field="amount",
            value=38.0,
            author="acceptance.p07b",
            reason="paper receipt was 38",
            rel_path=REL,
        ),
    )
    _replay(runtime.vault, 42.0, source_id="amex:p07-dinner")
    older = read_note(runtime.vault, REL)
    _replay(runtime.vault, 50.0, source_id="amex:p07-dinner-v2")
    newer = read_note(runtime.vault, REL)
    if older[0]["amount"] != 38.0 or newer[0]["amount"] != 38.0:
        raise AssertionError(f"source replay erased correction: older={older[0]['amount']} newer={newer[0]['amount']}")
    if newer[2]["amount"].method != PROVENANCE_METHOD_HUMAN:
        raise AssertionError("correction provenance was relabeled as source")
    if newer[0]["source"] != ["amex"]:
        raise AssertionError("correction was written onto the source field list")
    conflicts = open_conflicts_for(runtime.vault, UID, "amount")
    if not conflicts:
        raise AssertionError("newer source replay did not emit an explicit conflict")
    cleared = execute_correction_command(
        runtime.vault,
        CorrectionCommandRequest(
            action="clear_override",
            uid=UID,
            field="amount",
            author="acceptance.p07b",
            reason="restore provider amount",
            rel_path=REL,
        ),
    )
    restored = read_note(runtime.vault, REL)
    if restored[0]["amount"] != 50.0:
        raise AssertionError(f"clear-override did not restore source amount: {restored[0]['amount']}")
    manifest = generate_manifest(runtime.vault)
    if manifest["checkpoint"]["status"] != "available":
        raise AssertionError("recovery checkpoint did not bind after journaled correction")
    return {
        "id": "p07.corrections.replay",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "uid": UID,
        "corrected_amount": 38.0,
        "replayed_older_amount": older[0]["amount"],
        "replayed_newer_amount": newer[0]["amount"],
        "cleared_amount": restored[0]["amount"],
        "decision_id": applied.decision_id,
        "card_mutation_id": applied.card_mutation_id,
        "decision_mutation_id": applied.decision_mutation_id,
        "clear_decision_id": cleared.decision_id if not isinstance(cleared, list) else "",
        "conflict_ids": [item.decision_id for item in conflicts],
        "checkpoint": manifest["checkpoint"],
        "cited_amount": newer[0]["amount"],
        "cited_provenance_method": newer[2]["amount"].method,
    }


register(
    Scenario(
        id="p07.corrections.replay",
        suite="p07",
        product_guarantee="A human amount correction survives older and newer source replay and remains the amount a query would cite",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=("p07-corrections.json",),
        run=run_p07_corrections,
    )
)
