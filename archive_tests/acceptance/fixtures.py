"""Synthetic vault fixtures for acceptance scenarios."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from archive_tests.conftest import assert_owned_test_root
from archive_vault.card_contracts import CARD_TYPE_SPECS
from archive_vault.provenance import PROVENANCE_EXEMPT_FIELDS, ProvenanceEntry
from archive_vault.schema import BaseCard, PersonCard
from archive_vault.vault import write_card

CORPUS_SEED = 20260906
CONTRACT_VERSION = "p04b.1"
PROVENANCE_SOURCE = "acceptance.p04b"

BASELINE_UID = "hfa-person-p04abase001"
BASELINE_REL_PATH = "People/p04-acceptance-tracer.md"
BASELINE_QUERY = "P04 Acceptance Tracer"
BASELINE_BODY = (
    "P04 Acceptance Tracer is a synthetic person card used only by the isolated "
    "product-acceptance harness. It must travel vault write, warehouse materialize, "
    "Rust serving, and MCP/CLI read without touching the seed vault."
)


def _provenance(*fields: str, source: str = "acceptance.p04a") -> dict[str, ProvenanceEntry]:
    return {field: ProvenanceEntry(source, "2026-09-06", "deterministic") for field in fields}


def provenance_for(card: BaseCard, *, source: str = PROVENANCE_SOURCE) -> dict[str, ProvenanceEntry]:
    """Emit provenance for every populated, non-exempt field on ``card``."""

    fields: list[str] = []
    for key, value in card.model_dump(mode="python").items():
        if key in PROVENANCE_EXEMPT_FIELDS:
            continue
        if value in ("", [], None, {}, False):
            continue
        fields.append(key)
    return _provenance(*fields, source=source)


def init_vault(vault: Path, *, owned_root: Path) -> Path:
    """Create a vault skeleton under an owned test root (all card families)."""

    assert_owned_test_root(owned_root)
    vault = Path(vault)
    if owned_root.resolve() not in vault.resolve().parents and vault.resolve() != owned_root.resolve():
        raise AssertionError(f"vault {vault} is not under owned root {owned_root}")
    families = {spec.rel_path_family for spec in CARD_TYPE_SPECS.values()}
    for name in sorted(families | {"_templates", ".obsidian", "_meta"}):
        (vault / name).mkdir(parents=True, exist_ok=True)
    meta = vault / "_meta"
    (meta / "identity-map.json").write_text("{}", encoding="utf-8")
    (meta / "sync-state.json").write_text("{}", encoding="utf-8")
    (meta / "dedup-candidates.json").write_text(json.dumps([]), encoding="utf-8")
    return vault


def write_baseline_person_card(vault: Path, *, owned_root: Path) -> dict[str, str]:
    """Write the one-card baseline fixture via the canonical ``write_card`` path."""

    init_vault(vault, owned_root=owned_root)
    card = PersonCard(
        uid=BASELINE_UID,
        type="person",
        source=["acceptance.p04a"],
        source_id="p04-acceptance-tracer@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=BASELINE_QUERY,
        first_name="P04",
        last_name="Tracer",
        emails=["p04-acceptance-tracer@example.test"],
        company="Acceptance Harness",
        title="Baseline Fixture",
        description=BASELINE_BODY,
        tags=["p04-acceptance", "synthetic"],
    )
    path = write_card(
        vault,
        BASELINE_REL_PATH,
        card,
        body=BASELINE_BODY,
        provenance=_provenance(
            "summary",
            "first_name",
            "last_name",
            "emails",
            "company",
            "title",
            "description",
            "tags",
        ),
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        "uid": BASELINE_UID,
        "rel_path": BASELINE_REL_PATH,
        "query": BASELINE_QUERY,
        "path": str(path),
        "sha256": digest,
    }
