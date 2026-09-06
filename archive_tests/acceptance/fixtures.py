"""Synthetic vault fixtures for acceptance scenarios."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from archive_tests.conftest import assert_owned_test_root
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card

BASELINE_UID = "hfa-person-p04abase001"
BASELINE_REL_PATH = "People/p04-acceptance-tracer.md"
BASELINE_QUERY = "P04 Acceptance Tracer"
BASELINE_BODY = (
    "P04 Acceptance Tracer is a synthetic person card used only by the isolated "
    "product-acceptance harness. It must travel vault write, warehouse materialize, "
    "Rust serving, and MCP/CLI read without touching the seed vault."
)


def _provenance(*fields: str) -> dict[str, ProvenanceEntry]:
    return {field: ProvenanceEntry("acceptance.p04a", "2026-09-06", "deterministic") for field in fields}


def init_vault(vault: Path, *, owned_root: Path) -> Path:
    """Create a minimal vault skeleton under an owned test root."""

    assert_owned_test_root(owned_root)
    vault = Path(vault)
    if owned_root.resolve() not in vault.resolve().parents and vault.resolve() != owned_root.resolve():
        raise AssertionError(f"vault {vault} is not under owned root {owned_root}")
    for name in (
        "People",
        "Email",
        "Calendar",
        "_templates",
        ".obsidian",
        "_meta",
    ):
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
