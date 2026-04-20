"""Phase 6.5 Step 1c — declarative edge rules (ships_for via order_number, finance wikilink)."""

from __future__ import annotations

from pathlib import Path

from archive_cli.materializer import (_build_edges, _build_person_lookup,
                                      build_target_field_index)
from archive_cli.scanner import CanonicalRow
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import FinanceCard, PurchaseCard, ShipmentCard
from archive_vault.vault import read_note_file, write_card


def _prov(*fields: str) -> dict[str, ProvenanceEntry]:
    return {field: ProvenanceEntry("seed-test", "2026-04-23", "deterministic") for field in fields}


def test_shipment_linked_purchase_via_order_number_resolves(tmp_path: Path) -> None:
    vault = tmp_path / "v"
    (vault / "Purchases").mkdir(parents=True)
    (vault / "Shipments").mkdir(parents=True)
    (vault / "_meta").mkdir()
    for name in ("identity-map.json", "sync-state.json"):
        (vault / "_meta" / name).write_text("{}" if name.endswith("json") else "{}", encoding="utf-8")
    (vault / "_meta" / "dedup-candidates.json").write_text("[]", encoding="utf-8")

    purchase = PurchaseCard(
        uid="hfa-purchase-testord1",
        type="purchase",
        source=["amazon"],
        source_id="amz-1",
        created="2026-04-23",
        updated="2026-04-23",
        vendor="Test Vendor",
        order_number="P300275673",
        total=12.34,
    )
    write_card(
        vault,
        "Purchases/hfa-purchase-testord1.md",
        purchase,
        body="",
        provenance=_prov(
            "vendor",
            "order_number",
            "total",
            "source_email",
            "created",
            "updated",
            "summary",
        ),
    )
    shipment = ShipmentCard(
        uid="hfa-shipment-test1",
        type="shipment",
        source=["amazon"],
        source_id="ship-1",
        created="2026-04-23",
        updated="2026-04-23",
        carrier="UPS",
        tracking_number="1Z999",
        linked_purchase="P300275673",
    )
    write_card(
        vault,
        "Shipments/hfa-shipment-test1.md",
        shipment,
        body="",
        provenance=_prov(
            "carrier",
            "tracking_number",
            "linked_purchase",
            "created",
            "updated",
            "summary",
        ),
    )

    rows: list[CanonicalRow] = []
    for rel in ("Purchases/hfa-purchase-testord1.md", "Shipments/hfa-shipment-test1.md"):
        note = read_note_file(vault / rel, vault_root=str(vault))
        from archive_vault.schema import validate_card_permissive

        rows.append(
            CanonicalRow(rel_path=rel, frontmatter=dict(note.frontmatter), card=validate_card_permissive(note.frontmatter))
        )
    slug_map = {Path(rel).stem: rel for rel in ("Purchases/hfa-purchase-testord1.md", "Shipments/hfa-shipment-test1.md")}
    path_to_uid = {r.rel_path: str(r.card.uid) for r in rows}
    person_lookup = _build_person_lookup(rows)
    tfi = build_target_field_index(rows)

    ship_note = read_note_file(vault / "Shipments/hfa-shipment-test1.md", vault_root=str(vault))
    from archive_vault.schema import validate_card_permissive

    ship_card = validate_card_permissive(ship_note.frontmatter)
    edges = _build_edges(
        rel_path="Shipments/hfa-shipment-test1.md",
        frontmatter=dict(ship_note.frontmatter),
        card=ship_card,
        body=ship_note.body,
        slug_map=slug_map,
        path_to_uid=path_to_uid,
        person_lookup=person_lookup,
        target_field_index=tfi,
    )
    assert any(
        e["edge_type"] == "ships_for"
        and e["target_path"] == "Purchases/hfa-purchase-testord1.md"
        and e["target_uid"] == "hfa-purchase-testord1"
        for e in edges
    )


def test_shipment_linked_purchase_no_match_no_ships_for_edge(tmp_path: Path) -> None:
    vault = tmp_path / "v2"
    (vault / "Shipments").mkdir(parents=True)
    (vault / "_meta").mkdir()
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "dedup-candidates.json").write_text("[]", encoding="utf-8")

    shipment = ShipmentCard(
        uid="hfa-shipment-orphan",
        type="shipment",
        source=["amazon"],
        source_id="ship-o",
        created="2026-04-23",
        updated="2026-04-23",
        carrier="UPS",
        tracking_number="1Z000",
        linked_purchase="NONEXISTENT_ORDER_REF",
    )
    write_card(
        vault,
        "Shipments/hfa-shipment-orphan.md",
        shipment,
        body="",
        provenance=_prov("carrier", "tracking_number", "linked_purchase", "created", "updated", "summary"),
    )
    from archive_vault.schema import validate_card_permissive

    note = read_note_file(vault / "Shipments/hfa-shipment-orphan.md", vault_root=str(vault))
    row = CanonicalRow(
        rel_path="Shipments/hfa-shipment-orphan.md",
        frontmatter=dict(note.frontmatter),
        card=validate_card_permissive(note.frontmatter),
    )
    tfi = build_target_field_index([row])
    edges = _build_edges(
        rel_path=row.rel_path,
        frontmatter=dict(note.frontmatter),
        card=row.card,
        body=note.body,
        slug_map={"hfa-shipment-orphan": row.rel_path},
        path_to_uid={row.rel_path: row.card.uid},
        person_lookup={},
        target_field_index=tfi,
    )
    assert not any(e["edge_type"] == "ships_for" for e in edges)


def test_finance_source_email_wikilink_still_materializes(tmp_path: Path) -> None:
    vault = tmp_path / "v3"
    (vault / "Finance").mkdir(parents=True)
    (vault / "Email").mkdir(parents=True)
    (vault / "_meta").mkdir()
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "dedup-candidates.json").write_text("[]", encoding="utf-8")

    from archive_vault.schema import EmailMessageCard

    email = EmailMessageCard(
        uid="hfa-email-msg-xyz",
        type="email_message",
        source=["gmail"],
        source_id="m1",
        created="2026-04-23",
        updated="2026-04-23",
        summary="Receipt",
        gmail_message_id="msg-1",
        gmail_thread_id="t1",
    )
    write_card(
        vault,
        "Email/hfa-email-msg-xyz.md",
        email,
        body="hi",
        provenance=_prov("summary", "gmail_message_id", "gmail_thread_id", "created", "updated"),
    )
    fin = FinanceCard(
        uid="hfa-finance-abc",
        type="finance",
        source=["mint"],
        source_id="f1",
        created="2026-04-23",
        updated="2026-04-23",
        summary="Charge",
        amount=-9.99,
        currency="USD",
        counterparty="DoorDash",
        source_email="[[hfa-email-msg-xyz]]",
    )
    write_card(
        vault,
        "Finance/hfa-finance-abc.md",
        fin,
        body="",
        provenance=_prov(
            "amount",
            "currency",
            "counterparty",
            "source_email",
            "created",
            "updated",
            "summary",
        ),
    )
    from archive_vault.schema import validate_card_permissive

    rows = []
    for rel in ("Email/hfa-email-msg-xyz.md", "Finance/hfa-finance-abc.md"):
        n = read_note_file(vault / rel, vault_root=str(vault))
        rows.append(CanonicalRow(rel_path=rel, frontmatter=dict(n.frontmatter), card=validate_card_permissive(n.frontmatter)))
    slug_map = {"hfa-email-msg-xyz": "Email/hfa-email-msg-xyz.md", "hfa-finance-abc": "Finance/hfa-finance-abc.md"}
    path_to_uid = {r.rel_path: str(r.card.uid) for r in rows}
    person_lookup = _build_person_lookup(rows)
    tfi = build_target_field_index(rows)
    fnote = read_note_file(vault / "Finance/hfa-finance-abc.md", vault_root=str(vault))
    fcard = validate_card_permissive(fnote.frontmatter)
    edges = _build_edges(
        rel_path="Finance/hfa-finance-abc.md",
        frontmatter=dict(fnote.frontmatter),
        card=fcard,
        body=fnote.body,
        slug_map=slug_map,
        path_to_uid=path_to_uid,
        person_lookup=person_lookup,
        target_field_index=tfi,
    )
    assert any(
        e["edge_type"] == "finance_from_email" and e["target_uid"] == "hfa-email-msg-xyz" for e in edges
    )
