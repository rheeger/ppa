"""Archive-sync Copilot finance adapter tests."""

from archive_sync.adapters.copilot_finance import CopilotFinanceAdapter
from hfa.config import PPAConfig
from hfa.schema import FinanceCard


def test_fetch_applies_amount_threshold(tmp_path):
    csv_path = tmp_path / "copilot.csv"
    csv_path.write_text(
        "date,name,amount,status,category,parent category,excluded,tags,type,account,account mask,note,recurring\n"
        "2026-03-01,Coffee,-5.00,posted,Food,Meals,false,,regular,Checking,1234,,\n"
        "2026-03-02,Flight,-120.00,pending,Travel,Trips,true,business,regular,Checking,1234,work trip,Monthly\n"
    )
    items = CopilotFinanceAdapter().fetch(
        "/dev/null",
        {},
        config=PPAConfig(finance_min_amount=20.0),
        csv_path=str(csv_path),
    )
    assert len(items) == 1
    assert items[0]["merchant"] == "Flight"
    assert items[0]["transaction_status"] == "pending"
    assert items[0]["parent_category"] == "Trips"
    assert items[0]["excluded"] is True
    assert items[0]["provider_tags"] == ["business"]
    assert items[0]["recurring_label"] == "Monthly"


def test_to_card_returns_valid_finance_card():
    card, provenance, _ = CopilotFinanceAdapter().to_card(
        {
            "date": "2026-03-02",
            "merchant": "Flight",
            "amount": -120.0,
            "currency": "USD",
            "category": "Travel",
            "parent_category": "Trips",
            "account": "Checking",
            "account_mask": "1234",
            "transaction_status": "pending",
            "transaction_type": "regular",
            "excluded": True,
            "provider_tags": ["business"],
            "note": "work trip",
            "recurring_label": "Monthly",
        }
    )
    assert isinstance(card, FinanceCard)
    assert card.category == "Travel"
    assert card.parent_category == "Trips"
    assert card.account_mask == "1234"
    assert card.transaction_status == "pending"
    assert card.transaction_type == "regular"
    assert card.excluded is True
    assert card.provider_tags == ["business"]
    assert card.note == "work trip"
    assert card.recurring_label == "Monthly"
    assert provenance["amount"].source == "copilot"


def test_copilot_adapter_streams_batches_without_uid_preload(tmp_path):
    csv_path = tmp_path / "copilot.csv"
    csv_path.write_text(
        "date,name,amount,status,category,parent category,excluded,tags,type,account,account mask,note,recurring\n"
        "2026-03-01,Flight,-120.00,posted,Travel,Trips,false,,regular,Checking,1234,,\n"
        "2026-03-02,Hotel,-220.00,posted,Travel,Trips,false,,regular,Checking,1234,,\n"
        "2026-03-03,Dinner,-80.00,posted,Food,Meals,false,,regular,Checking,1234,,\n"
    )
    adapter = CopilotFinanceAdapter()
    batches = list(
        adapter.fetch_batches(
            "/dev/null",
            {},
            config=PPAConfig(finance_min_amount=20.0),
            csv_path=str(csv_path),
            batch_size=2,
        )
    )
    assert adapter.preload_existing_uid_index is False
    assert adapter.enable_person_resolution is False
    assert [len(batch.items) for batch in batches] == [2, 1]
