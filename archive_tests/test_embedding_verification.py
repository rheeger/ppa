"""Phase 5 embedding verification tests (Postgres + fixture embedding provider)."""

from __future__ import annotations

import json
import math
import re
import uuid
from pathlib import Path

import pytest

from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.server import (
    archive_embed_pending,
    archive_hybrid_search,
    archive_rebuild_indexes,
    archive_vector_search,
)
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card


class Phase5SemanticProvider:
    """Deterministic 10-dim topic-bucket provider covering new derived types."""

    name = "fixture-phase5"

    def __init__(self, *, model: str, dimension: int = 10):
        self.model = model
        self.dimension = dimension

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_text(t) for t in texts]

    def _embed_text(self, text: str) -> list[float]:
        lowered = text.lower()
        topics = [
            ("jane", "smith", "person", "donor", "endaoment"),
            ("board", "dinner", "thread", "email", "conversation"),
            ("calendar", "meeting", "event", "schedule"),
            ("doordash", "hero", "restaurant", "meal", "order", "turkey", "delivery"),
            ("uber", "lyft", "ride", "pickup", "dropoff", "airport", "downtown"),
            ("united", "flight", "jfk", "sfo", "airline", "departure", "boarding"),
            ("amazon", "purchase", "order", "philips", "hue", "shipping"),
            (
                "hotel",
                "airbnb",
                "accommodation",
                "check-in",
                "checkout",
                "handlery",
                "union square",
                "san francisco",
            ),
            ("subscription", "netflix", "spotify", "renewed", "cancelled"),
            ("payroll", "salary", "gross", "net", "deductions"),
        ]
        vector = [0.0] * self.dimension
        for idx, keywords in enumerate(topics):
            for kw in keywords:
                if kw in lowered:
                    vector[idx] += 1.0
        if not any(vector):
            vector[0] = 0.1
        norm = math.sqrt(sum(v * v for v in vector))
        return [v / norm for v in vector]


def _common_provenance(source: str, *fields: str) -> dict[str, ProvenanceEntry]:
    return {f: ProvenanceEntry(source, "2026-03-10", "deterministic") for f in fields}


def _seed_phase5_vault(vault: Path) -> None:
    """Seed vault with person cards + linked transaction cards for verification."""
    from archive_vault.schema import CARD_TYPES

    jane = PersonCard(
        uid="hfa-person-jane11111111",
        type="person",
        source=["contacts.apple"],
        source_id="jane@example.com",
        created="2026-03-08",
        updated="2026-03-10",
        summary="Jane Smith",
        first_name="Jane",
        last_name="Smith",
        emails=["jane@example.com"],
        company="Endaoment",
        title="Donor Operations Lead",
    )
    write_card(
        vault,
        "People/jane-smith.md",
        jane,
        body="Jane leads donor support at Endaoment.",
        provenance=_common_provenance(
            "contacts.apple",
            "summary",
            "first_name",
            "last_name",
            "emails",
            "company",
            "title",
        ),
    )

    CalendarEventCard = CARD_TYPES["calendar_event"]
    calendar_event = CalendarEventCard(
        uid="hfa-calendar-event-test0001",
        type="calendar_event",
        source=["google.calendar"],
        source_id="endaoment-gitcoin-impact-pool",
        created="2025-12-10",
        updated="2025-12-10",
        summary="Endaoment Gitcoin Universal Impact Pool",
        account_email="robbie@example.com",
        calendar_id="primary",
        event_id="endaoment-gitcoin-impact-pool",
        ical_uid="endaoment-gitcoin-impact-pool@google.com",
        status="confirmed",
        title="Endaoment Gitcoin Universal Impact Pool",
        description="Meeting about the Endaoment Gitcoin Universal Impact Pool.",
        start_at="2025-12-10T17:00:00Z",
        end_at="2025-12-10T18:00:00Z",
        timezone="UTC",
        meeting_transcripts=["[[endaoment-gitcoin-impact-pool-transcript]]"],
    )
    write_card(
        vault,
        "Calendar/endaoment-gitcoin-impact-pool.md",
        calendar_event,
        body="Calendar event linked to [[endaoment-gitcoin-impact-pool-transcript]].",
        provenance=_common_provenance(
            "google.calendar",
            "summary",
            "account_email",
            "calendar_id",
            "event_id",
            "ical_uid",
            "status",
            "title",
            "description",
            "start_at",
            "end_at",
            "timezone",
            "meeting_transcripts",
        ),
    )

    MeetingTranscriptCard = CARD_TYPES["meeting_transcript"]
    meeting_transcript = MeetingTranscriptCard(
        uid="hfa-meeting-transcript-test001",
        type="meeting_transcript",
        source=["otter.meeting"],
        source_id="otter-endaoment-gitcoin-impact-pool",
        created="2025-12-10",
        updated="2025-12-10",
        summary="Endaoment Gitcoin Universal Impact Pool transcript",
        otter_meeting_id="otter-endaoment-gitcoin-impact-pool",
        title="Endaoment Gitcoin Universal Impact Pool",
        status="processed",
        start_at="2025-12-10T17:00:00Z",
        end_at="2025-12-10T18:00:00Z",
        duration_seconds=3600,
        calendar_events=["[[endaoment-gitcoin-impact-pool]]"],
        event_id_hint="endaoment-gitcoin-impact-pool",
        ical_uid="endaoment-gitcoin-impact-pool@google.com",
    )
    write_card(
        vault,
        "MeetingTranscripts/endaoment-gitcoin-impact-pool-transcript.md",
        meeting_transcript,
        body="Endaoment and Gitcoin discussed the Universal Impact Pool in [[endaoment-gitcoin-impact-pool]].",
        provenance=_common_provenance(
            "otter.meeting",
            "summary",
            "otter_meeting_id",
            "title",
            "status",
            "start_at",
            "end_at",
            "duration_seconds",
            "calendar_events",
            "event_id_hint",
            "ical_uid",
        ),
    )

    MealOrderCard = CARD_TYPES["meal_order"]
    meal = MealOrderCard(
        uid="hfa-meal-order-test00001",
        type="meal_order",
        source=["extract-emails"],
        source_id="doordash-order-123",
        created="2025-12-15",
        updated="2025-12-15",
        summary="DoorDash order from Brooklyn Hero Shop",
        service="DoorDash",
        restaurant="Brooklyn Hero Shop",
        items=[{"name": "Turkey Hero", "qty": 1, "price": 14.99}],
        subtotal=14.99,
        total=21.50,
        tip=4.00,
        delivery_fee=2.99,
        tax=1.52,
        mode="delivery",
        delivery_address="123 Main St, Brooklyn, NY",
        source_email="[[hfa-email-message-doordash]]",
    )
    meal_dir = vault / "Transactions" / "MealOrders"
    meal_dir.mkdir(parents=True, exist_ok=True)
    write_card(
        vault,
        "Transactions/MealOrders/doordash-hero-shop.md",
        meal,
        body="## DoorDash Order\n\n| Item | Price |\n|---|---|\n| Turkey Hero | $14.99 |\n\nTotal: $21.50",
        provenance=_common_provenance(
            "extract-emails",
            "summary",
            "service",
            "restaurant",
            "items",
            "subtotal",
            "total",
            "tip",
            "delivery_fee",
            "tax",
            "mode",
            "delivery_address",
            "source_email",
        ),
    )

    FinanceCard = CARD_TYPES["finance"]
    finance = FinanceCard(
        uid="hfa-finance-test0000001",
        type="finance",
        source=["plaid"],
        source_id="plaid-doordash-order-123",
        created="2025-12-15",
        updated="2025-12-15",
        summary="DoorDash charge for Brooklyn Hero Shop order",
        amount=21.50,
        currency="USD",
        counterparty="DoorDash",
        category="Food and Drink",
        transaction_status="posted",
        transaction_type="card purchase",
        source_email="[[hfa-email-message-doordash]]",
    )
    finance_dir = vault / "Finance"
    finance_dir.mkdir(parents=True, exist_ok=True)
    write_card(
        vault,
        "Finance/doordash-hero-shop-charge.md",
        finance,
        body="DoorDash charge reconciled to [[doordash-hero-shop]] from the matching receipt and total.",
        provenance=_common_provenance(
            "plaid",
            "summary",
            "amount",
            "currency",
            "counterparty",
            "category",
            "transaction_status",
            "transaction_type",
            "source_email",
        ),
    )

    RideCard = CARD_TYPES["ride"]
    ride = RideCard(
        uid="hfa-ride-test000000001",
        type="ride",
        source=["extract-emails"],
        source_id="uber-ride-456",
        created="2025-11-20",
        updated="2025-11-20",
        summary="Uber from downtown Brooklyn to JFK Airport",
        service="Uber",
        ride_type="car",
        pickup_location="Downtown Brooklyn",
        dropoff_location="JFK Airport Terminal 4",
        pickup_at="2025-11-20T14:00:00-05:00",
        dropoff_at="2025-11-20T15:15:00-05:00",
        fare=54.20,
        tip=10.00,
        distance_miles=18.3,
        duration_minutes=75,
        driver_name="Alex",
        vehicle="Toyota Camry",
        source_email="[[hfa-email-message-uber]]",
    )
    ride_dir = vault / "Transactions" / "Rides"
    ride_dir.mkdir(parents=True, exist_ok=True)
    write_card(
        vault,
        "Transactions/Rides/uber-to-jfk.md",
        ride,
        body="Uber from Downtown Brooklyn to JFK Airport Terminal 4 — 18.3 mi, 75 min — $54.20",
        provenance=_common_provenance(
            "extract-emails",
            "summary",
            "service",
            "ride_type",
            "pickup_location",
            "dropoff_location",
            "pickup_at",
            "dropoff_at",
            "fare",
            "tip",
            "distance_miles",
            "duration_minutes",
            "driver_name",
            "vehicle",
            "source_email",
        ),
    )

    FlightCard = CARD_TYPES["flight"]
    flight = FlightCard(
        uid="hfa-flight-test0000001",
        type="flight",
        source=["extract-emails"],
        source_id="united-ua1234",
        created="2025-12-15",
        updated="2025-12-15",
        summary="United UA 1234 SFO to JFK",
        airline="United",
        confirmation_code="ABC123",
        origin_airport="SFO",
        destination_airport="JFK",
        departure_at="2025-12-15T08:00:00-08:00",
        arrival_at="2025-12-15T16:30:00-05:00",
        fare_class="Economy Plus",
        seat="12C",
        fare_amount=389.00,
        booking_source="united.com",
        passengers=["Robbie Heeger"],
        source_email="[[hfa-email-message-united]]",
    )
    flight_dir = vault / "Transactions" / "Flights"
    flight_dir.mkdir(parents=True, exist_ok=True)
    write_card(
        vault,
        "Transactions/Flights/united-sfo-jfk.md",
        flight,
        body=(
            "United UA 1234 — SFO to JFK — Dec 15 8:00am to 4:30pm — Economy Plus 12C. "
            "Part of the San Francisco trip with [[handlery-union-square]]."
        ),
        provenance=_common_provenance(
            "extract-emails",
            "summary",
            "airline",
            "confirmation_code",
            "origin_airport",
            "destination_airport",
            "departure_at",
            "arrival_at",
            "fare_class",
            "seat",
            "fare_amount",
            "booking_source",
            "passengers",
            "source_email",
        ),
    )

    AccommodationCard = CARD_TYPES["accommodation"]
    accommodation = AccommodationCard(
        uid="hfa-accommodation-test0001",
        type="accommodation",
        source=["extract-emails"],
        source_id="handlery-union-square-reservation",
        created="2025-12-10",
        updated="2025-12-10",
        summary="Handlery Union Square hotel stay",
        property_name="Handlery Union Square",
        property_type="hotel",
        address="351 Geary St, San Francisco, CA",
        check_in="2025-12-14T15:00:00-08:00",
        check_out="2025-12-16T11:00:00-08:00",
        confirmation_code="HANDLERY123",
        nightly_rate=199.00,
        total_cost=458.00,
        guests=["Robbie Heeger"],
        booking_source="handlery.com",
        source_email="[[hfa-email-message-handlery]]",
    )
    accommodation_dir = vault / "Transactions" / "Accommodations"
    accommodation_dir.mkdir(parents=True, exist_ok=True)
    write_card(
        vault,
        "Transactions/Accommodations/handlery-union-square.md",
        accommodation,
        body="Handlery Union Square hotel stay linked to [[united-sfo-jfk]] for the San Francisco trip.",
        provenance=_common_provenance(
            "extract-emails",
            "summary",
            "property_name",
            "property_type",
            "address",
            "check_in",
            "check_out",
            "confirmation_code",
            "nightly_rate",
            "total_cost",
            "guests",
            "booking_source",
            "source_email",
        ),
    )


@pytest.fixture
def live_phase5_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> tuple[Path, PostgresArchiveIndex, Phase5SemanticProvider]:
    """Build a vault with person + 3 derived type cards, return (vault, index, provider)."""
    vault = tmp_path / "hf-archives"
    for d in [
        "People",
        "Email",
        "Calendar",
        "MeetingTranscripts",
        "Transactions/MealOrders",
        "Transactions/Rides",
        "Transactions/Flights",
        "_templates",
        ".obsidian",
    ]:
        (vault / d).mkdir(parents=True, exist_ok=True)
    meta = vault / "_meta"
    meta.mkdir()
    (meta / "identity-map.json").write_text("{}", encoding="utf-8")
    (meta / "sync-state.json").write_text("{}", encoding="utf-8")
    (meta / "dedup-candidates.json").write_text(json.dumps([]), encoding="utf-8")

    _seed_phase5_vault(vault)

    schema_name = f"archive_phase5_{uuid.uuid4().hex[:10]}"
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema_name)
    monkeypatch.setenv("PPA_VECTOR_DIMENSION", "10")
    monkeypatch.setenv("PPA_EMBEDDING_MODEL", "fixture-phase5-v1")
    monkeypatch.setenv("PPA_EMBEDDING_VERSION", "1")
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(vault / "_meta" / "rust-search-index"))

    index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
    index.schema = schema_name
    provider = Phase5SemanticProvider(model="fixture-phase5-v1", dimension=10)

    def _fixture_provider(model: str = "") -> Phase5SemanticProvider:
        return provider

    monkeypatch.setattr("archive_cli.store.get_embedding_provider", _fixture_provider)
    monkeypatch.setattr(
        "archive_cli.commands._resolve.get_embedding_provider",
        _fixture_provider,
    )
    return vault, index, provider


def _type_line_present(text: str, card_type: str) -> bool:
    return f"[{card_type}," in text


def _vector_result_lines(text: str) -> int:
    return text.count("\n- ")


def _hybrid_has_positive_vector(text: str) -> bool:
    for m in re.finditer(r"vector=([0-9.]+)", text):
        if float(m.group(1)) > 0:
            return True
    return False


@pytest.mark.integration
class TestEmbeddingCompleteness:
    def test_zero_pending_after_embed_pending(self, live_phase5_archive):
        _vault, index, provider = live_phase5_archive
        archive_rebuild_indexes()
        archive_embed_pending(limit=0, embedding_model=provider.model, embedding_version=1)
        status = index.embedding_status(embedding_model=provider.model, embedding_version=1)
        assert status["pending_chunk_count"] == 0

    def test_embedded_equals_chunk_count(self, live_phase5_archive):
        _vault, index, provider = live_phase5_archive
        archive_rebuild_indexes()
        archive_embed_pending(limit=0, embedding_model=provider.model, embedding_version=1)
        status = index.embedding_status(embedding_model=provider.model, embedding_version=1)
        assert status["embedded_chunk_count"] == status["chunk_count"]
        assert status["chunk_count"] > 0


@pytest.mark.integration
class TestVectorSearchNewTypes:
    def _setup(self, live_phase5_archive):
        _vault, index, provider = live_phase5_archive
        archive_rebuild_indexes()
        archive_embed_pending(limit=0, embedding_model=provider.model, embedding_version=1)
        from archive_cli.serving_index import publish_serving_index
        from archive_cli.store import DefaultArchiveStore
        from archive_cli import serving_index as si

        si._HANDLE = None
        published = publish_serving_index(DefaultArchiveStore(vault=_vault, index=index))
        assert published.get("ok"), published

    def test_meal_order_findable_by_restaurant(self, live_phase5_archive):
        self._setup(live_phase5_archive)
        result = archive_vector_search(
            query="Brooklyn Hero Shop DoorDash order",
            limit=5,
            embedding_model="fixture-phase5-v1",
            embedding_version=1,
        )
        assert _type_line_present(result, "meal_order"), "meal_order not in top 5 vector results"

    def test_flight_findable_by_destination(self, live_phase5_archive):
        self._setup(live_phase5_archive)
        result = archive_vector_search(
            query="United flight SFO to JFK boarding",
            limit=5,
            embedding_model="fixture-phase5-v1",
            embedding_version=1,
        )
        assert _type_line_present(result, "flight"), "flight not in top 5 vector results"

    def test_ride_findable_by_route(self, live_phase5_archive):
        self._setup(live_phase5_archive)
        result = archive_vector_search(
            query="Uber ride pickup downtown dropoff airport",
            limit=5,
            embedding_model="fixture-phase5-v1",
            embedding_version=1,
        )
        assert _type_line_present(result, "ride"), "ride not in top 5 vector results"


@pytest.mark.integration
class TestHybridSearchFusion:
    def _setup(self, live_phase5_archive):
        _vault, index, provider = live_phase5_archive
        archive_rebuild_indexes()
        archive_embed_pending(limit=0, embedding_model=provider.model, embedding_version=1)
        from archive_cli.serving_index import publish_serving_index
        from archive_cli.store import DefaultArchiveStore
        from archive_cli import serving_index as si

        si._HANDLE = None
        published = publish_serving_index(DefaultArchiveStore(vault=_vault, index=index))
        assert published.get("ok"), published

    def test_hybrid_includes_vector_scores(self, live_phase5_archive):
        self._setup(live_phase5_archive)
        result = archive_hybrid_search(
            query="Brooklyn Hero Shop DoorDash delivery order",
            embedding_model="fixture-phase5-v1",
            embedding_version=1,
        )
        assert "No hybrid matches" not in result
        assert _hybrid_has_positive_vector(result), "expected non-zero vector similarity in hybrid output"

    def test_hybrid_new_type_discoverable(self, live_phase5_archive):
        self._setup(live_phase5_archive)
        result = archive_hybrid_search(
            query="DoorDash restaurant meal order Turkey Hero",
            embedding_model="fixture-phase5-v1",
            embedding_version=1,
        )
        assert _type_line_present(result, "meal_order"), "meal_order not discoverable via hybrid search"


@pytest.mark.integration
class TestManifestSemanticQueries:
    @pytest.fixture(autouse=True)
    def load_manifest(self):
        manifest_path = Path(__file__).parent / "slice_manifest.json"
        if not manifest_path.exists():
            pytest.skip("slice_manifest.json not found (Phase 0 deliverable)")
        with open(manifest_path) as f:
            self.manifest = json.load(f)
        if "semantic_queries" not in self.manifest:
            pytest.skip("slice_manifest.json has no semantic_queries key (Phase 5 Step 7)")
        active = [q for q in self.manifest["semantic_queries"] if not q.get("query", "").startswith("PLACEHOLDER")]
        if not active:
            pytest.skip("All semantic_queries are still placeholders")
        self.active_queries = active

    def test_manifest_semantic_queries_return_expected_types(self, live_phase5_archive):
        _vault, _index, provider = live_phase5_archive
        archive_rebuild_indexes()
        archive_embed_pending(limit=0, embedding_model=provider.model, embedding_version=1)
        from archive_cli.serving_index import publish_serving_index
        from archive_cli.store import DefaultArchiveStore
        from archive_cli import serving_index as si

        si._HANDLE = None
        published = publish_serving_index(DefaultArchiveStore(vault=_vault, index=_index))
        assert published.get("ok"), published

        for entry in self.active_queries:
            query = entry["query"]
            expected_types = set(entry.get("expected_types", []))
            min_hits = entry.get("min_hits", 1)

            result = archive_vector_search(
                query=query,
                limit=10,
                embedding_model=provider.model,
                embedding_version=1,
            )
            if "No vector matches" in result:
                assert False, f"Semantic query '{query}': no vector matches"
            hits = _vector_result_lines(result)
            assert hits >= min_hits, f"Semantic query '{query}': expected >= {min_hits} hits, got {hits}"
            for t in expected_types:
                assert _type_line_present(result, t), (
                    f"Semantic query '{query}': expected type {t} in formatted results"
                )
