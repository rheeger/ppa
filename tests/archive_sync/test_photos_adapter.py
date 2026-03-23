"""Archive-sync Photos adapter tests."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.adapters.photos import PhotosAdapter
from hfa.schema import MediaAssetCard, PersonCard
from hfa.vault import read_note, write_card


class FakePhotosSource:
    def __init__(self, photos: list[object], library_path: str):
        self._photos = photos
        self.library_path = library_path

    def iter_assets(self):
        return list(self._photos)


class FakePhotosAdapter(PhotosAdapter):
    def __init__(self, photos: list[object], *, library_path: str = "/tmp/Test.photoslibrary"):
        super().__init__()
        self._photos = photos
        self._library_path = library_path

    def _build_source(self, *, library_path: str | None):
        return FakePhotosSource(self._photos, library_path or self._library_path)


def _album(title: str, folder_names: list[str] | None = None) -> SimpleNamespace:
    return SimpleNamespace(title=title, folder_names=folder_names or [])


def _photo(photo_path: Path, **overrides) -> SimpleNamespace:
    payload = {
        "uuid": "asset-1",
        "filename": photo_path.name,
        "original_filename": photo_path.name,
        "path": str(photo_path),
        "path_edited": None,
        "date": datetime(2026, 3, 8, 10, 15, tzinfo=timezone.utc),
        "date_modified": datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc),
        "title": "Beach Walk",
        "description": "Sunset on the beach",
        "keywords": ["Travel", "Travel"],
        "persons": ["Alice Example"],
        "labels_normalized": ["beach", "sunset"],
        "album_info": [_album("Summer Trip", ["Family", "Trips"])],
        "favorite": True,
        "hidden": False,
        "hasadjustments": False,
        "live_photo": False,
        "burst": False,
        "screenshot": False,
        "slow_mo": False,
        "time_lapse": False,
        "ismissing": False,
        "ismovie": False,
        "width": 3024,
        "height": 4032,
        "duration": 0.0,
        "place": SimpleNamespace(name="Santa Barbara"),
        "city": "Santa Barbara",
        "state": "California",
        "country": "United States",
        "latitude": 34.4208,
        "longitude": -119.6982,
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _seed_person(tmp_vault: Path) -> None:
    person = PersonCard(
        uid="hfa-person-abc123def456",
        type="person",
        source=["contacts.apple"],
        source_id="alice@example.com",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Alice Example",
        emails=["alice@example.com"],
    )
    write_card(
        tmp_vault,
        "People/alice-example.md",
        person,
        provenance=deterministic_provenance(person, "contacts.apple"),
    )
    (tmp_vault / "_meta" / "identity-map.json").write_text(
        json.dumps(
            {
                "_comment": "Alias -> canonical person wikilink",
                "name:alice example": "[[alice-example]]",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_to_card_returns_media_asset_card():
    adapter = PhotosAdapter()
    card, _, body = adapter.to_card(
        {
            "kind": "asset",
            "source": ["photos.asset", "photos.private.person", "photos.private.label"],
            "source_id": "apple-photos:asset-1",
            "photos_asset_id": "asset-1",
            "photos_source_label": "apple-photos",
            "created": "2026-03-08",
            "summary": "Beach Walk",
            "people": ["[[alice-example]]"],
            "media_type": "photo",
            "filename": "IMG_1001.JPG",
            "original_filename": "IMG_1001.JPG",
            "mime_type": "image/jpeg",
            "captured_at": "2026-03-08T10:15:00+00:00",
            "modified_at": "2026-03-09T12:00:00+00:00",
            "title": "Beach Walk",
            "description": "Sunset on the beach",
            "keywords": ["Travel"],
            "labels": ["beach"],
            "person_labels": ["Alice Example"],
            "albums": ["Summer Trip"],
            "album_paths": ["Family/Trips/Summer Trip"],
            "folders": ["Family/Trips"],
            "favorite": True,
            "place_name": "Santa Barbara",
            "metadata_sha": "abc123def456",
            "body": "Labels: beach\nPeople labels: Alice Example",
        }
    )
    assert isinstance(card, MediaAssetCard)
    assert card.photos_asset_id == "asset-1"
    assert card.person_labels == ["Alice Example"]
    assert "People labels: Alice Example" in body


def test_ingest_creates_media_asset_card_and_resolves_people(tmp_vault, tmp_path):
    _seed_person(tmp_vault)
    photo_path = tmp_path / "IMG_1001.JPG"
    photo_path.write_bytes(b"jpeg-bytes")
    adapter = FakePhotosAdapter([_photo(photo_path)])

    result = adapter.ingest(str(tmp_vault), source_label="apple-photos", quick_update=True)

    assert result.created == 1
    rel_path = next((tmp_vault / "Photos").rglob("*.md")).relative_to(tmp_vault)
    frontmatter, body, _ = read_note(tmp_vault, str(rel_path))
    assert frontmatter["type"] == "media_asset"
    assert frontmatter["people"] == ["[[alice-example]]"]
    assert frontmatter["person_labels"] == ["Alice Example"]
    assert frontmatter["labels"] == ["beach", "sunset"]
    assert frontmatter["albums"] == ["Summer Trip"]
    assert frontmatter["album_paths"] == ["Family/Trips/Summer Trip"]
    assert frontmatter["metadata_sha"]
    assert "People labels: Alice Example" in body
    assert "Album paths: Family/Trips/Summer Trip" in body


def test_ingest_prefers_original_filename_and_filters_placeholder_person_labels(tmp_vault, tmp_path):
    _seed_person(tmp_vault)
    photo_path = tmp_path / "25A8B38F-C22A-4A01-AD54-BC94EE092F97.JPG"
    photo_path.write_bytes(b"jpeg-bytes")
    adapter = FakePhotosAdapter(
        [
            _photo(
                photo_path,
                title="",
                original_filename="IMG_5804.HEIC",
                persons=["_UNKNOWN_", "Alice Example"],
                labels_normalized=["beach"],
            )
        ]
    )

    result = adapter.ingest(str(tmp_vault), source_label="apple-photos", quick_update=True)

    assert result.created == 1
    rel_path = next((tmp_vault / "Photos").rglob("*.md")).relative_to(tmp_vault)
    frontmatter, body, _ = read_note(tmp_vault, str(rel_path))
    assert frontmatter["summary"] == "IMG_5804.HEIC"
    assert frontmatter["person_labels"] == ["Alice Example"]
    assert frontmatter["people"] == ["[[alice-example]]"]
    assert "_UNKNOWN_" not in body


def test_quick_update_skips_unchanged_assets(tmp_vault, tmp_path):
    photo_path = tmp_path / "IMG_1001.JPG"
    photo_path.write_bytes(b"jpeg-bytes")
    photo = _photo(photo_path)

    first = FakePhotosAdapter([photo])
    assert first.ingest(str(tmp_vault), source_label="apple-photos", quick_update=True).created == 1

    second = FakePhotosAdapter([photo])
    result = second.ingest(str(tmp_vault), source_label="apple-photos", quick_update=True)

    assert result.created == 0
    assert result.merged == 0
    assert result.skipped == 1
    assert result.skip_details["skipped_unchanged_assets"] == 1


def test_fetch_batches_streams_assets_with_progress_cursor(tmp_vault, tmp_path):
    _seed_person(tmp_vault)
    photo_a = tmp_path / "IMG_1001.JPG"
    photo_b = tmp_path / "IMG_1002.JPG"
    photo_c = tmp_path / "IMG_1003.JPG"
    for path in (photo_a, photo_b, photo_c):
        path.write_bytes(b"jpeg-bytes")
    adapter = FakePhotosAdapter(
        [
            _photo(photo_a, uuid="asset-1"),
            _photo(photo_b, uuid="asset-2"),
            _photo(photo_c, uuid="asset-3"),
        ]
    )
    batches = list(
        adapter.fetch_batches(
            str(tmp_vault),
            {},
            source_label="apple-photos",
            quick_update=True,
            batch_size=2,
        )
    )
    assert len(batches) == 2
    assert [len(batch.items) for batch in batches] == [2, 1]
    assert batches[0].cursor_patch["scanned_assets"] == 2
    assert batches[0].cursor_patch["emitted_assets"] == 2
    assert batches[1].cursor_patch["scanned_assets"] == 3
    assert batches[1].cursor_patch["emitted_assets"] == 3


def test_ingest_can_disable_private_people_and_labels(tmp_vault, tmp_path):
    _seed_person(tmp_vault)
    photo_path = tmp_path / "IMG_1001.JPG"
    photo_path.write_bytes(b"jpeg-bytes")
    adapter = FakePhotosAdapter([_photo(photo_path)])

    result = adapter.ingest(
        str(tmp_vault),
        source_label="apple-photos",
        include_private_people=False,
        include_private_labels=False,
    )

    assert result.created == 1
    rel_path = next((tmp_vault / "Photos").rglob("*.md")).relative_to(tmp_vault)
    frontmatter, body, _ = read_note(tmp_vault, str(rel_path))
    assert frontmatter["source"] == ["photos.asset"]
    assert "person_labels" not in frontmatter
    assert "labels" not in frontmatter
    assert "people" not in frontmatter
    assert "People labels:" not in body
