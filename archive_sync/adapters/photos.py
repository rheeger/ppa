"""Apple Photos adapter for HFA media asset imports."""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any

from hfa.identity import IdentityCache
from hfa.identity_resolver import PersonIndex, resolve_person
from hfa.schema import MediaAssetCard
from hfa.uid import generate_uid
from hfa.vault import iter_notes, read_note

from .base import BaseAdapter, FetchedBatch, deterministic_provenance
from .photos_private_meta import (
    NullPhotosPrivateMetadataProvider,
    OSXPhotosPrivateMetadataProvider,
    PhotosPrivateMetadataProvider,
)

ASSET_SOURCE = "photos.asset"
PRIVATE_PEOPLE_SOURCE = "photos.private.person"
PRIVATE_LABEL_SOURCE = "photos.private.label"


def _clean(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _clean_list(values: Any, *, lowercase: bool = False) -> list[str]:
    if not isinstance(values, (list, tuple, set)):
        return []
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean(value)
        if not text:
            continue
        if lowercase:
            text = text.lower()
        if text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return cleaned


def _iso(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = _clean(value)
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return text


def _date_bucket(*values: str) -> str:
    for value in values:
        text = _clean(value)
        if len(text) >= 10:
            return text[:10]
    return date.today().isoformat()


def _float_value(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int_value(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def _bool_value(value: Any) -> bool:
    return bool(value)


def _path_value(value: Any) -> str:
    text = _clean(value)
    return text


def _file_size(*paths: str) -> int:
    for raw_path in paths:
        path_value = _path_value(raw_path)
        if not path_value:
            continue
        try:
            return Path(path_value).stat().st_size
        except OSError:
            continue
    return 0


def _mime_type(*candidates: str) -> str:
    for candidate in candidates:
        text = _clean(candidate)
        if not text:
            continue
        guessed, _ = mimetypes.guess_type(text)
        if guessed:
            return guessed
    return ""


def _asset_uid(source_label: str, asset_id: str) -> str:
    return generate_uid("media-asset", ASSET_SOURCE, f"{source_label}:{asset_id}")


def _album_metadata(photo: Any) -> tuple[list[str], list[str], list[str]]:
    albums: list[str] = []
    album_paths: list[str] = []
    folders: list[str] = []
    seen_albums: set[str] = set()
    seen_album_paths: set[str] = set()
    seen_folders: set[str] = set()
    for album in list(getattr(photo, "album_info", []) or []):
        title = _clean(getattr(album, "title", ""))
        folder_names = [_clean(name) for name in list(getattr(album, "folder_names", []) or []) if _clean(name)]
        folder_path = "/".join(folder_names)
        album_path = "/".join([*folder_names, title]) if title else folder_path
        if title and title not in seen_albums:
            seen_albums.add(title)
            albums.append(title)
        if folder_path and folder_path not in seen_folders:
            seen_folders.add(folder_path)
            folders.append(folder_path)
        if album_path and album_path not in seen_album_paths:
            seen_album_paths.add(album_path)
            album_paths.append(album_path)
    return albums, album_paths, folders


def _render_body(item: dict[str, Any]) -> str:
    lines: list[str] = []
    if item.get("title"):
        lines.append(f"Title: {item['title']}")
    if item.get("description"):
        lines.append(f"Description: {item['description']}")
    if item.get("filename"):
        lines.append(f"Filename: {item['filename']}")
    if item.get("media_type"):
        lines.append(f"Media type: {item['media_type']}")
    if item.get("captured_at"):
        lines.append(f"Captured at: {item['captured_at']}")
    if item.get("modified_at"):
        lines.append(f"Modified at: {item['modified_at']}")
    if item.get("keywords"):
        lines.append(f"Keywords: {', '.join(item['keywords'])}")
    if item.get("labels"):
        lines.append(f"Labels: {', '.join(item['labels'])}")
    if item.get("person_labels"):
        lines.append(f"People labels: {', '.join(item['person_labels'])}")
    if item.get("people"):
        lines.append(f"Resolved people: {', '.join(item['people'])}")
    if item.get("albums"):
        lines.append(f"Albums: {', '.join(item['albums'])}")
    if item.get("album_paths"):
        lines.append(f"Album paths: {', '.join(item['album_paths'])}")
    if item.get("folders"):
        lines.append(f"Folders: {', '.join(item['folders'])}")
    place_bits = [
        item.get("place_name", ""),
        item.get("place_city", ""),
        item.get("place_state", ""),
        item.get("place_country", ""),
    ]
    place_value = ", ".join(bit for bit in place_bits if bit)
    if place_value:
        lines.append(f"Place: {place_value}")
    return "\n".join(lines).strip()


def _metadata_sha(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:12]


class OSXPhotosLibrarySource:
    """Thin runtime wrapper around osxphotos."""

    def __init__(self, library_path: str | None = None):
        try:
            import osxphotos  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - exercised in live use
            raise RuntimeError(
                "osxphotos is required for the Photos adapter. Install project dependencies first."
            ) from exc
        self._osxphotos = osxphotos
        self._db = osxphotos.PhotosDB(library_path) if library_path else osxphotos.PhotosDB()
        self.library_path = _clean(getattr(self._db, "library_path", library_path or ""))

    def iter_assets(self) -> Iterable[Any]:
        return list(self._db.photos())


class PhotosAdapter(BaseAdapter):
    source_id = "photos"

    def get_cursor_key(self, **kwargs) -> str:
        source_label = _clean(kwargs.get("source_label", "")).lower()
        return f"{self.source_id}:{source_label}" if source_label else self.source_id

    def _build_source(self, *, library_path: str | None) -> OSXPhotosLibrarySource:
        return OSXPhotosLibrarySource(library_path)

    def _build_private_metadata_provider(
        self,
        *,
        include_private_people: bool,
        include_private_labels: bool,
    ) -> PhotosPrivateMetadataProvider:
        if not include_private_people and not include_private_labels:
            return NullPhotosPrivateMetadataProvider()
        return OSXPhotosPrivateMetadataProvider(
            include_people=include_private_people,
            include_labels=include_private_labels,
        )

    def _resolve_people(
        self,
        *,
        vault_path: str,
        identity_cache: IdentityCache,
        people_index: PersonIndex,
        person_labels: list[str],
    ) -> list[str]:
        links: list[str] = []
        for name in person_labels:
            direct = identity_cache.resolve("name", name)
            if direct and direct not in links:
                links.append(direct)
                continue
            resolved = resolve_person(
                vault_path,
                {"summary": name, "name": name},
                cache=identity_cache,
                people_index=people_index,
            )
            if resolved.action == "merge" and resolved.wikilink and resolved.wikilink not in links:
                links.append(resolved.wikilink)
        return links

    def _load_existing_asset_hashes(self, vault_path: str, *, source_label: str) -> dict[str, str]:
        hashes: dict[str, str] = {}
        for rel_path, _ in iter_notes(vault_path):
            if not rel_path.parts or rel_path.parts[0] != "Photos":
                continue
            frontmatter, _, _ = read_note(vault_path, str(rel_path))
            if str(frontmatter.get("type", "")).strip() != "media_asset":
                continue
            if _clean(frontmatter.get("photos_source_label", "")) != source_label:
                continue
            source_id = _clean(frontmatter.get("source_id", ""))
            if not source_id:
                continue
            hashes[source_id] = _clean(frontmatter.get("metadata_sha", ""))
        return hashes

    def _normalize_photo(
        self,
        photo: Any,
        *,
        source_label: str,
        private_provider: PhotosPrivateMetadataProvider,
        vault_path: str,
        identity_cache: IdentityCache,
        people_index: PersonIndex,
    ) -> dict[str, Any]:
        asset_id = _clean(getattr(photo, "uuid", ""))
        if not asset_id:
            raise ValueError("Photos asset missing uuid")
        filename = _clean(getattr(photo, "filename", "")) or _clean(getattr(photo, "original_filename", ""))
        original_filename = _clean(getattr(photo, "original_filename", "")) or filename
        original_path = _path_value(getattr(photo, "path", None))
        edited_path = _path_value(getattr(photo, "path_edited", None))
        captured_at = _iso(getattr(photo, "date", None))
        modified_at = _iso(getattr(photo, "date_modified", None))
        title = _clean(getattr(photo, "title", ""))
        description = _clean(getattr(photo, "description", ""))
        keywords = _clean_list(getattr(photo, "keywords", []))
        albums, album_paths, folders = _album_metadata(photo)
        private_metadata = private_provider.metadata_for_photo(photo)
        people = self._resolve_people(
            vault_path=vault_path,
            identity_cache=identity_cache,
            people_index=people_index,
            person_labels=private_metadata.person_labels,
        )
        media_type = "video" if _bool_value(getattr(photo, "ismovie", False)) else "photo"
        source_id = f"{source_label}:{asset_id}"
        title_place = _clean(getattr(getattr(photo, "place", None), "name", "")) or _clean(
            getattr(photo, "place_name", "")
        )
        payload = {
            "kind": "asset",
            "source": [
                ASSET_SOURCE,
                *([PRIVATE_PEOPLE_SOURCE] if private_metadata.person_labels else []),
                *([PRIVATE_LABEL_SOURCE] if private_metadata.labels else []),
            ],
            "source_id": source_id,
            "photos_asset_id": asset_id,
            "photos_source_label": source_label,
            "created": _date_bucket(captured_at, modified_at),
            "summary": title or original_filename or filename or asset_id,
            "people": people,
            "media_type": media_type,
            "filename": filename,
            "original_filename": original_filename,
            "mime_type": _mime_type(edited_path, original_path, filename, original_filename),
            "original_path": original_path,
            "edited_path": edited_path,
            "size_bytes": _file_size(edited_path, original_path),
            "width": _int_value(getattr(photo, "width", 0)),
            "height": _int_value(getattr(photo, "height", 0)),
            "duration_seconds": round(_float_value(getattr(photo, "duration", 0.0)), 3),
            "captured_at": captured_at,
            "modified_at": modified_at,
            "title": title,
            "description": description,
            "keywords": keywords,
            "labels": private_metadata.labels,
            "person_labels": private_metadata.person_labels,
            "albums": albums,
            "album_paths": album_paths,
            "folders": folders,
            "favorite": _bool_value(getattr(photo, "favorite", False)),
            "hidden": _bool_value(getattr(photo, "hidden", False)),
            "has_adjustments": _bool_value(getattr(photo, "hasadjustments", False)),
            "live_photo": _bool_value(getattr(photo, "live_photo", False)),
            "burst": _bool_value(getattr(photo, "burst", False)),
            "screenshot": _bool_value(getattr(photo, "screenshot", False)),
            "slow_mo": _bool_value(getattr(photo, "slow_mo", False)),
            "time_lapse": _bool_value(getattr(photo, "time_lapse", False)),
            "is_missing": _bool_value(getattr(photo, "ismissing", False)),
            "place_name": title_place,
            "place_city": _clean(getattr(photo, "city", "")),
            "place_state": _clean(getattr(photo, "state", "")),
            "place_country": _clean(getattr(photo, "country", "")),
            "latitude": _float_value(getattr(photo, "latitude", None)),
            "longitude": _float_value(getattr(photo, "longitude", None)),
        }
        hash_payload = {key: value for key, value in payload.items() if key not in {"kind"}}
        payload["metadata_sha"] = _metadata_sha(hash_payload)
        payload["body"] = _render_body(payload)
        return payload

    def _fetch_records(
        self,
        vault_path: str,
        *,
        library_path: str | None = None,
        source_label: str = "apple-photos",
        max_assets: int | None = None,
        quick_update: bool = True,
        include_private_people: bool = True,
        include_private_labels: bool = True,
        batch_size: int | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any], int]:
        normalized_source_label = (_clean(source_label) or "apple-photos").lower()
        source = self._build_source(library_path=library_path)
        private_provider = self._build_private_metadata_provider(
            include_private_people=include_private_people,
            include_private_labels=include_private_labels,
        )
        identity_cache = IdentityCache(vault_path)
        people_index = PersonIndex(vault_path)
        existing_hashes = (
            self._load_existing_asset_hashes(vault_path, source_label=normalized_source_label) if quick_update else {}
        )
        items: list[dict[str, Any]] = []
        skipped_unchanged_assets = 0
        scanned_assets = 0
        max_modified_at = ""
        for photo in source.iter_assets():
            scanned_assets += 1
            item = self._normalize_photo(
                photo,
                source_label=normalized_source_label,
                private_provider=private_provider,
                vault_path=vault_path,
                identity_cache=identity_cache,
                people_index=people_index,
            )
            modified_at = _clean(item.get("modified_at", ""))
            if modified_at and (not max_modified_at or modified_at > max_modified_at):
                max_modified_at = modified_at
            if quick_update and existing_hashes.get(item["source_id"]) == item["metadata_sha"]:
                skipped_unchanged_assets += 1
                continue
            items.append(item)
            if max_assets is not None and len(items) >= max(0, int(max_assets)):
                break
        cursor_patch = {
            "source_label": normalized_source_label,
            "library_path": _clean(library_path or getattr(source, "library_path", "")),
            "scanned_assets": scanned_assets,
            "emitted_assets": len(items),
            "skipped_unchanged_assets": skipped_unchanged_assets,
            "last_modified_at": max_modified_at,
        }
        return items, cursor_patch, skipped_unchanged_assets

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        items, cursor_patch, _ = self._fetch_records(vault_path, **kwargs)
        cursor.update(cursor_patch)
        return items

    def fetch_batches(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        **kwargs,
    ) -> Iterable[FetchedBatch]:
        library_path = kwargs.get("library_path")
        source_label = kwargs.get("source_label", "apple-photos")
        max_assets = kwargs.get("max_assets")
        quick_update = bool(kwargs.get("quick_update", True))
        include_private_people = bool(kwargs.get("include_private_people", True))
        include_private_labels = bool(kwargs.get("include_private_labels", True))
        requested_batch_size = kwargs.get("batch_size")
        batch_size = max(1, int(requested_batch_size or os.environ.get("HFA_PHOTOS_BATCH_SIZE") or 250))

        normalized_source_label = (_clean(source_label) or "apple-photos").lower()
        source = self._build_source(library_path=library_path)
        private_provider = self._build_private_metadata_provider(
            include_private_people=include_private_people,
            include_private_labels=include_private_labels,
        )
        identity_cache = IdentityCache(vault_path)
        people_index = PersonIndex(vault_path)
        existing_hashes = (
            self._load_existing_asset_hashes(vault_path, source_label=normalized_source_label) if quick_update else {}
        )

        batch_items: list[dict[str, Any]] = []
        scanned_assets = 0
        emitted_assets = 0
        skipped_unchanged_assets = 0
        max_modified_at = ""
        sequence = 0
        skipped_since_yield = 0

        def _cursor_patch() -> dict[str, Any]:
            return {
                "source_label": normalized_source_label,
                "library_path": _clean(library_path or getattr(source, "library_path", "")),
                "scanned_assets": scanned_assets,
                "emitted_assets": emitted_assets,
                "skipped_unchanged_assets": skipped_unchanged_assets,
                "last_modified_at": max_modified_at,
            }

        for photo in source.iter_assets():
            scanned_assets += 1
            item = self._normalize_photo(
                photo,
                source_label=normalized_source_label,
                private_provider=private_provider,
                vault_path=vault_path,
                identity_cache=identity_cache,
                people_index=people_index,
            )
            modified_at = _clean(item.get("modified_at", ""))
            if modified_at and (not max_modified_at or modified_at > max_modified_at):
                max_modified_at = modified_at
            if quick_update and existing_hashes.get(item["source_id"]) == item["metadata_sha"]:
                skipped_unchanged_assets += 1
                skipped_since_yield += 1
                continue
            batch_items.append(item)
            emitted_assets += 1
            if max_assets is not None and emitted_assets >= max(0, int(max_assets)):
                yield FetchedBatch(
                    items=list(batch_items),
                    cursor_patch=_cursor_patch(),
                    sequence=sequence,
                    skipped_count=skipped_since_yield,
                    skip_details={"skipped_unchanged_assets": skipped_since_yield},
                )
                return
            if len(batch_items) >= batch_size:
                yield FetchedBatch(
                    items=list(batch_items),
                    cursor_patch=_cursor_patch(),
                    sequence=sequence,
                    skipped_count=skipped_since_yield,
                    skip_details={"skipped_unchanged_assets": skipped_since_yield},
                )
                batch_items = []
                sequence += 1
                skipped_since_yield = 0

        yield FetchedBatch(
            items=list(batch_items),
            cursor_patch=_cursor_patch(),
            sequence=sequence,
            skipped_count=skipped_since_yield,
            skip_details={"skipped_unchanged_assets": skipped_since_yield},
        )

    def to_card(self, item: dict[str, Any]):
        today = date.today().isoformat()
        if _clean(item.get("kind", "")) != "asset":
            raise ValueError(f"Unsupported Photos record kind: {_clean(item.get('kind', ''))}")
        card = MediaAssetCard(
            uid=_asset_uid(_clean(item.get("photos_source_label", "")), _clean(item.get("photos_asset_id", ""))),
            type="media_asset",
            source=list(item.get("source", [])) or [ASSET_SOURCE],
            source_id=_clean(item.get("source_id", "")),
            created=_clean(item.get("created", "")) or today,
            updated=today,
            summary=_clean(item.get("summary", "")),
            people=list(item.get("people", [])),
            photos_asset_id=_clean(item.get("photos_asset_id", "")),
            photos_source_label=_clean(item.get("photos_source_label", "")),
            media_type=_clean(item.get("media_type", "")),
            filename=_clean(item.get("filename", "")),
            original_filename=_clean(item.get("original_filename", "")),
            mime_type=_clean(item.get("mime_type", "")),
            original_path=_clean(item.get("original_path", "")),
            edited_path=_clean(item.get("edited_path", "")),
            size_bytes=_int_value(item.get("size_bytes", 0)),
            width=_int_value(item.get("width", 0)),
            height=_int_value(item.get("height", 0)),
            duration_seconds=_float_value(item.get("duration_seconds", 0.0)),
            captured_at=_clean(item.get("captured_at", "")),
            modified_at=_clean(item.get("modified_at", "")),
            title=_clean(item.get("title", "")),
            description=_clean(item.get("description", "")),
            keywords=list(item.get("keywords", [])),
            labels=list(item.get("labels", [])),
            person_labels=list(item.get("person_labels", [])),
            albums=list(item.get("albums", [])),
            album_paths=list(item.get("album_paths", [])),
            folders=list(item.get("folders", [])),
            favorite=_bool_value(item.get("favorite", False)),
            hidden=_bool_value(item.get("hidden", False)),
            has_adjustments=_bool_value(item.get("has_adjustments", False)),
            live_photo=_bool_value(item.get("live_photo", False)),
            burst=_bool_value(item.get("burst", False)),
            screenshot=_bool_value(item.get("screenshot", False)),
            slow_mo=_bool_value(item.get("slow_mo", False)),
            time_lapse=_bool_value(item.get("time_lapse", False)),
            is_missing=_bool_value(item.get("is_missing", False)),
            place_name=_clean(item.get("place_name", "")),
            place_city=_clean(item.get("place_city", "")),
            place_state=_clean(item.get("place_state", "")),
            place_country=_clean(item.get("place_country", "")),
            latitude=_float_value(item.get("latitude", 0.0)),
            longitude=_float_value(item.get("longitude", 0.0)),
            metadata_sha=_clean(item.get("metadata_sha", "")),
        )
        field_sources: dict[str, str] = {}
        if card.person_labels:
            field_sources["person_labels"] = PRIVATE_PEOPLE_SOURCE
            if card.people:
                field_sources["people"] = PRIVATE_PEOPLE_SOURCE
        if card.labels:
            field_sources["labels"] = PRIVATE_LABEL_SOURCE
        provenance = deterministic_provenance(card, ASSET_SOURCE, field_sources=field_sources)
        return card, provenance, str(item.get("body", "")).strip()

    def merge_card(self, vault_path, rel_path, card, body, provenance) -> None:
        self._replace_generic_card(vault_path, rel_path, card, body, provenance)
