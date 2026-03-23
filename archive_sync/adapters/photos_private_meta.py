"""Optional private metadata helpers for Apple Photos assets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

PLACEHOLDER_PERSON_LABELS = frozenset({"_unknown_", "unknown"})


def _clean_list(
    values: Any,
    *,
    lowercase: bool = False,
    invalid_values: set[str] | frozenset[str] | None = None,
) -> list[str]:
    if not isinstance(values, (list, tuple, set)):
        return []
    cleaned: list[str] = []
    seen: set[str] = set()
    blocked = {value.lower() for value in (invalid_values or set())}
    for value in values:
        if value is None:
            continue
        text = " ".join(str(value).strip().split())
        if not text:
            continue
        if text.lower() in blocked:
            continue
        if lowercase:
            text = text.lower()
        if text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return cleaned


@dataclass(frozen=True)
class PrivatePhotoMetadata:
    person_labels: list[str]
    labels: list[str]


class PhotosPrivateMetadataProvider:
    """Interface for optional private metadata extraction."""

    def metadata_for_photo(self, photo: Any) -> PrivatePhotoMetadata:
        return PrivatePhotoMetadata(person_labels=[], labels=[])


class NullPhotosPrivateMetadataProvider(PhotosPrivateMetadataProvider):
    """No-op provider used when private metadata is disabled."""


class OSXPhotosPrivateMetadataProvider(PhotosPrivateMetadataProvider):
    """Read best-effort people and label metadata from osxphotos."""

    def __init__(self, *, include_people: bool = True, include_labels: bool = True):
        self.include_people = bool(include_people)
        self.include_labels = bool(include_labels)

    def metadata_for_photo(self, photo: Any) -> PrivatePhotoMetadata:
        people = (
            _clean_list(getattr(photo, "persons", []), invalid_values=PLACEHOLDER_PERSON_LABELS)
            if self.include_people
            else []
        )
        raw_labels = getattr(photo, "labels_normalized", None)
        if raw_labels is None:
            raw_labels = getattr(photo, "labels", [])
        labels = _clean_list(raw_labels, lowercase=True) if self.include_labels else []
        return PrivatePhotoMetadata(person_labels=people, labels=labels)
