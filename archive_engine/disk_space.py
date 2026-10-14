"""Volume free space in the units Finder shows."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

_IMPORTANT_USAGE_KEY = "NSURLVolumeAvailableCapacityForImportantUsageKey"
_FOUNDATION: object | None = None
_FOUNDATION_TRIED = False


def volume_available_bytes(path: Path) -> int:
    """Bytes available for an important write, including purgeable space.

    This is ``NSURLVolumeAvailableCapacityForImportantUsageKey``, the capacity
    Finder shows as available. ``shutil.disk_usage().free`` leaves purgeable
    snapshots out, so the same volume can read 39 GB from that call and about
    130 GB in Finder. When the Finder key is unavailable, this falls back to
    ``disk_usage``.
    """

    root = Path(path)
    important = _important_usage_bytes(root)
    if important is not None and important >= 0:
        return int(important)
    return int(shutil.disk_usage(root).free)


def _important_usage_bytes(path: Path) -> int | None:
    foundation = _foundation()
    if foundation is None:
        return None
    url = foundation.NSURL.fileURLWithPath_(str(path))
    values, _error = url.resourceValuesForKeys_error_([_IMPORTANT_USAGE_KEY], None)
    if not values:
        return None
    raw = values.get(_IMPORTANT_USAGE_KEY)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _foundation() -> object | None:
    global _FOUNDATION, _FOUNDATION_TRIED
    if _FOUNDATION_TRIED:
        return _FOUNDATION
    _FOUNDATION_TRIED = True
    if sys.platform != "darwin":
        return None
    try:
        import Foundation
    except ImportError:
        return None
    _FOUNDATION = Foundation
    return Foundation
