"""Contained vault path resolution.

User-supplied and index-resolved paths must stay inside the configured vault.
This helper is the single check for card reads, UID→path reads, attachments,
and writes. It is not a substitute for OS process isolation: a same-UID
attacker who can mutate the vault during a check/open window can still race
``Path.resolve()``. Writes use directory-relative descriptors and no-follow
flags on POSIX to shrink that window; those flags are not available on every
platform.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path
from typing import Literal

_DENIAL = "path is not inside the vault"
_WINDOWS_DRIVE_PREFIX = ("\\\\", "//")


class PathEscapeError(ValueError):
    """Absolute, traversing, NUL, or symlink-escaping path rejected."""

    def __init__(self, message: str = _DENIAL):
        super().__init__(message)


def normalize_vault_rel(user_path: str) -> Path:
    """Reject absolute paths, ``..``, and NULs. Return a relative ``Path``."""

    if user_path is None:
        raise PathEscapeError(_DENIAL)
    raw = str(user_path)
    if "\x00" in raw:
        raise PathEscapeError(_DENIAL)
    stripped = raw.strip()
    if not stripped:
        raise PathEscapeError(_DENIAL)
    if stripped.startswith(_WINDOWS_DRIVE_PREFIX) or stripped.startswith("\\"):
        raise PathEscapeError(_DENIAL)
    if len(stripped) >= 2 and stripped[1] == ":" and stripped[0].isalpha():
        raise PathEscapeError(_DENIAL)

    parsed = Path(stripped)
    if parsed.is_absolute() or stripped.startswith("/"):
        raise PathEscapeError(_DENIAL)
    if parsed.anchor or parsed.drive:
        raise PathEscapeError(_DENIAL)

    parts: list[str] = []
    for part in parsed.parts:
        if part == "..":
            raise PathEscapeError(_DENIAL)
        if part in {"", "."}:
            continue
        if "\x00" in part:
            raise PathEscapeError(_DENIAL)
        parts.append(part)
    if not parts:
        raise PathEscapeError(_DENIAL)
    return Path(*parts)


def _require_under_root(candidate: Path, root: Path) -> Path:
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise PathEscapeError(_DENIAL) from exc
    return candidate


def _posix_parent_dir_fd(parent: Path) -> int:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    return os.open(parent, flags)


def _mkdir_contained_child(parent: Path, name: str, root: Path) -> Path:
    child = parent / name
    if child.is_symlink():
        raise PathEscapeError(_DENIAL)
    if child.exists():
        if not child.is_dir() or child.is_symlink():
            raise PathEscapeError(_DENIAL)
        return _require_under_root(child.resolve(), root)
    child.mkdir(mode=0o755)
    if child.is_symlink() or not child.is_dir():
        raise PathEscapeError(_DENIAL)
    return _require_under_root(child.resolve(), root)


def _resolve_for_read(root: Path, rel: Path) -> Path:
    candidate = (root / rel).resolve()
    _require_under_root(candidate, root)
    if candidate.exists() and not candidate.is_file():
        raise PathEscapeError(_DENIAL)
    return candidate


def _resolve_for_write(root: Path, rel: Path, *, create_parents: bool) -> Path:
    current = root
    parts = rel.parts
    for index, part in enumerate(parts):
        is_last = index == len(parts) - 1
        nxt = current / part
        if nxt.is_symlink():
            raise PathEscapeError(_DENIAL)
        if is_last:
            if nxt.exists() and (nxt.is_symlink() or not nxt.is_file()):
                raise PathEscapeError(_DENIAL)
            parent = current if current.is_absolute() else current.resolve()
            _require_under_root(parent, root)
            return parent / part
        if nxt.exists():
            if not nxt.is_dir() or nxt.is_symlink():
                raise PathEscapeError(_DENIAL)
            current = _require_under_root(nxt.resolve(), root)
            continue
        if not create_parents:
            raise PathEscapeError(_DENIAL)
        current = _mkdir_contained_child(current, part, root)
    raise PathEscapeError(_DENIAL)


def resolve_contained_path(
    root: str | Path,
    user_path: str,
    *,
    purpose: Literal["read", "write"] = "read",
    create_parents: bool = False,
) -> Path:
    """Resolve *user_path* under *root* or raise ``PathEscapeError``.

    Reads may follow a symlink whose final regular-file target stays inside
    *root*. Writes deny any symlink component, including a symlink target
    file, and only create missing parent directories when ``create_parents``.
    """

    rel = normalize_vault_rel(user_path)
    root_resolved = Path(root).resolve()
    if not root_resolved.is_dir():
        raise PathEscapeError(_DENIAL)
    if purpose == "write":
        return _resolve_for_write(root_resolved, rel, create_parents=create_parents)
    return _resolve_for_read(root_resolved, rel)


def resolve_existing_contained(root: str | Path, path: str | Path) -> Path:
    """Containment-check a path that is already supposed to live under *root*."""

    root_resolved = Path(root).resolve()
    candidate = Path(path).resolve()
    _require_under_root(candidate, root_resolved)
    if candidate.exists() and not candidate.is_file():
        raise PathEscapeError(_DENIAL)
    return candidate


def atomic_write_contained(root: str | Path, user_path: str, data: bytes) -> Path:
    """Write *data* to a contained relative path using no-follow parent fds."""

    target = resolve_contained_path(root, user_path, purpose="write", create_parents=True)
    parent = target.parent
    parent_fd = _posix_parent_dir_fd(parent)
    tmp_name = f".tmp_{target.name}.{os.getpid()}.{secrets.token_hex(4)}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        tmp_fd = os.open(tmp_name, flags, 0o644, dir_fd=parent_fd)
        try:
            with os.fdopen(tmp_fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, target.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        except Exception:
            try:
                os.unlink(tmp_name, dir_fd=parent_fd)
            except OSError:
                pass
            raise
        return target
    finally:
        os.close(parent_fd)
