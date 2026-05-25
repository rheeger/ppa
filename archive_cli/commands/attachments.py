"""Fetch source payloads for archived email attachments."""

from __future__ import annotations

import base64
import hashlib
import logging
from pathlib import Path
from typing import Any

from archive_sync.adapters.gmail_messages import GmailMessagesAdapter
from archive_vault.yaml_parser import parse_frontmatter

from ..errors import InvalidInputError
from ..store import DefaultArchiveStore


def _clean_filename(value: str) -> str:
    name = Path(value.strip()).name
    if not name or name in {".", ".."}:
        raise InvalidInputError("filename is required")
    return name


def _download_path(download_dir: str, filename: str, *, overwrite: bool) -> Path:
    root = Path(download_dir).expanduser() if download_dir.strip() else Path.home() / "Downloads" / "PPA Attachments"
    root.mkdir(parents=True, exist_ok=True)
    target = root / _clean_filename(filename)
    if target.exists() and not overwrite:
        raise InvalidInputError(f"Refusing to overwrite existing file: {target}")
    return target


def _attachment_payload(card: dict[str, Any]) -> dict[str, Any]:
    return {
        "uid": str(card.get("uid") or ""),
        "type": str(card.get("type") or ""),
        "source_id": str(card.get("source_id") or ""),
        "account_email": str(card.get("account_email") or ""),
        "gmail_message_id": str(card.get("gmail_message_id") or ""),
        "gmail_thread_id": str(card.get("gmail_thread_id") or ""),
        "attachment_id": str(card.get("attachment_id") or ""),
        "filename": str(card.get("filename") or ""),
        "mime_type": str(card.get("mime_type") or ""),
        "size_bytes": int(card.get("size_bytes") or 0),
        "content_id": str(card.get("content_id") or ""),
        "is_inline": bool(card.get("is_inline")),
        "attachment_metadata_sha": str(card.get("attachment_metadata_sha") or ""),
        "message": str(card.get("message") or ""),
        "thread": str(card.get("thread") or ""),
    }


def fetch_attachment(
    path_or_uid: str,
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    download: bool = False,
    download_dir: str = "",
    filename: str = "",
    overwrite: bool = False,
    include_base64: bool = False,
) -> dict[str, Any]:
    """Resolve an email attachment card and optionally download its Gmail payload."""

    logger.info("fetch_attachment_start path_or_uid=%r download=%s", path_or_uid, download)
    note = store.read(path_or_uid)
    if not note.get("found"):
        return {"found": False, "path_or_uid": path_or_uid}

    card, _body = parse_frontmatter(str(note.get("content") or ""))
    if str(card.get("type") or "") != "email_attachment":
        raise InvalidInputError("path_or_uid must resolve to an email_attachment card")

    attachment = _attachment_payload(card)
    if not attachment["gmail_message_id"] or not attachment["attachment_id"]:
        raise InvalidInputError("email_attachment card is missing gmail_message_id or attachment_id")

    rel_path = str(note.get("rel_path") or "")
    if not rel_path and str(path_or_uid).endswith(".md"):
        rel_path = str(path_or_uid)

    result: dict[str, Any] = {
        "found": True,
        "path_or_uid": path_or_uid,
        "rel_path": rel_path,
        "attachment": attachment,
        "download": {"downloaded": False},
        "download_hints": {
            "gmail_message_id": attachment["gmail_message_id"],
            "attachment_id": attachment["attachment_id"],
            "account_email": attachment["account_email"],
            "filename": attachment["filename"],
            "gws_args": [
                "gmail",
                "users",
                "messages",
                "attachments",
                "get",
                "--params",
                {
                    "userId": "me",
                    "messageId": attachment["gmail_message_id"],
                    "id": attachment["attachment_id"],
                },
            ],
        },
    }

    if not download and not include_base64:
        logger.info("fetch_attachment_done metadata_only=True")
        return result

    adapter = GmailMessagesAdapter()
    data = adapter.fetch_attachment_bytes(
        attachment["gmail_message_id"],
        attachment["attachment_id"],
        account_email=attachment["account_email"],
    )
    sha256 = hashlib.sha256(data).hexdigest()
    result["download"]["bytes"] = len(data)
    result["download"]["sha256"] = sha256

    if include_base64:
        result["data"] = {
            "encoding": "base64",
            "content": base64.b64encode(data).decode("ascii"),
        }

    if download:
        out_name = filename.strip() or attachment["filename"]
        target = _download_path(download_dir, out_name, overwrite=overwrite)
        target.write_bytes(data)
        result["download"].update(
            {
                "downloaded": True,
                "path": str(target),
            }
        )

    logger.info("fetch_attachment_done downloaded=%s bytes=%s", download, len(data))
    return result
