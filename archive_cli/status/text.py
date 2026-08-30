"""Human-readable production status from Section F JSON payload."""

from __future__ import annotations

from typing import Any


def format_status_text(payload: dict[str, Any]) -> str:
    """Render concise operator-facing status from the machine-readable payload."""

    if payload.get("blocked"):
        lines = [
            "PPA status: BLOCKED",
            f"  reason: {payload.get('reason')}",
            f"  message: {payload.get('message')}",
        ]
        return "\n".join(lines)

    archive = payload.get("archive") or {}
    lines = [
        f"PPA status: {str(archive.get('status') or 'unknown').upper()}",
        f"  instance: {archive.get('instance')}",
        f"  vault: {archive.get('vault_path')}",
        f"  schema: {archive.get('schema')}",
        f"  engine: {archive.get('engine_mode')}",
    ]

    v3 = payload.get("v3_readiness") or {}
    lines.append(f"v3 readiness: {'READY' if v3.get('ready') else 'NOT READY'}")
    failed = v3.get("failed_checks") or []
    if failed:
        lines.append("  failed:")
        for item in failed:
            lines.append(f"    - {item}")
    blocking = v3.get("blocking_reasons") or []
    if blocking:
        lines.append("  blocking:")
        for item in blocking[:12]:
            lines.append(f"    - {item}")

    sources = payload.get("sources") or []
    bad_sources = [s for s in sources if str(s.get("state")) in ("failed", "blocked", "stale", "never_synced")]
    if bad_sources:
        lines.append("sources:")
        for source in bad_sources[:8]:
            lines.append(
                f"  - {source.get('source_key')}: {source.get('state')} ({source.get('last_error') or 'no error'})"
            )

    proc_totals = payload.get("processor_totals") or {}
    if any(int(proc_totals.get(k) or 0) > 0 for k in ("pending", "stale", "failed")):
        lines.append(
            "processors:"
            f" pending={proc_totals.get('pending', 0)}"
            f" stale={proc_totals.get('stale', 0)}"
            f" failed={proc_totals.get('failed', 0)}"
        )

    errors = payload.get("errors") or []
    if errors:
        lines.append("errors:")
        for err in errors[:8]:
            lines.append(f"  - [{err.get('category')}] {err.get('message') or err.get('reason')}")

    warnings = payload.get("warnings") or []
    if warnings:
        lines.append("warnings:")
        for warn in warnings[:8]:
            lines.append(f"  - [{warn.get('category')}] {warn.get('message')}")

    return "\n".join(lines)
