"""Template era sampler — raw vs clean bodies by year (EDL Phase 2)."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from archive_mcp.commands.census import snapshot_subject_category, snapshot_subject_shape
from archive_mcp.errors import PpaError
from archive_sync.extractors.preprocessing import clean_email_body
from hfa.vault import iter_email_message_notes

_log = logging.getLogger("ppa.template_sampler")


def _domain_matches(from_email: str, domain: str) -> bool:
    from_email = (from_email or "").strip().lower()
    domain = domain.strip().lower().lstrip("@")
    if "@" not in from_email:
        return False
    host = from_email.rsplit("@", 1)[-1]
    return host == domain or host.endswith("." + domain)


def _year_from_sent(sent_at: str) -> int:
    s = (sent_at or "").strip()
    if len(s) >= 4 and s[:4].isdigit():
        return int(s[:4])
    return 0


def _parse_ts(sent_at: str) -> float:
    s = (sent_at or "").strip()
    if not s:
        return 0.0
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return datetime.fromisoformat(s[:10]).timestamp()
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return 0.0


def _even_sample(rows: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    if len(rows) <= n:
        return list(rows)
    rows = sorted(rows, key=lambda r: r["_ts"])
    if n <= 1:
        return [rows[0]]
    out: list[dict[str, Any]] = []
    for i in range(n):
        idx = round(i * (len(rows) - 1) / (n - 1))
        out.append(rows[idx])
    return out


def _append_note_if_matches(
    jobs: list["_TemplateJobSpec"],
    fm: dict[str, Any],
    raw_body: str,
    matches_by_name: dict[str, list[dict[str, Any]]],
) -> None:
    from_email = str(fm.get("from_email") or "")
    subject = str(fm.get("subject") or "")
    subject_l = subject.lower()
    sent_at = str(fm.get("sent_at") or "")
    uid = str(fm.get("uid") or "")
    if not uid:
        return
    yr = _year_from_sent(sent_at)
    if yr <= 0:
        return
    to_emails = fm.get("to_emails") if isinstance(fm.get("to_emails"), list) else []
    from_name = str(fm.get("from_name") or "").strip()
    row_base = {
        "uid": uid,
        "subject": subject,
        "from_email": from_email,
        "from_name": from_name,
        "sent_at": sent_at,
        "to_emails": to_emails,
        "raw_body": raw_body,
        "_ts": _parse_ts(sent_at),
        "_year": yr,
    }
    for job in jobs:
        if not _domain_matches(from_email, job.domain):
            continue
        if job.category_kw and job.category_kw not in subject_l:
            continue
        matches_by_name[job.name].append(dict(row_base))


def _write_samples_for_matches(
    matches: list[dict[str, Any]], *, per_year: int, out_root: Path
) -> tuple[list[int], int]:
    by_year: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for m in matches:
        by_year[m["_year"]].append(m)

    written = 0
    for year in sorted(by_year.keys()):
        pool = by_year[year]
        picked = _even_sample(pool, per_year)
        ydir = out_root / str(year)
        ydir.mkdir(parents=True, exist_ok=True)
        for row in picked:
            uid = row["uid"]
            raw_body = row["raw_body"]
            clean_body = clean_email_body(raw_body)
            (ydir / f"{uid}.raw.txt").write_text(raw_body, encoding="utf-8")
            (ydir / f"{uid}.clean.txt").write_text(clean_body, encoding="utf-8")
            subj = str(row["subject"] or "")
            meta = {
                "uid": uid,
                "subject": subj,
                "from_email": row["from_email"],
                "from_name": str(row.get("from_name") or ""),
                "sent_at": row["sent_at"],
                "to_emails": row["to_emails"],
                "subject_category": snapshot_subject_category(subj),
                "subject_shape": snapshot_subject_shape(subj),
            }
            (ydir / f"{uid}.meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
            written += 1
    return sorted(by_year.keys()), written


@dataclass(frozen=True)
class _TemplateJobSpec:
    name: str
    domain: str
    category_kw: str
    out_dir: Path


def run_template_sampler(
    *,
    vault_path: str,
    domain: str,
    category: str = "",
    per_year: int = 3,
    out_dir: str,
) -> dict[str, Any]:
    """Write per-year raw/clean/meta samples for template era discovery."""
    r = run_template_sampler_batch(
        vault_path=vault_path,
        jobs=[
            {
                "name": "default",
                "domain": domain,
                "category": category,
                "out_dir": out_dir,
            }
        ],
        per_year=per_year,
    )
    j = r["jobs"]["default"]
    return {"years": j["years"], "files_written": j["files_written"], "out_dir": j["out_dir"]}


def run_template_sampler_batch(
    *,
    vault_path: str,
    jobs: list[dict[str, Any]],
    per_year: int = 3,
) -> dict[str, Any]:
    """One Email/ walk; each job filters by domain + optional subject substring and writes to its out_dir.

    Each job dict: ``name`` (unique id), ``domain``, ``out_dir``, optional ``category``.
    """
    if not jobs:
        return {"vault": vault_path, "jobs": {}, "scanned": 0}

    specs: list[_TemplateJobSpec] = []
    for j in jobs:
        name = str(j.get("name") or "").strip()
        domain = str(j.get("domain") or "").strip()
        out = str(j.get("out_dir") or "").strip()
        if not name or not domain or not out:
            raise PpaError("each batch job requires name, domain, and out_dir")
        cat = str(j.get("category") or "").strip().lower()
        specs.append(
            _TemplateJobSpec(
                name=name,
                domain=domain,
                category_kw=cat,
                out_dir=Path(out).expanduser(),
            )
        )

    names = [s.name for s in specs]
    if len(names) != len(set(names)):
        raise PpaError("batch job names must be unique")

    matches_by_name: dict[str, list[dict[str, Any]]] = {s.name: [] for s in specs}
    scanned = 0
    for note in iter_email_message_notes(vault_path):
        scanned += 1
        if scanned % 25_000 == 0:
            _log.info("template-sampler progress: read %s notes under Email/", f"{scanned:,}")
        fm = note.frontmatter
        if fm.get("type") != "email_message":
            continue
        raw_body = str(note.body or "")
        _append_note_if_matches(specs, fm, raw_body, matches_by_name)

    out_jobs: dict[str, Any] = {}
    for spec in specs:
        pool = matches_by_name[spec.name]
        out_root = spec.out_dir
        out_root.mkdir(parents=True, exist_ok=True)
        years, written = _write_samples_for_matches(pool, per_year=per_year, out_root=out_root)
        _log.info(
            "template-sampler batch job=%s domain=%s category=%s years=%s files=%s",
            spec.name,
            spec.domain,
            spec.category_kw or "(none)",
            len(years),
            written,
        )
        out_jobs[spec.name] = {
            "years": years,
            "files_written": written,
            "out_dir": str(out_root),
            "domain": spec.domain,
            "category": spec.category_kw or "",
        }

    _log.info("template-sampler batch complete: scanned=%s jobs=%s", f"{scanned:,}", len(specs))
    return {"vault": vault_path, "scanned": scanned, "jobs": out_jobs}


def run_template_sampler_from_batch_file(
    *,
    vault_path: str,
    batch_path: str,
    per_year: int = 3,
    base_dir: Path | None = None,
) -> dict[str, Any]:
    """Load job list from JSON (array of objects); resolve relative ``out_dir`` against ``base_dir`` (default cwd)."""
    raw = Path(batch_path).read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, list):
        raise PpaError("batch file must be a JSON array")
    root = base_dir if base_dir is not None else Path.cwd()
    jobs: list[dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise PpaError(f"batch[{i}] must be an object")
        out = str(row.get("out_dir") or "").strip()
        p = Path(out)
        if not p.is_absolute():
            p = (root / p).resolve()
        jobs.append({**row, "out_dir": str(p)})
    return run_template_sampler_batch(vault_path=vault_path, jobs=jobs, per_year=per_year)
