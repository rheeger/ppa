"""Epic EHI TSV ingestion for archive-sync medical records."""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ..cli_logging import CliProgressReporter, log_cli_step

EPIC_EHI_SOURCE = "epic.ehi"
DATE_PATTERNS = (
    "%Y-%m-%d",
    "%Y-%m",
    "%Y",
    "%m/%d/%Y",
    "%m/%Y",
)
WHITESPACE_RE = re.compile(r"\s+")


def _clean(value: Any) -> str:
    return WHITESPACE_RE.sub(" ", str(value or "").strip())


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = _clean(value)
        if text:
            return text
    return ""


def _normalize_name_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _clean(value).lower()).strip()


def _parse_date_like(value: str) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    if raw.endswith("Z"):
        return raw
    for token in ("T",):
        if token in raw:
            return raw
    for fmt in DATE_PATTERNS:
        try:
            parsed = datetime.strptime(raw, fmt)
        except ValueError:
            continue
        if fmt == "%Y":
            return parsed.strftime("%Y")
        if fmt == "%Y-%m":
            return parsed.strftime("%Y-%m")
        if fmt == "%m/%Y":
            return parsed.strftime("%Y-%m")
        return parsed.strftime("%Y-%m-%d")
    return raw


def _safe_float(value: Any) -> float:
    if value in ("", None):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _bucket_date(value: str, *, fallback: str | None = None) -> str:
    normalized = _parse_date_like(value)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized):
        return normalized
    if re.fullmatch(r"\d{4}-\d{2}", normalized):
        return f"{normalized}-01"
    if re.fullmatch(r"\d{4}", normalized):
        return f"{normalized}-01-01"
    return fallback or date.today().isoformat()


def _file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_tsv_rows(path: str | Path | None) -> list[dict[str, str]]:
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    rows: list[dict[str, str]] = []
    text = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    if not text:
        return rows
    headers = text[0].split("\t")
    for raw_line in text[1:]:
        values = raw_line.split("\t")
        if len(values) < len(headers):
            values.extend([""] * (len(headers) - len(values)))
        row = {header: values[index] if index < len(values) else "" for index, header in enumerate(headers)}
        rows.append(row)
    return rows


def _looks_like_vaccine(text: str) -> bool:
    lowered = _clean(text).lower()
    return any(
        token in lowered
        for token in (
            "vaccine",
            "vacc.",
            "vaccination",
            "immunization",
            "hepatitis b",
            "dtap",
            "hib",
            "rotavirus",
            "pneumococcal",
            "polio",
            "influenza",
            "mmr",
            "varicella",
            "pcv",
            "ipv",
            "pediarix",
            "engerix",
            "prevnar",
            "nirsevimab",
            "rsv",
        )
    )


_VACCINE_STOP = frozenset(
    {
        "pf",
        "for",
        "age",
        "yr",
        "yrs",
        "week",
        "weeks",
        "mos",
        "ml",
        "mcg",
        "mg",
        "injection",
        "vaccine",
        "the",
        "and",
        "with",
        "old",
        "than",
        "less",
        "greater",
        "up",
        "to",
    }
)


def _significant_tokens(text: str) -> set[str]:
    return {
        t
        for t in _normalize_name_key(text).split()
        if len(t) >= 3 and t not in _VACCINE_STOP
    }


def _vaccine_same_event(date_a: str, label_a: str, date_b: str, label_b: str) -> bool:
    if _bucket_date(date_a) != _bucket_date(date_b):
        return False
    ta, tb = _significant_tokens(label_a), _significant_tokens(label_b)
    if ta & tb:
        return True
    la, lb = _clean(label_a).lower(), _clean(label_b).lower()
    if len(la) > 10 and la in lb:
        return True
    if len(lb) > 10 and lb in la:
        return True
    return False


def _resolve_patient_row(
    patient_rows: list[dict[str, str]],
    epic_pat_id: str | None,
) -> tuple[str, dict[str, str]]:
    if not patient_rows:
        raise ValueError("Epic EHI tables import requires PATIENT.tsv with at least one row")
    ids_ordered: list[str] = []
    seen: set[str] = set()
    for row in patient_rows:
        pid = _clean(row.get("PAT_ID"))
        if not pid:
            continue
        if pid not in seen:
            seen.add(pid)
            ids_ordered.append(pid)
    if not ids_ordered:
        raise ValueError("Epic EHI PATIENT.tsv did not expose PAT_ID")
    if epic_pat_id:
        want = _clean(epic_pat_id)
        if want not in seen:
            raise ValueError(f"epic_pat_id {want!r} not found in PATIENT.tsv PAT_ID values {sorted(seen)}")
        picked = next(r for r in patient_rows if _clean(r.get("PAT_ID")) == want)
        return want, picked
    if len(ids_ordered) > 1:
        raise ValueError(
            "Epic EHI PATIENT.tsv contains multiple PAT_ID values; pass epic_pat_id to select the patient anchor."
        )
    pid = ids_ordered[0]
    return pid, next(r for r in patient_rows if _clean(r.get("PAT_ID")) == pid)


def _hash_present_tables(base: Path, rels: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for rel in rels:
        path = base / rel
        if path.exists():
            key = rel.lower().replace(".tsv", "").replace("/", "_")
            out[key] = _file_sha256(path)
    return out


def collect_epic_ehi_items(
    *,
    ehi_tables_dir: str | Path,
    person_link: str,
    source_id: str,
    verbose: bool,
    progress_every: int | None,
    epic_pat_id: str | None = None,
    include_order_results: bool = True,
    include_adt: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build structured medical and vaccination items from Epic EHI TSV exports."""
    base = Path(ehi_tables_dir)
    total_steps = 9

    def _log_step(step_number: int, title: str, detail: str = "") -> None:
        if verbose:
            log_cli_step(source_id, step_number, total_steps, title, detail)

    _log_step(1, "Epic EHI: resolve patient anchor", f"dir={base}")
    patient_rows = _read_tsv_rows(base / "PATIENT.tsv")
    patient_id, _patient_row = _resolve_patient_row(patient_rows, epic_pat_id)
    _log_step(1, "Epic EHI: patient anchor ready", f"PAT_ID={patient_id}")

    table_list = [
        "PATIENT.tsv",
        "ORDER_MED.tsv",
        "CLARITY_MEDICATION.tsv",
        "ORDER_MEDINFO.tsv",
        "DOCS_RCVD.tsv",
        "IMM_ADMIN.tsv",
        "PAT_IMMUNIZATIONS.tsv",
        "IMMUNE.tsv",
        "IMMUNE_HISTORY.tsv",
        "CLARITY_IMMUNZATN.tsv",
        "PAT_PROBLEM_LIST.tsv",
        "PROBLEM_LIST.tsv",
        "PAT_ENC.tsv",
        "PAT_ENC_DX.tsv",
        "CLARITY_EDG.tsv",
        "CLARITY_DEP.tsv",
        "CLARITY_ADT.tsv",
        "ORDER_PROC.tsv",
        "ORDER_RESULTS.tsv",
        "CLARITY_COMPONENT.tsv",
    ]
    source_hashes = _hash_present_tables(base, table_list)

    _log_step(2, "Epic EHI: load reference tables", "")
    medication_names = {
        _clean(row.get("MEDICATION_ID")): _first_nonempty(row.get("GENERIC_NAME"), row.get("NAME"))
        for row in _read_tsv_rows(base / "CLARITY_MEDICATION.tsv")
        if _clean(row.get("MEDICATION_ID"))
    }
    medinfo_by_order: dict[str, dict[str, str]] = {}
    for row in _read_tsv_rows(base / "ORDER_MEDINFO.tsv"):
        oid = _clean(row.get("ORDER_MED_ID"))
        if oid:
            medinfo_by_order[oid] = row
    edg = {_clean(r.get("DX_ID")): r for r in _read_tsv_rows(base / "CLARITY_EDG.tsv") if _clean(r.get("DX_ID"))}
    dep_names = {
        _clean(r.get("DEPARTMENT_ID")): _first_nonempty(r.get("DEPARTMENT_NAME"), r.get("DEP_NAME"))
        for r in _read_tsv_rows(base / "CLARITY_DEP.tsv")
        if _clean(r.get("DEPARTMENT_ID"))
    }
    component_names = {
        _clean(r.get("COMPONENT_ID")): _clean(r.get("NAME"))
        for r in _read_tsv_rows(base / "CLARITY_COMPONENT.tsv")
        if _clean(r.get("COMPONENT_ID"))
    }

    docs_for_patient = {
        _clean(r.get("DOCUMENT_ID"))
        for r in _read_tsv_rows(base / "DOCS_RCVD.tsv")
        if _clean(r.get("PAT_ID")) == patient_id and _clean(r.get("DOCUMENT_ID"))
    }

    pat_enc_rows = [r for r in _read_tsv_rows(base / "PAT_ENC.tsv") if _clean(r.get("PAT_ID")) == patient_id]
    enc_csns = {_clean(r.get("PAT_ENC_CSN_ID")) for r in pat_enc_rows if _clean(r.get("PAT_ENC_CSN_ID"))}

    order_proc_ids = {
        _clean(r.get("ORDER_PROC_ID"))
        for r in _read_tsv_rows(base / "ORDER_PROC.tsv")
        if _clean(r.get("PAT_ID")) == patient_id and _clean(r.get("ORDER_PROC_ID"))
    }

    order_med_rows = _read_tsv_rows(base / "ORDER_MED.tsv")
    order_med_patient = [r for r in order_med_rows if _clean(r.get("PAT_ID")) == patient_id]
    _log_step(
        2,
        "Epic EHI: lookups ready",
        f"medications={len(medication_names)} order_med={len(order_med_patient)} enc={len(pat_enc_rows)} proc={len(order_proc_ids)}",
    )

    imm_admin_rows = [
        r
        for r in _read_tsv_rows(base / "IMM_ADMIN.tsv")
        if _clean(r.get("DOCUMENT_ID")) in docs_for_patient
    ]

    imm_admin_vaccinations: list[dict[str, Any]] = []
    for row in imm_admin_rows:
        doc_id = _clean(row.get("DOCUMENT_ID"))
        line = _clean(row.get("LINE"))
        cdr = _clean(row.get("CONTACT_DATE_REAL"))
        occurred = _parse_date_like(_first_nonempty(row.get("IMM_DATE"), row.get("CONTACT_DATE")))
        vname = _first_nonempty(row.get("IMM_TYPE_ID_NAME"), row.get("IMM_TYPE_FREE_TEXT"), row.get("IMM_PRODUCT_C_NAME"))
        details_json = {
            "epic_pathway": "epic_ehi_immunizations",
            "epic_patient_id": patient_id,
            "document_id": doc_id,
            "contact_date_real": cdr,
            "line": line,
            "imm_type_id": _clean(row.get("IMM_TYPE_ID")),
            "imm_route": _clean(row.get("IMM_ROUTE_C_NAME")),
            "imm_status": _clean(row.get("IMM_STATUS_C_NAME")),
            "imm_location": _clean(row.get("IMM_LOCATION")),
            "source_hashes": source_hashes,
        }
        imm_admin_vaccinations.append(
            {
                "kind": "vaccination",
                "source": [EPIC_EHI_SOURCE],
                "source_id": f"epic:imm_admin:{doc_id}:{cdr or 'na'}:{line or '0'}",
                "summary": vname or "Immunization (Epic)",
                "created": _bucket_date(occurred),
                "people": [person_link],
                "source_system": "epic",
                "source_format": "ehi_tsv",
                "occurred_at": occurred,
                "vaccine_name": vname or "Immunization (Epic)",
                "cvx_code": _clean(row.get("IMM_TYPE_ID")),
                "status": _clean(row.get("IMM_STATUS_C_NAME")) or "documented",
                "manufacturer": _first_nonempty(row.get("IMM_MANUFACTURER_C_NAME"), row.get("IMM_MANUF_FREE_TEXT")),
                "brand_name": _clean(row.get("IMM_PRODUCT_C_NAME")),
                "lot_number": _clean(row.get("IMM_LOT_NUMBER")),
                "expiration_date": "",
                "administered_at": _clean(row.get("CONTACT_DATE")),
                "performer_name": _clean(row.get("IMM_GIVEN_BY_ID_NAME")),
                "location": _clean(row.get("IMM_LOCATION")),
                "raw_source_ref": f"epic:imm_admin:{doc_id}:{line}",
                "details_json": details_json,
            }
        )

    _log_step(3, "Epic EHI: medications + vaccine orders", f"imm_admin_rows={len(imm_admin_vaccinations)}")
    med_reporter = CliProgressReporter(
        source_id=source_id,
        step_number=3,
        total_steps=total_steps,
        stage="Epic EHI order_med",
        total_items=max(len(order_med_patient), 1),
        progress_every=progress_every,
        enabled=verbose,
    )
    medication_items: list[dict[str, Any]] = []
    order_vacc_items: list[dict[str, Any]] = []
    processed = 0
    for row in order_med_patient:
        medication_id = _clean(row.get("MEDICATION_ID"))
        description = _first_nonempty(
            row.get("DISPLAY_NAME"),
            row.get("DESCRIPTION"),
            medication_names.get(medication_id, ""),
        )
        occurred_at = _parse_date_like(
            _first_nonempty(row.get("START_DATE"), row.get("ORDERING_DATE"), row.get("END_DATE"), row.get("ORDER_INST"))
        )
        order_med_id = _clean(row.get("ORDER_MED_ID"))
        medinfo = medinfo_by_order.get(order_med_id, {})
        rate_bits = _first_nonempty(
            medinfo.get("CALC_DOSE_INFO"),
            medinfo.get("MIN_RATE") and f"rate {medinfo.get('MIN_RATE')}-{medinfo.get('MAX_RATE')} {medinfo.get('RATE_UNIT_C_NAME', '')}".strip(),
            medinfo.get("MIN_VOLUME") and f"vol {medinfo.get('MIN_VOLUME')}-{medinfo.get('MAX_VOLUME')} {medinfo.get('VOLUME_UNIT_C_NAME', '')}".strip(),
        )
        details_json = {
            "epic_pathway": "epic_ehi_medications",
            "epic_patient_id": patient_id,
            "order_med_id": order_med_id,
            "medication_id": medication_id,
            "encounter_id": _clean(row.get("PAT_ENC_CSN_ID")),
            "order_class": _clean(row.get("ORDER_CLASS_C_NAME")),
            "route": _clean(row.get("MED_ROUTE_C_NAME")),
            "display_name": _clean(row.get("DISPLAY_NAME")),
            "description": _clean(row.get("DESCRIPTION")),
            "ordering_date": _clean(row.get("ORDERING_DATE")),
            "start_date": _clean(row.get("START_DATE")),
            "end_date": _clean(row.get("END_DATE")),
            "quantity": _clean(row.get("QUANTITY")),
            "refills": _clean(row.get("REFILLS")),
            "order_medinfo": {k: _clean(v) for k, v in medinfo.items() if _clean(v)},
            "source_hashes": source_hashes,
        }
        value_text = _first_nonempty(row.get("DOSAGE"), row.get("ORDER_INST"), rate_bits, row.get("MED_ROUTE_C_NAME"))

        if _looks_like_vaccine(description):
            skip = False
            for imm in imm_admin_rows:
                imm_date = _first_nonempty(imm.get("IMM_DATE"), imm.get("CONTACT_DATE"))
                imm_name = _first_nonempty(imm.get("IMM_TYPE_ID_NAME"), imm.get("IMM_TYPE_FREE_TEXT"))
                order_date = _first_nonempty(row.get("ORDER_INST"), row.get("START_DATE"), row.get("ORDERING_DATE"))
                if _vaccine_same_event(imm_date, imm_name, order_date, description):
                    skip = True
                    break
            if not skip:
                order_vacc_items.append(
                    {
                        "kind": "vaccination",
                        "source": [EPIC_EHI_SOURCE],
                        "source_id": f"epic:order_med:{order_med_id}",
                        "summary": description,
                        "created": _bucket_date(occurred_at),
                        "people": [person_link],
                        "source_system": "epic",
                        "source_format": "ehi_tsv",
                        "occurred_at": occurred_at,
                        "vaccine_name": description,
                        "cvx_code": "",
                        "status": "ordered",
                        "manufacturer": "",
                        "brand_name": medication_names.get(medication_id, ""),
                        "lot_number": "",
                        "expiration_date": "",
                        "administered_at": _clean(row.get("ORDER_INST")),
                        "performer_name": _clean(row.get("ORD_CREATR_USER_ID_NAME")),
                        "location": "",
                        "raw_source_ref": f"epic:order_med:{order_med_id}",
                        "details_json": {
                            **details_json,
                            "epic_pathway": "epic_ehi_medications",
                            "vaccine_from": "order_med",
                        },
                    }
                )
        else:
            medication_items.append(
                {
                    "kind": "medical_record",
                    "source": [EPIC_EHI_SOURCE],
                    "source_id": f"epic:order_med:{order_med_id}",
                    "summary": description,
                    "created": _bucket_date(occurred_at),
                    "people": [person_link],
                    "source_system": "epic",
                    "source_format": "ehi_tsv",
                    "record_type": "medication_request",
                    "record_subtype": _clean(row.get("ORDER_CLASS_C_NAME")) or "order_med",
                    "status": _clean(row.get("RSN_FOR_DISCON_C_NAME")) or "ordered",
                    "occurred_at": occurred_at,
                    "recorded_at": _parse_date_like(_clean(row.get("ORDERING_DATE"))),
                    "provider_name": _clean(row.get("ORD_CREATR_USER_ID_NAME")),
                    "facility_name": "",
                    "encounter_source_id": _clean(row.get("PAT_ENC_CSN_ID")),
                    "code_system": "epic_medication_id",
                    "code": medication_id,
                    "code_display": medication_names.get(medication_id, description),
                    "value_text": value_text,
                    "value_numeric": 0.0,
                    "unit": _clean(row.get("MED_ROUTE_C_NAME")),
                    "raw_source_ref": f"epic:order_med:{order_med_id}",
                    "details_json": details_json,
                }
            )
        processed += 1
        med_reporter.update(processed)
    med_reporter.complete(processed, extra=f"meds={len(medication_items)} order_vacc={len(order_vacc_items)}")

    _log_step(4, "Epic EHI: PAT_IMMUNIZATIONS + IMMUNE", "")
    pat_imm_rows = [r for r in _read_tsv_rows(base / "PAT_IMMUNIZATIONS.tsv") if _clean(r.get("PAT_ID")) == patient_id]
    immune_by_id = {_clean(r.get("IMMUNE_ID")): r for r in _read_tsv_rows(base / "IMMUNE.tsv") if _clean(r.get("IMMUNE_ID"))}
    imm_hx_by_immune: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in _read_tsv_rows(base / "IMMUNE_HISTORY.tsv"):
        iid = _clean(r.get("IMMUNE_ID"))
        if iid:
            imm_hx_by_immune[iid].append(r)

    pat_imm_items: list[dict[str, Any]] = []
    imm_reporter = CliProgressReporter(
        source_id=source_id,
        step_number=4,
        total_steps=total_steps,
        stage="Epic EHI pat_immunizations",
        total_items=max(len(pat_imm_rows), 1),
        progress_every=progress_every,
        enabled=verbose,
    )
    for idx, row in enumerate(pat_imm_rows, start=1):
        line = _clean(row.get("LINE"))
        iid = _clean(row.get("IMMUNE_ID"))
        immune_row = immune_by_id.get(iid, {})
        lot_id = _clean(immune_row.get("IMM_LOT_NUM_ID"))
        imm_name = "Epic immunization record"
        hx = imm_hx_by_immune.get(iid, [])
        details_json = {
            "epic_pathway": "epic_ehi_immunizations",
            "epic_patient_id": patient_id,
            "immune_id": iid,
            "line": line,
            "imm_lot_num_id": lot_id,
            "immune_history_lines": len(hx),
            "source_hashes": source_hashes,
        }
        pat_imm_items.append(
            {
                "kind": "vaccination",
                "source": [EPIC_EHI_SOURCE],
                "source_id": f"epic:pat_imm:{patient_id}:{line or iid}",
                "summary": imm_name,
                "created": _bucket_date(date.today().isoformat()),
                "people": [person_link],
                "source_system": "epic",
                "source_format": "ehi_tsv",
                "occurred_at": "",
                "vaccine_name": imm_name,
                "cvx_code": "",
                "status": "recorded",
                "manufacturer": "",
                "brand_name": "",
                "lot_number": lot_id,
                "expiration_date": "",
                "administered_at": "",
                "performer_name": "",
                "location": "",
                "raw_source_ref": f"epic:pat_imm:{patient_id}:{line}",
                "details_json": details_json,
            }
        )
        imm_reporter.update(idx)
    imm_reporter.complete(len(pat_imm_rows), extra=f"pat_imm_cards={len(pat_imm_items)}")

    _log_step(5, "Epic EHI: problems / conditions", "")
    problem_rows = [r for r in _read_tsv_rows(base / "PAT_PROBLEM_LIST.tsv") if _clean(r.get("PAT_ID")) == patient_id]
    problem_meta = {
        _clean(r.get("PROBLEM_LIST_ID")): r for r in _read_tsv_rows(base / "PROBLEM_LIST.tsv") if _clean(r.get("PROBLEM_LIST_ID"))
    }
    dx_link_rows = [r for r in _read_tsv_rows(base / "PAT_ENC_DX.tsv") if _clean(r.get("PAT_ENC_CSN_ID")) in enc_csns]
    dx_by_problem: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in dx_link_rows:
        plid = _clean(r.get("DX_LINK_PROB_ID"))
        if plid:
            dx_by_problem[plid].append(r)

    problem_items: list[dict[str, Any]] = []
    prob_reporter = CliProgressReporter(
        source_id=source_id,
        step_number=5,
        total_steps=total_steps,
        stage="Epic EHI problems",
        total_items=max(len(problem_rows), 1),
        progress_every=progress_every,
        enabled=verbose,
    )
    for idx, row in enumerate(problem_rows, start=1):
        plid = _clean(row.get("PROBLEM_LIST_ID"))
        meta = problem_meta.get(plid, {})
        dx_rows_p = dx_by_problem.get(plid, [])
        primary = next((d for d in dx_rows_p if _clean(d.get("PRIMARY_DX_YN")).upper() == "Y"), dx_rows_p[0] if dx_rows_p else {})
        dx_id = _clean(primary.get("DX_ID"))
        ed = edg.get(dx_id, {})
        label = _first_nonempty(ed.get("PAT_FRIENDLY_TEXT"), ed.get("DX_NAME"), f"Epic problem {plid}")
        onset = _parse_date_like(_first_nonempty(meta.get("DIAG_START_DATE"), primary.get("CONTACT_DATE")))
        ended = _parse_date_like(_clean(meta.get("DIAG_END_DATE")))
        enc_csn = _clean(primary.get("PAT_ENC_CSN_ID"))
        details_json = {
            "epic_pathway": "epic_ehi_problems",
            "epic_patient_id": patient_id,
            "problem_list_id": plid,
            "dx_id": dx_id,
            "dx_links": len(dx_rows_p),
            "source_hashes": source_hashes,
        }
        problem_items.append(
            {
                "kind": "medical_record",
                "source": [EPIC_EHI_SOURCE],
                "source_id": f"epic:problem:{patient_id}:{plid}",
                "summary": label,
                "created": _bucket_date(onset or date.today().isoformat()),
                "people": [person_link],
                "source_system": "epic",
                "source_format": "ehi_tsv",
                "record_type": "condition",
                "record_subtype": "problem_list",
                "status": _clean(primary.get("DX_CHRONIC_YN")) or "active",
                "occurred_at": onset,
                "recorded_at": _parse_date_like(_clean(primary.get("CONTACT_DATE"))),
                "provider_name": "",
                "facility_name": "",
                "encounter_source_id": enc_csn,
                "code_system": "epic_dx_id",
                "code": dx_id,
                "code_display": _clean(ed.get("DX_NAME")) or label,
                "value_text": _first_nonempty(primary.get("COMMENTS"), primary.get("ANNOTATION")),
                "value_numeric": 0.0,
                "unit": "",
                "raw_source_ref": f"epic:problem:{plid}",
                "details_json": details_json,
            }
        )
        if ended:
            problem_items[-1]["details_json"]["diag_end_date"] = ended
        prob_reporter.update(idx)
    prob_reporter.complete(len(problem_rows), extra=f"problems={len(problem_items)}")

    _log_step(6, "Epic EHI: encounters (PAT_ENC)", f"rows={len(pat_enc_rows)}")
    encounter_items: list[dict[str, Any]] = []
    enc_reporter = CliProgressReporter(
        source_id=source_id,
        step_number=6,
        total_steps=total_steps,
        stage="Epic EHI PAT_ENC",
        total_items=max(len(pat_enc_rows), 1),
        progress_every=progress_every,
        enabled=verbose,
    )
    for idx, row in enumerate(pat_enc_rows, start=1):
        csn = _clean(row.get("PAT_ENC_CSN_ID"))
        dept_id = _clean(row.get("DEPARTMENT_ID"))
        dept_name = dep_names.get(dept_id, dept_id)
        contact = _parse_date_like(_clean(row.get("CONTACT_DATE")))
        admit = _clean(row.get("HOSP_ADMSN_TIME"))
        disch = _clean(row.get("HOSP_DISCHRG_TIME"))
        value_bits = _first_nonempty(
            admit and f"admit {admit}",
            disch and f"discharge {disch}",
            _clean(row.get("HOSP_ADMSN_TYPE_C_NAME")),
        )
        encounter_items.append(
            {
                "kind": "medical_record",
                "source": [EPIC_EHI_SOURCE],
                "source_id": f"epic:encounter:{csn}",
                "summary": _first_nonempty(dept_name, f"Encounter {csn}"),
                "created": _bucket_date(contact),
                "people": [person_link],
                "source_system": "epic",
                "source_format": "ehi_tsv",
                "record_type": "encounter",
                "record_subtype": _clean(row.get("APPT_STATUS_C_NAME")) or "pat_enc",
                "status": _clean(row.get("ENC_CLOSED_YN")),
                "occurred_at": contact,
                "recorded_at": _parse_date_like(_clean(row.get("UPDATE_DATE"))),
                "provider_name": _clean(row.get("VISIT_PROV_TITLE_NAME")),
                "facility_name": dept_name,
                "encounter_source_id": csn,
                "code_system": "epic_pat_enc_csn",
                "code": csn,
                "code_display": dept_name,
                "value_text": value_bits,
                "value_numeric": 0.0,
                "unit": "",
                "raw_source_ref": f"epic:pat_enc:{csn}",
                "details_json": {
                    "epic_pathway": "epic_ehi_encounters",
                    "epic_patient_id": patient_id,
                    "pat_enc_date_real": _clean(row.get("PAT_ENC_DATE_REAL")),
                    "fin_class": _clean(row.get("FIN_CLASS_C_NAME")),
                    "hsp_account_id": _clean(row.get("HSP_ACCOUNT_ID")),
                    "source_hashes": source_hashes,
                },
            }
        )
        enc_reporter.update(idx)
    enc_reporter.complete(len(pat_enc_rows), extra=f"encounters={len(encounter_items)}")

    adt_items: list[dict[str, Any]] = []
    if include_adt:
        _log_step(7, "Epic EHI: CLARITY_ADT events", "")
        adt_rows = [r for r in _read_tsv_rows(base / "CLARITY_ADT.tsv") if _clean(r.get("PAT_ID")) == patient_id]
        adt_reporter = CliProgressReporter(
            source_id=source_id,
            step_number=7,
            total_steps=total_steps,
            stage="Epic EHI ADT",
            total_items=max(len(adt_rows), 1),
            progress_every=progress_every,
            enabled=verbose,
        )
        for idx, row in enumerate(adt_rows, start=1):
            eid = _clean(row.get("EVENT_ID"))
            if not eid:
                continue
            csn = _clean(row.get("PAT_ENC_CSN_ID"))
            eff = _parse_date_like(_first_nonempty(row.get("EFFECTIVE_TIME"), row.get("EVENT_TIME")))
            etype = _first_nonempty(row.get("EVENT_TYPE_C_NAME"), row.get("EVENT_SUBTYPE_C_NAME"))
            dept_id = _clean(row.get("DEPARTMENT_ID"))
            summary = _first_nonempty(etype, "ADT event")
            adt_items.append(
                {
                    "kind": "medical_record",
                    "source": [EPIC_EHI_SOURCE],
                    "source_id": f"epic:adt:{eid}",
                    "summary": f"{summary} ({eff or 'unknown time'})",
                    "created": _bucket_date(eff or date.today().isoformat()),
                    "people": [person_link],
                    "source_system": "epic",
                    "source_format": "ehi_tsv",
                    "record_type": "encounter",
                    "record_subtype": "adt_event",
                    "status": _clean(row.get("BED_STATUS_C_NAME")),
                    "occurred_at": eff,
                    "recorded_at": eff,
                    "provider_name": _clean(row.get("USER_ID_NAME")),
                    "facility_name": dep_names.get(dept_id, dept_id),
                    "encounter_source_id": csn,
                    "code_system": "epic_adt_event",
                    "code": eid,
                    "code_display": etype,
                    "value_text": _first_nonempty(row.get("COMMENTS"), row.get("REASON_C_NAME"), row.get("BED_ID_BED_LABEL")),
                    "value_numeric": 0.0,
                    "unit": "",
                    "raw_source_ref": f"epic:adt:{eid}",
                    "details_json": {
                        "epic_pathway": "epic_ehi_encounters",
                        "epic_patient_id": patient_id,
                        "event_subtype": _clean(row.get("EVENT_SUBTYPE_C_NAME")),
                        "pat_class": _clean(row.get("PAT_CLASS_C_NAME")),
                        "service": _clean(row.get("PAT_SERVICE_C_NAME")),
                        "source_hashes": source_hashes,
                    },
                }
            )
            adt_reporter.update(idx)
        adt_reporter.complete(len(adt_rows), extra=f"adt={len(adt_items)}")

    result_items: list[dict[str, Any]] = []
    if include_order_results and order_proc_ids:
        _log_step(8, "Epic EHI: ORDER_RESULTS (observations)", "")
        result_rows = [
            r for r in _read_tsv_rows(base / "ORDER_RESULTS.tsv") if _clean(r.get("ORDER_PROC_ID")) in order_proc_ids
        ]
        res_reporter = CliProgressReporter(
            source_id=source_id,
            step_number=8,
            total_steps=total_steps,
            stage="Epic EHI lab results",
            total_items=max(len(result_rows), 1),
            progress_every=progress_every,
            enabled=verbose,
        )
        for idx, row in enumerate(result_rows, start=1):
            proc_id = _clean(row.get("ORDER_PROC_ID"))
            line = _clean(row.get("LINE"))
            comp_id = _clean(row.get("COMPONENT_ID"))
            comp_name = _first_nonempty(row.get("COMPONENT_ID_NAME"), component_names.get(comp_id, ""))
            if not comp_name and not _clean(row.get("ORD_VALUE")):
                res_reporter.update(idx)
                continue
            val = _first_nonempty(row.get("ORD_VALUE"), str(row.get("ORD_NUM_VALUE", "")).strip())
            if not val:
                res_reporter.update(idx)
                continue
            unit = _clean(row.get("REFERENCE_UNIT"))
            occurred = _parse_date_like(_first_nonempty(row.get("RESULT_DATE"), row.get("ORD_DATE_REAL")))
            flag = _clean(row.get("RESULT_FLAG_C_NAME"))
            summary = f"{comp_name or comp_id or 'Lab'}: {val}" + (f" {unit}" if unit else "")
            result_items.append(
                {
                    "kind": "medical_record",
                    "source": [EPIC_EHI_SOURCE],
                    "source_id": f"epic:order_result:{proc_id}:{line or idx}",
                    "summary": summary[:500],
                    "created": _bucket_date(occurred or date.today().isoformat()),
                    "people": [person_link],
                    "source_system": "epic",
                    "source_format": "ehi_tsv",
                    "record_type": "observation",
                    "record_subtype": _clean(row.get("LAB_STATUS_C_NAME")) or "lab_result",
                    "status": _clean(row.get("RESULT_STATUS_C_NAME")),
                    "occurred_at": occurred,
                    "recorded_at": occurred,
                    "provider_name": _clean(row.get("RESULTING_LAB_ID_LLB_NAME")),
                    "facility_name": "",
                    "encounter_source_id": _clean(row.get("PAT_ENC_CSN_ID")),
                    "code_system": "epic_component_id",
                    "code": comp_id,
                    "code_display": comp_name,
                    "value_text": _first_nonempty(row.get("COMPONENT_COMMENT"), row.get("REF_NORMAL_VALS")),
                    "value_numeric": _safe_float(row.get("ORD_NUM_VALUE")),
                    "unit": unit,
                    "raw_source_ref": f"epic:order_result:{proc_id}:{line}",
                    "details_json": {
                        "epic_pathway": "epic_ehi_results",
                        "epic_patient_id": patient_id,
                        "order_proc_id": proc_id,
                        "line": line,
                        "result_flag": flag,
                        "reference_low": _clean(row.get("REFERENCE_LOW")),
                        "reference_high": _clean(row.get("REFERENCE_HIGH")),
                        "source_hashes": source_hashes,
                    },
                }
            )
            res_reporter.update(idx)
        res_reporter.complete(len(result_rows), extra=f"observations={len(result_items)}")

    items: list[dict[str, Any]] = []
    items.extend(medication_items)
    items.extend(order_vacc_items)
    items.extend(imm_admin_vaccinations)
    items.extend(pat_imm_items)
    items.extend(problem_items)
    items.extend(encounter_items)
    items.extend(adt_items)
    items.extend(result_items)

    _log_step(
        total_steps,
        "Epic EHI: assembly complete",
        f"items={len(items)} (meds={len(medication_items)} vacc_orders={len(order_vacc_items)} "
        f"imm_admin={len(imm_admin_vaccinations)} pat_imm={len(pat_imm_items)} problems={len(problem_items)} "
        f"enc={len(encounter_items)} adt={len(adt_items)} obs={len(result_items)})",
    )

    meta = {
        "patient_id": patient_id,
        "counts": {
            "medications": len(medication_items),
            "vaccine_orders": len(order_vacc_items),
            "imm_admin": len(imm_admin_vaccinations),
            "pat_immunizations": len(pat_imm_items),
            "problems": len(problem_items),
            "encounters": len(encounter_items),
            "adt": len(adt_items),
            "observations": len(result_items),
            "total": len(items),
        },
        "source_hashes": source_hashes,
    }
    return items, meta
