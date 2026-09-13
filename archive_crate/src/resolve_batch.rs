//! Batch person resolution (Step 14) — mirrors `archive_vault.identity_resolver._resolve_person_from_candidates`
//! + `is_same_person`, with GIL released for index build + parallel per-row resolve.

use pyo3::prelude::*;
use pyo3::types::PyList;
use rayon::prelude::*;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::fuzzy_resolver::token_sort_ratio_inner;
use crate::person_index;
use crate::person_index::{PersonRecord, PersonResolutionIndexInner};

const SOCIAL: &[&str] = &[
    "linkedin", "github", "twitter", "instagram", "telegram", "discord",
];

// Close-name and name-conflict rows at or above this score merge without a human.
const REVIEW_AUTO_APPROVE_MIN_CONFIDENCE: i32 = 80;

fn auto_approve_if_confident(mut out: ResolveOutput) -> ResolveOutput {
    if out.action != "conflict" || out.wikilink.is_none() {
        return out;
    }
    if out.confidence < REVIEW_AUTO_APPROVE_MIN_CONFIDENCE {
        return out;
    }
    if !out.reasons.iter().any(|r| r == "auto_approved") {
        out.reasons.push("auto_approved".to_string());
    }
    out.action = "merge".to_string();
    out
}

#[derive(Clone)]
pub(crate) struct ResolveConfig {
    pub merge_threshold: i32,
    pub conflict_threshold: i32,
    pub fuzzy_name_threshold: f64,
}

impl Default for ResolveConfig {
    fn default() -> Self {
        Self {
            merge_threshold: 90,
            conflict_threshold: 75,
            fuzzy_name_threshold: 85.0,
        }
    }
}

fn load_ppa_config(vault: &Path) -> ResolveConfig {
    let path = vault.join("_meta").join("ppa-config.json");
    let Ok(text) = std::fs::read_to_string(&path) else {
        return ResolveConfig::default();
    };
    let Ok(v) = serde_json::from_str::<Value>(&text) else {
        return ResolveConfig::default();
    };
    let Some(o) = v.as_object() else {
        return ResolveConfig::default();
    };
    let mut c = ResolveConfig::default();
    if let Some(n) = o.get("merge_threshold").and_then(|x| x.as_i64()) {
        c.merge_threshold = n as i32;
    }
    if let Some(n) = o.get("conflict_threshold").and_then(|x| x.as_i64()) {
        c.conflict_threshold = n as i32;
    }
    if let Some(n) = o.get("fuzzy_name_threshold").and_then(|x| x.as_f64()) {
        c.fuzzy_name_threshold = n;
    }
    c
}

fn load_identity_map(vault: &Path) -> Result<HashMap<String, String>, String> {
    let path = vault.join("_meta").join("identity-map.json");
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Ok(HashMap::new());
    };
    let Ok(v) = serde_json::from_str::<Value>(&text) else {
        return Ok(HashMap::new());
    };
    let Some(o) = v.as_object() else {
        return Ok(HashMap::new());
    };
    let mut out = HashMap::new();
    for (k, val) in o {
        if k.starts_with('_') {
            continue;
        }
        if let Some(s) = val.as_str() {
            out.insert(k.clone(), s.to_string());
        }
    }
    Ok(out)
}

fn alias_prefix(prefix: &str) -> &str {
    match prefix {
        "emails" => "email",
        "phones" => "phone",
        _ => prefix,
    }
}

/// `archive_vault.identity._normalize_identifier`
fn normalize_identifier(prefix: &str, value: &str) -> String {
    let prefix = alias_prefix(prefix);
    let raw = value.trim();
    if raw.is_empty() {
        return String::new();
    }
    match prefix {
        "email" | "github" | "linkedin" | "twitter" => raw.to_lowercase(),
        "name" => raw
            .to_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
        "phone" => person_index::normalize_phone(raw),
        _ => raw.to_string(),
    }
}

fn identity_lookup(map: &HashMap<String, String>, prefix: &str, value: &str) -> Option<String> {
    let p = alias_prefix(prefix);
    let n = normalize_identifier(p, value);
    if n.is_empty() {
        return None;
    }
    map.get(&format!("{p}:{n}")).cloned()
}

fn email_domains_from_strings(emails: &[String]) -> HashSet<String> {
    let mut s = HashSet::new();
    for e in emails {
        if let Some((_, d)) = e.split_once('@') {
            s.insert(d.to_string());
        }
    }
    s
}

fn email_domains_map(fm: &Map<String, Value>) -> HashSet<String> {
    let mut emails: Vec<String> = person_index::as_string_list(fm, "emails")
        .into_iter()
        .map(|e| person_index::normalize_email(&e))
        .filter(|e| !e.is_empty())
        .collect();
    let sv = person_index::string_value(fm, "email");
    if !sv.is_empty() {
        emails.push(person_index::normalize_email(&sv));
    }
    email_domains_from_strings(&emails)
}

fn email_domains_record(rec: &PersonRecord) -> HashSet<String> {
    email_domains_from_strings(&rec.emails)
}

fn canonicalize_name_tokens(name: &str, nicknames: &HashMap<String, Vec<String>>) -> String {
    let tokens: Vec<String> = person_index::normalize_person_name(name)
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();
    if tokens.is_empty() {
        return String::new();
    }
    let mut alias_to: HashMap<String, String> = HashMap::new();
    for (canonical, aliases) in nicknames {
        alias_to.insert(canonical.clone(), canonical.clone());
        for a in aliases {
            alias_to.insert(a.clone(), canonical.clone());
        }
    }
    let mut out = tokens;
    if let Some(first) = out.first_mut() {
        let rep = alias_to.get(first).cloned().unwrap_or_else(|| first.clone());
        *first = rep;
    }
    out.join(" ")
}

fn name_variants(name: &str, nicknames: &HashMap<String, Vec<String>>) -> Vec<String> {
    let normalized = person_index::normalize_person_name(name);
    let canonicalized = canonicalize_name_tokens(name, nicknames);
    let mut seen = HashSet::new();
    let mut v = Vec::new();
    for s in [normalized, canonicalized] {
        if !s.is_empty() && seen.insert(s.clone()) {
            v.push(s);
        }
    }
    v
}

fn names_match(a: &str, b: &str, nicknames: &HashMap<String, Vec<String>>) -> (bool, f64) {
    let left = person_index::normalize_person_name(a);
    let right = person_index::normalize_person_name(b);
    if left.is_empty() || right.is_empty() {
        return (false, 0.0);
    }
    if left == right {
        return (true, 100.0);
    }
    let lc = canonicalize_name_tokens(&left, nicknames);
    let rc = canonicalize_name_tokens(&right, nicknames);
    if !lc.is_empty() && lc == rc {
        return (true, 95.0);
    }
    let score = token_sort_ratio_inner(&left, &right);
    (score >= 85.0, score)
}

fn best_name_match(
    candidate: &Map<String, Value>,
    existing_fm: &Map<String, Value>,
    nicknames: &HashMap<String, Vec<String>>,
) -> (bool, f64) {
    let mut best_match = false;
    let mut best_score = 0.0_f64;
    for left in person_index::name_candidates(candidate) {
        for right in person_index::name_candidates(existing_fm) {
            let (m, score) = names_match(&left, &right, nicknames);
            if score > best_score {
                best_score = score;
                best_match = m;
            }
        }
    }
    (best_match, best_score)
}

fn best_similarity_lists(a: &[String], b: &[String]) -> f64 {
    let mut best = 0.0_f64;
    for left in a {
        let nl = person_index::normalize_person_name(left);
        if nl.is_empty() {
            continue;
        }
        for right in b {
            let nr = person_index::normalize_person_name(right);
            if nr.is_empty() {
                continue;
            }
            let s = token_sort_ratio_inner(&nl, &nr);
            if s > best {
                best = s;
            }
        }
    }
    best
}

fn has_exact_social_match(candidate: &Map<String, Value>, existing: &PersonRecord) -> (bool, String) {
    for field in SOCIAL {
        let cv = person_index::social_lower(&person_index::string_value(candidate, field));
        let ev = existing.socials.get(*field).map(|s| s.as_str()).unwrap_or("");
        if !cv.is_empty() && !ev.is_empty() && cv == ev {
            return (true, (*field).to_string());
        }
    }
    (false, String::new())
}

fn candidate_is_plausible(
    candidate: &Map<String, Value>,
    existing: &PersonRecord,
    nicknames: &HashMap<String, Vec<String>>,
    config: &ResolveConfig,
) -> bool {
    let existing_fm = person_index::fm_to_map(&existing.raw).clone();
    let (es, _) = has_exact_social_match(candidate, existing);
    if es {
        return true;
    }
    let (cf, cl) = person_index::name_parts(candidate);
    let (ef, el) = person_index::name_parts(&existing_fm);
    if !cl.is_empty() && !el.is_empty() && cl != el {
        return false;
    }
    if !cf.is_empty() && !ef.is_empty() {
        if cf == ef {
            return true;
        }
        let cc = canonicalize_name_tokens(&cf, nicknames);
        let ec = canonicalize_name_tokens(&ef, nicknames);
        if !cc.is_empty() && cc == ec {
            return true;
        }
        let c0 = cf.chars().next();
        let e0 = ef.chars().next();
        if c0.is_some() && c0 == e0 && !cl.is_empty() && !el.is_empty() {
            return true;
        }
    }
    let (name_match, name_score) = best_name_match(candidate, &existing_fm, nicknames);
    name_match && name_score >= config.fuzzy_name_threshold + 5.0
}

fn is_same_person(
    candidate: &Map<String, Value>,
    existing: &PersonRecord,
    nicknames: &HashMap<String, Vec<String>>,
    config: &ResolveConfig,
) -> (bool, i32, Vec<String>) {
    let existing_fm = person_index::fm_to_map(&existing.raw).clone();
    let mut reasons: Vec<String> = Vec::new();
    let mut confidence: i32 = 0;
    let mut support_score: i32 = 0;

    let (exact_social, social_field) = has_exact_social_match(candidate, existing);
    if exact_social {
        confidence += 95;
        reasons.push(format!("exact_{}", social_field));
        support_score += 95;
    }

    let (name_match, name_score) = best_name_match(candidate, &existing_fm, nicknames);
    if name_match {
        if name_score >= 100.0 {
            confidence += 80;
            reasons.push("exact_name".to_string());
        } else if name_score >= 95.0 {
            confidence += 70;
            reasons.push("nickname_name".to_string());
        } else if name_score >= config.fuzzy_name_threshold {
            confidence += if name_score < 90.0 { 50 } else { 60 };
            reasons.push("fuzzy_name".to_string());
        }
    }

    let ce: HashSet<String> = person_index::as_string_list(candidate, "emails")
        .into_iter()
        .map(|e| person_index::normalize_email(&e))
        .filter(|e| !e.is_empty())
        .collect();
    let ee: HashSet<String> = existing.emails.iter().cloned().collect();
    if !ce.is_disjoint(&ee) {
        confidence += 100;
        reasons.push("exact_email".to_string());
        support_score += 100;
    }

    let cp: HashSet<String> = person_index::as_string_list(candidate, "phones")
        .into_iter()
        .map(|p| person_index::normalize_phone(&p))
        .filter(|p| !p.is_empty())
        .collect();
    let ep: HashSet<String> = existing.phones.iter().cloned().collect();
    if !cp.is_disjoint(&ep) {
        confidence += 100;
        reasons.push("exact_phone".to_string());
        support_score += 100;
    }

    let cd = email_domains_map(candidate);
    let ed = email_domains_record(existing);
    if !cd.is_disjoint(&ed) {
        confidence += 15;
        reasons.push("same_email_domain".to_string());
        support_score += 15;
    }

    let company_score = best_similarity_lists(
        &person_index::company_values(candidate),
        &existing.companies,
    );
    if company_score >= 90.0 {
        confidence += 20;
        reasons.push("same_company".to_string());
        support_score += 20;
    } else if company_score >= 75.0 {
        confidence += 10;
        reasons.push("close_company".to_string());
        support_score += 10;
    }

    let title_score = best_similarity_lists(
        &person_index::title_values(candidate),
        &existing.titles,
    );
    if title_score >= 90.0 {
        confidence += 10;
        reasons.push("same_title".to_string());
        support_score += 10;
    } else if title_score >= 75.0 {
        confidence += 5;
        reasons.push("close_title".to_string());
        support_score += 5;
    }

    if !exact_social && !name_match {
        return (false, confidence.min(100), reasons);
    }

    let confidence = confidence.min(100);
    if reasons.iter().any(|r| r == "fuzzy_name") && support_score == 0 {
        return (false, confidence, reasons);
    }
    (
        confidence >= config.conflict_threshold,
        confidence,
        reasons,
    )
}

pub(crate) struct ResolveOutput {
    pub action: String,
    pub wikilink: Option<String>,
    pub confidence: i32,
    pub reasons: Vec<String>,
}

fn is_unnamed_stub_map(data: &Map<String, Value>) -> bool {
    let (first, last) = person_index::name_parts(data);
    if !first.is_empty() || !last.is_empty() {
        return false;
    }
    let summary = person_index::string_value(data, "summary");
    let name = if summary.is_empty() {
        person_index::string_value(data, "name")
    } else {
        summary
    };
    name.is_empty() || name.contains('@') || !name.contains(' ')
}

fn is_unnamed_stub_record(rec: &PersonRecord) -> bool {
    if !rec.first_name.is_empty() || !rec.last_name.is_empty() {
        return false;
    }
    let summary = rec.names.first().cloned().unwrap_or_default();
    summary.is_empty() || summary.contains('@') || !summary.contains(' ')
}

fn names_safe_for_auto_merge(
    identifiers: &Map<String, Value>,
    rec: &PersonRecord,
    nicknames: &HashMap<String, Vec<String>>,
) -> bool {
    if is_unnamed_stub_map(identifiers) || is_unnamed_stub_record(rec) {
        return true;
    }
    let existing_fm = person_index::fm_to_map(&rec.raw);
    let (matched, score) = best_name_match(identifiers, existing_fm, nicknames);
    if matched && score >= 95.0 {
        return true;
    }
    let (cf, cl) = person_index::name_parts(identifiers);
    if !cf.is_empty() && !cl.is_empty() && cf == rec.first_name && cl == rec.last_name {
        return true;
    }
    if !cl.is_empty() && cl == rec.last_name && !cf.is_empty() && !rec.first_name.is_empty() {
        let left = canonicalize_name_tokens(&cf, nicknames);
        let right = canonicalize_name_tokens(&rec.first_name, nicknames);
        if !left.is_empty() && left == right {
            return true;
        }
    }
    false
}

fn identifier_auto_merge_or_review(
    identifiers: &Map<String, Value>,
    index: &PersonResolutionIndexInner,
    wikilink: String,
    reason: &str,
    nicknames: &HashMap<String, Vec<String>>,
) -> ResolveOutput {
    if let Some(rec) = index.records.get(&wikilink) {
        if !names_safe_for_auto_merge(identifiers, rec, nicknames) {
            return auto_approve_if_confident(ResolveOutput {
                action: "conflict".to_string(),
                wikilink: Some(wikilink),
                confidence: 100,
                reasons: vec![reason.to_string(), "name_conflict".to_string()],
            });
        }
    }
    ResolveOutput {
        action: "merge".to_string(),
        wikilink: Some(wikilink),
        confidence: 100,
        reasons: vec![reason.to_string()],
    }
}

fn looks_like_person_name(name: &str) -> bool {
    let cleaned: Vec<&str> = name.split_whitespace().collect();
    if cleaned.len() < 2 || cleaned.len() > 4 {
        return false;
    }
    if name.contains('@') || name.contains('&') || name.contains('/') || name.contains(',') {
        return false;
    }
    if name.chars().any(|c| c.is_ascii_digit()) {
        return false;
    }
    true
}

fn create_or_skip(identifiers: &Map<String, Value>) -> ResolveOutput {
    let emails = person_index::as_string_list(identifiers, "emails");
    let phones = person_index::as_string_list(identifiers, "phones");
    if emails.is_empty() && phones.is_empty() {
        return ResolveOutput {
            action: "skip".to_string(),
            wikilink: None,
            confidence: 0,
            reasons: vec!["no_identifier".to_string()],
        };
    }
    let summary = person_index::string_value(identifiers, "summary");
    let name = if summary.is_empty() {
        person_index::string_value(identifiers, "name")
    } else {
        summary
    };
    let (first, last) = person_index::name_parts(identifiers);
    let composed = [first.as_str(), last.as_str()]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    if looks_like_person_name(&name) || looks_like_person_name(&composed) {
        return ResolveOutput {
            action: "create".to_string(),
            wikilink: None,
            confidence: 0,
            reasons: vec!["no_match".to_string()],
        };
    }
    ResolveOutput {
        action: "skip".to_string(),
        wikilink: None,
        confidence: 0,
        reasons: vec!["implausible_name".to_string()],
    }
}

fn resolve_one(
    identifiers: &Map<String, Value>,
    index: &PersonResolutionIndexInner,
    identity: &HashMap<String, String>,
    nicknames: &HashMap<String, Vec<String>>,
    config: &ResolveConfig,
) -> ResolveOutput {
    for e in person_index::as_string_list(identifiers, "emails") {
        if let Some(w) = identity_lookup(identity, "email", &e) {
            return identifier_auto_merge_or_review(identifiers, index, w, "exact_email", nicknames);
        }
    }
    for p in person_index::as_string_list(identifiers, "phones") {
        if let Some(w) = identity_lookup(identity, "phone", &p) {
            return identifier_auto_merge_or_review(identifiers, index, w, "exact_phone", nicknames);
        }
    }
    for field in SOCIAL {
        for v in person_index::as_string_list(identifiers, field) {
            if let Some(w) = identity_lookup(identity, field, &v) {
                return identifier_auto_merge_or_review(
                    identifiers,
                    index,
                    w,
                    &format!("exact_{field}"),
                    nicknames,
                );
            }
        }
    }

    let wikilinks = index.candidate_wikilinks(identifiers);
    let mut best: Option<ResolveOutput> = None;

    for w in wikilinks {
        let Some(rec) = index.records.get(&w) else {
            continue;
        };
        if !candidate_is_plausible(identifiers, rec, nicknames, config) {
            continue;
        }
        let (is_m, conf, reasons) = is_same_person(identifiers, rec, nicknames, config);
        if !is_m && conf < config.conflict_threshold {
            continue;
        }
        let mut tags = reasons;
        if !tags.iter().any(|r| r == "fuzzy_name" || r == "close_name") {
            tags.push("close_name".to_string());
        }
        let cand = ResolveOutput {
            action: "conflict".to_string(),
            wikilink: Some(w.clone()),
            confidence: conf,
            reasons: tags,
        };
        let replace = match &best {
            None => true,
            Some(b) => cand.confidence > b.confidence,
        };
        if replace {
            best = Some(cand);
        }
    }

    if let Some(b) = best {
        if b.confidence >= config.conflict_threshold {
            return auto_approve_if_confident(b);
        }
    }

    create_or_skip(identifiers)
}

fn parse_identifiers_list(py: Python<'_>, identifiers_list: &Bound<'_, PyAny>) -> PyResult<Vec<Map<String, Value>>> {
    let list = identifiers_list.downcast::<PyList>()?;
    let json = py.import_bound("json")?;
    let dumps = json.getattr("dumps")?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter() {
        let s: String = dumps.call1((&item,))?.extract()?;
        let v: Value = serde_json::from_str(&s).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("identifiers JSON: {e}"))
        })?;
        match v {
            Value::Object(m) => out.push(m),
            _ => out.push(Map::new()),
        }
    }
    Ok(out)
}

/// Batch resolve — builds index once, releases GIL for index + parallel resolve.
#[pyfunction]
pub fn resolve_person_batch(
    py: Python<'_>,
    vault_path: &str,
    identifiers_list: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let vault = PathBuf::from(vault_path);
    let inputs = parse_identifiers_list(py, identifiers_list)?;
    let nicknames = person_index::load_nicknames_json(&vault);
    let config = load_ppa_config(&vault);
    let identity = load_identity_map(&vault).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(e)
    })?;

    let index = py
        .allow_threads(|| person_index::build_index_from_vault_path(&vault))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

    let results: Vec<ResolveOutput> = py.allow_threads(|| {
        inputs
            .par_iter()
            .map(|ids| resolve_one(ids, &index, &identity, &nicknames, &config))
            .collect()
    });

    let ir = py.import_bound("archive_vault.identity_resolver")?;
    let cls = ir.getattr("ResolveResult")?;
    let out_list = PyList::empty_bound(py);
    for r in results {
        let wikilink_obj: PyObject = match &r.wikilink {
            Some(s) => s.into_py(py),
            None => py.None().into_py(py),
        };
        let rr = cls.call1((r.action, wikilink_obj, r.confidence, r.reasons))?;
        out_list.append(rr)?;
    }
    Ok(out_list.to_object(py))
}
