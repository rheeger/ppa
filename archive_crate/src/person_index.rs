//! Person resolution index (Step 13) — Tier 2.5a.
//!
//! Mirrors `archive_vault.identity_resolver.PersonIndex` secondary indexes plus fast lookups
//! (`by_email`, `by_phone`, `by_social`, `by_name_exact`) for Step 14.

use pyo3::prelude::*;
use regex::Regex;
use rusqlite::Connection;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::walk;

// --- normalization (archive_vault.identity + identity_resolver) ------------------------

pub(crate) fn normalize_person_name(name: &str) -> String {
    let lower = name.trim().to_lowercase();
    static RE_NON_WORD: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE_NON_WORD.get_or_init(|| Regex::new(r"[^\w\s]").expect("regex"));
    let s = re.replace_all(&lower, " ");
    let s = Regex::new(r"\s+")
        .expect("regex")
        .replace_all(s.as_ref(), " ");
    s.trim().to_string()
}

pub(crate) fn normalize_email(raw: &str) -> String {
    raw.trim().to_lowercase()
}

/// Matches `archive_vault.identity._normalize_identifier(prefix="phone", ...)`.
pub(crate) fn normalize_phone(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return String::new();
    }
    let digits: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return String::new();
    }
    if raw.starts_with('+') {
        return format!("+{digits}");
    }
    if digits.len() == 11 && digits.starts_with('1') {
        return format!("+{digits}");
    }
    if digits.len() == 10 {
        return format!("+1{digits}");
    }
    digits
}

pub(crate) fn social_lower(raw: &str) -> String {
    raw.trim().to_lowercase()
}

/// Nickname map: canonical (normalized) -> list of normalized aliases (archive_vault.identity_resolver.load_nicknames).
pub(crate) fn load_nicknames_json(vault: &Path) -> HashMap<String, Vec<String>> {
    let path = vault.join("_meta").join("nicknames.json");
    let Ok(text) = std::fs::read_to_string(&path) else {
        return HashMap::new();
    };
    let Ok(payload) = serde_json::from_str::<JsonValue>(&text) else {
        return HashMap::new();
    };
    let Some(obj) = payload.as_object() else {
        return HashMap::new();
    };
    let mut out = HashMap::new();
    for (canonical, aliases) in obj {
        let Some(arr) = aliases.as_array() else {
            continue;
        };
        let c = normalize_person_name(canonical);
        if c.is_empty() {
            continue;
        }
        let list: Vec<String> = arr
            .iter()
            .filter_map(|a| a.as_str().map(|s| s.trim()))
            .filter(|s| !s.is_empty())
            .map(|s| normalize_person_name(s))
            .filter(|s| !s.is_empty())
            .collect();
        out.insert(c, list);
    }
    out
}

pub(crate) fn string_value(fm: &Map<String, JsonValue>, key: &str) -> String {
    fm.get(key)
        .and_then(|v| match v {
            JsonValue::String(s) => Some(s.trim().to_string()),
            JsonValue::Number(n) => Some(n.to_string()),
            JsonValue::Bool(b) => Some(b.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

pub(crate) fn as_string_list(fm: &Map<String, JsonValue>, key: &str) -> Vec<String> {
    match fm.get(key) {
        Some(JsonValue::Array(arr)) => arr
            .iter()
            .filter_map(|v| {
                v.as_str()
                    .map(|s| s.trim().to_string())
                    .or_else(|| v.as_f64().map(|n| n.to_string()))
            })
            .filter(|s| !s.is_empty())
            .collect(),
        Some(JsonValue::String(s)) if !s.trim().is_empty() => vec![s.trim().to_string()],
        _ => vec![],
    }
}

/// Same as `_name_parts` in identity_resolver.py.
pub(crate) fn name_parts(fm: &Map<String, JsonValue>) -> (String, String) {
    let mut first = normalize_person_name(&string_value(fm, "first_name"));
    let mut last = normalize_person_name(&string_value(fm, "last_name"));
    if !first.is_empty() && !last.is_empty() {
        return (first, last);
    }
    let summary_raw = fm
        .get("summary")
        .or_else(|| fm.get("name"))
        .map(|v| str_or(Some(v), ""))
        .unwrap_or_default();
    let summary = normalize_person_name(&summary_raw);
    let tokens: Vec<&str> = summary.split_whitespace().collect();
    if first.is_empty() && !tokens.is_empty() {
        first = tokens[0].to_string();
    }
    if last.is_empty() && tokens.len() > 1 {
        last = tokens[tokens.len() - 1].to_string();
    }
    (first, last)
}

fn str_or(v: Option<&JsonValue>, default: &str) -> String {
    let Some(v) = v else {
        return default.to_string();
    };
    match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        _ => default.to_string(),
    }
}

pub(crate) fn name_candidates(fm: &Map<String, JsonValue>) -> Vec<String> {
    let mut values: Vec<String> = vec![
        string_value(fm, "summary"),
        string_value(fm, "name"),
    ];
    values.extend(as_string_list(fm, "aliases"));
    let (first, last) = name_parts(fm);
    if first.is_empty() && last.is_empty() {
        // nothing
    } else {
        let mut fl = Vec::new();
        if !first.is_empty() {
            fl.push(first.as_str());
        }
        if !last.is_empty() {
            fl.push(last.as_str());
        }
        if !fl.is_empty() {
            values.push(fl.join(" "));
        }
    }
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for v in values {
        let t = v.trim();
        if t.is_empty() {
            continue;
        }
        if seen.insert(t.to_string()) {
            out.push(t.to_string());
        }
    }
    out
}

pub(crate) fn company_values(fm: &Map<String, JsonValue>) -> Vec<String> {
    let mut values = vec![string_value(fm, "company")];
    values.extend(as_string_list(fm, "companies"));
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for v in values {
        let t = v.trim();
        if t.is_empty() {
            continue;
        }
        if seen.insert(t.to_string()) {
            out.push(t.to_string());
        }
    }
    out
}

pub(crate) fn title_values(fm: &Map<String, JsonValue>) -> Vec<String> {
    let mut values = vec![string_value(fm, "title")];
    values.extend(as_string_list(fm, "titles"));
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for v in values {
        let t = v.trim();
        if t.is_empty() {
            continue;
        }
        if seen.insert(t.to_string()) {
            out.push(t.to_string());
        }
    }
    out
}

const SOCIAL_FIELDS: &[&str] = &[
    "linkedin",
    "github",
    "twitter",
    "instagram",
    "telegram",
    "discord",
];

// --- core record + index ----------------------------------------------------

#[derive(Clone)]
pub struct PersonRecord {
    pub rel_path: String,
    pub wikilink: String,
    pub names: Vec<String>,
    pub first_name: String,
    pub last_name: String,
    pub emails: Vec<String>,
    pub phones: Vec<String>,
    pub socials: HashMap<String, String>,
    #[allow(dead_code)]
    pub companies: Vec<String>,
    #[allow(dead_code)]
    pub titles: Vec<String>,
    #[allow(dead_code)]
    pub raw: JsonValue,
}

fn wikilink_from_rel_path(rel_path: &str) -> String {
    let p = Path::new(rel_path);
    let stem = p
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    format!("[[{stem}]]")
}

pub(crate) fn fm_to_map(v: &JsonValue) -> &Map<String, JsonValue> {
    static EMPTY: std::sync::OnceLock<Map<String, JsonValue>> = std::sync::OnceLock::new();
    match v {
        JsonValue::Object(m) => m,
        _ => EMPTY.get_or_init(Map::new),
    }
}

fn record_from_frontmatter(rel_path: String, fm: JsonValue) -> PersonRecord {
    let m = fm_to_map(&fm).clone();
    let wikilink = wikilink_from_rel_path(&rel_path);
    let (first_name, last_name) = name_parts(&m);
    let emails: Vec<String> = as_string_list(&m, "emails")
        .into_iter()
        .map(|e| normalize_email(&e))
        .filter(|e| !e.is_empty())
        .collect();
    let phones: Vec<String> = as_string_list(&m, "phones")
        .into_iter()
        .map(|p| normalize_phone(&p))
        .filter(|p| !p.is_empty())
        .collect();
    let mut socials = HashMap::new();
    for field in SOCIAL_FIELDS {
        let v = social_lower(&string_value(&m, field));
        if !v.is_empty() {
            socials.insert((*field).to_string(), v);
        }
    }
    let names: Vec<String> = name_candidates(&m)
        .into_iter()
        .map(|n| normalize_person_name(&n))
        .filter(|n| !n.is_empty())
        .collect();
    let companies: Vec<String> = company_values(&m)
        .into_iter()
        .map(|c| normalize_person_name(&c))
        .filter(|c| !c.is_empty())
        .collect();
    let titles: Vec<String> = title_values(&m)
        .into_iter()
        .map(|t| normalize_person_name(&t))
        .filter(|t| !t.is_empty())
        .collect();
    PersonRecord {
        rel_path,
        wikilink,
        names,
        first_name,
        last_name,
        emails,
        phones,
        socials,
        companies,
        titles,
        raw: fm,
    }
}

fn push_wiki(map: &mut HashMap<String, Vec<String>>, key: String, wikilink: &str) {
    map.entry(key).or_default().push(wikilink.to_string());
}

fn dedupe_sort_lists(map: &mut HashMap<String, Vec<String>>) {
    for paths in map.values_mut() {
        paths.sort();
        paths.dedup();
    }
}

pub(crate) struct PersonResolutionIndexInner {
    by_email: HashMap<String, String>,
    by_phone: HashMap<String, String>,
    by_social: HashMap<(String, String), String>,
    by_name_exact: HashMap<String, Vec<String>>,
    by_last_name: HashMap<String, Vec<String>>,
    by_first_initial_last: HashMap<(String, char), Vec<String>>,
    pub(crate) records: HashMap<String, PersonRecord>,
    #[allow(dead_code)]
    nicknames: HashMap<String, Vec<String>>,
}

impl PersonResolutionIndexInner {
    fn empty(nicknames: HashMap<String, Vec<String>>) -> Self {
        Self {
            by_email: HashMap::new(),
            by_phone: HashMap::new(),
            by_social: HashMap::new(),
            by_name_exact: HashMap::new(),
            by_last_name: HashMap::new(),
            by_first_initial_last: HashMap::new(),
            records: HashMap::new(),
            nicknames,
        }
    }

    fn insert_record_indexes(&mut self, rec: &PersonRecord) {
        let w = &rec.wikilink;
        for e in &rec.emails {
            self.by_email.insert(e.clone(), w.clone());
        }
        for p in &rec.phones {
            self.by_phone.insert(p.clone(), w.clone());
        }
        for (platform, handle) in &rec.socials {
            self
                .by_social
                .insert((platform.clone(), handle.clone()), w.clone());
        }
        for n in &rec.names {
            if !n.is_empty() {
                push_wiki(&mut self.by_name_exact, n.clone(), w);
            }
        }
        let last = rec.last_name.clone();
        let first = rec.first_name.clone();
        if !last.is_empty() {
            push_wiki(&mut self.by_last_name, last.clone(), w);
        }
        if !first.is_empty() && !last.is_empty() {
            let c = first.chars().next().unwrap_or('_');
            self
                .by_first_initial_last
                .entry((last, c))
                .or_default()
                .push(w.clone());
        }
    }

    fn finalize_lists(&mut self) {
        dedupe_sort_lists(&mut self.by_name_exact);
        dedupe_sort_lists(&mut self.by_last_name);
        for paths in self.by_first_initial_last.values_mut() {
            paths.sort();
            paths.dedup();
        }
    }

    /// Same candidate set as `archive_vault.identity_resolver.PersonIndex.candidates`.
    pub(crate) fn candidate_wikilinks(&self, identifiers: &Map<String, JsonValue>) -> Vec<String> {
        let (first, last) = name_parts(identifiers);
        let mut candidate_links: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        if !last.is_empty() {
            if let Some(paths) = self.by_last_name.get(&last) {
                candidate_links.extend(paths.iter().cloned());
            }
            if !first.is_empty() {
                let c = first.chars().next().unwrap_or('_');
                if let Some(paths) = self.by_first_initial_last.get(&(last.clone(), c)) {
                    candidate_links.extend(paths.iter().cloned());
                }
            }
        }
        if candidate_links.is_empty() {
            Vec::new()
        } else {
            candidate_links.into_iter().collect()
        }
    }
}

pub(crate) fn build_index_from_records_fixed(
    records: Vec<PersonRecord>,
    nicknames: HashMap<String, Vec<String>>,
) -> PersonResolutionIndexInner {
    let mut records_sorted = records;
    records_sorted.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    let mut inner = PersonResolutionIndexInner::empty(nicknames);
    for rec in records_sorted {
        inner.insert_record_indexes(&rec);
        inner.records.insert(rec.wikilink.clone(), rec);
    }
    inner.finalize_lists();
    inner
}

#[pyclass(name = "PersonResolutionIndex", module = "archive_crate")]
pub struct PersonResolutionIndex {
    inner: PersonResolutionIndexInner,
}

#[pymethods]
impl PersonResolutionIndex {
    #[getter]
    fn record_count(&self) -> usize {
        self.inner.records.len()
    }

    /// Normalized email → canonical person wikilink `[[stem]]`.
    fn wikilink_for_email(&self, email: &str) -> Option<String> {
        let n = normalize_email(email);
        if n.is_empty() {
            return None;
        }
        self.inner.by_email.get(&n).cloned()
    }

    fn wikilink_for_phone(&self, phone: &str) -> Option<String> {
        let n = normalize_phone(phone);
        if n.is_empty() {
            return None;
        }
        self.inner.by_phone.get(&n).cloned()
    }

    fn wikilink_for_social(&self, platform: &str, handle: &str) -> Option<String> {
        let h = social_lower(handle);
        if h.is_empty() {
            return None;
        }
        self
            .inner
            .by_social
            .get(&(platform.to_string(), h))
            .cloned()
    }

    fn wikilinks_for_last_name(&self, last_name: &str) -> Vec<String> {
        let k = normalize_person_name(last_name);
        self.inner.by_last_name.get(&k).cloned().unwrap_or_default()
    }

    fn wikilinks_for_first_initial_last(&self, last_name: &str, first_initial: char) -> Vec<String> {
        let k = normalize_person_name(last_name);
        self
            .inner
            .by_first_initial_last
            .get(&(k, first_initial))
            .cloned()
            .unwrap_or_default()
    }

    fn has_wikilink(&self, wikilink: &str) -> bool {
        self.inner.records.contains_key(wikilink)
    }

    fn __len__(&self) -> usize {
        self.inner.records.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "PersonResolutionIndex(records={})",
            self.inner.records.len()
        )
    }
}

#[pyfunction]
#[pyo3(signature = (vault_path, cache_path=None))]
pub fn build_person_index(
    py: Python<'_>,
    vault_path: &str,
    cache_path: Option<&str>,
) -> PyResult<Py<PersonResolutionIndex>> {
    let vault = PathBuf::from(vault_path);
    let inner = if let Some(cp) = cache_path {
        if cp.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "cache_path must be non-empty when provided",
            ));
        }
        py.allow_threads(|| build_from_cache_sqlite(cp, &vault))?
    } else {
        py.allow_threads(|| build_index_from_vault_path(&vault))
            .map_err(|e: String| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?
    };
    let idx = PersonResolutionIndex { inner };
    Py::new(py, idx)
}

/// Build person index from vault `People/` notes (pure Rust — call inside `Python::allow_threads`).
pub(crate) fn build_index_from_vault_path(
    vault: &Path,
) -> Result<PersonResolutionIndexInner, String> {
    let mut paths = walk::collect_note_paths(vault.to_str().unwrap_or(""))
        .map_err(|e| e.to_string())?;
    paths.retain(|p| p.starts_with("People/"));
    paths.sort();
    let nicknames = load_nicknames_json(vault);
    let mut records = Vec::new();
    for rel in paths {
        let full = vault.join(&rel);
        let content = match std::fs::read_to_string(&full) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let fm = match frontmatter_value_from_content(&content) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let rec = record_from_frontmatter(rel, fm);
        records.push(rec);
    }
    Ok(build_index_from_records_fixed(records, nicknames))
}

fn build_from_cache_sqlite(
    cache_path: &str,
    vault: &Path,
) -> PyResult<PersonResolutionIndexInner> {
    let nicknames = load_nicknames_json(vault);
    let conn = Connection::open(cache_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
    let mut stmt = conn
        .prepare(
            "SELECT rel_path, frontmatter_json FROM notes WHERE card_type = 'person' ORDER BY rel_path",
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
    let mut records = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?
    {
        let rel_path: String = row
            .get(0)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
        let fj: String = row
            .get(1)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
        let fm: JsonValue = serde_json::from_str(&fj).unwrap_or(JsonValue::Null);
        let rec = record_from_frontmatter(rel_path, fm);
        records.push(rec);
    }
    Ok(build_index_from_records_fixed(records, nicknames))
}

fn frontmatter_value_from_content(content: &str) -> Result<JsonValue, String> {
    let Some((fm_text, _)) = crate::frontmatter::split_frontmatter_text(content) else {
        return Ok(JsonValue::Object(Map::new()));
    };
    if fm_text.trim().is_empty() {
        return Ok(JsonValue::Object(Map::new()));
    }
    serde_yaml::from_str(&fm_text).map_err(|e| e.to_string())
}

/// Count person rows and total ``emails`` array entries in ``frontmatter_json`` (tier ≥1 cache).
#[pyfunction]
pub fn person_index_counts_from_cache(cache_path: String) -> PyResult<(usize, usize)> {
    let conn = Connection::open(&cache_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM notes WHERE card_type = 'person'",
            [],
            |r| r.get(0),
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
    let mut stmt = conn
        .prepare("SELECT frontmatter_json FROM notes WHERE card_type = 'person'")
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
    let mut email_slots = 0usize;
    while let Some(row) = rows
        .next()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?
    {
        let fj: String = row
            .get(0)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
        if let Ok(v) = serde_json::from_str::<JsonValue>(&fj) {
            if let Some(arr) = v.get("emails").and_then(|e| e.as_array()) {
                email_slots += arr.len();
            }
        }
    }
    Ok((n as usize, email_slots))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_phone_us_10() {
        assert_eq!(normalize_phone("5551234567"), "+15551234567");
        assert_eq!(normalize_phone("+1 555 123 4567"), "+15551234567");
    }

    #[test]
    fn wikilink_stem() {
        assert_eq!(
            wikilink_from_rel_path("People/jane-doe.md"),
            "[[jane-doe]]"
        );
    }
}
