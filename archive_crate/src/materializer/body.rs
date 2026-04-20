//! Body text for materialization — prefers tier-2 vault-scan-cache (already provenance-stripped),
//! falls back to disk read + frontmatter split + provenance strip.

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use flate2::read::ZlibDecoder;
use pyo3::prelude::*;
use regex::Regex;
use rusqlite::Connection;
use std::sync::OnceLock;

fn split_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)^---\s*\n(.*?)\n---\s*\n?(.*)$").expect("regex"))
}

fn provenance_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)\n?<!-- provenance\n(.*?)\n-->\s*").expect("regex"))
}

/// Read note body from disk (fallback when cache is unavailable).
pub fn read_note_body(vault_root: &str, rel_path: &str) -> std::io::Result<String> {
    let path = Path::new(vault_root).join(rel_path);
    let content = fs::read_to_string(path)?;
    let body_raw = split_body_after_frontmatter(&content);
    let stripped = provenance_regex().replace_all(&body_raw, "");
    Ok(stripped.trim().to_string())
}

pub fn read_note_body_from_cache(
    cache: &HashMap<String, Vec<u8>>,
    vault_root: &str,
    rel_path: &str,
) -> std::io::Result<String> {
    if let Some(compressed) = cache.get(rel_path) {
        let mut decoder = ZlibDecoder::new(&compressed[..]);
        let mut body = String::new();
        decoder.read_to_string(&mut body)?;
        Ok(body)
    } else {
        read_note_body(vault_root, rel_path)
    }
}

fn load_body_cache_map(cache_path: &str) -> Result<HashMap<String, Vec<u8>>, String> {
    let conn = Connection::open(cache_path).map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare("SELECT rel_path, body_compressed FROM notes WHERE body_compressed IS NOT NULL")
        .map_err(|e| e.to_string())?;
    let rows: Vec<(String, Vec<u8>)> = stmt
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)))
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .collect();
    let mut map = HashMap::with_capacity(rows.len());
    for (rel_path, compressed) in rows {
        map.insert(rel_path, compressed);
    }
    Ok(map)
}

/// Opaque handle holding pre-loaded compressed bodies. Created once, passed to every batch call.
#[pyclass]
pub struct BodyCache {
    pub(crate) inner: Arc<HashMap<String, Vec<u8>>>,
}

#[pymethods]
impl BodyCache {
    #[staticmethod]
    fn load(cache_path: String) -> PyResult<Self> {
        let map = load_body_cache_map(&cache_path)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;
        Ok(BodyCache { inner: Arc::new(map) })
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }
}

fn split_body_after_frontmatter(content: &str) -> String {
    if let Some(caps) = split_regex().captures(content) {
        caps.get(2)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default()
    } else {
        content.to_string()
    }
}
