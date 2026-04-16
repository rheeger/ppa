//! Step 5a — pure-Rust vault cache row build (parity with `archive_cli.vault_cache._populate_db`).

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::OnceLock;

use regex::Regex;
use rayon::prelude::*;
use serde_json::{Map, Value};

use serde_json::Value as JsonValue;

use crate::frontmatter::split_frontmatter_text;
use crate::hasher;
use crate::json_stable::{
    content_hash_from_value, frontmatter_hash_stable_from_value, python_style_json_dumps_sorted,
};
use crate::materializer::card_fields::CardFields;

fn provenance_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?s)\n?<!-- provenance\n(.*?)\n-->\s*").expect("provenance regex")
    })
}

/// Matches `archive_vault.provenance.strip_provenance`.
pub fn strip_provenance(body: &str) -> String {
    provenance_re()
        .replace_all(body, "")
        .trim()
        .to_string()
}

/// Extract provenance entries from the raw body (before stripping) and serialize
/// as sorted JSON: `{"field": {"method": "...", "source": "...", ...}, ...}`.
/// Mirrors `archive_vault.provenance.read_provenance`.
pub fn extract_provenance_json(body_raw: &str) -> String {
    let caps = match provenance_re().captures(body_raw) {
        Some(c) => c,
        None => return "{}".to_string(),
    };
    let block = caps.get(1).unwrap().as_str();
    let mut entries: std::collections::BTreeMap<String, JsonValue> = std::collections::BTreeMap::new();
    for line in block.lines() {
        let line = line.trim();
        if line.is_empty() || !line.contains(':') {
            continue;
        }
        let Some((field, json_part)) = line.split_once(':') else {
            continue;
        };
        let field = field.trim();
        let json_part = json_part.trim();
        let Ok(v) = serde_json::from_str::<JsonValue>(json_part) else {
            continue;
        };
        if !v.is_object() {
            continue;
        }
        entries.insert(field.to_string(), v);
    }
    serde_json::to_string(&entries).unwrap_or_else(|_| "{}".to_string())
}

fn wikilink_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\[\[([^\]|]+)(?:\|[^\]]*)?\]\]").expect("wikilink regex"))
}

/// Matches `archive_vault.vault.extract_wikilinks` (targets only).
pub fn extract_wikilinks(content: &str) -> Vec<String> {
    wikilink_re()
        .captures_iter(content)
        .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
        .collect()
}

fn parse_yaml_mapping(yaml_text: &str) -> Result<Value, String> {
    let t = yaml_text.trim();
    if t.is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    let v: Value = serde_yaml::from_str(yaml_text).map_err(|e| e.to_string())?;
    match v {
        Value::Null => Ok(Value::Object(Map::new())),
        Value::Object(_) => Ok(v),
        _ => Err("Frontmatter must parse to a mapping".to_string()),
    }
}

/// Full note: same split + provenance strip as `archive_vault.vault.parse_note_content`.
pub fn parse_note_content_rust(content: &str) -> Result<(Value, String), String> {
    match split_frontmatter_text(content) {
        Some((yaml_text, body_raw)) => {
            let fm = parse_yaml_mapping(&yaml_text)?;
            let body = strip_provenance(&body_raw);
            Ok((fm, body))
        }
        None => Ok((
            Value::Object(Map::new()),
            strip_provenance(content),
        )),
    }
}

/// Tier 1: `archive_vault.vault._read_frontmatter_prefix` + YAML parse of the fenced block only.
fn read_frontmatter_prefix(path: &Path) -> std::io::Result<String> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut first = String::new();
    reader.read_line(&mut first)?;
    if first.trim() != "---" {
        return Ok(String::new());
    }
    let mut out = String::from("---\n");
    for line in reader.lines() {
        let line = line?;
        out.push_str(&line);
        out.push('\n');
        if line.trim() == "---" {
            break;
        }
    }
    Ok(out)
}

fn frontmatter_from_prefix(prefix: &str) -> Result<Value, String> {
    if prefix.is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    match split_frontmatter_text(prefix) {
        Some((yaml_text, _)) => parse_yaml_mapping(&yaml_text),
        None => Ok(Value::Object(Map::new())),
    }
}

/// `json.dumps(fm, sort_keys=True, default=str)` for `serde_json::Value`.
fn frontmatter_json_string(fm: &Value) -> String {
    python_style_json_dumps_sorted(fm)
}

pub struct Tier1Row {
    pub rel_path: String,
    pub uid: String,
    pub card_type: String,
    pub slug: String,
    pub mtime_ns: i64,
    pub file_size: i64,
    pub fm_json: String,
    pub fm_hash: String,
}

pub struct Tier2Row {
    pub rel_path: String,
    pub uid: String,
    pub card_type: String,
    pub slug: String,
    pub mtime_ns: i64,
    pub file_size: i64,
    pub fm_json: String,
    pub fm_hash: String,
    pub body_compressed: Vec<u8>,
    pub content_hash: String,
    pub wikilinks_json: String,
    pub raw_content_sha256: String,
    pub provenance_json: String,
}

fn slug_from_rel(rel_path: &str) -> String {
    Path::new(rel_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string()
}

fn build_tier1_row(
    vault: &Path,
    rel_path: &str,
    stats: &HashMap<String, (i64, i64)>,
) -> Result<Tier1Row, String> {
    let abs = vault.join(rel_path);
    let prefix = read_frontmatter_prefix(&abs).map_err(|e| format!("{rel_path}: {e}"))?;
    let fm = frontmatter_from_prefix(&prefix)?;
    let card = CardFields::from_frontmatter_value(fm.clone()).map_err(|e| format!("{rel_path}: {e}"))?;
    let fm_json = frontmatter_json_string(&fm);
    let fm_hash = frontmatter_hash_stable_from_value(fm.clone()).map_err(|e| format!("{rel_path}: {e}"))?;
    let (mtime_ns, file_size) = stats.get(rel_path).copied().unwrap_or((0, 0));
    Ok(Tier1Row {
        rel_path: rel_path.to_string(),
        uid: card.uid.trim().to_string(),
        card_type: card.card_type,
        slug: slug_from_rel(rel_path),
        mtime_ns,
        file_size,
        fm_json,
        fm_hash,
    })
}

fn zlib_compress_utf8_body(body: &str, level: u32) -> std::io::Result<Vec<u8>> {
    use flate2::write::ZlibEncoder;
    use flate2::Compression;
    use std::io::Write;
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::new(level));
    enc.write_all(body.as_bytes())?;
    enc.finish()
}

fn build_tier2_row(
    vault: &Path,
    rel_path: &str,
    stats: &HashMap<String, (i64, i64)>,
    zlib_level: u32,
) -> Result<Tier2Row, String> {
    let abs = vault.join(rel_path);
    let content = fs::read_to_string(&abs).map_err(|e| format!("{rel_path}: {e}"))?;
    let (fm, body_raw) = match split_frontmatter_text(&content) {
        Some((yaml_text, raw)) => (parse_yaml_mapping(&yaml_text)?, raw),
        None => (Value::Object(Map::new()), content.clone()),
    };
    let provenance_json = extract_provenance_json(&body_raw);
    let body = strip_provenance(&body_raw);
    let card = CardFields::from_frontmatter_value(fm.clone()).map_err(|e| format!("{rel_path}: {e}"))?;
    let fm_json = frontmatter_json_string(&fm);
    let fm_hash = frontmatter_hash_stable_from_value(fm.clone()).map_err(|e| format!("{rel_path}: {e}"))?;
    let content_hash = content_hash_from_value(fm.clone(), &body).map_err(|e| format!("{rel_path}: {e}"))?;
    let wikis = extract_wikilinks(&body);
    let wikilinks_json = serde_json::to_string(&wikis).map_err(|e| format!("{rel_path}: {e}"))?;
    let body_compressed = zlib_compress_utf8_body(&body, zlib_level).map_err(|e| format!("{rel_path}: {e}"))?;
    let raw_content_sha256 = hasher::raw_content_sha256(content.as_bytes());
    let (mtime_ns, file_size) = stats.get(rel_path).copied().unwrap_or((0, 0));
    Ok(Tier2Row {
        rel_path: rel_path.to_string(),
        uid: card.uid.trim().to_string(),
        card_type: card.card_type,
        slug: slug_from_rel(rel_path),
        mtime_ns,
        file_size,
        fm_json,
        fm_hash,
        body_compressed,
        content_hash,
        wikilinks_json,
        raw_content_sha256,
        provenance_json,
    })
}

pub enum BuiltRows {
    Tier1(Vec<Tier1Row>),
    Tier2(Vec<Tier2Row>),
}

/// Build all note rows in parallel; results sorted by `rel_path` (stable vs Python insert order).
/// Notes that fail YAML parsing are skipped (matches Python `validate_card_permissive` behavior).
/// Returns `(rows, skipped_count)`.
pub fn build_all_rows(
    vault: &Path,
    rel_paths: &[String],
    stats: &HashMap<String, (i64, i64)>,
    tier_ge2: bool,
    zlib_level: u32,
) -> Result<(BuiltRows, usize), String> {
    if tier_ge2 {
        let results: Vec<Result<Tier2Row, String>> = rel_paths
            .par_iter()
            .map(|rel_path| build_tier2_row(vault, rel_path, stats, zlib_level))
            .collect();
        let mut rows = Vec::with_capacity(results.len());
        let mut skipped: usize = 0;
        for r in results {
            match r {
                Ok(row) => rows.push(row),
                Err(e) => {
                    eprintln!("vault-cache rust: skipping note (parse error): {e}");
                    skipped += 1;
                }
            }
        }
        rows.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        Ok((BuiltRows::Tier2(rows), skipped))
    } else {
        let results: Vec<Result<Tier1Row, String>> = rel_paths
            .par_iter()
            .map(|rel_path| build_tier1_row(vault, rel_path, stats))
            .collect();
        let mut rows = Vec::with_capacity(results.len());
        let mut skipped: usize = 0;
        for r in results {
            match r {
                Ok(row) => rows.push(row),
                Err(e) => {
                    eprintln!("vault-cache rust: skipping note (parse error): {e}");
                    skipped += 1;
                }
            }
        }
        rows.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        Ok((BuiltRows::Tier1(rows), skipped))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_provenance_removes_block() {
        let body = "hello\n\n<!-- provenance\nx: {}\n-->\n\nrest";
        // Matches `archive_vault.provenance.strip_provenance` (regex sub + strip).
        assert_eq!(strip_provenance(body), "hello\nrest");
    }
}
