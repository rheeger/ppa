//! Body text matching `read_note_file(...).body` (frontmatter fence split + provenance strip).

use std::fs;
use std::path::Path;

use regex::Regex;
use std::sync::OnceLock;

fn split_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)^---\s*\n(.*?)\n---\s*\n?(.*)$").expect("regex"))
}

fn provenance_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)\n?<!-- provenance\n(.*?)\n-->\s*").expect("regex"))
}

/// Read note body the same way as `archive_vault.vault.parse_note_content` / `strip_provenance`.
pub fn read_note_body(vault_root: &str, rel_path: &str) -> std::io::Result<String> {
    let path = Path::new(vault_root).join(rel_path);
    let content = fs::read_to_string(path)?;
    let body_raw = split_body_after_frontmatter(&content);
    let stripped = provenance_regex().replace_all(&body_raw, "");
    Ok(stripped.trim().to_string())
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
