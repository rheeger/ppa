//! Join-key canon twins for hot-path person resolution.

use regex::Regex;
use std::sync::OnceLock;

fn non_digit() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\D").expect("phone digits"))
}

fn non_slug() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"[^a-z0-9\s-]").expect("slug"))
}

fn ws() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\s+").expect("ws"))
}

fn multi_hyphen() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"-{2,}").expect("hyphen"))
}

pub fn normalize_email(raw: &str) -> String {
    ws().replace_all(raw.trim(), " ").to_lowercase()
}

pub fn normalize_phone(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return String::new();
    }
    let digits: String = non_digit().replace_all(raw, "").into_owned();
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

pub fn normalize_slug(raw: &str) -> String {
    let mut slug = raw.trim().to_lowercase();
    slug = non_slug().replace_all(&slug, "").into_owned();
    slug = ws().replace_all(&slug, "-").into_owned();
    slug = multi_hyphen().replace_all(&slug, "-").into_owned();
    let slug = slug.trim_matches('-').to_string();
    if slug.is_empty() {
        "unknown".to_string()
    } else {
        slug
    }
}

pub fn parse_wikilink(raw: &str) -> String {
    raw.trim().trim_start_matches("[[").trim_end_matches("]]").trim().to_string()
}
