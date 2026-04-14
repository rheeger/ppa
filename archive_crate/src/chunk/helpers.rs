//! Parity with `archive_cli/chunk_builders.py` helpers (pure Rust).

use std::sync::OnceLock;

use regex::Regex;
use sha2::{Digest, Sha256};

use crate::chunk::constants::CHUNK_SCHEMA_VERSION;

/// Same bytes as Python `json.dumps(s, ensure_ascii=True)` for string values (quoted).
fn encode_json_string_python_ensure_ascii(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        let c = ch as u32;
        match c {
            0..=31 => match c {
                8 => out.push_str("\\b"),
                9 => out.push_str("\\t"),
                10 => out.push_str("\\n"),
                12 => out.push_str("\\f"),
                13 => out.push_str("\\r"),
                _ => out.push_str(&format!("\\u{:04x}", c)),
            },
            34 => out.push_str("\\\""),
            92 => out.push_str("\\\\"),
            32..=126 => out.push(ch),
            127..=0xFFFF => out.push_str(&format!("\\u{:04x}", c)),
            _ => {
                let cp = c - 0x10000;
                let high = (cp >> 10) + 0xD800;
                let low = (cp & 0x3FF) + 0xDC00;
                out.push_str(&format!("\\u{:04x}\\u{:04x}", high, low));
            }
        }
    }
    out.push('"');
    out
}

/// Byte-for-byte match for `json.dumps({...}, sort_keys=True)` payload used by `_chunk_hash`.
pub fn chunk_hash_payload_json(
    schema_version: i32,
    chunk_type: &str,
    content: &str,
    source_fields: &[String],
) -> String {
    let mut out = String::new();
    out.push('{');
    out.push_str("\"chunk_schema_version\": ");
    out.push_str(&schema_version.to_string());
    out.push_str(", \"chunk_type\": ");
    out.push_str(&encode_json_string_python_ensure_ascii(chunk_type));
    out.push_str(", \"content\": ");
    out.push_str(&encode_json_string_python_ensure_ascii(content));
    out.push_str(", \"source_fields\": ");
    out.push('[');
    for (i, sf) in source_fields.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&encode_json_string_python_ensure_ascii(sf));
    }
    out.push(']');
    out.push('}');
    out
}

/// Same SHA-256 hex as `archive_cli.chunk_builders._chunk_hash`.
pub fn chunk_hash(
    schema_version: i32,
    chunk_type: &str,
    content: &str,
    source_fields: &[String],
) -> String {
    let payload = chunk_hash_payload_json(schema_version, chunk_type, content, source_fields);
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Convenience for the current schema version (matches `_chunk_hash` in Python).
pub fn chunk_hash_current(chunk_type: &str, content: &str, source_fields: &[String]) -> String {
    chunk_hash(CHUNK_SCHEMA_VERSION, chunk_type, content, source_fields)
}

pub fn clean_text(value: &str) -> String {
    let sanitized = value.replace('\x00', "");
    let collapsed: String = sanitized.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.trim().to_string()
}

pub fn token_count(content: &str) -> i32 {
    if content.trim().is_empty() {
        0
    } else {
        std::cmp::max(content.split_whitespace().count() as i32, 1)
    }
}

/// Python `len(s)` for str — Unicode code points (scalar values in Rust for valid UTF-8).
pub fn py_str_len(s: &str) -> usize {
    s.chars().count()
}

/// `s[..byte_end]` where byte_end is the exclusive end byte index of the first `n` chars.
fn prefix_end_byte(s: &str, n_chars: usize) -> usize {
    if n_chars == 0 {
        return 0;
    }
    let mut count = 0;
    for (i, ch) in s.char_indices() {
        count += 1;
        if count == n_chars {
            return i + ch.len_utf8();
        }
    }
    s.len()
}

/// First `n` chars of `s` (Python `s[:n]` with char index `n`).
pub fn py_prefix(s: &str, n: usize) -> &str {
    let end = prefix_end_byte(s, n);
    &s[..end]
}

/// Parity with `_split_text_chunks`.
pub fn split_text_chunks(text: &str, limit: usize) -> Vec<String> {
    let cleaned: String = text.replace('\x00', "").trim().to_string();
    if cleaned.is_empty() {
        return vec![];
    }
    if py_str_len(&cleaned) <= limit {
        return vec![cleaned];
    }

    let mut paragraphs: Vec<String> = cleaned
        .split("\n\n")
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();
    if paragraphs.is_empty() {
        paragraphs.push(cleaned.clone());
    }

    let mut chunks: Vec<String> = Vec::new();
    let mut current = String::new();

    for paragraph in paragraphs {
        let candidate = if current.is_empty() {
            paragraph.clone()
        } else {
            format!("{current}\n\n{paragraph}")
        };
        if py_str_len(&candidate) <= limit {
            current = candidate;
            continue;
        }
        if !current.is_empty() {
            chunks.push(current.clone());
        }

        let mut remainder = paragraph;
        while py_str_len(&remainder) > limit {
            let window = py_prefix(&remainder, limit);
            let split_byte = match window.rfind(' ') {
                Some(byte_idx) if byte_idx > 0 => byte_idx,
                _ => prefix_end_byte(&remainder, limit),
            };
            let chunk_part = remainder[..split_byte].trim();
            if !chunk_part.is_empty() {
                chunks.push(chunk_part.to_string());
            }
            remainder = remainder[split_byte..].trim().to_string();
        }
        current = remainder;
    }

    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

pub fn split_paragraphs(text: &str) -> Vec<String> {
    let sanitized = text.replace('\x00', "");
    sanitized
        .split("\n\n")
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

pub fn rolling_text_windows(text: &str, limit: usize, window_size: usize) -> Vec<String> {
    let paragraphs = split_paragraphs(text);
    if paragraphs.is_empty() {
        return vec![];
    }
    if paragraphs.len() == 1 {
        return split_text_chunks(&paragraphs[0], limit);
    }
    let mut windows: Vec<String> = Vec::new();
    for start in 0..paragraphs.len() {
        let end = (start + window_size).min(paragraphs.len());
        let piece = paragraphs[start..end].join("\n\n").trim().to_string();
        if piece.is_empty() {
            continue;
        }
        for c in split_text_chunks(&piece, limit) {
            windows.push(c);
        }
    }
    let mut deduped: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::<String>::new();
    for w in windows {
        if seen.contains(&w) {
            continue;
        }
        seen.insert(w.clone());
        deduped.push(w);
    }
    deduped
}

fn markdown_heading_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?m)^(#{1,6}\s+[^\n]+)\s*$").expect("markdown heading regex"))
}

pub fn markdown_heading_sections(text: &str) -> Vec<String> {
    let cleaned: String = text.replace('\x00', "").trim().to_string();
    if cleaned.is_empty() {
        return vec![];
    }
    let re = markdown_heading_re();
    let matches: Vec<regex::Match<'_>> = re.find_iter(&cleaned).collect();
    if matches.len() < 2 {
        return vec![];
    }
    let mut sections = Vec::new();
    for i in 0..matches.len() {
        let start = matches[i].start();
        let end = if i + 1 < matches.len() {
            matches[i + 1].start()
        } else {
            cleaned.len()
        };
        let piece = cleaned[start..end].trim();
        if !piece.is_empty() {
            sections.push(piece.to_string());
        }
    }
    sections
}

pub fn otter_pipe_turns(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for raw in text.replace('\x00', "").lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if !line.contains('|') {
            continue;
        }
        let (left, right) = match line.split_once('|') {
            Some((l, r)) => (l.trim(), r.trim()),
            None => continue,
        };
        if !left.is_empty() && !right.is_empty() {
            out.push(format!("{left}: {right}"));
        }
    }
    out
}

fn colon_speaker_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[^:\n]{1,100}: \S").expect("colon speaker regex"))
}

pub fn colon_speaker_turns(text: &str) -> Vec<String> {
    let lines: Vec<String> = text
        .replace('\x00', "")
        .lines()
        .map(|l| l.to_string())
        .collect();
    if lines.is_empty() {
        return vec![];
    }
    let start_pat = colon_speaker_re();
    let mut blocks: Vec<String> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    for line in lines {
        let stripped = line.trim();
        if stripped.is_empty() {
            if !current.is_empty() {
                current.push(line.clone());
            }
            continue;
        }
        if start_pat.is_match(stripped)
            && !current.is_empty()
            && current.iter().any(|c| c.trim().len() > 0)
        {
            blocks.push(current.join("\n").trim().to_string());
            current = vec![line];
        } else {
            current.push(line);
        }
    }
    if !current.is_empty() && current.iter().any(|c| c.trim().len() > 0) {
        blocks.push(current.join("\n").trim().to_string());
    }
    blocks.into_iter().filter(|b| b.trim().len() > 0).collect()
}

pub fn meeting_transcript_focus_section(sections: &[String]) -> String {
    for sec in sections {
        let head = sec
            .trim()
            .split('\n')
            .next()
            .unwrap_or("")
            .trim()
            .to_lowercase();
        if head.starts_with('#') && head.contains("transcript") {
            return sec.clone();
        }
    }
    sections.last().cloned().unwrap_or_default()
}

pub fn format_labeled_block(title: &str, values: &[String]) -> String {
    let cleaned: Vec<String> = values
        .iter()
        .filter_map(|v| {
            let c = clean_text(v);
            if c.is_empty() {
                None
            } else {
                Some(c)
            }
        })
        .collect();
    if cleaned.is_empty() {
        return String::new();
    }
    format!("{title}: {}", cleaned.join("; "))
}

#[cfg(test)]
#[test]
fn chunk_hash_payload_matches_python_json_dumps_sort_keys() {
    let payload = chunk_hash_payload_json(5, "summary", "Hello Beeper", &[String::from("summary")]);
    assert_eq!(
        payload,
        r#"{"chunk_schema_version": 5, "chunk_type": "summary", "content": "Hello Beeper", "source_fields": ["summary"]}"#
    );
}

#[cfg(test)]
#[test]
fn json_string_escaping_matches_python_samples() {
    let cases = [
        ("café", r#""caf\u00e9""#),
        ("\u{1F600}", r#""\ud83d\ude00""#),
        ("\u{10000}", r#""\ud800\udc00""#),
        ("\t", r#""\t""#),
        ("\n", r#""\n""#),
        ("\"", r#""\"""#),
        ("\\", r#""\\""#),
        ("\u{7F}", r#""\u007f""#),
    ];
    for (s, want_quoted) in cases {
        let got = encode_json_string_python_ensure_ascii(s);
        assert_eq!(got, want_quoted, "input={s:?}");
    }
}
