//! `_build_edges` — mirrors `archive_cli.materializer._build_edges`.

use std::collections::{HashMap, HashSet};

use regex::Regex;
use serde_json::{Map, Value};
use std::sync::OnceLock;

use crate::materializer::card_fields::CardFields;
use crate::materializer::external_ids::iter_external_ids_value;
use crate::materializer::fm_value::iter_string_values_json;
use crate::materializer::registry::registry;

const EXTERNAL_ID_PREFIX: &str = "external-id://";

fn wikilink_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\[\[([^\]|]+)(?:\|[^\]]*)?\]\]").expect("regex"))
}

pub fn normalize_slug(value: &str) -> String {
    value.replace(' ', "-").to_lowercase().trim().to_string()
}

fn normalize_exact_text(value: &str) -> String {
    let cleaned: String = value.replace('\0', "");
    let collapsed: String = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.trim().to_lowercase()
}

fn slug_from_wikilink(value: &str) -> String {
    let cleaned = value.trim();
    if cleaned.starts_with("[[") && cleaned.ends_with("]]") {
        let inner = &cleaned[2..cleaned.len() - 2];
        inner.split('|').next().unwrap_or("").trim().to_string()
    } else {
        cleaned.to_string()
    }
}

fn resolve_slug(slug_map: &HashMap<String, String>, slug: &str) -> Option<String> {
    let cleaned = slug.trim();
    if cleaned.is_empty() {
        return None;
    }
    slug_map
        .get(cleaned)
        .cloned()
        .or_else(|| slug_map.get(&normalize_slug(cleaned)).cloned())
}

fn synthetic_external_id_path(provider: &str, external_id: &str) -> String {
    format!("{EXTERNAL_ID_PREFIX}{provider}/{external_id}")
}

fn wikilinks_from_frontmatter_value(frontmatter: &Map<String, Value>) -> Vec<(String, String)> {
    let mut matches: Vec<(String, String)> = Vec::new();
    for (field_name, value) in frontmatter.iter() {
        for text in iter_string_values_json(value) {
            let t = text.trim();
            if t.starts_with("[[") && t.ends_with("]]") {
                let slug = slug_from_wikilink(t);
                if !slug.is_empty() {
                    matches.push((field_name.clone(), slug));
                }
            }
        }
    }
    matches
}

fn scalar_string_for_edge_rule(v: &Value) -> String {
    match v {
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

fn body_wikilinks(body: &str) -> Vec<(String, String)> {
    wikilink_re()
        .captures_iter(body)
        .filter_map(|c| {
            c.get(1)
                .map(|m| ("body".to_string(), m.as_str().trim().to_string()))
        })
        .filter(|(_, s)| !s.is_empty())
        .collect()
}

fn push_edge(
    edges: &mut Vec<EdgeRow>,
    seen: &mut HashSet<(String, String, String, String)>,
    row: EdgeRow,
) {
    let key = (
        row.source_uid.clone(),
        row.target_path.clone(),
        row.edge_type.clone(),
        row.field_name.clone(),
    );
    if seen.insert(key) {
        edges.push(row);
    }
}

fn target_field_key(target_card_type: &str, lookup_field: &str) -> String {
    format!("{}\u{001f}{}", target_card_type, lookup_field)
}

fn append_card_edge_via_field_index(
    edges: &mut Vec<EdgeRow>,
    seen: &mut HashSet<(String, String, String, String)>,
    card_uid: &str,
    rel_path: &str,
    field_name: &str,
    raw_value: &str,
    edge_type: &str,
    target_field_index: &HashMap<String, HashMap<String, String>>,
    target_card_type: &str,
    target_lookup_field: &str,
    path_to_uid: &HashMap<String, String>,
) -> bool {
    let key = target_field_key(target_card_type, target_lookup_field);
    let Some(m) = target_field_index.get(&key) else {
        return false;
    };
    let v = raw_value.trim();
    let target_path = m
        .get(v)
        .or_else(|| m.get(&v.to_lowercase()))
        .cloned();
    let Some(tp) = target_path else {
        return false;
    };
    let target_uid = path_to_uid.get(&tp).cloned().unwrap_or_default();
    push_edge(
        edges,
        seen,
        EdgeRow {
            source_uid: card_uid.to_string(),
            source_path: rel_path.to_string(),
            target_slug: v.to_string(),
            target_path: tp,
            target_uid,
            target_kind: "card".to_string(),
            edge_type: edge_type.to_string(),
            field_name: field_name.to_string(),
        },
    );
    true
}

fn append_card_edge(
    edges: &mut Vec<EdgeRow>,
    seen: &mut HashSet<(String, String, String, String)>,
    card_uid: &str,
    rel_path: &str,
    field_name: &str,
    slug_or_ref: &str,
    edge_type: &str,
    slug_map: &HashMap<String, String>,
    path_to_uid: &HashMap<String, String>,
) {
    let slug = slug_from_wikilink(slug_or_ref);
    let target_path = match resolve_slug(slug_map, &slug) {
        Some(p) => p,
        None => return,
    };
    let target_uid = path_to_uid.get(&target_path).cloned().unwrap_or_default();
    push_edge(
        edges,
        seen,
        EdgeRow {
            source_uid: card_uid.to_string(),
            source_path: rel_path.to_string(),
            target_slug: slug,
            target_path,
            target_uid,
            target_kind: "card".to_string(),
            edge_type: edge_type.to_string(),
            field_name: field_name.to_string(),
        },
    );
}

fn append_person_edge(
    edges: &mut Vec<EdgeRow>,
    seen: &mut HashSet<(String, String, String, String)>,
    card_uid: &str,
    rel_path: &str,
    field_name: &str,
    ref_: &str,
    edge_type: &str,
    path_to_uid: &HashMap<String, String>,
    person_lookup: &HashMap<String, String>,
) {
    let Some(target_path) = resolve_person_reference(person_lookup, ref_) else {
        return;
    };
    let target_uid = path_to_uid.get(&target_path).cloned().unwrap_or_default();
    push_edge(
        edges,
        seen,
        EdgeRow {
            source_uid: card_uid.to_string(),
            source_path: rel_path.to_string(),
            target_slug: slug_from_wikilink(ref_),
            target_path,
            target_uid,
            target_kind: "card".to_string(),
            edge_type: edge_type.to_string(),
            field_name: field_name.to_string(),
        },
    );
}

pub fn resolve_person_reference(
    person_lookup: &HashMap<String, String>,
    value: &str,
) -> Option<String> {
    let slug = slug_from_wikilink(value);
    let normalized_slug = normalize_slug(&slug);
    let normalized_value = normalize_exact_text(value);
    person_lookup
        .get(&normalized_slug)
        .cloned()
        .or_else(|| person_lookup.get(&normalized_value).cloned())
}

#[allow(clippy::too_many_arguments)]
pub fn build_edges(
    rel_path: &str,
    fm: &Map<String, Value>,
    card: &CardFields,
    body: &str,
    slug_map: &HashMap<String, String>,
    path_to_uid: &HashMap<String, String>,
    person_lookup: &HashMap<String, String>,
    target_field_index: &HashMap<String, HashMap<String, String>>,
) -> Vec<EdgeRow> {
    let card_uid = card.uid.clone();
    let card_type = card.card_type.clone();
    let reg = registry().get(&card_type);
    let person_edge_type = reg
        .map(|r| r.person_edge_type.as_str())
        .unwrap_or("mentions_person");

    let mut edges: Vec<EdgeRow> = Vec::new();
    let mut seen: HashSet<(String, String, String, String)> = HashSet::new();

    for (field_name, slug) in wikilinks_from_frontmatter_value(fm) {
        append_card_edge(
            &mut edges,
            &mut seen,
            &card_uid,
            rel_path,
            &field_name,
            &slug,
            "wikilink",
            slug_map,
            path_to_uid,
        );
    }
    for (field_name, slug) in body_wikilinks(body) {
        append_card_edge(
            &mut edges,
            &mut seen,
            &card_uid,
            rel_path,
            &field_name,
            &slug,
            "wikilink",
            slug_map,
            path_to_uid,
        );
    }

    for (field_name, provider, external_id) in iter_external_ids_value(fm) {
        push_edge(
            &mut edges,
            &mut seen,
            EdgeRow {
                source_uid: card_uid.clone(),
                source_path: rel_path.to_string(),
                target_slug: external_id.clone(),
                target_path: synthetic_external_id_path(&provider, &external_id),
                target_uid: String::new(),
                target_kind: "external_id".to_string(),
                edge_type: "entity_has_external_id".to_string(),
                field_name,
            },
        );
    }

    for ps in &card.people {
        let ps = ps.trim();
        if ps.is_empty() {
            continue;
        }
        append_person_edge(
            &mut edges,
            &mut seen,
            &card_uid,
            rel_path,
            "people",
            ps,
            person_edge_type,
            path_to_uid,
            person_lookup,
        );
    }

    if let Some(reg) = reg {
        for rule in &reg.edge_rules {
            let values: Vec<String> = if rule.multi {
                let mut acc: Vec<String> = Vec::new();
                for sf in &rule.source_fields {
                    let Some(raw) = fm.get(sf.as_str()) else {
                        continue;
                    };
                    if raw.is_null() {
                        continue;
                    }
                    if matches!(raw, Value::Array(_) | Value::Object(_)) {
                        acc.extend(iter_string_values_json(raw));
                    } else {
                        acc.push(scalar_string_for_edge_rule(raw));
                    }
                }
                acc
            } else {
                let sf = rule.source_fields.get(0).map(|s| s.as_str()).unwrap_or("");
                let val_str = fm
                    .get(sf)
                    .map(|v| scalar_string_for_edge_rule(v))
                    .unwrap_or_default();
                if val_str.is_empty() {
                    vec![]
                } else {
                    vec![val_str]
                }
            };

            for v in values {
                if v.trim().is_empty() {
                    continue;
                }
                if rule.target == "card" {
                    let mut resolved = false;
                    if let (Some(tlf), Some(tct)) = (
                        rule.target_lookup_field.as_deref(),
                        rule.target_card_type.as_deref(),
                    ) {
                        resolved = append_card_edge_via_field_index(
                            &mut edges,
                            &mut seen,
                            &card_uid,
                            rel_path,
                            &rule.field_name,
                            &v,
                            &rule.edge_type,
                            target_field_index,
                            tct,
                            tlf,
                            path_to_uid,
                        );
                    }
                    if !resolved {
                        append_card_edge(
                            &mut edges,
                            &mut seen,
                            &card_uid,
                            rel_path,
                            &rule.field_name,
                            &v,
                            &rule.edge_type,
                            slug_map,
                            path_to_uid,
                        );
                    }
                } else {
                    append_person_edge(
                        &mut edges,
                        &mut seen,
                        &card_uid,
                        rel_path,
                        &rule.field_name,
                        &v,
                        &rule.edge_type,
                        path_to_uid,
                        person_lookup,
                    );
                }
            }
        }
    }

    edges
}

#[derive(Debug, Clone)]
pub struct EdgeRow {
    pub source_uid: String,
    pub source_path: String,
    pub target_slug: String,
    pub target_path: String,
    pub target_uid: String,
    pub target_kind: String,
    pub edge_type: String,
    pub field_name: String,
}
