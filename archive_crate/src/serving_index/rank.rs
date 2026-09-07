use std::collections::HashMap;

use super::metadata::{AccessPolicy, CardMeta};
use super::schema::{QUARANTINE_RETRIEVAL_WEIGHT, RANKING_VERSION, UNKNOWN};

const PIPELINE_VERSION: &str = "2026.09.06.p01b2";
const DEFAULT_RRF_K: f64 = 60.0;
const EXACT_WEIGHT: f64 = 2.0;
const LEXICAL_WEIGHT: f64 = 1.0;
const VECTOR_WEIGHT: f64 = 1.0;
const GRAPH_WEIGHT: f64 = 0.25;

pub fn pipeline_version() -> &'static str {
    PIPELINE_VERSION
}

pub fn ranking_version() -> &'static str {
    RANKING_VERSION
}

fn type_prior(card_type: &str) -> f64 {
    match card_type {
        "person" => 0.14,
        "calendar_event" => 0.12,
        "meeting_transcript" => 0.11,
        "email_thread" => 0.1,
        "git_repository" => 0.1,
        "git_thread" => 0.09,
        "email_message" => 0.08,
        "imessage_thread" => 0.08,
        "git_commit" => 0.08,
        "document" => 0.07,
        "beeper_thread" => 0.07,
        "place" => 0.08,
        "organization" => 0.08,
        _ => 0.02,
    }
}

fn corpus_weight(card: &CardMeta) -> f64 {
    if let Some(weight) = card.retrieval_weight {
        return weight;
    }
    match card.corpus_state.as_str() {
        "quarantine" => QUARANTINE_RETRIEVAL_WEIGHT,
        "active" => 1.0,
        "suppressed" => 0.0,
        _ => 1.0,
    }
}

fn provenance_label(card: &CardMeta) -> &str {
    let raw = card.provenance_summary.trim();
    if raw.is_empty() {
        UNKNOWN
    } else {
        raw
    }
}

fn provenance_score(label: &str) -> f64 {
    match label {
        "deterministic" | "manual" => 0.08,
        "mixed" => 0.04,
        "llm" | "llm_derived" => 0.01,
        _ => 0.0,
    }
}

pub fn exact_flags(card: &CardMeta, query: &str) -> (bool, i32, i32, i32, i32) {
    let trimmed = query.trim();
    let q = trimmed.to_lowercase();
    let uid = i32::from(card.card_uid == trimmed);
    let slug = i32::from(card.slug.to_lowercase() == q);
    let summary = i32::from(card.summary.to_lowercase() == q);
    let person = i32::from(
        card.r#type == "person"
            && (card.aliases.iter().any(|p| p.to_lowercase() == q)
                || card.emails.iter().any(|p| p.to_lowercase() == q)
                || crate::canon::phone_alias_forms(trimmed).iter().any(|form| {
                    card.phones.iter().any(|phone| crate::canon::phone_alias_forms(phone).contains(form))
                })),
    );
    let external = i32::from(card.external_ids.iter().any(|ext| {
        ext == trimmed || ext.eq_ignore_ascii_case(trimmed)
    }));
    let exact = uid + slug + summary + person + external > 0;
    (exact, slug, summary, external, person)
}

pub fn fuse(
    lexical: &HashMap<String, f32>,
    vector: &HashMap<String, (f32, String, i32, usize)>,
    neighbor_trust: &HashMap<String, f64>,
    meta: &HashMap<String, CardMeta>,
    query: &str,
    limit: usize,
    policy: &AccessPolicy,
) -> Vec<serde_json::Value> {
    let mut uids: Vec<String> = lexical.keys().cloned().collect();
    for uid in vector.keys() {
        if !uids.iter().any(|u| u == uid) {
            uids.push(uid.clone());
        }
    }
    let mut dated: Vec<(String, String)> = uids
        .iter()
        .filter_map(|u| meta.get(u).map(|c| (u.clone(), c.activity_at.clone())))
        .filter(|(_, a)| !a.is_empty())
        .collect();
    dated.sort_by(|a, b| b.1.cmp(&a.1));
    let total = dated.len().saturating_sub(1).max(1);
    let recency: HashMap<String, f64> = dated
        .iter()
        .enumerate()
        .map(|(i, (uid, _))| (uid.clone(), ((1.0 - (i as f64 / total as f64)) * 0.06 * 1e6).round() / 1e6))
        .collect();

    let mut lex_order: Vec<(String, f32)> = lexical
        .iter()
        .filter(|(_, s)| **s > 0.0)
        .map(|(u, s)| (u.clone(), *s))
        .collect();
    lex_order.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
    let lexical_ranks: HashMap<String, usize> = lex_order
        .iter()
        .enumerate()
        .map(|(i, (uid, _))| (uid.clone(), i + 1))
        .collect();
    let mut vec_order: Vec<(String, f32)> = vector
        .iter()
        .filter(|(_, v)| v.0 > 0.0)
        .map(|(u, v)| (u.clone(), v.0))
        .collect();
    vec_order.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
    let vector_ranks: HashMap<String, usize> = vec_order
        .iter()
        .enumerate()
        .map(|(i, (uid, _))| (uid.clone(), i + 1))
        .collect();
    let mut graph_order: Vec<(String, f64)> = neighbor_trust
        .iter()
        .filter(|(_, t)| **t > 0.0)
        .map(|(u, t)| (u.clone(), *t))
        .collect();
    graph_order.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
    let graph_ranks: HashMap<String, usize> = graph_order
        .iter()
        .enumerate()
        .map(|(i, (uid, _))| (uid.clone(), i + 1))
        .collect();
    let mut exact_order: Vec<String> = uids
        .iter()
        .filter(|uid| meta.get(*uid).is_some_and(|c| exact_flags(c, query).0))
        .cloned()
        .collect();
    exact_order.sort();
    let exact_ranks: HashMap<String, usize> = exact_order
        .iter()
        .enumerate()
        .map(|(i, uid)| (uid.clone(), i + 1))
        .collect();
    let rrf = |weight: f64, rank: Option<usize>| -> f64 {
        rank.map(|r| weight / (DEFAULT_RRF_K + r as f64)).unwrap_or(0.0)
    };

    let mut rows = Vec::new();
    for uid in uids {
        let Some(card) = meta.get(&uid) else {
            continue;
        };
        if !policy.permits(card) || card.corpus_state == "suppressed" {
            continue;
        }
        let (exact, slug_e, sum_e, ext_e, per_e) = exact_flags(card, query);
        let lex = *lexical.get(&uid).unwrap_or(&0.0);
        let (sim, chunk_type, chunk_index, matched) = vector
            .get(&uid)
            .cloned()
            .unwrap_or((0.0, String::new(), -1, 0));
        let matched_by = if exact {
            "exact"
        } else {
            match (lexical.contains_key(&uid), vector.contains_key(&uid)) {
                (true, true) => "hybrid",
                (true, false) => "lexical",
                _ => "vector",
            }
        };
        let trust = *neighbor_trust.get(&uid).unwrap_or(&0.0);
        let provenance_bias = provenance_label(card);
        let provenance = provenance_score(provenance_bias);
        let rec = *recency.get(&uid).unwrap_or(&0.0);
        let rrf_exact = rrf(EXACT_WEIGHT, exact_ranks.get(&uid).copied());
        let rrf_lex = rrf(LEXICAL_WEIGHT, lexical_ranks.get(&uid).copied());
        let rrf_vec = rrf(VECTOR_WEIGHT, vector_ranks.get(&uid).copied());
        let rrf_graph = rrf(GRAPH_WEIGHT, graph_ranks.get(&uid).copied());
        let rrf_score = rrf_exact + rrf_lex + rrf_vec + rrf_graph;
        let score = ((rrf_score * corpus_weight(card)) * 1e8).round() / 1e8;
        let match_channel = {
            let mut parts = Vec::new();
            if exact {
                parts.push("exact");
            }
            if lexical_ranks.contains_key(&uid) {
                parts.push("lexical");
            }
            if vector_ranks.contains_key(&uid) {
                parts.push("vector");
            }
            if graph_ranks.contains_key(&uid) {
                parts.push("graph");
            }
            if parts.is_empty() {
                matched_by.to_string()
            } else {
                parts.join("+")
            }
        };
        rows.push(serde_json::json!({
            "card_uid": uid,
            "rel_path": card.rel_path,
            "summary": card.summary,
            "type": card.r#type,
            "activity_at": card.activity_at,
            "preview": card.summary.chars().take(160).collect::<String>(),
            "matched_by": matched_by,
            "match_channel": match_channel,
            "lexical_score": lex,
            "vector_similarity": sim,
            "exact_match": exact,
            "slug_exact": slug_e,
            "summary_exact": sum_e,
            "external_id_exact": ext_e,
            "person_exact": per_e,
            "chunk_type": chunk_type,
            "chunk_index": chunk_index,
            "matched_chunk_count": matched,
            "provenance_bias": provenance_bias,
            "provenance_score": provenance,
            "provenance_summary": provenance_bias,
            "graph_hops": if trust > 0.0 { "1" } else if exact { "0" } else { "" },
            "graph_neighbor_trust": trust,
            "corpus_state": if card.corpus_state.is_empty() { UNKNOWN } else { card.corpus_state.as_str() },
            "retrieval_weight": card.retrieval_weight,
            "source_revision": card.source_revision,
            "recency_score": rec,
            "type_prior": type_prior(&card.r#type),
            "rrf_score": ((rrf_score * 1e8).round()) / 1e8,
            "rrf_exact": ((rrf_exact * 1e8).round()) / 1e8,
            "rrf_lexical": ((rrf_lex * 1e8).round()) / 1e8,
            "rrf_vector": ((rrf_vec * 1e8).round()) / 1e8,
            "rrf_graph": ((rrf_graph * 1e8).round()) / 1e8,
            "score": score,
            "pipeline_version": PIPELINE_VERSION,
            "ranking_version": RANKING_VERSION,
            "fusion_strategy": "rrf_k60",
        }));
    }
    rows.sort_by(|a, b| {
        let ea = a.get("exact_match").and_then(|v| v.as_bool()).unwrap_or(false);
        let eb = b.get("exact_match").and_then(|v| v.as_bool()).unwrap_or(false);
        eb.cmp(&ea).then_with(|| {
            let sa = a.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let sb = b.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
            sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
        }).then_with(|| {
            let ua = a.get("card_uid").and_then(|v| v.as_str()).unwrap_or("");
            let ub = b.get("card_uid").and_then(|v| v.as_str()).unwrap_or("");
            ua.cmp(ub)
        })
    });
    rows.truncate(limit);
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card(state: &str, weight: Option<f64>) -> CardMeta {
        CardMeta {
            card_uid: "hfa-person-uid".into(),
            r#type: "person".into(),
            slug: "jane-smith".into(),
            summary: "Jane Smith".into(),
            corpus_state: state.into(),
            retrieval_weight: weight,
            aliases: vec!["jane".into()],
            emails: vec!["jane@example.test".into()],
            external_ids: vec!["ext-99".into()],
            provenance_summary: String::new(),
            ..CardMeta::default()
        }
    }

    #[test]
    fn quarantine_weight_is_local_contract() {
        assert!((corpus_weight(&card("quarantine", None)) - 0.35).abs() < 1e-9);
        assert!((corpus_weight(&card("quarantine", Some(0.35))) - 0.35).abs() < 1e-9);
        assert!((corpus_weight(&card("active", None)) - 1.0).abs() < 1e-9);
        assert_eq!(corpus_weight(&card("unknown", None)), 1.0);
    }

    #[test]
    fn exact_flags_cover_uid_and_external_id() {
        let c = card("active", Some(1.0));
        assert!(exact_flags(&c, "hfa-person-uid").0);
        assert!(exact_flags(&c, "ext-99").0);
        assert!(exact_flags(&c, "jane@example.test").0);
        assert!(!exact_flags(&c, "nope").0);
        let mut email = c.clone();
        email.r#type = "email_message".into();
        assert!(!exact_flags(&email, "jane@example.test").0);
    }

    #[test]
    fn provenance_is_not_inferred_from_exact_match() {
        let c = card("active", Some(1.0));
        assert_eq!(provenance_label(&c), UNKNOWN);
        assert_eq!(provenance_score(provenance_label(&c)), 0.0);
    }
}
