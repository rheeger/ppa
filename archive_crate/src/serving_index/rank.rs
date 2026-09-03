use std::collections::HashMap;

use super::metadata::CardMeta;

const PIPELINE_VERSION: &str = "2026.03.19.hfa1";

pub fn pipeline_version() -> &'static str {
    PIPELINE_VERSION
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

fn corpus_weight(state: &str) -> f64 {
    if state == "quarantine" {
        0.15
    } else {
        1.0
    }
}

pub fn exact_flags(card: &CardMeta, query: &str) -> (bool, i32, i32, i32, i32) {
    let q = query.trim().to_lowercase();
    let slug = i32::from(card.slug.to_lowercase() == q);
    let summary = i32::from(card.summary.to_lowercase() == q);
    let person = i32::from(
        card.people.iter().any(|p| p.to_lowercase() == q)
            || card.aliases.iter().any(|p| p.to_lowercase() == q)
            || card.emails.iter().any(|p| p.to_lowercase() == q),
    );
    let external = 0;
    let exact = slug + summary + person + external > 0;
    (exact, slug, summary, external, person)
}

pub fn fuse(
    lexical: &HashMap<String, f32>,
    vector: &HashMap<String, (f32, String, i32, usize)>,
    neighbor_trust: &HashMap<String, f64>,
    meta: &HashMap<String, CardMeta>,
    query: &str,
    limit: usize,
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

    let mut rows = Vec::new();
    for uid in uids {
        let Some(card) = meta.get(&uid) else {
            continue;
        };
        let (exact, slug_e, sum_e, ext_e, per_e) = exact_flags(card, query);
        let lex = *lexical.get(&uid).unwrap_or(&0.0);
        let (sim, chunk_type, chunk_index, matched) = vector
            .get(&uid)
            .cloned()
            .unwrap_or((0.0, String::new(), -1, 0));
        let matched_by = match (lexical.contains_key(&uid), vector.contains_key(&uid)) {
            (true, true) => "hybrid",
            (true, false) => "lexical",
            _ => "vector",
        };
        let trust = *neighbor_trust.get(&uid).unwrap_or(&0.0);
        let graph_boost = if trust > 0.0 { 0.22 * trust } else { 0.0 };
        let exact_boost = if exact { 3.0 } else { 0.0 };
        let lexical_component = (lex as f64).min(1.5) * if exact { 1.4 } else { 1.2 };
        let vector_component = sim as f64 * 1.2;
        let multi = if matched_by == "hybrid" { 0.2 } else { 0.0 };
        let provenance = if exact { 0.08 } else { 0.04 };
        let rec = *recency.get(&uid).unwrap_or(&0.0);
        let raw = exact_boost
            + lexical_component
            + vector_component
            + multi
            + graph_boost
            + type_prior(&card.r#type)
            + rec
            + provenance;
        let score = ((raw * corpus_weight(&card.corpus_state)) * 1e6).round() / 1e6;
        rows.push(serde_json::json!({
            "card_uid": uid,
            "rel_path": card.rel_path,
            "summary": card.summary,
            "type": card.r#type,
            "activity_at": card.activity_at,
            "preview": card.summary.chars().take(160).collect::<String>(),
            "matched_by": matched_by,
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
            "provenance_bias": if exact { "deterministic" } else { "mixed" },
            "provenance_score": provenance,
            "graph_hops": if trust > 0.0 { "1" } else if exact { "0" } else { "" },
            "graph_neighbor_trust": trust,
            "corpus_state": card.corpus_state,
            "score": score,
            "pipeline_version": PIPELINE_VERSION,
        }));
    }
    rows.sort_by(|a, b| {
        let sa = a.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let sb = b.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
    });
    rows.truncate(limit);
    rows
}
