mod dirty;
mod evidence;
mod generation;
mod graph;
mod lexical;
mod metadata;
mod rank;
mod schema;
mod segments;
mod vector;
mod vector_train;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use serde::Serialize;

use crate::serving_index::graph::GraphStore;
use crate::serving_index::lexical::LexicalIndex;
use crate::serving_index::metadata::{AccessPolicy, CardMeta, MetadataStore};
use crate::serving_index::vector::IvfMmapAnn;
use crate::serving_index::vector_train::TrainConfig;

#[pyclass]
pub struct ServingIndex {
    #[allow(dead_code)]
    generation_id: String,
    #[allow(dead_code)]
    dir: PathBuf,
    meta: MetadataStore,
    graph: GraphStore,
    lexical: Vec<LexicalIndex>,
    vectors: Vec<IvfMmapAnn>,
    chunk_to_card: HashMap<String, (String, String, i32)>,
    chunk_evidence: HashMap<String, serde_json::Value>,
    live_chunk_keys: HashSet<String>,
    chain_depth: usize,
    embedding_spec: Option<serde_json::Value>,
}

static OPEN_LOCK: Mutex<()> = Mutex::new(());

#[pymethods]
impl ServingIndex {
    fn generation_id(&self) -> &str {
        &self.generation_id
    }
}

fn open_generation(index_root: &Path) -> PyResult<ServingIndex> {
    let _g = OPEN_LOCK.lock().unwrap();
    let Some(gid) = generation::read_active(index_root)? else {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err(
            "serving_index_unavailable",
        ));
    };
    let dir = generation::generation_dir(index_root, &gid);
    if !dir.join("manifest.json").exists() {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err(
            "serving_index_unavailable",
        ));
    }
    if dir.join("INCOMPLETE").exists() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "serving_index_incomplete",
        ));
    }
    let format = read_format_version(&dir)?;
    if format != schema::SERVING_INDEX_FORMAT_VERSION {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "serving_index_format_unsupported: found {format}, need {} ({})",
            schema::SERVING_INDEX_FORMAT_VERSION,
            schema::VECTOR_IMPL
        )));
    }
    let resolved = segments::resolve_live(index_root, &gid)?;
    let meta = MetadataStore::from_cards(resolved.cards.values().cloned());
    let graph = GraphStore::from_edges(resolved.edges.values().cloned());
    let mut lexical = Vec::new();
    let mut vectors = Vec::new();
    let mut embedding_spec = resolved.embedding_spec.clone();
    for seg in &resolved.chain {
        if seg.join("tantivy").exists() {
            if let Ok(lex) = LexicalIndex::open(&seg.join("tantivy")) {
                lexical.push(lex);
            }
        }
        if seg.join("embedding_keys.txt").exists() || seg.join("embeddings.bin").exists() {
            let ann = IvfMmapAnn::open(seg)?;
            embedding_spec = segments::check_spec_compat(embedding_spec.as_ref(), ann.embedding_spec())?;
            vectors.push(ann);
        }
    }
    Ok(ServingIndex {
        generation_id: gid,
        dir,
        meta,
        graph,
        lexical,
        vectors,
        chunk_to_card: resolved.chunk_to_card(),
        chunk_evidence: resolved.chunk_evidence(),
        live_chunk_keys: resolved.live_chunk_keys,
        chain_depth: resolved.chain.len().max(1),
        embedding_spec,
    })
}

fn read_format_version(dir: &Path) -> PyResult<u32> {
    let raw = fs::read_to_string(dir.join("manifest.json"))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let value: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    Ok(value
        .get("serving_index_format_version")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32)
}

fn req_str(req: &Bound<'_, PyDict>, key: &str) -> String {
    req.get_item(key)
        .ok()
        .flatten()
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default()
}

fn req_i64(req: &Bound<'_, PyDict>, key: &str, default: i64) -> i64 {
    req.get_item(key)
        .ok()
        .flatten()
        .and_then(|v| v.extract::<i64>().ok())
        .unwrap_or(default)
}

fn req_bool(req: &Bound<'_, PyDict>, key: &str) -> bool {
    req.get_item(key)
        .ok()
        .flatten()
        .and_then(|v| v.extract::<bool>().ok())
        .unwrap_or(false)
}

fn req_str_list(req: &Bound<'_, PyDict>, key: &str) -> Vec<String> {
    if let Some(value) = req.get_item(key).ok().flatten() {
        if let Ok(items) = value.extract::<Vec<String>>() {
            return items
                .into_iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        if let Ok(raw) = value.extract::<String>() {
            return raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
    }
    Vec::new()
}

fn access_policy(req: &Bound<'_, PyDict>) -> AccessPolicy {
    let deny = req_bool(req, "access_deny");
    let sources = req_str_list(req, "access_sources");
    let domains = req_str_list(req, "access_domains");
    let restricted = req_bool(req, "access_restricted") || deny || !sources.is_empty() || !domains.is_empty();
    AccessPolicy {
        deny,
        restricted,
        allowed_sources: sources,
        allowed_domains: domains,
    }
}

fn card_allowed(idx: &ServingIndex, uid: &str, policy: &AccessPolicy) -> bool {
    idx.meta
        .by_uid
        .get(uid)
        .map(|card| policy.permits(card) && !MetadataStore::is_suppressed(card))
        .unwrap_or(false)
}

fn card_to_row(card: &CardMeta, extra: serde_json::Value) -> serde_json::Value {
    let corpus_state = if card.corpus_state.is_empty() {
        schema::UNKNOWN
    } else {
        card.corpus_state.as_str()
    };
    let provenance = if card.provenance_summary.is_empty() {
        schema::UNKNOWN
    } else {
        card.provenance_summary.as_str()
    };
    let mut row = serde_json::json!({
        "card_uid": card.card_uid,
        "uid": card.card_uid,
        "rel_path": card.rel_path,
        "summary": card.summary,
        "type": card.r#type,
        "activity_at": card.activity_at,
        "activity_end_at": card.activity_end_at,
        "corpus_state": corpus_state,
        "retrieval_weight": card.retrieval_weight,
        "slug": card.slug,
        "aliases": card.aliases,
        "emails": card.emails,
        "external_ids": card.external_ids,
        "source_revision": card.source_revision,
        "provenance_summary": provenance,
    });
    if let Some(obj) = row.as_object_mut() {
        if let Some(extra_obj) = extra.as_object() {
            for (k, v) in extra_obj {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    row
}

fn json_to_py(py: Python<'_>, value: serde_json::Value) -> PyResult<PyObject> {
    let json = serde_json::to_string(&value)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let json_mod = py.import_bound("json")?;
    json_mod.call_method1("loads", (json,)).map(|o| o.unbind())
}

#[pyfunction]
pub fn serving_index_open(py: Python<'_>, path: &str) -> PyResult<Py<ServingIndex>> {
    py.allow_threads(|| open_generation(Path::new(path)))
        .and_then(|idx| Py::new(py, idx))
}

#[pyfunction]
pub fn serving_index_search(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    req: Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
    let query = req_str(&req, "query");
    let limit = req_i64(&req, "limit", 20).max(1) as usize;
    let type_filter = req_str(&req, "type_filter");
    let source_filter = req_str(&req, "source_filter");
    let people_filter = req_str(&req, "people_filter");
    let start_date = req_str(&req, "start_date");
    let end_date = req_str(&req, "end_date");
    let policy = access_policy(&req);
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    if let Some(card) = idx.meta.exact_identifier(&query) {
        if idx.meta.eligible(
            card,
            &policy,
            &type_filter,
            &source_filter,
            &people_filter,
            "",
            &start_date,
            &end_date,
        ) {
            let (exact, slug_e, sum_e, ext_e, per_e) = rank::exact_flags(card, &query);
            rows.push(card_to_row(
                card,
                serde_json::json!({
                    "matched_by": "exact",
                    "match_channel": "exact",
                    "exact_match": true,
                    "slug_exact": slug_e,
                    "summary_exact": sum_e,
                    "external_id_exact": ext_e,
                    "person_exact": per_e,
                    "serving_generation": idx.generation_id,
                    "citation": {
                        "card_uid": card.card_uid,
                        "source_revision": card.source_revision,
                        "generation": idx.generation_id,
                        "match_channel": "exact",
                    },
                    "uid_exact": i32::from(exact),
                }),
            ));
            seen.insert(card.card_uid.clone());
        }
    }
    let mut merged: HashMap<String, f32> = HashMap::new();
    for lex in &idx.lexical {
        for (uid, score) in lex.search(&query, limit * 4, &type_filter)? {
            if !idx.meta.by_uid.contains_key(&uid) {
                continue;
            }
            let entry = merged.entry(uid).or_insert(score);
            if score > *entry {
                *entry = score;
            }
        }
    }
    let mut hits: Vec<(String, f32)> = merged.into_iter().collect();
    hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    for (uid, score) in hits {
        if !seen.insert(uid.clone()) {
            continue;
        }
        if let Some(card) = idx.meta.by_uid.get(&uid) {
            if !idx.meta.eligible(
                card,
                &policy,
                &type_filter,
                &source_filter,
                &people_filter,
                "",
                &start_date,
                &end_date,
            ) {
                continue;
            }
            let (exact, slug_e, sum_e, ext_e, per_e) = rank::exact_flags(card, &query);
            let channel = if exact { "exact" } else { "lexical" };
            rows.push(card_to_row(
                card,
                serde_json::json!({
                    "matched_by": channel,
                    "match_channel": channel,
                    "lexical_score": score,
                    "exact_match": exact,
                    "slug_exact": slug_e,
                    "summary_exact": sum_e,
                    "external_id_exact": ext_e,
                    "person_exact": per_e,
                    "serving_generation": idx.generation_id,
                    "citation": {
                        "card_uid": card.card_uid,
                        "source_revision": card.source_revision,
                        "generation": idx.generation_id,
                        "match_channel": channel,
                    },
                }),
            ));
        }
        if rows.len() >= limit {
            break;
        }
    }
    json_to_py(py, serde_json::Value::Array(rows))
}

#[pyfunction]
pub fn serving_index_query(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    req: Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
    let type_filter = req_str(&req, "type_filter");
    let source_filter = req_str(&req, "source_filter");
    let people_filter = req_str(&req, "people_filter");
    let org_filter = req_str(&req, "org_filter");
    let start_date = req_str(&req, "start_date");
    let end_date = req_str(&req, "end_date");
    let limit = req_i64(&req, "limit", 20).max(1) as usize;
    let policy = access_policy(&req);
    let mut cards: Vec<&CardMeta> = idx
        .meta
        .by_uid
        .values()
        .filter(|c| {
            idx.meta.eligible(
                c,
                &policy,
                &type_filter,
                &source_filter,
                &people_filter,
                &org_filter,
                &start_date,
                &end_date,
            )
        })
        .collect();
    cards.sort_by(|a, b| b.activity_at.cmp(&a.activity_at).then_with(|| a.rel_path.cmp(&b.rel_path)));
    cards.truncate(limit);
    let rows: Vec<serde_json::Value> = cards.into_iter().map(|c| card_to_row(c, serde_json::json!({}))).collect();
    json_to_py(py, serde_json::Value::Array(rows))
}

#[pyfunction]
pub fn serving_index_vector(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    query_vector: Vec<f32>,
    req: Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
    let limit = req_i64(&req, "limit", 20).max(1) as usize;
    let type_filter = req_str(&req, "type_filter");
    let source_filter = req_str(&req, "source_filter");
    let people_filter = req_str(&req, "people_filter");
    let start_date = req_str(&req, "start_date");
    let end_date = req_str(&req, "end_date");
    let policy = access_policy(&req);
    if idx.vectors.is_empty() {
        return json_to_py(py, serde_json::Value::Array(vec![]));
    };
    if !query_vector.iter().all(|v| v.is_finite()) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "query embedding contains non-finite values",
        ));
    }
    let dim = idx
        .vectors
        .iter()
        .map(|ann| ann.dim())
        .find(|d| *d != 0)
        .unwrap_or(0);
    if !query_vector.is_empty() && dim != 0 && query_vector.len() != dim {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "query dimension {} != serving dimension {}",
            query_vector.len(),
            dim
        )));
    }
    if let Some(expected) = &idx.embedding_spec {
        if let Some(raw) = req.get_item("embedding_spec").ok().flatten() {
            if let Ok(got) = raw.extract::<String>() {
                let got_v: serde_json::Value = serde_json::from_str(&got)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                segments::check_spec_compat(Some(expected), Some(&got_v))?;
            }
        }
    }
    let default_nprobe = idx.vectors.first().map(|a| a.nprobe() as i64).unwrap_or(32);
    let default_budget = idx
        .vectors
        .first()
        .map(|a| a.candidate_budget() as i64)
        .unwrap_or(4096);
    let nprobe = req_i64(&req, "nprobe", default_nprobe).max(1) as usize;
    let budget = req_i64(&req, "candidate_budget", default_budget).max(1) as usize;
    let has_filter = policy.restricted
        || policy.deny
        || !type_filter.is_empty()
        || !source_filter.is_empty()
        || !people_filter.is_empty()
        || !start_date.is_empty()
        || !end_date.is_empty();
    let per_k = (limit * 8).max(budget).max(limit * idx.chain_depth);
    let mut report = crate::serving_index::vector::KnnReport {
        hits: Vec::new(),
        nlist: 0,
        nprobe,
        lists_probed: 0,
        candidates_scored: 0,
        scanned_all: false,
        skipped_invalid: 0,
        skipped_zero: 0,
        truncated: false,
    };
    let mut best_key: HashMap<String, f32> = HashMap::new();
    for ann in &idx.vectors {
        let eligible = if has_filter {
            let mut set = HashSet::new();
            for (i, key) in ann.keys().iter().enumerate() {
                if !idx.live_chunk_keys.contains(key) {
                    continue;
                }
                if let Some((uid, _, _)) = idx.chunk_to_card.get(key) {
                    if let Some(card) = idx.meta.by_uid.get(uid) {
                        if idx.meta.eligible(
                            card,
                            &policy,
                            &type_filter,
                            &source_filter,
                            &people_filter,
                            "",
                            &start_date,
                            &end_date,
                        ) {
                            set.insert(i);
                        }
                    }
                }
            }
            Some(set)
        } else {
            None
        };
        let part = ann.knn_live(
            &query_vector,
            per_k,
            &idx.live_chunk_keys,
            nprobe,
            budget.max(per_k),
            eligible.as_ref(),
        );
        report.nlist = report.nlist.max(part.nlist);
        report.lists_probed += part.lists_probed;
        report.candidates_scored += part.candidates_scored;
        report.scanned_all |= part.scanned_all;
        report.skipped_invalid += part.skipped_invalid;
        report.skipped_zero += part.skipped_zero;
        report.truncated |= part.truncated;
        for hit in part.hits {
            let e = best_key.entry(hit.key.clone()).or_insert(hit.score);
            if hit.score > *e {
                *e = hit.score;
            }
        }
    }
    let mut union_hits: Vec<(String, f32)> = best_key.into_iter().collect();
    union_hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    report.hits = union_hits
        .into_iter()
        .map(|(key, score)| crate::serving_index::vector::KnnHit { key, score })
        .collect();
    let mut best: HashMap<String, (f32, String, i32, usize, String)> = HashMap::new();
    for hit in &report.hits {
        if let Some((card_uid, ctype, cidx)) = idx.chunk_to_card.get(&hit.key) {
            let e = best
                .entry(card_uid.clone())
                .or_insert((hit.score, ctype.clone(), *cidx, 0, hit.key.clone()));
            e.3 += 1;
            if hit.score > e.0 {
                e.0 = hit.score;
                e.1 = ctype.clone();
                e.2 = *cidx;
                e.4 = hit.key.clone();
            }
        }
    }
    let mut rows = Vec::new();
    let mut items: Vec<_> = best.into_iter().collect();
    items.sort_by(|a, b| b.1.0.partial_cmp(&a.1.0).unwrap_or(std::cmp::Ordering::Equal));
    for (uid, (sim, ctype, cidx, matched, chunk_key)) in items {
        if let Some(card) = idx.meta.by_uid.get(&uid) {
            if !idx.meta.eligible(
                card,
                &policy,
                &type_filter,
                &source_filter,
                &people_filter,
                "",
                &start_date,
                &end_date,
            ) {
                continue;
            }
            let provenance = if card.provenance_summary.is_empty() {
                schema::UNKNOWN
            } else {
                card.provenance_summary.as_str()
            };
            let evidence = idx.chunk_evidence.get(&chunk_key).cloned().unwrap_or(serde_json::Value::Null);
            rows.push(card_to_row(
                card,
                serde_json::json!({
                    "matched_by": "vector",
                    "match_channel": "vector",
                    "score": sim,
                    "similarity": sim,
                    "vector_similarity": sim,
                    "chunk_type": ctype,
                    "chunk_index": cidx,
                    "matched_chunk_count": matched,
                    "chunk_key": chunk_key,
                    "evidence_ref": evidence,
                    "preview": card.summary.chars().take(160).collect::<String>(),
                    "provenance_bias": provenance,
                    "provenance_score": 0.0,
                    "serving_generation": idx.generation_id,
                    "ann_nlist": report.nlist,
                    "ann_nprobe": report.nprobe,
                    "ann_lists_probed": report.lists_probed,
                    "ann_candidates": report.candidates_scored,
                    "ann_scanned_all": report.scanned_all,
                    "truncated": report.truncated,
                    "citation": {
                        "card_uid": card.card_uid,
                        "source_revision": card.source_revision,
                        "generation": idx.generation_id,
                        "match_channel": "vector",
                        "chunk_key": chunk_key,
                    },
                }),
            ));
        }
        if rows.len() >= limit {
            break;
        }
    }
    json_to_py(py, serde_json::Value::Array(rows))
}

#[pyfunction]
pub fn serving_index_hybrid(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    query: &str,
    query_vector: Vec<f32>,
    req: Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
    let limit = req_i64(&req, "limit", 20).max(1) as usize;
    let type_filter = req_str(&req, "type_filter");
    let source_filter = req_str(&req, "source_filter");
    let people_filter = req_str(&req, "people_filter");
    let start_date = req_str(&req, "start_date");
    let end_date = req_str(&req, "end_date");
    let policy = access_policy(&req);
    let cap = (limit * 8).max(limit).max(limit * idx.chain_depth);
    let mut lexical = HashMap::new();
    for lex in &idx.lexical {
        for (uid, score) in lex.search(query, cap, &type_filter)? {
            if !idx.meta.by_uid.contains_key(&uid) {
                continue;
            }
            let e = lexical.entry(uid).or_insert(score);
            if score > *e {
                *e = score;
            }
        }
    }
    let mut vector = HashMap::new();
    for ann in &idx.vectors {
        if policy.restricted || policy.deny {
            let mut eligible = HashSet::new();
            for (i, key) in ann.keys().iter().enumerate() {
                if !idx.live_chunk_keys.contains(key) {
                    continue;
                }
                if let Some((uid, _, _)) = idx.chunk_to_card.get(key) {
                    if let Some(card) = idx.meta.by_uid.get(uid) {
                        if idx.meta.eligible(
                            card,
                            &policy,
                            &type_filter,
                            &source_filter,
                            &people_filter,
                            "",
                            &start_date,
                            &end_date,
                        ) {
                            eligible.insert(i);
                        }
                    }
                }
            }
            let part = ann.knn_live(
                &query_vector,
                cap,
                &idx.live_chunk_keys,
                ann.nprobe(),
                cap,
                Some(&eligible),
            );
            for hit in part.hits {
                if let Some((card_uid, ctype, cidx)) = idx.chunk_to_card.get(&hit.key) {
                    let e = vector
                        .entry(card_uid.clone())
                        .or_insert((hit.score, ctype.clone(), *cidx, 0usize));
                    e.3 += 1;
                    if hit.score > e.0 {
                        *e = (hit.score, ctype.clone(), *cidx, e.3);
                    }
                }
            }
            continue;
        }
        for (chunk_key, sim) in ann.knn(&query_vector, cap) {
            if !idx.live_chunk_keys.contains(&chunk_key) {
                continue;
            }
            if let Some((card_uid, ctype, cidx)) = idx.chunk_to_card.get(&chunk_key) {
                let e = vector
                    .entry(card_uid.clone())
                    .or_insert((sim, ctype.clone(), *cidx, 0usize));
                e.3 += 1;
                if sim > e.0 {
                    *e = (sim, ctype.clone(), *cidx, e.3);
                }
            }
        }
    }
    let anchors: Vec<String> = lexical
        .keys()
        .filter(|uid| {
            idx.meta
                .by_uid
                .get(*uid)
                .map(|c| rank::exact_flags(c, query).0)
                .unwrap_or(false)
        })
        .cloned()
        .collect();
    lexical.retain(|uid, _| {
        idx.meta
            .by_uid
            .get(uid)
            .map(|c| {
                idx.meta.eligible(
                    c,
                    &policy,
                    &type_filter,
                    &source_filter,
                    &people_filter,
                    "",
                    &start_date,
                    &end_date,
                )
            })
            .unwrap_or(false)
    });
    vector.retain(|uid, _| {
        idx.meta
            .by_uid
            .get(uid)
            .map(|c| {
                idx.meta.eligible(
                    c,
                    &policy,
                    &type_filter,
                    &source_filter,
                    &people_filter,
                    "",
                    &start_date,
                    &end_date,
                )
            })
            .unwrap_or(false)
    });
    let trust = idx.graph.neighbor_trust(&anchors, |uid| card_allowed(&idx, uid, &policy));
    let rows = rank::fuse(&lexical, &vector, &trust, &idx.meta.by_uid, query, limit, &policy);
    json_to_py(py, serde_json::Value::Array(rows))
}

#[pyfunction]
#[pyo3(signature = (handle, start, hops, req=None))]
pub fn serving_index_graph(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    start: &str,
    hops: usize,
    req: Option<Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
    let policy = req.as_ref().map(access_policy).unwrap_or_else(AccessPolicy::unrestricted);
    let uid = if idx.meta.by_uid.contains_key(start) {
        start.to_string()
    } else {
        idx.meta
            .by_path
            .get(start)
            .cloned()
            .or_else(|| idx.meta.by_path.get(&format!("{start}.md")).cloned())
            .unwrap_or_else(|| start.to_string())
    };
    let hops = hops.clamp(1, 2);
    let graph = idx.graph.hops_where(&uid, hops, |node| card_allowed(&idx, node, &policy));
    let mut out = serde_json::Map::new();
    for (node, targets) in graph {
        let items: Vec<serde_json::Value> = targets
            .into_iter()
            .filter(|edge| card_allowed(&idx, &edge.neighbor_uid, &policy))
            .map(|edge| {
                let path = idx
                    .meta
                    .by_uid
                    .get(&edge.neighbor_uid)
                    .map(|c| c.rel_path.clone())
                    .unwrap_or_else(|| edge.neighbor_uid.clone());
                let mut item = edge.to_json(path);
                if edge.method == "inferred" {
                    if let Some(obj) = item.as_object_mut() {
                        obj.insert("match_channel".into(), serde_json::json!("seed-link"));
                    }
                } else if let Some(obj) = item.as_object_mut() {
                    obj.insert("match_channel".into(), serde_json::json!("graph"));
                }
                item
            })
            .collect();
        let key = idx
            .meta
            .by_uid
            .get(&node)
            .map(|c| c.rel_path.clone())
            .unwrap_or(node);
        out.insert(key, serde_json::Value::Array(items));
    }
    json_to_py(py, serde_json::Value::Object(out))
}

#[pyfunction]
#[pyo3(signature = (handle, name, req=None))]
pub fn serving_index_person(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    name: &str,
    req: Option<Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
    let policy = req.as_ref().map(access_policy).unwrap_or_else(AccessPolicy::unrestricted);
    let needle = name.trim().to_lowercase().replace(' ', "-");
    let uid = idx
        .meta
        .by_slug
        .get(&name.trim().to_lowercase())
        .or_else(|| idx.meta.by_slug.get(&needle))
        .cloned();
    if let Some(uid) = uid {
        if let Some(card) = idx.meta.by_uid.get(&uid) {
            if policy.permits(card) && !MetadataStore::is_suppressed(card) {
                return json_to_py(
                    py,
                    serde_json::json!({"found": true, "rel_path": card.rel_path, "card_uid": uid}),
                );
            }
        }
    }
    json_to_py(py, serde_json::json!({"found": false, "rel_path": "", "card_uid": ""}))
}

#[pyfunction]
#[pyo3(signature = (handle, uids, req=None))]
pub fn serving_index_pointers(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    uids: Vec<String>,
    req: Option<Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
    let policy = req.as_ref().map(access_policy).unwrap_or_else(AccessPolicy::unrestricted);
    let visible: Vec<String> = uids
        .into_iter()
        .filter(|uid| card_allowed(&idx, uid, &policy))
        .collect();
    let mut map = idx.graph.pointers(&visible);
    for slot in map.values_mut() {
        for list in slot.values_mut() {
            list.retain(|uid| card_allowed(&idx, uid, &policy));
        }
    }
    json_to_py(py, serde_json::to_value(map).unwrap_or(serde_json::json!({})))
}

#[pyfunction]
#[pyo3(signature = (handle, uids, hops, req=None))]
pub fn serving_index_neighbor_uids(
    handle: &Bound<'_, ServingIndex>,
    uids: Vec<String>,
    hops: usize,
    req: Option<Bound<'_, PyDict>>,
) -> PyResult<Vec<String>> {
    let idx = handle.borrow();
    let policy = req.as_ref().map(access_policy).unwrap_or_else(AccessPolicy::unrestricted);
    let hops = hops.clamp(1, 2);
    let mut out = HashSet::new();
    for uid in uids {
        let uid = uid.trim();
        if uid.is_empty() || !card_allowed(&idx, uid, &policy) {
            continue;
        }
        out.insert(uid.to_string());
        for (node, targets) in idx.graph.hops_where(uid, hops, |node| card_allowed(&idx, node, &policy)) {
            out.insert(node);
            for edge in targets {
                if card_allowed(&idx, &edge.neighbor_uid, &policy) {
                    out.insert(edge.neighbor_uid);
                }
            }
        }
    }
    Ok(out.into_iter().collect())
}

#[pyfunction]
pub fn serving_index_timeline(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    req: Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
    let start_date = req_str(&req, "start_date");
    let end_date = req_str(&req, "end_date");
    let type_filter = req_str(&req, "type_filter");
    let source_filter = req_str(&req, "source_filter");
    let people_filter = req_str(&req, "people_filter");
    let limit = req_i64(&req, "limit", 20).max(1) as usize;
    let policy = access_policy(&req);
    let cards = idx.meta.timeline_range(
        &start_date,
        &end_date,
        limit,
        &type_filter,
        &source_filter,
        &people_filter,
        &policy,
    );
    let rows: Vec<serde_json::Value> = cards
        .into_iter()
        .map(|c| {
            let mut row = card_to_row(c, serde_json::json!({}));
            if let Some(obj) = row.as_object_mut() {
                obj.insert("created".into(), serde_json::Value::String(c.activity_at.clone()));
            }
            row
        })
        .collect();
    json_to_py(py, serde_json::Value::Array(rows))
}

#[pyfunction]
pub fn serving_index_temporal_neighbors(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    timestamp: &str,
    req: Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
    let limit = req_i64(&req, "limit", 20).max(1) as usize;
    let direction = req_str(&req, "direction");
    let direction = if direction.is_empty() { "both" } else { direction.as_str() };
    let type_filter = req_str(&req, "type_filter");
    let source_filter = req_str(&req, "source_filter");
    let people_filter = req_str(&req, "people_filter");
    let policy = access_policy(&req);
    let Some(hits) = idx.meta.temporal_neighbors(
        timestamp,
        direction,
        limit,
        &type_filter,
        &source_filter,
        &people_filter,
        &policy,
    ) else {
        return json_to_py(
            py,
            serde_json::json!({
                "ok": false,
                "error": "invalid_timestamp",
                "timestamp": timestamp,
                "results": [],
            }),
        );
    };
    let results: Vec<serde_json::Value> = hits
        .into_iter()
        .map(|hit| card_to_row(hit.card, serde_json::json!({"leg": hit.leg})))
        .collect();
    json_to_py(
        py,
        serde_json::json!({
            "ok": true,
            "timestamp": timestamp,
            "count": results.len(),
            "results": results,
        }),
    )
}

#[derive(Serialize)]
struct Manifest {
    serving_index_format_version: u32,
    analyzer_id: String,
    vector_impl: String,
    pipeline_version: String,
    ranking_version: String,
    card_count: usize,
    chunk_count: usize,
    embedding_count: usize,
    nlist: usize,
    nprobe: usize,
    ivf_checksum: String,
    embedding_spec: Option<serde_json::Value>,
}

#[pyfunction]
#[pyo3(signature = (dest_generation, cards_jsonl, chunks_jsonl, embedding_keys_path, embeddings_bin_path, dim, edges_jsonl, train_config=None))]
pub fn serving_index_build(
    py: Python<'_>,
    dest_generation: &str,
    cards_jsonl: &str,
    chunks_jsonl: &str,
    embedding_keys_path: &str,
    embeddings_bin_path: &str,
    dim: usize,
    edges_jsonl: &str,
    train_config: Option<&str>,
) -> PyResult<PyObject> {
    let dest = PathBuf::from(dest_generation);
    py.allow_threads(|| {
        fs::create_dir_all(&dest).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        for name in ["cards.jsonl", "chunks.jsonl", "edges.jsonl"] {
            let src = match name {
                "cards.jsonl" => cards_jsonl,
                "chunks.jsonl" => chunks_jsonl,
                _ => edges_jsonl,
            };
            let dest_path = dest.join(name);
            if Path::new(src).exists() {
                if Path::new(src) != dest_path.as_path() {
                    fs::copy(src, &dest_path)
                        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
                }
            } else {
                fs::write(dest.join(name), "")
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            }
        }
        let cards = MetadataStore::load(&dest)?.by_uid.into_values().collect::<Vec<_>>();
        LexicalIndex::build(&dest.join("tantivy"), &cards)?;
        let dest_keys = dest.join("embedding_keys.txt");
        let dest_bin = dest.join("embeddings.bin");
        if Path::new(embedding_keys_path).exists() {
            if Path::new(embedding_keys_path) != dest_keys.as_path() {
                fs::copy(embedding_keys_path, &dest_keys)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            }
        } else if !dest_keys.exists() {
            fs::write(&dest_keys, "").map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        if Path::new(embeddings_bin_path).exists() {
            if Path::new(embeddings_bin_path) != dest_bin.as_path() {
                fs::copy(embeddings_bin_path, &dest_bin)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            }
        }
        let key_count = if dest_keys.exists() {
            BufReader::new(fs::File::open(&dest_keys).map_err(|e| {
                pyo3::exceptions::PyIOError::new_err(e.to_string())
            })?)
            .lines()
            .filter_map(Result::ok)
            .filter(|l| !l.trim().is_empty())
            .count()
        } else {
            0
        };
        let cfg: TrainConfig = match train_config {
            Some(raw) if !raw.trim().is_empty() => serde_json::from_str(raw)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("train_config: {e}")))?,
            _ => TrainConfig::default(),
        };
        let ivf_meta = vector::write_trained_ivf(&dest, dim, &cfg)?;
        let chunk_count = if dest.join("chunks.jsonl").exists() {
            BufReader::new(fs::File::open(dest.join("chunks.jsonl")).map_err(|e| {
                pyo3::exceptions::PyIOError::new_err(e.to_string())
            })?)
            .lines()
            .filter_map(Result::ok)
            .filter(|l| !l.trim().is_empty())
            .count()
        } else {
            0
        };
        let manifest = Manifest {
            serving_index_format_version: schema::SERVING_INDEX_FORMAT_VERSION,
            analyzer_id: schema::ANALYZER_ID.to_string(),
            vector_impl: schema::VECTOR_IMPL.to_string(),
            pipeline_version: rank::pipeline_version().to_string(),
            ranking_version: rank::ranking_version().to_string(),
            card_count: cards.len(),
            chunk_count,
            embedding_count: key_count,
            nlist: ivf_meta.nlist,
            nprobe: ivf_meta.nprobe,
            ivf_checksum: ivf_meta.checksum.clone(),
            embedding_spec: ivf_meta.embedding_spec.clone(),
        };
        fs::write(
            dest.join("manifest.json"),
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok::<_, PyErr>(manifest)
    })
    .and_then(|m| json_to_py(py, serde_json::to_value(m).unwrap()))
}

#[pyfunction]
pub fn serving_index_publish(index_root: &str, generation_id: &str) -> PyResult<String> {
    generation::publish_active(Path::new(index_root), generation_id)
}

#[pyfunction]
pub fn serving_index_status(py: Python<'_>, index_root: &str) -> PyResult<PyObject> {
    let root = Path::new(index_root);
    let active = generation::read_active(root)?;
    let dirty = dirty::read_dirty(root)?;
    let mut manifest = serde_json::json!({});
    if let Some(gid) = &active {
        let p = generation::generation_dir(root, gid).join("manifest.json");
        if p.exists() {
            if let Ok(raw) = fs::read_to_string(p) {
                manifest = serde_json::from_str(&raw).unwrap_or(serde_json::json!({}));
            }
        }
    }
    json_to_py(
        py,
        serde_json::json!({
            "serving_index_generation": active.clone().unwrap_or_default(),
            "serving_index_format": manifest.get("serving_index_format_version").cloned().unwrap_or(serde_json::json!(0)),
            "serving_index_dirty_records": dirty.len(),
            "serving_index_ready": active.is_some(),
            "manifest": manifest,
        }),
    )
}

#[pyfunction]
pub fn serving_index_mark_dirty(index_root: &str, reason: &str, uids: Vec<String>) -> PyResult<u64> {
    dirty::append_dirty(Path::new(index_root), reason, &uids)
}

#[pyfunction]
pub fn serving_index_truncate_dirty(index_root: &str) -> PyResult<()> {
    dirty::truncate_dirty(Path::new(index_root))
}

#[pyfunction]
pub fn serving_index_read_path(handle: &Bound<'_, ServingIndex>, uid: &str) -> PyResult<Option<String>> {
    let idx = handle.borrow();
    Ok(idx.meta.by_uid.get(uid).map(|c| c.rel_path.clone()))
}

#[pyfunction]
#[pyo3(signature = (generation_dir, query_vector, k, nprobe=None, candidate_budget=None))]
pub fn serving_index_ann_knn(
    py: Python<'_>,
    generation_dir: &str,
    query_vector: Vec<f32>,
    k: usize,
    nprobe: Option<usize>,
    candidate_budget: Option<usize>,
) -> PyResult<PyObject> {
    if query_vector.iter().any(|v| !v.is_finite()) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "query embedding contains non-finite values",
        ));
    }
    let ann = IvfMmapAnn::open(Path::new(generation_dir))?;
    if ann.dim() != 0 && query_vector.len() != ann.dim() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "query dimension {} != serving dimension {}",
            query_vector.len(),
            ann.dim()
        )));
    }
    let report = ann.knn_report(
        &query_vector,
        k.max(1),
        None,
        nprobe.unwrap_or(ann.nprobe()),
        candidate_budget.unwrap_or(4096),
    );
    let hits: Vec<serde_json::Value> = report
        .hits
        .iter()
        .map(|h| serde_json::json!({"key": h.key, "score": h.score}))
        .collect();
    json_to_py(
        py,
        serde_json::json!({
            "hits": hits,
            "nlist": report.nlist,
            "nprobe": report.nprobe,
            "lists_probed": report.lists_probed,
            "candidates_scored": report.candidates_scored,
            "scanned_all": report.scanned_all,
            "truncated": report.truncated,
            "skipped_invalid": report.skipped_invalid,
            "skipped_zero": report.skipped_zero,
        }),
    )
}

#[pyfunction]
pub fn serving_index_resolve_layout(py: Python<'_>, index_root: &str) -> PyResult<PyObject> {
    let root = Path::new(index_root);
    let Some(gid) = generation::read_active(root)? else {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err(
            "serving_index_unavailable",
        ));
    };
    let resolved = segments::resolve_live(root, &gid)?;
    json_to_py(py, resolved.to_json())
}

#[pyfunction]
pub fn serving_index_chunk_evidence(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    chunk_key: &str,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
    json_to_py(
        py,
        idx.chunk_evidence
            .get(chunk_key)
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ServingIndex>()?;
    m.add_function(wrap_pyfunction!(serving_index_open, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_search, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_query, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_vector, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_hybrid, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_graph, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_person, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_pointers, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_neighbor_uids, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_timeline, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_temporal_neighbors, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_build, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_publish, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_status, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_mark_dirty, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_truncate_dirty, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_read_path, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_ann_knn, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_chunk_evidence, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_resolve_layout, m)?)?;
    Ok(())
}
