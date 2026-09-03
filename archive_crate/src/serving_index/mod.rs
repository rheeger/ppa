mod dirty;
mod generation;
mod graph;
mod lexical;
mod metadata;
mod rank;
mod schema;
mod vector;

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use serde::Serialize;

use crate::serving_index::graph::GraphStore;
use crate::serving_index::lexical::LexicalIndex;
use crate::serving_index::metadata::{CardMeta, MetadataStore};
use crate::serving_index::vector::{IvfMmapAnn, VectorAnn};

#[pyclass]
pub struct ServingIndex {
    #[allow(dead_code)]
    generation_id: String,
    #[allow(dead_code)]
    dir: PathBuf,
    meta: MetadataStore,
    graph: GraphStore,
    lexical: Option<LexicalIndex>,
    vectors: Option<IvfMmapAnn>,
    chunk_to_card: HashMap<String, (String, String, i32)>,
}

static OPEN_LOCK: Mutex<()> = Mutex::new(());

fn chunk_map_path(dir: &Path) -> PathBuf {
    dir.join("chunks.jsonl")
}

fn load_chunk_map(dir: &Path) -> PyResult<HashMap<String, (String, String, i32)>> {
    let path = chunk_map_path(dir);
    let mut out = HashMap::new();
    if !path.exists() {
        return Ok(out);
    }
    let f = fs::File::open(&path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    for line in BufReader::new(f).lines() {
        let line = line.map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&line)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let key = v.get("chunk_key").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let card = v.get("card_uid").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let ctype = v.get("chunk_type").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let idx = v.get("chunk_index").and_then(|x| x.as_i64()).unwrap_or(0) as i32;
        if !key.is_empty() {
            out.insert(key, (card, ctype, idx));
        }
    }
    Ok(out)
}

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
    let meta = MetadataStore::load(&dir)?;
    let graph = GraphStore::load(&dir)?;
    let lexical = LexicalIndex::open(&dir.join("tantivy")).ok();
    let vectors = IvfMmapAnn::open(&dir).ok();
    let chunk_to_card = load_chunk_map(&dir)?;
    Ok(ServingIndex {
        generation_id: gid,
        dir,
        meta,
        graph,
        lexical,
        vectors,
        chunk_to_card,
    })
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

fn card_to_row(card: &CardMeta, extra: serde_json::Value) -> serde_json::Value {
    let mut row = serde_json::json!({
        "card_uid": card.card_uid,
        "uid": card.card_uid,
        "rel_path": card.rel_path,
        "summary": card.summary,
        "type": card.r#type,
        "activity_at": card.activity_at,
        "activity_end_at": card.activity_end_at,
        "corpus_state": card.corpus_state,
        "slug": card.slug,
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
    let hits = if let Some(lex) = &idx.lexical {
        lex.search(&query, limit * 4, &type_filter)?
    } else {
        Vec::new()
    };
    let mut rows = Vec::new();
    for (uid, score) in hits {
        if let Some(card) = idx.meta.by_uid.get(&uid) {
            if !idx.meta.matches_filters(
                card,
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
            rows.push(card_to_row(
                card,
                serde_json::json!({
                    "matched_by": "lexical",
                    "lexical_score": score,
                    "exact_match": exact,
                    "slug_exact": slug_e,
                    "summary_exact": sum_e,
                    "external_id_exact": ext_e,
                    "person_exact": per_e,
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
    let mut cards: Vec<&CardMeta> = idx
        .meta
        .by_uid
        .values()
        .filter(|c| {
            idx.meta.matches_filters(
                c,
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
    let Some(ann) = &idx.vectors else {
        return json_to_py(py, serde_json::Value::Array(vec![]));
    };
    let knn = ann.knn(&query_vector, limit * 8);
    let mut best: HashMap<String, (f32, String, i32, usize)> = HashMap::new();
    for (chunk_key, sim) in knn {
        if let Some((card_uid, ctype, cidx)) = idx.chunk_to_card.get(&chunk_key) {
            let e = best.entry(card_uid.clone()).or_insert((sim, ctype.clone(), *cidx, 0));
            e.3 += 1;
            if sim > e.0 {
                e.0 = sim;
                e.1 = ctype.clone();
                e.2 = *cidx;
            }
        }
    }
    let mut rows = Vec::new();
    let mut items: Vec<_> = best.into_iter().collect();
    items.sort_by(|a, b| b.1.0.partial_cmp(&a.1.0).unwrap_or(std::cmp::Ordering::Equal));
    for (uid, (sim, ctype, cidx, matched)) in items {
        if let Some(card) = idx.meta.by_uid.get(&uid) {
            if !idx.meta.matches_filters(
                card,
                &type_filter,
                &source_filter,
                &people_filter,
                "",
                &start_date,
                &end_date,
            ) {
                continue;
            }
            rows.push(card_to_row(
                card,
                serde_json::json!({
                    "matched_by": "vector",
                    "similarity": sim,
                    "vector_similarity": sim,
                    "chunk_type": ctype,
                    "chunk_index": cidx,
                    "matched_chunk_count": matched,
                    "preview": card.summary.chars().take(160).collect::<String>(),
                    "provenance_bias": "mixed",
                    "provenance_score": 0.04,
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
    let cap = (limit * 8).max(limit);
    let mut lexical = HashMap::new();
    if let Some(lex) = &idx.lexical {
        for (uid, score) in lex.search(query, cap, &type_filter)? {
            lexical.insert(uid, score);
        }
    }
    let mut vector = HashMap::new();
    if let Some(ann) = &idx.vectors {
        for (chunk_key, sim) in ann.knn(&query_vector, cap) {
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
                idx.meta.matches_filters(
                    c,
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
                idx.meta.matches_filters(
                    c,
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
    let trust = idx.graph.neighbor_trust(&anchors);
    let rows = rank::fuse(&lexical, &vector, &trust, &idx.meta.by_uid, query, limit);
    json_to_py(py, serde_json::Value::Array(rows))
}

#[pyfunction]
pub fn serving_index_graph(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    start: &str,
    hops: usize,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
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
    let graph = idx.graph.hops(&uid, hops);
    let mut out = serde_json::Map::new();
    for (node, targets) in graph {
        let items: Vec<serde_json::Value> = targets
            .into_iter()
            .map(|(path_or_uid, edge_type)| {
                let path = idx
                    .meta
                    .by_uid
                    .get(&path_or_uid)
                    .map(|c| c.rel_path.clone())
                    .unwrap_or(path_or_uid);
                serde_json::json!({"path": path, "edge_type": edge_type})
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
pub fn serving_index_person(py: Python<'_>, handle: &Bound<'_, ServingIndex>, name: &str) -> PyResult<PyObject> {
    let idx = handle.borrow();
    let needle = name.trim().to_lowercase().replace(' ', "-");
    let uid = idx
        .meta
        .by_slug
        .get(&name.trim().to_lowercase())
        .or_else(|| idx.meta.by_slug.get(&needle))
        .cloned();
    if let Some(uid) = uid {
        if let Some(card) = idx.meta.by_uid.get(&uid) {
            return json_to_py(
                py,
                serde_json::json!({"found": true, "rel_path": card.rel_path, "card_uid": uid}),
            );
        }
    }
    json_to_py(py, serde_json::json!({"found": false, "rel_path": "", "card_uid": ""}))
}

#[pyfunction]
pub fn serving_index_pointers(
    py: Python<'_>,
    handle: &Bound<'_, ServingIndex>,
    uids: Vec<String>,
) -> PyResult<PyObject> {
    let idx = handle.borrow();
    let map = idx.graph.pointers(&uids);
    json_to_py(py, serde_json::to_value(map).unwrap_or(serde_json::json!({})))
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
    let cards = idx.meta.timeline_range(
        &start_date,
        &end_date,
        limit,
        &type_filter,
        &source_filter,
        &people_filter,
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
    let Some(hits) = idx.meta.temporal_neighbors(
        timestamp,
        direction,
        limit,
        &type_filter,
        &source_filter,
        &people_filter,
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
    card_count: usize,
    chunk_count: usize,
    embedding_count: usize,
}

#[pyfunction]
pub fn serving_index_build(
    py: Python<'_>,
    dest_generation: &str,
    cards_jsonl: &str,
    chunks_jsonl: &str,
    embedding_keys_path: &str,
    embeddings_bin_path: &str,
    dim: usize,
    edges_jsonl: &str,
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
        let _ = dim;
        vector::write_ivf_assignments(&dest, key_count)?;
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
            card_count: cards.len(),
            chunk_count,
            embedding_count: key_count,
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
    m.add_function(wrap_pyfunction!(serving_index_timeline, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_temporal_neighbors, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_build, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_publish, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_status, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_mark_dirty, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_truncate_dirty, m)?)?;
    m.add_function(wrap_pyfunction!(serving_index_read_path, m)?)?;
    Ok(())
}
