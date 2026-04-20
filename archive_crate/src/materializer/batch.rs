//! Full `_materialize_row` / `_materialize_row_batch` — native Rust including chunk rows.
//!
//! Step 8e — `Python::allow_threads` + `rayon` over rows; PyO3 only when building `ProjectionRowBuffer`.

use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PySet, PyTuple};
use rayon::prelude::*;
use serde_json::Value as JsonValue;

use crate::chunk::build_chunks;
use crate::json_stable;
use crate::materializer::activity::{card_activity_at_value, card_activity_end_at_value};
use crate::materializer::card_fields::CardFields;
use crate::materializer::body::read_note_body;
use crate::materializer::edges;
use crate::materializer::external_ids::iter_external_ids_value;
use crate::materializer::projection::{build_typed_projection_row_rust, ProjectionCell};
use crate::materializer::quality::compute_quality_score_value;
use crate::materializer::registry::registry;
use crate::materializer::text_hash::{build_search_text_value, content_hash_value};
use crate::materializer::time_parse::{optional_utc_to_py, parse_timestamp_to_utc_rust};
use sha2::{Digest, Sha256};

pub(crate) struct CardsRowRust {
    pub(crate) card_uid: String,
    pub(crate) rel_path: String,
    pub(crate) stem: String,
    pub(crate) card_type: String,
    pub(crate) summary: String,
    pub(crate) source_id: String,
    pub(crate) timeline: HashMap<String, String>,
    pub(crate) activity_at: Option<DateTime<Utc>>,
    pub(crate) activity_end_at: Option<DateTime<Utc>>,
    pub(crate) quality_score: f64,
    pub(crate) quality_flags: Vec<String>,
    pub(crate) content_hash: String,
    pub(crate) search_text: String,
}

pub(crate) struct ChunkRowRust {
    pub(crate) ck: String,
    pub(crate) card_uid: String,
    pub(crate) rel_path: String,
    pub(crate) chunk_type: String,
    pub(crate) chunk_index: i32,
    pub(crate) chunk_schema_version: i32,
    pub(crate) source_fields: Vec<String>,
    pub(crate) content: String,
    pub(crate) content_hash: String,
    pub(crate) token_count: i32,
}

pub(crate) struct MaterializedOneRust {
    pub(crate) cards: CardsRowRust,
    pub(crate) ingestion: (String, String, String, String),
    pub(crate) card_sources: Vec<(String, String)>,
    pub(crate) card_people: Vec<(String, String)>,
    pub(crate) card_orgs: Vec<(String, String)>,
    pub(crate) typed_projection: Option<(String, Vec<ProjectionCell>, bool, String)>,
    pub(crate) external_ids: Vec<(String, String, String, String)>,
    pub(crate) edges: Vec<edges::EdgeRow>,
    pub(crate) chunks: Vec<ChunkRowRust>,
}

/// Same keys as `archive_cli.features.TIMELINE_FIELDS`.
const TIMELINE_FIELDS: &[&str] = &[
    "created",
    "updated",
    "created_at",
    "updated_at",
    "sent_at",
    "start_at",
    "end_at",
    "first_message_at",
    "last_message_at",
    "captured_at",
    "occurred_at",
    "recorded_at",
    "committed_at",
    "departure_at",
    "arrival_at",
    "pickup_at",
    "dropoff_at",
    "check_in",
    "check_out",
    "pay_date",
    "shipped_at",
    "event_at",
    "activity_end_at",
];

fn chunk_key(card_uid: &str, chunk_type: &str, chunk_index: i32, content_hash: &str) -> String {
    let payload = format!("{card_uid}:{chunk_type}:{chunk_index}:{content_hash}");
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn timeline_str_value(fm: &serde_json::Map<String, JsonValue>, field: &str) -> String {
    fm.get(field)
        .map(|v| match v {
            JsonValue::Null => String::new(),
            JsonValue::String(s) => s.trim().to_string(),
            _ => v.to_string().trim().to_string(),
        })
        .unwrap_or_default()
}

fn timeline_strings_value(fm: &serde_json::Map<String, JsonValue>) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for field in TIMELINE_FIELDS {
        m.insert((*field).to_string(), timeline_str_value(fm, field));
    }
    m
}

fn build_dedupe_key_tuple(py: Python<'_>, row: &Bound<'_, PyTuple>) -> PyResult<PyObject> {
    let mut parts = Vec::new();
    for i in 0..row.len() {
        let cell = row.get_item(i)?;
        if let Ok(list) = cell.downcast::<PyList>() {
            let elems: Vec<PyObject> = (0..list.len())
                .map(|j| Ok::<_, PyErr>(list.get_item(j)?.unbind()))
                .collect::<Result<Vec<_>, _>>()?;
            parts.push(PyTuple::new_bound(py, elems).to_object(py));
        } else {
            parts.push(cell.unbind());
        }
    }
    Ok(PyTuple::new_bound(py, parts).to_object(py))
}

fn dedupe_table_rows(py: Python<'_>, rows: Vec<Py<PyTuple>>) -> PyResult<Vec<Py<PyTuple>>> {
    let seen = PySet::empty_bound(py)?;
    let mut out = Vec::new();
    for row in rows {
        let row_b = row.bind(py);
        let key = build_dedupe_key_tuple(py, &row_b)?;
        if seen.contains(&key)? {
            continue;
        }
        seen.add(key)?;
        out.push(row);
    }
    Ok(out)
}

fn materialize_one_rust(
    rel_path: String,
    fm_val: JsonValue,
    vault_root: &str,
    slug_map: &HashMap<String, String>,
    path_to_uid: &HashMap<String, String>,
    person_lookup: &HashMap<String, String>,
    target_field_index: &HashMap<String, HashMap<String, String>>,
    batch_id: &str,
    chunk_schema_version: i32,
    body_cache: Option<&HashMap<String, Vec<u8>>>,
) -> Result<MaterializedOneRust, String> {
    let fm = fm_val
        .as_object()
        .ok_or_else(|| "frontmatter must be a JSON object".to_string())?;
    let card = CardFields::from_frontmatter_value(fm_val.clone())
        .map_err(|e| format!("CardFields: {e}"))?;

    let body = if let Some(cache) = body_cache {
        crate::materializer::body::read_note_body_from_cache(cache, vault_root, &rel_path)
    } else {
        read_note_body(vault_root, &rel_path)
    }
    .map_err(|e| format!("read {rel_path}: {e}"))?;

    let search_text = build_search_text_value(fm, &body);
    let content_hash_val =
        content_hash_value(fm_val.clone(), &body).map_err(|e| e.to_string())?;

    let activity_raw = card_activity_at_value(fm);
    let activity_at = parse_timestamp_to_utc_rust(&activity_raw);
    let card_type = card.card_type.clone();
    let activity_end_raw = card_activity_end_at_value(&card_type, fm);
    let activity_end_at = parse_timestamp_to_utc_rust(&activity_end_raw);

    let timeline = timeline_strings_value(fm);
    let summary = card.summary.clone();
    let (quality_score, quality_flags) =
        compute_quality_score_value(&card_type, fm, &body, &summary);

    let source_adapter = card
        .source
        .first()
        .map(|s| s.trim().to_string())
        .unwrap_or_default();

    let stem = Path::new(&rel_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();

    let card_uid = card.uid.clone();

    let source_id = fm
        .get("source_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let cards = CardsRowRust {
        card_uid: card_uid.clone(),
        rel_path: rel_path.clone(),
        stem,
        card_type: card.card_type.clone(),
        summary: summary.clone(),
        source_id,
        timeline,
        activity_at,
        activity_end_at,
        quality_score,
        quality_flags,
        content_hash: content_hash_val,
        search_text,
    };

    let mut card_sources = Vec::new();
    for s in &card.source {
        card_sources.push((card_uid.clone(), s.clone()));
    }

    let mut card_people = Vec::new();
    for ps in &card.people {
        let ps = ps.trim();
        if ps.is_empty() {
            continue;
        }
        let resolved_path = edges::resolve_person_reference(person_lookup, ps);
        let person_key = if let Some(rp) = resolved_path {
            if let Some(uid) = path_to_uid.get(&rp) {
                uid.clone()
            } else {
                ps.to_string()
            }
        } else {
            ps.to_string()
        };
        card_people.push((card_uid.clone(), person_key));
    }

    let mut card_orgs = Vec::new();
    for o in &card.orgs {
        card_orgs.push((card_uid.clone(), o.clone()));
    }

    let typed_projection = if registry().contains_key(&card_type) {
        build_typed_projection_row_rust(&card_type, &card, &rel_path, fm)?
    } else {
        None
    };

    let mut external_ids = Vec::new();
    for (field_name, provider, ext_id) in iter_external_ids_value(fm) {
        external_ids.push((card_uid.clone(), field_name, provider, ext_id));
    }

    let edge_rows = edges::build_edges(
        &rel_path,
        fm,
        &card,
        &body,
        slug_map,
        path_to_uid,
        person_lookup,
        target_field_index,
    );

    let chunk_records = build_chunks(&fm_val, body.as_str());
    let mut chunks = Vec::with_capacity(chunk_records.len());
    for chunk in chunk_records {
        let ck = chunk_key(
            &card_uid,
            &chunk.chunk_type,
            chunk.chunk_index,
            &chunk.content_hash,
        );
        chunks.push(ChunkRowRust {
            ck,
            card_uid: card_uid.clone(),
            rel_path: rel_path.clone(),
            chunk_type: chunk.chunk_type,
            chunk_index: chunk.chunk_index,
            chunk_schema_version,
            source_fields: chunk.source_fields,
            content: chunk.content,
            content_hash: chunk.content_hash,
            token_count: chunk.token_count,
        });
    }

    Ok(MaterializedOneRust {
        cards,
        ingestion: (
            card_uid,
            "created".to_string(),
            source_adapter,
            batch_id.to_string(),
        ),
        card_sources,
        card_people,
        card_orgs,
        typed_projection,
        external_ids,
        edges: edge_rows,
        chunks,
    })
}

fn materialize_rust_to_py(py: Python<'_>, m: MaterializedOneRust) -> PyResult<Py<PyAny>> {
    let buf_cls = py
        .import_bound("archive_cli.projections.base")?
        .getattr("ProjectionRowBuffer")?;
    let buf = buf_cls.call0()?;

    let c = m.cards;
    let activity_at_obj = match optional_utc_to_py(py, c.activity_at)? {
        Some(o) => o,
        None => py.None().to_object(py),
    };
    let activity_end_obj = match optional_utc_to_py(py, c.activity_end_at)? {
        Some(o) => o,
        None => py.None().to_object(py),
    };
    let quality_flags = PyList::empty_bound(py);
    for f in c.quality_flags {
        quality_flags.append(f)?;
    }

    let cards_row = PyTuple::new_bound(
        py,
        [
            c.card_uid.to_object(py),
            c.rel_path.to_object(py),
            c.stem.to_object(py),
            c.card_type.to_object(py),
            c.summary.to_object(py),
            c.source_id.to_object(py),
            c.timeline
                .get("created")
                .cloned()
                .unwrap_or_default()
                .to_object(py),
            c.timeline
                .get("updated")
                .cloned()
                .unwrap_or_default()
                .to_object(py),
            activity_at_obj,
            activity_end_obj,
            c.timeline
                .get("sent_at")
                .cloned()
                .unwrap_or_default()
                .to_object(py),
            c.timeline
                .get("start_at")
                .cloned()
                .unwrap_or_default()
                .to_object(py),
            c.timeline
                .get("first_message_at")
                .cloned()
                .unwrap_or_default()
                .to_object(py),
            c.timeline
                .get("last_message_at")
                .cloned()
                .unwrap_or_default()
                .to_object(py),
            c.quality_score.to_object(py),
            quality_flags.to_object(py),
            0_i32.to_object(py),
            "none".to_object(py),
            py.None().to_object(py),
            c.content_hash.to_object(py),
            c.search_text.to_object(py),
        ],
    );
    buf.call_method1("add", ("cards", cards_row))?;

    let log_tuple = PyTuple::new_bound(
        py,
        [
            m.ingestion.0.as_str(),
            m.ingestion.1.as_str(),
            m.ingestion.2.as_str(),
            m.ingestion.3.as_str(),
        ],
    );
    buf.getattr("ingestion_log_rows")?
        .call_method1("append", (log_tuple,))?;

    for (uid, s) in m.card_sources {
        let t = PyTuple::new_bound(py, [uid.as_str(), s.as_str()]);
        buf.call_method1("add", ("card_sources", t))?;
    }

    for (uid, pk) in m.card_people {
        let t = PyTuple::new_bound(py, [uid.as_str(), pk.as_str()]);
        buf.call_method1("add", ("card_people", t))?;
    }

    for (uid, o) in m.card_orgs {
        let t = PyTuple::new_bound(py, [uid.as_str(), o.as_str()]);
        buf.call_method1("add", ("card_orgs", t))?;
    }

    if let Some((table, cells, _cr, _mn)) = m.typed_projection {
        let py_cells: Vec<PyObject> = cells
            .into_iter()
            .map(|cell| cell.to_py(py))
            .collect::<PyResult<Vec<_>>>()?;
        let typed_row = PyTuple::new_bound(py, py_cells);
        buf.call_method1("add", (table.as_str(), typed_row))?;
    }

    for (uid, field_name, provider, ext_id) in m.external_ids {
        let t = PyTuple::new_bound(
            py,
            [
                uid.as_str(),
                field_name.as_str(),
                provider.as_str(),
                ext_id.as_str(),
            ],
        );
        buf.call_method1("add", ("external_ids", t))?;
    }

    for e in m.edges {
        let t = PyTuple::new_bound(
            py,
            [
                e.source_uid.as_str(),
                e.source_path.as_str(),
                e.target_uid.as_str(),
                e.target_slug.as_str(),
                e.target_path.as_str(),
                e.target_kind.as_str(),
                e.edge_type.as_str(),
                e.field_name.as_str(),
            ],
        );
        buf.call_method1("add", ("edges", t))?;
    }

    let json_mod = py.import_bound("json")?;
    for ch in m.chunks {
        let source_fields = PyList::empty_bound(py);
        for s in &ch.source_fields {
            source_fields.append(s)?;
        }
        let sf_str: String = json_mod
            .call_method1("dumps", (source_fields,))?
            .extract()?;
        let row = PyTuple::new_bound(
            py,
            [
                ch.ck.to_object(py),
                ch.card_uid.to_object(py),
                ch.rel_path.to_object(py),
                ch.chunk_type.to_object(py),
                ch.chunk_index.to_object(py),
                ch.chunk_schema_version.to_object(py),
                sf_str.to_object(py),
                ch.content.to_object(py),
                ch.content_hash.to_object(py),
                ch.token_count.to_object(py),
            ],
        );
        buf.call_method1("add", ("chunks", row))?;
    }

    let rbt = buf.getattr("rows_by_table")?;
    let rows_by_table = rbt.downcast::<PyDict>()?;
    let table_names: Vec<String> = rows_by_table
        .iter()
        .map(|(k, _)| k.extract::<String>())
        .collect::<PyResult<Vec<_>>>()?;
    for table_name in table_names {
        let py_rows = rows_by_table
            .get_item(&table_name)?
            .ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!("missing table {table_name}"))
            })?
            .downcast_into::<PyList>()?;
        let mut tuples: Vec<Py<PyTuple>> = Vec::new();
        for i in 0..py_rows.len() {
            let item = py_rows.get_item(i)?;
            tuples.push(item.downcast_into::<PyTuple>()?.unbind());
        }
        let deduped = dedupe_table_rows(py, tuples)?;
        let new_list = PyList::empty_bound(py);
        for t in deduped {
            new_list.append(t)?;
        }
        rows_by_table.set_item(table_name, new_list)?;
    }

    Ok(buf.unbind())
}

/// Native materializer — parity with `archive_cli.materializer._materialize_row_batch`.
///
/// When `body_cache` (a `BodyCache` handle) is provided, bodies are read from the pre-loaded
/// in-memory cache (zlib-decompressed, already provenance-stripped) instead of disk.
/// Create the `BodyCache` once via `BodyCache.load(path)` and pass it to every batch call.
#[pyfunction]
#[pyo3(signature = (rows, vault_root, slug_map, path_to_uid, person_lookup, target_field_index, batch_id=None, chunk_schema_version=5, body_cache=None))]
pub fn materialize_row_batch(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
    vault_root: String,
    slug_map: HashMap<String, String>,
    path_to_uid: HashMap<String, String>,
    person_lookup: HashMap<String, String>,
    target_field_index: HashMap<String, HashMap<String, String>>,
    batch_id: Option<String>,
    chunk_schema_version: i32,
    body_cache: Option<&crate::materializer::body::BodyCache>,
) -> PyResult<PyObject> {
    let batch_id = batch_id.unwrap_or_default();
    let seq = rows.downcast::<PyList>()?;
    let n = seq.len();
    let mut inputs: Vec<(String, JsonValue)> = Vec::with_capacity(n);
    for i in 0..n {
        let row = seq.get_item(i)?;
        let rel_path: String = row.getattr("rel_path")?.extract()?;
        let row_fm = row.getattr("frontmatter")?;
        let fm_val = json_stable::json_value_from_py_any(py, &row_fm)?;
        inputs.push((rel_path, fm_val));
    }

    let vault_root_s = vault_root.clone();
    let bid = batch_id.clone();
    let sm = slug_map.clone();
    let ptu = path_to_uid.clone();
    let pl = person_lookup.clone();
    let tfi = target_field_index.clone();
    let cache_arc = body_cache.map(|bc| bc.inner.clone());

    let results: Vec<Result<MaterializedOneRust, String>> = py.allow_threads(|| {
        inputs
            .into_par_iter()
            .map(|(rel_path, fm_val)| {
                materialize_one_rust(
                    rel_path,
                    fm_val,
                    vault_root_s.as_str(),
                    &sm,
                    &ptu,
                    &pl,
                    &tfi,
                    bid.as_str(),
                    chunk_schema_version,
                    cache_arc.as_deref(),
                )
            })
            .collect()
    });

    let out_cls = py
        .import_bound("archive_cli.projections.base")?
        .getattr("ProjectionRowBuffer")?;
    let out = out_cls.call0()?;

    for r in results {
        let m = r.map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;
        let one = materialize_rust_to_py(py, m)?;
        out.call_method1("extend", (one,))?;
    }

    Ok(out.into())
}

/// Materialize ALL rows — maps converted once from Python, then shared as `&HashMap` refs
/// across rayon workers (zero cloning per batch). Processes in chunks of `batch_size`,
/// materializing each chunk in parallel then converting to Python. Logs progress to stderr.
/// Returns a Python list of `ProjectionRowBuffer` objects.
#[pyfunction]
#[pyo3(signature = (rows, vault_root, slug_map, path_to_uid, person_lookup, target_field_index, batch_id=None, chunk_schema_version=5, body_cache=None, batch_size=5000))]
pub fn materialize_all_rows(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
    vault_root: String,
    slug_map: HashMap<String, String>,
    path_to_uid: HashMap<String, String>,
    person_lookup: HashMap<String, String>,
    target_field_index: HashMap<String, HashMap<String, String>>,
    batch_id: Option<String>,
    chunk_schema_version: i32,
    body_cache: Option<&crate::materializer::body::BodyCache>,
    batch_size: usize,
) -> PyResult<PyObject> {
    let batch_id = batch_id.unwrap_or_default();
    let seq = rows.downcast::<PyList>()?;
    let n = seq.len();
    let mut inputs: Vec<(String, JsonValue)> = Vec::with_capacity(n);
    for i in 0..n {
        let row = seq.get_item(i)?;
        let rel_path: String = row.getattr("rel_path")?.extract()?;
        let row_fm = row.getattr("frontmatter")?;
        let fm_val = json_stable::json_value_from_py_any(py, &row_fm)?;
        inputs.push((rel_path, fm_val));
    }

    let cache_ref = body_cache.map(|bc| &*bc.inner);
    let bs = if batch_size == 0 { 5000 } else { batch_size };

    let out_list = PyList::empty_bound(py);

    let total = inputs.len();
    let mut processed = 0usize;
    let started = std::time::Instant::now();
    let mut remaining = inputs;

    while !remaining.is_empty() {
        let split_at = bs.min(remaining.len());
        let chunk_vec: Vec<(String, JsonValue)> = remaining.drain(..split_at).collect();

        let tfi_ref = target_field_index.clone();
        let chunk_results: Vec<Result<MaterializedOneRust, String>> = py.allow_threads(|| {
            chunk_vec
                .into_par_iter()
                .map(|(rel_path, fm_val)| {
                    materialize_one_rust(
                        rel_path,
                        fm_val,
                        vault_root.as_str(),
                        &slug_map,
                        &path_to_uid,
                        &person_lookup,
                        &tfi_ref,
                        batch_id.as_str(),
                        chunk_schema_version,
                        cache_ref,
                    )
                })
                .collect()
        });

        let mut materialized: Vec<MaterializedOneRust> = Vec::with_capacity(chunk_results.len());
        for r in chunk_results {
            let m = r.map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;
            materialized.push(m);
        }
        let copy_buf = crate::materializer::copy_buffer::build_copy_buffer(materialized);
        let py_buf = Py::new(py, copy_buf)?;
        out_list.append(py_buf)?;
        processed += split_at;

        let elapsed = started.elapsed().as_secs_f64();
        let rate = processed as f64 / elapsed.max(0.001);
        let eta = (total - processed) as f64 / rate.max(1.0);
        eprintln!(
            "[archive_crate] materialize {}/{} ({:.1}%) elapsed={:.1}s rate={:.0}/s eta={:.0}s",
            processed, total,
            processed as f64 / total as f64 * 100.0,
            elapsed, rate, eta,
        );
    }

    Ok(out_list.into())
}
