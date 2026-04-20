//! Pre-serialized COPY buffer — builds tab-separated text per table in Rust,
//! avoiding all PyTuple allocation. Python pipes the bytes through `cur.copy().write()`.

use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::batch::MaterializedOneRust;
use super::projection::ProjectionCell;

fn escape_copy(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\0', "")
}

fn write_field(buf: &mut String, val: &str) {
    buf.push_str(&escape_copy(val));
}

fn write_tab(buf: &mut String) {
    buf.push('\t');
}

fn write_newline(buf: &mut String) {
    buf.push('\n');
}

fn opt_datetime_copy(dt: &Option<chrono::DateTime<chrono::Utc>>) -> String {
    match dt {
        Some(d) => d.to_rfc3339(),
        None => "\\N".to_string(),
    }
}

fn pg_text_array(vals: &[String]) -> String {
    if vals.is_empty() {
        return "{}".to_string();
    }
    let escaped: Vec<String> = vals
        .iter()
        .map(|v| {
            let inner = v.replace('\\', "\\\\").replace('"', "\\\"").replace('\0', "");
            format!("\"{inner}\"")
        })
        .collect();
    format!("{{{}}}", escaped.join(","))
}

fn projection_cell_to_copy(cell: &ProjectionCell) -> String {
    match cell {
        ProjectionCell::NoneVal => "\\N".to_string(),
        ProjectionCell::Str(s) => escape_copy(s),
        ProjectionCell::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        ProjectionCell::F64(f) => f.to_string(),
        ProjectionCell::I32(i) => i.to_string(),
        ProjectionCell::DateTime(dt) => dt.to_rfc3339(),
    }
}

/// Pre-serialized COPY data for all tables from a batch of materialized rows.
#[pyclass]
pub struct CopyBuffer {
    pub(crate) tables: HashMap<String, Vec<u8>>,
    pub(crate) card_count: usize,
}

#[pymethods]
impl CopyBuffer {
    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    fn table_data(&self, table_name: &str) -> Option<&[u8]> {
        self.tables.get(table_name).map(|v| v.as_slice())
    }

    fn card_count(&self) -> usize {
        self.card_count
    }

    fn table_row_count(&self, table_name: &str) -> usize {
        self.tables
            .get(table_name)
            .map(|v| bytecount::count(v, b'\n'))
            .unwrap_or(0)
    }

    fn to_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new_bound(py);
        for (table, data) in &self.tables {
            d.set_item(table, pyo3::types::PyBytes::new_bound(py, data))?;
        }
        Ok(d)
    }
}

pub(crate) fn build_copy_buffer(rows: Vec<MaterializedOneRust>) -> CopyBuffer {
    let card_count = rows.len();
    let mut tables: HashMap<String, String> = HashMap::new();
    let mut seen_card_sources: HashSet<(String, String)> = HashSet::new();
    let mut seen_card_people: HashSet<(String, String)> = HashSet::new();
    let mut seen_card_orgs: HashSet<(String, String)> = HashSet::new();
    let mut seen_external_ids: HashSet<(String, String, String, String)> = HashSet::new();

    for m in rows {
        let c = &m.cards;

        // cards table
        {
            let buf = tables.entry("cards".to_string()).or_default();
            write_field(buf, &c.card_uid); write_tab(buf);
            write_field(buf, &c.rel_path); write_tab(buf);
            write_field(buf, &c.stem); write_tab(buf);
            write_field(buf, &c.card_type); write_tab(buf);
            write_field(buf, &c.summary); write_tab(buf);
            write_field(buf, &c.source_id); write_tab(buf);
            write_field(buf, c.timeline.get("created").map(|s| s.as_str()).unwrap_or("")); write_tab(buf);
            write_field(buf, c.timeline.get("updated").map(|s| s.as_str()).unwrap_or("")); write_tab(buf);
            buf.push_str(&opt_datetime_copy(&c.activity_at)); write_tab(buf);
            buf.push_str(&opt_datetime_copy(&c.activity_end_at)); write_tab(buf);
            write_field(buf, c.timeline.get("sent_at").map(|s| s.as_str()).unwrap_or("")); write_tab(buf);
            write_field(buf, c.timeline.get("start_at").map(|s| s.as_str()).unwrap_or("")); write_tab(buf);
            write_field(buf, c.timeline.get("first_message_at").map(|s| s.as_str()).unwrap_or("")); write_tab(buf);
            write_field(buf, c.timeline.get("last_message_at").map(|s| s.as_str()).unwrap_or("")); write_tab(buf);
            let _ = write!(buf, "{}", c.quality_score); write_tab(buf);
            buf.push_str(&pg_text_array(&c.quality_flags)); write_tab(buf);
            buf.push_str("0"); write_tab(buf);
            buf.push_str("none"); write_tab(buf);
            buf.push_str("\\N"); write_tab(buf);
            write_field(buf, &c.content_hash); write_tab(buf);
            write_field(buf, &c.search_text);
            write_newline(buf);
        }

        // ingestion_log
        {
            let buf = tables.entry("ingestion_log".to_string()).or_default();
            write_field(buf, &m.ingestion.0); write_tab(buf);
            write_field(buf, &m.ingestion.1); write_tab(buf);
            write_field(buf, &m.ingestion.2); write_tab(buf);
            write_field(buf, &m.ingestion.3);
            write_newline(buf);
        }

        // card_sources (deduplicate)
        for (uid, src) in &m.card_sources {
            if seen_card_sources.insert((uid.clone(), src.clone())) {
                let buf = tables.entry("card_sources".to_string()).or_default();
                write_field(buf, uid); write_tab(buf);
                write_field(buf, src);
                write_newline(buf);
            }
        }

        // card_people (deduplicate)
        for (uid, pk) in &m.card_people {
            if seen_card_people.insert((uid.clone(), pk.clone())) {
                let buf = tables.entry("card_people".to_string()).or_default();
                write_field(buf, uid); write_tab(buf);
                write_field(buf, pk);
                write_newline(buf);
            }
        }

        // card_orgs (deduplicate)
        for (uid, org) in &m.card_orgs {
            if seen_card_orgs.insert((uid.clone(), org.clone())) {
                let buf = tables.entry("card_orgs".to_string()).or_default();
                write_field(buf, uid); write_tab(buf);
                write_field(buf, org);
                write_newline(buf);
            }
        }

        // typed projection
        if let Some((table, cells, _cr, _mn)) = &m.typed_projection {
            let buf = tables.entry(table.clone()).or_default();
            for (i, cell) in cells.iter().enumerate() {
                if i > 0 { write_tab(buf); }
                buf.push_str(&projection_cell_to_copy(cell));
            }
            write_newline(buf);
        }

        // external_ids (deduplicate)
        for (uid, field, provider, ext_id) in &m.external_ids {
            if seen_external_ids.insert((uid.clone(), field.clone(), provider.clone(), ext_id.clone())) {
                let buf = tables.entry("external_ids".to_string()).or_default();
                write_field(buf, uid); write_tab(buf);
                write_field(buf, field); write_tab(buf);
                write_field(buf, provider); write_tab(buf);
                write_field(buf, ext_id);
                write_newline(buf);
            }
        }

        // edges
        {
            let mut seen_edges: HashSet<String> = HashSet::new();
            for e in &m.edges {
                let key = format!("{}:{}:{}:{}:{}", e.source_uid, e.target_uid, e.edge_type, e.field_name, e.target_slug);
                if seen_edges.insert(key) {
                    let buf = tables.entry("edges".to_string()).or_default();
                    write_field(buf, &e.source_uid); write_tab(buf);
                    write_field(buf, &e.source_path); write_tab(buf);
                    write_field(buf, &e.target_uid); write_tab(buf);
                    write_field(buf, &e.target_slug); write_tab(buf);
                    write_field(buf, &e.target_path); write_tab(buf);
                    write_field(buf, &e.target_kind); write_tab(buf);
                    write_field(buf, &e.edge_type); write_tab(buf);
                    write_field(buf, &e.field_name);
                    write_newline(buf);
                }
            }
        }

        // chunks
        for ch in &m.chunks {
            let buf = tables.entry("chunks".to_string()).or_default();
            write_field(buf, &ch.ck); write_tab(buf);
            write_field(buf, &ch.card_uid); write_tab(buf);
            write_field(buf, &ch.rel_path); write_tab(buf);
            write_field(buf, &ch.chunk_type); write_tab(buf);
            let _ = write!(buf, "{}", ch.chunk_index); write_tab(buf);
            let _ = write!(buf, "{}", ch.chunk_schema_version); write_tab(buf);
            let sf_json = serde_json::to_string(&ch.source_fields).unwrap_or_else(|_| "[]".to_string());
            write_field(buf, &sf_json); write_tab(buf);
            write_field(buf, &ch.content); write_tab(buf);
            write_field(buf, &ch.content_hash); write_tab(buf);
            let _ = write!(buf, "{}", ch.token_count);
            write_newline(buf);
        }
    }

    let byte_tables: HashMap<String, Vec<u8>> = tables
        .into_iter()
        .map(|(k, v)| (k, v.into_bytes()))
        .collect();

    CopyBuffer {
        tables: byte_tables,
        card_count,
    }
}
