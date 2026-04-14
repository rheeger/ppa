//! Typed projection row building — **Step 8d** parity with `archive_cli.projections.base.build_projection_row` / `_column_value`.
//!
//! All cell logic uses `serde_json::Value` (`fm_value`): `json_text_value` (Python `json.dumps(..., sort_keys=True)` style via `json_stable::python_style_json_dumps_sorted`), `external_ids_by_provider_value`, `relationship_payload_value`, `iter_string_values_json`, `parse_timestamp_to_utc` (Rust). No `py.import_bound("archive_cli.features")`. PyO3 is only used to allocate Python `datetime` / `bool` / `str` objects for `ProjectionRowBuffer` tuple cells.
//!
//! Evidence: `tests/test_archive_crate_materializer_chunker.py::test_materialize_row_batch_rust_matches_python` (typed table rows included).

use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use serde_json::{Map, Value};

use crate::materializer::activity::card_activity_at_value;
use crate::materializer::card_fields::CardFields;
use crate::materializer::fm_value::{
    external_ids_by_provider_value, iter_string_values_json, json_text_value,
    relationship_payload_value,
};
use crate::materializer::registry::{merged_columns, registry, ColumnSpec};
use crate::materializer::time_parse::{parse_timestamp_to_utc_rust, utc_datetime_to_py};

/// Intermediate cell for parallel materialization (Step 8e) — converted to Python under the GIL.
#[derive(Debug)]
pub(crate) enum ProjectionCell {
    NoneVal,
    Str(String),
    Bool(bool),
    F64(f64),
    I32(i32),
    DateTime(DateTime<Utc>),
}

impl ProjectionCell {
    pub(crate) fn to_py(self, py: Python<'_>) -> PyResult<PyObject> {
        match self {
            ProjectionCell::NoneVal => Ok(py.None().to_object(py)),
            ProjectionCell::Str(s) => Ok(s.to_object(py)),
            ProjectionCell::Bool(b) => Ok(b.to_object(py)),
            ProjectionCell::F64(f) => Ok(f.to_object(py)),
            ProjectionCell::I32(i) => Ok(i.to_object(py)),
            ProjectionCell::DateTime(dt) => utc_datetime_to_py(py, &dt),
        }
    }
}

fn fm_get_default<'a>(fm: &'a Map<String, Value>, key: &str, default: &'a Value) -> &'a Value {
    fm.get(key).unwrap_or(default)
}

fn value_truthy_json(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|x| x != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn python_or3_json(a: &Value, b: &Value, c: &Value) -> Value {
    if value_truthy_json(a) {
        return a.clone();
    }
    if value_truthy_json(b) {
        return b.clone();
    }
    c.clone()
}

/// str(merged) matching Python `builtins.str` on JSON-backed cells — scalar `fm_str`-like.
fn value_to_display_string(v: &Value) -> String {
    match v {
        Value::Null => "None".to_string(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
    }
}

fn column_value_rust(
    column: &ColumnSpec,
    card: &CardFields,
    rel_path: &str,
    fm: &Map<String, Value>,
    typed_projection_version: i32,
    canonical_ready: bool,
    migration_notes: &str,
) -> Result<ProjectionCell, String> {
    let mode = column.value_mode.as_str();
    if matches!(
        mode,
        "card_uid"
            | "rel_path"
            | "card_type"
            | "summary"
            | "primary_source"
            | "activity_at"
            | "external_ids_json"
            | "relationships_json"
            | "typed_projection_version"
            | "canonical_ready"
            | "migration_notes"
            | "primary_person"
    ) {
        return column_value_special_rust(
            column,
            card,
            rel_path,
            fm,
            typed_projection_version,
            canonical_ready,
            migration_notes,
        );
    }

    let source_field = column
        .source_field
        .as_deref()
        .unwrap_or(column.name.as_str());
    let value = fm_get_default(fm, source_field, &column.default);

    if mode == "json" {
        let filler: Value = if column.default.is_array() {
            Value::Array(vec![])
        } else if column.sql_type == "JSONB" {
            Value::Object(Map::new())
        } else {
            column.default.clone()
        };
        let payload = if !value_truthy_json(value) {
            &filler
        } else {
            value
        };
        let jt = json_text_value(payload);
        return Ok(ProjectionCell::Str(jt));
    }
    if mode == "bool" {
        return Ok(ProjectionCell::Bool(value_truthy_json(value)));
    }
    if mode == "float" {
        let d = column.default.as_f64().unwrap_or(0.0);
        let f = match value {
            Value::Number(n) => n.as_f64().unwrap_or(d),
            Value::String(s) => s.parse::<f64>().unwrap_or(d),
            _ => d,
        };
        return Ok(ProjectionCell::F64(f));
    }
    if mode == "int" {
        let d = column.default.as_i64().unwrap_or(0) as i32;
        let i = match value {
            Value::Number(n) => n.as_i64().unwrap_or(d as i64) as i32,
            Value::String(s) => s.parse::<i64>().unwrap_or(d as i64) as i32,
            _ => d,
        };
        return Ok(ProjectionCell::I32(i));
    }
    if matches!(value, Value::Array(_)) {
        let vals = iter_string_values_json(value);
        let def_str = py_json_default_str(&column.default);
        return Ok(ProjectionCell::Str(
            vals.into_iter().next().unwrap_or(def_str),
        ));
    }
    let merged = python_or3_json(
        value,
        &column.default,
        &Value::String(String::new()),
    );
    let s = value_to_display_string(&merged);
    Ok(ProjectionCell::Str(s))
}

fn py_json_default_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        _ => String::new(),
    }
}

fn column_value_special_rust(
    column: &ColumnSpec,
    card: &CardFields,
    rel_path: &str,
    fm: &Map<String, Value>,
    typed_projection_version: i32,
    canonical_ready: bool,
    migration_notes: &str,
) -> Result<ProjectionCell, String> {
    match column.value_mode.as_str() {
        "card_uid" => Ok(ProjectionCell::Str(card.uid.clone())),
        "rel_path" => Ok(ProjectionCell::Str(rel_path.to_string())),
        "card_type" => Ok(ProjectionCell::Str(card.card_type.clone())),
        "summary" => Ok(ProjectionCell::Str(card.summary.clone())),
        "primary_source" => {
            let v = fm.get("source").unwrap_or(&Value::Null);
            let sources = iter_string_values_json(v);
            Ok(ProjectionCell::Str(
                sources.into_iter().next().unwrap_or_default(),
            ))
        }
        "activity_at" => {
            let raw = card_activity_at_value(fm);
            Ok(match parse_timestamp_to_utc_rust(&raw) {
                Some(dt) => ProjectionCell::DateTime(dt),
                None => ProjectionCell::NoneVal,
            })
        }
        "external_ids_json" => {
            let grouped = external_ids_by_provider_value(fm);
            let jt = json_text_value(&Value::Object(grouped));
            Ok(ProjectionCell::Str(jt))
        }
        "relationships_json" => {
            let rel = relationship_payload_value(fm);
            let jt = json_text_value(&Value::Object(rel));
            Ok(ProjectionCell::Str(jt))
        }
        "typed_projection_version" => Ok(ProjectionCell::I32(typed_projection_version)),
        "canonical_ready" => Ok(ProjectionCell::Bool(canonical_ready)),
        "migration_notes" => Ok(ProjectionCell::Str(migration_notes.to_string())),
        "primary_person" => {
            let sf = column.source_field.as_deref().unwrap_or(&column.name);
            let v = fm.get(sf).unwrap_or(&Value::Null);
            let people = iter_string_values_json(v);
            Ok(ProjectionCell::Str(
                people.into_iter().next().unwrap_or_default(),
            ))
        }
        _ => Err(format!("unknown value_mode {}", column.value_mode)),
    }
}

fn is_empty_cell_json(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
        _ => false,
    }
}

/// Step 8e — pure Rust; convert with [`ProjectionCell::to_py`] under the GIL.
pub(crate) fn build_typed_projection_row_rust(
    card_type: &str,
    card: &CardFields,
    rel_path: &str,
    fm: &Map<String, Value>,
) -> Result<Option<(String, Vec<ProjectionCell>, bool, String)>, String> {
    let Some(spec) = registry().get(card_type) else {
        return Ok(None);
    };
    let table = spec.projection_table.clone();
    let columns = merged_columns(spec);
    let mut missing: Vec<String> = Vec::new();
    for column in &columns {
        let skip_modes = matches!(
            column.value_mode.as_str(),
            "card_uid"
                | "rel_path"
                | "card_type"
                | "summary"
                | "primary_source"
                | "activity_at"
                | "external_ids_json"
                | "relationships_json"
                | "typed_projection_version"
                | "canonical_ready"
                | "migration_notes"
        );
        if skip_modes {
            continue;
        }
        let sf = column.source_field.as_deref().unwrap_or(&column.name);
        let v = fm.get(sf);
        let empty = match v {
            None => true,
            Some(v) => is_empty_cell_json(v),
        };
        if !column.nullable && empty {
            missing.push(sf.to_string());
        }
    }
    missing.sort();
    missing.dedup();
    let canonical_ready = missing.is_empty();
    let migration_notes = missing.join(", ");
    let mut cells: Vec<ProjectionCell> = Vec::with_capacity(columns.len());
    for column in &columns {
        let cell = column_value_rust(
            column,
            card,
            rel_path,
            fm,
            1,
            canonical_ready,
            &migration_notes,
        )?;
        cells.push(cell);
    }
    Ok(Some((table, cells, canonical_ready, migration_notes)))
}

/// Kept for parity with a potential PyO3-only call path; batch uses [`build_typed_projection_row_rust`] + [`ProjectionCell::to_py`].
#[allow(dead_code)]
pub fn build_typed_projection_row(
    py: Python<'_>,
    card_type: &str,
    card: &CardFields,
    rel_path: &str,
    fm: &Map<String, Value>,
) -> PyResult<Option<(Py<PyTuple>, bool, String)>> {
    let Some((_, cells, canonical_ready, migration_notes)) =
        build_typed_projection_row_rust(card_type, card, rel_path, fm)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?
    else {
        return Ok(None);
    };
    let mut py_cells: Vec<PyObject> = Vec::with_capacity(cells.len());
    for cell in cells {
        py_cells.push(cell.to_py(py)?);
    }
    let tuple = PyTuple::new_bound(py, py_cells);
    Ok(Some((tuple.unbind(), canonical_ready, migration_notes)))
}
