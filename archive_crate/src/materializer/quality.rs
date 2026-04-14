//! `_compute_quality_score` parity.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde_json::{Map, Value};

use crate::materializer::registry::registry;

pub fn compute_quality_score_value(
    card_type: &str,
    frontmatter: &Map<String, Value>,
    body: &str,
    summary: &str,
) -> (f64, Vec<String>) {
    let reg = registry().get(card_type);
    let critical: &[String] = reg
        .map(|r| r.quality_critical_fields.as_slice())
        .unwrap_or(&[]);
    let mut flags: Vec<String> = Vec::new();
    let score = if critical.is_empty() {
        0.5_f64
    } else {
        let mut filled = 0_usize;
        for field in critical {
            let empty = match frontmatter.get(field) {
                None => true,
                Some(v) => is_empty_value_json(v),
            };
            if empty {
                flags.push(format!("missing:{field}"));
            } else {
                filled += 1;
            }
        }
        (filled as f64) / (critical.len() as f64)
    };
    let mut score = score;
    let body_stripped = body.trim();
    if body_stripped.len() > 80 {
        score = (score + 0.08_f64).min(1.0);
    } else if body_stripped.len() > 20 {
        score = (score + 0.04_f64).min(1.0);
    }
    if !summary.trim().is_empty() {
        score = (score + 0.04_f64).min(1.0);
    }
    (round4(score), flags)
}

fn is_empty_value_json(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
        _ => false,
    }
}

pub fn compute_quality_score(
    card_type: &str,
    frontmatter: &Bound<'_, PyDict>,
    body: &str,
    summary: &str,
) -> PyResult<(f64, Vec<String>)> {
    let reg = registry().get(card_type);
    let critical: &[String] = reg
        .map(|r| r.quality_critical_fields.as_slice())
        .unwrap_or(&[]);
    let mut flags: Vec<String> = Vec::new();
    let score = if critical.is_empty() {
        0.5_f64
    } else {
        let mut filled = 0_usize;
        for field in critical {
            let empty = match frontmatter.get_item(field)? {
                None => true,
                Some(v) => is_empty_value(&v)?,
            };
            if empty {
                flags.push(format!("missing:{field}"));
            } else {
                filled += 1;
            }
        }
        (filled as f64) / (critical.len() as f64)
    };
    let mut score = score;
    let body_stripped = body.trim();
    if body_stripped.len() > 80 {
        score = (score + 0.08_f64).min(1.0);
    } else if body_stripped.len() > 20 {
        score = (score + 0.04_f64).min(1.0);
    }
    if !summary.trim().is_empty() {
        score = (score + 0.04_f64).min(1.0);
    }
    Ok((round4(score), flags))
}

fn is_empty_value(v: &Bound<'_, PyAny>) -> PyResult<bool> {
    if v.is_none() {
        return Ok(true);
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(s.is_empty());
    }
    if let Ok(seq) = v.downcast::<PyList>() {
        return Ok(seq.is_empty());
    }
    if let Ok(d) = v.downcast::<PyDict>() {
        return Ok(d.is_empty());
    }
    Ok(false)
}

fn round4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}
