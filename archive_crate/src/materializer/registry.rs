//! Typed projection + edge rules loaded from `materializer_registry.json` (see `scripts/export_materializer_registry.py`).

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(default)]
    pub sql_type: String,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    #[serde(default)]
    #[allow(dead_code)]
    pub indexed: bool,
    #[serde(default)]
    pub source_field: Option<String>,
    #[serde(default = "default_text_mode")]
    pub value_mode: String,
    #[serde(default)]
    pub default: serde_json::Value,
}

fn default_nullable() -> bool {
    true
}

fn default_text_mode() -> String {
    "text".to_string()
}

#[derive(Clone, Debug, Deserialize)]
pub struct EdgeRuleSpec {
    pub field_name: String,
    pub edge_type: String,
    pub target: String,
    pub source_fields: Vec<String>,
    #[serde(default)]
    pub multi: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CardTypeSpec {
    pub card_type: String,
    pub projection_table: String,
    pub person_edge_type: String,
    #[serde(default)]
    pub quality_critical_fields: Vec<String>,
    #[serde(default)]
    pub edge_rules: Vec<EdgeRuleSpec>,
    pub shared_typed_columns: Vec<ColumnSpec>,
    pub typed_columns: Vec<ColumnSpec>,
}

#[derive(Debug, Deserialize)]
struct RegistryFile {
    #[allow(dead_code)]
    pub registry_version: u32,
    #[allow(dead_code)]
    pub projection_registry_version: u32,
    pub card_types: Vec<CardTypeSpec>,
}

static REGISTRY: OnceLock<HashMap<String, CardTypeSpec>> = OnceLock::new();

pub fn registry() -> &'static HashMap<String, CardTypeSpec> {
    REGISTRY.get_or_init(|| {
        let raw = include_str!("../../materializer_registry.json");
        let parsed: RegistryFile =
            serde_json::from_str(raw).expect("materializer_registry.json valid");
        parsed
            .card_types
            .into_iter()
            .map(|c| (c.card_type.clone(), c))
            .collect()
    })
}

pub fn merged_columns(spec: &CardTypeSpec) -> Vec<ColumnSpec> {
    let mut out = Vec::with_capacity(spec.shared_typed_columns.len() + spec.typed_columns.len());
    out.extend(spec.shared_typed_columns.iter().cloned());
    out.extend(spec.typed_columns.iter().cloned());
    out
}
