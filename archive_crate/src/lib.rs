//! `archive_crate` — Rust performance engine for PPA (PyO3 extension).

use pyo3::prelude::*;

mod bridge;
mod cache;
mod cache_build;
mod cache_iter;
mod chunk;
mod chunker;
mod config;
mod frontmatter;
mod json_stable;
mod fuzzy_resolver;
mod resolve_batch;
mod hasher;
mod materializer;
mod person_index;
mod progress;
mod scanner;
mod walk;

#[pymodule]
fn archive_crate(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(walk::walk_vault, m)?)?;
    m.add_function(wrap_pyfunction!(walk::walk_vault_count, m)?)?;
    m.add_function(wrap_pyfunction!(walk::walk_vault_monolithic, m)?)?;
    m.add_function(wrap_pyfunction!(hasher::raw_content_sha256, m)?)?;
    m.add_function(wrap_pyfunction!(hasher::content_hash, m)?)?;
    m.add_function(wrap_pyfunction!(frontmatter::parse_frontmatter, m)?)?;
    m.add_function(wrap_pyfunction!(json_stable::stable_json_from_yaml_frontmatter, m)?)?;
    m.add_function(wrap_pyfunction!(cache::vault_fingerprint, m)?)?;
    m.add_function(wrap_pyfunction!(cache::build_vault_cache, m)?)?;
    m.add_function(wrap_pyfunction!(scanner::vault_paths_and_fingerprint, m)?)?;
    m.add_function(wrap_pyfunction!(scanner::cards_by_type_from_cache, m)?)?;
    m.add_function(wrap_pyfunction!(scanner::cards_by_type, m)?)?;
    m.add_function(wrap_pyfunction!(materializer::build_search_text, m)?)?;
    m.add_function(wrap_pyfunction!(materializer::materialize_content_hash, m)?)?;
    m.add_function(wrap_pyfunction!(materializer::materialize_row_batch, m)?)?;
    m.add_function(wrap_pyfunction!(chunker::chunk_hash, m)?)?;
    m.add_function(wrap_pyfunction!(chunker::render_chunks_for_card, m)?)?;
    m.add_function(wrap_pyfunction!(bridge::rebuild_index, m)?)?;
    m.add_function(wrap_pyfunction!(person_index::build_person_index, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_batch::resolve_person_batch, m)?)?;
    m.add_class::<person_index::PersonResolutionIndex>()?;
    m.add_function(wrap_pyfunction!(
        person_index::person_index_counts_from_cache,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(fuzzy_resolver::token_sort_ratio, m)?)?;
    m.add_function(wrap_pyfunction!(cache_iter::notes_from_cache, m)?)?;
    m.add_function(wrap_pyfunction!(cache_iter::frontmatter_dicts_from_cache, m)?)?;
    m.add_function(wrap_pyfunction!(cache_iter::note_paths_from_cache, m)?)?;
    m.add_class::<progress::ProgressCallback>()?;
    Ok(())
}
