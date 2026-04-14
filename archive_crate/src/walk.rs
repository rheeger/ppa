//! Vault walk — matches `archive_vault.vault.iter_note_paths` (EXCLUDED_DIRS, `.md` only, skip dot dirs).
//!
//! Top-level subdirectories of the vault are walked in parallel with **rayon**; each subtree uses
//! `walkdir::WalkDir` with the same `filter_entry` semantics as a single monolithic walk from the
//! vault root.

use pyo3::prelude::*;
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

const EXCLUDED_DIRS: &[&str] = &["_templates", "Attachments", ".obsidian", "_meta"];

fn rel_path_string(vault: &Path, file_path: &Path) -> Option<String> {
    let rel = file_path.strip_prefix(vault).ok()?;
    Some(rel.to_string_lossy().replace('\\', "/"))
}

/// When walking from *vault root* with `WalkDir::new(vault)`.
fn filter_walk_entry(e: &DirEntry) -> bool {
    if e.depth() == 0 {
        return true;
    }
    let name = e.file_name().to_string_lossy();
    if name.starts_with('.') {
        return false;
    }
    if e.file_type().is_dir() && EXCLUDED_DIRS.contains(&name.as_ref()) {
        return false;
    }
    true
}

/// When walking a subtree rooted at a direct child of the vault (depth 0 = that folder).
fn filter_subtree_entry(e: &DirEntry) -> bool {
    if e.depth() == 0 {
        return true;
    }
    let name = e.file_name().to_string_lossy();
    if name.starts_with('.') {
        return false;
    }
    if e.file_type().is_dir() && EXCLUDED_DIRS.contains(&name.as_ref()) {
        return false;
    }
    true
}

fn collect_md_paths_under(
    vault: &Path,
    walk_root: &Path,
    filter: fn(&DirEntry) -> bool,
) -> Vec<String> {
    WalkDir::new(walk_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(filter)
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| {
            let name = e.file_name().to_str()?;
            if name.starts_with('.') || !name.ends_with(".md") {
                return None;
            }
            rel_path_string(vault, e.path())
        })
        .collect()
}

fn count_md_under(walk_root: &Path, filter: fn(&DirEntry) -> bool) -> usize {
    WalkDir::new(walk_root)
        .follow_links(false)
        .into_iter()
        .filter_entry(filter)
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|name| !name.starts_with('.') && name.ends_with(".md"))
                .unwrap_or(false)
        })
        .count()
}

/// Monolithic walk — reference for parity (single-threaded). Used by tests via Python.
pub(crate) fn collect_note_paths_monolithic(vault_path: &str) -> PyResult<Vec<String>> {
    let vault = Path::new(vault_path);
    if !vault.is_dir() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "not a directory: {vault_path}"
        )));
    }
    let mut out: Vec<String> = WalkDir::new(vault)
        .follow_links(false)
        .into_iter()
        .filter_entry(filter_walk_entry)
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| {
            let name = e.file_name().to_str()?;
            if name.starts_with('.') || !name.ends_with(".md") {
                return None;
            }
            rel_path_string(vault, e.path())
        })
        .collect();
    out.sort_unstable();
    Ok(out)
}

/// Parallel walk: root-level `*.md` + `rayon` over each non-excluded top-level directory.
pub(crate) fn collect_note_paths(vault_path: &str) -> PyResult<Vec<String>> {
    let vault = Path::new(vault_path);
    if !vault.is_dir() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "not a directory: {vault_path}"
        )));
    }

    let read_dir = std::fs::read_dir(vault).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("read_dir {}: {e}", vault.display()))
    })?;

    let mut root_md: Vec<String> = Vec::new();
    let mut subdirs: Vec<PathBuf> = Vec::new();

    for entry in read_dir {
        let entry = entry.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("read_dir entry: {e}"))
        })?;
        let path = entry.path();
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with('.') {
            continue;
        }
        if path.is_file() {
            if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
                if fname.ends_with(".md") && !fname.starts_with('.') {
                    if let Some(rel) = rel_path_string(vault, &path) {
                        root_md.push(rel);
                    }
                }
            }
        } else if path.is_dir() {
            if !EXCLUDED_DIRS.contains(&name_str.as_ref()) {
                subdirs.push(path);
            }
        }
    }

    let mut from_subtrees: Vec<Vec<String>> = subdirs
        .par_iter()
        .map(|root| collect_md_paths_under(vault, root, filter_subtree_entry))
        .collect();

    let mut out = root_md;
    for mut v in from_subtrees.drain(..) {
        out.append(&mut v);
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

pub(crate) fn collect_note_paths_count(vault_path: &str) -> PyResult<usize> {
    let vault = Path::new(vault_path);
    if !vault.is_dir() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "not a directory: {vault_path}"
        )));
    }

    let read_dir = std::fs::read_dir(vault).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("read_dir {}: {e}", vault.display()))
    })?;

    let mut root_count: usize = 0;
    let mut subdirs: Vec<PathBuf> = Vec::new();

    for entry in read_dir {
        let entry = entry.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("read_dir entry: {e}"))
        })?;
        let path = entry.path();
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with('.') {
            continue;
        }
        if path.is_file() {
            if let Some(fname) = path.file_name().and_then(|s| s.to_str()) {
                if fname.ends_with(".md") && !fname.starts_with('.') {
                    root_count += 1;
                }
            }
        } else if path.is_dir() {
            if !EXCLUDED_DIRS.contains(&name_str.as_ref()) {
                subdirs.push(path);
            }
        }
    }

    let sub_total: usize = subdirs
        .par_iter()
        .map(|root| count_md_under(root, filter_subtree_entry))
        .sum();

    Ok(root_count + sub_total)
}

#[pyfunction]
pub fn walk_vault(vault_path: String) -> PyResult<Vec<String>> {
    collect_note_paths(&vault_path)
}

#[pyfunction]
pub fn walk_vault_count(vault_path: String) -> PyResult<usize> {
    collect_note_paths_count(&vault_path)
}

/// Exposed for tests: monolithic walk (single WalkDir from vault root).
#[pyfunction]
pub fn walk_vault_monolithic(vault_path: String) -> PyResult<Vec<String>> {
    collect_note_paths_monolithic(&vault_path)
}
