use std::fs;
use std::path::{Path, PathBuf};

use pyo3::prelude::*;

pub fn generations_dir(index_root: &Path) -> PathBuf {
    index_root.join("generations")
}

pub fn active_path(index_root: &Path) -> PathBuf {
    index_root.join("ACTIVE")
}

pub fn read_active(index_root: &Path) -> PyResult<Option<String>> {
    let path = active_path(index_root);
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("read ACTIVE: {e}")))?;
    let id = raw.trim();
    if id.is_empty() {
        return Ok(None);
    }
    Ok(Some(id.to_string()))
}

pub fn generation_dir(index_root: &Path, generation_id: &str) -> PathBuf {
    generations_dir(index_root).join(generation_id)
}

pub fn publish_active(index_root: &Path, generation_id: &str) -> PyResult<String> {
    let dest = generation_dir(index_root, generation_id);
    if !dest.join("manifest.json").exists() {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err(format!(
            "generation missing manifest: {}",
            dest.display()
        )));
    }
    fs::create_dir_all(index_root)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let tmp = index_root.join("ACTIVE.tmp");
    fs::write(&tmp, format!("{generation_id}\n"))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("write ACTIVE.tmp: {e}")))?;
    fs::rename(&tmp, active_path(index_root))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("publish ACTIVE: {e}")))?;
    Ok(generation_id.to_string())
}
