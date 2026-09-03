use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirtyRecord {
    pub ts: String,
    pub reason: String,
    pub uids: Vec<String>,
}

pub fn dirty_path(index_root: &Path) -> std::path::PathBuf {
    index_root.join("DIRTY")
}

pub fn append_dirty(index_root: &Path, reason: &str, uids: &[String]) -> PyResult<u64> {
    fs::create_dir_all(index_root).map_err(|e| {
        pyo3::exceptions::PyIOError::new_err(format!("create serving index root: {e}"))
    })?;
    let rec = DirtyRecord {
        ts: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_else(|_| "0".into()),
        reason: reason.to_string(),
        uids: uids.to_vec(),
    };
    let line = serde_json::to_string(&rec)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dirty_path(index_root))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("open DIRTY: {e}")))?;
    writeln!(f, "{line}")
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("write DIRTY: {e}")))?;
    Ok(1)
}

pub fn read_dirty(index_root: &Path) -> PyResult<Vec<DirtyRecord>> {
    let path = dirty_path(index_root);
    if !path.exists() {
        return Ok(vec![]);
    }
    let f = fs::File::open(&path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("read DIRTY: {e}")))?;
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line.map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: DirtyRecord = serde_json::from_str(&line)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        out.push(rec);
    }
    Ok(out)
}

pub fn truncate_dirty(index_root: &Path) -> PyResult<()> {
    let path = dirty_path(index_root);
    if path.exists() {
        fs::write(&path, "").map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    }
    Ok(())
}
