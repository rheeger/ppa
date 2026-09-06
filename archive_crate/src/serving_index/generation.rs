use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use pyo3::prelude::*;

/// Immutable generation directories. ACTIVE is a pointer; parents stay until unpinned.

pub const COMPLETE_FILE: &str = "COMPLETE";

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

fn fsync_path(path: &Path) -> PyResult<()> {
    let file = File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    file.sync_all()
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("fsync {}: {e}", path.display())))?;
    Ok(())
}

fn fsync_dir(path: &Path) -> PyResult<()> {
    let file = File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    file.sync_all()
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("fsync dir {}: {e}", path.display())))?;
    Ok(())
}

fn count_nonempty_lines(path: &Path) -> PyResult<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    Ok(BufReader::new(file)
        .lines()
        .filter_map(Result::ok)
        .filter(|line| !line.trim().is_empty())
        .count())
}

/// File-level pre-promotion gate. A manifest alone is not enough.
pub fn validate_artifacts(dest: &Path) -> PyResult<serde_json::Value> {
    let required = [
        "manifest.json",
        "cards.jsonl",
        "chunks.jsonl",
        "edges.jsonl",
        "embedding_keys.txt",
    ];
    for name in required {
        if !dest.join(name).exists() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "publication_validation_failed: missing:{name}"
            )));
        }
    }
    let raw = fs::read_to_string(dest.join("manifest.json"))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("manifest.json: {e}")))?;
    if raw.trim().is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "publication_validation_failed: truncated:manifest.json",
        ));
    }
    let manifest: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("manifest.json: {e}")))?;
    let format = manifest
        .get("serving_index_format_version")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    if format != super::schema::SERVING_INDEX_FORMAT_VERSION as u64 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "publication_validation_failed: format:{format}"
        )));
    }
    let key_count = count_nonempty_lines(&dest.join("embedding_keys.txt"))?;
    let bin_len = dest
        .join("embeddings.bin")
        .metadata()
        .map(|m| m.len())
        .unwrap_or(0);
    let dim = manifest
        .get("embedding_spec")
        .and_then(|v| v.get("dimension"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    if key_count > 0 && dim > 0 && bin_len != key_count as u64 * dim * 4 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "publication_validation_failed: embeddings_truncated",
        ));
    }
    if key_count > 0 && !dest.join("ivf_meta.json").exists() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "publication_validation_failed: missing:ivf_meta.json",
        ));
    }
    Ok(serde_json::json!({
        "ok": true,
        "card_lines": count_nonempty_lines(&dest.join("cards.jsonl"))?,
        "chunk_lines": count_nonempty_lines(&dest.join("chunks.jsonl"))?,
        "embedding_keys": key_count,
        "format": format,
    }))
}

pub fn write_complete(dest: &Path, payload: &serde_json::Value) -> PyResult<()> {
    let path = dest.join(COMPLETE_FILE);
    let tmp = dest.join("COMPLETE.tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("COMPLETE.tmp: {e}")))?;
        file.write_all(serde_json::to_string_pretty(payload).unwrap().as_bytes())
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        file.sync_all()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("fsync COMPLETE.tmp: {e}")))?;
    }
    fs::rename(&tmp, &path).map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("COMPLETE: {e}")))?;
    fsync_path(&path)?;
    fsync_dir(dest)?;
    Ok(())
}

pub fn has_complete(dest: &Path) -> bool {
    dest.join(COMPLETE_FILE).exists()
}

pub fn publish_active(index_root: &Path, generation_id: &str) -> PyResult<String> {
    let dest = generation_dir(index_root, generation_id);
    if !dest.join("manifest.json").exists() {
        return Err(pyo3::exceptions::PyFileNotFoundError::new_err(format!(
            "generation missing manifest: {}",
            dest.display()
        )));
    }
    let report = validate_artifacts(&dest)?;
    if !has_complete(&dest) {
        write_complete(
            &dest,
            &serde_json::json!({
                "generation_id": generation_id,
                "checks": report,
                "source": "native_pre_promotion",
            }),
        )?;
    }
    if !has_complete(&dest) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "publication_validation_failed: missing:COMPLETE",
        ));
    }
    fs::create_dir_all(index_root)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let tmp = index_root.join("ACTIVE.tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("write ACTIVE.tmp: {e}")))?;
        file.write_all(format!("{generation_id}\n").as_bytes())
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        file.sync_all()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("fsync ACTIVE.tmp: {e}")))?;
    }
    fs::rename(&tmp, active_path(index_root))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("publish ACTIVE: {e}")))?;
    fsync_dir(index_root)?;
    Ok(generation_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_rejects_truncated_embeddings() {
        let root = std::env::temp_dir().join(format!("ppa-gen-trunc-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let dest = generation_dir(&root, "g1");
        fs::create_dir_all(&dest).unwrap();
        fs::write(
            dest.join("manifest.json"),
            r#"{"serving_index_format_version":2,"embedding_spec":{"dimension":4}}"#,
        )
        .unwrap();
        fs::write(dest.join("cards.jsonl"), "").unwrap();
        fs::write(dest.join("chunks.jsonl"), "").unwrap();
        fs::write(dest.join("edges.jsonl"), "").unwrap();
        fs::write(dest.join("embedding_keys.txt"), "ck-a\n").unwrap();
        fs::write(dest.join("embeddings.bin"), b"xx").unwrap();
        assert!(validate_artifacts(&dest).is_err());
        let _ = fs::remove_dir_all(&root);
    }
}
