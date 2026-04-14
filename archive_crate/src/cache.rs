//! Vault scan cache — SQLite layout matches `archive_cli.vault_cache` (tier 1 frontmatter-only; tier ≥2 full note).

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache_build::{self, BuiltRows};
use crate::walk;

const CACHE_VERSION: i32 = 1;
const BATCH_INSERT: usize = 1000;
/// Match `archive_cli.vault_cache.ZLIB_LEVEL`.
const ZLIB_LEVEL: u32 = 6;

/// Match `archive_cli.vault_cache._compute_vault_fingerprint` (sorted paths, same line format).
pub fn compute_vault_fingerprint(
    vault: &Path,
    rel_paths: &[String],
) -> Result<(HashMap<String, (i64, i64)>, String), String> {
    let mut sorted: Vec<&String> = rel_paths.iter().collect();
    sorted.sort();
    let mut lines: Vec<String> = Vec::new();
    let mut stats: HashMap<String, (i64, i64)> = HashMap::new();
    for rel_path in sorted {
        let target = vault.join(rel_path);
        let meta = match fs::metadata(&target) {
            Ok(m) => m,
            Err(_) => continue,
        };
        let mtime_ns = file_mtime_ns(&meta);
        let size = meta.len() as i64;
        stats.insert(rel_path.clone(), (mtime_ns, size));
        lines.push(format!("{rel_path}\t{mtime_ns}\t{size}"));
    }
    let joined = lines.join("\n");
    let fp = hex::encode(Sha256::digest(joined.as_bytes()));
    Ok((stats, fp))
}

fn file_mtime_ns(meta: &std::fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return meta.mtime() * 1_000_000_000 + meta.mtime_nsec() as i64;
    }
    #[cfg(not(unix))]
    {
        meta.modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64 * 1_000_000_000 + d.subsec_nanos() as i64)
            .unwrap_or(0)
    }
}

fn init_db(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS cache_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS notes (
            rel_path TEXT PRIMARY KEY,
            uid TEXT NOT NULL DEFAULT '',
            card_type TEXT NOT NULL DEFAULT '',
            slug TEXT NOT NULL DEFAULT '',
            mtime_ns INTEGER NOT NULL DEFAULT 0,
            file_size INTEGER NOT NULL DEFAULT 0,
            frontmatter_json TEXT NOT NULL DEFAULT '{}',
            frontmatter_hash TEXT NOT NULL DEFAULT '',
            body_compressed BLOB,
            content_hash TEXT,
            wikilinks_json TEXT,
            raw_content_sha256 TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_notes_uid ON notes(uid);
        CREATE INDEX IF NOT EXISTS idx_notes_card_type ON notes(card_type);
        "#,
    )?;
    Ok(())
}

fn meta_set(conn: &Connection, key: &str, value: &str) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO cache_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

/// Fingerprint only: `(stats_dict, fingerprint_hex)` matching Python `_compute_vault_fingerprint`.
#[pyfunction]
pub fn vault_fingerprint(py: Python<'_>, vault_path: String) -> PyResult<(PyObject, String)> {
    let vault = PathBuf::from(&vault_path);
    let rel_paths = walk::collect_note_paths(&vault_path)?;
    let (stats, fp) = compute_vault_fingerprint(&vault, &rel_paths)
        .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
    let dict = PyDict::new_bound(py);
    for (k, (mt, sz)) in stats {
        let tup = PyTuple::new_bound(py, [mt, sz]);
        dict.set_item(k, tup)?;
    }
    Ok((dict.to_object(py), fp))
}

fn flush_tier2_batch_rs(
    conn: &mut Connection,
    batch: &mut Vec<cache_build::Tier2Row>,
    inserted: &mut usize,
) -> rusqlite::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let tx = conn.transaction()?;
    for row in batch.drain(..) {
        tx.execute(
            r#"INSERT INTO notes (
                rel_path, uid, card_type, slug, mtime_ns, file_size,
                frontmatter_json, frontmatter_hash, body_compressed, content_hash, wikilinks_json,
                raw_content_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            params![
                row.rel_path,
                row.uid,
                row.card_type,
                row.slug,
                row.mtime_ns,
                row.file_size,
                row.fm_json,
                row.fm_hash,
                row.body_compressed,
                row.content_hash,
                row.wikilinks_json,
                row.raw_content_sha256,
            ],
        )?;
        *inserted += 1;
    }
    tx.commit()?;
    Ok(())
}

fn flush_tier1_batch_rs(
    conn: &mut Connection,
    batch: &mut Vec<cache_build::Tier1Row>,
    inserted: &mut usize,
) -> rusqlite::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let tx = conn.transaction()?;
    for row in batch.drain(..) {
        tx.execute(
            r#"INSERT INTO notes (
                rel_path, uid, card_type, slug, mtime_ns, file_size,
                frontmatter_json, frontmatter_hash, body_compressed, content_hash, wikilinks_json,
                raw_content_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)"#,
            params![
                row.rel_path,
                row.uid,
                row.card_type,
                row.slug,
                row.mtime_ns,
                row.file_size,
                row.fm_json,
                row.fm_hash,
            ],
        )?;
        *inserted += 1;
    }
    tx.commit()?;
    Ok(())
}

/// Build vault scan cache at `cache_path` (same schema as Python). `tier` 1 = frontmatter-only; `tier` ≥ 2 = full note (body, wikilinks, hashes).
/// Step 5a: pure-Rust per-note path + `rayon` + releases the GIL for the whole build.
#[pyfunction]
#[pyo3(signature = (vault_path, cache_path, tier=1))]
pub fn build_vault_cache(
    py: Python<'_>,
    vault_path: String,
    cache_path: String,
    tier: i32,
) -> PyResult<PyObject> {
    if tier < 1 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "tier must be >= 1",
        ));
    }
    let tier_ge2 = tier >= 2;
    let vault = PathBuf::from(&vault_path);
    let rel_paths = walk::collect_note_paths(&vault_path)?;
    let (stats, fp) = compute_vault_fingerprint(&vault, &rel_paths)
        .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;

    let vault_path_owned = vault_path.clone();
    let cache_path_owned = cache_path.clone();
    let rel_paths_owned = rel_paths.clone();
    let stats_owned = stats;
    let fp_owned = fp.clone();
    let tier_meta = tier;

    let (inserted, fp_out) = py.allow_threads(move || {
        if let Some(parent) = Path::new(&cache_path_owned).parent() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let mut conn = Connection::open(&cache_path_owned).map_err(|e| e.to_string())?;
        init_db(&conn).map_err(|e| e.to_string())?;
        conn.execute("DELETE FROM notes", [])
            .map_err(|e| e.to_string())?;
        conn.execute("DELETE FROM cache_meta", [])
            .map_err(|e| e.to_string())?;

        let built = cache_build::build_all_rows(
            Path::new(&vault_path_owned),
            &rel_paths_owned,
            &stats_owned,
            tier_ge2,
            ZLIB_LEVEL,
        )
        .map_err(|e| e.to_string())?;

        let mut inserted: usize = 0;
        match built {
            BuiltRows::Tier1(rows) => {
                let mut chunk: Vec<cache_build::Tier1Row> = Vec::with_capacity(BATCH_INSERT);
                for row in rows {
                    chunk.push(row);
                    if chunk.len() >= BATCH_INSERT {
                        flush_tier1_batch_rs(&mut conn, &mut chunk, &mut inserted)
                            .map_err(|e| e.to_string())?;
                    }
                }
                flush_tier1_batch_rs(&mut conn, &mut chunk, &mut inserted)
                    .map_err(|e| e.to_string())?;
            }
            BuiltRows::Tier2(rows) => {
                let mut chunk: Vec<cache_build::Tier2Row> = Vec::with_capacity(BATCH_INSERT);
                for row in rows {
                    chunk.push(row);
                    if chunk.len() >= BATCH_INSERT {
                        flush_tier2_batch_rs(&mut conn, &mut chunk, &mut inserted)
                            .map_err(|e| e.to_string())?;
                    }
                }
                flush_tier2_batch_rs(&mut conn, &mut chunk, &mut inserted)
                    .map_err(|e| e.to_string())?;
            }
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        meta_set(&conn, "vault_fingerprint", &fp_owned).map_err(|e| e.to_string())?;
        meta_set(&conn, "tier", &tier_meta.to_string()).map_err(|e| e.to_string())?;
        meta_set(&conn, "cache_version", &CACHE_VERSION.to_string())
            .map_err(|e| e.to_string())?;
        meta_set(&conn, "generated_at", &now.to_string()).map_err(|e| e.to_string())?;
        meta_set(&conn, "note_count", &inserted.to_string()).map_err(|e| e.to_string())?;

        Ok::<(usize, String), String>((inserted, fp_owned))
    })
    .map_err(|e: String| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let out = PyDict::new_bound(py);
    out.set_item("note_count", inserted)?;
    out.set_item("fingerprint", fp_out)?;
    Ok(out.to_object(py))
}
