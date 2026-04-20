//! Vault scan cache — SQLite layout matches `archive_cli.vault_cache` (tier 1 frontmatter-only; tier ≥2 full note).
//!
//! Supports incremental rebuilds: on cache miss, only notes whose `mtime_ns` or `file_size`
//! changed (or that are new/deleted) are re-parsed. Unchanged notes keep their existing rows.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use rayon::prelude::*;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache_build::{self, BuiltRows};
use crate::walk;

const BATCH_INSERT: usize = 1000;
/// Match `archive_cli.vault_cache.ZLIB_LEVEL`.
const ZLIB_LEVEL: u32 = 6;

/// Match `archive_cli.vault_cache._compute_vault_fingerprint` (sorted paths, same line format).
/// Stat calls are parallelised with rayon; the hash is computed over sorted paths for determinism.
pub fn compute_vault_fingerprint(
    vault: &Path,
    rel_paths: &[String],
) -> Result<(HashMap<String, (i64, i64)>, String), String> {
    let stat_results: Vec<Option<(String, i64, i64)>> = rel_paths
        .par_iter()
        .map(|rel_path| {
            let target = vault.join(rel_path);
            match fs::metadata(&target) {
                Ok(meta) => {
                    let mtime_ns = file_mtime_ns(&meta);
                    let size = meta.len() as i64;
                    Some((rel_path.clone(), mtime_ns, size))
                }
                Err(_) => None,
            }
        })
        .collect();

    let mut stats: HashMap<String, (i64, i64)> = HashMap::with_capacity(rel_paths.len());
    for item in stat_results.into_iter().flatten() {
        stats.insert(item.0, (item.1, item.2));
    }

    let mut sorted_keys: Vec<&String> = stats.keys().collect();
    sorted_keys.sort();
    let mut lines: Vec<String> = Vec::with_capacity(sorted_keys.len());
    for rel_path in &sorted_keys {
        let (mtime_ns, size) = stats[*rel_path];
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
            raw_content_sha256 TEXT,
            provenance_json TEXT NOT NULL DEFAULT '{}'
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

/// Walk + stat + fingerprint in a single Rust call, returning `(rel_paths, stats_dict, fp)`.
/// Uses rayon-parallelised stat and avoids re-walking when callers need both paths and fingerprint.
#[pyfunction]
pub fn vault_fingerprint_with_paths(
    py: Python<'_>,
    vault_path: String,
) -> PyResult<(PyObject, PyObject, String)> {
    let vault = PathBuf::from(&vault_path);
    let rel_paths = walk::collect_note_paths(&vault_path)?;
    let (stats, fp) = compute_vault_fingerprint(&vault, &rel_paths)
        .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
    let py_paths = PyList::new_bound(py, &rel_paths);
    let dict = PyDict::new_bound(py);
    for (k, (mt, sz)) in stats {
        let tup = PyTuple::new_bound(py, [mt, sz]);
        dict.set_item(k, tup)?;
    }
    Ok((py_paths.to_object(py), dict.to_object(py), fp))
}

/// Read `(rel_path, mtime_ns, file_size)` from the existing cache for delta comparison.
fn load_cached_stats(conn: &Connection) -> rusqlite::Result<HashMap<String, (i64, i64)>> {
    let mut stmt = conn.prepare("SELECT rel_path, mtime_ns, file_size FROM notes")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    let mut map = HashMap::new();
    for r in rows {
        let (path, mtime, size) = r?;
        map.insert(path, (mtime, size));
    }
    Ok(map)
}

/// Classify notes into new, changed, deleted, and unchanged sets by comparing
/// current disk stats against the cached `(mtime_ns, file_size)` per note.
struct CacheDelta {
    rebuild_paths: Vec<String>,
    delete_paths: Vec<String>,
    unchanged_count: usize,
}

fn compute_cache_delta(
    disk_stats: &HashMap<String, (i64, i64)>,
    cached_stats: &HashMap<String, (i64, i64)>,
) -> CacheDelta {
    let disk_paths: HashSet<&String> = disk_stats.keys().collect();
    let cached_paths: HashSet<&String> = cached_stats.keys().collect();

    let deleted: Vec<String> = cached_paths
        .difference(&disk_paths)
        .map(|p| (*p).clone())
        .collect();

    let mut rebuild = Vec::new();
    let mut unchanged: usize = 0;

    for path in &disk_paths {
        match cached_stats.get(*path) {
            Some(&(cached_mt, cached_sz)) => {
                let &(disk_mt, disk_sz) = disk_stats.get(*path).unwrap();
                if disk_mt != cached_mt || disk_sz != cached_sz {
                    rebuild.push((*path).clone());
                } else {
                    unchanged += 1;
                }
            }
            None => {
                rebuild.push((*path).clone());
            }
        }
    }

    CacheDelta {
        rebuild_paths: rebuild,
        delete_paths: deleted,
        unchanged_count: unchanged,
    }
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
            r#"INSERT OR REPLACE INTO notes (
                rel_path, uid, card_type, slug, mtime_ns, file_size,
                frontmatter_json, frontmatter_hash, body_compressed, content_hash, wikilinks_json,
                raw_content_sha256, provenance_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
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
                row.provenance_json,
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
            r#"INSERT OR REPLACE INTO notes (
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

/// Delete rows for paths that no longer exist on disk.
fn delete_stale_rows(conn: &Connection, paths: &[String]) -> rusqlite::Result<usize> {
    if paths.is_empty() {
        return Ok(0);
    }
    let mut deleted = 0usize;
    for chunk in paths.chunks(500) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "DELETE FROM notes WHERE rel_path IN ({})",
            placeholders.join(",")
        );
        let params: Vec<&dyn rusqlite::types::ToSql> =
            chunk.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
        deleted += conn.execute(&sql, params.as_slice())? as usize;
    }
    Ok(deleted)
}

/// Shared implementation for both full and incremental cache builds.
/// When `incremental` is true, reads existing cached stats and only rebuilds changed/new notes.
/// `cache_version` is passed through from the Python orchestrator (code-fingerprint derived).
fn build_vault_cache_inner(
    vault_path: &str,
    cache_path: &str,
    tier: i32,
    incremental: bool,
    cache_version: &str,
) -> Result<(usize, usize, usize, usize, String), String> {
    let tier_ge2 = tier >= 2;
    let vault = PathBuf::from(vault_path);
    let rel_paths = walk::collect_note_paths(vault_path)
        .map_err(|e| e.to_string())?;
    let (disk_stats, fp) = compute_vault_fingerprint(&vault, &rel_paths)
        .map_err(|_| "fingerprint computation failed".to_string())?;

    if let Some(parent) = Path::new(cache_path).parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let mut conn = Connection::open(cache_path).map_err(|e| e.to_string())?;
    conn.busy_timeout(std::time::Duration::from_secs(120))
        .map_err(|e| e.to_string())?;
    init_db(&conn).map_err(|e| e.to_string())?;

    let (paths_to_build, deleted_count, unchanged_count) = if incremental {
        let cached_stats = load_cached_stats(&conn).map_err(|e| e.to_string())?;
        if cached_stats.is_empty() {
            // No existing rows — fall through to full build
            (rel_paths.clone(), 0usize, 0usize)
        } else {
            let delta = compute_cache_delta(&disk_stats, &cached_stats);
            let del = delete_stale_rows(&conn, &delta.delete_paths)
                .map_err(|e| e.to_string())?;
            (delta.rebuild_paths, del, delta.unchanged_count)
        }
    } else {
        conn.execute("DELETE FROM notes", [])
            .map_err(|e| e.to_string())?;
        (rel_paths.clone(), 0usize, 0usize)
    };

    conn.execute("DELETE FROM cache_meta", [])
        .map_err(|e| e.to_string())?;

    let (built, skipped) = cache_build::build_all_rows(
        &vault,
        &paths_to_build,
        &disk_stats,
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

    let total_notes = unchanged_count + inserted;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    meta_set(&conn, "vault_fingerprint", &fp).map_err(|e| e.to_string())?;
    meta_set(&conn, "tier", &tier.to_string()).map_err(|e| e.to_string())?;
    meta_set(&conn, "cache_version", cache_version)
        .map_err(|e| e.to_string())?;
    meta_set(&conn, "generated_at", &now.to_string()).map_err(|e| e.to_string())?;
    meta_set(&conn, "note_count", &total_notes.to_string())
        .map_err(|e| e.to_string())?;

    Ok((inserted, skipped, deleted_count, unchanged_count, fp))
}

/// Build vault scan cache at `cache_path` (same schema as Python). `tier` 1 = frontmatter-only; `tier` ≥ 2 = full note (body, wikilinks, hashes).
/// Always performs a full rebuild — deletes all existing rows and re-parses every note.
/// `cache_version` is the code-fingerprint string from Python (stored in `cache_meta`).
#[pyfunction]
#[pyo3(signature = (vault_path, cache_path, tier=1, cache_version=None))]
pub fn build_vault_cache(
    py: Python<'_>,
    vault_path: String,
    cache_path: String,
    tier: i32,
    cache_version: Option<String>,
) -> PyResult<PyObject> {
    if tier < 1 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "tier must be >= 1",
        ));
    }
    let vp = vault_path.clone();
    let cp = cache_path.clone();
    let cv = cache_version.unwrap_or_else(|| "1".to_string());
    let (inserted, skipped, _deleted, _unchanged, fp) = py
        .allow_threads(move || build_vault_cache_inner(&vp, &cp, tier, false, &cv))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let out = PyDict::new_bound(py);
    out.set_item("note_count", inserted)?;
    out.set_item("skipped", skipped)?;
    out.set_item("fingerprint", fp)?;
    Ok(out.to_object(py))
}

/// Incremental vault scan cache build. Compares on-disk `(mtime_ns, file_size)` against existing
/// cached rows and only re-parses notes that are new or changed. Deleted notes are purged.
/// `cache_version` is the code-fingerprint string from Python (stored in `cache_meta`).
///
/// Returns a dict with `rebuilt`, `skipped`, `deleted`, `unchanged`, `note_count`, `fingerprint`.
#[pyfunction]
#[pyo3(signature = (vault_path, cache_path, tier=1, cache_version=None))]
pub fn build_vault_cache_incremental(
    py: Python<'_>,
    vault_path: String,
    cache_path: String,
    tier: i32,
    cache_version: Option<String>,
) -> PyResult<PyObject> {
    if tier < 1 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "tier must be >= 1",
        ));
    }
    let vp = vault_path.clone();
    let cp = cache_path.clone();
    let cv = cache_version.unwrap_or_else(|| "1".to_string());
    let (rebuilt, skipped, deleted, unchanged, fp) = py
        .allow_threads(move || build_vault_cache_inner(&vp, &cp, tier, true, &cv))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let out = PyDict::new_bound(py);
    out.set_item("rebuilt", rebuilt)?;
    out.set_item("skipped", skipped)?;
    out.set_item("deleted", deleted)?;
    out.set_item("unchanged", unchanged)?;
    out.set_item("note_count", rebuilt + unchanged)?;
    out.set_item("fingerprint", fp)?;
    Ok(out.to_object(py))
}
