//! Parity with `archive_cli.index_config.get_chunk_char_limit` / `_ppa_env_int`.

use crate::chunk::constants::DEFAULT_CHUNK_CHAR_LIMIT;

pub fn get_chunk_char_limit() -> usize {
    let default = DEFAULT_CHUNK_CHAR_LIMIT;
    let raw = std::env::var("PPA_CHUNK_CHAR_LIMIT").unwrap_or_default();
    let trimmed = raw.trim();
    let v = if trimmed.is_empty() {
        default
    } else {
        trimmed.parse::<i64>().unwrap_or(default)
    };
    if v > 0 {
        v as usize
    } else {
        default as usize
    }
}
