//! Parity with `archive_cli.index_config.get_chunk_char_limit` / `_ppa_env_int`.

use crate::chunk::constants::DEFAULT_CHUNK_CHAR_LIMIT;

pub fn env_positive(name: &str, default: i64) -> usize {
    let raw = std::env::var(name).unwrap_or_default();
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

pub fn get_chunk_char_limit() -> usize {
    env_positive("PPA_CHUNK_CHAR_LIMIT", DEFAULT_CHUNK_CHAR_LIMIT)
}

pub fn get_burst_token_limit() -> usize {
    env_positive(
        "PPA_BURST_TOKEN_LIMIT",
        crate::chunk::constants::DEFAULT_BURST_TOKEN_LIMIT,
    )
}

pub fn get_burst_chat_gap_seconds() -> i64 {
    let default = crate::chunk::constants::DEFAULT_BURST_CHAT_GAP_SECONDS;
    let raw = std::env::var("PPA_BURST_CHAT_GAP_SECONDS").unwrap_or_default();
    let trimmed = raw.trim();
    let v = if trimmed.is_empty() {
        default
    } else {
        trimmed.parse::<i64>().unwrap_or(default)
    };
    if v > 0 {
        v
    } else {
        default
    }
}
