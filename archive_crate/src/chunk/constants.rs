//! Parity with `archive_cli.index_config`.

pub const CHUNK_SCHEMA_VERSION: i32 = 6;
pub const DEFAULT_CHUNK_CHAR_LIMIT: i64 = 1200;
pub const DEFAULT_BURST_TOKEN_LIMIT: i64 = 800;
pub const DEFAULT_BURST_CHAT_GAP_SECONDS: i64 = 300;
pub const BURST_ALGORITHM_VERSION: &str = "p01b1-burst-1";
pub const BURST_CHUNK_TYPE: &str = "conversation_burst";
