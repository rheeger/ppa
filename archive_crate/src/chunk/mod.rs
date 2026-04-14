//! Native chunk pipeline (Step 9) — parity with `archive_cli.chunk_builders` / `chunking`.

mod accumulator;
mod builders;
mod config;
mod constants;
mod dispatch;
mod fm;
pub(crate) mod helpers;

pub use dispatch::{build_chunks, chunk_records_to_py_list};
