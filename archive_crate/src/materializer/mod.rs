//! Native row materialization (Step 8) — parity with `archive_cli.materializer`.

mod activity;
pub(crate) mod batch;
pub(crate) mod body;
pub(crate) mod card_fields;
pub(crate) mod copy_buffer;
mod edges;
mod external_ids;
pub(crate) mod fm_value;
mod projection;
mod pyutil;
mod quality;
mod registry;
mod text_hash;
mod time_parse;

pub use batch::{materialize_all_rows, materialize_row_batch};
pub use copy_buffer::CopyBuffer;
pub use text_hash::{build_search_text, content_hash as materialize_content_hash};
