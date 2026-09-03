use std::path::Path;

use pyo3::prelude::*;
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, QueryParser, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, STORED, STRING, TEXT, Value,
};
use tantivy::{doc, Index, IndexReader, ReloadPolicy, TantivyDocument, Term};

use super::metadata::CardMeta;

pub struct LexicalIndex {
    inner: Index,
    reader: IndexReader,
    fields: LexFields,
}

struct LexFields {
    card_uid: Field,
    search_text: Field,
    slug: Field,
    summary: Field,
    card_type: Field,
}

impl LexicalIndex {
    fn schema() -> (Schema, LexFields) {
        let mut b = Schema::builder();
        let text_opts = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        );
        let fields = LexFields {
            card_uid: b.add_text_field("card_uid", STRING | STORED),
            search_text: b.add_text_field("search_text", text_opts.clone() | TEXT),
            slug: b.add_text_field("slug", STRING | STORED),
            summary: b.add_text_field("summary", text_opts | STORED),
            card_type: b.add_text_field("card_type", STRING | STORED),
        };
        (b.build(), fields)
    }

    pub fn build(dir: &Path, cards: &[CardMeta]) -> PyResult<()> {
        let (schema, fields) = Self::schema();
        if dir.exists() {
            std::fs::remove_dir_all(dir)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        std::fs::create_dir_all(dir)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let index = Index::create_in_dir(dir, schema)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let mut writer = index
            .writer(512_000_000)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        for card in cards {
            let text = if card.search_text.is_empty() {
                format!("{} {}", card.summary, card.slug)
            } else {
                card.search_text.clone()
            };
            writer
                .add_document(doc!(
                    fields.card_uid => card.card_uid.as_str(),
                    fields.search_text => text.as_str(),
                    fields.slug => card.slug.as_str(),
                    fields.summary => card.summary.as_str(),
                    fields.card_type => card.r#type.as_str(),
                ))
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        }
        writer
            .commit()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    pub fn open(dir: &Path) -> PyResult<Self> {
        let inner = Index::open_in_dir(dir)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("open tantivy: {e}")))?;
        let reader = inner
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let (_schema, fields) = Self::schema();
        Ok(Self {
            inner,
            reader,
            fields,
        })
    }

    pub fn search(&self, query: &str, limit: usize, type_filter: &str) -> PyResult<Vec<(String, f32)>> {
        if query.trim().is_empty() || limit == 0 {
            return Ok(vec![]);
        }
        let searcher = self.reader.searcher();
        let parser = QueryParser::for_index(&self.inner, vec![self.fields.search_text, self.fields.summary]);
        let parsed = parser
            .parse_query(query)
            .or_else(|_| parser.parse_query(&format!("\"{query}\"")))
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let boxed: Box<dyn tantivy::query::Query> = if type_filter.is_empty() {
            parsed
        } else {
            let term = Term::from_field_text(self.fields.card_type, type_filter);
            Box::new(BooleanQuery::new(vec![
                (Occur::Must, parsed),
                (
                    Occur::Must,
                    Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
                ),
            ]))
        };
        let hits = searcher
            .search(&boxed, &TopDocs::with_limit(limit))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let mut out = Vec::new();
        for (score, addr) in hits {
            let doc: TantivyDocument = searcher
                .doc(addr)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            if let Some(uid) = doc.get_first(self.fields.card_uid).and_then(|v| v.as_str()) {
                out.push((uid.to_string(), score));
            }
        }
        Ok(out)
    }
}
