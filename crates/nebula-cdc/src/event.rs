//! Typed wire shape for a single change. Translates `WalRecord`'s
//! storage-flavored variants into operation-named events that an
//! external consumer doesn't need to pattern-match.
//!
//! # Why a separate type
//!
//! `WalRecord` is a **storage** type. Its variants are sized for
//! disk efficiency and deliberately carry the resolved-vector bytes
//! so replay is embedder-free. A CDC consumer needs:
//!
//! - **A stable JSON serialization**. `WalRecord` is bincode-only;
//!   webhook subscribers want JSON.
//! - **An operation name**. `UpsertText` vs. `UpsertDocument` is a
//!   storage distinction; CDC consumers think in `Insert`/`Delete`.
//! - **Vector visibility**. Vectors are surfaced as JSON arrays,
//!   not opaque bincode bytes, so a webhook subscriber that wants to
//!   mirror to an external vector store can do so directly.
//!
//! # Cursor included for resumability
//!
//! Every `CdcEvent` carries the [`WalCursor`] of the originating
//! record (the position past it, suitable for handing back to
//! `Wal::subscribe`). Consumers ack via that cursor; an engine
//! restart resumes from the last acked position.

use serde::{Deserialize, Serialize};

use nebula_wal::{WalCursor, WalRecord};

/// One change observed by a CDC consumer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    /// Position of this record in the WAL. Sortable; subscribers ack
    /// by handing the cursor of the latest durably-applied event.
    pub cursor: WalCursor,
    /// Cursor *past* this event — feed back into `Wal::subscribe` to
    /// resume from the next record.
    pub next_cursor: WalCursor,
    /// Operation type, named so external consumers don't pattern-match
    /// on storage internals.
    pub op: CdcOp,
    /// Primary key of the changed entity. Format depends on `op`:
    ///   - `Insert` / `Delete`: `(bucket, external_id)`
    ///   - `InsertDocument` / `DeleteDocument`: `(bucket, doc_id)`
    ///   - `EmptyBucket`: `(bucket, "")`
    pub key: CdcKey,
    /// Body — text, vector, metadata. `None` for delete-shaped ops.
    pub body: Option<CdcBody>,
}

/// Operation taxonomy the consumer sees. A 1:1 mapping onto today's
/// `WalRecord` variants — adding a new `WalRecord` variant later
/// would append a new `CdcOp` variant here (no reorder, ever).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcOp {
    /// Single text upsert. Maps to `WalRecord::UpsertText`.
    Insert,
    /// Chunked-document upsert. Maps to `WalRecord::UpsertDocument`.
    /// Body carries `chunks: [{ index, vector, text, char_start }]`.
    InsertDocument,
    /// Single-key delete. Maps to `WalRecord::Delete`.
    Delete,
    /// Document-level delete; tombstones every chunk. Maps to
    /// `WalRecord::DeleteDocument`.
    DeleteDocument,
    /// Bucket-wide tombstone. Maps to `WalRecord::EmptyBucket`.
    EmptyBucket,
    /// Text upsert accepted without a vector (deferred embedding,
    /// design 0010 §5). Maps to `WalRecord::UpsertTextPending`. The
    /// body carries text + metadata; `vector` is absent. A follow-up
    /// `Insert` for the same key arrives once embedding completes.
    InsertPending,
}

/// Composite key. We carry both `bucket` and the entity id so a
/// consumer can filter on either dimension without parsing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcKey {
    pub bucket: String,
    /// `external_id` for `Insert`/`Delete`; `doc_id` for the
    /// document-shaped ops; empty for `EmptyBucket`.
    pub id: String,
}

/// Body payload. Serialized as JSON for webhook consumers; vectors
/// land as plain arrays so an external vector store can ingest them
/// without a bincode hop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcBody {
    /// Raw text for `Insert`. `None` for document-shaped ops where
    /// the text lives inside `chunks[].text`.
    pub text: Option<String>,
    /// Resolved vector for the `Insert` op. `None` for everything
    /// else.
    pub vector: Option<Vec<f32>>,
    /// Per-chunk payload for `InsertDocument`. One entry per chunk;
    /// each carries text + resolved vector. Empty for non-document
    /// ops.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chunks: Vec<CdcChunk>,
    /// Free-form metadata as a JSON value, decoded from the WAL's
    /// `metadata_json` string. `Null` when none was set.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// Per-chunk subset of a chunked document. Mirrors `WalChunk` but
/// reachable from a JSON consumer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcChunk {
    pub index: usize,
    pub char_start: usize,
    pub text: String,
    pub vector: Vec<f32>,
}

impl CdcEvent {
    /// Translate a raw WAL entry into a CDC event. The cursor pair
    /// is preserved verbatim from the entry; only the body shape is
    /// reorganized.
    pub fn from_wal_entry(entry: nebula_wal::WalEntry) -> Self {
        let nebula_wal::WalEntry {
            cursor,
            next_cursor,
            record,
        } = entry;
        let (op, key, body) = match record {
            WalRecord::UpsertText {
                bucket,
                external_id,
                text,
                vector,
                metadata_json,
            } => (
                CdcOp::Insert,
                CdcKey {
                    bucket,
                    id: external_id,
                },
                Some(CdcBody {
                    text: Some(text),
                    vector: Some(vector),
                    chunks: Vec::new(),
                    metadata: parse_metadata(&metadata_json),
                }),
            ),
            WalRecord::UpsertDocument {
                bucket,
                doc_id,
                chunks,
                metadata_json,
            } => {
                let cdc_chunks = chunks
                    .into_iter()
                    .map(|c| CdcChunk {
                        index: c.index,
                        char_start: c.char_start,
                        text: c.text,
                        vector: c.vector,
                    })
                    .collect();
                (
                    CdcOp::InsertDocument,
                    CdcKey { bucket, id: doc_id },
                    Some(CdcBody {
                        text: None,
                        vector: None,
                        chunks: cdc_chunks,
                        metadata: parse_metadata(&metadata_json),
                    }),
                )
            }
            WalRecord::Delete {
                bucket,
                external_id,
            } => (
                CdcOp::Delete,
                CdcKey {
                    bucket,
                    id: external_id,
                },
                None,
            ),
            WalRecord::DeleteDocument { bucket, doc_id } => {
                (CdcOp::DeleteDocument, CdcKey { bucket, id: doc_id }, None)
            }
            WalRecord::EmptyBucket { bucket } => (
                CdcOp::EmptyBucket,
                CdcKey {
                    bucket,
                    id: String::new(),
                },
                None,
            ),
            WalRecord::UpsertTextPending {
                bucket,
                external_id,
                text,
                metadata_json,
            } => (
                CdcOp::InsertPending,
                CdcKey {
                    bucket,
                    id: external_id,
                },
                Some(CdcBody {
                    text: Some(text),
                    vector: None,
                    chunks: Vec::new(),
                    metadata: parse_metadata(&metadata_json),
                }),
            ),
        };
        Self {
            cursor,
            next_cursor,
            op,
            key,
            body,
        }
    }

    /// Whether the event carries any vector payload. Useful for
    /// vector-store mirroring consumers that want to skip
    /// non-vector events without parsing the body.
    pub fn has_vector(&self) -> bool {
        match &self.body {
            None => false,
            Some(b) => b.vector.is_some() || !b.chunks.is_empty(),
        }
    }
}

fn parse_metadata(s: &str) -> serde_json::Value {
    serde_json::from_str(s).unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebula_wal::{WalChunk, WalCursor, WalEntry};

    fn cur(seg: u64, off: u64) -> WalCursor {
        WalCursor {
            segment_seq: seg,
            byte_offset: off,
        }
    }

    #[test]
    fn upsert_text_round_trips_through_event() {
        let rec = WalRecord::UpsertText {
            bucket: "docs".into(),
            external_id: "d1".into(),
            text: "hello".into(),
            vector: vec![0.1, 0.2, 0.3],
            metadata_json: r#"{"region":"eu"}"#.into(),
        };
        let entry = WalEntry {
            cursor: cur(1, 100),
            next_cursor: cur(1, 200),
            record: rec,
        };
        let ev = CdcEvent::from_wal_entry(entry);
        assert!(matches!(ev.op, CdcOp::Insert));
        assert_eq!(ev.key.bucket, "docs");
        assert_eq!(ev.key.id, "d1");
        let body = ev.body.unwrap();
        assert_eq!(body.text.as_deref(), Some("hello"));
        assert_eq!(body.vector.unwrap(), vec![0.1, 0.2, 0.3]);
        assert_eq!(body.metadata["region"], "eu");
    }

    #[test]
    fn upsert_document_surfaces_chunked_vectors() {
        let rec = WalRecord::UpsertDocument {
            bucket: "docs".into(),
            doc_id: "p1".into(),
            chunks: vec![
                WalChunk {
                    index: 0,
                    char_start: 0,
                    text: "first chunk".into(),
                    vector: vec![1.0, 2.0],
                },
                WalChunk {
                    index: 1,
                    char_start: 100,
                    text: "second chunk".into(),
                    vector: vec![3.0, 4.0],
                },
            ],
            metadata_json: "null".into(),
        };
        let entry = WalEntry {
            cursor: cur(2, 50),
            next_cursor: cur(2, 200),
            record: rec,
        };
        let ev = CdcEvent::from_wal_entry(entry);
        assert!(matches!(ev.op, CdcOp::InsertDocument));
        let body = ev.body.unwrap();
        assert_eq!(body.chunks.len(), 2);
        assert_eq!(body.chunks[0].vector, vec![1.0, 2.0]);
        assert_eq!(body.chunks[1].text, "second chunk");
        assert!(body.text.is_none(), "document body has no top-level text");
    }

    #[test]
    fn delete_has_no_body() {
        let rec = WalRecord::Delete {
            bucket: "docs".into(),
            external_id: "d1".into(),
        };
        let entry = WalEntry {
            cursor: cur(1, 0),
            next_cursor: cur(1, 50),
            record: rec,
        };
        let ev = CdcEvent::from_wal_entry(entry);
        assert!(matches!(ev.op, CdcOp::Delete));
        assert!(ev.body.is_none());
    }

    #[test]
    fn has_vector_only_true_for_vectorful_events() {
        let with_vec = WalRecord::UpsertText {
            bucket: "b".into(),
            external_id: "x".into(),
            text: "t".into(),
            vector: vec![1.0],
            metadata_json: "null".into(),
        };
        let entry = WalEntry {
            cursor: cur(1, 0),
            next_cursor: cur(1, 1),
            record: with_vec,
        };
        assert!(CdcEvent::from_wal_entry(entry).has_vector());

        let del = WalRecord::Delete {
            bucket: "b".into(),
            external_id: "x".into(),
        };
        let entry = WalEntry {
            cursor: cur(1, 0),
            next_cursor: cur(1, 1),
            record: del,
        };
        assert!(!CdcEvent::from_wal_entry(entry).has_vector());
    }

    #[test]
    fn json_serialization_is_stable_shape() {
        // The wire shape is what webhook consumers parse; pin the
        // top-level keys so a reorder doesn't break them silently.
        let ev = CdcEvent::from_wal_entry(WalEntry {
            cursor: cur(1, 0),
            next_cursor: cur(1, 1),
            record: WalRecord::Delete {
                bucket: "b".into(),
                external_id: "x".into(),
            },
        });
        let json = serde_json::to_value(&ev).unwrap();
        assert!(json.get("cursor").is_some());
        assert!(json.get("next_cursor").is_some());
        assert!(json.get("op").is_some());
        assert!(json.get("key").is_some());
        assert_eq!(json["op"], "delete");
    }
}
