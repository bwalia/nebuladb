//! Change Data Capture consumers for NebulaDB.
//!
//! Implements the "vector-aware CDC consumer" gap from RFC 0005's
//! status table: subscribers that tail the existing WAL via
//! [`nebula_wal::Wal::subscribe`], translate raw `WalRecord`s into a
//! typed [`CdcEvent`], filter by user-supplied predicates, and hand
//! them to a pluggable [`CdcConsumer`] for side effects (webhook
//! POSTs, file logging, future Kafka sinks).
//!
//! # What this crate is and isn't
//!
//! - **Is**: a small fan-out layer that turns `Wal::subscribe`
//!   (already shipping) into "do something with every committed
//!   write." Two concrete consumer impls are included: webhook
//!   (HTTP POST) and file-logger (JSONL).
//! - **Isn't**: a re-implementation of the WAL, the subscriber hub,
//!   or the gRPC tail. Those are reused as-is from `nebula-wal` and
//!   `nebula-grpc` (RFC 0005 §1).
//! - **Isn't yet**: a persistent checkpoint store. Today's
//!   [`CdcEngine`] tracks the last-acked cursor in memory only, so a
//!   process restart re-tails from the cursor the caller passes in.
//!   RFC 0005 §5.3 covers persistence as a follow-up.
//!
//! # Vector-awareness
//!
//! `WalRecord::UpsertDocument` carries the chunked text **plus
//! resolved vectors** (`Vec<WalChunk>` where each `WalChunk.vector`
//! is the embedded form). Consumers receive these vectors directly —
//! no re-embedding required, no extra round-trip to the embedder.
//! That's what makes the consumer "vector-aware": it can mirror
//! embeddings into an external store, fire a webhook with the vector
//! payload, or build an external HNSW from the same bytes the leader
//! committed.
//!
//! # Backpressure model
//!
//! Each consumer runs on its own `tokio::spawn`'d task. A slow
//! consumer slows itself down via the `WalSubscriber`'s broadcast
//! channel — past `BROADCAST_CAPACITY` (1024 entries) the receiver
//! returns `Lagged` and the consumer reconnects from its last-acked
//! cursor. This means a misbehaving webhook can't wedge the leader's
//! write path; it can only stall its own apply.

pub mod consumer;
pub mod engine;
pub mod event;
pub mod filter;

pub use consumer::{CdcConsumer, ConsumerError, FileLogConsumer, WebhookConsumer};
pub use engine::{CdcEngine, EngineError, EngineHandle};
pub use event::{CdcEvent, CdcOp};
pub use filter::CdcFilter;
