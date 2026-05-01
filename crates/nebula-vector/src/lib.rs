//! Vector engine for NebulaDB.
//!
//! Provides:
//! - [`distance`]: distance metrics and a `Metric` enum.
//! - [`hnsw`]: a Hierarchical Navigable Small World graph index.
//!
//! The index is generic over the distance metric and stores vectors
//! densely in a single arena, with the graph represented as per-node
//! per-layer neighbor lists protected by fine-grained locks.

pub mod distance;
pub mod hnsw;

pub use distance::{Metric, dot, euclidean_sq, cosine_distance};
pub use hnsw::{Hnsw, HnswConfig, HnswSnapshot, SearchResult};
