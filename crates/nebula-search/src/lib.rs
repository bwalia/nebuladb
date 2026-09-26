//! Unified enterprise search + Azure AI Search compatibility layer.
//!
//! See `docs/design/0013-azure-ai-search-compat.md` and
//! `docs/compat/registry.yaml`.

pub mod azure;
pub mod backend;
pub mod compat;
pub mod filter;
pub mod native;
pub mod types;

pub use azure::{
    AzureSearchBackend, AzureSearchClient, AzureSearchConfig, DualSearchBackend,
};
pub use backend::{IndexRegistry, SearchBackend, SearchError};
pub use compat::{
    embedded_registry, CompatCapability, CompatProduct, CompatRegistry, CompatStatus,
};
pub use filter::{parse_filter, FilterError, FilterExpr, FilterPred};
pub use native::{NativeBackend, WeightFn};
pub use types::*;
