//! Live Azure AI Search smoke test — ignored unless credentials present.
//!
//! ```
//! NEBULA_AZURE_SEARCH_ENDPOINT=... NEBULA_AZURE_SEARCH_API_KEY=... \
//!   cargo test -p nebula-search --test azure_live -- --ignored
//! ```

use nebula_search::{AzureSearchBackend, AzureSearchConfig, SearchBackend};

#[tokio::test]
#[ignore = "requires live Azure AI Search credentials"]
async fn azure_live_list_indexes() {
    let cfg = AzureSearchConfig::from_env().expect("set NEBULA_AZURE_SEARCH_ENDPOINT + API_KEY");
    let backend = AzureSearchBackend::new(cfg);
    let indexes = backend.list_indexes().await.expect("list indexes");
    let _ = indexes.len();
}
