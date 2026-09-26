#[cfg(test)]
mod native_tests {
    use std::sync::Arc;

    use nebula_embed::MockEmbedder;
    use nebula_index::TextIndex;
    use nebula_search::{
        IndexBatch, IndexDefinition, IndexRegistry, NativeBackend, SearchBackend, SearchMode,
        SearchRequest,
    };
    use nebula_search::types::IndexAction;
    use nebula_vector::{HnswConfig, Metric};

    async fn backend() -> NativeBackend {
        let embedder = Arc::new(MockEmbedder::new(8));
        let index = Arc::new(
            TextIndex::new(embedder, Metric::Cosine, HnswConfig::default()).unwrap(),
        );
        NativeBackend::new(index, IndexRegistry::new())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn index_and_hybrid_search() {
        let b = backend().await;
        b.create_index(IndexDefinition::default_for("demo"))
            .await
            .unwrap();
        let batch = IndexBatch {
            value: vec![
                IndexAction {
                    action: "upload".into(),
                    id: "1".into(),
                    content: Some("zero trust networking policy".into()),
                    text: None,
                    extra: Default::default(),
                },
                IndexAction {
                    action: "upload".into(),
                    id: "2".into(),
                    content: Some("payroll tax calendar".into()),
                    text: None,
                    extra: Default::default(),
                },
            ],
        };
        let r = b.index_docs("demo", batch).await.unwrap();
        assert!(r.value.iter().all(|x| x.status));

        let resp = b
            .search(SearchRequest {
                query: "zero trust".into(),
                mode: SearchMode::Hybrid,
                index: Some("demo".into()),
                bucket: Some("demo".into()),
                top: 5,
                skip: 0,
                filter: None,
                facets: vec![],
                orderby: vec![],
                search_fields: vec![],
                vector_queries: vec![],
                semantic_configuration: None,
                ef: None,
                explain: false,
                backend: Default::default(),
                tenant: None,
            })
            .await
            .unwrap();
        assert!(!resp.hits.is_empty());
        assert_eq!(resp.hits[0].id, "1");
    }
}
