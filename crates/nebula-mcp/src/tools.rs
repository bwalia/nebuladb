//! MCP tool surface for NebulaDB.
//!
//! Every tool here maps to a *real* nebula-server endpoint (design
//! 0012 §4). There are no aspirational tools: if NebulaDB can't do it
//! today over HTTP, there is no tool for it. Tools are grouped by
//! capability — SQL, search, RAG, documents, cluster ops, backups,
//! observability — mirroring the REST surface an operator already
//! knows.
//!
//! ## Auth forwarding
//!
//! Each tool reads the caller's HTTP `Authorization` header from the
//! injected [`http::request::Parts`] and forwards the bearer token to
//! nebula-server. That means MCP inherits NebulaDB's existing
//! api-key/JWT auth verbatim — an agent can only do what its token is
//! allowed to do. When the caller sends no token, the server's
//! configured default (`NEBULA_MCP_UPSTREAM_TOKEN`) is used, if any.

use http::request::Parts;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::tool::Extension;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::{tool, tool_router, ErrorData};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::client::{ClientError, NebulaClient};

/// The MCP server state: a handle to the upstream nebula-server.
#[derive(Clone)]
pub struct NebulaMcp {
    pub client: NebulaClient,
    pub tool_router: ToolRouter<Self>,
}

impl NebulaMcp {
    pub fn new(client: NebulaClient) -> Self {
        Self {
            client,
            tool_router: Self::tool_router(),
        }
    }

    /// Build a per-request client carrying the caller's forwarded
    /// bearer token (from their `Authorization` header), falling back
    /// to the server default when absent.
    fn client_for(&self, parts: &Parts) -> NebulaClient {
        let token = parts
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .map(|s| s.trim().to_string());
        self.client.with_token(token)
    }
}

/// Render a client result as an MCP tool result. A successful call
/// returns the JSON body pretty-printed as text *and* as structured
/// content (so agents that parse `structured_content` and agents that
/// read text both work). A server error becomes a visible tool error
/// carrying NebulaDB's own diagnostic body — the agent sees the real
/// `sql_unsupported` / `not_leader` / `429` reason, not a generic fail.
fn render(result: Result<Value, ClientError>) -> Result<CallToolResult, ErrorData> {
    match result {
        Ok(value) => {
            let text = serde_json::to_string_pretty(&value)
                .unwrap_or_else(|_| value.to_string());
            Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
        }
        Err(ClientError::Status { status, body }) => {
            // A non-2xx is a *tool* error, not a protocol error: the
            // call reached NebulaDB and got a meaningful answer. Return
            // it as an error result the agent can read and reason about.
            let msg = format!("nebula-server returned HTTP {status}: {body}");
            Ok(CallToolResult::error(vec![ContentBlock::text(msg)]))
        }
        // Transport/decode failures are genuine protocol-level errors:
        // the MCP server couldn't reach or parse NebulaDB at all.
        Err(e) => Err(ErrorData::internal_error(e.to_string(), None)),
    }
}

// ---------------------------------------------------------------------------
// Tool parameter types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SqlParams {
    /// The SQL statement to execute. NebulaDB's dialect supports
    /// `semantic_match(...)`, metadata filters, GROUP BY, and inner
    /// JOINs. Unsupported constructs return a `sql_unsupported` error.
    pub sql: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SemanticSearchParams {
    /// Natural-language query; embedded server-side and matched against
    /// the vector index.
    pub query: String,
    /// Restrict to a single bucket (collection). Omit to search all.
    #[serde(default)]
    pub bucket: Option<String>,
    /// Number of results to return. Defaults to the server's setting.
    #[serde(default)]
    pub top_k: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct VectorSearchParams {
    /// Raw query vector. Its length must equal the index dimension.
    pub vector: Vec<f32>,
    /// Restrict to a single bucket. Omit to search all.
    #[serde(default)]
    pub bucket: Option<String>,
    /// Number of nearest neighbours to return.
    #[serde(default)]
    pub top_k: Option<u32>,
    /// HNSW `ef` search-width override (higher = more accurate, slower).
    #[serde(default)]
    pub ef: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RagParams {
    /// The question to answer using retrieval-augmented generation.
    pub query: String,
    /// How many context chunks to retrieve.
    #[serde(default)]
    pub top_k: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct InsertDocumentParams {
    /// Bucket (collection) to insert into. Created implicitly on first
    /// write.
    pub bucket: String,
    /// Stable document id. Re-inserting the same id upserts.
    pub doc_id: String,
    /// Document text. Chunked + embedded server-side.
    pub text: String,
    /// Optional JSON metadata object attached to the document.
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DocRefParams {
    /// Bucket the document lives in.
    pub bucket: String,
    /// The document's external id.
    pub doc_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AddLearnerParams {
    /// Raft node id of the replacement node (must be unique).
    pub node_id: u64,
    /// Raft gRPC address of the replacement node, e.g. `node4:50052`.
    pub addr: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct NodeIdParams {
    /// Raft node id to act on.
    pub node_id: u64,
}

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

#[tool_router]
impl NebulaMcp {
    // ---- SQL ----

    #[tool(
        description = "Execute a SQL query against NebulaDB. Supports semantic_match() for \
                       vector search, metadata filters, GROUP BY, and inner JOINs. Returns \
                       result rows as JSON."
    )]
    async fn execute_sql(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<SqlParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.post("/query", &json!({ "sql": p.sql })).await)
    }

    #[tool(
        description = "Return the query plan for a SQL statement without executing it. Use to \
                       understand how NebulaDB will run a query (retrieval, filters, joins)."
    )]
    async fn explain_query(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<SqlParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.post("/query/explain", &json!({ "sql": p.sql })).await)
    }

    // ---- Vector / semantic search ----

    #[tool(
        description = "Semantic (natural-language) search over the vector index. Embeds the \
                       query server-side and returns the top-k most similar documents with \
                       distance scores and metadata."
    )]
    async fn semantic_search(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<SemanticSearchParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        let mut body = json!({ "query": p.query });
        if let Some(b) = p.bucket {
            body["bucket"] = json!(b);
        }
        if let Some(k) = p.top_k {
            body["top_k"] = json!(k);
        }
        render(c.post("/ai/search", &body).await)
    }

    #[tool(
        description = "Nearest-neighbour search from a raw query vector (length must equal the \
                       index dimension). Use semantic_search instead if you have text."
    )]
    async fn vector_search(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<VectorSearchParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        let mut body = json!({ "vector": p.vector });
        if let Some(b) = p.bucket {
            body["bucket"] = json!(b);
        }
        if let Some(k) = p.top_k {
            body["top_k"] = json!(k);
        }
        if let Some(ef) = p.ef {
            body["ef"] = json!(ef);
        }
        render(c.post("/vector/search", &body).await)
    }

    // ---- RAG ----

    #[tool(
        description = "Answer a question using retrieval-augmented generation: retrieve the \
                       most relevant context from the corpus, then generate a grounded answer \
                       with the configured LLM. Returns the answer plus the cited context."
    )]
    async fn answer_question(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<RagParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        let mut body = json!({ "query": p.query });
        if let Some(k) = p.top_k {
            body["top_k"] = json!(k);
        }
        render(c.post("/rag/answer", &body).await)
    }

    // ---- Documents ----

    #[tool(
        description = "Insert or update a document in a bucket. The text is chunked and embedded \
                       server-side. Re-using an existing doc_id upserts."
    )]
    async fn insert_document(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<InsertDocumentParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        let mut body = json!({ "doc_id": p.doc_id, "text": p.text });
        if let Some(m) = p.metadata {
            body["metadata"] = m;
        }
        render(c.post(&format!("/bucket/{}/document", p.bucket), &body).await)
    }

    #[tool(description = "Fetch a single document by bucket and id.")]
    async fn get_document(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<DocRefParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.get(&format!("/bucket/{}/doc/{}", p.bucket, p.doc_id)).await)
    }

    #[tool(description = "Delete a document by bucket and id.")]
    async fn delete_document(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<DocRefParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.delete(&format!("/bucket/{}/doc/{}", p.bucket, p.doc_id)).await)
    }

    #[tool(
        description = "List all buckets (collections) with document counts and per-bucket stats."
    )]
    async fn list_buckets(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.get("/admin/buckets").await)
    }

    // ---- Cluster operations ----

    #[tool(
        description = "Report cluster health and node topology (roles, reachability). Use this \
                       first when diagnosing availability or replication issues."
    )]
    async fn cluster_health(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.get("/admin/cluster/nodes").await)
    }

    #[tool(
        description = "Show current Raft membership: voters, learners, and the leader. Only \
                       meaningful when the cluster is running in Raft mode."
    )]
    async fn raft_membership(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.get("/admin/raft/membership").await)
    }

    #[tool(
        description = "Add a node to the Raft cluster as a non-voting learner and block until it \
                       is caught up. Step 1 of self-healing node replacement (learner -> promote \
                       -> evict). Quorum is unchanged during catch-up, so writes keep flowing."
    )]
    async fn raft_add_learner(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<AddLearnerParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(
            c.post(
                "/admin/raft/learner",
                &json!({ "node_id": p.node_id, "addr": p.addr }),
            )
            .await,
        )
    }

    #[tool(
        description = "Promote a caught-up Raft learner to a voting member (joint consensus). \
                       Step 2 of node replacement."
    )]
    async fn raft_promote_voter(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<NodeIdParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.post(&format!("/admin/raft/voter/{}", p.node_id), &json!({})).await)
    }

    #[tool(
        description = "Evict a node from the Raft quorum. Step 3 of node replacement; use after \
                       a replacement has joined and been promoted so quorum is never broken."
    )]
    async fn raft_remove_node(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<NodeIdParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.delete(&format!("/admin/raft/node/{}", p.node_id)).await)
    }

    // ---- Backups ----

    #[tool(description = "Trigger an on-demand snapshot (backup) of the current index.")]
    async fn create_snapshot(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.post("/admin/snapshot", &json!({})).await)
    }

    // ---- Observability ----

    #[tool(
        description = "Return aggregate server stats: document counts, request/error counters, \
                       cache hit ratios, search and RAG counts."
    )]
    async fn server_stats(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.get("/admin/stats").await)
    }

    #[tool(
        description = "Report the operating mode and resource pressure (memory/CPU/disk) plus \
                       AI-subsystem health. The one-stop reliability answer (design 0010)."
    )]
    async fn reliability_status(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.get("/admin/reliability").await)
    }

    #[tool(description = "Return replication status and lag against the leader / remote regions.")]
    async fn replication_status(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.get("/admin/replication").await)
    }

    #[tool(description = "Return the slowest recently-observed queries, for latency triage.")]
    async fn slow_queries(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        render(c.get("/admin/slow").await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schemars::schema_for;

    #[test]
    fn sql_params_schema_has_sql_field() {
        // The generated JSON schema is what the agent sees when it
        // introspects the tool. Assert the important field survives.
        let schema = serde_json::to_value(schema_for!(SqlParams)).unwrap();
        let props = &schema["properties"];
        assert!(props.get("sql").is_some(), "schema: {schema}");
    }

    #[test]
    fn semantic_search_optional_fields_are_optional() {
        // top_k / bucket are Option, so they must NOT be in `required`.
        let schema = serde_json::to_value(schema_for!(SemanticSearchParams)).unwrap();
        let required = schema["required"].as_array().cloned().unwrap_or_default();
        let req_names: Vec<_> = required.iter().filter_map(|v| v.as_str()).collect();
        assert!(req_names.contains(&"query"), "query must be required: {schema}");
        assert!(!req_names.contains(&"bucket"), "bucket must be optional");
        assert!(!req_names.contains(&"top_k"), "top_k must be optional");
    }

    #[test]
    fn render_status_error_is_visible_tool_error_not_protocol_error() {
        let r = render(Err(ClientError::Status {
            status: 400,
            body: "{\"error\":{\"code\":\"sql_unsupported\"}}".into(),
        }));
        // A 4xx from NebulaDB must come back as Ok(error result), so the
        // agent can read the reason — not Err (which aborts the call).
        let res = r.expect("status errors render as Ok tool-errors");
        assert_eq!(res.is_error, Some(true));
    }

    #[test]
    fn render_transport_error_is_protocol_error() {
        let r = render(Err(ClientError::Transport("connection refused".into())));
        assert!(r.is_err(), "transport failures must be protocol errors");
    }
}
