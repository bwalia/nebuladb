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
            let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
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

/// Percent-encode one URL path segment. Document ids are caller-chosen
/// and routinely carry `:` / `#` / `/` (`u-42:turn-7`, chunk ids
/// `doc#0`); interpolated raw, `#` truncates the URL into a fragment
/// and `/` changes the route. Everything outside RFC 3986 unreserved
/// is escaped.
fn seg(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Upper bound on chunks `get_document` will reassemble. A document
/// larger than this is still readable chunk-by-chunk via search; the
/// response says it was truncated.
const MAX_GET_CHUNKS: usize = 64;

/// Candidate pool for `recall`. Bucket and metadata filters both run
/// *after* ANN (nebula-index `search_vector`), so recall overfetches
/// and filters by `user_id` here. 100 is nebula-server's default
/// `max_top_k`; asking for more is a 400.
const RECALL_POOL: u32 = 100;
const RECALL_EF: u32 = 256;

/// Default bucket for the agent-memory tools.
pub const MEMORY_BUCKET: &str = "agent_memory";

/// Keep the hits whose `metadata.user_id` equals `user_id`, in rank
/// order, up to `limit`.
fn filter_hits_by_user(hits: &[Value], user_id: &str, limit: usize) -> Vec<Value> {
    hits.iter()
        .filter(|h| h["metadata"]["user_id"].as_str() == Some(user_id))
        .take(limit)
        .cloned()
        .collect()
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
    /// Fuse BM25 keyword scores with vector similarity. Better for
    /// queries containing exact names, codes, or rare terms.
    #[serde(default)]
    pub hybrid: Option<bool>,
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
    /// Ground the answer only in this bucket. Omit to use all buckets.
    #[serde(default)]
    pub bucket: Option<String>,
    /// Use hybrid (BM25 + vector) retrieval for grounding.
    #[serde(default)]
    pub hybrid: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RememberParams {
    /// The fact, preference, or event to remember, written as a
    /// self-contained sentence ("User prefers TypeScript over JS").
    pub text: String,
    /// Whose memory this is. Recall is scoped to this id.
    pub user_id: String,
    /// Optional category, e.g. `preference`, `fact`, `task`, `episode`.
    #[serde(default)]
    pub kind: Option<String>,
    /// Memory bucket. Defaults to `agent_memory`.
    #[serde(default)]
    pub bucket: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RecallParams {
    /// What to recall, in natural language ("editor preferences").
    pub query: String,
    /// Whose memories to search.
    pub user_id: String,
    /// Maximum memories to return (default 5).
    #[serde(default)]
    pub limit: Option<u32>,
    /// Memory bucket. Defaults to `agent_memory`.
    #[serde(default)]
    pub bucket: Option<String>,
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
        if let Some(h) = p.hybrid {
            body["hybrid"] = json!(h);
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
        if let Some(b) = p.bucket {
            body["bucket"] = json!(b);
        }
        if let Some(h) = p.hybrid {
            body["hybrid"] = json!(h);
        }
        render(c.post("/rag/answer", &body).await)
    }

    // ---- Agent memory ----

    #[tool(
        description = "Store a long-term memory for a user (a preference, fact, or event) so it \
                       can be recalled in later sessions. Write `text` as a self-contained \
                       sentence. Memories are durable and survive restarts."
    )]
    async fn remember(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<RememberParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        let bucket = p.bucket.unwrap_or_else(|| MEMORY_BUCKET.to_string());
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_default();
        // Time-ordered, per-user id: distinct memories never collide and
        // an operator can eyeball a user's history by id prefix.
        let doc_id = format!("{}:{ts}", p.user_id);
        let body = json!({
            "doc_id": doc_id,
            "text": p.text,
            "metadata": {
                "user_id": p.user_id,
                "kind": p.kind.unwrap_or_else(|| "fact".to_string()),
                "ts": ts as u64,
            },
        });
        render(
            c.post(&format!("/bucket/{}/document", seg(&bucket)), &body)
                .await,
        )
    }

    #[tool(
        description = "Recall a user's long-term memories most relevant to a query. Returns only \
                       that user's memories, best match first. Call this at the start of a task \
                       to personalise it."
    )]
    async fn recall(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<RecallParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        let bucket = p.bucket.unwrap_or_else(|| MEMORY_BUCKET.to_string());
        let limit = p.limit.unwrap_or(5).clamp(1, RECALL_POOL) as usize;
        let body = json!({
            "query": p.query,
            "bucket": bucket,
            "top_k": RECALL_POOL,
            "ef": RECALL_EF,
        });
        let resp = match c.post("/ai/search", &body).await {
            Ok(v) => v,
            Err(e) => return render(Err(e)),
        };
        let hits = resp["hits"].as_array().cloned().unwrap_or_default();
        let memories = filter_hits_by_user(&hits, &p.user_id, limit);
        render(Ok(json!({
            "user_id": p.user_id,
            "memories": memories,
            "candidates_scanned": hits.len(),
        })))
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
        render(
            c.post(&format!("/bucket/{}/document", seg(&p.bucket)), &body)
                .await,
        )
    }

    #[tool(
        description = "Fetch a document by bucket and id. Works for both single-row documents \
                       and chunked ones written by insert_document (chunks are returned in order)."
    )]
    async fn get_document(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<DocRefParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        let bucket = seg(&p.bucket);
        match c
            .get(&format!("/bucket/{bucket}/doc/{}", seg(&p.doc_id)))
            .await
        {
            Err(ClientError::Status { status: 404, .. }) => {}
            other => return render(other),
        }
        // insert_document stores `{doc_id}#{i}` rows, never the bare id.
        // Walk the chunks until the first gap.
        let mut chunks = Vec::new();
        let mut metadata = Value::Null;
        for i in 0..MAX_GET_CHUNKS {
            let id = format!("{}#{i}", p.doc_id);
            match c.get(&format!("/bucket/{bucket}/doc/{}", seg(&id))).await {
                Ok(v) => {
                    if metadata.is_null() {
                        metadata = v["metadata"].clone();
                    }
                    chunks.push(json!({ "chunk": i, "text": v["text"] }));
                }
                Err(ClientError::Status { status: 404, .. }) => break,
                Err(e) => return render(Err(e)),
            }
        }
        if chunks.is_empty() {
            return render(Err(ClientError::Status {
                status: 404,
                body: format!("no document {}/{}", p.bucket, p.doc_id),
            }));
        }
        let truncated = chunks.len() == MAX_GET_CHUNKS;
        render(Ok(json!({
            "bucket": p.bucket,
            "doc_id": p.doc_id,
            "metadata": metadata,
            "chunks": chunks,
            "truncated": truncated,
        })))
    }

    #[tool(description = "Delete a document (and all of its chunks) by bucket and id.")]
    async fn delete_document(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<DocRefParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = self.client_for(&parts);
        let (bucket, id) = (seg(&p.bucket), seg(&p.doc_id));
        // The chunk-aware route removes `{doc_id}#*`. It reports
        // chunks_removed = 0 for a single-row doc (written via the
        // plain /doc route), so fall back to that route in that case.
        match c.delete(&format!("/bucket/{bucket}/document/{id}")).await {
            Ok(v) if v["chunks_removed"].as_u64() == Some(0) => {
                render(c.delete(&format!("/bucket/{bucket}/doc/{id}")).await.map(|_| {
                    json!({ "bucket": p.bucket, "doc_id": p.doc_id, "chunks_removed": 0, "deleted": true })
                }))
            }
            other => render(other),
        }
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
        assert!(
            req_names.contains(&"query"),
            "query must be required: {schema}"
        );
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
    fn seg_escapes_route_breaking_characters() {
        assert_eq!(seg("u-42:turn-7"), "u-42%3Aturn-7");
        assert_eq!(seg("doc#0"), "doc%230");
        assert_eq!(seg("a/b c"), "a%2Fb%20c");
        assert_eq!(seg("plain_id.v2~x"), "plain_id.v2~x");
    }

    #[test]
    fn recall_filter_keeps_only_that_users_hits_in_rank_order() {
        let hits = vec![
            json!({"id": "a", "metadata": {"user_id": "u-7"}}),
            json!({"id": "b", "metadata": {"user_id": "u-42"}}),
            json!({"id": "c", "metadata": {}}),
            json!({"id": "d", "metadata": {"user_id": "u-42"}}),
            json!({"id": "e", "metadata": {"user_id": "u-42"}}),
        ];
        let got: Vec<_> = filter_hits_by_user(&hits, "u-42", 2)
            .iter()
            .map(|h| h["id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(got, vec!["b", "d"]);
    }

    #[test]
    fn remember_and_recall_require_user_id() {
        for schema in [schema_for!(RememberParams), schema_for!(RecallParams)] {
            let v = serde_json::to_value(schema).unwrap();
            let req: Vec<_> = v["required"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|x| x.as_str())
                .collect();
            assert!(req.contains(&"user_id"), "user_id must be required: {v}");
        }
    }

    #[test]
    fn render_transport_error_is_protocol_error() {
        let r = render(Err(ClientError::Transport("connection refused".into())));
        assert!(r.is_err(), "transport failures must be protocol errors");
    }
}
