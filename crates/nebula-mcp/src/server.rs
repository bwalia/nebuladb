//! `ServerHandler` implementation: tools + resources + prompts.
//!
//! We implement `ServerHandler` by hand (rather than `#[tool_handler]`)
//! so `get_info` can advertise all three capabilities — tools,
//! resources, and prompts — and so resource/prompt handling lives
//! alongside the tool delegation. Tool dispatch is delegated to the
//! `ToolRouter` exactly as the macro would.
//!
//! **Resources** are read-only live views of cluster state (buckets,
//! topology, reliability, stats). An agent can `read_resource` to load
//! context without a tool call. **Prompts** are reusable operator
//! playbooks (diagnose cluster health, explain slow queries, recommend
//! indexes) that stitch several tools into a workflow.

use rmcp::handler::server::tool::ToolCallContext;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, GetPromptRequestParams, GetPromptResult,
    ListPromptsResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams, Prompt,
    PromptMessage, ReadResourceRequestParams, ReadResourceResult, Resource, ResourceContents,
    Role, ServerCapabilities, ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler};

use crate::tools::NebulaMcp;

/// URI scheme for NebulaDB MCP resources.
const RES_BUCKETS: &str = "nebula://buckets";
const RES_TOPOLOGY: &str = "nebula://cluster/topology";
const RES_RELIABILITY: &str = "nebula://cluster/reliability";
const RES_STATS: &str = "nebula://stats";

impl ServerHandler for NebulaMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
        )
        .with_instructions(
            "NebulaDB MCP server. Query and operate an AI-native database: SQL, semantic and \
             vector search, RAG, document CRUD, cluster operations (Raft membership, backups), \
             and observability. Tools forward your Authorization bearer token to NebulaDB, so \
             you can only do what your token permits. Start with the `reliability_status` or \
             `cluster_health` tools, or the `diagnose_cluster_health` prompt, when triaging.",
        )
    }

    // ---- tools: delegate to the ToolRouter (same as #[tool_handler]) ----

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let tcc = ToolCallContext::new(self, request, context);
        self.tool_router.call(tcc).await
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            tools: self.tool_router.list_all(),
            meta: None,
            next_cursor: None,
        })
    }

    // ---- resources: read-only live views of cluster state ----

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        Ok(ListResourcesResult::with_all_items(vec![
            Resource::new(RES_BUCKETS, "buckets")
                .with_description("All buckets (collections) with document counts")
                .with_mime_type("application/json"),
            Resource::new(RES_TOPOLOGY, "cluster-topology")
                .with_description("Cluster nodes, roles, and reachability")
                .with_mime_type("application/json"),
            Resource::new(RES_RELIABILITY, "reliability")
                .with_description("Operating mode + resource pressure + AI health (design 0010)")
                .with_mime_type("application/json"),
            Resource::new(RES_STATS, "stats")
                .with_description("Aggregate server stats and counters")
                .with_mime_type("application/json"),
        ]))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        // Resources have no request body to carry a bearer, so forward
        // the token from the HTTP parts in the request extensions if
        // present; otherwise the client's default token is used.
        let client = self.client_from_ctx(&context);
        let path = match request.uri.as_str() {
            RES_BUCKETS => "/admin/buckets",
            RES_TOPOLOGY => "/admin/cluster/nodes",
            RES_RELIABILITY => "/admin/reliability",
            RES_STATS => "/admin/stats",
            other => {
                return Err(ErrorData::resource_not_found(
                    format!("unknown resource: {other}"),
                    None,
                ))
            }
        };
        match client.get(path).await {
            Ok(value) => {
                let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
                Ok(ReadResourceResult::new(vec![ResourceContents::text(
                    text,
                    request.uri,
                )]))
            }
            Err(e) => Err(ErrorData::internal_error(e.to_string(), None)),
        }
    }

    // ---- prompts: reusable operator playbooks ----

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        Ok(ListPromptsResult {
            prompts: vec![
                Prompt::new(
                    "diagnose_cluster_health",
                    Some(
                        "Guide the agent through a full cluster-health triage using the \
                         reliability, topology, replication, and slow-query tools.",
                    ),
                    None,
                ),
                Prompt::new(
                    "explain_slow_queries",
                    Some(
                        "Investigate query latency: pull the slow-query log and propose \
                         concrete fixes (schema, filters, top_k).",
                    ),
                    None,
                ),
                Prompt::new(
                    "replace_dead_node",
                    Some(
                        "Walk the safe Raft node-replacement flow: add learner, wait for \
                         catch-up, promote to voter, evict the dead node.",
                    ),
                    None,
                ),
            ],
            meta: None,
            next_cursor: None,
        })
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, ErrorData> {
        let body = match request.name.as_str() {
            "diagnose_cluster_health" => {
                "You are triaging a NebulaDB cluster. Work through these steps and report \
                 findings with the evidence from each tool:\n\
                 1. Call `reliability_status` — note the operating mode and any memory/CPU/disk \
                 pressure or AI degradation.\n\
                 2. Call `cluster_health` and `raft_membership` — confirm every expected node is \
                 present, there is a leader, and no voter is missing.\n\
                 3. Call `replication_status` — flag any follower/region lag.\n\
                 4. Call `slow_queries` — surface the worst offenders.\n\
                 5. Summarize root cause and propose concrete next actions."
            }
            "explain_slow_queries" => {
                "Investigate NebulaDB query latency:\n\
                 1. Call `slow_queries` to get the slowest recent statements.\n\
                 2. For each, call `explain_query` to inspect its plan.\n\
                 3. Propose fixes: narrower `semantic_match` top_k, added metadata filters, or \
                 restructured JOINs. Explain the expected impact of each."
            }
            "replace_dead_node" => {
                "Safely replace a dead Raft node without losing quorum:\n\
                 1. Call `raft_membership` to see current voters and the leader.\n\
                 2. Call `raft_add_learner` with the replacement node's id and address; it blocks \
                 until the new node is caught up (quorum is unaffected).\n\
                 3. Call `raft_promote_voter` for the new node.\n\
                 4. Call `raft_remove_node` for the dead node.\n\
                 5. Confirm with `raft_membership` that voters are healthy. Never remove a voter \
                 before its replacement has been promoted."
            }
            other => {
                return Err(ErrorData::invalid_params(
                    format!("unknown prompt: {other}"),
                    None,
                ))
            }
        };
        Ok(GetPromptResult::new(vec![PromptMessage::new_text(
            Role::User,
            body,
        )]))
    }
}

impl NebulaMcp {
    /// Build a client for a resource/prompt request, forwarding the
    /// caller's bearer from the HTTP parts in the request extensions
    /// when present.
    fn client_from_ctx(&self, context: &RequestContext<RoleServer>) -> crate::client::NebulaClient {
        let token = context
            .extensions
            .get::<http::request::Parts>()
            .and_then(|parts| parts.headers.get(http::header::AUTHORIZATION))
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .map(|s| s.trim().to_string());
        self.client.with_token(token)
    }
}
