//! Canonical NebulaDB AI tool definitions + local executors for the agent loop.

use std::sync::Arc;
use std::time::Duration;

use nebula_llm::ToolSpec;
use serde_json::{json, Value};

use crate::ai::sql_guard::validate_readonly_sql;
use crate::state::AppState;

/// Deterministic tool catalog shared with MCP docs / showcase.
pub fn nebula_tool_specs() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "semantic_search".into(),
            description: "Search NebulaDB using semantic similarity (HNSW)".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "top_k": {"type": "integer", "minimum": 1, "maximum": 50},
                    "bucket": {"type": "string"},
                    "tenant_id": {"type": "string", "description": "Optional tenant filter metadata"}
                },
                "required": ["query"]
            }),
        },
        ToolSpec {
            name: "hybrid_search".into(),
            description: "Hybrid vector + BM25 search in NebulaDB".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "top_k": {"type": "integer"},
                    "bucket": {"type": "string"},
                    "tenant_id": {"type": "string"}
                },
                "required": ["query"]
            }),
        },
        ToolSpec {
            name: "get_document".into(),
            description: "Fetch a document by bucket and id".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "bucket": {"type": "string"},
                    "id": {"type": "string"}
                },
                "required": ["bucket", "id"]
            }),
        },
        ToolSpec {
            name: "query_sql".into(),
            description: "Run a read-only SQL query against NebulaDB (SELECT only)".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "sql": {"type": "string"}
                },
                "required": ["sql"]
            }),
        },
        ToolSpec {
            name: "store_memory".into(),
            description: "Persist a long-term / semantic / episodic memory in NebulaDB".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "kind": {"type": "string", "enum": ["short", "long", "semantic", "episodic"]},
                    "user_id": {"type": "string"},
                    "tenant_id": {"type": "string"}
                },
                "required": ["text"]
            }),
        },
        ToolSpec {
            name: "search_memory".into(),
            description: "Retrieve relevant memories from NebulaDB by semantic similarity".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "user_id": {"type": "string"},
                    "tenant_id": {"type": "string"},
                    "top_k": {"type": "integer"}
                },
                "required": ["query"]
            }),
        },
        ToolSpec {
            name: "list_collections".into(),
            description: "List NebulaDB buckets / collections".into(),
            input_schema: json!({"type": "object", "properties": {}}),
        },
        ToolSpec {
            name: "retrieve_context".into(),
            description: "Assemble top-k context snippets for a question (no LLM generation)".into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "top_k": {"type": "integer"},
                    "bucket": {"type": "string"},
                    "tenant_id": {"type": "string"}
                },
                "required": ["query"]
            }),
        },
    ]
}

fn meta_str(meta: &Value, key: &str) -> Option<String> {
    meta.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn tenant_ok(meta: &Value, tenant: Option<&str>) -> bool {
    match tenant {
        None => true,
        Some(t) => meta_str(meta, "tenant_id").as_deref() == Some(t),
    }
}

/// Execute one tool call against live AppState. Returns JSON result.
pub async fn execute_tool(
    state: &AppState,
    name: &str,
    arguments: &str,
) -> Result<Value, String> {
    let args: Value = serde_json::from_str(arguments).unwrap_or(json!({}));
    match name {
        "semantic_search" | "hybrid_search" | "retrieve_context" => {
            let query = args
                .get("query")
                .and_then(|v| v.as_str())
                .ok_or("query required")?
                .to_string();
            let top_k = args.get("top_k").and_then(|v| v.as_u64()).unwrap_or(5) as usize;
            let bucket = args
                .get("bucket")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let tenant = args.get("tenant_id").and_then(|v| v.as_str());
            let hybrid = name == "hybrid_search";
            let hits = if hybrid {
                let weights = state.hybrid_weights.resolve(bucket.as_deref());
                Arc::clone(&state.index)
                    .search_text_hybrid_blocking(query, bucket, top_k * 3, None, weights)
                    .await
                    .map_err(|e| e.to_string())?
            } else {
                Arc::clone(&state.index)
                    .search_text_blocking(query, bucket, top_k * 3, None)
                    .await
                    .map_err(|e| e.to_string())?
            };
            let filtered: Vec<_> = hits
                .into_iter()
                .filter(|h| tenant_ok(&h.metadata, tenant))
                .take(top_k)
                .map(|h| {
                    json!({
                        "id": h.id,
                        "score": h.score,
                        "text": h.text,
                        "metadata": h.metadata,
                    })
                })
                .collect();
            Ok(json!({ "hits": filtered, "count": filtered.len() }))
        }
        "get_document" => {
            let bucket = args
                .get("bucket")
                .and_then(|v| v.as_str())
                .ok_or("bucket required")?;
            let id = args.get("id").and_then(|v| v.as_str()).ok_or("id required")?;
            let doc = state
                .index
                .get(bucket, id)
                .ok_or_else(|| format!("document {bucket}/{id} not found"))?;
            Ok(json!({
                "bucket": bucket,
                "id": id,
                "text": doc.text,
                "metadata": doc.metadata,
            }))
        }
        "query_sql" => {
            let sql = args.get("sql").and_then(|v| v.as_str()).ok_or("sql required")?;
            let allow: Vec<String> = std::env::var("NEBULA_AI_SQL_ALLOWLIST")
                .ok()
                .map(|s| {
                    s.split(',')
                        .map(|t| t.trim().to_string())
                        .filter(|t| !t.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            let safe = validate_readonly_sql(sql, &allow).map_err(|e| e.to_string())?;
            let timeout = Duration::from_secs(
                std::env::var("NEBULA_AI_SQL_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(5),
            );
            let (result, _) = tokio::time::timeout(timeout, state.sql.execute(&safe, false))
                .await
                .map_err(|_| "sql timeout".to_string())?
                .map_err(|e| e.to_string())?;
            Ok(serde_json::to_value(result).unwrap_or(json!({})))
        }
        "store_memory" => {
            let text = args
                .get("text")
                .and_then(|v| v.as_str())
                .ok_or("text required")?
                .to_string();
            let kind = args
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("long")
                .to_string();
            let user_id = args
                .get("user_id")
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            let tenant_id = args.get("tenant_id").and_then(|v| v.as_str());
            let id = format!(
                "mem_{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0)
            );
            let mut meta = json!({
                "kind": kind,
                "user_id": user_id,
            });
            if let Some(t) = tenant_id {
                meta["tenant_id"] = json!(t);
            }
            state
                .index
                .upsert_document(
                    "agent_memory",
                    &id,
                    &text,
                    state.chunker.as_ref(),
                    meta,
                )
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!({ "id": id, "bucket": "agent_memory", "stored": true }))
        }
        "search_memory" => {
            let query = args
                .get("query")
                .and_then(|v| v.as_str())
                .ok_or("query required")?
                .to_string();
            let top_k = args.get("top_k").and_then(|v| v.as_u64()).unwrap_or(5) as usize;
            let user_id = args.get("user_id").and_then(|v| v.as_str());
            let tenant = args.get("tenant_id").and_then(|v| v.as_str());
            let hits = Arc::clone(&state.index)
                .search_text_blocking(query, Some("agent_memory".into()), top_k * 4, None)
                .await
                .map_err(|e| e.to_string())?;
            let filtered: Vec<_> = hits
                .into_iter()
                .filter(|h| {
                    tenant_ok(&h.metadata, tenant)
                        && user_id
                            .map(|u| meta_str(&h.metadata, "user_id").as_deref() == Some(u))
                            .unwrap_or(true)
                })
                .take(top_k)
                .map(|h| {
                    json!({
                        "id": h.id,
                        "score": h.score,
                        "text": h.text,
                        "metadata": h.metadata,
                    })
                })
                .collect();
            Ok(json!({ "memories": filtered }))
        }
        "list_collections" => {
            let stats = state.index.bucket_stats(0);
            let names: Vec<_> = stats.into_iter().map(|b| b.bucket).collect();
            Ok(json!({ "collections": names }))
        }
        other => Err(format!("unknown tool '{other}'")),
    }
}
