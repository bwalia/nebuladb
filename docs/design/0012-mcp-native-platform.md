# Design 0012: MCP-native platform — NebulaDB for AI agents

- **Status**: Phase 1 implemented. Ported to `main` standalone (without the
  design-0011 Raft stack); see §4a.
- **Author**: Claude + @bwalia
- **Created**: 2026-07-12
- **Tracks**: vision — "the PostgreSQL of AI agents": point any MCP-compatible assistant at NebulaDB and get secure access to data, search, RAG, ops, and cluster operations with no custom plugin.
- **Relates to**: 0001 (multi-region), 0010 (reliability), 0011 (dynamic membership)

---

## 1. Why this exists

AI agents increasingly speak the [Model Context Protocol](https://modelcontextprotocol.io)
(MCP) — a JSON-RPC standard for tools, resources, and prompts. Today an
agent can only reach NebulaDB by hand-rolling HTTP calls. This ADR makes
NebulaDB a first-class MCP citizen: any MCP client (Claude Desktop, IDE
assistants, agent frameworks) can discover and use NebulaDB's full
capability surface over a standard protocol.

The north star is large — a fleet of modular MCP servers, six SDKs,
OAuth 2.1 / ABAC, 500-node scale. This ADR is deliberately **Phase 1**:
build one real, working MCP server against NebulaDB's *actual* current
capabilities, prove the end-to-end path, and lay a foundation the rest
of the vision extends rather than replaces. Shipping a working server
beats shipping sixteen shallow artifacts that don't run.

## 2. Principle: adapter, not second database

`nebula-mcp` is a **client of a running `nebula-server`**, not an
embedder of the index. It holds no data and owns no durability. Every
tool maps to a real REST endpoint that already exists (`/query`,
`/ai/search`, `/rag/answer`, `/admin/reliability`, …).

Why this matters:

- **No capability drift.** If NebulaDB can't do it over HTTP today,
  there is no tool for it. The MCP surface can't over-promise.
- **Auth for free.** The MCP layer forwards the caller's
  `Authorization: Bearer` token to nebula-server, which applies its
  existing api-key / JWT auth, rate limiting, admission control, and
  disk-critical write gate unchanged. An agent can only do what its
  token permits — MCP adds a protocol, not a second trust boundary.
- **Independently deployable.** It's a separate binary/pod that scales
  on its own axis (agent concurrency) without touching the data plane.
  This is the seed of the modular multi-server vision (§6).

## 3. Architecture (Phase 1)

```
  MCP client (Claude Desktop, agent)
        │  MCP / JSON-RPC over Streamable HTTP  (POST /mcp)
        ▼
  ┌─────────────────────────┐
  │ nebula-mcp (this crate)  │   rmcp 2.2 StreamableHttpService on axum
  │  ├─ tools.rs   (17 tools)│   #[tool_router] over NebulaMcp
  │  ├─ server.rs  (resources│   ServerHandler: resources + prompts + get_info
  │  │             + prompts) │
  │  └─ client.rs  (reqwest) │   forwards caller bearer → upstream
  └───────────┬─────────────┘
              │  HTTP + Bearer  (/api/v1/...)
              ▼
        nebula-server  (unchanged: auth, index, Raft, backups)
```

Transport is **Streamable HTTP** (chosen over stdio): network-accessible
so remote/containerized agents can reach it, mounted at `/mcp`. rmcp
injects the HTTP request `Parts` into each tool's context, so tools read
the caller's `Authorization` header and forward it upstream.

## 4. Tool surface (Phase 1 — all backed by real endpoints)

| Category | Tools | Upstream |
|----------|-------|----------|
| SQL | `execute_sql`, `explain_query` | `/query`, `/query/explain` |
| Search | `semantic_search`, `vector_search` | `/ai/search`, `/vector/search` |
| RAG | `answer_question` | `/rag/answer` |
| Documents | `insert_document`, `get_document`, `delete_document`, `list_buckets` | `/bucket/*`, `/admin/buckets` |
| Agent memory | `remember`, `recall` | `/bucket/agent_memory/document`, `/ai/search` |
| Cluster | `cluster_health` | `/admin/cluster/nodes` |
| Backups | `create_snapshot` | `/admin/snapshot` |
| Observability | `server_stats`, `reliability_status`, `replication_status`, `slow_queries` | `/admin/*` |

### 4a. Changes in the port to `main`

The crate was first merged into the stacked design-0010/0011 branch,
which has not reached `main`. It was ported on its own because it is
a pure REST client. Differences from that first version:

- **Raft tools removed** (`raft_membership`, `raft_add_learner`,
  `raft_promote_voter`, `raft_remove_node`) and the `replace_dead_node`
  prompt. They call `/admin/raft/*`, which only exists with design 0011;
  restore them when that stack lands.
- **Chunked documents fixed.** `insert_document` writes through
  `/bucket/:b/document`, which stores rows as `{doc_id}#{i}` — never the
  bare id. `get_document` previously 404'd on every document it had
  written, and `delete_document` removed nothing. `get_document` now
  falls back to reassembling `#i` chunks; `delete_document` uses the
  chunk-aware route and falls back to the single-row route.
- **Path segments percent-encoded.** Ids like `u-42:turn-7` or
  `runbook/dr#v2` previously broke the URL (`#` became a fragment, `/`
  changed the route).
- **Agent memory tools** (`remember`, `recall`). `recall` is scoped to a
  `user_id`. Bucket and metadata filters both run *after* ANN in
  nebula-index, and SQL overfetches only `LIMIT × 4` (min 32) before its
  residual filter — too narrow for a shared memory bucket. `recall`
  therefore asks `/ai/search` for the server's max pool (100, `ef` 256)
  and filters by `user_id` itself. A user whose memories rank below the
  top 100 of the bucket will still be missed; true pre-filtered HNSW is
  the fix.
- `semantic_search` and `answer_question` gained `hybrid`;
  `answer_question` gained `bucket`.
- Default bind is `127.0.0.1:8090`. `NEBULA_MCP_UPSTREAM_TOKEN` is lent to
  any caller without its own bearer, so listening on all interfaces must
  be an explicit choice.

**Error semantics.** A non-2xx from nebula-server (e.g. `sql_unsupported`,
`not_leader`, `429`) becomes a *visible tool error* (`isError: true`)
carrying NebulaDB's own diagnostic body — the agent reads the real
reason and can adapt. Only transport/decode failures (can't reach
NebulaDB at all) become protocol-level errors.

## 5. Resources & prompts

**Resources** (read-only live views, no tool call needed):
`nebula://buckets`, `nebula://cluster/topology`,
`nebula://cluster/reliability`, `nebula://stats`.

**Prompts** (operator playbooks stitching several tools):
`diagnose_cluster_health`, `explain_slow_queries`.

## 6. Roadmap — the rest of the vision

Phase 1 is a single server. The larger vision is phased so each step
ships something usable:

- **Phase 2 — richer tools & write safety.** `aggregate_documents`,
  `bulk` ingest, `rerank_results`, `hybrid_search` (once a first-class
  endpoint exists), CDC/event `subscribe` over MCP's streaming. Tool
  annotations (`readOnlyHint`/`destructiveHint`) so clients gate writes.
- **Phase 3 — modular split.** Break `nebula-mcp` into independently
  deployable servers behind a shared framework crate
  (`nebula-mcp-core`): `-sql`, `-rag`, `-cluster`, `-ai`, `-monitoring`,
  `-security`. Only split when a capability needs its own scaling or
  blast-radius boundary — not before.
- **Phase 4 — enterprise auth.** OAuth 2.1 / OIDC resource-server flow
  (MCP's auth spec), mTLS, per-tool RBAC/ABAC beyond the forwarded
  bearer. Multi-tenant context isolation.
- **Phase 5 — agent context & multi-agent.** Session/workspace memory,
  resumable subscriptions, shared team context, sampling-based
  sub-agents.
- **Phase 6 — SDKs, K8s, OTel.** Generated SDKs, Helm chart + HPA for
  the MCP tier, OpenTelemetry spans on every tool call, Grafana panels.

Non-goals held for now: cross-region MCP consensus, 500-node targets as
a *gate* (the adapter scales horizontally by definition — it's
stateless), and any tool that isn't backed by a real capability.

## 7. Rollout & compatibility

Purely additive: a new crate + binary, zero changes to nebula-server.
Default deployments are unaffected. `nebula-mcp` only runs when you
start it and point it at a nebula-server.

## 8. Verification

Unit tests cover tool parameter schemas (what the agent introspects) and
the error-rendering contract. A live smoke test boots a real
nebula-server + nebula-mcp and drives the full MCP JSON-RPC lifecycle:
`initialize` → `tools/list` (17 tools) → `tools/call semantic_search`
(real scored hit) → `execute_sql` (real `sql_invalid` surfaced as a
visible tool error) → `resources/read` → `prompts/list`.
