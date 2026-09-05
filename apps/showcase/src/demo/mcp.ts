/**
 * MCP tool catalogue (build brief section 17).
 *
 * NebulaDB does not ship an MCP server endpoint yet, so the transport
 * is simulated — but the *tools are not*. Each entry below is a thin,
 * honest wrapper over a REST endpoint that genuinely exists on this
 * cluster, so invoking a tool in the playground performs the real call
 * and the trace panel shows the real traffic.
 *
 * That distinction matters: an audience can accept "the MCP wire
 * protocol is stubbed" far more easily than a tool that returns
 * invented data. Tools whose backing endpoint does not exist are
 * marked `backing: null` and refuse to execute rather than fake it.
 */
import { api } from "../api";

export type ParamType = "string" | "number";

export interface McpParam {
  name: string;
  type: ParamType;
  required: boolean;
  description: string;
  default?: string | number;
}

export interface McpTool {
  name: string;
  summary: string;
  description: string;
  /** Least-privilege role this tool needs. */
  permission: "read" | "write" | "admin";
  category: "retrieval" | "sql" | "ops" | "data";
  params: McpParam[];
  /** The REST endpoint this tool actually calls, or null if unbacked. */
  backing: string | null;
  run?: (args: Record<string, string | number>) => Promise<unknown>;
}

const str = (v: unknown, d = ""): string => (v == null ? d : String(v));
const num = (v: unknown, d: number): number => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};

export const MCP_TOOLS: McpTool[] = [
  {
    name: "semantic_search",
    summary: "Embed a question and retrieve the nearest chunks",
    description:
      "Runs the query through the configured embedder and searches the HNSW index for the nearest neighbours, returning chunks with cosine scores and metadata.",
    permission: "read",
    category: "retrieval",
    backing: "POST /api/v1/ai/search",
    params: [
      { name: "query", type: "string", required: true, description: "Natural-language question" },
      { name: "top_k", type: "number", required: false, description: "Neighbours to return", default: 5 },
      { name: "bucket", type: "string", required: false, description: "Restrict to one bucket" },
    ],
    run: (a) => api.search(str(a.query), num(a.top_k, 5), str(a.bucket) || undefined),
  },
  {
    name: "vector_search",
    summary: "Neighbour lookup from a raw embedding",
    description:
      "Lower-level search against the HNSW index. The server requires an already-computed embedding (`vector: Vec<f32>`) and rejects a bare query string, so this tool takes comma-separated floats. For text, use semantic_search. `ef` widens the HNSW search beam, trading latency for recall.",
    permission: "read",
    category: "retrieval",
    backing: "POST /api/v1/vector/search",
    params: [
      {
        name: "vector",
        type: "string",
        required: true,
        description: "Comma-separated floats, e.g. 0.12,-0.04,0.98 (must match index dim)",
      },
      { name: "top_k", type: "number", required: false, description: "Neighbours", default: 5 },
      { name: "ef", type: "number", required: false, description: "HNSW search breadth" },
    ],
    run: (a) => {
      const vector = str(a.vector)
        .split(",")
        .map((x) => Number(x.trim()))
        .filter((x) => Number.isFinite(x));
      if (vector.length === 0) throw new Error("vector must be comma-separated floats");
      const ef = a.ef == null || a.ef === "" ? undefined : num(a.ef, 0);
      return api.vectorSearch(vector, num(a.top_k, 5), undefined, ef);
    },
  },
  {
    name: "ai_answer",
    summary: "Answer a question with retrieved context and citations",
    description:
      "Full RAG round trip: retrieve, assemble context, call the LLM, return an answer alongside the chunks used as sources.",
    permission: "read",
    category: "retrieval",
    backing: "POST /api/v1/ai/rag",
    params: [
      { name: "query", type: "string", required: true, description: "Question to answer" },
      { name: "top_k", type: "number", required: false, description: "Context chunks", default: 5 },
    ],
    run: (a) => api.ragJson(str(a.query), num(a.top_k, 5)),
  },
  {
    name: "execute_sql",
    summary: "Run a read-only SQL statement",
    description:
      "Executes SQL against the shared corpus. NebulaDB's dialect is AI-native: a WHERE clause must include semantic_match(col, 'text') or vector_distance(...), so retrieval and filtering are planned together. The showcase restricts this tool to SELECT so an agent cannot mutate demo data.",
    permission: "read",
    category: "sql",
    backing: "POST /api/v1/query",
    params: [
      {
        name: "sql",
        type: "string",
        required: true,
        description: "SELECT with a semantic predicate",
        default: "SELECT id, text FROM docs WHERE semantic_match(text, 'disaster recovery') LIMIT 5",
      },
    ],
    run: async (a) => {
      const sql = str(a.sql).trim();
      if (!/^select\b/i.test(sql)) {
        throw new Error("execute_sql is restricted to SELECT in the showcase");
      }
      return api.sql(sql);
    },
  },
  {
    name: "explain_query",
    summary: "Return the execution plan for a statement",
    description:
      "Parses and plans the statement without running it — the plan tree shows the scan target, the semantic predicate, filters, projection and limit the engine would use.",
    permission: "read",
    category: "sql",
    backing: "POST /api/v1/query/explain",
    params: [
      {
        name: "sql",
        type: "string",
        required: true,
        description: "Statement to plan",
        default: "SELECT id, text FROM docs WHERE semantic_match(text, 'disaster recovery') LIMIT 5",
      },
    ],
    run: (a) => api.explain(str(a.sql)),
  },
  {
    name: "list_buckets",
    summary: "List buckets with document counts",
    description: "Inventory of buckets, live document counts and the metadata keys present in each.",
    permission: "read",
    category: "data",
    backing: "GET /api/v1/admin/buckets",
    params: [],
    run: () => api.buckets(),
  },
  {
    name: "query_metrics",
    summary: "Read the server's counter snapshot",
    description:
      "Request totals, error counts, search counts by kind, RAG requests and embedding-cache hit/miss counters.",
    permission: "read",
    category: "ops",
    backing: "GET /api/v1/admin/stats",
    params: [],
    run: () => api.stats(),
  },
  {
    name: "cluster_health",
    summary: "Durability and WAL health",
    description:
      "Reports whether persistence is enabled, the data directory, and WAL segment counts and sequence range.",
    permission: "read",
    category: "ops",
    backing: "GET /api/v1/admin/durability",
    params: [],
    run: () => api.durability(),
  },
  {
    name: "list_nodes",
    summary: "Enumerate cluster members",
    description: "Cluster membership as the server sees it. On a standalone node this returns a single entry.",
    permission: "read",
    category: "ops",
    backing: "GET /api/v1/admin/cluster/nodes",
    params: [],
    run: () => api.clusterNodes(),
  },
  {
    name: "replication_status",
    summary: "Inspect replication role and peers",
    description: "Current role, region and applied sequence — the basis for lag calculations.",
    permission: "read",
    category: "ops",
    backing: "GET /api/v1/admin/replication",
    params: [],
    run: () => api.replication(),
  },
  {
    name: "backup_status",
    summary: "List backups and their metadata",
    description: "Known backups with creation time, size and the WAL sequence each captured.",
    permission: "admin",
    category: "ops",
    backing: "GET /api/v1/admin/backups",
    params: [],
    run: () => api.backups(),
  },
  {
    name: "slow_queries",
    summary: "Recent statements over the slow threshold",
    description: "Ring buffer of slow statements with duration, row count and success flag.",
    permission: "admin",
    category: "ops",
    backing: "GET /api/v1/admin/slow",
    params: [],
    run: () => api.slow(),
  },
  {
    name: "audit_log",
    summary: "Recent authenticated API calls",
    description: "Principal, method, path and status for recent requests — the input to the security review agent.",
    permission: "admin",
    category: "ops",
    backing: "GET /api/v1/admin/audit",
    params: [{ name: "limit", type: "number", required: false, description: "Entries", default: 50 }],
    run: (a) => api.audit(num(a.limit, 50)),
  },
  {
    name: "rebalance_status",
    summary: "Swap-rebalance progress",
    description:
      "No server endpoint backs this yet — the Rebalance page drives a deterministic simulator instead. Listed here so the catalogue reflects the intended surface.",
    permission: "admin",
    category: "ops",
    backing: null,
    params: [],
  },
];

export const toolByName = (n: string): McpTool | undefined =>
  MCP_TOOLS.find((t) => t.name === n);
