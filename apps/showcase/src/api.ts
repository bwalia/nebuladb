/**
 * Typed client for the NebulaDB REST surface.
 *
 * Every URL is relative — Vite proxies in dev and nginx does the
 * same in prod (see apps/showcase/nginx.conf), so the app bundle is
 * portable across environments without rebuilding.
 *
 * Shapes mirror the server's JSON responses 1:1; if the server
 * changes, TypeScript breaks here first.
 */

import { record } from "./demo/tracer";

export interface Health {
  status: "ok";
  docs: number;
  dim: number;
  model: string;
  // Present on the deployed server; shown in the bottom status bar.
  version?: string;
  git_commit?: string;
}

export interface Hit {
  bucket: string;
  id: string;
  text: string;
  score: number;
  metadata: Record<string, unknown>;
}

export interface SearchResponse {
  hits: Hit[];
  took_ms: number;
  /** Present when the request set `explain: true`. */
  explain?: Explain;
}

export interface SqlRow {
  id: string;
  bucket: string;
  score: number;
  fields: Record<string, unknown>;
}

export interface SqlResponse {
  took_ms: number;
  rows: SqlRow[];
  /** Present for `explain: true` and for `EXPLAIN [ANALYZE] ...` SQL. */
  explain?: Explain;
}

// ---- EXPLAIN ------------------------------------------------------------
//
// How a query arrived at its result — the server's analogue of Postgres
// `EXPLAIN ANALYZE`, shared by SQL, search and RAG. Mirrors the server's
// JSON 1:1 (contract: every field name here is what the server emits).

/** One executed (or, for plan-only EXPLAIN, planned) pipeline stage. */
export interface ExplainStage {
  /** Stable id: "parse" | "plan" | "cache" | "embed" | "hnsw" | "bm25" |
   *  "fuse" | "bucket_filter" | "filter" | "sort" | "limit" | "project" |
   *  "aggregate" | "join" | "expand" | "merge" | "rerank" | "prompt" |
   *  "llm". Unknown names render generically. */
  name: string;
  label: string;
  detail: string;
  rows_in: number | null;
  rows_out: number | null;
  /** Microseconds; 0 for plan-only EXPLAIN. */
  took_us: number;
  attrs: Record<string, unknown>;
}

export interface ExplainNote {
  level: "info" | "warn";
  message: string;
}

export interface TermContribution {
  term: string;
  tf: number;
  df: number;
  idf: number;
  contribution: number;
}

export interface HitExplain {
  id: string;
  rank: number;
  final_score: number;
  score_kind: "distance" | "fused" | "bm25";
  vector: null | {
    distance: number;
    similarity: number;
    normalized: number | null;
    /** Fusion weight applied to `normalized` (hybrid only). */
    weight: number | null;
    weighted: number | null;
    rank: number | null;
  };
  bm25: null | {
    score: number;
    normalized: number | null;
    /** Fusion weight applied to `normalized` (hybrid only). */
    weight: number | null;
    weighted: number | null;
    rank: number | null;
    terms: TermContribution[];
  };
  filters: null | Array<{ predicate: string; actual: unknown; passed: boolean }>;
}

export interface Explain {
  kind: "sql" | "search" | "rag";
  /** true = the query ran (ANALYZE); false = plan-only EXPLAIN. */
  analyzed: boolean;
  summary: string;
  total_us: number;
  stages: ExplainStage[];
  notes: ExplainNote[];
  hits: HitExplain[];
  plan: unknown | null;
  prompt: string | null;
  text: string[];
}

export interface RagJsonResponse {
  query: string;
  context: Hit[];
  answer: string;
}

export interface ApiErrorBody {
  error: { code: string; message: string };
}

export interface BucketStats {
  bucket: string;
  docs: number;
  parent_docs: number;
  metadata_keys: Array<[string, number]>;
}

export interface AuditEntry {
  ts_ms: number;
  principal: string;
  method: string;
  path: string;
  status: number;
}

/**
 * The server returns a tagged plan tree; we leave the shape `unknown`
 * to avoid mirroring every QueryPlan variant in TypeScript. The
 * Admin tab renders it as pretty-printed JSON anyway.
 */
export type QueryPlan = unknown;

export interface StatsSnapshot {
  requests_total: number;
  requests_errors: number;
  auth_failures: number;
  rate_limited: number;
  jwt_failures: number;
  docs_inserted: number;
  docs_deleted: number;
  searches_vector: number;
  searches_semantic: number;
  rag_requests: number;
  embed_cache_hits: number;
  embed_cache_misses: number;
  embed_cache_evictions: number;
  embed_cache_inserts: number;
  total_docs_live: number;
}

export interface SlowQueryEntry {
  ts_ms: number;
  took_ms: number;
  rows: number;
  sql: string;
  ok: boolean;
}

export interface WalStats {
  segment_count: number;
  total_bytes: number;
  oldest_seq: number;
  newest_seq: number;
}

export interface DurabilityInfo {
  persistent: boolean;
  data_dir: string | null;
  wal: WalStats | null;
}

export interface SnapshotOutcome {
  path: string;
  wal_seq_captured: number;
}

/**
 * Thrown on non-2xx responses. Carries the decoded body so the UI
 * can show the server's stable `code` string (e.g. `sql_parse`)
 * rather than a generic "something went wrong".
 */

// ---- cluster / replication / backup ------------------------------------
// These mirror the server's admin surface (crates/nebula-server routes:
// /admin/cluster/nodes, /admin/replication, /admin/backups, ...). Fields
// are optional where the server only populates them in some roles, so a
// standalone node renders without exploding.

export interface ClusterNode {
  id?: string;
  address?: string;
  role?: string;
  state?: string;
  region?: string;
  [k: string]: unknown;
}

export interface ReplicationInfo {
  role?: string;
  region?: string;
  peers?: unknown[];
  applied_seq?: number;
  [k: string]: unknown;
}

export interface BackupEntry {
  id?: string;
  path?: string;
  created_ms?: number;
  bytes?: number;
  wal_seq?: number;
  [k: string]: unknown;
}

export interface VersionInfo {
  version?: string;
  git_commit?: string;
  [k: string]: unknown;
}

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    public body: string
  ) {
    super(`${status}: ${code}`);
  }
}

async function request<T>(
  path: string,
  init?: RequestInit
): Promise<T> {
  const method = (init?.method ?? "GET").toUpperCase();
  // Parsed back out so the trace panel can pretty-print the body we
  // actually sent rather than a re-stringified guess.
  let requestBody: unknown;
  if (typeof init?.body === "string") {
    try {
      requestBody = JSON.parse(init.body);
    } catch {
      requestBody = init.body;
    }
  }
  const started = performance.now();
  let resp: Response;
  try {
    resp = await fetch(path, {
      ...init,
      headers: {
        "content-type": "application/json",
        ...(init?.headers || {}),
      },
    });
  } catch (e) {
    record({
      method,
      path,
      status: 0,
      tookMs: Math.round(performance.now() - started),
      requestBody,
      error: (e as Error).message,
    });
    throw e;
  }
  const text = await resp.text();
  const tookMs = Math.round(performance.now() - started);

  if (!resp.ok) {
    let code = "unknown";
    try {
      const parsed: ApiErrorBody = JSON.parse(text);
      code = parsed.error?.code ?? code;
    } catch {
      /* non-JSON error body (e.g. pgwire path) — fall back to status */
    }
    record({ method, path, status: resp.status, tookMs, requestBody, error: `${code}: ${text.slice(0, 400)}` });
    throw new ApiError(resp.status, code, text);
  }

  // Endpoints returning 204 No Content (document delete) produce an
  // empty body; callers just discard the result.
  const parsed = text ? (JSON.parse(text) as T) : (undefined as T);
  record({ method, path, status: resp.status, tookMs, requestBody, responseBody: parsed });
  return parsed;
}

export const api = {
  health: () => request<Health>("/healthz"),

  upsertDoc: (bucket: string, id: string, text: string, metadata: unknown = {}) =>
    request<{ bucket: string; id: string; dim: number }>(
      `/api/v1/bucket/${encodeURIComponent(bucket)}/doc`,
      {
        method: "POST",
        body: JSON.stringify({ id, text, metadata }),
      }
    ),

  upsertDocument: (bucket: string, docId: string, text: string, metadata: unknown = {}) =>
    request<{ bucket: string; doc_id: string; chunks: number }>(
      `/api/v1/bucket/${encodeURIComponent(bucket)}/document`,
      {
        method: "POST",
        body: JSON.stringify({ doc_id: docId, text, metadata }),
      }
    ),

  getDoc: (bucket: string, id: string) =>
    request<{ bucket: string; id: string; text: string; metadata: Record<string, unknown> }>(
      `/api/v1/bucket/${encodeURIComponent(bucket)}/doc/${encodeURIComponent(id)}`
    ),

  deleteDoc: (bucket: string, id: string) =>
    request<void>(
      `/api/v1/bucket/${encodeURIComponent(bucket)}/doc/${encodeURIComponent(id)}`,
      { method: "DELETE" }
    ),

  deleteDocument: (bucket: string, docId: string) =>
    request<{ bucket: string; doc_id: string; chunks_removed: number }>(
      `/api/v1/bucket/${encodeURIComponent(bucket)}/document/${encodeURIComponent(docId)}`,
      { method: "DELETE" }
    ),

  search: (
    query: string,
    top_k = 5,
    bucket?: string,
    opts: { hybrid?: boolean; explain?: boolean } = {}
  ) =>
    request<SearchResponse>("/api/v1/ai/search", {
      method: "POST",
      // Only send the flags when set so the request body for the plain
      // path is unchanged.
      body: JSON.stringify({
        query,
        top_k,
        bucket,
        ...(opts.hybrid ? { hybrid: true } : {}),
        ...(opts.explain ? { explain: true } : {}),
      }),
    }),

  sql: (sql: string, explain = false) =>
    request<SqlResponse>("/api/v1/query", {
      method: "POST",
      body: JSON.stringify(explain ? { sql, explain: true } : { sql }),
    }),

  ragJson: (query: string, top_k = 5, bucket?: string) =>
    request<RagJsonResponse>("/api/v1/ai/rag", {
      method: "POST",
      body: JSON.stringify({ query, top_k, bucket, stream: false }),
    }),

  explain: (sql: string) =>
    request<QueryPlan>("/api/v1/query/explain", {
      method: "POST",
      body: JSON.stringify({ sql }),
    }),

  buckets: () =>
    request<BucketStats[]>("/api/v1/admin/buckets"),

  audit: (limit = 200) =>
    request<AuditEntry[]>(`/api/v1/admin/audit?limit=${limit}`),

  stats: () => request<StatsSnapshot>("/api/v1/admin/stats"),

  slow: () => request<SlowQueryEntry[]>("/api/v1/admin/slow"),

  durability: () => request<DurabilityInfo>("/api/v1/admin/durability"),

  takeSnapshot: () =>
    request<SnapshotOutcome>("/api/v1/admin/snapshot", { method: "POST" }),

  compactWal: () =>
    request<{ removed_segments: number }>("/api/v1/admin/wal/compact", {
      method: "POST",
    }),

  emptyBucket: (bucket: string) =>
    request<{ bucket: string; removed: number }>(
      `/api/v1/admin/bucket/${encodeURIComponent(bucket)}/empty`,
      { method: "POST" }
    ),

  // ---- real endpoints beyond the original 8 tabs -----------------------

  /**
   * Raw neighbour lookup against the HNSW index.
   *
   * Takes an already-computed embedding — the server's
   * `VectorSearchRequest` requires `vector: Vec<f32>` and will reject a
   * bare query string with 422. Use `search()` for text.
   * `ef` tunes HNSW search breadth (recall vs latency).
   */
  vectorSearch: (vector: number[], top_k = 5, bucket?: string, ef?: number) =>
    request<SearchResponse>("/api/v1/vector/search", {
      method: "POST",
      body: JSON.stringify({ vector, top_k, bucket, ef }),
    }),

  /** Non-streaming RAG answer with citations. */
  ragAnswer: (query: string, top_k = 5, bucket?: string) =>
    request<RagJsonResponse>("/api/v1/rag/answer", {
      method: "POST",
      body: JSON.stringify({ query, top_k, bucket }),
    }),

  version: () => request<VersionInfo>("/api/v1/admin/version"),

  clusterNodes: () => request<ClusterNode[]>("/api/v1/admin/cluster/nodes"),

  replication: () => request<ReplicationInfo>("/api/v1/admin/replication"),

  backups: () => request<BackupEntry[]>("/api/v1/admin/backups"),

  createBackup: () =>
    request<BackupEntry>("/api/v1/admin/backup", { method: "POST" }),

  restore: (id: string) =>
    request<{ ok: boolean }>(`/api/v1/admin/restore/${encodeURIComponent(id)}`, {
      method: "POST",
    }),

  exportBucket: (bucket: string) =>
    request<unknown>(`/api/v1/admin/bucket/${encodeURIComponent(bucket)}/export`),

  bulkDocs: (bucket: string, docs: Array<{ id: string; text: string; metadata?: unknown }>) =>
    request<{ inserted: number }>(
      `/api/v1/bucket/${encodeURIComponent(bucket)}/docs/bulk`,
      { method: "POST", body: JSON.stringify({ docs }) }
    ),
};
