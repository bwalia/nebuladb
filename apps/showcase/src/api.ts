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

export interface Health {
  status: "ok";
  docs: number;
  dim: number;
  model: string;
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

/**
 * Thrown on non-2xx responses. Carries the decoded body so the UI
 * can show the server's stable `code` string (e.g. `sql_parse`)
 * rather than a generic "something went wrong".
 */
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
  const resp = await fetch(path, {
    ...init,
    headers: {
      "content-type": "application/json",
      ...(init?.headers || {}),
    },
  });
  const text = await resp.text();
  if (!resp.ok) {
    let code = "unknown";
    try {
      const parsed: ApiErrorBody = JSON.parse(text);
      code = parsed.error?.code ?? code;
    } catch {
      /* non-JSON error body (e.g. pgwire path) — fall back to status */
    }
    throw new ApiError(resp.status, code, text);
  }
  // Endpoints returning 204 No Content (document delete) produce an
  // empty body; callers just discard the result.
  return text ? (JSON.parse(text) as T) : (undefined as T);
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

  search: (query: string, top_k = 5, bucket?: string) =>
    request<SearchResponse>("/api/v1/ai/search", {
      method: "POST",
      body: JSON.stringify({ query, top_k, bucket }),
    }),

  sql: (sql: string) =>
    request<SqlResponse>("/api/v1/query", {
      method: "POST",
      body: JSON.stringify({ sql }),
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

  emptyBucket: (bucket: string) =>
    request<{ bucket: string; removed: number }>(
      `/api/v1/admin/bucket/${encodeURIComponent(bucket)}/empty`,
      { method: "POST" }
    ),
};
