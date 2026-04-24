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
};
