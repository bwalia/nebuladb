/**
 * Browser MCP client for the showcase's MCP tab.
 *
 * Speaks the Model Context Protocol (JSON-RPC 2.0 over Streamable HTTP)
 * to `nebula-mcp` at `/mcp`. nginx (or the Vite dev proxy) forwards that
 * path and injects the app bearer, which nebula-mcp passes on to
 * nebula-server. Nothing here is simulated: tool lists, schemas,
 * annotations and results all come from the server.
 *
 * Every JSON-RPC exchange goes through the request tracer as
 * `POST /mcp · <method>`, so the Under-the-Hood panel shows the real
 * frames on the wire.
 */
import { record } from "./tracer";

const ENDPOINT = "/mcp";
const PROTOCOL_VERSION = "2025-06-18";

export interface JsonSchema {
  type?: string | string[];
  description?: string;
  properties?: Record<string, JsonSchema>;
  required?: string[];
  items?: JsonSchema;
  default?: unknown;
  format?: string;
}

export interface ToolAnnotations {
  readOnlyHint?: boolean;
  destructiveHint?: boolean;
  idempotentHint?: boolean;
  openWorldHint?: boolean;
}

export interface McpToolDef {
  name: string;
  description?: string;
  inputSchema: JsonSchema;
  annotations?: ToolAnnotations;
}

export interface McpResource {
  uri: string;
  name: string;
  description?: string;
  mimeType?: string;
}

export interface McpPrompt {
  name: string;
  description?: string;
}

export interface ContentBlock {
  type: string;
  text?: string;
}

export interface ToolResult {
  content: ContentBlock[];
  isError?: boolean;
}

export interface ServerInfo {
  protocolVersion: string;
  serverInfo: { name: string; version: string };
  capabilities: Record<string, unknown>;
  instructions?: string;
}

/** A JSON-RPC error returned by the server (as opposed to a transport failure). */
export class McpError extends Error {
  constructor(public code: number, message: string) {
    super(`MCP error ${code}: ${message}`);
  }
}

/** The server at /mcp could not be reached or answered with non-MCP HTTP. */
export class McpTransportError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

interface RpcResponse {
  jsonrpc: "2.0";
  id?: number;
  result?: unknown;
  error?: { code: number; message: string };
}

/**
 * Extract the JSON-RPC response for `id` from a response body, which the
 * server may send as plain JSON or as a text/event-stream. rmcp opens the
 * stream with an empty priming event, so data-less events are skipped.
 */
export function parseRpcBody(body: string, contentType: string, id: number): RpcResponse | undefined {
  if (!contentType.includes("text/event-stream")) {
    return body.trim() ? (JSON.parse(body) as RpcResponse) : undefined;
  }
  for (const event of body.split(/\r?\n\r?\n/)) {
    const data = event
      .split(/\r?\n/)
      .filter((l) => l.startsWith("data:"))
      .map((l) => l.slice(5).replace(/^ /, ""))
      .join("\n")
      .trim();
    if (!data) continue;
    const msg = JSON.parse(data) as RpcResponse;
    if (msg.id === id) return msg;
  }
  return undefined;
}

export class McpSession {
  private sessionId: string | null = null;
  private nextId = 1;
  info: ServerInfo | null = null;

  get id(): string | null {
    return this.sessionId;
  }

  async connect(): Promise<ServerInfo> {
    this.sessionId = null;
    const info = (await this.rpc("initialize", {
      protocolVersion: PROTOCOL_VERSION,
      capabilities: {},
      clientInfo: { name: "nebuladb-showcase", version: "1" },
    })) as ServerInfo;
    await this.notify("notifications/initialized");
    this.info = info;
    return info;
  }

  async listTools(): Promise<McpToolDef[]> {
    return ((await this.call("tools/list")) as { tools: McpToolDef[] }).tools;
  }

  async callTool(name: string, args: Record<string, unknown>): Promise<ToolResult> {
    return (await this.call("tools/call", { name, arguments: args })) as ToolResult;
  }

  async listResources(): Promise<McpResource[]> {
    return ((await this.call("resources/list")) as { resources: McpResource[] }).resources;
  }

  async readResource(uri: string): Promise<{ contents: Array<{ uri: string; text?: string }> }> {
    return (await this.call("resources/read", { uri })) as { contents: Array<{ uri: string; text?: string }> };
  }

  async listPrompts(): Promise<McpPrompt[]> {
    return ((await this.call("prompts/list")) as { prompts: McpPrompt[] }).prompts;
  }

  async getPrompt(name: string): Promise<{ messages: Array<{ role: string; content: ContentBlock }> }> {
    return (await this.call("prompts/get", { name })) as {
      messages: Array<{ role: string; content: ContentBlock }>;
    };
  }

  /**
   * A request inside the session. If the server has forgotten the session
   * (nebula-mcp restarted, so the id 404s), re-initialize once and retry.
   */
  private async call(method: string, params?: unknown): Promise<unknown> {
    if (!this.sessionId) await this.connect();
    try {
      return await this.rpc(method, params);
    } catch (e) {
      if (e instanceof McpTransportError && e.status === 404) {
        await this.connect();
        return this.rpc(method, params);
      }
      throw e;
    }
  }

  private headers(): HeadersInit {
    const h: Record<string, string> = {
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
    };
    if (this.sessionId) h["mcp-session-id"] = this.sessionId;
    if (this.info) h["mcp-protocol-version"] = this.info.protocolVersion;
    return h;
  }

  private async rpc(method: string, params?: unknown): Promise<unknown> {
    const id = this.nextId++;
    const frame = { jsonrpc: "2.0", id, method, ...(params === undefined ? {} : { params }) };
    const t0 = performance.now();
    let resp: Response;
    try {
      resp = await fetch(ENDPOINT, { method: "POST", headers: this.headers(), body: JSON.stringify(frame) });
    } catch (e) {
      const msg = `cannot reach ${ENDPOINT}: ${(e as Error).message}`;
      record({ method: "POST", path: `${ENDPOINT} · ${method}`, status: 0, tookMs: 0, requestBody: frame, error: msg });
      throw new McpTransportError(0, msg);
    }
    const body = await resp.text();
    const tookMs = Math.round(performance.now() - t0);
    const path = `${ENDPOINT} · ${method}`;
    if (!resp.ok) {
      record({ method: "POST", path, status: resp.status, tookMs, requestBody: frame, error: body.slice(0, 400) });
      throw new McpTransportError(resp.status, `${ENDPOINT} returned HTTP ${resp.status}${body ? `: ${body.slice(0, 200)}` : ""}`);
    }
    const sid = resp.headers.get("mcp-session-id");
    if (sid) this.sessionId = sid;

    let msg: RpcResponse | undefined;
    try {
      msg = parseRpcBody(body, resp.headers.get("content-type") ?? "", id);
    } catch {
      record({ method: "POST", path, status: resp.status, tookMs, requestBody: frame, error: body.slice(0, 400) });
      throw new McpTransportError(resp.status, `${ENDPOINT} did not return MCP JSON-RPC (is nebula-mcp behind it?)`);
    }
    record({ method: "POST", path, status: resp.status, tookMs, requestBody: frame, responseBody: msg });
    if (!msg) throw new McpTransportError(resp.status, `no JSON-RPC response for ${method}`);
    if (msg.error) throw new McpError(msg.error.code, msg.error.message);
    return msg.result;
  }

  private async notify(method: string): Promise<void> {
    const frame = { jsonrpc: "2.0", method };
    const resp = await fetch(ENDPOINT, { method: "POST", headers: this.headers(), body: JSON.stringify(frame) });
    record({ method: "POST", path: `${ENDPOINT} · ${method}`, status: resp.status, tookMs: 0, requestBody: frame });
  }
}

/** Parse a tool result's text back into JSON when it is JSON (NebulaDB responses are). */
export function resultValue(r: ToolResult): unknown {
  const text = r.content
    .filter((b) => b.type === "text" && b.text != null)
    .map((b) => b.text)
    .join("\n");
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}
