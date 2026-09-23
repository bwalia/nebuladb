/**
 * MCP showcase + playground (build brief section 17), against the real
 * nebula-mcp server.
 *
 * The page opens an MCP session at /mcp and renders what the server
 * advertises: tools with their JSON Schemas and read/write annotations,
 * resources, and prompts. Invoking a tool sends a genuine `tools/call`;
 * nebula-mcp turns it into a REST call to nebula-server and returns the
 * result. The Under-the-Hood panel shows each JSON-RPC frame.
 *
 * If /mcp is unreachable (nebula-mcp not deployed), the page says so and
 * shows how to run it rather than falling back to a simulation.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import {
  McpSession,
  McpError,
  resultValue,
  type JsonSchema,
  type McpPrompt,
  type McpResource,
  type McpToolDef,
  type ServerInfo,
} from "../demo/mcpClient";
import { Panel, JsonView, Spinner, ErrorBanner } from "../components";
import { OriginBadge } from "../components/Provenance";
import { UnderTheHood } from "../components/UnderTheHood";

type Category = "Retrieval" | "SQL" | "Documents" | "Agent memory" | "Operations" | "Security & recovery" | "Other";

const CATEGORY: Record<string, Category> = {
  semantic_search: "Retrieval",
  vector_search: "Retrieval",
  answer_question: "Retrieval",
  execute_sql: "SQL",
  explain_query: "SQL",
  insert_document: "Documents",
  get_document: "Documents",
  delete_document: "Documents",
  list_buckets: "Documents",
  remember: "Agent memory",
  recall: "Agent memory",
  cluster_health: "Operations",
  server_stats: "Operations",
  reliability_status: "Operations",
  replication_status: "Operations",
  slow_queries: "Operations",
  audit_log: "Security & recovery",
  list_backups: "Security & recovery",
  durability_status: "Security & recovery",
  create_snapshot: "Security & recovery",
};
const CATEGORY_ORDER: Category[] = [
  "Retrieval",
  "SQL",
  "Documents",
  "Agent memory",
  "Operations",
  "Security & recovery",
  "Other",
];

/** Starting arguments so a first click returns something useful. */
const EXAMPLES: Record<string, Record<string, string>> = {
  semantic_search: { query: "disaster recovery", top_k: "5" },
  answer_question: { query: "What is our disaster recovery process?", top_k: "3" },
  execute_sql: { sql: "SELECT id, text FROM docs WHERE semantic_match(content, 'disaster recovery') LIMIT 5" },
  explain_query: { sql: "SELECT id, text FROM docs WHERE semantic_match(content, 'disaster recovery') LIMIT 5" },
  remember: { user_id: "showcase-visitor", text: "Visitor is evaluating NebulaDB for agent memory.", kind: "fact" },
  recall: { user_id: "showcase-visitor", query: "what is the visitor evaluating?" },
  audit_log: { limit: "20" },
};

type Access = "read" | "write" | "destructive";

function access(t: McpToolDef): Access {
  const a = t.annotations;
  if (a?.readOnlyHint) return "read";
  return a?.destructiveHint ? "destructive" : "write";
}

const ACCESS_CLS: Record<Access, string> = {
  read: "border-ok/40 bg-ok/10 text-ok",
  write: "border-warn/40 bg-warn/10 text-warn",
  destructive: "border-bad/40 bg-bad/10 text-bad",
};

/** The schema's non-null type: schemars emits `["integer","null"]` for Option<u32>. */
function baseType(s: JsonSchema): string {
  const t = Array.isArray(s.type) ? s.type.find((x) => x !== "null") : s.type;
  return t ?? "json";
}

/** Convert the form's strings into typed tool arguments, validating as we go. */
function buildArgs(schema: JsonSchema, raw: Record<string, string>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  const required = new Set(schema.required ?? []);
  for (const [name, prop] of Object.entries(schema.properties ?? {})) {
    const v = (raw[name] ?? "").trim();
    if (!v) {
      if (required.has(name)) throw new Error(`Missing required argument: ${name}`);
      continue;
    }
    switch (baseType(prop)) {
      case "integer":
      case "number": {
        const n = Number(v);
        if (!Number.isFinite(n)) throw new Error(`${name} must be a number`);
        out[name] = n;
        break;
      }
      case "boolean":
        out[name] = v === "true";
        break;
      case "array":
        out[name] = v
          .split(",")
          .map((x) => x.trim())
          .filter(Boolean)
          .map((x) => (baseType(prop.items ?? {}) === "string" ? x : Number(x)));
        break;
      case "string":
        out[name] = v;
        break;
      default:
        try {
          out[name] = JSON.parse(v);
        } catch {
          throw new Error(`${name} must be valid JSON`);
        }
    }
  }
  return out;
}

function ArgField({
  name,
  prop,
  required,
  value,
  onChange,
}: {
  name: string;
  prop: JsonSchema;
  required: boolean;
  value: string;
  onChange: (v: string) => void;
}) {
  const t = baseType(prop);
  const label = t === "array" ? `${baseType(prop.items ?? {})}[] · comma-separated` : t === "json" ? "JSON" : t;
  return (
    <label className="block">
      <div className="mb-1 flex items-baseline gap-2">
        <span className="font-mono text-[11px] font-semibold text-gray-800 dark:text-ink">{name}</span>
        <span className="font-mono text-[10px] text-gray-400 dark:text-faint">
          {label}
          {required ? " · required" : " · optional"}
        </span>
      </div>
      {t === "boolean" ? (
        <select className="input w-full !text-xs" value={value} onChange={(e) => onChange(e.target.value)}>
          <option value="">(server default)</option>
          <option value="true">true</option>
          <option value="false">false</option>
        </select>
      ) : t === "json" || name === "sql" || name === "text" ? (
        <textarea
          className="input w-full font-mono !text-xs"
          rows={t === "json" ? 3 : 2}
          value={value}
          placeholder={prop.description}
          onChange={(e) => onChange(e.target.value)}
        />
      ) : (
        <input
          className="input w-full !text-xs"
          value={value}
          placeholder={prop.description}
          onChange={(e) => onChange(e.target.value)}
        />
      )}
      {prop.description && (
        <p className="mt-0.5 text-[10px] leading-snug text-gray-500 dark:text-muted">{prop.description}</p>
      )}
    </label>
  );
}

export function McpTab() {
  const session = useRef(new McpSession()).current;
  const [status, setStatus] = useState<"connecting" | "connected" | "offline">("connecting");
  const [connectErr, setConnectErr] = useState<string | null>(null);
  const [info, setInfo] = useState<ServerInfo | null>(null);
  const [tools, setTools] = useState<McpToolDef[]>([]);
  const [resources, setResources] = useState<McpResource[]>([]);
  const [prompts, setPrompts] = useState<McpPrompt[]>([]);

  const [selected, setSelected] = useState<McpToolDef | null>(null);
  const [args, setArgs] = useState<Record<string, string>>({});
  const [result, setResult] = useState<{ value: unknown; isError: boolean } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [tookMs, setTookMs] = useState<number | null>(null);

  const [viewer, setViewer] = useState<{ title: string; value: unknown } | null>(null);

  const choose = (t: McpToolDef) => {
    setSelected(t);
    setResult(null);
    setErr(null);
    setTookMs(null);
    setArgs(EXAMPLES[t.name] ?? {});
  };

  const connect = async () => {
    setStatus("connecting");
    setConnectErr(null);
    try {
      const i = await session.connect();
      const [t, r, p] = await Promise.all([session.listTools(), session.listResources(), session.listPrompts()]);
      setInfo(i);
      setTools(t);
      setResources(r);
      setPrompts(p);
      setStatus("connected");
      if (!selected && t.length) choose(t.find((x) => x.name === "semantic_search") ?? t[0]);
    } catch (e) {
      setConnectErr((e as Error).message);
      setStatus("offline");
    }
  };

  useEffect(() => {
    void connect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const invoke = async () => {
    if (!selected) return;
    let parsed: Record<string, unknown>;
    try {
      parsed = buildArgs(selected.inputSchema, args);
    } catch (e) {
      setErr((e as Error).message);
      return;
    }
    const kind = access(selected);
    if (
      kind !== "read" &&
      !window.confirm(
        `${selected.name} ${kind === "destructive" ? "can overwrite or delete data" : "writes data"} in this NebulaDB cluster. Continue?`
      )
    ) {
      return;
    }
    setBusy(true);
    setErr(null);
    setResult(null);
    const t0 = performance.now();
    try {
      const r = await session.callTool(selected.name, parsed);
      setResult({ value: resultValue(r), isError: !!r.isError });
      setTookMs(Math.round(performance.now() - t0));
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const show = async (title: string, load: () => Promise<unknown>) => {
    try {
      setViewer({ title, value: await load() });
    } catch (e) {
      setViewer({ title, value: { error: e instanceof McpError ? e.message : (e as Error).message } });
    }
  };

  const grouped = useMemo(() => {
    const g = new Map<Category, McpToolDef[]>();
    for (const t of tools) {
      const c = CATEGORY[t.name] ?? "Other";
      g.set(c, [...(g.get(c) ?? []), t]);
    }
    return CATEGORY_ORDER.filter((c) => g.has(c)).map((c) => [c, g.get(c)!] as const);
  }, [tools]);

  const writes = tools.filter((t) => access(t) !== "read").length;

  return (
    <div className="space-y-5">
      <Panel
        title="Agent → MCP → NebulaDB"
        subtitle="A live Model Context Protocol session with nebula-mcp"
        action={status === "connected" ? <OriginBadge origin="live" from="POST /mcp" /> : undefined}
      >
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-2 font-mono text-[11px]">
            {["Browser", "nginx /mcp", "nebula-mcp", "nebula-server REST"].map((n, i, all) => (
              <span key={n} className="flex items-center gap-2">
                <span className="rounded border border-gray-200 bg-white px-2 py-1 text-gray-800 dark:border-edge dark:bg-carbon-900 dark:text-ink">
                  {n}
                </span>
                {i < all.length - 1 && <span className="text-gray-400 dark:text-faint">→</span>}
              </span>
            ))}
          </div>

          {status === "connecting" && <Spinner label="opening MCP session…" />}

          {status === "connected" && info && (
            <div className="grid gap-x-6 gap-y-1 text-[11px] sm:grid-cols-2">
              <div>
                <span className="eyebrow mr-2">Server</span>
                <code className="font-mono text-gray-800 dark:text-ink">
                  {info.serverInfo.name} {info.serverInfo.version}
                </code>
              </div>
              <div>
                <span className="eyebrow mr-2">Protocol</span>
                <code className="font-mono text-gray-800 dark:text-ink">{info.protocolVersion}</code>
              </div>
              <div>
                <span className="eyebrow mr-2">Session</span>
                <code className="font-mono text-gray-800 dark:text-ink">{session.id?.slice(0, 8) ?? "stateless"}…</code>
              </div>
              <div>
                <span className="eyebrow mr-2">Advertised</span>
                <span className="text-gray-700 dark:text-ink">
                  {tools.length} tools ({writes} write) · {resources.length} resources · {prompts.length} prompts
                </span>
              </div>
              {info.instructions && (
                <p className="mt-1 text-gray-500 dark:text-muted sm:col-span-2">{info.instructions}</p>
              )}
            </div>
          )}

          {status === "offline" && (
            <div className="space-y-2">
              <ErrorBanner err={`MCP server unreachable: ${connectErr}`} />
              <p className="text-xs text-gray-600 dark:text-muted">
                This page talks to <code className="font-mono">nebula-mcp</code> through <code className="font-mono">/mcp</code>.
                Run it next to nebula-server — <code className="font-mono">cargo run -p nebula-mcp</code> locally (the dev
                proxy expects <code className="font-mono">:8090</code>, override with{" "}
                <code className="font-mono">NEBULA_MCP_TARGET</code>), the <code className="font-mono">nebula-mcp</code>{" "}
                compose service, or the Helm chart's <code className="font-mono">showcase.mcp</code> sidecar.
              </p>
              <button className="btn !text-xs" onClick={connect}>
                Retry
              </button>
            </div>
          )}
        </div>
      </Panel>

      {status === "connected" && (
        <div className="grid gap-4 lg:grid-cols-[18rem_1fr]">
          {/* Catalogue: exactly what tools/list returned ----------------- */}
          <div className="space-y-3">
            {grouped.map(([cat, list]) => (
              <div key={cat}>
                <div className="eyebrow mb-1.5">{cat}</div>
                <ul className="space-y-1">
                  {list.map((t) => (
                    <li key={t.name}>
                      <button
                        onClick={() => choose(t)}
                        className={`w-full rounded border px-2.5 py-1.5 text-left transition-colors ${
                          selected?.name === t.name
                            ? "border-accent/50 bg-accent/10"
                            : "border-gray-200 hover:bg-gray-50 dark:border-edge dark:hover:bg-carbon-900"
                        }`}
                      >
                        <div className="flex items-center gap-1.5">
                          <span className="font-mono text-[11px] font-semibold text-gray-900 dark:text-ink">
                            {t.name}
                          </span>
                          {access(t) !== "read" && (
                            <span
                              className={`rounded border px-1 font-mono text-[8px] ${ACCESS_CLS[access(t)]}`}
                            >
                              {access(t).toUpperCase()}
                            </span>
                          )}
                        </div>
                        <p className="mt-0.5 line-clamp-2 text-[10px] leading-snug text-gray-500 dark:text-muted">
                          {t.description}
                        </p>
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>

          {/* Playground -------------------------------------------------- */}
          <div className="space-y-4">
            {selected && (
              <Panel
                title={selected.name}
                subtitle={selected.description}
                action={
                  <span
                    title="From the tool's MCP annotations (readOnlyHint / destructiveHint)"
                    className={`rounded border px-1.5 py-px font-mono text-[9px] font-semibold tracking-wider ${ACCESS_CLS[access(selected)]}`}
                  >
                    {access(selected).toUpperCase()}
                  </span>
                }
              >
                <div className="space-y-3">
                  {Object.keys(selected.inputSchema.properties ?? {}).length === 0 ? (
                    <p className="text-xs text-gray-500 dark:text-muted">This tool takes no arguments.</p>
                  ) : (
                    <div className="space-y-2">
                      {Object.entries(selected.inputSchema.properties ?? {}).map(([name, prop]) => (
                        <ArgField
                          key={name}
                          name={name}
                          prop={prop}
                          required={(selected.inputSchema.required ?? []).includes(name)}
                          value={args[name] ?? ""}
                          onChange={(v) => setArgs((a) => ({ ...a, [name]: v }))}
                        />
                      ))}
                    </div>
                  )}

                  <div className="flex items-center gap-3">
                    <button className="btn !text-xs" onClick={invoke} disabled={busy}>
                      {busy ? "Calling…" : "tools/call"}
                    </button>
                    {busy && <Spinner label="nebula-mcp → NebulaDB…" />}
                    {tookMs != null && !busy && (
                      <span className="font-mono text-[11px] text-gray-500 dark:text-muted">
                        returned in {tookMs} ms
                      </span>
                    )}
                  </div>

                  <ErrorBanner err={err} />

                  {result && (
                    <div>
                      <div className="eyebrow mb-1">
                        {result.isError ? "Tool error (isError: true) — NebulaDB's own diagnostic" : "Tool result"}
                      </div>
                      <JsonView value={result.value} />
                    </div>
                  )}
                </div>
              </Panel>
            )}

            <div className="grid gap-4 md:grid-cols-2">
              <Panel title="Resources" subtitle="Read-only context an agent can load without a tool call">
                <ul className="space-y-1.5">
                  {resources.map((r) => (
                    <li key={r.uri} className="flex items-start justify-between gap-2">
                      <div>
                        <code className="font-mono text-[11px] text-gray-900 dark:text-ink">{r.uri}</code>
                        <p className="text-[10px] text-gray-500 dark:text-muted">{r.description}</p>
                      </div>
                      <button
                        className="btn !px-2 !py-0.5 !text-[10px]"
                        onClick={() =>
                          show(r.uri, async () => {
                            const res = await session.readResource(r.uri);
                            const text = res.contents[0]?.text ?? "";
                            try {
                              return JSON.parse(text);
                            } catch {
                              return text;
                            }
                          })
                        }
                      >
                        read
                      </button>
                    </li>
                  ))}
                </ul>
              </Panel>
              <Panel title="Prompts" subtitle="Operator playbooks the server ships to agents">
                <ul className="space-y-1.5">
                  {prompts.map((p) => (
                    <li key={p.name} className="flex items-start justify-between gap-2">
                      <div>
                        <code className="font-mono text-[11px] text-gray-900 dark:text-ink">{p.name}</code>
                        <p className="text-[10px] text-gray-500 dark:text-muted">{p.description}</p>
                      </div>
                      <button
                        className="btn !px-2 !py-0.5 !text-[10px]"
                        onClick={() =>
                          show(p.name, async () => {
                            const res = await session.getPrompt(p.name);
                            return res.messages.map((m) => `${m.role}: ${m.content.text ?? ""}`).join("\n\n");
                          })
                        }
                      >
                        get
                      </button>
                    </li>
                  ))}
                </ul>
              </Panel>
            </div>

            {viewer && (
              <Panel title={viewer.title} action={<button className="btn !text-[10px]" onClick={() => setViewer(null)}>close</button>}>
                {typeof viewer.value === "string" ? (
                  <pre className="whitespace-pre-wrap font-mono text-[11px] text-gray-800 dark:text-ink">{viewer.value}</pre>
                ) : (
                  <JsonView value={viewer.value} />
                )}
              </Panel>
            )}

            <Panel title="Use these tools from your own agent" subtitle="Same server, any MCP client">
              <div className="space-y-2 text-xs text-gray-600 dark:text-muted">
                <p>Run nebula-mcp next to your NebulaDB, then point a client at it:</p>
                <pre className="overflow-x-auto rounded border border-gray-200 bg-gray-50 p-2 font-mono text-[11px] text-gray-800 dark:border-edge dark:bg-carbon-950 dark:text-ink">
{`cargo run -p nebula-mcp          # http://127.0.0.1:8090/mcp
claude mcp add --transport http nebuladb http://127.0.0.1:8090/mcp`}
                </pre>
                <p>
                  <code className="font-mono">examples/nebula-agents</code> runs this showcase's agent team with Claude
                  planning the tool calls, over the same MCP server.
                </p>
              </div>
            </Panel>

            <UnderTheHood filter={["/mcp"]} label="Show the JSON-RPC frames" />
          </div>
        </div>
      )}
    </div>
  );
}
