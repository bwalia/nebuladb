/**
 * MCP showcase + playground (build brief section 17).
 *
 * The MCP *transport* is not implemented on the server, so this page
 * is explicit about that: it renders the tool catalogue an MCP server
 * would expose, and executing a tool performs the real REST call that
 * would sit behind it. The trace panel then shows the genuine request
 * and response.
 *
 * Tools with no backing endpoint (rebalance_status) refuse to run and
 * say so, rather than returning a plausible-looking fabrication.
 */
import { useState } from "react";
import { MCP_TOOLS, type McpTool } from "../demo/mcp";
import { Panel, JsonView, Spinner, ErrorBanner } from "../components";
import { OriginBadge, SimulationNotice } from "../components/Provenance";
import { UnderTheHood } from "../components/UnderTheHood";

const CATEGORY_LABEL: Record<McpTool["category"], string> = {
  retrieval: "Retrieval",
  sql: "SQL",
  data: "Data",
  ops: "Operations",
};

const PERM_CLS: Record<McpTool["permission"], string> = {
  read: "border-ok/40 bg-ok/10 text-ok",
  write: "border-warn/40 bg-warn/10 text-warn",
  admin: "border-bad/40 bg-bad/10 text-bad",
};

export function McpTab() {
  const [selected, setSelected] = useState<McpTool>(MCP_TOOLS[0]);
  const [args, setArgs] = useState<Record<string, string>>({});
  const [result, setResult] = useState<unknown>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [tookMs, setTookMs] = useState<number | null>(null);

  const choose = (t: McpTool) => {
    setSelected(t);
    setResult(null);
    setErr(null);
    setTookMs(null);
    const seed: Record<string, string> = {};
    t.params.forEach((p) => {
      if (p.default != null) seed[p.name] = String(p.default);
    });
    setArgs(seed);
  };

  const invoke = async () => {
    if (!selected.run) {
      setErr(
        `${selected.name} has no backing endpoint on this server yet, so the showcase will not fabricate a response.`
      );
      return;
    }
    const missing = selected.params.filter((p) => p.required && !args[p.name]?.trim());
    if (missing.length) {
      setErr(`Missing required argument: ${missing.map((m) => m.name).join(", ")}`);
      return;
    }
    setBusy(true);
    setErr(null);
    setResult(null);
    const t0 = performance.now();
    try {
      const out = await selected.run(args);
      setResult(out);
      setTookMs(Math.round(performance.now() - t0));
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const grouped = MCP_TOOLS.reduce<Record<string, McpTool[]>>((acc, t) => {
    (acc[t.category] ||= []).push(t);
    return acc;
  }, {});

  return (
    <div className="space-y-5">
      <SimulationNotice
        what="MCP transport is stubbed; the tools are real"
        why="NebulaDB does not expose an MCP server endpoint yet. This page renders the tool catalogue such a server would advertise, and invoking a tool performs the actual REST call listed under 'Backed by' — so responses are genuine, even though no MCP handshake occurs."
      />

      <Panel title="Agent → MCP → NebulaDB" subtitle="Every tool maps to a concrete endpoint">
        <div className="flex flex-wrap items-center gap-2 font-mono text-[11px]">
          {["AI Agent", "MCP", "NebulaDB"].map((n, i) => (
            <span key={n} className="flex items-center gap-2">
              <span className="rounded border border-gray-200 bg-white px-2 py-1 text-gray-800 dark:border-edge dark:bg-carbon-900 dark:text-ink">
                {n}
              </span>
              {i < 2 && <span className="text-gray-400 dark:text-faint">→</span>}
            </span>
          ))}
          <span className="ml-2 text-gray-500 dark:text-muted">
            {MCP_TOOLS.filter((t) => t.backing).length} of {MCP_TOOLS.length} tools are wired to a live endpoint
          </span>
        </div>
      </Panel>

      <div className="grid gap-4 lg:grid-cols-[18rem_1fr]">
        {/* Catalogue -------------------------------------------------- */}
        <div className="space-y-3">
          {Object.entries(grouped).map(([cat, tools]) => (
            <div key={cat}>
              <div className="eyebrow mb-1.5">{CATEGORY_LABEL[cat as McpTool["category"]]}</div>
              <ul className="space-y-1">
                {tools.map((t) => (
                  <li key={t.name}>
                    <button
                      onClick={() => choose(t)}
                      className={`w-full rounded border px-2.5 py-1.5 text-left transition-colors
                        ${
                          selected.name === t.name
                            ? "border-accent/50 bg-accent/10"
                            : "border-gray-200 hover:bg-gray-50 dark:border-edge dark:hover:bg-carbon-900"
                        }`}
                    >
                      <div className="flex items-center gap-1.5">
                        <span className="font-mono text-[11px] font-semibold text-gray-900 dark:text-ink">
                          {t.name}
                        </span>
                        {!t.backing && (
                          <span className="rounded border border-warn/40 bg-warn/10 px-1 font-mono text-[8px] text-warn">
                            NO ENDPOINT
                          </span>
                        )}
                      </div>
                      <p className="mt-0.5 text-[10px] leading-snug text-gray-500 dark:text-muted">
                        {t.summary}
                      </p>
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>

        {/* Playground ------------------------------------------------- */}
        <div className="space-y-4">
          <Panel
            title={selected.name}
            subtitle={selected.description}
            action={
              <span
                className={`rounded border px-1.5 py-px font-mono text-[9px] font-semibold tracking-wider ${PERM_CLS[selected.permission]}`}
              >
                {selected.permission.toUpperCase()}
              </span>
            }
          >
            <div className="space-y-3">
              <div className="flex items-center gap-2 text-[11px]">
                <span className="eyebrow">Backed by</span>
                {selected.backing ? (
                  <>
                    <code className="font-mono text-gray-800 dark:text-ink">{selected.backing}</code>
                    <OriginBadge origin="live" from={selected.backing} />
                  </>
                ) : (
                  <span className="text-warn">no server endpoint — cannot execute</span>
                )}
              </div>

              {selected.params.length === 0 ? (
                <p className="text-xs text-gray-500 dark:text-muted">This tool takes no arguments.</p>
              ) : (
                <div className="space-y-2">
                  {selected.params.map((p) => (
                    <label key={p.name} className="block">
                      <div className="mb-1 flex items-baseline gap-2">
                        <span className="font-mono text-[11px] font-semibold text-gray-800 dark:text-ink">
                          {p.name}
                        </span>
                        <span className="font-mono text-[10px] text-gray-400 dark:text-faint">
                          {p.type}
                          {p.required ? " · required" : " · optional"}
                        </span>
                      </div>
                      <input
                        className="input w-full !text-xs"
                        value={args[p.name] ?? ""}
                        placeholder={p.description}
                        onChange={(e) => setArgs((a) => ({ ...a, [p.name]: e.target.value }))}
                      />
                    </label>
                  ))}
                </div>
              )}

              <div className="flex items-center gap-3">
                <button className="btn !text-xs" onClick={invoke} disabled={busy || !selected.run}>
                  {busy ? "Invoking…" : "Invoke tool"}
                </button>
                {busy && <Spinner label="calling NebulaDB…" />}
                {tookMs != null && !busy && (
                  <span className="font-mono text-[11px] text-gray-500 dark:text-muted">
                    returned in {tookMs} ms
                  </span>
                )}
              </div>

              <ErrorBanner err={err} />

              {result != null && (
                <div>
                  <div className="eyebrow mb-1">Tool response</div>
                  <JsonView value={result} />
                </div>
              )}
            </div>
          </Panel>

          <UnderTheHood filter={["/api/v1/"]} />
        </div>
      </div>
    </div>
  );
}
