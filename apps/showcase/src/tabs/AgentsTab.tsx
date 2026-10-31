/**
 * AI agents and multi-agent orchestration (build brief sections 15-16).
 *
 * Honest split, stated on the page:
 *   - The *plan* each agent follows is scripted, not model-generated.
 *     There is no planner LLM in the loop here.
 *   - The *tool calls* are real. Each step invokes an MCP tool from
 *     demo/mcp.ts, which performs an actual REST call against this
 *     cluster. The response shown is what the server returned.
 *
 * That keeps the demo reproducible for a presenter while remaining
 * defensible to an engineer who opens the network tab.
 */
import { useState } from "react";
import { MCP_TOOLS, toolByName, type McpTool } from "../demo/mcp";
import { Panel, JsonView, Spinner, ErrorBanner } from "../components";
import { SimulationNotice, OriginBadge } from "../components/Provenance";
import { UnderTheHood } from "../components/UnderTheHood";
import { SeedChip, useSeeds } from "../source/common";

interface Step {
  tool: string;
  args: Record<string, string | number>;
  why: string;
}

interface Agent {
  id: string;
  name: string;
  role: string;
  blurb: string;
  icon: string;
  plan: Step[];
  /** Declared tool list, when it is wider than the scripted plan uses. */
  tools?: string[];
}

const AGENTS: Agent[] = [
  {
    id: "db",
    name: "Database Agent",
    role: "Schema and query specialist",
    icon: "⌘",
    blurb: "Inspects the corpus, runs read-only SQL and explains how the engine would execute a statement.",
    plan: [
      { tool: "list_buckets", args: {}, why: "discover what data exists before querying it" },
      { tool: "explain_query", args: { sql: "SELECT id, text FROM docs WHERE semantic_match(text, 'disaster recovery') LIMIT 5" }, why: "check the plan before execution" },
      { tool: "execute_sql", args: { sql: "SELECT id, text FROM docs WHERE semantic_match(text, 'disaster recovery') LIMIT 5" }, why: "run the validated statement" },
    ],
  },
  {
    id: "rag",
    name: "RAG Agent",
    role: "Knowledge retrieval specialist",
    icon: "❯",
    blurb: "Searches the knowledge base, retrieves supporting chunks and answers with citations.",
    plan: [
      { tool: "semantic_search", args: { query: "disaster recovery process", top_k: 5 }, why: "find candidate chunks" },
      { tool: "ai_answer", args: { query: "What is our disaster recovery process?", top_k: 5 }, why: "ground an answer in those chunks" },
    ],
  },
  {
    id: "sre",
    name: "SRE Agent",
    role: "Reliability and performance",
    icon: "▟",
    blurb: "Reads counters, durability state and slow queries to explain what the cluster is doing.",
    plan: [
      { tool: "query_metrics", args: {}, why: "establish a baseline from the counter snapshot" },
      { tool: "cluster_health", args: {}, why: "confirm persistence and WAL health" },
      { tool: "slow_queries", args: {}, why: "identify statements over the slow threshold" },
    ],
  },
  {
    id: "sec",
    name: "Security Agent",
    role: "Access and audit review",
    icon: "⚿",
    blurb: "Reviews the audit trail and authentication counters for anomalies.",
    plan: [
      { tool: "audit_log", args: { limit: 50 }, why: "read recent authenticated calls" },
      { tool: "query_metrics", args: {}, why: "check auth_failures and rate_limited counters" },
    ],
  },
  {
    id: "backup",
    name: "Backup Agent",
    role: "Recovery readiness",
    icon: "⛁",
    blurb: "Verifies that backups exist, are recent, and that the WAL can support recovery.",
    plan: [
      { tool: "backup_status", args: {}, why: "enumerate known backups" },
      { tool: "cluster_health", args: {}, why: "confirm the WAL range needed for point-in-time recovery" },
    ],
  },
];

/** The supervisor scenario from section 16, delegating across agents. */
const SUPERVISOR: { question: string; delegates: Array<{ agent: string; step: Step }> } = {
  question: "Why has our customer API become slower?",
  delegates: [
    { agent: "SRE Agent", step: { tool: "query_metrics", args: {}, why: "quantify request and error volume" } },
    { agent: "SRE Agent", step: { tool: "slow_queries", args: {}, why: "find the slowest statements" } },
    { agent: "Database Agent", step: { tool: "list_buckets", args: {}, why: "check corpus size for index pressure" } },
    { agent: "SRE Agent", step: { tool: "cluster_health", args: {}, why: "rule out WAL or persistence stalls" } },
    { agent: "RAG Agent", step: { tool: "semantic_search", args: { query: "API latency runbook", top_k: 3 }, why: "pull the relevant runbook" } },
  ],
};


/** Source Supplies preset (demo data): the same team as examples/nebula-agents --team source. */
const BRIGHTWAY_OLD = ["FC-010", "WR-020", "MP-050", "JR-400", "HT-300", "HS-060"].map((s) => `'${s}'`).join(", ");
const SOURCE_AGENTS: Agent[] = [
  {
    id: "ss-product",
    name: "Product Advisor",
    role: "Products and greener swaps",
    icon: "❀",
    tools: ["execute_sql", "semantic_search", "get_document"],
    blurb: "Finds what an account buys now, then the in-stock products whose replaces_sku points at those lines.",
    plan: [
      { tool: "execute_sql", args: { sql: "SELECT * FROM ss_orders WHERE semantic_match(text, 'order lines') AND account_id = 'BRIGHTWAY' AND month >= '2026-08' LIMIT 215" }, why: "what Brightway FM buys now" },
      { tool: "execute_sql", args: { sql: `SELECT * FROM ss_products WHERE semantic_match(text, 'greener swap') AND replaces_sku IN (${BRIGHTWAY_OLD}) AND in_stock = TRUE LIMIT 39` }, why: "greener products that replace those lines" },
    ],
  },
  {
    id: "ss-safety",
    name: "Safety Agent",
    role: "SDS and COSHH answers",
    icon: "⚠",
    tools: ["semantic_search (ss_sds, ss_guides only)", "get_document"],
    blurb: "Answers only from the demo SDS and guides; its search tool rejects every other bucket in code.",
    plan: [
      { tool: "semantic_search", args: { query: "PPE for the DS-200 descaler, exposure controls", bucket: "ss_sds", top_k: 5 }, why: "Section 8 of the demo SDS" },
      { tool: "semantic_search", args: { query: "descaling toilets and urinals safely", bucket: "ss_guides", top_k: 3 }, why: "safe-use steps from the guides" },
    ],
  },
  {
    id: "ss-order",
    name: "Order Agent",
    role: "Reorder drafts",
    icon: "↻",
    tools: ["execute_sql", "semantic_search", "insert_document (ss_order_drafts, status = draft only)"],
    blurb: "Reads a site's delivery day and order history and drafts next month's basket. It can only write drafts.",
    plan: [
      { tool: "execute_sql", args: { sql: "SELECT * FROM ss_accounts WHERE semantic_match(text, 'site delivery day') AND account_id = 'KINGSMEAD' AND site = 'North site' LIMIT 14" }, why: "the site's delivery day" },
      { tool: "execute_sql", args: { sql: "SELECT * FROM ss_orders WHERE semantic_match(text, 'order lines') AND account_id = 'KINGSMEAD' AND site = 'North site' AND month >= '2026-05' LIMIT 215" }, why: "six months of order lines to forecast from" },
      { tool: "execute_sql", args: { sql: "SELECT * FROM ss_products WHERE semantic_match(text, 'stock and lead time') AND sku IN ('FC-110', 'WR-220', 'JR-410', 'HT-300', 'HS-270', 'BG-610') LIMIT 39" }, why: "stock and lead times for the basket" },
    ],
  },
  {
    id: "ss-washroom",
    name: "Washroom Agent",
    role: "Triage and missed visits",
    icon: "⌖",
    tools: ["execute_sql", "semantic_search", "get_document"],
    blurb: "Finds missed sanitary visits and the past tickets closest to a new issue.",
    plan: [
      { tool: "execute_sql", args: { sql: "SELECT * FROM ss_washroom_jobs WHERE semantic_match(text, 'missed sanitary visit') AND status = 'missed' AND service = 'sanitary' AND visit_date >= '2026-10-01' LIMIT 39" }, why: "sanitary visits missed in the last 30 days" },
      { tool: "semantic_search", args: { query: "sanitary bin not changed for weeks and smells", bucket: "ss_washroom_jobs", top_k: 5 }, why: "how similar tickets were fixed" },
    ],
  },
  {
    id: "ss-sustainability",
    name: "Sustainability Agent",
    role: "CO2e and savings",
    icon: "♻",
    tools: ["execute_sql", "get_document"],
    blurb: "Adds up an account's CO2e and spend per site from its orders and the products' CO2e per pack.",
    plan: [
      { tool: "execute_sql", args: { sql: "SELECT * FROM ss_orders WHERE semantic_match(text, 'order lines') AND account_id = 'BRIGHTWAY' AND month >= '2026-05' LIMIT 215" }, why: "six months of Brightway order lines" },
      { tool: "execute_sql", args: { sql: `SELECT * FROM ss_products WHERE semantic_match(text, 'co2e per pack') AND sku IN (${BRIGHTWAY_OLD}) LIMIT 39` }, why: "CO2e and price per pack for those lines" },
    ],
  },
];

const SOURCE_SUPERVISOR: typeof SUPERVISOR = {
  question: "Brightway FM wants to cut CO2 by 20% at their 4 office sites without spending more. What can we do this month?",
  delegates: [
    { agent: "Sustainability Agent", step: SOURCE_AGENTS[4].plan[0] },
    { agent: "Sustainability Agent", step: SOURCE_AGENTS[4].plan[1] },
    { agent: "Product Advisor", step: SOURCE_AGENTS[0].plan[1] },
    { agent: "Order Agent", step: { tool: "execute_sql", args: { sql: "SELECT * FROM ss_accounts WHERE semantic_match(text, 'site delivery day') AND account_id = 'BRIGHTWAY' LIMIT 14" }, why: "delivery days for the 4 sites" } },
  ],
};

interface RunStep {
  tool: string;
  agent?: string;
  why: string;
  status: "pending" | "running" | "ok" | "error";
  result?: unknown;
  error?: string;
  tookMs?: number;
}

function SourceSeedChip() {
  const seed = useSeeds(["products", "sds", "guides", "accounts", "orders", "washroom"]);
  return <SeedChip seed={seed} />;
}

export function AgentsTab() {
  const [team, setTeam] = useState<"ops" | "source">("ops");
  const agents = team === "source" ? SOURCE_AGENTS : AGENTS;
  const supervisor = team === "source" ? SOURCE_SUPERVISOR : SUPERVISOR;
  const [selected, setSelected] = useState<Agent>(AGENTS[0]);
  const [steps, setSteps] = useState<RunStep[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [mode, setMode] = useState<"single" | "supervisor">("single");

  const execute = async (plan: Array<{ agent?: string; step: Step }>) => {
    setBusy(true);
    setErr(null);
    const initial: RunStep[] = plan.map((p) => ({
      tool: p.step.tool,
      agent: p.agent,
      why: p.step.why,
      status: "pending",
    }));
    setSteps(initial);

    for (let i = 0; i < plan.length; i++) {
      const { step } = plan[i];
      const tool: McpTool | undefined = toolByName(step.tool);
      setSteps((s) => s.map((x, j) => (j === i ? { ...x, status: "running" } : x)));
      const t0 = performance.now();
      try {
        if (!tool?.run) throw new Error(`${step.tool} has no backing endpoint`);
        const out = await tool.run(step.args);
        const took = Math.round(performance.now() - t0);
        setSteps((s) =>
          s.map((x, j) => (j === i ? { ...x, status: "ok", result: out, tookMs: took } : x))
        );
      } catch (e) {
        const took = Math.round(performance.now() - t0);
        setSteps((s) =>
          s.map((x, j) =>
            j === i ? { ...x, status: "error", error: (e as Error).message, tookMs: took } : x
          )
        );
      }
    }
    setBusy(false);
  };

  return (
    <div className="space-y-5">
      <SimulationNotice
        what="Agent plans are scripted; the tool calls are real"
        why="There is no planner LLM choosing these steps — each agent follows a fixed sequence so a demo is reproducible. Every step, however, invokes a real MCP tool that performs an actual REST call against this cluster, and the responses below are exactly what the server returned."
      />

      <div className="flex flex-wrap items-center gap-2">
        <span className="eyebrow mr-1">Preset</span>
        {(["ops", "source"] as const).map((t) => (
          <button
            key={t}
            className={team === t ? "btn !text-xs" : "btn-secondary !text-xs"}
            onClick={() => {
              setTeam(t);
              setSelected((t === "source" ? SOURCE_AGENTS : AGENTS)[0]);
              setSteps([]);
            }}
          >
            {t === "ops" ? "NebulaDB ops" : "Source Supplies"}
          </button>
        ))}
        {team === "source" && (
          <>
            <SourceSeedChip />
            <span className="text-xs text-gray-500 dark:text-muted">Demo data — not real Source Supplies records.</span>
          </>
        )}
      </div>

      <div className="flex gap-2">
        <button
          className={mode === "single" ? "btn !text-xs" : "btn-secondary !text-xs"}
          onClick={() => { setMode("single"); setSteps([]); }}
        >
          Single agent
        </button>
        <button
          className={mode === "supervisor" ? "btn !text-xs" : "btn-secondary !text-xs"}
          onClick={() => { setMode("supervisor"); setSteps([]); }}
        >
          Multi-agent scenario
        </button>
      </div>

      {mode === "single" ? (
        <div className="grid gap-4 lg:grid-cols-[16rem_1fr]">
          <ul className="space-y-1.5">
            {agents.map((a) => (
              <li key={a.id}>
                <button
                  onClick={() => { setSelected(a); setSteps([]); }}
                  className={`w-full rounded border px-2.5 py-2 text-left transition-colors
                    ${
                      selected.id === a.id
                        ? "border-accent/50 bg-accent/10"
                        : "border-gray-200 hover:bg-gray-50 dark:border-edge dark:hover:bg-carbon-900"
                    }`}
                >
                  <div className="flex items-center gap-2">
                    <span className="text-sm">{a.icon}</span>
                    <span className="text-xs font-semibold text-gray-900 dark:text-ink">{a.name}</span>
                  </div>
                  <p className="mt-0.5 text-[10px] text-gray-500 dark:text-muted">{a.role}</p>
                </button>
              </li>
            ))}
          </ul>

          <div className="space-y-4">
            <Panel
              title={selected.name}
              subtitle={selected.blurb}
              action={
                <button
                  className="btn !text-xs"
                  disabled={busy}
                  onClick={() => execute(selected.plan.map((step) => ({ step })))}
                >
                  {busy ? "Running…" : "Run agent"}
                </button>
              }
            >
              <div className="space-y-1.5">
                <div className="eyebrow">Tools available to this agent</div>
                <div className="flex flex-wrap gap-1.5">
                  {(selected.tools ?? [...new Set(selected.plan.map((s) => s.tool))]).map((tool) => (
                    <span
                      key={tool}
                      className="rounded border border-gray-200 bg-gray-50 px-1.5 py-px font-mono text-[10px] text-gray-700 dark:border-edge dark:bg-carbon-950 dark:text-muted"
                    >
                      {tool}
                    </span>
                  ))}
                </div>
              </div>
            </Panel>
            <ErrorBanner err={err} />
            <StepList steps={steps} busy={busy} />
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          <Panel
            title="Supervisor orchestration"
            subtitle={supervisor.question}
            action={
              <button
                className="btn !text-xs"
                disabled={busy}
                onClick={() => execute(supervisor.delegates)}
              >
                {busy ? "Investigating…" : "Run investigation"}
              </button>
            }
          >
            <div className="flex flex-wrap items-center gap-2 font-mono text-[11px]">
              <span className="rounded border border-accent/40 bg-accent/10 px-2 py-1 text-accent">
                Supervisor
              </span>
              <span className="text-gray-400 dark:text-faint">delegates to</span>
              {[...new Set(supervisor.delegates.map((d) => d.agent))].map((a) => (
                <span
                  key={a}
                  className="rounded border border-gray-200 bg-white px-2 py-1 text-gray-800 dark:border-edge dark:bg-carbon-900 dark:text-ink"
                >
                  {a}
                </span>
              ))}
            </div>
          </Panel>
          <StepList steps={steps} busy={busy} />
        </div>
      )}

      <Panel title="Tool catalogue" subtitle="What any agent on this cluster may call">
        <div className="flex flex-wrap gap-1.5">
          {MCP_TOOLS.map((t) => (
            <span
              key={t.name}
              title={t.backing ?? "no backing endpoint"}
              className={`rounded border px-1.5 py-px font-mono text-[10px]
                ${
                  t.backing
                    ? "border-ok/30 bg-ok/5 text-gray-700 dark:text-muted"
                    : "border-warn/40 bg-warn/10 text-warn"
                }`}
            >
              {t.name}
            </span>
          ))}
        </div>
      </Panel>

      <UnderTheHood filter={["/api/v1/"]} />
    </div>
  );
}

function StepList({ steps, busy }: { steps: RunStep[]; busy: boolean }) {
  if (steps.length === 0) {
    return (
      <p className="text-xs text-gray-500 dark:text-muted">
        Run an agent to watch it call MCP tools against this cluster.
      </p>
    );
  }
  return (
    <div className="space-y-2">
      {steps.map((s, i) => (
        <StepCard key={`${s.tool}-${i}`} s={s} n={i + 1} />
      ))}
      {busy && <Spinner label="agent working…" />}
    </div>
  );
}

function StepCard({ s, n }: { s: RunStep; n: number }) {
  const [open, setOpen] = useState(false);
  const dot =
    s.status === "ok" ? "dot-ok" : s.status === "error" ? "dot-bad" : s.status === "running" ? "dot-warn" : "dot-idle";
  return (
    <div className="rounded border border-gray-200 dark:border-edge">
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-2.5 px-2.5 py-2 text-left"
      >
        <span className={`dot ${dot} ${s.status === "running" ? "animate-pulseok" : ""}`} />
        <span className="font-mono text-[10px] text-gray-400 dark:text-faint">{n}</span>
        {s.agent && (
          <span className="rounded border border-gray-200 px-1 font-mono text-[9px] text-gray-500 dark:border-edge dark:text-muted">
            {s.agent}
          </span>
        )}
        <span className="font-mono text-[11px] font-semibold text-gray-900 dark:text-ink">{s.tool}</span>
        <span className="truncate text-[10px] text-gray-500 dark:text-muted">— {s.why}</span>
        {s.tookMs != null && (
          <span className="ml-auto shrink-0 font-mono text-[10px] tabular-nums text-gray-400 dark:text-faint">
            {s.tookMs}ms
          </span>
        )}
      </button>
      {open && (s.result != null || s.error) && (
        <div className="border-t border-gray-100 p-2 dark:border-edge">
          {s.error ? (
            <pre className="whitespace-pre-wrap font-mono text-[11px] text-bad">{s.error}</pre>
          ) : (
            <>
              <div className="eyebrow mb-1 flex items-center gap-2">
                Tool response <OriginBadge origin="live" from={toolByName(s.tool)?.backing ?? ""} />
              </div>
              <JsonView value={s.result} />
            </>
          )}
        </div>
      )}
    </div>
  );
}
