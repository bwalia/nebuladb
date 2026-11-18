/**
 * Customer Report Desk — from the Source Supplies discovery call.
 *
 * Dave: Merlin has the data but custom reports take days (and cost) from the
 * vendor. Buyers ask “what did Ashby buy last quarter?” in plain English.
 *
 * Demo: pick an account + period → SQL over ss_orders → tabulated spend and
 * a short grounded summary. Replaces the “email Merlin at half five” loop.
 */
import { useCallback, useState } from "react";
import { ApiError } from "../api";
import { ErrorBanner, Panel, Spinner } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { GroundedChat, type ContextDoc } from "../rag/GroundedChat";
import { lit, runSql } from "../rag/sql";
import { B, ORDER_MONTHS, exhaustiveLimit } from "./data/common";
import { ACCOUNTS, ORDER_DOCS } from "./data/accounts";
import { PRODUCT_BY_SKU } from "./data/products";
import { Chips, DemoNote, SeedChip, gbp, metaOf, num, str, ukMonth, useSeeds } from "./common";

const ORDERS_LIMIT = exhaustiveLimit(ORDER_DOCS.length);

const PERIODS = [
  { label: "Last 3 months", months: ORDER_MONTHS.slice(-3) },
  { label: "Last 6 months", months: [...ORDER_MONTHS] },
  { label: "Latest month only", months: ORDER_MONTHS.slice(-1) },
];

const SCENARIOS = [
  { label: "Ashby spend", account: "ASHBY", period: "Last 3 months", q: "What did Ashby Contract Cleaning buy in the last quarter, by site?" },
  { label: "Kingsmead paper", account: "KINGSMEAD", period: "Last 3 months", q: "How much recycled paper and washroom did Kingsmead take?" },
  { label: "Harbour care", account: "HARBOUR", period: "Last 6 months", q: "Summarise Harbour Care Homes orders for the half year." },
];

interface LineAgg {
  sku: string;
  name: string;
  site: string;
  qty: number;
  spend: number;
}

const SYSTEM =
  "You are Jordan AI for Source Supplies, answering a customer-spend question for an internal sales or account person. " +
  "Use ONLY the order summary documents provided, citing them as [n]. Quote GBP excluding VAT. " +
  "Use UK English. Do not invent SKUs, quantities or sites. If something is missing, say so.";

export function ReportDeskTab() {
  const seed = useSeeds(["products", "accounts", "orders"]);
  const [account, setAccount] = useState(SCENARIOS[0].account);
  const [periodLabel, setPeriodLabel] = useState(SCENARIOS[0].period);
  const [q, setQ] = useState(SCENARIOS[0].q);
  const [scenario, setScenario] = useState(SCENARIOS[0].label);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [lines, setLines] = useState<LineAgg[]>([]);
  const [docs, setDocs] = useState<ContextDoc[]>([]);
  const [runId, setRunId] = useState(0);

  const period = PERIODS.find((p) => p.label === periodLabel) ?? PERIODS[0];
  const accountName = ACCOUNTS.find((a) => a.id === account)?.name ?? account;

  const run = async () => {
    setBusy(true);
    setErr(null);
    setDocs([]);
    setLines([]);
    const trace: PipelineStep[] = [];
    const push = (s: PipelineStep) => {
      trace.push(s);
      setSteps([...trace]);
      return trace.length - 1;
    };
    const done = (i: number, patch: Partial<PipelineStep>) => {
      trace[i] = { ...trace[i], ...patch, status: "ok" };
      setSteps([...trace]);
    };
    try {
      const months = period.months.map((m) => lit(m)).join(", ");
      const sql =
        `SELECT * FROM ${B.orders}\n WHERE semantic_match(text, ${lit(q || "customer order spend")})` +
        `\n   AND account_id = ${lit(account)}\n   AND month IN (${months})\n LIMIT ${ORDERS_LIMIT}`;
      const i = push({ label: `Orders for ${accountName}`, detail: sql, status: "running" });
      const res = await runSql(sql);
      done(i, { rowsOut: res.rows.length, ms: res.tookMs });

      const agg = new Map<string, LineAgg>();
      for (const r of res.rows) {
        const m = metaOf(r);
        const sku = str(m.sku);
        const site = str(m.site);
        const qty = num(m.qty);
        const key = `${site}|${sku}`;
        const p = PRODUCT_BY_SKU[sku];
        const spend = qty * (p?.price_gbp ?? 0);
        const prev = agg.get(key);
        if (prev) {
          prev.qty += qty;
          prev.spend += spend;
        } else {
          agg.set(key, { sku, name: p?.name ?? sku, site, qty, spend });
        }
      }
      const sorted = [...agg.values()].sort((a, b) => b.spend - a.spend);
      setLines(sorted);

      const total = sorted.reduce((s, l) => s + l.spend, 0);
      const bySite = new Map<string, number>();
      for (const l of sorted) bySite.set(l.site, (bySite.get(l.site) ?? 0) + l.spend);

      const ctx: ContextDoc[] = [
        {
          id: "summary",
          label: "Summary",
          text:
            `Account ${accountName} (${account}). Period ${period.months.map(ukMonth).join(", ")}. ` +
            `${sorted.length} SKU-site lines. Total spend ${gbp(total)} ex VAT. ` +
            `Sites: ${[...bySite.entries()].map(([s, v]) => `${s} ${gbp(v)}`).join("; ")}.`,
          metadata: { kind: "summary" },
        },
        ...sorted.slice(0, 40).map((l, idx) => ({
          id: `line-${idx}`,
          label: `${l.sku} @ ${l.site}`,
          text: `${l.site}: ${l.qty} × ${l.sku} ${l.name} = ${gbp(l.spend)} ex VAT.`,
          metadata: { sku: l.sku, site: l.site, qty: l.qty, spend: l.spend },
        })),
      ];
      setDocs(ctx);
      push({
        label: "Roll up spend by site and SKU",
        detail: `Total ${gbp(total)} · ${bySite.size} sites · no Merlin report ticket needed`,
        rowsIn: res.rows.length,
        rowsOut: sorted.length,
        status: "ok",
      });
      setRunId((n) => n + 1);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.status}: ${e.body || e.code}` : (e as Error).message);
      if (trace.length) {
        trace[trace.length - 1] = { ...trace[trace.length - 1], status: "error" };
        setSteps([...trace]);
      }
    } finally {
      setBusy(false);
    }
  };

  const getContext = useCallback(async () => docs, [docs]);
  const total = lines.reduce((s, l) => s + l.spend, 0);

  return (
    <div className="space-y-4">
      <DemoNote extra="Plain-English customer spend without waiting days for a Merlin custom report." />
      <ArchitecturePanel id="ss_reports" />
      <Panel
        title="Customer Report Desk"
        subtitle="Ask what a trade account bought — Jordan answers from order history, not a vendor ticket."
        action={<SeedChip seed={seed} />}
      >
        <Chips
          items={SCENARIOS}
          active={scenario}
          onPick={(s) => {
            setScenario(s.label);
            setAccount(s.account);
            setPeriodLabel(s.period);
            setQ(s.q);
          }}
        />
        <div className="flex flex-wrap gap-3 items-end mb-3">
          <label className="text-xs">
            <span className="block font-medium mb-1">Account</span>
            <select className="input" value={account} onChange={(e) => setAccount(e.target.value)}>
              {ACCOUNTS.map((a) => (
                <option key={a.id} value={a.id}>
                  {a.name}
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs">
            <span className="block font-medium mb-1">Period</span>
            <select className="input" value={periodLabel} onChange={(e) => setPeriodLabel(e.target.value)}>
              {PERIODS.map((p) => (
                <option key={p.label} value={p.label}>
                  {p.label}
                </option>
              ))}
            </select>
          </label>
        </div>
        <label className="block mb-3">
          <span className="block text-xs font-medium mb-1">Question</span>
          <textarea className="code-input w-full h-16" value={q} onChange={(e) => setQ(e.target.value)} />
        </label>
        <button type="button" className="btn" disabled={busy || !seed.ready} onClick={() => void run()}>
          {busy ? <Spinner /> : null}
          {busy ? "Building…" : "Build report"}
        </button>
      </Panel>

      <ErrorBanner err={err} />
      {steps.length > 0 && <PipelineTrace steps={steps} />}

      {lines.length > 0 && (
        <Panel title={`${accountName} · ${periodLabel}`} subtitle={`Total ${gbp(total)} ex VAT · ${lines.length} lines`}>
          <div className="overflow-x-auto -mx-1 max-h-80 overflow-y-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs uppercase tracking-wider text-gray-500 dark:text-muted border-b border-gray-200 dark:border-edge sticky top-0 bg-white dark:bg-carbon-950">
                  <th className="py-2 pr-3">Site</th>
                  <th className="py-2 pr-3">SKU</th>
                  <th className="py-2 pr-3">Product</th>
                  <th className="py-2 pr-3">Qty</th>
                  <th className="py-2">Spend</th>
                </tr>
              </thead>
              <tbody>
                {lines.slice(0, 60).map((l) => (
                  <tr key={`${l.site}-${l.sku}`} className="border-b border-gray-100 dark:border-edge/60">
                    <td className="py-1.5 pr-3">{l.site}</td>
                    <td className="py-1.5 pr-3 font-mono text-xs">{l.sku}</td>
                    <td className="py-1.5 pr-3">{l.name}</td>
                    <td className="py-1.5 pr-3 font-mono">{l.qty}</td>
                    <td className="py-1.5 font-mono">{gbp(l.spend)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}

      {docs.length > 0 && (
        <GroundedChat
          title="Jordan AI summary"
          subtitle="Grounded on the order roll-up above — no Merlin report ticket"
          system={SYSTEM}
          getContext={getContext}
          resetKey={String(runId)}
          suggestions={[q, "Which site spent the most?", "Top three SKUs by spend?"]}
          placeholder="Ask about this report…"
        />
      )}

      <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "ss_orders"]} />
    </div>
  );
}
