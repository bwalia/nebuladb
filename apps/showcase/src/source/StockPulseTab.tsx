/**
 * Stock Pulse — from the Source Supplies discovery call.
 *
 * Dave's pain: no live stock in Merlin, but popular (~95% repeat) lines must
 * never run short. He was pasting Merlin averages into ChatGPT to draft POs.
 *
 * Demo: pull recent order volume → flag core movers that look short → draft a
 * supplier PO from a 3-month average. Nothing is ordered until a person confirms.
 */
import { useState } from "react";
import { ApiError } from "../api";
import { ErrorBanner, Panel, Spinner } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { lit, runSql } from "../rag/sql";
import { B, ORDER_MONTHS, exhaustiveLimit } from "./data/common";
import { ORDER_DOCS } from "./data/accounts";
import { PRODUCT_BY_SKU } from "./data/products";
import { Chips, DemoNote, SeedChip, metaOf, num, str, useSeeds } from "./common";

const ORDERS_LIMIT = exhaustiveLimit(ORDER_DOCS.length);
const RECENT = ORDER_MONTHS.slice(-3);

interface PulseRow {
  sku: string;
  name: string;
  packs3m: number;
  avgMonth: number;
  inStock: boolean;
  leadDays: number;
  suggested: number;
  reason: string;
  severity: "ok" | "watch" | "urgent";
}

const SCENARIOS = [
  { label: "Paper & washroom cores", hint: "toilet roll jumbo recycled towels soap" },
  { label: "Floor chemical cores", hint: "floor cleaner concentrate washroom degreaser" },
  { label: "Whole catalogue pulse", hint: "cleaning supplies stock reorder" },
];

export function StockPulseTab() {
  const seed = useSeeds(["products", "orders"]);
  const [scenario, setScenario] = useState(SCENARIOS[0].label);
  const [hint, setHint] = useState(SCENARIOS[0].hint);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [rows, setRows] = useState<PulseRow[]>([]);
  const [poDraft, setPoDraft] = useState<string | null>(null);

  const run = async () => {
    setBusy(true);
    setErr(null);
    setPoDraft(null);
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
      const months = RECENT.map((m) => lit(m)).join(", ");
      const sql =
        `SELECT * FROM ${B.orders}\n WHERE semantic_match(text, ${lit(hint)})` +
        `\n   AND month IN (${months})\n LIMIT ${ORDERS_LIMIT}`;
      const i = push({
        label: "3-month order lines (Merlin export)",
        detail: sql,
        status: "running",
      });
      const res = await runSql(sql);
      done(i, { rowsOut: res.rows.length, ms: res.tookMs });

      const bySku = new Map<string, number>();
      for (const r of res.rows) {
        const m = metaOf(r);
        const sku = str(m.sku);
        if (!sku) continue;
        bySku.set(sku, (bySku.get(sku) ?? 0) + num(m.qty));
      }

      const pulse: PulseRow[] = [...bySku.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 24)
        .map(([sku, packs]) => {
          const avg = Math.max(1, Math.ceil(packs / RECENT.length));
          const p = PRODUCT_BY_SKU[sku];
          const inStock = p?.in_stock !== false;
          const lead = p?.lead_days ?? 5;
          let severity: PulseRow["severity"] = "ok";
          let reason = "Healthy against recent demand";
          let suggested = 0;
          if (!inStock) {
            severity = "urgent";
            reason = "Out of stock while still selling — classic “yes company” gap";
            suggested = Math.ceil(avg * 1.5);
          } else if (avg >= 8 && lead > 7) {
            severity = "watch";
            reason = "Popular line with a long lead time";
            suggested = Math.ceil(avg * 1.2);
          } else if (avg >= 12) {
            severity = "watch";
            reason = "High-velocity core SKU — never leave this short";
            suggested = Math.ceil(avg);
          }
          return {
            sku,
            name: p?.name ?? sku,
            packs3m: packs,
            avgMonth: avg,
            inStock,
            leadDays: lead,
            suggested,
            reason,
            severity,
          };
        });
      setRows(pulse);

      const lines = pulse.filter((r) => r.suggested > 0);
      push({
        label: "Draft supplier PO from averages",
        detail: `${lines.length} lines · months ${RECENT.join(", ")} · no live Merlin stock`,
        rowsIn: pulse.length,
        rowsOut: lines.length,
        status: "ok",
      });

      if (lines.length) {
        setPoDraft(
          [
            "SOURCE SUPPLIES — DRAFT PURCHASE ORDER (demo)",
            `Based on ${RECENT[0]}–${RECENT[RECENT.length - 1]} sales averages. Not sent.`,
            "",
            ...lines.map(
              (l) =>
                `${l.sku.padEnd(8)} × ${String(l.suggested).padStart(3)}   ${l.name}  [${l.severity}] ${l.reason}`
            ),
            "",
            "A person confirms before this reaches a supplier. Jordan AI would open this in Merlin via MCP.",
          ].join("\n")
        );
      }
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

  const urgent = rows.filter((r) => r.severity === "urgent").length;
  const watch = rows.filter((r) => r.severity === "watch").length;

  return (
    <div className="space-y-4">
      <DemoNote extra="Mirrors the stock-control + ChatGPT average-order workflow from the Source Supplies discovery call." />
      <ArchitecturePanel id="ss_stock" />
      <Panel
        title="Stock Pulse"
        subtitle="Popular lines first — draft a PO from recent demand without waiting on Merlin reports."
        action={<SeedChip seed={seed} />}
      >
        <Chips
          items={SCENARIOS}
          active={scenario}
          onPick={(s) => {
            setScenario(s.label);
            setHint(s.hint);
          }}
        />
        <label className="block mb-3">
          <span className="block text-xs font-medium mb-1">Focus (plain English)</span>
          <input className="input w-full" value={hint} onChange={(e) => setHint(e.target.value)} />
        </label>
        <button type="button" className="btn" disabled={busy || !seed.ready} onClick={() => void run()}>
          {busy ? <Spinner /> : null}
          {busy ? "Scanning…" : "Run stock pulse"}
        </button>
        {rows.length > 0 && (
          <p className="mt-2 text-xs text-gray-500 dark:text-muted">
            {urgent} urgent · {watch} watch · {rows.length} SKUs ranked by 3-month volume
          </p>
        )}
      </Panel>

      <ErrorBanner err={err} />
      {steps.length > 0 && <PipelineTrace steps={steps} />}

      {rows.length > 0 && (
        <Panel title="Core movers" subtitle="≈95% of Source volume is repeat — this list is that core">
          <div className="overflow-x-auto -mx-1">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs uppercase tracking-wider text-gray-500 dark:text-muted border-b border-gray-200 dark:border-edge">
                  <th className="py-2 pr-3">SKU</th>
                  <th className="py-2 pr-3">Product</th>
                  <th className="py-2 pr-3">3 mo</th>
                  <th className="py-2 pr-3">Avg / mo</th>
                  <th className="py-2 pr-3">Stock</th>
                  <th className="py-2 pr-3">Suggest</th>
                  <th className="py-2">Why</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.sku} className="border-b border-gray-100 dark:border-edge/60">
                    <td className="py-2 pr-3 font-mono text-xs">{r.sku}</td>
                    <td className="py-2 pr-3">{r.name}</td>
                    <td className="py-2 pr-3 font-mono">{r.packs3m}</td>
                    <td className="py-2 pr-3 font-mono">{r.avgMonth}</td>
                    <td className="py-2 pr-3">
                      <span className={`dot ${r.inStock ? "dot-ok" : "dot-bad"}`} />{" "}
                      {r.inStock ? `${r.leadDays}d lead` : "out"}
                    </td>
                    <td className="py-2 pr-3 font-mono">{r.suggested || "—"}</td>
                    <td className="py-2 text-xs text-gray-600 dark:text-muted">
                      <span className={r.severity === "urgent" ? "text-bad" : r.severity === "watch" ? "text-warn" : ""}>
                        {r.reason}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}

      {poDraft && (
        <Panel title="Draft purchase order" subtitle="Ready for a human to confirm into Merlin">
          <pre className="code-input !p-3 text-xs whitespace-pre-wrap">{poDraft}</pre>
        </Panel>
      )}

      <UnderTheHood filter={["/api/v1/query", "ss_orders", "ss_products"]} />
    </div>
  );
}
