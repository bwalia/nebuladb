/**
 * Stock Autopilot — discovery-call pain #2 (stock control).
 *
 * Dave: "our stock control is always an issue… when we run out of those
 * popular items is when there's always that frustration", Merlin has no
 * live stock, and he has been pasting three months of averages into
 * ChatGPT to draft POs. "It's cleverer than just looking at an average of
 * 3 months… if Christmas is a busier period or quieter period."
 *
 * Jordan reads every month of despatches from NebulaDB (`ss_demand`),
 * estimates today's stock, forecasts with seasonality + growth, cleans
 * one-offs, spots new-contract step-ups, sets safety stock from its own
 * error, and drafts one PO per supplier. A 12-month backtest proves it
 * against the 3-month-average method. Nothing is sent to a supplier.
 */
import { useMemo, useState } from "react";
import { ApiError } from "../api";
import { ErrorBanner, Panel, Spinner } from "../components";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { runSql } from "../rag/sql";
import { exhaustiveLimit } from "./data/common";
import { BUCKET, CORE, DEMAND, DEMAND_DOCS, DEMAND_MONTHS, SUPPLIERS, unitCost } from "./data/demand";
import { PRODUCT_BY_SKU } from "./data/products";
import { backtest, draftPos, plan, shortfallCost, ukDay, type Backtest, type Risk, type SkuInput, type SkuPlan } from "./stockPilot";
import { BenefitStrip, Kpi, Pill, Sparkline } from "./autopilotUi";
import { DemoNote, SeedChip, gbp, metaOf, num, str, useSeeds } from "./common";

/** LIMIT caps rows returned as well as the candidate pool: 16 lines × 24 months must all come back. */
const LIMIT = Math.max(exhaustiveLimit(DEMAND_DOCS.length), DEMAND_DOCS.length);
const TODAY = "2026-10-31";
const RISK: Record<Risk, { tone: "bad" | "warn" | "ok" | "accent"; label: string }> = {
  urgent: { tone: "bad", label: "Will run out" },
  watch: { tone: "warn", label: "Reorder now" },
  ok: { tone: "ok", label: "Healthy" },
  overstock: { tone: "accent", label: "Overstocked" },
};
const ORDER: Record<Risk, number> = { urgent: 0, watch: 1, ok: 2, overstock: 3 };

/** The 12-month backtest is deterministic, so the benefit strip can show it before the first run. */
function bundledTotals() {
  const n = { s: 0, p: 0, v: 0 };
  const j = { s: 0, p: 0, v: 0 };
  for (const c of CORE) {
    const rows = DEMAND.filter((r) => r.sku === c.sku);
    const sup = SUPPLIERS[c.supplier];
    const b = backtest({
      sku: c.sku, months: rows.map((r) => r.month), packs: rows.map((r) => r.packs), topShare: rows.map((r) => r.top_order_share),
      name: c.sku, supplier: sup.name, lead_days: sup.lead_days, moq: c.moq, unit_cost: unitCost(c.sku), on_hand: 0, on_order: 0, eta: null,
    });
    n.s += b.naive.stockoutMonths; n.p += b.naive.shortPacks; n.v += b.naive.avgStockValue;
    j.s += b.jordan.stockoutMonths; j.p += b.jordan.shortPacks; j.v += b.jordan.avgStockValue;
  }
  return { n, j, nc: shortfallCost(n.p), jc: shortfallCost(j.p) };
}
const PREVIEW = bundledTotals();

interface Result {
  plans: SkuPlan[];
  tests: Map<string, Backtest>;
  specials: { skus: number; orders: number };
}

export function StockPulseTab() {
  const seed = useSeeds(["products", "demand"]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [res, setRes] = useState<Result | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [approved, setApproved] = useState<Record<string, boolean>>({});

  const run = async () => {
    setBusy(true);
    setErr(null);
    setApproved({});
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
      const dSql = `SELECT * FROM ${BUCKET}\n WHERE semantic_match(text, 'monthly despatches core line')\n   AND kind = 'demand'\n   AND core = TRUE\n LIMIT ${LIMIT}`;
      let i = push({ label: "24 months of despatches for core lines (Merlin sales export)", detail: dSql, status: "running" });
      const dr = await runSql(dSql);
      done(i, { rowsOut: dr.rows.length, ms: dr.tookMs });

      const sSql = `SELECT * FROM ${BUCKET}\n WHERE semantic_match(text, 'estimated stock on hand supplier lead time')\n   AND kind = 'stock'\n LIMIT ${LIMIT}`;
      i = push({ label: "Estimated stock position (last count + goods in − despatches)", detail: sSql, status: "running" });
      const sr = await runSql(sSql);
      done(i, { rowsOut: sr.rows.length, ms: sr.tookMs });

      const oSql = `SELECT * FROM ${BUCKET}\n WHERE semantic_match(text, 'special order for one customer')\n   AND kind = 'demand'\n   AND core = FALSE\n LIMIT ${LIMIT}`;
      i = push({ label: "“Yes company” specials — kept out of core POs", detail: oSql, status: "running" });
      const or = await runSql(oSql);
      done(i, { rowsOut: or.rows.length, ms: or.tookMs });

      const bySku = new Map<string, { month: string; packs: number; share: number }[]>();
      for (const row of dr.rows) {
        const m = metaOf(row);
        const sku = str(m.sku);
        bySku.set(sku, [...(bySku.get(sku) ?? []), { month: str(m.month), packs: num(m.packs), share: num(m.top_order_share) }]);
      }
      const inputs: SkuInput[] = sr.rows.map((row) => {
        const m = metaOf(row);
        const sku = str(m.sku);
        const hist = (bySku.get(sku) ?? []).sort((a, b) => a.month.localeCompare(b.month));
        return {
          sku,
          name: PRODUCT_BY_SKU[sku]?.name ?? sku,
          months: hist.map((h) => h.month),
          packs: hist.map((h) => h.packs),
          topShare: hist.map((h) => h.share),
          supplier: str(m.supplier),
          lead_days: num(m.lead_days),
          moq: num(m.moq, 1),
          unit_cost: num(m.unit_cost),
          on_hand: num(m.on_hand),
          on_order: num(m.on_order),
          eta: m.eta ? str(m.eta) : null,
        };
      });
      const full = inputs.filter((x) => x.packs.length === DEMAND_MONTHS.length);
      const plans = full.map((x) => plan(x, TODAY)).sort((a, b) => ORDER[a.risk] - ORDER[b.risk] || (a.stockoutDate ?? "9").localeCompare(b.stockoutDate ?? "9"));
      push({
        label: "Forecast: clean one-offs → step-ups → seasonality × growth → safety stock",
        detail: "level = mean(last 3 deseasonalised) · (1+g)^(h/12) · season[month]; safety = 1.65 · σ · forecast · √((lead + 30) / 30)",
        rowsIn: full.length,
        rowsOut: plans.filter((p) => p.orderQty > 0).length,
        status: "ok",
      });
      const tests = new Map(full.map((x) => [x.sku, backtest(x)]));
      push({ label: "Backtest both methods on the last 12 months", detail: "each month: order up to the forecast, then replay what actually shipped", rowsIn: full.length * 12, status: "ok" });
      const specials = new Set(or.rows.map((r) => str(metaOf(r).sku)));
      setRes({ plans, tests, specials: { skus: specials.size, orders: or.rows.length } });
      setSelected(plans[0]?.input.sku ?? null);
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : (e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const pos = useMemo(() => (res ? draftPos(res.plans, TODAY) : []), [res]);
  const totals = useMemo(() => {
    if (!res) return PREVIEW;
    const n = { s: 0, p: 0, v: 0 };
    const j = { s: 0, p: 0, v: 0 };
    for (const b of res.tests.values()) {
      n.s += b.naive.stockoutMonths;
      n.p += b.naive.shortPacks;
      n.v += b.naive.avgStockValue;
      j.s += b.jordan.stockoutMonths;
      j.p += b.jordan.shortPacks;
      j.v += b.jordan.avgStockValue;
    }
    return { n, j, nc: shortfallCost(n.p), jc: shortfallCost(j.p) };
  }, [res]);
  const sel = res?.plans.find((p) => p.input.sku === selected) ?? null;
  const cut = totals ? Math.round((1 - totals.j.s / Math.max(1, totals.n.s)) * 100) : 60;

  return (
    <div className="space-y-4">
      <DemoNote extra="Demo volumes, suppliers and stock. Merlin has no live stock, so on-hand is an estimate Jordan maintains. No PO is sent." />
      <BenefitStrip
        headline={`${cut}% fewer stock-outs`}
        sub={totals ? `and ${gbp(totals.n.v - totals.j.v).replace(".00", "")} less cash sitting in stock on average` : "on the lines that must never run out"}
        rows={[
          { label: "Months a core line ran short (last 12)", before: totals ? `${totals.n.s}` : "—", after: totals ? `${totals.j.s}` : "—" },
          { label: "Re-deliveries from short stock", before: totals ? `${totals.nc.redeliveries}` : "—", after: totals ? `${totals.jc.redeliveries}` : "—" },
          { label: "Cost of shortfalls (vans + lost margin)", before: totals ? gbp(totals.nc.pounds).replace(".00", "") : "—", after: totals ? gbp(totals.jc.pounds).replace(".00", "") : "—" },
          { label: "Drafting the POs", before: "half a day of averages + ChatGPT", after: "every night, 1 click to approve" },
        ]}
        quote="It’s cleverer than just looking at an average of 3 months… if Christmas is a busier period or quieter period"
      />
      <ArchitecturePanel id="ss_stock" />
      <Panel title="Stock Autopilot" subtitle={`Nightly check · ${ukDay(TODAY)} 2026 · 16 core lines (~95% of volume)`} action={<SeedChip seed={seed} />}>
        <div className="flex flex-wrap items-center gap-2">
          <button type="button" className="btn" onClick={run} disabled={busy || !seed.ready} data-tour="run">
            {busy ? <Spinner label="Forecasting…" /> : "▶ Run tonight’s stock check"}
          </button>
          <span className="text-xs text-gray-500 dark:text-muted">Grey = 24 months shipped · blue dashed = Jordan’s forecast · dotted = 3-month average · amber dot = one-off ignored</span>
        </div>
        <ErrorBanner err={err} />
      </Panel>
      {steps.length > 0 && <PipelineTrace steps={steps} />}

      {res && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-2" data-tour="kpis">
            <Kpi label="Core lines checked" value={res.plans.length} />
            <Kpi label="Will run out" value={res.plans.filter((p) => p.risk === "urgent").length} tone="bad" hint="before a normal order lands" />
            <Kpi label="Below reorder point" value={res.plans.filter((p) => p.risk === "watch").length} tone="warn" />
            <Kpi label="Supplier POs drafted" value={pos.length} hint={gbp(pos.reduce((a, p) => a + p.total, 0)).replace(".00", "")} />
            <Kpi label="Specials kept out" value={res.specials.skus} hint={`${res.specials.orders} one-customer orders`} />
          </div>

          <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_21rem]">
            <Panel title="Core lines" subtitle="Sorted by risk · click a line for Jordan’s reasoning">
              <div className="overflow-x-auto -mx-1">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-left text-[11px] uppercase tracking-wider text-gray-500 dark:text-muted border-b border-gray-200 dark:border-edge">
                      <th className="py-1.5 pr-2">Line</th>
                      <th className="py-1.5 pr-2">24 months + forecast</th>
                      <th className="py-1.5 pr-2 text-right">On hand</th>
                      <th className="py-1.5 pr-2">Runs out</th>
                      <th className="py-1.5 pr-2 text-right">Order</th>
                      <th className="py-1.5 pr-2 text-right text-gray-400 dark:text-faint whitespace-nowrap">3-mo avg</th>
                      <th className="py-1.5">Risk</th>
                    </tr>
                  </thead>
                  <tbody>
                    {res.plans.map((p) => (
                      <tr
                        key={p.input.sku}
                        onClick={() => setSelected(p.input.sku)}
                        data-tour={`row-${p.input.sku}`}
                        className={`border-b border-gray-100 dark:border-edge/60 cursor-pointer ${selected === p.input.sku ? "bg-accent/10" : "hover:bg-gray-50 dark:hover:bg-carbon-800"}`}
                      >
                        <td className="py-1.5 pr-2">
                          <div className="font-mono">{p.input.sku}</div>
                          <div className="text-[11px] text-gray-500 dark:text-muted truncate max-w-[12rem]">{p.input.name}</div>
                        </td>
                        <td className="py-1.5 pr-2">
                          <Sparkline history={p.input.packs} forecast={p.fc.values} naive={p.fc.naive} marks={p.fc.cleaned.outliers.map((o) => o.index)} width={180} height={36} />
                        </td>
                        <td className="py-1.5 pr-2 text-right tabular-nums">
                          {p.input.on_hand.toLocaleString("en-GB")}
                          <div className="text-[11px] text-gray-500 dark:text-faint">{p.daysCover} days</div>
                        </td>
                        <td className="py-1.5 pr-2 whitespace-nowrap">{p.stockoutDate ? ukDay(p.stockoutDate) : "—"}</td>
                        <td className="py-1.5 pr-2 text-right tabular-nums font-semibold">{p.orderQty ? p.orderQty.toLocaleString("en-GB") : "—"}</td>
                        <td className="py-1.5 pr-2 text-right tabular-nums text-gray-400 dark:text-faint">{p.naiveOrderQty ? p.naiveOrderQty.toLocaleString("en-GB") : "—"}</td>
                        <td className="py-1.5">
                          <div className="flex flex-wrap gap-1">
                            <Pill tone={RISK[p.risk].tone}>{RISK[p.risk].label}</Pill>
                            {p.fc.stepUp && <Pill tone="accent">new contract</Pill>}
                            {p.fc.cleaned.outliers.length > 0 && <Pill tone="warn">one-off ignored</Pill>}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Panel>

            <div className="space-y-4">
              <Panel title={sel ? `Why · ${sel.input.sku}` : "Why"} subtitle={sel?.input.name}>
                {sel && (
                  <div className="space-y-2" data-tour="why">
                    <Sparkline history={sel.input.packs} forecast={sel.fc.values} naive={sel.fc.naive} marks={sel.fc.cleaned.outliers.map((o) => o.index)} width={340} height={90} />
                    <ul className="space-y-1 text-xs text-gray-700 dark:text-muted">
                      {sel.why.map((w) => (
                        <li key={w}>· {w}</li>
                      ))}
                    </ul>
                    {res.tests.get(sel.input.sku) && (
                      <p className="text-[11px] text-gray-500 dark:text-faint">
                        Last 12 months for this line: 3-month average ran short in {res.tests.get(sel.input.sku)!.naive.stockoutMonths} months, Jordan in{" "}
                        {res.tests.get(sel.input.sku)!.jordan.stockoutMonths}.
                      </p>
                    )}
                  </div>
                )}
              </Panel>
              <Panel title="Draft supplier POs" subtitle="One per supplier · MOQ-rounded · a buyer approves">
                <div className="space-y-2" data-tour="pos">
                  {pos.map((po) => (
                    <div key={po.supplier} className={`rounded-md border p-2 text-xs ${po.urgent ? "border-bad/40" : "border-gray-200 dark:border-edge"}`}>
                      <div className="flex items-center justify-between gap-2">
                        <span className="font-medium text-gray-900 dark:text-ink">{po.supplier}</span>
                        {po.urgent && <Pill tone="bad">urgent</Pill>}
                      </div>
                      <ul className="mt-1 text-gray-600 dark:text-muted">
                        {po.lines.map((l) => (
                          <li key={l.sku} className="flex justify-between gap-2">
                            <span className="truncate">
                              {l.qty.toLocaleString("en-GB")} × {l.sku}
                            </span>
                            <span className="tabular-nums">{gbp(l.value)}</span>
                          </li>
                        ))}
                      </ul>
                      <div className="mt-1 flex items-center justify-between">
                        <span className="text-gray-500 dark:text-faint">
                          {gbp(po.total)} · deliver by {ukDay(po.deliverBy)}
                        </span>
                        <button
                          type="button"
                          className={approved[po.supplier] ? "btn-secondary text-[11px] !py-0.5" : "btn text-[11px] !py-0.5 !px-2"}
                          onClick={() => setApproved((a) => ({ ...a, [po.supplier]: true }))}
                          disabled={approved[po.supplier]}
                        >
                          {approved[po.supplier] ? "✓ Draft PO in Merlin" : "Approve"}
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              </Panel>
            </div>
          </div>

          {totals && (
            <Panel title="Proof: the last 12 months replayed" subtitle="Each month both methods order up to their forecast; then what actually shipped is replayed">
              <div className="grid gap-3 md:grid-cols-2" data-tour="backtest">
                {[
                  { name: "Today: 3-month average + 10% buffer", t: totals.n, c: totals.nc, tone: "bad" as const },
                  { name: "Jordan: seasonal forecast + safety stock", t: totals.j, c: totals.jc, tone: "ok" as const },
                ].map((m) => (
                  <div key={m.name} className={`rounded-lg border p-3 ${m.tone === "ok" ? "border-ok/40 bg-ok/5" : "border-gray-200 dark:border-edge"}`}>
                    <div className="text-xs font-medium text-gray-900 dark:text-ink">{m.name}</div>
                    <div className="mt-2 grid grid-cols-2 gap-2">
                      <Kpi label="Months short" value={m.t.s} tone={m.tone} />
                      <Kpi label="Packs short" value={m.t.p.toLocaleString("en-GB")} tone={m.tone} />
                      <Kpi label="Re-deliveries" value={m.c.redeliveries} hint={`${gbp(m.c.pounds).replace(".00", "")} vans + margin`} />
                      <Kpi label="Avg stock held" value={gbp(m.t.v).replace(".00", "")} hint="cash tied up" />
                    </div>
                  </div>
                ))}
              </div>
            </Panel>
          )}
        </>
      )}
    </div>
  );
}
