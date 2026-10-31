/**
 * App D — Smart Reorder for multi-site cleaners.
 *
 *   0. The site's delivery day from ss_accounts.
 *   1. Six months of order lines for that site from ss_orders.
 *   2. Deterministic forecast per SKU (3-month average, rounded up to
 *      whole packs, spikes over 50% flagged) — see ./reorder.ts.
 *   3. Each "switch to…" request → semantic_match over ss_products with
 *      category and in_stock filters.
 *   4. Stock and lead-time check: lines that cannot make the site's next
 *      delivery day are split to the following one.
 *   5. "Save draft" writes to ss_order_drafts (status = draft). Nothing
 *      is ordered until a person confirms.
 */
import { useCallback, useMemo, useState } from "react";
import { ApiError, api } from "../api";
import { ErrorBanner, Panel, Spinner } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { GroundedChat, type ContextDoc } from "../rag/GroundedChat";
import { lit, runSql, semanticText, similarity } from "../rag/sql";
import { B, DEMO_TODAY, NEXT_MONTH, ORDER_MONTHS, exhaustiveLimit, slug } from "./data/common";
import { ACCOUNT_DOCS, ORDER_DOCS, SITES, siteKey } from "./data/accounts";
import type { Product } from "./data/products";
import { PRODUCTS_LIMIT } from "./finder";
import { addDays } from "./data/washroom";
import { daysUntil, forecastLines, guessCategory, makesDelivery, matchSite, nextDelivery, parseSwitches, type ForecastRow } from "./reorder";
import { Chips, DemoNote, SeedChip, gbp, kg, metaOf, num, overlap, productOf, str, ukDate, ukMonth, useSeeds } from "./common";

const ORDERS_LIMIT = exhaustiveLimit(ORDER_DOCS.length);
const ACCOUNTS_LIMIT = exhaustiveLimit(ACCOUNT_DOCS.length);

const SCENARIOS = [
  { label: "Kingsmead North, recycled towels", site: siteKey("KINGSMEAD", "North site"), text: "Same as last month for Kingsmead North site, but switch to recycled hand towels." },
  { label: "Brightway Reading, go concentrate", site: siteKey("BRIGHTWAY", "Reading Gateway"), text: "Next month for Brightway, Reading Gateway. Switch to a floor cleaner concentrate and switch to a washroom cleaner concentrate." },
  { label: "Harbour Seaview, same again", site: siteKey("HARBOUR", "Seaview House"), text: "Same as last month for Harbour, Seaview House." },
  { label: "Meridian Canal Halls, mould", site: siteKey("MERIDIAN", "Canal Halls"), text: "Same as last month for Meridian, Canal Halls, but switch to a lower odour mould remover." },
];

interface BasketLine {
  sku: string;
  product: Product;
  qty: number;
  last: number;
  reason: string;
  spike: boolean;
  swappedFrom?: string;
  arrives: string;
  split: boolean;
}

interface SiteInfo {
  account_id: string;
  account_name: string;
  site: string;
  delivery_day: string;
}

const SYSTEM =
  "You are a Source Supplies account assistant explaining a draft reorder to a contract-cleaning buyer. " +
  "Use ONLY the order history, forecast, swap and basket documents provided, citing them as [n]. " +
  "The forecast is the average of the last 3 months rounded up to whole packs; a spike is last month more than 50% above the 3 months before. " +
  "Quote the actual numbers. Prices are GBP excluding VAT. Use UK English and plain words. " +
  "You never place the order: a person checks and confirms the draft.";

export function ReorderTab() {
  const seed = useSeeds(["products", "accounts", "orders"]);
  const [site, setSite] = useState(SCENARIOS[0].site);
  const [text, setText] = useState(SCENARIOS[0].text);
  const [scenario, setScenario] = useState(SCENARIOS[0].label);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [info, setInfo] = useState<SiteInfo | null>(null);
  const [rows, setRows] = useState<ForecastRow[]>([]);
  const [basket, setBasket] = useState<BasketLine[]>([]);
  const [notes, setNotes] = useState<string[]>([]);
  const [runId, setRunId] = useState(0);
  const [saved, setSaved] = useState<string | null>(null);

  const run = async () => {
    setErr(null);
    setSaved(null);
    setBusy(true);
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
      // Free text can name the account and site; otherwise use the picker.
      const named = matchSite(text, SITES);
      let [account, siteName] = site.split("/") as [string, string];
      if (named.account && named.site) [account, siteName] = [named.account, named.site];
      else if (named.account && named.account !== account) {
        account = named.account;
        siteName = SITES.find((s) => s.account_id === account)?.site ?? siteName;
      }
      setSite(siteKey(account, siteName));

      const aSql =
        `SELECT * FROM ss_accounts\n WHERE semantic_match(text, ${lit("site delivery day")})` +
        `\n   AND account_id = ${lit(account)}\n   AND site = ${lit(siteName)}\n LIMIT ${ACCOUNTS_LIMIT}`;
      let i = push({ label: "The site and its delivery day", detail: aSql, status: "running" });
      const a = await runSql(aSql);
      const am = a.rows[0] ? metaOf(a.rows[0]) : null;
      if (!am) throw new Error(`No site ${siteName} for ${account}`);
      const siteInfo: SiteInfo = { account_id: account, account_name: str(am.account_name), site: siteName, delivery_day: str(am.delivery_day) };
      done(i, { rowsOut: a.rows.length, ms: a.tookMs });

      const oSql =
        `SELECT * FROM ss_orders\n WHERE semantic_match(text, ${lit("order lines")})` +
        `\n   AND account_id = ${lit(account)}\n   AND site = ${lit(siteName)}\n   AND month >= ${lit(ORDER_MONTHS[0])}\n LIMIT ${ORDERS_LIMIT}`;
      i = push({ label: "Order history: last 6 months for this site", detail: oSql, status: "running" });
      const o = await runSql(oSql);
      const hist = o.rows.map((r) => {
        const m = metaOf(r);
        return { sku: str(m.sku), month: str(m.month), qty: num(m.qty) };
      });
      done(i, { rowsOut: hist.length, ms: o.tookMs });

      const forecast = forecastLines(hist, ORDER_MONTHS);
      push({
        label: "Forecast per SKU (in the browser)",
        detail: "forecast = ceil(average of last 3 months)  ·  spike = last month > 1.5 × average of the 3 months before",
        rowsIn: hist.length,
        rowsOut: forecast.length,
        status: "ok",
      });

      const skus = forecast.map((f) => f.sku);
      const pSql = `SELECT * FROM ss_products\n WHERE semantic_match(text, ${lit("basket products")})\n   AND sku IN (${skus.map(lit).join(", ")})\n LIMIT ${PRODUCTS_LIMIT}`;
      i = push({ label: "Price, stock and lead time for the basket", detail: pSql, status: "running" });
      const p = await runSql(pSql);
      const bySku = new Map(p.rows.map((r) => [str(metaOf(r).sku), productOf(r)]));
      done(i, { rowsOut: p.rows.length, ms: p.tookMs });

      let lines: BasketLine[] = forecast
        .filter((f) => bySku.has(f.sku))
        .map((f) => ({ sku: f.sku, product: bySku.get(f.sku) as Product, qty: f.forecast, last: f.last, reason: f.reason, spike: f.spike, arrives: "", split: false }));

      const runNotes: string[] = [];
      for (const phrase of parseSwitches(text)) {
        const cat = guessCategory(phrase);
        const sSql =
          `SELECT * FROM ss_products\n WHERE semantic_match(text, ${lit(semanticText(phrase))})` +
          (cat ? `\n   AND category = ${lit(cat)}` : "") +
          `\n   AND in_stock = ${lit(true)}\n LIMIT ${PRODUCTS_LIMIT}`;
        i = push({ label: `Swap: "${phrase}"`, detail: sSql, status: "running" });
        const s = await runSql(sSql);
        const inBasket = new Set(lines.map((l) => l.sku));
        const cands = s.rows
          .map((r) => ({ p: productOf(r), sim: similarity(r.score), words: overlap(phrase, str(metaOf(r).text)) }))
          .filter((c) => !inBasket.has(c.p.sku))
          // Words in common and "replaces something in this basket" keep it sensible under the mock embedder.
          .map((c) => ({ ...c, score: c.words * 2 + c.sim * 0.5 + (c.p.replaces_sku && inBasket.has(c.p.replaces_sku) ? 1 : 0) }))
          .sort((x, y) => y.score - x.score);
        const best = cands[0];
        if (!best) {
          runNotes.push(`No in-stock product found for "${phrase}".`);
          done(i, { rowsOut: 0, ms: s.tookMs });
          continue;
        }
        const target =
          lines.find((l) => l.sku === best.p.replaces_sku) ??
          lines
            .filter((l) => l.product.category === best.p.category)
            .map((l) => ({ l, w: overlap(phrase, l.product.name) }))
            .sort((x, y) => y.w - x.w)[0]?.l;
        if (!target) {
          runNotes.push(`Found ${best.p.sku} for "${phrase}", but nothing in the basket to swap it for. Not added.`);
        } else {
          const use = target.qty * target.product.uses_per_pack;
          const qty = Math.max(1, Math.ceil(use / Math.max(1e-9, best.p.uses_per_pack)));
          lines = lines.map((l) =>
            l === target
              ? { ...l, sku: best.p.sku, product: best.p, qty, swappedFrom: target.sku, reason: `Switched from ${target.sku} as asked: same use, ${qty} pack${qty === 1 ? "" : "s"} instead of ${target.qty}` }
              : l
          );
        }
        done(i, { rowsOut: s.rows.length, ms: s.tookMs, detail: sSql + `\n→ ${best.p.sku} ${best.p.name}${target ? ` replaces ${target.sku}` : ""}` });
      }

      // Delivery-day split.
      const next = nextDelivery(DEMO_TODAY, siteInfo.delivery_day);
      const days = daysUntil(DEMO_TODAY, next);
      const after = addDays(next, 7);
      lines = lines.map((l) => {
        const ok = makesDelivery(l.product, l.qty, days);
        return {
          ...l,
          arrives: ok ? next : after,
          split: !ok,
          reason: ok ? l.reason : `${l.reason}. ${l.product.in_stock && l.product.stock_qty >= l.qty ? `Lead time ${l.product.lead_days} days` : "Not enough stock"}: comes on the following delivery`,
        };
      });
      push({
        label: "Stock and lead-time check",
        detail: `next ${siteInfo.delivery_day} delivery ${next} (${days} days away) · lines that cannot make it move to ${after}`,
        rowsIn: lines.length,
        rowsOut: lines.filter((l) => l.split).length,
        status: "ok",
      });

      setInfo(siteInfo);
      setRows(forecast);
      setBasket(lines);
      setNotes(runNotes);
      setRunId((n) => n + 1);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const totals = useMemo(
    () => ({
      gbp: basket.reduce((n, l) => n + l.qty * l.product.price_gbp, 0),
      kg: basket.reduce((n, l) => n + l.qty * l.product.co2e_kg_per_unit, 0),
    }),
    [basket]
  );

  const save = async () => {
    if (!info) return;
    const id = `draft-reorder-${info.account_id}-${slug(info.site)}-${NEXT_MONTH}`;
    const lines = basket.map((l) => ({ sku: l.sku, qty: l.qty, price_gbp: l.product.price_gbp, arrives: l.arrives }));
    try {
      await api.upsertDoc(
        B.drafts,
        id,
        `Draft reorder for ${info.account_name}, ${info.site}, ${NEXT_MONTH}: ` +
          lines.map((l) => `${l.qty} × ${l.sku}`).join(", ") +
          `. Total ${gbp(totals.gbp)} ex VAT. Draft only: not ordered until a person confirms.`,
        {
          account_id: info.account_id,
          site: info.site,
          status: "draft",
          created_by: "smart-reorder",
          month: NEXT_MONTH,
          lines,
          total_gbp: Math.round(totals.gbp * 100) / 100,
          co2e_kg: Math.round(totals.kg * 10) / 10,
        }
      );
      setSaved(id);
    } catch (e) {
      setErr((e as Error).message);
    }
  };

  const getContext = useCallback(async (): Promise<ContextDoc[]> => {
    const docs: ContextDoc[] = rows.map((r) => ({
      id: `hist-${r.sku}`,
      label: `${r.sku} history`,
      bucket: B.orders,
      text:
        `ORDER HISTORY for ${r.sku} at ${info?.site ?? ""}: ` +
        r.history.map((h) => `${ukMonth(h.month)} ${h.qty} packs`).join(", ") +
        `. FORECAST ${r.forecast} packs (3-month average ${r.avg3.toFixed(1)}). ${r.spike ? `SPIKE: last month ${Math.round(r.spikeShare * 100)}% above the 3 months before. ` : ""}${r.reason}.`,
    }));
    for (const l of basket) {
      docs.push({
        id: `line-${l.sku}`,
        label: `basket ${l.sku}`,
        text:
          `BASKET LINE ${l.sku} ${l.product.name} (${l.product.pack}): ${l.qty} packs at ${gbp(l.product.price_gbp)} = ${gbp(l.qty * l.product.price_gbp)} ex VAT, ` +
          `${kg(l.qty * l.product.co2e_kg_per_unit)}. Arrives ${ukDate(l.arrives)}. ${l.swappedFrom ? `Swapped from ${l.swappedFrom}. ` : ""}Reason: ${l.reason}.`,
      });
    }
    docs.push({ id: "basket-total", label: "basket total", text: `BASKET TOTAL ${gbp(totals.gbp)} ex VAT, ${kg(totals.kg)}. Status: draft, not ordered.` });
    return docs;
  }, [rows, basket, info, totals]);

  return (
    <div className="space-y-4">
      <ArchitecturePanel id="ss_reorder" />
      <DemoNote extra={`Demo calendar: today is ${ukDate(DEMO_TODAY)}.`} />
      <Panel title="Smart Reorder" subtitle="Pick a site or type what you want. We draft next month's basket from what the site really used." action={<SeedChip seed={seed} />}>
        <Chips
          items={SCENARIOS}
          active={scenario}
          onPick={(s) => {
            setScenario(s.label);
            setSite(s.site);
            setText(s.text);
          }}
        />
        <div className="flex flex-wrap gap-3 items-end">
          <label className="text-xs">
            <span className="block font-medium mb-1">Account and site</span>
            <select className="code-input !py-1.5 !text-sm" value={site} onChange={(e) => setSite(e.target.value)}>
              {SITES.map((s) => (
                <option key={siteKey(s.account_id, s.site)} value={siteKey(s.account_id, s.site)}>
                  {s.account_name} · {s.site}
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs flex-1 min-w-[18rem]">
            <span className="block font-medium mb-1">Request (optional)</span>
            <input className="input w-full" value={text} onChange={(e) => setText(e.target.value)} placeholder="same as last month, but switch to recycled hand towels" />
          </label>
          <button className="btn" disabled={busy || !seed.ready} onClick={() => void run()}>
            {busy ? <Spinner label="drafting…" /> : "Draft basket"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {runId > 0 && info && (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]">
          <div className="space-y-4">
            <Panel title={`Draft basket · ${info.account_name}, ${info.site}`} subtitle={`For ${ukMonth(NEXT_MONTH)} · delivery day ${info.delivery_day}`}>
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead className="text-left text-gray-500 dark:text-muted">
                    <tr>
                      <th className="py-1 pr-2">Product</th>
                      <th className="py-1 pr-2 text-right">Last month</th>
                      <th className="py-1 pr-2 text-right">Qty</th>
                      <th className="py-1 pr-2 text-right">Line ex VAT</th>
                      <th className="py-1 pr-2 text-right">CO2e</th>
                      <th className="py-1 pr-2">Arrives</th>
                    </tr>
                  </thead>
                  <tbody>
                    {basket.map((l) => (
                      <tr key={l.sku} className="border-t border-gray-100 dark:border-edge align-top">
                        <td className="py-1.5 pr-2">
                          <div className="font-medium">
                            <span className="font-mono">{l.sku}</span> {l.product.name}
                          </div>
                          <div className={`text-[11px] ${l.spike || l.split ? "text-warn" : l.swappedFrom ? "text-ok" : "text-gray-500 dark:text-muted"}`}>{l.reason}</div>
                        </td>
                        <td className="py-1.5 pr-2 text-right tabular-nums">{l.swappedFrom ? `${l.last} × ${l.swappedFrom}` : l.last}</td>
                        <td className="py-1.5 pr-2 text-right tabular-nums font-semibold">{l.qty}</td>
                        <td className="py-1.5 pr-2 text-right tabular-nums">{gbp(l.qty * l.product.price_gbp)}</td>
                        <td className="py-1.5 pr-2 text-right tabular-nums">{(l.qty * l.product.co2e_kg_per_unit).toFixed(1)} kg</td>
                        <td className={`py-1.5 pr-2 ${l.split ? "text-warn" : ""}`}>{ukDate(l.arrives)}</td>
                      </tr>
                    ))}
                  </tbody>
                  <tfoot>
                    <tr className="border-t-2 border-gray-200 dark:border-edge font-semibold">
                      <td className="py-1.5 pr-2">Total</td>
                      <td />
                      <td />
                      <td className="py-1.5 pr-2 text-right tabular-nums">{gbp(totals.gbp)}</td>
                      <td className="py-1.5 pr-2 text-right tabular-nums">{totals.kg.toFixed(1)} kg</td>
                      <td className="py-1.5 pr-2 text-[11px] font-normal text-gray-500">ex VAT</td>
                    </tr>
                  </tfoot>
                </table>
              </div>
              {notes.map((n) => (
                <p key={n} className="text-xs text-warn mt-1">
                  {n}
                </p>
              ))}
              <div className="mt-3 flex items-center gap-3">
                <button className="btn !text-xs" disabled={basket.length === 0} onClick={() => void save()}>
                  Save draft
                </button>
                <span className="text-xs text-gray-500 dark:text-muted">Saves to ss_order_drafts as a draft. Nothing is ordered until a person confirms it.</span>
              </div>
              {saved && (
                <p className="mt-2 text-xs text-ok">
                  Draft saved as <span className="font-mono">{saved}</span>.
                </p>
              )}
            </Panel>
            <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
          </div>
          <GroundedChat
            title="Ask about this basket"
            subtitle="Grounded on the order history and the forecast numbers above"
            system={SYSTEM}
            getContext={getContext}
            resetKey={String(runId)}
            disabledReason={basket.length === 0 ? "Draft a basket first." : undefined}
            suggestions={["Why did toilet roll go up?", "What changed from last month, and why?", "Which lines arrive late?"]}
            placeholder="Ask about quantities, swaps or delivery…"
          />
        </div>
      )}
    </div>
  );
}
