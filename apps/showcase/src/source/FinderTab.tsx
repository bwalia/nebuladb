/**
 * App A — Green Product Finder ("What should I use?").
 *
 *   1. One SQL query mixes meaning (semantic_match) with hard filters
 *      (badge, sector, concentrate, in stock, max price).
 *   2. The same meaning without filters gives the near-miss pool; each
 *      near miss says which filter it failed.
 *   3. Swaps: hits with `replaces_sku` are priced against the product
 *      they replace, at the account's real order volume (ss_orders).
 *   4. Ranking happens in the browser with tunable weights.
 *   5. Grounded chat over exactly the ranked shortlist.
 */
import { useCallback, useMemo, useState } from "react";
import { ApiError } from "../api";
import { ErrorBanner, Panel, Spinner } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { GroundedChat, type ContextDoc } from "../rag/GroundedChat";
import { lit, runSql, similarity } from "../rag/sql";
import { BADGES, BADGE_SLUGS, ORDER_MONTHS, SECTORS, SECTOR_LABEL, exhaustiveLimit, type Badge, type Sector } from "./data/common";
import { ACCOUNTS, ORDER_DOCS } from "./data/accounts";
import { costPerUse, type Product } from "./data/products";
import {
  ASSUMED_PACKS_PER_YEAR,
  DEFAULT_WEIGHTS,
  FACETS,
  NO_FILTERS,
  PRODUCTS_LIMIT,
  composite,
  finderSql,
  missReasons,
  nearMissSql,
  scoreHits,
  swapSaving,
  type FinderFilters,
  type Swap,
  type Weights,
} from "./finder";
import { Chips, DemoNote, SeedChip, gbp, gbpFine, kg, metaOf, num, overlap, productFacts, productOf, str, useSeeds } from "./common";

interface Scenario {
  label: string;
  q: string;
  filters: Partial<FinderFilters>;
  account?: string;
}

const SCENARIOS: Scenario[] = [
  {
    label: "Low-odour canteen floor cleaner",
    q: "low odour floor cleaner for a school canteen",
    filters: { sector: "education", badge: "nordic-swan", inStockOnly: true, maxPrice: "30" },
    account: "KINGSMEAD",
  },
  {
    label: "Washroom cleaner, less plastic",
    q: "washroom cleaner concentrate that cuts single-use plastic trigger bottles",
    filters: { sector: "office", badge: "eu-ecolabel", concentrateOnly: true },
    account: "BRIGHTWAY",
  },
  {
    label: "Recycled hand towels",
    q: "recycled paper hand towels that fit our existing C-fold dispensers",
    filters: { sector: "education", badge: "fsc", inStockOnly: true },
    account: "KINGSMEAD",
  },
  {
    label: "Mould remover for student rooms",
    q: "black mould remover for student bedrooms, lower odour than bleach",
    filters: { sector: "higher-education", badge: "vegan", inStockOnly: true },
    account: "MERIDIAN",
  },
];

const ORDERS_LIMIT = exhaustiveLimit(ORDER_DOCS.length);

interface NearMiss {
  product: Product;
  reasons: string[];
  rank: number;
}

const SYSTEM =
  "You are a product adviser at Source Supplies, a UK B2B supplier of sustainable cleaning and hygiene products. " +
  "Answer ONLY from the shortlist documents. Each SHORTLIST doc is a product that passed the buyer's filters; NEAR MISS docs failed a filter (the reason is given); SWAP docs are worked-out greener swaps. " +
  "Always state the dilution and the pack size for any product you recommend, and cite it as [n]. " +
  "Never claim an eco badge that is not in that product's 'Eco badges (complete list)'. " +
  "Prices are in GBP excluding VAT. Use UK English and plain words. " +
  "If nothing on the shortlist fits, say so plainly and suggest the closest near miss with its reason. " +
  "You do not place orders; a person adds items to the basket.";

export function FinderTab() {
  const seed = useSeeds(["products", "accounts", "orders"]);
  const [query, setQuery] = useState(SCENARIOS[0].q);
  const [filters, setFilters] = useState<FinderFilters>({ ...NO_FILTERS, ...SCENARIOS[0].filters });
  const [account, setAccount] = useState<string>(SCENARIOS[0].account ?? "");
  const [scenario, setScenario] = useState(SCENARIOS[0].label);
  const [weights, setWeights] = useState<Weights>(DEFAULT_WEIGHTS);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [hits, setHits] = useState<Array<{ product: Product; sim: number }>>([]);
  const [near, setNear] = useState<NearMiss[]>([]);
  const [swaps, setSwaps] = useState<Swap[]>([]);
  const [runId, setRunId] = useState(0);

  const pick = (s: Scenario) => {
    setScenario(s.label);
    setQuery(s.q);
    setFilters({ ...NO_FILTERS, ...s.filters });
    setAccount(s.account ?? "");
  };

  const run = async () => {
    const q = query.trim();
    if (!q) return;
    setErr(null);
    setBusy(true);
    const trace: PipelineStep[] = [];
    const push = (s: PipelineStep) => {
      trace.push(s);
      setSteps([...trace]);
      return trace.length - 1;
    };
    const done = (i: number, patch: Partial<PipelineStep>) => {
      trace[i] = { ...trace[i], ...patch, status: patch.status ?? "ok" };
      setSteps([...trace]);
    };
    try {
      // 1. Meaning + hard filters in one query.
      const sql = finderSql(q, filters);
      let i = push({ label: "Find products: meaning + hard filters (one SQL query)", detail: sql, status: "running" });
      const main = await runSql(sql);
      const found = main.rows.map((r) => ({ product: productOf(r), sim: similarity(r.score) }));
      done(i, { rowsOut: found.length, ms: main.tookMs });

      // 2. Near misses: same meaning, no filters, then say which filter failed.
      const nSql = nearMissSql(q);
      i = push({ label: "Near misses: same meaning, no filters", detail: nSql, status: "running" });
      const pool = await runSql(nSql);
      const inHits = new Set(found.map((h) => h.product.sku));
      const misses = pool.rows
        .map((r) => ({ product: productOf(r), sim: similarity(r.score), text: str(metaOf(r).text) }))
        .filter((m) => !inHits.has(m.product.sku))
        .map((m) => ({ ...m, reasons: missReasons(m.product, filters), words: overlap(q, m.text) }))
        .filter((m) => m.reasons.length > 0 && m.reasons.length <= 2)
        // Words in common keep near misses sensible even with the mock embedder.
        .sort((a, b) => b.words + b.sim - (a.words + a.sim))
        .slice(0, 5)
        .map((m, rank) => ({ product: m.product, reasons: m.reasons, rank }));
      done(i, { rowsIn: pool.rows.length, rowsOut: misses.length, ms: pool.tookMs });

      // 3. Swaps: look up the products the hits replace, then the account's volume of them.
      const replaced = [...new Set(found.map((h) => h.product.replaces_sku).filter((s): s is string => !!s))];
      let swapList: Swap[] = [];
      if (replaced.length) {
        const rSql =
          `SELECT * FROM ss_products\n WHERE semantic_match(text, ${lit("less green product being replaced")})` +
          `\n   AND sku IN (${replaced.map(lit).join(", ")})\n LIMIT ${PRODUCTS_LIMIT}`;
        i = push({ label: "Swaps: the products these replace", detail: rSql, status: "running" });
        const old = await runSql(rSql);
        const oldBySku = new Map(old.rows.map((r) => [str(metaOf(r).sku), productOf(r)]));
        done(i, { rowsOut: old.rows.length, ms: old.tookMs });

        const packs = new Map<string, number>();
        if (account) {
          const oSql =
            `SELECT * FROM ss_orders\n WHERE semantic_match(text, ${lit("order lines")})` +
            `\n   AND account_id = ${lit(account)}\n   AND sku IN (${replaced.map(lit).join(", ")})` +
            `\n   AND month >= ${lit(ORDER_MONTHS[0])}\n LIMIT ${ORDERS_LIMIT}`;
          i = push({ label: "Your volume: order lines for those products (last 6 months)", detail: oSql, status: "running" });
          const o = await runSql(oSql);
          for (const r of o.rows) {
            const m = metaOf(r);
            packs.set(str(m.sku), (packs.get(str(m.sku)) ?? 0) + num(m.qty));
          }
          done(i, { rowsOut: o.rows.length, ms: o.tookMs });
        }
        swapList = found
          .filter((h) => h.product.replaces_sku && oldBySku.has(h.product.replaces_sku))
          // With an account picked, only swap what that account actually buys.
          .filter((h) => !account || packs.has(h.product.replaces_sku as string))
          .map((h) => {
            const from = oldBySku.get(h.product.replaces_sku as string) as Product;
            const sixMonths = packs.get(from.sku);
            return sixMonths !== undefined
              ? swapSaving(from, h.product, sixMonths * 2, "orders")
              : swapSaving(from, h.product, ASSUMED_PACKS_PER_YEAR, "assumed");
          })
          .sort((a, b) => b.savingKg - a.savingKg);
      }
      push({
        label: "Rank in the browser",
        detail:
          "score = Σ weight × facet ÷ Σ weights\n" +
          "fit = min-max of semantic similarity · eco = badges ÷ most badges in list\n" +
          "cost = 1 − min-max(log cost per use) · lead = 1 − min-max(lead days)",
        rowsIn: found.length,
        rowsOut: found.length,
        status: "ok",
      });
      setHits(found);
      setNear(misses);
      setSwaps(swapList);
      setRunId((n) => n + 1);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const scored = useMemo(
    () =>
      scoreHits(hits)
        .map((s) => ({ ...s, total: composite(s, weights) }))
        .sort((a, b) => b.total - a.total),
    [hits, weights]
  );
  const totalWeight = weights.semantic + weights.eco + weights.cost + weights.lead || 1;

  const getContext = useCallback(async (): Promise<ContextDoc[]> => {
    const docs: ContextDoc[] = scored.slice(0, 10).map((s, i) => ({
      id: `ss-prod-${s.product.sku}`,
      label: s.product.sku,
      bucket: "ss_products",
      text: `SHORTLIST #${i + 1}. ${productFacts(s.product)} ${s.product.summary}`,
    }));
    for (const n of near.slice(0, 3)) {
      docs.push({
        id: `near-${n.product.sku}`,
        label: `${n.product.sku} (near miss)`,
        bucket: "ss_products",
        text: `NEAR MISS. ${productFacts(n.product)} Failed filter: ${n.reasons.join("; ")}.`,
      });
    }
    for (const s of swaps) {
      docs.push({
        id: `swap-${s.from.sku}-${s.to.sku}`,
        label: `swap ${s.from.sku}→${s.to.sku}`,
        text:
          `SWAP. Replace ${s.from.name} (${s.from.sku}) with ${s.to.name} (${s.to.sku}). ` +
          `Volume: ${s.packsPerYear} packs a year (${s.volumeSource === "orders" ? "from this account's orders" : "assumed, no account picked"}). ` +
          `Same use needs ${s.newPacks} pack(s) of ${s.to.sku}. Saves ${gbp(s.savingGbp)} a year ex VAT and ${kg(s.savingKg)} a year.`,
      });
    }
    return docs;
  }, [scored, near, swaps]);

  const set = <K extends keyof FinderFilters>(k: K, v: FinderFilters[K]) => setFilters((f) => ({ ...f, [k]: v }));

  return (
    <div className="space-y-4">
      <ArchitecturePanel id="ss_finder" />
      <DemoNote />
      <Panel
        title="Green Product Finder"
        subtitle="Say what you need. We find products that pass your filters, rank them, and show greener swaps."
        action={<SeedChip seed={seed} />}
      >
        <Chips items={SCENARIOS} active={scenario} onPick={pick} />
        <label className="block mb-3">
          <span className="block text-xs font-medium mb-1">What do you need?</span>
          <textarea className="code-input w-full h-16" value={query} onChange={(e) => setQuery(e.target.value)} />
        </label>
        <div className="flex flex-wrap gap-3 items-end">
          <label className="text-xs">
            <span className="block font-medium mb-1">Sector</span>
            <select className="code-input !py-1.5 !text-sm" value={filters.sector} onChange={(e) => set("sector", e.target.value as Sector | "")}>
              <option value="">any</option>
              {SECTORS.map((s) => (
                <option key={s} value={s}>
                  {SECTOR_LABEL[s]}
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs">
            <span className="block font-medium mb-1">Eco badge</span>
            <select className="code-input !py-1.5 !text-sm" value={filters.badge} onChange={(e) => set("badge", e.target.value as Badge | "")}>
              <option value="">any</option>
              {BADGE_SLUGS.map((b) => (
                <option key={b} value={b}>
                  {BADGES[b]}
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs">
            <span className="block font-medium mb-1">Max price per pack (£ ex VAT)</span>
            <input type="number" min={0} className="code-input !py-1.5 !text-sm w-28" placeholder="no max" value={filters.maxPrice} onChange={(e) => set("maxPrice", e.target.value)} />
          </label>
          <label className="text-xs flex items-center gap-1.5 pb-2">
            <input type="checkbox" checked={filters.concentrateOnly} onChange={(e) => set("concentrateOnly", e.target.checked)} />
            Concentrates only
          </label>
          <label className="text-xs flex items-center gap-1.5 pb-2">
            <input type="checkbox" checked={filters.inStockOnly} onChange={(e) => set("inStockOnly", e.target.checked)} />
            In stock only
          </label>
          <label className="text-xs">
            <span className="block font-medium mb-1">Your account (for swap savings)</span>
            <select className="code-input !py-1.5 !text-sm" value={account} onChange={(e) => setAccount(e.target.value)}>
              <option value="">none</option>
              {ACCOUNTS.map((a) => (
                <option key={a.id} value={a.id}>
                  {a.name}
                </option>
              ))}
            </select>
          </label>
          <button className="btn" disabled={busy || !seed.ready || !query.trim()} onClick={() => void run()}>
            {busy ? <Spinner label="searching…" /> : "Find products"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {runId > 0 && (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1.15fr)_minmax(0,1fr)]">
          <div className="space-y-4">
            <Panel title="Best matches" subtitle={`${scored.length} products passed every filter · drag the weights to re-rank`}>
              <div className="space-y-1.5 mb-3">
                <div className="eyebrow">What matters most</div>
                {FACETS.map((f) => (
                  <label key={f.key} className="flex items-center gap-2 text-xs" title={f.hint}>
                    <span className="w-24 text-gray-600 dark:text-muted">{f.label}</span>
                    <input
                      type="range"
                      min={0}
                      max={50}
                      value={weights[f.key]}
                      onChange={(e) => setWeights((w) => ({ ...w, [f.key]: Number(e.target.value) }))}
                      className="flex-1 accent-[#4C8DFF]"
                    />
                    <span className="w-9 text-right font-mono tabular-nums">{Math.round((weights[f.key] / totalWeight) * 100)}%</span>
                  </label>
                ))}
              </div>
              {scored.length === 0 ? (
                <p className="text-sm text-gray-500 dark:text-muted">Nothing passed every filter. See the near misses below.</p>
              ) : (
                <ul className="space-y-2">
                  {scored.map((s, i) => (
                    <li key={s.product.sku} className="rounded-md border border-gray-200 dark:border-edge px-3 py-2">
                      <div className="flex items-start justify-between gap-2">
                        <div className="min-w-0">
                          <div className="text-sm font-medium">
                            <span className="text-accent mr-1">{i + 1}.</span>
                            {s.product.name} <span className="font-mono text-xs text-gray-500">{s.product.sku}</span>
                          </div>
                          <div className="text-xs text-gray-500 dark:text-muted">
                            {s.product.dilution} · {s.product.pack} · {gbp(s.product.price_gbp)} ex VAT · {gbpFine(costPerUse(s.product))} per {s.product.use_unit} ·{" "}
                            {s.product.in_stock ? `in stock, ${s.product.lead_days} day delivery` : `out of stock, back in ${s.product.lead_days} days`}
                          </div>
                          {s.product.badges.length > 0 && (
                            <div className="mt-1 flex flex-wrap gap-1">
                              {s.product.badges.map((b) => (
                                <span key={b} className="rounded border border-ok/40 bg-ok/5 px-1.5 text-[10px] text-gray-700 dark:text-muted">
                                  {BADGES[b] ?? b}
                                </span>
                              ))}
                            </div>
                          )}
                        </div>
                        <span className="font-mono text-sm font-semibold tabular-nums">{Math.round(s.total * 100)}</span>
                      </div>
                      <div className="mt-1.5 grid grid-cols-4 gap-2">
                        {FACETS.map((f) => (
                          <div key={f.key} title={f.hint}>
                            <div className="text-[10px] text-gray-500 dark:text-muted">{f.label}</div>
                            <div className="h-1.5 rounded bg-gray-100 dark:bg-carbon-800">
                              <div className="h-1.5 rounded bg-accent" style={{ width: `${Math.round(s.facets[f.key] * 100)}%` }} />
                            </div>
                          </div>
                        ))}
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </Panel>

            <Panel title="Greener swaps" subtitle={account ? "At this account's order volume (last 6 months × 2)" : `No account picked: assumes ${ASSUMED_PACKS_PER_YEAR} packs a year`}>
              {swaps.length === 0 ? (
                <p className="text-sm text-gray-500 dark:text-muted">{account ? "None of these matches replaces a product this account buys today." : "None of these matches replaces a less-green product."}</p>
              ) : (
                <ul className="space-y-2 text-sm">
                  {swaps.map((s) => (
                    <li key={s.from.sku + s.to.sku} className="rounded-md border border-ok/40 bg-ok/5 px-3 py-2">
                      <div>
                        Swap <span className="font-mono">{s.from.sku}</span> {s.from.name} → <span className="font-mono">{s.to.sku}</span> {s.to.name}
                      </div>
                      <div className="text-xs text-gray-600 dark:text-muted mt-0.5">
                        {s.savingKg >= 0 ? "Saves" : "Adds"} <b>{kg(Math.abs(s.savingKg))}</b> and {s.savingGbp >= 0 ? "saves" : "costs"}{" "}
                        <b>{gbp(Math.abs(s.savingGbp))}</b> a year ex VAT · {s.packsPerYear} packs of {s.from.sku} a year → {s.newPacks} of {s.to.sku}
                        {s.volumeSource === "assumed" ? " (assumed volume)" : ""}
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </Panel>

            <Panel title="Near misses" subtitle="Good matches that failed one or two filters">
              {near.length === 0 ? (
                <p className="text-sm text-gray-500 dark:text-muted">No near misses.</p>
              ) : (
                <ul className="space-y-1.5 text-sm">
                  {near.map((n) => (
                    <li key={n.product.sku} className="flex flex-wrap items-baseline gap-x-2">
                      <span className="font-mono text-xs">{n.product.sku}</span>
                      <span>{n.product.name}</span>
                      <span className="text-xs text-warn">{n.reasons.join(" · ")}</span>
                    </li>
                  ))}
                </ul>
              )}
            </Panel>
            <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
          </div>
          <GroundedChat
            title="Ask about these products"
            subtitle="Answers only from the ranked list, near misses and swaps shown here"
            system={SYSTEM}
            getContext={getContext}
            resetKey={String(runId)}
            disabledReason={scored.length + near.length === 0 ? "Run a search with at least one match first." : undefined}
            suggestions={["Which one should I buy, and how do I dilute it?", "What does the swap save us a year?", "Is there anything with a Nordic Swan badge?"]}
            placeholder="Ask about dilution, packs, badges or swaps…"
          />
        </div>
      )}
    </div>
  );
}
