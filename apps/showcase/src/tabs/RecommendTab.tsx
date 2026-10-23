/**
 * Recommendations — basket → semantic neighbours with price/stock/category
 * hard filters. Classic commerce ranking on a hybrid store (no RAG chat).
 */
import { useEffect, useMemo, useState } from "react";
import { ApiError, type SqlRow } from "../api";
import { ErrorBanner, Panel, Spinner, Stat } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ensureSeeded } from "../rag/seed";
import { field, lit, minMax, runSql, semanticText, similarity } from "../rag/sql";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import {
  BASKETS,
  CATEGORIES,
  PRODUCT_BUCKET,
  PRODUCTS,
  SEED_VERSION,
  type Category,
} from "../usecases/data/recommend";

interface Prod {
  id: string;
  name: string;
  category: string;
  price: number;
  stock: number;
  sim: number;
  text: string;
}

function toProd(r: SqlRow): Prod {
  return {
    id: String(field(r, "sku") ?? r.id),
    name: String(field(r, "name") ?? ""),
    category: String(field(r, "category") ?? ""),
    price: Number(field(r, "price_gbp") ?? 0),
    stock: Number(field(r, "stock") ?? 0),
    sim: similarity(r.score),
    text: String(field(r, "text") ?? ""),
  };
}

export function RecommendTab() {
  const [seedMsg, setSeedMsg] = useState("Checking catalogue…");
  const [seeded, setSeeded] = useState(false);
  const [basketId, setBasketId] = useState(BASKETS[0].id);
  const [category, setCategory] = useState<Category | "">("");
  const [maxPrice, setMaxPrice] = useState(8000);
  const [inStockOnly, setInStockOnly] = useState(true);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [hits, setHits] = useState<Prod[]>([]);

  const basket = BASKETS.find((b) => b.id === basketId) ?? BASKETS[0];
  const basketDocs = PRODUCTS.filter((p) => basket.skus.includes(p.id));

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await ensureSeeded(PRODUCT_BUCKET, PRODUCTS, SEED_VERSION, (d, t) => {
          if (!cancelled) setSeedMsg(`Seeding products ${d}/${t}…`);
        });
        if (!cancelled) {
          setSeeded(true);
          setSeedMsg(r.written ? `Seeded ${r.written} SKUs.` : `${r.total} SKUs ready.`);
        }
      } catch (e) {
        if (!cancelled) setSeedMsg(`Seeding failed: ${(e as Error).message}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const run = async () => {
    setErr(null);
    setBusy(true);
    const blob = basketDocs.map((d) => d.text).join("\n");
    const q = lit(semanticText(blob, 800));
    const exclude = basket.skus.map((s) => lit(s)).join(", ");
    let sql =
      `SELECT * FROM ${PRODUCT_BUCKET}\nWHERE semantic_match(text, ${q})\n` +
      `  AND sku NOT IN (${exclude})\n` +
      `  AND price_gbp <= ${lit(maxPrice)}\n`;
    if (inStockOnly) sql += `  AND stock > 0\n`;
    if (category) sql += `  AND category = ${lit(category)}\n`;
    sql += `LIMIT 12`;
    const trace: PipelineStep[] = [{ label: "Complementary SKUs", detail: sql, status: "running" }];
    setSteps([...trace]);
    try {
      const r = await runSql(sql);
      const rows = r.rows.map(toProd).sort((a, b) => b.sim - a.sim);
      trace[0] = { ...trace[0], rowsOut: rows.length, ms: r.tookMs, status: "ok" };
      setSteps([...trace]);
      setHits(rows);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const norm = useMemo(() => minMax(hits.map((h) => h.sim)), [hits]);

  return (
    <div className="space-y-4">
      <Panel
        title="Recommendations"
        subtitle="Basket embedding → similar in-stock SKUs under a price cap — commerce ranking without a chat model"
        action={
          <span className="metachip">
            <span className={`dot ${seeded ? "dot-ok" : "dot-warn animate-pulseok"}`} />
            {seedMsg}
          </span>
        }
      >
        <div className="flex flex-wrap gap-1.5 mb-3">
          {BASKETS.map((b) => (
            <button
              key={b.id}
              type="button"
              onClick={() => setBasketId(b.id)}
              className={`rounded-full border px-2.5 py-1 text-xs ${
                basketId === b.id
                  ? "border-accent text-accent bg-accent/10"
                  : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted"
              }`}
            >
              {b.label}
            </button>
          ))}
        </div>
        <ul className="text-xs text-gray-600 dark:text-muted mb-3 space-y-1">
          {basketDocs.map((d) => (
            <li key={d.id}>
              <span className="font-mono">{d.id}</span> — {String(d.metadata.name)}
            </li>
          ))}
        </ul>
        <div className="flex flex-wrap gap-3 items-end">
          <label className="text-xs">
            <span className="block font-medium mb-1">Category</span>
            <select
              className="code-input !py-1.5 !text-sm"
              value={category}
              onChange={(e) => setCategory(e.target.value as Category | "")}
            >
              <option value="">any</option>
              {CATEGORIES.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs">
            <span className="block font-medium mb-1">Max £</span>
            <input
              type="number"
              className="code-input !py-1.5 !text-sm w-28"
              value={maxPrice}
              onChange={(e) => setMaxPrice(Number(e.target.value) || 0)}
            />
          </label>
          <label className="text-xs flex items-center gap-2 pb-2">
            <input type="checkbox" checked={inStockOnly} onChange={(e) => setInStockOnly(e.target.checked)} />
            In stock
          </label>
          <button className="btn" disabled={!seeded || busy} onClick={() => void run()}>
            {busy ? <Spinner label="ranking…" /> : "Recommend"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {hits.length > 0 && (
        <Panel title="Recommended next" subtitle="Excludes basket SKUs · SQL predicates applied">
          <ul className="space-y-2">
            {hits.map((h) => (
              <li key={h.id} className="rounded-md border border-gray-200 dark:border-edge px-3 py-2 flex justify-between gap-3">
                <div>
                  <div className="text-sm font-medium">
                    {h.id} · {h.name}
                  </div>
                  <div className="text-xs text-gray-500">
                    {h.category} · £{h.price.toLocaleString()} · stock {h.stock}
                  </div>
                </div>
                <Stat label="sim" value={`${Math.round(norm(h.sim) * 100)}%`} />
              </li>
            ))}
          </ul>
          <UnderTheHood filter={["/api/v1/query", "/api/v1/bucket"]} />
        </Panel>
      )}
    </div>
  );
}
