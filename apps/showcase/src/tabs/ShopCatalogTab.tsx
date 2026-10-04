import { useCallback, useEffect, useRef, useState } from "react";
import { api, ApiError, type Hit } from "../api";
import { sseStream } from "../sse";
import { ErrorBanner, Panel, Spinner, Stat } from "../components";

interface Turn {
  query: string;
  answer: string;
  context: Hit[];
  done: boolean;
  error?: string;
}

interface SourceRow {
  id?: string;
  kind?: string;
  slug?: string;
  sku?: string;
  qty_available?: number;
}

/**
 * Workstation AI Shop catalogue + stock SOT in NebulaDB.
 * Load the seed (or posted) catalogue into `shop_catalog` + `shop_stock_levels`,
 * then chat with hybrid RAG. Chat turns can be archived into `shop_chats`.
 */
export function ShopCatalogTab() {
  const [statusMsg, setStatusMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [ingestBusy, setIngestBusy] = useState(false);
  const [searchBusy, setSearchBusy] = useState(false);
  const [q, setQ] = useState("RTX PRO 6000 Blackwell");
  const [hits, setHits] = useState<
    { id: string; score: number; text: string; metadata?: Record<string, unknown> }[]
  >([]);
  const [catalogBucket, setCatalogBucket] = useState("shop_catalog");
  const [stockBucket, setStockBucket] = useState("shop_stock_levels");
  const [chatsBucket] = useState("shop_chats");
  const [seedProducts, setSeedProducts] = useState<number | null>(null);
  const [shopUrl, setShopUrl] = useState("https://int-shop.workstation.co.uk");
  const [sources, setSources] = useState<SourceRow[]>([]);
  const [stockCount, setStockCount] = useState(0);
  const [input, setInput] = useState(
    "What Blackwell GPUs are in stock and can I put 4× Max-Q in a TR PRO tower?"
  );
  const [turns, setTurns] = useState<Turn[]>([]);
  const [chatBusy, setChatBusy] = useState(false);
  const [sessionId] = useState(() => `showcase-${Date.now()}`);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    void api.shopStatus().then((s) => {
      setSeedProducts(s.seed_products ?? null);
      if (s.shop_public_url) setShopUrl(s.shop_public_url);
      setStatusMsg(
        s.configured
          ? `Seed ready: ${s.seed_categories} categories, ${s.seed_products} products. Buckets ${s.catalog_bucket} + ${s.stock_bucket}.`
          : "Shop seed not embedded on server."
      );
    }).catch(() => setStatusMsg("Could not reach /api/v1/shop/status"));
  }, []);

  const ingest = async () => {
    setErr(null);
    setIngestBusy(true);
    try {
      const r = await api.shopIngestCatalog({
        catalog_bucket: catalogBucket,
        stock_bucket: stockBucket,
      });
      setSources(Array.isArray(r.sources) ? (r.sources as SourceRow[]) : []);
      setStockCount(r.stock_docs ?? 0);
      setStatusMsg(
        `Loaded ${r.catalog_docs} catalogue docs → ${r.catalog_bucket} and ${r.stock_docs} stock docs → ${r.stock_bucket}.`
      );
    } catch (e) {
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    } finally {
      setIngestBusy(false);
    }
  };

  const search = async () => {
    setErr(null);
    setSearchBusy(true);
    try {
      const r = await api.shopSearchProducts(q, catalogBucket, 8);
      setHits(r.hits ?? []);
    } catch (e) {
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    } finally {
      setSearchBusy(false);
    }
  };

  const ask = useCallback(async () => {
    const query = input.trim();
    if (!query) return;
    setInput("");
    setChatBusy(true);
    const turnIdx = turns.length;
    setTurns((prev) => [...prev, { query, answer: "", context: [], done: false }]);
    const updater = (mut: (t: Turn) => Turn) =>
      setTurns((prev) => prev.map((t, i) => (i === turnIdx ? mut(t) : t)));
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    try {
      // Archive the user turn for later support analysis.
      void api.shopIngestChat({
        bucket: chatsBucket,
        session_id: sessionId,
        role: "user",
        content: query,
      }).catch(() => undefined);

      for await (const frame of sseStream(
        "/api/v1/ai/rag",
        { query, top_k: 8, stream: true, bucket: catalogBucket, hybrid: true },
        ctrl.signal
      )) {
        if (frame.event === "context") {
          try {
            const hit = JSON.parse(frame.data) as Hit;
            updater((t) => ({ ...t, context: [...t.context, hit] }));
          } catch {
            /* ignore */
          }
        } else if (frame.event === "answer_delta") {
          updater((t) => ({ ...t, answer: t.answer + frame.data }));
        } else if (frame.event === "done") {
          updater((t) => ({ ...t, done: true }));
          break;
        } else if (frame.event === "error") {
          updater((t) => ({ ...t, error: frame.data, done: true }));
          break;
        }
      }
      // Persist assistant answer when complete.
      setTurns((prev) => {
        const t = prev[turnIdx];
        if (t?.answer) {
          void api.shopIngestChat({
            bucket: chatsBucket,
            session_id: sessionId,
            role: "assistant",
            content: t.answer,
          }).catch(() => undefined);
        }
        return prev;
      });
    } catch (e) {
      updater((t) => ({ ...t, error: (e as Error).message, done: true }));
    } finally {
      setChatBusy(false);
      abortRef.current = null;
    }
  }, [input, turns.length, catalogBucket, chatsBucket, sessionId]);

  const stockSamples = sources
    .filter((s) => s.kind === "stock")
    .slice(0, 6);

  return (
    <div className="space-y-4">
      <Panel
        title="Workstation AI Shop"
        subtitle="Catalogue RAG + stock_levels SOT from int-shop — configuration grounding for the sales agent"
      >
        {statusMsg && (
          <p className="text-sm mb-3 opacity-80 border border-gray-200 dark:border-edge rounded px-3 py-2">
            {statusMsg}{" "}
            <a className="text-accent underline" href={shopUrl} target="_blank" rel="noreferrer">
              {shopUrl.replace(/^https?:\/\//, "")}
            </a>
          </p>
        )}
        <div className="flex flex-wrap gap-3 mb-3 items-end">
          <Stat label="seed products" value={seedProducts ?? "—"} />
          <Stat label="stock docs" value={stockCount || "—"} />
          <label className="block">
            <span className="block text-xs font-medium mb-1">catalog bucket</span>
            <input
              className="input !w-40"
              value={catalogBucket}
              onChange={(e) => setCatalogBucket(e.target.value)}
            />
          </label>
          <label className="block">
            <span className="block text-xs font-medium mb-1">stock bucket</span>
            <input
              className="input !w-44"
              value={stockBucket}
              onChange={(e) => setStockBucket(e.target.value)}
            />
          </label>
          <button className="btn" disabled={ingestBusy} onClick={() => void ingest()}>
            {ingestBusy ? <Spinner /> : "Load catalogue + stock into RAG"}
          </button>
        </div>
        {err && (
          <div className="mt-2">
            <ErrorBanner err={err} />
          </div>
        )}
        {stockSamples.length > 0 && (
          <div className="mt-3 text-xs border border-gray-200 dark:border-edge rounded p-3">
            <div className="font-medium mb-1">Stock SOT samples ({stockBucket})</div>
            <ul className="space-y-1 font-mono">
              {stockSamples.map((s) => (
                <li key={s.id}>
                  {s.sku ?? s.id} · qty={s.qty_available ?? "?"} · {s.slug}
                </li>
              ))}
            </ul>
          </div>
        )}
      </Panel>

      <Panel title="Catalogue search" subtitle={`Hybrid search in ${catalogBucket}`}>
        <div className="flex flex-wrap gap-2 items-end">
          <label className="block flex-1 min-w-[12rem]">
            <span className="block text-xs font-medium mb-1">query</span>
            <input
              className="input"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void search();
              }}
              placeholder="GPU, Mac Studio, DGX…"
            />
          </label>
          <button className="btn" disabled={searchBusy} onClick={() => void search()}>
            {searchBusy ? <Spinner /> : "Search"}
          </button>
        </div>
        {hits.length > 0 && (
          <ul className="mt-3 divide-y divide-gray-200 dark:divide-edge max-h-72 overflow-auto text-sm">
            {hits.map((h) => (
              <li key={h.id} className="py-2">
                <div className="font-medium font-mono text-xs opacity-70">
                  {h.id} · score {h.score.toFixed(3)} · {String(h.metadata?.kind ?? "")}
                </div>
                <div className="mt-0.5 line-clamp-3 opacity-90">{h.text}</div>
              </li>
            ))}
          </ul>
        )}
      </Panel>

      <Panel
        title="Ask the catalogue"
        subtitle={`RAG over ${catalogBucket}; turns archived to ${chatsBucket} (session ${sessionId})`}
      >
        <div className="space-y-3 mb-3 max-h-96 overflow-auto">
          {turns.map((t, i) => (
            <div key={i} className="border border-gray-200 dark:border-edge rounded p-3 text-sm">
              <div className="font-medium">You: {t.query}</div>
              {t.context.length > 0 && (
                <details className="mt-1 text-xs opacity-70">
                  <summary>{t.context.length} retrieved sources</summary>
                  <ul className="mt-1 space-y-1">
                    {t.context.map((c, j) => (
                      <li key={j} className="font-mono">
                        [{j}] {c.id} · {c.score.toFixed(3)}
                      </li>
                    ))}
                  </ul>
                </details>
              )}
              <div className="mt-2 whitespace-pre-wrap">
                {t.answer || (t.done ? "" : "…")}
                {t.error && <span className="text-red-600"> {t.error}</span>}
              </div>
            </div>
          ))}
        </div>
        <div className="flex gap-2 items-end">
          <label className="block flex-1">
            <span className="block text-xs font-medium mb-1">question</span>
            <input
              className="input"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  void ask();
                }
              }}
              disabled={chatBusy}
            />
          </label>
          <button className="btn" disabled={chatBusy} onClick={() => void ask()}>
            {chatBusy ? <Spinner /> : "Ask"}
          </button>
        </div>
      </Panel>
    </div>
  );
}
