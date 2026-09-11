import { useState } from "react";
import { api, ApiError, type Explain, type Hit } from "../api";
import { ErrorBanner, JsonView, Panel, Spinner, Stat } from "../components";
import { ExplainPanel } from "../explain";

/**
 * Semantic search explorer. Deliberately small — this is the
 * "exposition" tab: query goes in, scored hits come out. The score
 * is distance under the index metric (smaller = more similar), which
 * we render as-is so demo viewers see honest numbers rather than a
 * synthetic 0-100 similarity.
 */
export function SearchTab() {
  const [query, setQuery] = useState("zero trust networking");
  const [bucket, setBucket] = useState("docs");
  const [topK, setTopK] = useState(5);
  const [hits, setHits] = useState<Hit[] | null>(null);
  const [took, setTook] = useState<number | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [expanded, setExpanded] = useState<number | null>(null);
  // Hybrid fuses vector similarity with BM25 keyword relevance; its
  // score is higher-is-better, unlike the plain vector distance.
  const [hybrid, setHybrid] = useState(false);
  const [explainOn, setExplainOn] = useState(false);
  const [explain, setExplain] = useState<Explain | null>(null);
  // The score semantics of the hits on screen, not of the checkbox —
  // flipping Hybrid shouldn't relabel results from the previous run.
  const [ranHybrid, setRanHybrid] = useState(false);

  const run = async () => {
    setErr(null);
    setBusy(true);
    try {
      const r = await api.search(query, topK, bucket || undefined, {
        hybrid,
        explain: explainOn,
      });
      setHits(r.hits);
      setTook(r.took_ms);
      setExplain(r.explain ?? null);
      setRanHybrid(hybrid);
      setExpanded(null);
    } catch (e) {
      setExplain(null);
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-4">
      <Panel
        title="Semantic search"
        subtitle="Embed your query, find the nearest chunks in the HNSW index"
      >
        <div className="grid grid-cols-1 md:grid-cols-[1fr_auto_auto_auto] gap-3 items-end">
          <label className="block md:col-span-1">
            <span className="block text-xs font-medium mb-1">query</span>
            <input
              className="input"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void run();
              }}
              placeholder="Ask something in natural language…"
            />
          </label>
          <label className="block">
            <span className="block text-xs font-medium mb-1">bucket</span>
            <input
              className="input !w-32"
              value={bucket}
              onChange={(e) => setBucket(e.target.value)}
              placeholder="docs"
            />
          </label>
          <label className="block">
            <span className="block text-xs font-medium mb-1">top_k</span>
            <input
              type="number"
              min={1}
              max={50}
              className="input !w-20"
              value={topK}
              onChange={(e) => setTopK(Math.max(1, Math.min(50, Number(e.target.value) || 1)))}
            />
          </label>
          <button className="btn" onClick={run} disabled={busy}>
            {busy ? <Spinner label="searching…" /> : "Search"}
          </button>
        </div>

        <div className="flex flex-wrap items-center gap-3 pt-2">
          <label
            className="inline-flex items-center gap-1.5 text-xs text-gray-600 dark:text-muted cursor-pointer select-none"
            title="Fuse vector similarity with BM25 keyword relevance"
          >
            <input type="checkbox" checked={hybrid} onChange={(e) => setHybrid(e.target.checked)} />
            Hybrid
          </label>
          <label
            className="inline-flex items-center gap-1.5 text-xs text-gray-600 dark:text-muted cursor-pointer select-none"
            title="Show how the results were produced: stages, candidate counts, timings and per-hit scoring"
          >
            <input type="checkbox" checked={explainOn} onChange={(e) => setExplainOn(e.target.checked)} />
            Explain
          </label>
          {took !== null && <Stat label="took" value={`${took}ms`} />}
          {hits !== null && <Stat label="hits" value={hits.length} />}
          <span className="text-xs text-gray-500 dark:text-gray-400">
            {ranHybrid
              ? "score = fused vector + keyword relevance (higher is better)"
              : "score = distance (lower is closer)"}
          </span>
        </div>

        <ErrorBanner err={err} />
      </Panel>

      {hits && (
        <div className="space-y-2">
          {hits.map((h, i) => (
            <div key={i} className="card space-y-2">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-xs text-gray-500 dark:text-gray-400 font-mono">
                    {h.bucket}/{h.id}
                  </div>
                  <p className="text-sm pt-1 whitespace-pre-wrap">{h.text}</p>
                </div>
                <div className="text-xs font-mono text-accent dark:text-accent whitespace-nowrap">
                  {h.score.toFixed(4)}
                </div>
              </div>
              <div>
                <button
                  className="text-xs text-gray-500 hover:text-gray-900 dark:hover:text-gray-100 underline"
                  onClick={() => setExpanded(expanded === i ? null : i)}
                >
                  {expanded === i ? "hide metadata" : "show metadata"}
                </button>
                {expanded === i && (
                  <div className="pt-2">
                    <JsonView value={h.metadata} />
                  </div>
                )}
              </div>
            </div>
          ))}
          {hits.length === 0 && (
            <div className="card text-sm text-gray-500 dark:text-gray-400">
              No hits. Try the Documents tab to ingest content first.
            </div>
          )}
        </div>
      )}

      {explain && <ExplainPanel explain={explain} />}
    </div>
  );
}
