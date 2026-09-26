import { useState } from "react";
import { api, ApiError } from "../api";
import { ErrorBanner, JsonView, Panel, Spinner, Stat } from "../components";
import { ExplainPanel } from "../explain";

/**
 * Azure AI Search–compatible enterprise search explorer.
 * Hits `/api/v1/azure-search/indexes/{name}/docs/search`.
 */
export function EnterpriseSearchTab() {
  const [index, setIndex] = useState("docs");
  const [search, setSearch] = useState("zero trust networking");
  const [queryType, setQueryType] = useState<"simple" | "semantic">("semantic");
  const [filter, setFilter] = useState("");
  const [top, setTop] = useState(5);
  const [backend, setBackend] = useState<"native" | "azure">("native");
  const [explainOn, setExplainOn] = useState(true);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [result, setResult] = useState<Record<string, unknown> | null>(null);

  const run = async () => {
    setErr(null);
    setBusy(true);
    try {
      const r = await api.azureSearch(index, {
        search,
        top,
        queryType,
        filter: filter.trim() || undefined,
        explain: explainOn,
        backend,
      });
      setResult(r as Record<string, unknown>);
    } catch (e) {
      setResult(null);
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const hits = (result?.value as Array<Record<string, unknown>>) ?? [];
  const explain = result?.explain as import("../api").Explain | undefined;

  return (
    <div className="space-y-4">
      <Panel
        title="Enterprise search"
        subtitle="Azure AI Search–compatible API on NebulaDB (native or external Azure backend)"
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <label className="block md:col-span-2">
            <span className="block text-xs font-medium mb-1">search</span>
            <input
              className="input"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void run();
              }}
            />
          </label>
          <label className="block">
            <span className="block text-xs font-medium mb-1">index</span>
            <input className="input" value={index} onChange={(e) => setIndex(e.target.value)} />
          </label>
          <label className="block">
            <span className="block text-xs font-medium mb-1">queryType</span>
            <select
              className="input"
              value={queryType}
              onChange={(e) => setQueryType(e.target.value as "simple" | "semantic")}
            >
              <option value="simple">simple (hybrid)</option>
              <option value="semantic">semantic</option>
            </select>
          </label>
          <label className="block md:col-span-2">
            <span className="block text-xs font-medium mb-1">
              filter (OData-lite, e.g. tenant eq &apos;acme&apos;)
            </span>
            <input
              className="input font-mono text-sm"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="optional"
            />
          </label>
          <label className="block">
            <span className="block text-xs font-medium mb-1">top</span>
            <input
              className="input !w-24"
              type="number"
              min={1}
              max={50}
              value={top}
              onChange={(e) => setTop(Number(e.target.value) || 5)}
            />
          </label>
          <label className="block">
            <span className="block text-xs font-medium mb-1">backend</span>
            <select
              className="input"
              value={backend}
              onChange={(e) => setBackend(e.target.value as "native" | "azure")}
            >
              <option value="native">native (NebulaDB)</option>
              <option value="azure">azure (external)</option>
            </select>
          </label>
        </div>
        <div className="flex items-center gap-4 mt-3">
          <label className="flex items-center gap-2 text-sm">
            <input
              type="checkbox"
              checked={explainOn}
              onChange={(e) => setExplainOn(e.target.checked)}
            />
            explain
          </label>
          <button className="btn" disabled={busy} onClick={() => void run()}>
            {busy ? <Spinner /> : "Search"}
          </button>
        </div>
        {err && <div className="mt-3"><ErrorBanner err={err} /></div>}
      </Panel>

      {result && (
        <Panel title="Results" subtitle={`took ${String(result.took_ms ?? "?")} ms`}>
          <div className="flex gap-4 mb-3">
            <Stat label="hits" value={String(hits.length)} />
            <Stat label="capability" value={String(result.capability_id ?? "—")} />
          </div>
          <ul className="space-y-2">
            {hits.map((h, i) => (
              <li key={i} className="border border-gray-200 dark:border-edge rounded p-3 text-sm">
                <div className="font-medium">
                  {String(h.id)}{" "}
                  <span className="text-xs opacity-60">
                    @search.score={String(h["@search.score"] ?? "")}
                  </span>
                </div>
                <p className="mt-1 whitespace-pre-wrap opacity-90">{String(h.content ?? "")}</p>
              </li>
            ))}
          </ul>
          {explain && (
            <div className="mt-4">
              <ExplainPanel explain={explain} />
            </div>
          )}
          <details className="mt-4">
            <summary className="text-xs cursor-pointer">raw JSON</summary>
            <JsonView value={result} />
          </details>
        </Panel>
      )}
    </div>
  );
}
