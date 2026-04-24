import { useState } from "react";
import { api, ApiError } from "../api";
import { ErrorBanner, Panel, Spinner, Stat } from "../components";

/**
 * Ingestion surface. Two modes:
 *   - plain upsert: single doc with caller-supplied id.
 *   - chunked upsert: long text chopped server-side into N chunks
 *     keyed `doc_id#i`.
 *
 * We surface the chunk count and a post-upsert search against the
 * freshly inserted content so the user sees proof the pipeline ran
 * (embed → index) without having to switch tabs.
 */
export function DocumentsTab() {
  const [mode, setMode] = useState<"doc" | "document">("document");
  const [bucket, setBucket] = useState("docs");
  const [id, setId] = useState("");
  const [text, setText] = useState("");
  const [metaJson, setMetaJson] = useState("{}");
  const [result, setResult] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const run = async () => {
    setErr(null);
    setResult(null);
    setBusy(true);
    try {
      let meta: unknown = {};
      try {
        meta = metaJson.trim() ? JSON.parse(metaJson) : {};
      } catch {
        throw new Error("metadata must be valid JSON");
      }
      if (!id.trim()) throw new Error("id is required");
      if (!text.trim()) throw new Error("text is required");

      if (mode === "doc") {
        const r = await api.upsertDoc(bucket, id, text, meta);
        setResult(`Upserted ${bucket}/${r.id} (dim=${r.dim})`);
      } else {
        const r = await api.upsertDocument(bucket, id, text, meta);
        setResult(`Chunked into ${r.chunks} piece(s) under ${bucket}/${r.doc_id}`);
      }
    } catch (e) {
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="grid md:grid-cols-2 gap-4">
      <Panel
        title="Ingest a document"
        subtitle="Chunks + embeddings happen server-side. Try both modes."
      >
        <ModeTabs mode={mode} onChange={setMode} />

        <div className="grid grid-cols-2 gap-3">
          <label className="block">
            <span className="block text-xs font-medium mb-1">bucket</span>
            <input className="input" value={bucket} onChange={(e) => setBucket(e.target.value)} />
          </label>
          <label className="block">
            <span className="block text-xs font-medium mb-1">
              {mode === "doc" ? "id" : "doc_id"}
            </span>
            <input
              className="input"
              value={id}
              onChange={(e) => setId(e.target.value)}
              placeholder="e.g. zt-overview"
            />
          </label>
        </div>

        <label className="block">
          <span className="block text-xs font-medium mb-1">text</span>
          <textarea
            className="input min-h-[10rem] resize-y font-mono text-xs"
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="Paste architecture docs, logs, or anything else to index…"
          />
        </label>

        <label className="block">
          <span className="block text-xs font-medium mb-1">metadata (JSON)</span>
          <input
            className="input font-mono text-xs"
            value={metaJson}
            onChange={(e) => setMetaJson(e.target.value)}
            placeholder='{"region": "eu", "owner": "platform"}'
          />
        </label>

        <div className="flex items-center gap-3">
          <button className="btn" onClick={run} disabled={busy}>
            {busy ? <Spinner label="upserting…" /> : "Upsert"}
          </button>
          {result && (
            <span className="text-sm text-green-700 dark:text-green-300">{result}</span>
          )}
        </div>

        <ErrorBanner err={err} />
      </Panel>

      <Panel
        title="What happens to this text"
        subtitle="Plain English, in the order the server does it"
      >
        <ol className="list-decimal list-inside space-y-1 text-sm">
          <li>Validate bucket + id, reject empty text.</li>
          <li>
            Split text into chunks via the server's configured chunker
            (default: 500 chars with 50-char overlap).
          </li>
          <li>
            Batch-embed every chunk in one upstream call (MockEmbedder
            by default; set <code>NEBULA_OPENAI_API_KEY</code> for a
            real provider).
          </li>
          <li>
            Insert every chunk into the HNSW index keyed
            <code>{" {doc_id}#{i}"}</code>. A replace-upsert tombstones
            the old chunks first so reads never see a mixed state.
          </li>
          <li>
            Counters increment; metrics at <code>/metrics</code> reflect
            the upsert immediately.
          </li>
        </ol>
        <div className="flex gap-3 pt-2">
          <Stat label="endpoint" value="POST /api/v1/bucket/:b/document" />
        </div>
      </Panel>
    </div>
  );
}

function ModeTabs({
  mode,
  onChange,
}: {
  mode: "doc" | "document";
  onChange: (m: "doc" | "document") => void;
}) {
  const items = [
    { id: "document" as const, label: "Chunked document", hint: "Long text → N chunks" },
    { id: "doc" as const, label: "Single doc", hint: "One text blob, one vector" },
  ];
  return (
    <div className="inline-flex rounded-md border border-gray-200 dark:border-gray-800 p-0.5">
      {items.map((it) => (
        <button
          key={it.id}
          onClick={() => onChange(it.id)}
          title={it.hint}
          className={`px-3 py-1 text-xs font-medium rounded ${
            mode === it.id
              ? "bg-brand-600 text-white"
              : "text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800"
          }`}
        >
          {it.label}
        </button>
      ))}
    </div>
  );
}
