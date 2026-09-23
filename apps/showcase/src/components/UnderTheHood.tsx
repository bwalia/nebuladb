/**
 * "Show me what's happening" — build brief section 37.
 *
 * Every major demo can expose the machinery underneath it: the HTTP
 * call it just made, the body it sent, what came back and how long it
 * took. Traces come from the real tracer (`demo/tracer.ts`), so this
 * panel is evidence rather than illustration.
 *
 * Sections may also pass `extra` rows — an execution plan, a retrieval
 * pipeline breakdown, an MCP tool call — which render alongside.
 */
import { useEffect, useState, type ReactNode } from "react";
import { getTraces, subscribe, type TraceEntry } from "../demo/tracer";
import { JsonView } from "../components";

export function useTraces(): TraceEntry[] {
  const [, force] = useState(0);
  useEffect(() => subscribe(() => force((n) => n + 1)), []);
  return getTraces();
}

export function UnderTheHood({
  /** Only show traces whose path contains one of these. */
  filter,
  extra,
  label = "Show me what's happening",
}: {
  filter?: string[];
  extra?: ReactNode;
  label?: string;
}) {
  const [open, setOpen] = useState(false);
  const all = useTraces();
  const traces = filter?.length
    ? all.filter((t) => filter.some((f) => t.path.includes(f)))
    : all;

  return (
    <div className="rounded-md border border-gray-200 dark:border-edge overflow-hidden">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center gap-2 px-3 py-2 text-left text-xs font-medium
                   text-gray-600 dark:text-muted hover:bg-gray-50 dark:hover:bg-carbon-900
                   transition-colors"
        aria-expanded={open}
      >
        <span className={`transition-transform ${open ? "rotate-90" : ""}`}>▸</span>
        <span>{label}</span>
        {traces.length > 0 && (
          <span className="ml-auto font-mono text-[10px] text-gray-400 dark:text-faint">
            {traces.length} call{traces.length === 1 ? "" : "s"}
          </span>
        )}
      </button>

      {open && (
        <div className="border-t border-gray-200 dark:border-edge p-3 space-y-3 bg-gray-50/60 dark:bg-carbon-950">
          {extra}
          {traces.length === 0 ? (
            <p className="text-xs text-gray-500 dark:text-muted">
              No API calls recorded yet — run something on this page first.
            </p>
          ) : (
            traces.slice(0, 6).map((t) => <TraceCard key={t.id} t={t} />)
          )}
        </div>
      )}
    </div>
  );
}

function TraceCard({ t }: { t: TraceEntry }) {
  const [show, setShow] = useState(false);
  const ok = t.status >= 200 && t.status < 300;
  return (
    <div className="rounded border border-gray-200 dark:border-edge bg-white dark:bg-carbon-900">
      <button
        onClick={() => setShow((v) => !v)}
        className="w-full flex items-center gap-2 px-2.5 py-1.5 font-mono text-[11px] text-left"
      >
        <span className={`dot ${ok ? "dot-ok" : "dot-bad"}`} />
        <span className="font-semibold text-gray-700 dark:text-ink">{t.method}</span>
        <span className="truncate text-gray-600 dark:text-muted">{t.path}</span>
        <span className="ml-auto shrink-0 text-gray-400 dark:text-faint">
          {t.status} · {t.tookMs}ms
        </span>
      </button>
      {show && (
        <div className="border-t border-gray-100 dark:border-edge p-2 space-y-2">
          {t.requestBody != null && (
            <div>
              <div className="eyebrow mb-1">Request</div>
              <JsonView value={t.requestBody} />
            </div>
          )}
          {t.error ? (
            <div>
              <div className="eyebrow mb-1">Error</div>
              <pre className="text-[11px] font-mono text-bad whitespace-pre-wrap">{t.error}</pre>
            </div>
          ) : (
            t.responseBody != null && (
              <div>
                <div className="eyebrow mb-1">Response</div>
                <JsonView value={t.responseBody} />
              </div>
            )
          )}
        </div>
      )}
    </div>
  );
}

/** A labelled pipeline stage list — used by hybrid search and RAG. */
export function PipelineStages({
  stages,
}: {
  stages: Array<{ name: string; detail?: string; tookMs?: number; active?: boolean }>;
}) {
  return (
    <ol className="space-y-1.5">
      {stages.map((s, i) => (
        <li key={s.name} className="flex items-start gap-2.5 text-xs">
          <span
            className={`mt-0.5 grid h-4 w-4 shrink-0 place-items-center rounded-full border font-mono text-[9px]
              ${
                s.active
                  ? "border-accent bg-accent/15 text-accent"
                  : "border-gray-300 text-gray-400 dark:border-edge dark:text-faint"
              }`}
          >
            {i + 1}
          </span>
          <div className="min-w-0 flex-1">
            <span className="font-medium text-gray-800 dark:text-ink">{s.name}</span>
            {s.detail && (
              <span className="ml-2 text-gray-500 dark:text-muted">{s.detail}</span>
            )}
          </div>
          {s.tookMs != null && (
            <span className="font-mono text-[10px] tabular-nums text-gray-400 dark:text-faint">
              {s.tookMs}ms
            </span>
          )}
        </li>
      ))}
    </ol>
  );
}
