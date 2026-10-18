/**
 * The "how did we get this answer" strip each RAG app shows above its
 * results: one row per real step (SQL filter, facet search, scoring,
 * LLM) with the exact query text, row counts and wall time. These are
 * measured from the calls the app made, not illustrative.
 */
export interface PipelineStep {
  label: string;
  /** SQL text, request summary or scoring formula. */
  detail?: string;
  rowsIn?: number;
  rowsOut?: number;
  ms?: number;
  status?: "ok" | "running" | "error" | "skipped";
}

export function PipelineTrace({ steps }: { steps: PipelineStep[] }) {
  if (steps.length === 0) return null;
  return (
    <ol className="space-y-1.5">
      {steps.map((s, i) => (
        <li
          key={i}
          className="rounded-md border border-gray-200 dark:border-edge bg-gray-50/60 dark:bg-carbon-950 px-3 py-2"
        >
          <div className="flex items-center gap-2 text-xs">
            <span
              className={`dot ${
                s.status === "error"
                  ? "dot-bad"
                  : s.status === "running"
                    ? "dot-warn animate-pulseok"
                    : s.status === "skipped"
                      ? "dot-idle"
                      : "dot-ok"
              }`}
            />
            <span className="font-mono text-gray-400 dark:text-faint">{i + 1}</span>
            <span className="font-medium text-gray-800 dark:text-ink">{s.label}</span>
            <span className="ml-auto flex items-center gap-3 font-mono text-[11px] text-gray-500 dark:text-muted tabular-nums">
              {s.rowsIn !== undefined && s.rowsOut !== undefined && (
                <span title="rows in → rows out">
                  {s.rowsIn} → {s.rowsOut}
                </span>
              )}
              {s.rowsIn === undefined && s.rowsOut !== undefined && <span>{s.rowsOut} rows</span>}
              {s.ms !== undefined && <span>{s.ms} ms</span>}
            </span>
          </div>
          {s.detail && (
            <pre className="mt-1.5 text-[11px] leading-snug font-mono whitespace-pre-wrap break-words text-gray-600 dark:text-muted">
              {s.detail}
            </pre>
          )}
        </li>
      ))}
    </ol>
  );
}
