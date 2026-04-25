import { useCallback, useRef, useState } from "react";
import { type Hit } from "../api";
import { sseStream } from "../sse";
import { ErrorBanner, JsonView, Panel, Spinner, Stat } from "../components";

interface Turn {
  query: string;
  context: Hit[];
  answer: string;
  done: boolean;
  error?: string;
  startedAt: number;
  firstTokenAt?: number;
  endedAt?: number;
}

/**
 * Streaming RAG chat. We parse the SSE stream ourselves (the browser's
 * EventSource is GET-only) and append tokens to the current turn as
 * they arrive. Context events from NebulaDB land first, so the user
 * sees retrieved chunks while the LLM is still generating — this is
 * the single most demo-worthy property of the pipeline.
 */
export function RagTab() {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [input, setInput] = useState("What is NebulaDB?");
  const [topK, setTopK] = useState(3);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const cancel = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
    setBusy(false);
  }, []);

  const ask = async () => {
    const query = input.trim();
    if (!query) return;
    setErr(null);
    setBusy(true);
    setInput("");

    // `updater` is the only place we mutate the last turn. Extracting
    // it keeps the `for await` loop below pure-ish and avoids a race
    // where two token chunks arrive out of order in React's batching.
    const startedAt = performance.now();
    const turnIdx = turns.length;
    setTurns((prev) => [
      ...prev,
      { query, context: [], answer: "", done: false, startedAt },
    ]);
    const updater = (mut: (t: Turn) => Turn) =>
      setTurns((prev) => prev.map((t, i) => (i === turnIdx ? mut(t) : t)));

    const ctrl = new AbortController();
    abortRef.current = ctrl;

    try {
      for await (const frame of sseStream(
        "/api/v1/ai/rag",
        { query, top_k: topK, stream: true },
        ctrl.signal
      )) {
        switch (frame.event) {
          case "context":
            try {
              const hit = JSON.parse(frame.data) as Hit;
              updater((t) => ({ ...t, context: [...t.context, hit] }));
            } catch {
              /* ignore malformed frame */
            }
            break;
          case "answer_delta":
            updater((t) => ({
              ...t,
              answer: t.answer + frame.data,
              firstTokenAt: t.firstTokenAt ?? performance.now(),
            }));
            break;
          case "done":
            updater((t) => ({ ...t, done: true, endedAt: performance.now() }));
            return;
          case "error":
            updater((t) => ({ ...t, error: frame.data, done: true }));
            return;
        }
      }
      // Stream ended without a `done` marker — treat it as an ok
      // completion (the trailing frame is optional in our server).
      updater((t) => ({ ...t, done: true, endedAt: performance.now() }));
    } catch (e) {
      const raw = (e as Error).message;
      if (ctrl.signal.aborted) {
        updater((t) => ({ ...t, error: "cancelled", done: true }));
      } else {
        // Detect the common local-dev failure mode: the server returns
        // 500 because the LLM backend (Ollama) isn't reachable. Show
        // a helpful explanation instead of the raw HTTP error.
        const friendly = raw.includes("ollama") || raw.includes("llm:")
          ? "LLM backend unreachable. Start Ollama (docker compose up ollama) or set NEBULA_LLM_OLLAMA_URL to a running instance. The rest of NebulaDB works without it."
          : raw;
        setErr(friendly);
        updater((t) => ({ ...t, error: friendly, done: true }));
      }
    } finally {
      if (abortRef.current === ctrl) abortRef.current = null;
      setBusy(false);
    }
  };

  return (
    <div className="space-y-4">
      <Panel
        title="RAG chat"
        subtitle="Retrieval-augmented generation. Context chunks land first, answer streams after."
      >
        <div className="flex gap-3 items-end">
          <label className="block flex-1">
            <span className="block text-xs font-medium mb-1">question</span>
            <input
              className="input"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !busy) void ask();
              }}
              placeholder="e.g. How does NebulaDB handle failover?"
            />
          </label>
          <label className="block">
            <span className="block text-xs font-medium mb-1">top_k</span>
            <input
              type="number"
              min={1}
              max={20}
              className="input !w-20"
              value={topK}
              onChange={(e) => setTopK(Math.max(1, Math.min(20, Number(e.target.value) || 1)))}
            />
          </label>
          {busy ? (
            <button className="btn-secondary" onClick={cancel}>
              Cancel
            </button>
          ) : (
            <button className="btn" onClick={ask}>
              Ask
            </button>
          )}
        </div>
        <ErrorBanner err={err} />
      </Panel>

      <div className="space-y-4">
        {turns
          .slice()
          .reverse()
          .map((t, revIdx) => (
            // We reverse for display but keep the forward index in the
            // key so React re-uses DOM nodes when tokens stream into
            // the most-recent turn.
            <TurnView key={turns.length - 1 - revIdx} turn={t} />
          ))}
        {turns.length === 0 && !busy && (
          <div className="card text-sm text-gray-500 dark:text-gray-400">
            Ask a question to start. Make sure you've ingested some content first, or
            run <code>./scripts/seed.sh</code> to load the knowledge corpus.
          </div>
        )}
      </div>
    </div>
  );
}

function TurnView({ turn }: { turn: Turn }) {
  const ttft = turn.firstTokenAt ? Math.round(turn.firstTokenAt - turn.startedAt) : null;
  const total = turn.endedAt ? Math.round(turn.endedAt - turn.startedAt) : null;
  const [showContext, setShowContext] = useState(true);

  return (
    <section className="card space-y-3">
      <div className="flex items-start justify-between gap-3">
        <div className="text-sm font-semibold whitespace-pre-wrap">{turn.query}</div>
        <div className="flex gap-3">
          {ttft !== null && <Stat label="ttft" value={`${ttft}ms`} />}
          {total !== null && <Stat label="total" value={`${total}ms`} />}
          {!turn.done && <Spinner />}
        </div>
      </div>

      {turn.context.length > 0 && (
        <div>
          <button
            className="text-xs text-gray-500 hover:underline"
            onClick={() => setShowContext((v) => !v)}
          >
            {showContext ? "hide" : "show"} {turn.context.length} retrieved chunk
            {turn.context.length === 1 ? "" : "s"}
          </button>
          {showContext && (
            <ul className="mt-2 space-y-1">
              {turn.context.map((h, i) => (
                <li
                  key={i}
                  className="text-xs bg-gray-100 dark:bg-gray-950 rounded-md p-2 border border-gray-200 dark:border-gray-800"
                >
                  <div className="flex items-center justify-between">
                    <span className="font-mono text-gray-500">
                      [{i}] {h.bucket}/{h.id}
                    </span>
                    <span className="font-mono text-brand-600 dark:text-brand-500">
                      {h.score.toFixed(4)}
                    </span>
                  </div>
                  <p className="pt-1 whitespace-pre-wrap">{h.text}</p>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}

      <div>
        <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">answer</div>
        <div className="text-sm whitespace-pre-wrap min-h-[1.25rem]">
          {turn.answer || (turn.done ? "— empty —" : "")}
          {!turn.done && turn.answer && (
            <span className="inline-block align-baseline w-1 h-4 ml-0.5 bg-current animate-pulse" />
          )}
        </div>
        {turn.error && (
          <div className="mt-2 text-xs text-red-700 dark:text-red-300">
            stream error: {turn.error}
          </div>
        )}
      </div>

      <details className="text-xs">
        <summary className="cursor-pointer text-gray-500">debug</summary>
        <div className="pt-2">
          <JsonView value={turn} />
        </div>
      </details>
    </section>
  );
}
