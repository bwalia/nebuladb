/**
 * Multi-turn, streaming, grounded chat shared by every RAG app.
 *
 * The app decides *what the model may read*: `getContext(question)`
 * returns the grounding set — a SQL-filtered, re-scored shortlist, or a
 * fresh retrieval — and the server's `/api/v1/ai/rag` answers over
 * exactly that (`context`), with the app's domain `system` instructions
 * and the prior turns as `history`. Citations `[n]` index into the
 * turn's context and render as clickable chips.
 */
import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { sseStream } from "../sse";
import { Spinner } from "../components";
import { Markdown } from "./Markdown";

export interface ContextDoc {
  id: string;
  /** Short human label for citation chips (a name, a ticket key…). */
  label: string;
  text: string;
  bucket?: string;
  metadata?: Record<string, unknown>;
}

interface Turn {
  query: string;
  answer: string;
  context: ContextDoc[];
  done: boolean;
  error?: string;
  startedAt: number;
  firstTokenMs?: number;
  totalMs?: number;
}

// Mirrors the server bounds in router.rs (RAG_MAX_*), with headroom.
const MAX_CONTEXT_CHARS = 60_000;
const MAX_HISTORY_TURNS = 6;
const MAX_HISTORY_ANSWER_CHARS = 1_500;

export function GroundedChat({
  title = "Chat",
  subtitle,
  system,
  getContext,
  suggestions = [],
  placeholder = "Ask a question…",
  disabledReason,
  resetKey,
  onCite,
  footer,
}: {
  title?: string;
  subtitle?: string;
  system: string;
  getContext: (question: string) => Promise<ContextDoc[]>;
  suggestions?: string[];
  placeholder?: string;
  /** When set, the input is disabled and this explains why. */
  disabledReason?: string;
  /** Changing this clears the conversation (e.g. a different job selected). */
  resetKey?: string;
  onCite?: (doc: ContextDoc) => void;
  footer?: ReactNode;
}) {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const scrollRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    abortRef.current?.abort();
    setTurns([]);
    setBusy(false);
  }, [resetKey]);

  useEffect(() => () => abortRef.current?.abort(), []);

  useEffect(() => {
    const el = scrollRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [turns]);

  const stop = useCallback(() => {
    abortRef.current?.abort();
  }, []);

  const ask = useCallback(
    async (q?: string) => {
      const query = (q ?? input).trim();
      if (!query || busy || disabledReason) return;
      setInput("");
      setBusy(true);
      const startedAt = performance.now();
      const idx = turns.length;
      const history = turns
        .filter((t) => t.done && !t.error && t.answer)
        .slice(-MAX_HISTORY_TURNS)
        .flatMap((t) => [
          { role: "user", content: t.query },
          { role: "assistant", content: t.answer.slice(0, MAX_HISTORY_ANSWER_CHARS) },
        ]);
      setTurns((prev) => [...prev, { query, answer: "", context: [], done: false, startedAt }]);
      const update = (mut: (t: Turn) => Turn) =>
        setTurns((prev) => prev.map((t, i) => (i === idx ? mut(t) : t)));

      const ctrl = new AbortController();
      abortRef.current = ctrl;
      try {
        const context = fitContext(await getContext(query));
        update((t) => ({ ...t, context }));
        if (context.length === 0) {
          update((t) => ({
            ...t,
            done: true,
            error: "Nothing to ground on — the retrieval step returned no documents.",
          }));
          return;
        }
        for await (const frame of sseStream(
          "/api/v1/ai/rag",
          {
            query,
            stream: true,
            system,
            history,
            context: context.map((d) => ({
              id: d.id,
              text: d.text,
              bucket: d.bucket,
              metadata: d.metadata ?? {},
            })),
          },
          ctrl.signal
        )) {
          if (frame.event === "answer_delta") {
            update((t) => ({
              ...t,
              answer: t.answer + frame.data,
              firstTokenMs: t.firstTokenMs ?? Math.round(performance.now() - startedAt),
            }));
          } else if (frame.event === "done") {
            break;
          } else if (frame.event === "error") {
            update((t) => ({ ...t, error: frame.data }));
            break;
          }
        }
        update((t) => ({ ...t, done: true, totalMs: Math.round(performance.now() - startedAt) }));
      } catch (e) {
        const aborted = (e as Error).name === "AbortError";
        update((t) => ({
          ...t,
          done: true,
          error: aborted ? undefined : (e as Error).message,
          answer: aborted && !t.answer ? "(stopped)" : t.answer,
        }));
      } finally {
        setBusy(false);
        if (abortRef.current === ctrl) abortRef.current = null;
      }
    },
    [input, busy, disabledReason, turns, getContext, system]
  );

  return (
    <section className="card flex flex-col gap-3">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="font-display text-base font-semibold text-gray-900 dark:text-ink">{title}</h3>
          {subtitle && <p className="text-xs text-gray-500 dark:text-muted mt-0.5">{subtitle}</p>}
        </div>
        {turns.length > 0 && (
          <button className="btn-secondary !py-1 !px-2 !text-xs" onClick={() => setTurns([])} disabled={busy}>
            Clear
          </button>
        )}
      </div>

      <div ref={scrollRef} className="space-y-3 max-h-[32rem] overflow-y-auto pr-1">
        {turns.length === 0 && (
          <p className="text-sm text-gray-500 dark:text-muted">
            {disabledReason ?? "Answers are grounded only on the documents shown under each reply, with [n] citations."}
          </p>
        )}
        {turns.map((t, i) => (
          <TurnView key={i} turn={t} onCite={onCite} />
        ))}
      </div>

      {suggestions.length > 0 && (
        <div className="flex flex-wrap gap-1.5">
          {suggestions.map((s) => (
            <button
              key={s}
              type="button"
              disabled={busy || !!disabledReason}
              onClick={() => void ask(s)}
              className="rounded-full border border-gray-200 dark:border-edge px-2.5 py-1 text-xs
                         text-gray-600 dark:text-muted hover:border-accent/60 hover:text-accent
                         disabled:opacity-40 disabled:hover:border-gray-200 disabled:hover:text-gray-600"
            >
              {s}
            </button>
          ))}
        </div>
      )}

      <form
        className="flex gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          void ask();
        }}
      >
        <input
          className="input flex-1"
          value={input}
          disabled={!!disabledReason}
          onChange={(e) => setInput(e.target.value)}
          placeholder={disabledReason ?? placeholder}
          aria-label="Chat message"
        />
        {busy ? (
          <button type="button" className="btn-secondary" onClick={stop}>
            Stop
          </button>
        ) : (
          <button type="submit" className="btn" disabled={!input.trim() || !!disabledReason}>
            Ask
          </button>
        )}
      </form>
      {footer}
    </section>
  );
}

function TurnView({ turn, onCite }: { turn: Turn; onCite?: (doc: ContextDoc) => void }) {
  const [open, setOpen] = useState(false);
  const [flash, setFlash] = useState<number | null>(null);
  const cite = (n: number) => {
    const doc = turn.context[n];
    if (!doc) return;
    setOpen(true);
    setFlash(n);
    window.setTimeout(() => setFlash((f) => (f === n ? null : f)), 1600);
    onCite?.(doc);
  };
  return (
    <div className="space-y-2">
      <div className="flex justify-end">
        <div className="max-w-[85%] rounded-xl rounded-br-sm bg-gray-900 text-white dark:bg-carbon-800 dark:text-ink px-3 py-2 text-sm">
          {turn.query}
        </div>
      </div>
      <div className="rounded-xl rounded-bl-sm border border-gray-200 dark:border-edge px-3 py-2.5 text-sm">
        {turn.answer ? (
          <Markdown text={turn.answer} onCite={cite} citeLabel={(n) => turn.context[n]?.label} />
        ) : !turn.done ? (
          <Spinner label={turn.context.length ? `reading ${turn.context.length} documents…` : "retrieving…"} />
        ) : null}
        {turn.error && <p className="mt-2 text-xs font-mono text-bad">{turn.error}</p>}
        <div className="mt-2 flex items-center gap-3 text-[11px] font-mono text-gray-400 dark:text-faint">
          {turn.context.length > 0 && (
            <button type="button" className="hover:text-accent" onClick={() => setOpen((v) => !v)}>
              {open ? "▾" : "▸"} {turn.context.length} source{turn.context.length === 1 ? "" : "s"}
            </button>
          )}
          {turn.firstTokenMs !== undefined && <span>first token {turn.firstTokenMs} ms</span>}
          {turn.totalMs !== undefined && <span>total {turn.totalMs} ms</span>}
          {turn.done && turn.answer && (
            <button
              type="button"
              className="ml-auto hover:text-accent"
              onClick={() => void navigator.clipboard?.writeText(turn.answer)}
            >
              copy
            </button>
          )}
        </div>
        {open && (
          <ol className="mt-2 space-y-1">
            {turn.context.map((d, n) => (
              <li
                key={d.id + n}
                className={`rounded border px-2 py-1 text-xs transition-colors ${
                  flash === n
                    ? "border-accent bg-accent/10"
                    : "border-gray-200 dark:border-edge"
                }`}
              >
                <span className="font-mono text-accent mr-1.5">[{n}]</span>
                <span className="font-medium">{d.label}</span>
                <span className="text-gray-500 dark:text-muted"> — {d.text.slice(0, 180)}{d.text.length > 180 ? "…" : ""}</span>
              </li>
            ))}
          </ol>
        )}
      </div>
    </div>
  );
}

/** Keep the grounding set under the server's total-chars cap by trimming
 *  each doc evenly, rather than dropping the lowest-ranked ones outright. */
function fitContext(docs: ContextDoc[]): ContextDoc[] {
  const capped = docs.slice(0, 50);
  const total = capped.reduce((n, d) => n + d.text.length, 0);
  if (total <= MAX_CONTEXT_CHARS) return capped;
  const per = Math.floor(MAX_CONTEXT_CHARS / capped.length);
  return capped.map((d) => (d.text.length > per ? { ...d, text: d.text.slice(0, per - 1) + "…" } : d));
}
