import { useCallback, useEffect, useRef, useState } from "react";
import { api, ApiError } from "../api";
import { sseStream } from "../sse";
import { ErrorBanner, JsonView, Panel, Spinner, Stat } from "../components";

interface ChHit {
  company_number?: string;
  title?: string;
  company_status?: string;
  company_type?: string;
  date_of_creation?: string;
  address_snippet?: string;
}

interface Turn {
  query: string;
  answer: string;
  done: boolean;
  error?: string;
}

/**
 * UK Companies House → select company → chat with live CH RAG context.
 * API key stays on the server (`NEBULA_COMPANIES_HOUSE_API_KEY`).
 */
export function CompaniesHouseTab() {
  const [q, setQ] = useState("Nebula");
  const [hits, setHits] = useState<ChHit[]>([]);
  const [fixture, setFixture] = useState(false);
  const [configured, setConfigured] = useState<boolean | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [company, setCompany] = useState<Record<string, unknown> | null>(null);
  const [bucket, setBucket] = useState("companies_house");
  const [busy, setBusy] = useState(false);
  const [ingestBusy, setIngestBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [statusMsg, setStatusMsg] = useState<string | null>(null);
  const [input, setInput] = useState("");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [chatBusy, setChatBusy] = useState(false);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    void api.companiesHouseStatus().then((s) => {
      setConfigured(s.configured);
      if (!s.configured) {
        setStatusMsg(
          "No Companies House API key on the server — using fixture data. Set NEBULA_COMPANIES_HOUSE_API_KEY (see docs/companies-house-demo.md)."
        );
      }
    }).catch(() => setConfigured(false));
  }, []);

  const search = async () => {
    setErr(null);
    setBusy(true);
    try {
      const r = await api.companiesHouseSearch(q);
      setHits((r.items as ChHit[]) ?? []);
      setFixture(Boolean(r.fixture));
      if (r.message) setStatusMsg(String(r.message));
    } catch (e) {
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const select = async (number: string) => {
    setSelected(number);
    setErr(null);
    setBusy(true);
    try {
      const c = await api.companiesHouseCompany(number);
      setCompany(c);
      setFixture(Boolean(c.fixture));
    } catch (e) {
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const ingest = async () => {
    if (!selected) return;
    setErr(null);
    setIngestBusy(true);
    try {
      const r = await api.companiesHouseIngest(selected, bucket);
      setCompany((r.company as Record<string, unknown>) ?? company);
      setStatusMsg(
        `Loaded ${Array.isArray(r.upserted) ? r.upserted.length : 0} docs into bucket "${bucket}" for RAG.`
      );
    } catch (e) {
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    } finally {
      setIngestBusy(false);
    }
  };

  const ask = useCallback(async () => {
    const query = input.trim();
    if (!query) return;
    setInput("");
    setChatBusy(true);
    const turnIdx = turns.length;
    setTurns((prev) => [...prev, { query, answer: "", done: false }]);
    const updater = (mut: (t: Turn) => Turn) =>
      setTurns((prev) => prev.map((t, i) => (i === turnIdx ? mut(t) : t)));
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    try {
      for await (const frame of sseStream(
        "/api/v1/ai/rag",
        { query, top_k: 6, stream: true, bucket, hybrid: true },
        ctrl.signal
      )) {
        if (frame.event === "answer_delta") {
          updater((t) => ({ ...t, answer: t.answer + frame.data }));
        } else if (frame.event === "done") {
          updater((t) => ({ ...t, done: true }));
          break;
        } else if (frame.event === "error") {
          updater((t) => ({ ...t, error: frame.data, done: true }));
          break;
        }
      }
    } catch (e) {
      updater((t) => ({
        ...t,
        error: (e as Error).message,
        done: true,
      }));
    } finally {
      setChatBusy(false);
      abortRef.current = null;
    }
  }, [input, turns.length, bucket]);

  const name =
    (company?.company_name as string) ||
    (company?.title as string) ||
    selected ||
    "—";

  return (
    <div className="space-y-4">
      <Panel
        title="Companies House"
        subtitle="Search UK companies → load profile → chat with live CH context as RAG"
      >
        {statusMsg && (
          <p className="text-sm mb-3 opacity-80 border border-gray-200 dark:border-edge rounded px-3 py-2">
            {statusMsg}
            {configured === false && " (fixture mode)"}
            {fixture && configured && " · response marked fixture"}
          </p>
        )}
        <div className="flex flex-wrap gap-2 items-end">
          <label className="block flex-1 min-w-[12rem]">
            <span className="block text-xs font-medium mb-1">company search</span>
            <input
              className="input"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void search();
              }}
              placeholder="Name or number…"
            />
          </label>
          <button className="btn" disabled={busy} onClick={() => void search()}>
            {busy ? <Spinner /> : "Search"}
          </button>
        </div>
        {err && <div className="mt-3"><ErrorBanner err={err} /></div>}
        {hits.length > 0 && (
          <ul className="mt-3 divide-y divide-gray-200 dark:divide-edge max-h-64 overflow-auto">
            {hits.map((h) => (
              <li key={h.company_number}>
                <button
                  type="button"
                  className={`w-full text-left px-2 py-2 text-sm hover:bg-gray-50 dark:hover:bg-carbon-900 ${
                    selected === h.company_number ? "bg-gray-100 dark:bg-carbon-800" : ""
                  }`}
                  onClick={() => void select(h.company_number!)}
                >
                  <span className="font-medium">{h.title}</span>
                  <span className="opacity-60 text-xs ml-2">
                    {h.company_number} · {h.company_status} · {h.address_snippet}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        )}
      </Panel>

      {company && (
        <Panel title={name} subtitle={`Company ${selected}`}>
          <div className="flex flex-wrap gap-3 mb-3 items-end">
            <Stat label="status" value={String(company.company_status ?? "—")} />
            <Stat label="type" value={String(company.type ?? company.company_type ?? "—")} />
            <label className="block">
              <span className="block text-xs font-medium mb-1">RAG bucket</span>
              <input
                className="input !w-40"
                value={bucket}
                onChange={(e) => setBucket(e.target.value)}
              />
            </label>
            <button className="btn" disabled={ingestBusy} onClick={() => void ingest()}>
              {ingestBusy ? <Spinner /> : "Load into RAG"}
            </button>
          </div>
          <details>
            <summary className="text-xs cursor-pointer mb-2">company JSON</summary>
            <JsonView value={company} />
          </details>
        </Panel>
      )}

      <Panel
        title="Chat about this company"
        subtitle={`Grounded on bucket "${bucket}" after Load into RAG`}
      >
        <div className="space-y-3 mb-3 max-h-80 overflow-auto">
          {turns.map((t, i) => (
            <div key={i} className="text-sm border border-gray-200 dark:border-edge rounded p-3">
              <div className="font-medium opacity-70">You: {t.query}</div>
              <div className="mt-1 whitespace-pre-wrap">
                {t.answer || (t.done ? "" : "…")}
                {t.error && <span className="text-red-500"> {t.error}</span>}
              </div>
            </div>
          ))}
        </div>
        <div className="flex gap-2">
          <input
            className="input flex-1"
            value={input}
            disabled={chatBusy || !selected}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") void ask();
            }}
            placeholder={
              selected
                ? "Who are the directors? What is the registered office?"
                : "Select a company first"
            }
          />
          <button className="btn" disabled={chatBusy || !selected} onClick={() => void ask()}>
            {chatBusy ? <Spinner /> : "Ask"}
          </button>
        </div>
      </Panel>
    </div>
  );
}
