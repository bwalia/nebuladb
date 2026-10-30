import { useCallback, useEffect, useRef, useState } from "react";
import { api, ApiError, type Hit } from "../api";
import { sseStream } from "../sse";
import { ErrorBanner, JsonView, Panel, Spinner, Stat } from "../components";
import { CompaniesMap } from "../CompaniesMap";
import { CompaniesExplorer } from "./CompaniesExplorer";

interface ChHit {
  company_number?: string;
  title?: string;
  company_status?: string;
  company_type?: string;
  date_of_creation?: string;
  address_snippet?: string;
  /** Structured registered-office address; postal_code places the map pin. */
  address?: { postal_code?: string; locality?: string };
}

interface SourceInfo {
  id?: string;
  source?: string;
  kind?: string;
  url?: string | null;
  email?: string | null;
  found?: boolean;
}

type AddressSync = Awaited<ReturnType<typeof api.companiesHouseSyncAddress>>;

const SEARCH_EXAMPLES = ["Tesco", "Rolls-Royce Holdings", "Monzo Bank", "Octopus Energy", "00445790"];

const QUESTION_EXAMPLES = [
  "Who are the directors?",
  "Who has significant control?",
  "What was filed most recently?",
  "Where is the registered office?",
  "What is the company website?",
];

interface Turn {
  query: string;
  answer: string;
  context: Hit[];
  done: boolean;
  error?: string;
}

/**
 * UK Companies House → select company → chat with live CH RAG context.
 * API key stays on the server (`NEBULA_COMPANIES_HOUSE_API_KEY`).
 * Load into RAG also researches a public website (CH does not publish
 * websites) and upserts a sources catalogue for the LLM.
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
  const [sources, setSources] = useState<SourceInfo[]>([]);
  const [website, setWebsite] = useState<{
    url: string;
    source: string;
    email?: string | null;
  } | null>(null);
  const [firecrawl, setFirecrawl] = useState<boolean | null>(null);
  const [input, setInput] = useState("");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [addressSync, setAddressSync] = useState<AddressSync | "checking" | null>(null);
  const companyRef = useRef<HTMLDivElement | null>(null);
  const [chatBusy, setChatBusy] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  // Scope retrieval to one company: RAG only filters by bucket, so a
  // shared bucket mixes every loaded company's PSC/officer chunks.
  const ragBucket = selected ? `${bucket}_${selected}` : bucket;

  useEffect(() => {
    void api.companiesHouseStatus().then((s) => {
      setConfigured(s.configured);
      setFirecrawl(Boolean(s.firecrawl));
      if (!s.configured) {
        setStatusMsg(
          "No Companies House API key on the server — using fixture data. Set NEBULA_COMPANIES_HOUSE_API_KEY (see docs/companies-house-demo.md)."
        );
      } else if (!s.firecrawl) {
        setStatusMsg(
          "Companies House configured. Website/email enrichment works best with NEBULA_FIRECRAWL_API_KEY (Firecrawl) — without it, fallback search is unreliable."
        );
      }
    }).catch(() => setConfigured(false));
  }, []);

  const search = async (query = q) => {
    setErr(null);
    setBusy(true);
    try {
      const r = await api.companiesHouseSearch(query);
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
    setTurns([]);
    setSources([]);
    setWebsite(null);
    setErr(null);
    setAddressSync(null);
    setBusy(true);
    try {
      const c = await api.companiesHouseCompany(number);
      setCompany(c);
      setFixture(Boolean(c.fixture));
      requestAnimationFrame(() => companyRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }));
      // Keep the bulk-loaded register copy in step with the live address.
      setAddressSync("checking");
      api
        .companiesHouseSyncAddress(number)
        .then(setAddressSync)
        .catch(() => setAddressSync(null));
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
      const r = await api.companiesHouseIngest(selected, ragBucket);
      setCompany((r.company as Record<string, unknown>) ?? company);
      setSources(Array.isArray(r.sources) ? (r.sources as SourceInfo[]) : []);
      setWebsite(
        r.website && typeof r.website === "object" && "url" in r.website
          ? (r.website as { url: string; source: string; email?: string | null })
          : null
      );
      const n = Array.isArray(r.upserted) ? r.upserted.length : 0;
      const w = r.website as { url?: string; email?: string | null } | null | undefined;
      const webNote = w?.url
        ? ` Website: ${w.url}${w.email ? ` · email: ${w.email}` : ""}.`
        : " Website not found (CH does not publish websites).";
      setStatusMsg(
        `Loaded ${n} docs into bucket "${ragBucket}" for RAG.${webNote} Sources are listed below for the LLM.`
      );
    } catch (e) {
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    } finally {
      setIngestBusy(false);
    }
  };

  const ask = useCallback(async (asked?: string) => {
    const query = (asked ?? input).trim();
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
      for await (const frame of sseStream(
        "/api/v1/ai/rag",
        { query, top_k: 6, stream: true, bucket: ragBucket, hybrid: true },
        ctrl.signal
      )) {
        if (frame.event === "context") {
          try {
            const hit = JSON.parse(frame.data) as Hit;
            updater((t) => ({ ...t, context: [...t.context, hit] }));
          } catch {
            /* ignore malformed context frames */
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
  }, [input, turns.length, ragBucket]);

  const name =
    (company?.company_name as string) ||
    (company?.title as string) ||
    selected ||
    "—";

  return (
    <div className="space-y-4">
      <Panel
        title="Companies House"
        subtitle="Search UK companies → load profile + web enrichment → chat with cited sources"
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
        <div className="flex flex-wrap gap-1.5 mt-2">
          <span className="eyebrow self-center mr-1">Try</span>
          {SEARCH_EXAMPLES.map((ex) => (
            <button
              key={ex}
              type="button"
              onClick={() => {
                setQ(ex);
                void search(ex);
              }}
              className={`rounded-full border px-2.5 py-1 text-xs transition-colors ${
                q === ex
                  ? "border-accent text-accent bg-accent/10"
                  : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted hover:border-accent/60"
              }`}
            >
              {ex}
            </button>
          ))}
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
        {hits.length > 0 && (
          <CompaniesMap
            companies={hits.map((h) => ({
              company_number: h.company_number,
              title: h.title,
              company_status: h.company_status,
              address_snippet: h.address_snippet,
              postcode: h.address?.postal_code,
            }))}
            selected={selected}
            onSelect={(n) => void select(n)}
          />
        )}
      </Panel>

      <CompaniesExplorer onPick={(n) => void select(n)} />

      {company && (
        <div ref={companyRef}>
        <Panel title={name} subtitle={`Company ${selected}`}>
          <AddressSyncNote sync={addressSync} />
          <div className="flex flex-wrap gap-3 mb-3 items-end">
            <Stat label="status" value={String(company.company_status ?? "—")} />
            <Stat label="type" value={String(company.type ?? company.company_type ?? "—")} />
            {website && (
              <Stat
                label="website"
                value={website.url.replace(/^https?:\/\//, "")}
              />
            )}
            {website?.email && <Stat label="email" value={website.email} />}
            {firecrawl !== null && (
              <Stat label="enrichment" value={firecrawl ? "firecrawl" : "fallback"} />
            )}
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
          {sources.length > 0 && (
            <div className="mb-3 text-xs border border-gray-200 dark:border-edge rounded p-3">
              <div className="font-medium mb-1">Sources for the LLM</div>
              <p className="opacity-70 mb-2">
                Companies House covers register facts only. Website and email come from Firecrawl
                (search + scrape) when configured; they are not on the UK register.
              </p>
              <ul className="space-y-1">
                {sources.map((s) => (
                  <li key={s.id} className="font-mono flex flex-wrap gap-x-2 gap-y-0.5">
                    <span className="opacity-60">{s.source}/{s.kind}</span>
                    <span>{s.id}</span>
                    {s.email ? <span>{s.email}</span> : null}
                    {s.url ? (
                      <a
                        className="text-accent underline"
                        href={String(s.url)}
                        target="_blank"
                        rel="noreferrer"
                      >
                        {String(s.url)}
                      </a>
                    ) : s.kind === "website" ? (
                      <span className="opacity-50">not found</span>
                    ) : null}
                  </li>
                ))}
              </ul>
            </div>
          )}
          <details>
            <summary className="text-xs cursor-pointer mb-2">company JSON</summary>
            <JsonView value={company} />
          </details>
        </Panel>
        </div>
      )}

      <Panel
        title="Chat about this company"
        subtitle={`Grounded on bucket "${ragBucket}" after Load into RAG — retrieved chunks shown before the answer`}
      >
        <div className="space-y-3 mb-3 max-h-96 overflow-auto">
          {turns.map((t, i) => (
            <TurnCard key={i} turn={t} />
          ))}
        </div>
        {selected && (
          <div className="flex flex-wrap gap-1.5 mb-2">
            {QUESTION_EXAMPLES.map((ex) => (
              <button
                key={ex}
                type="button"
                disabled={chatBusy}
                onClick={() => void ask(ex)}
                className="rounded-full border border-gray-200 dark:border-edge px-2.5 py-1 text-xs text-gray-600 dark:text-muted hover:border-accent/60 disabled:opacity-50"
              >
                {ex}
              </button>
            ))}
          </div>
        )}
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
                ? "Who are the directors? What is the company website?"
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

/** Result of checking the live registered office against the local register copy. */
function AddressSyncNote({ sync }: { sync: AddressSync | "checking" | null }) {
  if (!sync) return null;
  const box = "mb-3 text-xs rounded border px-3 py-2";
  if (sync === "checking") {
    return <p className={`${box} border-gray-200 dark:border-edge opacity-70`}>Checking the registered office against the local register copy…</p>;
  }
  if (sync.status === "updated") {
    return (
      <div className={`${box} border-amber-400/60 bg-amber-50 dark:bg-amber-950/30`}>
        <div className="font-medium">Registered office has moved. Local register copy updated.</div>
        <div className="mt-1 opacity-80">
          <span className="line-through">{sync.local ?? "—"}</span>
          <span className="mx-1">→</span>
          <span>{sync.live}</span>
        </div>
        <div className="mt-1 opacity-60 font-mono">{sync.doc_id} re-embedded with the new address</div>
      </div>
    );
  }
  if (sync.status === "unchanged") {
    return (
      <p className={`${box} border-gray-200 dark:border-edge`}>
        <span className="dot dot-ok mr-1.5" />
        Local register copy matches Companies House: {sync.live ?? sync.local}
      </p>
    );
  }
  return (
    <p className={`${box} border-gray-200 dark:border-edge opacity-70`}>
      Not in the local register copy ({sync.doc_id}), so there is no address to keep in sync.
    </p>
  );
}

function TurnCard({ turn }: { turn: Turn }) {
  const [showContext, setShowContext] = useState(true);
  return (
    <div className="text-sm border border-gray-200 dark:border-edge rounded p-3 space-y-2">
      <div className="font-medium opacity-70">You: {turn.query}</div>
      {turn.context.length > 0 && (
        <div>
          <button
            type="button"
            className="text-xs opacity-60 hover:underline"
            onClick={() => setShowContext((v) => !v)}
          >
            {showContext ? "hide" : "show"} {turn.context.length} retrieved source
            {turn.context.length === 1 ? "" : "s"}
          </button>
          {showContext && (
            <ul className="mt-1 space-y-1">
              {turn.context.map((h, j) => {
                const meta = (h.metadata ?? {}) as Record<string, unknown>;
                const src = String(meta.source ?? "unknown");
                const kind = String(meta.kind ?? "");
                return (
                  <li
                    key={j}
                    className="text-xs bg-gray-50 dark:bg-carbon-900 rounded p-2 border border-gray-200 dark:border-edge"
                  >
                    <div className="flex justify-between gap-2 font-mono opacity-70">
                      <span>
                        [{j}] {src}
                        {kind ? `/${kind}` : ""} · {h.id}
                      </span>
                      <span>{h.score.toFixed(3)}</span>
                    </div>
                    <p className="pt-1 whitespace-pre-wrap">{h.text.slice(0, 400)}</p>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      )}
      <div className="whitespace-pre-wrap">
        {turn.answer || (turn.done ? "" : "…")}
        {turn.error && <span className="text-red-500"> {turn.error}</span>}
      </div>
    </div>
  );
}
