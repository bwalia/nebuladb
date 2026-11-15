/**
 * Visa AI Agent — a conversational Thai visa assistant over NebulaDB.
 *
 * Code decides what to look up (profile extraction, tool choice, SQL),
 * NebulaDB answers the lookups, and the configured LLM only writes the
 * reply from those results. Every reply shows its tool calls, sources and
 * a numbers check. The Visa Finder shortlist comes from the Labs `visa` pack
 * (a fictional agency's notes); the written answer is grounded only in
 * passages from official Thai government websites (gov-corpus.json).
 */
import { useEffect, useRef, useState } from "react";
import { ensureSeeded } from "../../rag/seed";
import { api } from "../../api";
import { ErrorBanner } from "../../components";
import { claims, flatFields, unsupportedNumbers } from "../engine/core";
import { PACK } from "../packs/visa";
import { Banner, ModelPicker, useLabEnv, usePackSeed } from "../shell";
import {
  answerPrompt,
  gatherEvidence,
  GOV_BUCKET,
  NOT_COVERED,
  officialFollowUps,
  parseProfile,
  plan,
  rewritePrompt,
  type Evidence,
  type Profile,
  type ToolCall,
  type Turn,
} from "./agent";

interface StepRun {
  call: ToolCall;
  rows: { id: string; text: string; meta?: Record<string, unknown> }[];
  ms: number;
  note?: string;
  error?: string;
}

interface Reply {
  question: string;
  profile: Profile;
  card?: string;
  steps: StepRun[];
  evidence: Evidence[];
  answer?: string;
  model?: string;
  unsupported: string[];
  /** Numbers the first draft invented, when the reply was rewritten from the passages. */
  rewritten?: string[];
  error?: string;
}

const STARTERS = [
  "I'm 55, British, with 900k THB savings and want to retire in Chiang Mai. Which visas fit?",
  "I work remotely for a UK company. DTV or LTR — what money do I need to show?",
  "My wife is Thai. How do I stay long term and can I work?",
  "How does 90-day reporting work and what if I'm late?",
  "I overstayed 3 days by mistake. What happens?",
  "What's the next step on my application?",
];

const CLIENTS = PACK.personas.filter((p) => p.filter.client_id);

interface GovCorpus {
  fetched: string;
  version: string;
  sources: string[];
  docs: { id: string; text: string; metadata: Record<string, string> }[];
}

/** Load the official-sources corpus (a separate chunk) and make sure NebulaDB has this version. */
function useGovSeed() {
  const [s, setS] = useState<{ status: "seeding" | "ready" | "error"; corpus?: GovCorpus; error?: string }>({ status: "seeding" });
  useEffect(() => {
    let live = true;
    import("./gov-corpus.json")
      .then(async (m) => {
        const corpus = (m.default ?? m) as GovCorpus;
        await ensureSeeded(GOV_BUCKET, corpus.docs, `gov-${corpus.version}`);
        if (live) setS({ status: "ready", corpus });
      })
      .catch((e: Error) => live && setS({ status: "error", error: e.message }));
    return () => {
      live = false;
    };
  }, []);
  return s;
}

export function VisaAgentTab() {
  const env = useLabEnv();
  const packSeed = usePackSeed(PACK);
  const gov = useGovSeed();
  const seed =
    packSeed.status === "error" || gov.status === "error"
      ? { ...packSeed, status: "error" as const, error: packSeed.error ?? gov.error }
      : gov.status === "ready" && packSeed.status === "ready"
        ? packSeed
        : { ...packSeed, status: "seeding" as const };
  const [client, setClient] = useState<string>("");
  const [provider, setProvider] = useState<string | undefined>();
  const [profile, setProfile] = useState<Profile>({ purposes: [] });
  const [history, setHistory] = useState<Turn[]>([]);
  const [replies, setReplies] = useState<Reply[]>([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const endRef = useRef<HTMLDivElement>(null);
  const persona = CLIENTS.find((p) => p.id === client);

  // Block body on purpose: newer browsers return a Promise from scrollIntoView, and an
  // effect must return nothing or a cleanup function.
  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [replies]);

  const reset = () => {
    setProfile({ purposes: [] });
    setHistory([]);
    setReplies([]);
  };

  const ask = async (question: string) => {
    if (!question.trim() || busy) return;
    setInput("");
    setBusy(true);
    setErr(null);
    const prof = parseProfile(question, profile);
    setProfile(prof);
    const p = plan(PACK, question, prof, persona);
    const reply: Reply = { question, profile: prof, steps: [], evidence: [], unsupported: [] };
    const push = () => setReplies((r) => [...r.slice(0, -1), { ...reply, steps: [...reply.steps], evidence: [...reply.evidence] }]);
    setReplies((r) => [...r, reply]);

    try {
      if (p.safety) {
        reply.card = p.safety.card;
        push();
        setHistory((h) => [...h, { role: "user", text: question }, { role: "agent", text: p.safety!.card }]);
        return;
      }
      const run1 = async (call: ToolCall) => {
        const t0 = performance.now();
        const run: StepRun = { call, rows: [], ms: 0 };
        try {
          let sql = call.sql!;
          let r = await api.sql(sql);
          // Finder with no exact fit: relax the funds limit, then the purpose, and say so.
          if (call.tool === "visa_finder" && r.rows.length === 0 && prof.fundsThb !== undefined) {
            sql = sql.replace(/\n\s+AND min_funds_thb <= \d+/, "");
            r = await api.sql(sql);
            run.note = "No visa type matched every limit, so the funds limit was dropped — check the funds each option needs.";
          }
          run.call = { ...call, sql };
          run.rows = r.rows.map((x) => ({ id: x.id, text: String(x.fields.text ?? ""), meta: flatFields(x.fields) }));
        } catch (e) {
          run.error = (e as Error).message;
        }
        run.ms = Math.round(performance.now() - t0);
        reply.steps.push(run);
        push();
        return run;
      };
      for (const call of p.calls) {
        const run = await run1(call);
        // The shortlist comes from the agency's notes; fetch the government's own requirements for its top routes.
        if (call.tool === "visa_finder")
          for (const f of officialFollowUps(question, run.rows.slice(0, 6).map((r) => String(r.meta?.visa ?? "")))) await run1(f);
      }
      // Evidence for the model: official passages (+ the client's own case notes), never the agency's notes.
      reply.evidence = gatherEvidence(reply.steps);
      push();
      if (!reply.evidence.length) {
        reply.answer = `${NOT_COVERED}. Please check www.thaievisa.go.th (Ministry of Foreign Affairs) or ask the Thai embassy or Immigration Bureau.`;
        reply.model = "fixed reply — no official passage matched, the model was not asked";
        push();
        setHistory((h) => [...h, { role: "user", text: question }, { role: "agent", text: reply.answer! }]);
        return;
      }

      const prompt = answerPrompt(PACK, question, prof, reply.evidence, history, gov.corpus?.fetched);
      const asked = [question, gov.corpus?.fetched ?? "", ...history.filter((h) => h.role === "user").map((h) => h.text)].join(" ");
      let r = await api.aiChat({ provider, system: prompt.system, message: prompt.user, temperature: 0.1, max_tokens: 600 });
      let unsupported = unsupportedNumbers(r.answer, reply.evidence.map((e) => e.text), asked);
      // A number that is in no official passage: ask once for a rewrite from the passages, keep the better reply.
      if (unsupported.length) {
        const again = await api.aiChat({ provider, system: prompt.system, message: `${prompt.user}\n\n${rewritePrompt(r.answer, unsupported)}`, temperature: 0, max_tokens: 600 });
        const still = unsupportedNumbers(again.answer, reply.evidence.map((e) => e.text), asked);
        if (still.length < unsupported.length) {
          reply.rewritten = unsupported;
          r = again;
          unsupported = still;
        }
      }
      reply.answer = r.answer;
      reply.model = `${r.model} (${r.provider})`;
      reply.unsupported = unsupported;
      push();
      setHistory((h) => [...h, { role: "user", text: question }, { role: "agent", text: r.answer }]);
    } catch (e) {
      reply.error = (e as Error).message;
      push();
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="card space-y-3">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <p className="max-w-3xl text-sm text-gray-600 dark:text-muted">
            A Thai visa assistant for <b>{PACK.name}</b> (a fictional agency). Ask in plain words. It works out your age, money and purpose,
            shortlists visa routes with SQL in NebulaDB, then answers <b>only from official Thai government websites</b>
            {gov.corpus && <> ({gov.corpus.sources.join(", ")}, checked {gov.corpus.fetched})</>}, with a link for every claim.
          </p>
          <div className="flex flex-wrap items-center gap-2">
            <label className="inline-flex items-center gap-2 text-xs text-gray-500 dark:text-muted">
              <span className="eyebrow">Signed in as</span>
              <select className="input !w-auto !py-1.5" value={client} onChange={(e) => setClient(e.target.value)}>
                <option value="">Guest</option>
                {CLIENTS.map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.label}
                  </option>
                ))}
              </select>
            </label>
            <ModelPicker env={env} value={provider} onChange={setProvider} />
            {replies.length > 0 && (
              <button className="btn-secondary !py-1" onClick={reset}>
                New conversation
              </button>
            )}
          </div>
        </div>
        <Banner kind="info">
          Not legal advice. Answers quote official government pages as they were when last checked; rules change, so confirm with the Thai
          embassy or Immigration before you apply. Clients and case notes are fictional. The agent never prepares false documents and never
          files anything.
        </Banner>
        {env?.mockLlmOnly && <Banner kind="warn">Only the mock LLM is configured: lookups and sources are real, the written reply is a placeholder.</Banner>}
        {env?.mockEmbedder && <Banner kind="warn">Mock embedder: rule search ranks by keywords only on this server.</Banner>}
        {seed.status !== "ready" && (
          <p className="text-xs text-gray-500 dark:text-muted">
            {seed.status === "error"
              ? `Couldn't load the visa data: ${seed.error}`
              : `Loading visa data and official sources into NebulaDB… ${packSeed.done}/${packSeed.total}${gov.status === "ready" ? "" : " · official pages"}`}
          </p>
        )}
      </div>

      <div className="grid gap-4 lg:grid-cols-[1fr_17rem]">
        <div className="space-y-3">
          {replies.length === 0 && (
            <div className="card space-y-2">
              <div className="eyebrow">Try</div>
              <div className="flex flex-wrap gap-1.5">
                {STARTERS.map((s) => (
                  <button
                    key={s}
                    onClick={() => void ask(s)}
                    disabled={busy || seed.status !== "ready"}
                    className="rounded-full border border-gray-200 dark:border-edge px-2.5 py-1 text-xs text-gray-700 dark:text-ink hover:border-accent hover:text-accent disabled:opacity-40"
                  >
                    {s}
                  </button>
                ))}
              </div>
              {!client && <p className="text-[11px] text-gray-500 dark:text-muted">Sign in as a demo client to ask about “my application”.</p>}
            </div>
          )}

          {replies.map((r, i) => (
            <ReplyCard key={i} r={r} />
          ))}
          <div ref={endRef} />
          <ErrorBanner err={err} />

          <form
            className="card !p-3 flex gap-2 sticky bottom-2"
            onSubmit={(e) => {
              e.preventDefault();
              void ask(input);
            }}
          >
            <input
              className="input"
              placeholder="Ask about a Thai visa…"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              disabled={seed.status !== "ready"}
              aria-label="Your question"
            />
            <button className="btn" disabled={busy || !input.trim() || seed.status !== "ready"}>
              {busy ? "Thinking…" : "Ask"}
            </button>
          </form>
        </div>

        <aside className="space-y-3">
          <div className="card !p-4 space-y-2">
            <div className="eyebrow">What the agent knows</div>
            <ProfileView p={profile} />
            <p className="text-[11px] text-gray-500 dark:text-muted">Read from your messages by code, not guessed by the model. It carries over between questions.</p>
          </div>
          <div className="card !p-4 space-y-1 text-[11px] text-gray-500 dark:text-muted">
            <div className="eyebrow">Guards in code</div>
            {PACK.safety.map((s) => (
              <div key={s.id}>• {s.label}</div>
            ))}
            <div>• Answers only from official government pages</div>
            <div>• Agency notes shortlist, never answer</div>
            <div>• Case notes only for the signed-in client</div>
            <div>• Numbers in replies checked against official text</div>
          </div>
        </aside>
      </div>
    </div>
  );
}

function ProfileView({ p }: { p: Profile }) {
  const rows: [string, string | undefined][] = [
    ["Age", p.age !== undefined ? String(p.age) : undefined],
    ["Savings", p.fundsThb !== undefined ? `${p.fundsThb.toLocaleString("en-GB")} THB` : undefined],
    ["Income", p.incomeThb !== undefined ? `${p.incomeThb.toLocaleString("en-GB")} THB / month` : undefined],
    ["Purpose", p.purposes.length ? p.purposes.join(", ") : undefined],
    ["Visa asked about", p.visa],
  ];
  return (
    <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs">
      {rows.map(([k, v]) => (
        <div key={k} className="contents">
          <dt className="text-gray-500 dark:text-muted">{k}</dt>
          <dd className={v ? "text-gray-900 dark:text-ink" : "text-gray-300 dark:text-faint"}>{v ?? "—"}</dd>
        </div>
      ))}
    </dl>
  );
}

function ReplyCard({ r }: { r: Reply }) {
  const [open, setOpen] = useState(false);
  const finder = r.steps.find((s) => s.call.tool === "visa_finder");
  const cs = r.answer ? claims(r.answer) : [];
  return (
    <div className="space-y-2">
      <div className="ml-auto max-w-[85%] rounded-2xl rounded-br-sm bg-black text-white dark:bg-ink dark:text-black px-3.5 py-2 text-sm w-fit">{r.question}</div>
      <div className="card !p-4 space-y-3">
        {r.card ? (
          <>
            <div className="eyebrow text-warn">Fixed response — a rule in code, the model was not asked</div>
            <p className="whitespace-pre-wrap text-sm text-black dark:text-ink">{r.card}</p>
          </>
        ) : (
          <>
            {finder && finder.rows.length > 0 && (
              <div>
                <div className="eyebrow mb-1.5">Shortlist from Lotus Gate's notes (SQL in NebulaDB) — the answer below uses official sources only</div>
                <div className="grid gap-2 sm:grid-cols-2">
                  {finder.rows.slice(0, 6).map((row) => (
                    <div key={row.id} className="rounded-lg border border-gray-200 dark:border-edge p-2.5 text-xs">
                      <div className="font-semibold text-black dark:text-ink">{String(row.meta?.visa ?? row.id)}</div>
                      <div className="mt-0.5 font-mono text-[10px] text-gray-500 dark:text-muted">
                        min age {String(row.meta?.min_age ?? 0) === "0" ? "—" : String(row.meta?.min_age)} · funds{" "}
                        {Number(row.meta?.min_funds_thb ?? 0) ? `${Number(row.meta?.min_funds_thb).toLocaleString("en-GB")} THB` : "—"}
                      </div>
                      <p className="mt-1 text-gray-600 dark:text-muted line-clamp-3">{row.text}</p>
                    </div>
                  ))}
                </div>
                {finder.note && <p className="mt-1 text-[11px] text-warn">{finder.note}</p>}
              </div>
            )}
            {finder && finder.rows.length === 0 && !finder.error && (
              <p className="text-xs text-gray-500 dark:text-muted">No visa type in our documents matches those details exactly.</p>
            )}
            {r.answer === undefined && !r.error && <p className="text-xs text-gray-500 dark:text-muted animate-pulse">Looking it up and writing a reply…</p>}
            {r.answer !== undefined && (
              <div className="space-y-1.5 text-sm">
                {cs.map((c, i) => (
                  <p key={i} className="text-gray-800 dark:text-ink">
                    {c.text.replace(/\[(\d+(?:\s*,\s*\d+)*)\]/g, "").replace(/\(\s*sources?:?\s*\)/gi, "")}{" "}
                    {c.cites.map((n) => (
                      <span
                        key={n}
                        title={r.evidence[n - 1] ? `${r.evidence[n - 1].authority ?? "Your case file"}\n${r.evidence[n - 1].url ?? ""}\n\n${r.evidence[n - 1].text}` : "no such source"}
                        className={`mx-0.5 rounded px-1 font-mono text-[10px] ${r.evidence[n - 1] ? "bg-accent/15 text-accent" : "bg-bad/15 text-bad"}`}
                      >
                        {n}
                      </span>
                    ))}
                  </p>
                ))}
              </div>
            )}
            {r.answer !== undefined && (
              <div className="flex flex-wrap gap-3 text-[11px] text-gray-500 dark:text-muted">
                <span>{r.model}</span>
                <span>
                  {cs.filter((c) => c.cites.length).length}/{cs.length} sentences cited
                </span>
                <span className={r.unsupported.length ? "text-bad" : "text-ok"}>
                  {r.unsupported.length ? `numbers not in any official passage: ${r.unsupported.join(", ")}` : "every number is in an official passage"}
                </span>
                {r.rewritten && <span className="text-warn">first draft quoted {r.rewritten.join(", ")} (not in any passage) — rewritten from the sources</span>}
              </div>
            )}
            {r.answer !== undefined && r.evidence.length > 0 && <SourceList evidence={r.evidence} />}
            {r.error && <p className="text-xs text-bad">{r.error}</p>}
          </>
        )}
        {(r.steps.length > 0 || r.evidence.length > 0) && (
          <div>
            <button className="text-[11px] text-accent hover:underline" onClick={() => setOpen((v) => !v)}>
              {open ? "Hide" : "Show"} how it answered ({r.steps.length} lookup{r.steps.length === 1 ? "" : "s"}, {r.evidence.length} sources)
            </button>
            {open && (
              <div className="mt-2 space-y-2">
                {r.steps.map((s, i) => (
                  <div key={i} className="rounded-md border border-gray-200 dark:border-edge p-2 text-[11px]">
                    <div className="flex gap-2">
                      <span className="font-mono text-accent">{s.call.tool}</span>
                      <span className="text-gray-500 dark:text-muted">{s.call.why}</span>
                      <span className="ml-auto font-mono text-gray-400 dark:text-faint">
                        {s.rows.length} rows · {s.ms} ms
                      </span>
                    </div>
                    <pre className="mt-1 whitespace-pre-wrap font-mono text-gray-600 dark:text-muted">{s.call.sql ?? s.call.query}</pre>
                    {s.error && <p className="text-bad">{s.error}</p>}
                  </div>
                ))}
                <ol className="space-y-1">
                  {r.evidence.map((e, i) => (
                    <li key={e.id} className="text-[11px] text-gray-600 dark:text-muted">
                      <span className="font-mono text-accent">[{i + 1}]</span> <span className="font-mono">{e.id}</span>
                      {e.url && <> · {e.url}</>} — {e.text}
                    </li>
                  ))}
                </ol>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

/** Official pages behind a reply, one line each with a link. */
function SourceList({ evidence }: { evidence: Evidence[] }) {
  const pages = new Map<string, { authority: string; nums: number[] }>();
  evidence.forEach((e, i) => {
    const key = e.url || "case";
    const p = pages.get(key) ?? { authority: e.authority || "Your case file", nums: [] };
    p.nums.push(i + 1);
    pages.set(key, p);
  });
  return (
    <div className="rounded-md bg-gray-50 dark:bg-white/5 p-2 text-[11px]">
      <div className="eyebrow mb-1">Official sources</div>
      <ul className="space-y-0.5">
        {[...pages].map(([url, p]) => (
          <li key={url} className="text-gray-600 dark:text-muted">
            <span className="font-mono text-accent">[{p.nums.join(",")}]</span> {p.authority}
            {url !== "case" && (
              <>
                {" — "}
                <a className="text-accent hover:underline break-all" href={url} target="_blank" rel="noreferrer">
                  {url.replace(/^https:\/\//, "")}
                </a>
              </>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}
