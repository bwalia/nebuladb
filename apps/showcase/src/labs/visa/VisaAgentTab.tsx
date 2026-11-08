/**
 * Visa AI Agent — a conversational Thai visa assistant over NebulaDB.
 *
 * Code decides what to look up (profile extraction, tool choice, SQL),
 * NebulaDB answers the lookups, and the configured LLM only writes the
 * reply from those results. Every reply shows its tool calls, sources and
 * a numbers check. Data: the Labs `visa` pack (fictional agency).
 */
import { useEffect, useRef, useState } from "react";
import { api } from "../../api";
import { ErrorBanner } from "../../components";
import { claims, flatFields, unsupportedNumbers } from "../engine/core";
import { PACK } from "../packs/visa";
import { Banner, ModelPicker, useLabEnv, usePackSeed } from "../shell";
import { answerPrompt, parseProfile, plan, type Evidence, type Profile, type ToolCall, type Turn } from "./agent";

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

export function VisaAgentTab() {
  const env = useLabEnv();
  const seed = usePackSeed(PACK);
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
      for (const call of p.calls) {
        const t0 = performance.now();
        const run: StepRun = { call, rows: [], ms: 0 };
        try {
          if (call.tool === "search_rules") {
            const r = await api.searchEx({ query: call.query!, top_k: 5, bucket: PACK.bucket, hybrid: true });
            run.rows = r.hits.map((h) => ({ id: h.id, text: h.text, meta: h.metadata as Record<string, unknown> }));
          } else {
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
          }
        } catch (e) {
          run.error = (e as Error).message;
        }
        run.ms = Math.round(performance.now() - t0);
        reply.steps.push(run);
        push();
      }
      // Evidence: finder hits first (ranked by meaning), then rule passages; dedupe; cap.
      const seen = new Set<string>();
      for (const s of reply.steps)
        for (const row of s.rows.slice(0, s.call.tool === "search_rules" ? 5 : 6)) {
          if (seen.has(row.id) || !row.text) continue;
          seen.add(row.id);
          reply.evidence.push({ id: row.id, text: row.text, from: s.call.tool });
        }
      reply.evidence = reply.evidence.slice(0, 10);
      push();

      const prompt = answerPrompt(PACK, question, prof, reply.evidence, history);
      const r = await api.aiChat({ provider, system: prompt.system, message: prompt.user, temperature: 0.1, max_tokens: 600 });
      reply.answer = r.answer;
      reply.model = `${r.model} (${r.provider})`;
      reply.unsupported = unsupportedNumbers(r.answer, reply.evidence.map((e) => e.text), [question, ...history.filter((h) => h.role === "user").map((h) => h.text)].join(" "));
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
            looks up the visa routes that fit with SQL in NebulaDB, searches the rules, and answers only from what it found, with sources.
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
          Demo data, not legal advice. Thai immigration rules change often — every answer must be checked with Thai Immigration or a Thai
          embassy. The agent never prepares false documents and never files anything.
        </Banner>
        {env?.mockLlmOnly && <Banner kind="warn">Only the mock LLM is configured: lookups and sources are real, the written reply is a placeholder.</Banner>}
        {env?.mockEmbedder && <Banner kind="warn">Mock embedder: rule search ranks by keywords only on this server.</Banner>}
        {seed.status !== "ready" && (
          <p className="text-xs text-gray-500 dark:text-muted">
            {seed.status === "error" ? `Couldn't load the visa data: ${seed.error}` : `Loading visa data into NebulaDB… ${seed.done}/${seed.total}`}
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
            <div>• Case notes only for the signed-in client</div>
            <div>• Numbers in replies checked against sources</div>
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
                <div className="eyebrow mb-1.5">Visa routes that fit (from NebulaDB, not the model)</div>
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
                        title={r.evidence[n - 1]?.text ?? "no such source"}
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
                  {r.unsupported.length ? `numbers not in any source: ${r.unsupported.join(", ")}` : "every number is in a source"}
                </span>
              </div>
            )}
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
                    <pre className="mt-1 whitespace-pre-wrap font-mono text-gray-600 dark:text-muted">{s.call.sql ?? `hybrid search: "${s.call.query}"`}</pre>
                    {s.error && <p className="text-bad">{s.error}</p>}
                  </div>
                ))}
                <ol className="space-y-1">
                  {r.evidence.map((e, i) => (
                    <li key={e.id} className="text-[11px] text-gray-600 dark:text-muted">
                      <span className="font-mono text-accent">[{i + 1}]</span> <span className="font-mono">{e.id}</span> — {e.text}
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
