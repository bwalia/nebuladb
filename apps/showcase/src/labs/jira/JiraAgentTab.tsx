/**
 * Jira Triage Agent — watches the custom Priority Score field (1–100) and
 * writes the manager's digest.
 *
 * Each run: load the project's tickets and stored snapshots from NebulaDB
 * (SQL), diff the chosen run against the previous one in code (new, up,
 * down, resolved, rank moves), look up near-duplicates for every new ticket
 * (vector search), and have the LLM write only the opening paragraph of the
 * email from numbered facts. The history table is the Confluence page the
 * agent keeps.
 */
import { useEffect, useMemo, useState } from "react";
import { ensureSeeded } from "../../rag/seed";
import { api } from "../../api";
import { flatFields, unsupportedNumbers } from "../engine/core";
import { Banner, ModelPicker, useLabEnv } from "../shell";
import { DAILY_RUNS, PROJECTS, RUN_TIME, WEEKLY_RUNS, type Project, type Ticket } from "./data";
import {
  askPrompt,
  BUCKET,
  buildCorpus,
  checkCites,
  type CheckedClaim,
  confluenceStorage,
  CORPUS_VERSION,
  csv,
  diff,
  digestEvidence,
  directionErrors,
  directionRewritePrompt,
  finalSummary,
  numberErrors,
  type DirectionError,
  DUPLICATE_DISTANCE,
  eml,
  emailHtml,
  facts,
  headlineFacts,
  heat,
  historyMatrix,
  jql,
  KEY_RE,
  NOT_COVERED,
  position,
  RELATED_DISTANCE,
  rewritePrompt,
  searchSql,
  similarSql,
  snapshotFromRow,
  snapshotsSql,
  subject,
  summaryPrompt,
  ticketEvidence,
  ticketFromRow,
  ticketsSql,
  type Cadence,
  type Evidence,
  type Fact,
  type Snapshot,
  type ToolCall,
} from "./agent";

interface StepRun {
  call: ToolCall;
  rows: number;
  ms: number;
  error?: string;
}

interface Similar {
  key: string;
  summary: string;
  team: string;
  distance: number;
}

interface Summary {
  text?: string;
  model?: string;
  unsupported: string[];
  rewritten?: string[];
  /** Direction mistakes in the first draft that a rewrite fixed. */
  directionFixed?: DirectionError[];
  /** Sentences still wrong after the rewrite, left out of the email. */
  removed?: DirectionError[];
  error?: string;
}

const STARTERS: Record<string, string[]> = {
  TIX: ["Why did TIX-1201 jump to the top?", "What could block the stadium tour on-sale?", "What did Legal ask us to fix, and by when?"],
  PAY: ["Why is PAY-2231 climbing every few weeks?", "What happened with the contactless declines?", "Which items have a deadline in December?"],
  GRO: ["Why is GRO-881 suddenly number one?", "What else is linked to Christmas peak?", "What dropped in priority this week and why?"],
};

const fmt = (iso: string) => new Date(`${iso}T12:00:00Z`).toLocaleDateString("en-GB", { weekday: "short", day: "numeric", month: "short", timeZone: "UTC" });

function download(name: string, body: string, type: string) {
  const url = URL.createObjectURL(new Blob([body], { type }));
  const a = Object.assign(document.createElement("a"), { href: url, download: name });
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function useJiraSeed() {
  const [s, setS] = useState<{ status: "seeding" | "ready" | "error"; error?: string }>({ status: "seeding" });
  useEffect(() => {
    let live = true;
    ensureSeeded(BUCKET, buildCorpus(), CORPUS_VERSION)
      .then(() => live && setS({ status: "ready" }))
      .catch((e: Error) => live && setS({ status: "error", error: e.message }));
    return () => {
      live = false;
    };
  }, []);
  return s;
}

export function JiraAgentTab() {
  const env = useLabEnv();
  const seed = useJiraSeed();
  const [projectKey, setProjectKey] = useState("GRO");
  const [cadence, setCadence] = useState<Cadence>("weekly");
  const runs = cadence === "weekly" ? WEEKLY_RUNS : DAILY_RUNS;
  const [runDate, setRunDate] = useState(runs[runs.length - 1]);
  const [provider, setProvider] = useState<string | undefined>();
  const project = PROJECTS.find((p) => p.key === projectKey)!;

  const [tickets, setTickets] = useState<Ticket[]>([]);
  const [snaps, setSnaps] = useState<Snapshot[]>([]);
  const [steps, setSteps] = useState<StepRun[]>([]);
  const [similar, setSimilar] = useState<Record<string, Similar[]>>({});
  const [summary, setSummary] = useState<Summary>({ unsupported: [] });
  const [loadErr, setLoadErr] = useState<string | null>(null);

  useEffect(() => setRunDate(runs[runs.length - 1]), [cadence]); // eslint-disable-line react-hooks/exhaustive-deps

  /** Run one SQL tool call; its trace entry is only added if the run that started it is still current. */
  const sqlStep = async (call: ToolCall, onRows: (rows: Record<string, unknown>[], scores: number[]) => void, live: () => boolean) => {
    const t0 = performance.now();
    const run: StepRun = { call, rows: 0, ms: 0 };
    try {
      const r = await api.sql(call.sql);
      run.rows = r.rows.length;
      onRows(
        r.rows.map((x) => flatFields(x.fields)),
        r.rows.map((x) => x.score)
      );
    } catch (e) {
      run.error = (e as Error).message;
    }
    run.ms = Math.round(performance.now() - t0);
    if (live()) setSteps((s) => [...s, run]);
    return run;
  };

  // 1. Load the project's tickets and stored snapshots.
  useEffect(() => {
    if (seed.status !== "ready") return;
    let live = true;
    setSteps([]);
    setLoadErr(null);
    setTickets([]);
    setSnaps([]);
    (async () => {
      let ts: Ticket[] = [];
      let ss: Snapshot[] = [];
      const a = await sqlStep({ tool: "load_tickets", why: `every ${project.key} ticket with a Priority Score, with its score changelog`, sql: ticketsSql(project.key) }, (rows) => {
        ts = rows.map(ticketFromRow);
      }, () => live);
      const b = await sqlStep({ tool: "load_snapshots", why: `the ${cadence} snapshots this agent stored after each run`, sql: snapshotsSql(project.key, cadence) }, (rows) => {
        ss = rows.map(snapshotFromRow).sort((x, y) => x.date.localeCompare(y.date));
      }, () => live);
      if (!live) return;
      if (a.error || b.error) setLoadErr(a.error ?? b.error ?? null);
      setTickets(ts);
      setSnaps(ss);
    })();
    return () => {
      live = false;
    };
  }, [seed.status, project.key, cadence]); // eslint-disable-line react-hooks/exhaustive-deps

  const shown = useMemo(() => snaps.filter((s) => s.date <= runDate), [snaps, runDate]);
  const digest = useMemo(() => (shown.length >= 2 ? diff(shown[shown.length - 2], shown[shown.length - 1], tickets) : null), [shown, tickets]);
  const fs = useMemo<Fact[]>(() => (digest ? facts(digest, tickets) : []), [digest, tickets]);
  /** What the LLM sees: headline counts and the biggest moves (the email's tables carry the full lists). */
  const hf = useMemo(() => headlineFacts(fs), [fs]);

  // 2. Triage the new tickets (nearest neighbours) and write the summary.
  useEffect(() => {
    if (!digest) return;
    let live = true;
    setSimilar({});
    setSummary({ unsupported: [] });
    setSteps((s) => s.filter((x) => x.call.tool === "load_tickets" || x.call.tool === "load_snapshots"));
    (async () => {
      for (const m of digest.added) {
        const t = tickets.find((x) => x.key === m.key);
        if (!t) continue;
        await sqlStep({ tool: "find_similar", why: `possible duplicates of new ticket ${t.key}`, sql: similarSql(project.key, t) }, (rows, scores) => {
          if (!live) return;
          const list = rows
            .map((r, i) => ({ key: String(r.key), summary: String(r.summary ?? ""), team: String(r.team ?? ""), distance: scores[i] }))
            .filter((r) => r.key !== t.key)
            .slice(0, 3);
          setSimilar((s) => ({ ...s, [t.key]: list }));
        }, () => live);
      }
      if (!live) return;
      const pr = summaryPrompt(project, digest, hf, cadence);
      const factText = hf.map((f) => f.text);
      try {
        let r = await api.aiChat({ provider, system: pr.system, message: pr.user, temperature: 0.1, max_tokens: 400 });
        let un = unsupportedNumbers(r.answer, factText);
        let rewritten: string[] | undefined;
        if (un.length) {
          const again = await api.aiChat({ provider, system: pr.system, message: `${pr.user}\n\n${rewritePrompt(r.answer, un)}`, temperature: 0, max_tokens: 400 });
          const still = unsupportedNumbers(again.answer, factText);
          if (still.length < un.length) {
            rewritten = un;
            r = again;
            un = still;
          }
        }
        // Direction check: "eased off from 71 to 88" passes the number check but is wrong.
        let dir = directionErrors(r.answer, digest);
        let directionFixed: DirectionError[] | undefined;
        if (dir.length) {
          const again = await api.aiChat({ provider, system: pr.system, message: `${pr.user}\n\n${directionRewritePrompt(r.answer, dir)}`, temperature: 0, max_tokens: 400 });
          const dir2 = directionErrors(again.answer, digest);
          const un2 = unsupportedNumbers(again.answer, factText);
          if (dir2.length < dir.length && un2.length <= un.length) {
            directionFixed = dir;
            r = again;
            un = un2;
            dir = dir2;
          }
        }
        // Anything still wrong after one rewrite is left out of the email (or the paragraph falls back to the facts).
        dir = [...dir, ...numberErrors(r.answer, factText)];
        const fin = finalSummary(r.answer, dir, hf);
        if (live)
          setSummary({
            text: fin.text,
            model: fin.fallback ? `facts only — too little of the ${r.model} paragraph passed the checks` : `${r.model} (${r.provider})`,
            unsupported: unsupportedNumbers(fin.text, factText),
            rewritten,
            directionFixed,
            removed: dir.length ? dir : undefined,
          });
      } catch (e) {
        if (live) setSummary({ unsupported: [], error: (e as Error).message });
      }
    })();
    return () => {
      live = false;
    };
  }, [digest, provider]); // eslint-disable-line react-hooks/exhaustive-deps

  const subj = digest ? subject(project, digest, cadence) : "";
  const fallback = digest ? `${fs[0]?.text ?? ""}` : "";
  const html = digest ? emailHtml(project, digest, tickets, cadence, summary.text ?? fallback) : "";
  const rows = useMemo(() => historyMatrix(shown, tickets), [shown, tickets]);

  return (
    <div className="space-y-4">
      <div className="card space-y-3">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <p className="max-w-3xl text-sm text-gray-600 dark:text-muted">
            An agent that watches one Jira field — the custom <b>Priority Score</b> (1–100, 100 = most urgent), not Jira's own Priority. After every{" "}
            {cadence} run it emails the manager what is new, what moved up or down and what was resolved, ranks tickets <b>relative to each other</b>, flags
            likely duplicates, and keeps the score history as a Confluence table. Tickets and snapshots live in NebulaDB; code computes every change; the LLM
            only writes the opening paragraph, and every number in it is checked.
          </p>
          <ModelPicker env={env} value={provider} onChange={setProvider} />
        </div>
        <div className="grid gap-2 sm:grid-cols-3">
          {PROJECTS.map((p) => (
            <button
              key={p.key}
              onClick={() => setProjectKey(p.key)}
              className={`rounded-lg border p-2.5 text-left text-xs transition ${p.key === projectKey ? "border-accent bg-accent/5" : "border-gray-200 dark:border-edge hover:border-accent"}`}
            >
              <div className="font-semibold text-black dark:text-ink">
                {p.company} · <span className="font-mono">{p.key}</span>
              </div>
              <div className="text-gray-500 dark:text-muted">
                {p.name} — {p.business}
              </div>
            </button>
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-3 text-xs">
          <div className="inline-flex rounded-md border border-gray-200 dark:border-edge overflow-hidden">
            {(["weekly", "daily"] as Cadence[]).map((c) => (
              <button key={c} onClick={() => setCadence(c)} className={`px-3 py-1.5 ${c === cadence ? "bg-accent text-white" : "text-gray-600 dark:text-muted"}`}>
                {c === "weekly" ? "Weekly digest" : "Daily digest"}
              </button>
            ))}
          </div>
          <label className="inline-flex items-center gap-2 text-gray-500 dark:text-muted">
            <span className="eyebrow">Run</span>
            <select className="input !w-auto !py-1.5" value={runDate} onChange={(e) => setRunDate(e.target.value)}>
              {runs.slice(1).map((d) => (
                <option key={d} value={d}>
                  {fmt(d)} {RUN_TIME}
                </option>
              ))}
            </select>
          </label>
          <MonitorSettings p={project} cadence={cadence} />
        </div>
        <Banner kind="info">
          Encore Tickets, Harbour Bank and Freshway are fictional companies; the tickets are realistic demo data. In production the same agent reads Jira
          Cloud with the JQL shown under “Monitor settings” and sends the email and page update itself.
        </Banner>
        {env?.mockLlmOnly && <Banner kind="warn">Only the mock LLM is configured: the tables and changes are real, the opening paragraph is a placeholder.</Banner>}
        {seed.status !== "ready" && (
          <p className="text-xs text-gray-500 dark:text-muted">{seed.status === "error" ? `Couldn't load the Jira data: ${seed.error}` : "Loading tickets and snapshots into NebulaDB…"}</p>
        )}
        {loadErr && <p className="text-xs text-bad">{loadErr}</p>}
      </div>

      <div className="grid gap-4 xl:grid-cols-[1fr_20rem]">
        <div className="space-y-4 min-w-0">
          {digest && (
            <div className="card space-y-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="eyebrow">The email the manager gets</div>
                <div className="flex gap-2">
                  <button className="btn-secondary !py-1 text-xs" onClick={() => void navigator.clipboard.writeText(html)}>
                    Copy HTML
                  </button>
                  <button className="btn-secondary !py-1 text-xs" onClick={() => download(`${project.key}-digest-${digest.to}.eml`, eml(project, subj, html, digest.to), "message/rfc822")}>
                    Download .eml
                  </button>
                </div>
              </div>
              <div className="rounded-lg border border-gray-200 dark:border-edge overflow-hidden">
                <div className="bg-gray-50 dark:bg-white/5 px-3 py-2 text-xs space-y-0.5">
                  <div>
                    <span className="text-gray-500 dark:text-muted">From</span> Jira Triage Agent
                  </div>
                  <div>
                    <span className="text-gray-500 dark:text-muted">To</span> {project.manager} &lt;{project.managerEmail}&gt;
                  </div>
                  <div className="font-semibold text-black dark:text-ink">{subj}</div>
                </div>
                {summary.text === undefined && !summary.error && <p className="px-4 pt-3 text-xs text-gray-500 animate-pulse">Writing the summary from the facts…</p>}
                {summary.error && <p className="px-4 pt-3 text-xs text-bad">Summary not written ({summary.error}) — the email falls back to the plain counts.</p>}
                <div className="bg-white p-4 overflow-x-auto" dangerouslySetInnerHTML={{ __html: html }} />
              </div>
              {summary.text && <SummaryCheck text={summary.text} facts={hf} s={summary} />}
            </div>
          )}

          {digest && digest.added.length > 0 && (
            <div className="card space-y-2">
              <div className="eyebrow">Triage: new tickets and their nearest existing tickets (vector search in NebulaDB)</div>
              {digest.added.map((m) => {
                const t = tickets.find((x) => x.key === m.key);
                const sims = similar[m.key];
                const dup = sims?.find((s) => s.distance < DUPLICATE_DISTANCE);
                const unowned = t && (t.assignee === "Unassigned" || /support/i.test(t.team));
                return (
                  <div key={m.key} className="rounded-lg border border-gray-200 dark:border-edge p-3 text-xs space-y-1.5">
                    <div className="flex flex-wrap items-baseline gap-2">
                      <span className="font-mono font-semibold text-accent">{m.key}</span>
                      <span className="text-black dark:text-ink">{t?.summary}</span>
                      <span className="ml-auto font-mono">
                        score <b>{m.to}</b> · {position(m.rankTo!, digest.openTo)}
                      </span>
                    </div>
                    <div className="text-gray-500 dark:text-muted">
                      {t?.type} · {t?.component} · {t?.team} · {t?.assignee} · Jira Priority “{t?.jiraPriority}” (ignored for ranking)
                    </div>
                    {!sims && <div className="text-gray-400 animate-pulse">Searching similar tickets…</div>}
                    {sims?.map((s) => (
                      <div key={s.key} className="flex items-center gap-2">
                        <span className="font-mono w-16">{s.key}</span>
                        <span className="flex-1 text-gray-600 dark:text-muted truncate">{s.summary}</span>
                        <span className="font-mono text-[10px] text-gray-400">distance {s.distance.toFixed(3)}</span>
                        <span
                          className={`rounded px-1.5 py-0.5 text-[10px] ${s.distance < DUPLICATE_DISTANCE ? "bg-bad/15 text-bad" : s.distance < RELATED_DISTANCE ? "bg-warn/15 text-warn" : "bg-gray-100 dark:bg-white/10 text-gray-500"}`}
                        >
                          {s.distance < DUPLICATE_DISTANCE ? "likely duplicate" : s.distance < RELATED_DISTANCE ? "related" : "different"}
                        </span>
                      </div>
                    ))}
                    {dup && (
                      <p className="text-bad">
                        Suggest: link {m.key} as a duplicate of {dup.key} (owned by {dup.team}) and keep one Priority Score, so the same problem is not
                        ranked twice.
                      </p>
                    )}
                    {!dup && unowned && sims?.[0] && <p className="text-warn">Suggest: route to {sims[0].team}, which owns the closest ticket ({sims[0].key}).</p>}
                  </div>
                );
              })}
            </div>
          )}

          {shown.length > 1 && <HistoryPage p={project} cadence={cadence} snaps={shown} rows={rows} />}

          <AskBox p={project} tickets={tickets} snaps={shown} provider={provider} ready={tickets.length > 0} digest={digest ? digestEvidence(digest, fs, cadence) : undefined} />
        </div>

        <aside className="space-y-3">
          <div className="card !p-4 space-y-2">
            <div className="eyebrow">This run — what the agent did</div>
            {steps.map((s, i) => (
              <StepView key={i} s={s} />
            ))}
            {digest && (
              <div className="rounded-md border border-gray-200 dark:border-edge p-2 text-[11px]">
                <span className="font-mono text-accent">diff (code)</span>{" "}
                <span className="text-gray-500 dark:text-muted">
                  {fmt(digest.from)} → {fmt(digest.to)}: {digest.added.length} new, {digest.up.length} up, {digest.down.length} down, {digest.resolved.length} resolved
                </span>
              </div>
            )}
            {summary.model && (
              <div className="rounded-md border border-gray-200 dark:border-edge p-2 text-[11px]">
                <span className="font-mono text-accent">write_summary (LLM)</span> <span className="text-gray-500 dark:text-muted">{summary.model}</span>
              </div>
            )}
          </div>
          <div className="card !p-4 space-y-1 text-[11px] text-gray-500 dark:text-muted">
            <div className="eyebrow">Guards in code</div>
            <div>• Ranks by Priority Score only, never Jira Priority</div>
            <div>• Every change is computed by code from stored snapshots</div>
            <div>• The LLM writes one paragraph from numbered facts</div>
            <div>• Every number it writes is checked; one rewrite if not</div>
            <div>• Tables in the email are generated, not written by the model</div>
            <div>• Duplicates are suggested, never merged automatically</div>
          </div>
        </aside>
      </div>
    </div>
  );
}

function StepView({ s }: { s: StepRun }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="rounded-md border border-gray-200 dark:border-edge p-2 text-[11px]">
      <button className="flex w-full gap-2 text-left" onClick={() => setOpen((v) => !v)}>
        <span className="font-mono text-accent">{s.call.tool}</span>
        <span className="ml-auto font-mono text-gray-400 dark:text-faint">
          {s.rows} rows · {s.ms} ms
        </span>
      </button>
      <div className="text-gray-500 dark:text-muted">{s.call.why}</div>
      {open && <pre className="mt-1 whitespace-pre-wrap font-mono text-gray-600 dark:text-muted">{s.call.sql}</pre>}
      {s.error && <p className="text-bad">{s.error}</p>}
    </div>
  );
}

function SummaryCheck({ text, facts: fs, s }: { text: string; facts: Fact[]; s: Summary }) {
  const [open, setOpen] = useState(false);
  const cs = checkCites(text, fs.map((f) => f.text));
  return (
    <div className="rounded-md bg-gray-50 dark:bg-white/5 p-3 text-xs space-y-2">
      <div className="eyebrow">Where the opening paragraph came from</div>
      <div className="space-y-1">
        {cs.map((c, i) => (
          <Sentence key={i} c={c} label={(n) => String(n)} title={(n) => fs[n - 1]?.text} />
        ))}
      </div>
      <div className="flex flex-wrap gap-3 text-[11px] text-gray-500 dark:text-muted">
        <span>{s.model}</span>
        <CiteStats cs={cs} />
        <span className={s.unsupported.length ? "text-bad" : "text-ok"}>
          {s.unsupported.length ? `numbers not in any fact: ${s.unsupported.join(", ")}` : "every number is in a fact"}
        </span>
        {s.rewritten && <span className="text-warn">first draft used {s.rewritten.join(", ")} (not in the facts) — rewritten</span>}
        <span className={s.removed ? "text-warn" : "text-ok"}>{s.removed ? `${s.removed.length} claim(s) failed the checks and were left out` : "every up/down/top claim matches the data"}</span>
        <button className="text-accent hover:underline" onClick={() => setOpen((v) => !v)}>
          {open ? "Hide" : "Show"} the {fs.length} facts
        </button>
      </div>
      {(s.directionFixed || s.removed) && (
        <div className="space-y-0.5 text-[11px]">
          {s.directionFixed?.map((e, i) => (
            <div key={i} className="text-warn">
              Draft fixed: “{e.sentence}” — {e.problem}
            </div>
          ))}
          {s.removed?.map((e, i) => (
            <div key={i} className="text-bad">
              Removed from the email: “{e.sentence}” — {e.problem}
            </div>
          ))}
        </div>
      )}
      {open && (
        <ol className="space-y-0.5 text-[11px] text-gray-600 dark:text-muted">
          {fs.map((f, i) => (
            <li key={i}>
              <span className="font-mono text-accent">[{i + 1}]</span> {f.text}
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}

/** One sentence with its citation chips; a dashed chip is a citation code corrected or added. */
function Sentence({ c, label, title }: { c: CheckedClaim; label: (n: number) => string; title: (n: number) => string | undefined }) {
  return (
    <p className="text-gray-800 dark:text-ink">
      {c.text}{" "}
      {c.cites.map((n) => (
        <span
          key={n}
          title={`${c.fixed ? "Citation set by the code check (the model's was missing or wrong)\n\n" : ""}${title(n) ?? ""}`}
          className={`mx-0.5 rounded px-1 font-mono text-[10px] bg-accent/15 text-accent ${c.fixed ? "border border-dashed border-warn" : ""}`}
        >
          {label(n)}
        </span>
      ))}
      {!c.cites.length && <span className="mx-0.5 rounded px-1 font-mono text-[10px] bg-gray-100 dark:bg-white/10 text-gray-400">no source</span>}
    </p>
  );
}

function CiteStats({ cs }: { cs: CheckedClaim[] }) {
  const fixed = cs.filter((c) => c.fixed).length;
  return (
    <span>
      {cs.filter((c) => c.cites.length).length}/{cs.length} sentences sourced
      {fixed > 0 && <span className="text-warn"> · {fixed} citation{fixed === 1 ? "" : "s"} corrected by code</span>}
    </span>
  );
}

function Spark({ series }: { series: (number | null)[] }) {
  const pts = series.map((v, i) => (v === null ? null : `${(i / Math.max(series.length - 1, 1)) * 60},${20 - (v / 100) * 18}`)).filter(Boolean);
  return (
    <svg width="62" height="22" viewBox="-1 0 62 22" aria-hidden className="inline-block">
      <polyline points={pts.join(" ")} fill="none" stroke="currentColor" strokeWidth="1.5" className="text-accent" />
    </svg>
  );
}

function HistoryPage({ p, cadence, snaps, rows }: { p: Project; cadence: Cadence; snaps: Snapshot[]; rows: ReturnType<typeof historyMatrix> }) {
  const [copied, setCopied] = useState(false);
  const last = snaps[snaps.length - 1];
  return (
    <div className="card space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <div className="eyebrow">Confluence · {p.confluenceSpace} · page kept by the agent</div>
          <h3 className="text-base font-semibold text-black dark:text-ink">
            {p.key} Priority Score history ({cadence})
          </h3>
          <p className="text-[11px] text-gray-500 dark:text-muted">
            One column per run, rows ordered by the latest score. Updated {fmt(last.date)} {RUN_TIME} by Jira Triage Agent.
          </p>
        </div>
        <div className="flex gap-2">
          <button
            className="btn-secondary !py-1 text-xs"
            onClick={() => {
              void navigator.clipboard.writeText(confluenceStorage(p, cadence, snaps, rows));
              setCopied(true);
              setTimeout(() => setCopied(false), 1500);
            }}
          >
            {copied ? "Copied" : "Copy Confluence storage format"}
          </button>
          <button className="btn-secondary !py-1 text-xs" onClick={() => download(`${p.key}-priority-score-${cadence}.csv`, csv(snaps, rows), "text/csv")}>
            CSV
          </button>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-[11px]">
          <thead>
            <tr className="text-gray-500 dark:text-muted">
              <th className="p-1 text-left font-medium">Ticket</th>
              <th className="p-1 text-left font-medium min-w-[14rem]">Summary</th>
              {snaps.map((s) => (
                <th key={s.date} className="p-1 font-medium whitespace-nowrap">
                  {fmt(s.date).replace(/^\w+ /, "")}
                </th>
              ))}
              <th className="p-1 font-medium">Trend</th>
              <th className="p-1 font-medium">Change</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.key} className="border-t border-gray-100 dark:border-edge">
                <td className="p-1 font-mono text-accent whitespace-nowrap">{r.key}</td>
                <td className="p-1 text-gray-700 dark:text-ink">{r.summary}</td>
                {r.series.map((v, i) => (
                  <td key={i} className="p-1 text-center font-mono" style={{ background: heat(v), color: v !== null && v >= 70 ? "white" : undefined }}>
                    {v ?? ""}
                  </td>
                ))}
                <td className="p-1">
                  <Spark series={r.series} />
                </td>
                <td className="p-1 whitespace-nowrap font-mono">
                  {r.latest === undefined ? (
                    <span className="text-gray-400">resolved</span>
                  ) : r.delta === undefined ? (
                    <span className="text-accent">new</span>
                  ) : r.delta > 0 ? (
                    <span className="text-bad">▲ {r.delta}</span>
                  ) : r.delta < 0 ? (
                    <span className="text-ok">▼ {-r.delta}</span>
                  ) : (
                    <span className="text-gray-400">=</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function MonitorSettings({ p, cadence }: { p: Project; cadence: Cadence }) {
  return (
    <details className="text-xs">
      <summary className="cursor-pointer text-accent">Monitor settings</summary>
      <dl className="mt-2 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 rounded-md bg-gray-50 dark:bg-white/5 p-3">
        <dt className="text-gray-500">Field</dt>
        <dd>
          Priority Score <span className="font-mono">{p.scoreField}</span> (number 1–100)
        </dd>
        <dt className="text-gray-500">JQL</dt>
        <dd className="font-mono break-all">{jql(p)}</dd>
        <dt className="text-gray-500">Schedule</dt>
        <dd>{cadence === "weekly" ? `Mondays ${RUN_TIME} Europe/London` : `Weekdays ${RUN_TIME} Europe/London`}</dd>
        <dt className="text-gray-500">Email to</dt>
        <dd>
          {p.manager} &lt;{p.managerEmail}&gt;
        </dd>
        <dt className="text-gray-500">Confluence</dt>
        <dd>
          space {p.confluenceSpace}, page “{p.key} Priority Score history”
        </dd>
        <dt className="text-gray-500">Stored in</dt>
        <dd>
          NebulaDB bucket <span className="font-mono">{BUCKET}</span>: one doc per ticket, one snapshot doc per run
        </dd>
      </dl>
    </details>
  );
}

interface Answer {
  q: string;
  evidence: Evidence[];
  text?: string;
  model?: string;
  unsupported: string[];
  error?: string;
}

function AskBox({ p, tickets, snaps, provider, ready, digest }: { p: Project; tickets: Ticket[]; snaps: Snapshot[]; provider?: string; ready: boolean; digest?: Evidence }) {
  const [input, setInput] = useState("");
  const [answers, setAnswers] = useState<Answer[]>([]);
  const [busy, setBusy] = useState(false);
  useEffect(() => setAnswers([]), [p.key]);

  const ask = async (q: string) => {
    if (!q.trim() || busy) return;
    setBusy(true);
    setInput("");
    const a: Answer = { q, evidence: [], unsupported: [] };
    setAnswers((x) => [a, ...x]);
    const put = () => setAnswers((x) => [{ ...a }, ...x.slice(1)]);
    try {
      // Tickets named in the question first, then the closest tickets by meaning.
      const named = [...q.matchAll(KEY_RE)].map((m) => m[1]);
      const r = await api.sql(searchSql(p.key, q));
      const hits = r.rows.map((x) => String(flatFields(x.fields).key));
      const keys = [...new Set([...named, ...hits])].slice(0, 5);
      a.evidence = [
        ...keys
          .map((k) => tickets.find((t) => t.key === k))
          .filter((t): t is Ticket => !!t)
          .map((t) => ticketEvidence(t, snaps)),
        ...(digest ? [digest] : []),
      ];
      put();
      if (!keys.length) {
        a.text = `${NOT_COVERED}.`;
        a.model = "fixed reply — no ticket matched";
      } else {
        const pr = askPrompt(p, q, a.evidence);
        const res = await api.aiChat({ provider, system: pr.system, message: pr.user, temperature: 0.1, max_tokens: 350 });
        a.text = res.answer.trim();
        a.model = `${res.model} (${res.provider})`;
        a.unsupported = unsupportedNumbers(a.text, a.evidence.map((e) => e.text), q);
      }
    } catch (e) {
      a.error = (e as Error).message;
    }
    put();
    setBusy(false);
  };

  return (
    <div className="card space-y-3">
      <div className="eyebrow">Ask about {p.key}</div>
      <div className="flex flex-wrap gap-1.5">
        {STARTERS[p.key].map((s) => (
          <button
            key={s}
            disabled={busy || !ready}
            onClick={() => void ask(s)}
            className="rounded-full border border-gray-200 dark:border-edge px-2.5 py-1 text-xs text-gray-700 dark:text-ink hover:border-accent hover:text-accent disabled:opacity-40"
          >
            {s}
          </button>
        ))}
      </div>
      <form
        className="flex gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          void ask(input);
        }}
      >
        <input className="input" placeholder={`Ask about ${p.key} tickets, scores and why they moved…`} value={input} onChange={(e) => setInput(e.target.value)} aria-label="Question" />
        <button className="btn" disabled={busy || !input.trim() || !ready}>
          {busy ? "Thinking…" : "Ask"}
        </button>
      </form>
      {answers.map((a, i) => (
        <div key={i} className="rounded-lg border border-gray-200 dark:border-edge p-3 text-sm space-y-2">
          <div className="font-medium text-black dark:text-ink">{a.q}</div>
          {a.text === undefined && !a.error && <p className="text-xs text-gray-500 animate-pulse">Searching tickets and writing an answer…</p>}
          {a.text !== undefined &&
            checkCites(a.text, a.evidence.map((e) => e.text)).map((c, j) => (
              <Sentence key={j} c={c} label={(n) => a.evidence[n - 1]?.key ?? String(n)} title={(n) => a.evidence[n - 1]?.text} />
            ))}
          {a.text !== undefined && (
            <div className="flex flex-wrap gap-3 text-[11px] text-gray-500 dark:text-muted">
              <span>{a.model}</span>
              <span>sources: {a.evidence.map((e) => e.key).join(", ") || "none"}</span>
              {a.text && <CiteStats cs={checkCites(a.text, a.evidence.map((e) => e.text))} />}
              <span className={a.unsupported.length ? "text-bad" : "text-ok"}>
                {a.unsupported.length ? `numbers not in the tickets: ${a.unsupported.join(", ")}` : "every number is in a ticket"}
              </span>
            </div>
          )}
          {a.error && <p className="text-xs text-bad">{a.error}</p>}
        </div>
      ))}
    </div>
  );
}
