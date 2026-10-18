/**
 * Support Desk — ticket triage by nearest neighbours + cited reply drafts.
 *
 * For "Ledgerly", a fictional UK invoicing SaaS:
 *   1. SQL semantic search over resolved tickets → similarity-weighted
 *      vote for category, priority and owning team.
 *   2. SLA due time from a plan × priority table.
 *   3. SQL semantic search over the help centre, optionally filtered to
 *      the predicted category → top articles + deflection confidence.
 *   4. Deterministic urgency / sentiment rules → escalation flags.
 * The reply drafter is grounded only on the ticket, those articles and
 * the closest past resolutions. "Resolve & learn" writes the outcome
 * back to the tickets bucket, so the next similar ticket votes with it.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, ApiError, type SqlRow } from "../api";
import { ErrorBanner, Panel, Spectrum, Spinner, Stat } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ensureSeeded } from "../rag/seed";
import { field, lit, runSql, semanticText, similarity } from "../rag/sql";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { GroundedChat, type ContextDoc } from "../rag/GroundedChat";
import {
  CATEGORIES,
  INBOX,
  KB,
  KB_BUCKET,
  SEED_VERSION,
  TEAMS,
  TICKETS,
  TICKETS_BUCKET,
  type InboxTicket,
  type Plan,
} from "../rag/data/support";

const NEIGHBOURS = 12;

// First-response SLA. Minutes for wall-clock targets, business days
// (Mon–Fri) for the slower tiers.
type Sla = { minutes: number } | { businessDays: number };
const SLA: Record<Plan, Record<1 | 2 | 3 | 4, Sla>> = {
  enterprise: { 1: { minutes: 60 }, 2: { minutes: 120 }, 3: { minutes: 240 }, 4: { minutes: 480 } },
  growth: { 1: { minutes: 120 }, 2: { minutes: 240 }, 3: { minutes: 480 }, 4: { businessDays: 1 } },
  starter: { 1: { minutes: 240 }, 2: { minutes: 480 }, 3: { businessDays: 1 }, 4: { businessDays: 2 } },
};

interface Flag {
  id: string;
  label: string;
  severity: "high" | "medium";
  /** Priority the rule asks for, if it raises one. */
  priority?: 1 | 2;
}

interface Neighbour {
  id: string;
  sim: number;
  subject: string;
  category: string;
  priority: number;
  team: string;
  csat: number;
  minutes: number;
  articleId: string;
  text: string;
  learned: boolean;
}

interface Vote<T> {
  value: T;
  confidence: number;
}

interface Kb {
  id: string;
  title: string;
  category: string;
  sim: number;
  text: string;
}

interface Triage {
  ticketId: string;
  neighbours: Neighbour[];
  category: Vote<string>;
  priority: Vote<number>;
  team: Vote<string>;
  finalPriority: 1 | 2 | 3 | 4;
  flags: Flag[];
  due: Date;
  slaLabel: string;
  kb: Kb[];
  kbFiltered: boolean;
  deflection: number;
  steps: PipelineStep[];
}

type SeedState =
  | { phase: "seeding"; done: number; total: number }
  | { phase: "ready"; written: number }
  | { phase: "error"; message: string };

export function SupportDeskTab() {
  const [seed, setSeed] = useState<SeedState>({ phase: "seeding", done: 0, total: KB.length + TICKETS.length });
  const [inbox, setInbox] = useState<InboxTicket[]>(INBOX);
  const [selectedId, setSelectedId] = useState<string>(INBOX[0].id);
  const [kbByCategory, setKbByCategory] = useState(true);
  const [triage, setTriage] = useState<Triage | null>(null);
  const [triaging, setTriaging] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [composing, setComposing] = useState(false);
  const [learnedIds, setLearnedIds] = useState<string[]>([]);
  const runId = useRef(0);

  const ticket = inbox.find((t) => t.id === selectedId) ?? null;

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const a = await ensureSeeded(KB_BUCKET, KB, SEED_VERSION, (d) =>
          !cancelled && setSeed({ phase: "seeding", done: d, total: KB.length + TICKETS.length })
        );
        const b = await ensureSeeded(TICKETS_BUCKET, TICKETS, SEED_VERSION, (d) =>
          !cancelled && setSeed({ phase: "seeding", done: KB.length + d, total: KB.length + TICKETS.length })
        );
        if (!cancelled) setSeed({ phase: "ready", written: a.written + b.written });
      } catch (e) {
        if (!cancelled) setSeed({ phase: "error", message: errText(e) });
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const runTriage = useCallback(
    async (t: InboxTicket, filterKb: boolean, learned: string[]) => {
      const my = ++runId.current;
      setTriaging(true);
      setErr(null);
      try {
        const result = await triageTicket(t, filterKb, learned);
        if (my === runId.current) setTriage(result);
      } catch (e) {
        if (my === runId.current) {
          setErr(errText(e));
          setTriage(null);
        }
      } finally {
        if (my === runId.current) setTriaging(false);
      }
    },
    []
  );

  useEffect(() => {
    if (seed.phase !== "ready" || !ticket) return;
    void runTriage(ticket, kbByCategory, learnedIds);
    // learnedIds deliberately excluded: the learn handler re-runs triage itself.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [seed.phase, ticket?.id, kbByCategory, runTriage]);

  const getContext = useCallback(async (): Promise<ContextDoc[]> => {
    if (!ticket || !triage) return [];
    return buildContext(ticket, triage);
  }, [ticket, triage]);

  const system = useMemo(() => systemPrompt(triage), [triage]);

  return (
    <div className="space-y-4">
      <SeedBanner seed={seed} />

      <div className="grid gap-4 lg:grid-cols-[18rem_1fr]">
        <Panel
          title="Inbox"
          subtitle={`${inbox.length} open tickets`}
          action={
            <button className="btn-secondary !py-1 !px-2 !text-xs" onClick={() => setComposing((v) => !v)}>
              {composing ? "Close" : "+ New"}
            </button>
          }
        >
          {composing && (
            <ComposeForm
              onSubmit={(t) => {
                setInbox((prev) => [t, ...prev]);
                setSelectedId(t.id);
                setComposing(false);
              }}
            />
          )}
          <ul className="space-y-1.5">
            {inbox.map((t) => (
              <li key={t.id}>
                <button
                  type="button"
                  onClick={() => setSelectedId(t.id)}
                  className={`w-full text-left rounded-md border px-3 py-2 transition-colors ${
                    t.id === selectedId
                      ? "border-accent/60 bg-accent/5"
                      : "border-gray-200 dark:border-edge hover:bg-gray-50 dark:hover:bg-carbon-900"
                  }`}
                >
                  <div className="flex items-center gap-2 text-[11px] font-mono text-gray-400 dark:text-faint">
                    <span>{t.id}</span>
                    <PlanBadge plan={t.plan} />
                    {learnedIds.includes(learnedDocId(t)) && <span className="text-ok">resolved</span>}
                  </div>
                  <div className="text-sm font-medium text-gray-900 dark:text-ink truncate">{t.subject}</div>
                  <div className="text-xs text-gray-500 dark:text-muted truncate">{t.customer}</div>
                </button>
              </li>
            ))}
          </ul>
        </Panel>

        <div className="space-y-4 min-w-0">
          {ticket && (
            <Panel
              title={ticket.subject}
              subtitle={`${ticket.id} · ${ticket.customer} · ${ticket.plan} plan`}
              action={
                <button
                  className="btn-secondary !py-1 !px-2 !text-xs"
                  disabled={seed.phase !== "ready" || triaging}
                  onClick={() => void runTriage(ticket, kbByCategory, learnedIds)}
                >
                  {triaging ? <Spinner label="triaging" /> : "Re-run triage"}
                </button>
              }
            >
              <p className="text-sm whitespace-pre-wrap text-gray-700 dark:text-ink/90">{ticket.body}</p>
              <ErrorBanner err={err} />
              {triage && triage.ticketId === ticket.id && <TriageCard triage={triage} plan={ticket.plan} />}
              {!triage && triaging && <Spinner label="finding similar tickets…" />}
            </Panel>
          )}

          {triage && ticket && triage.ticketId === ticket.id && (
            <>
              <Panel title="Triage pipeline" subtitle="Every step is a real call — SQL, rows and wall time as measured">
                <label className="flex items-center gap-2 text-xs text-gray-600 dark:text-muted">
                  <input
                    type="checkbox"
                    checked={kbByCategory}
                    onChange={(e) => setKbByCategory(e.target.checked)}
                  />
                  Restrict help-centre search to the predicted category
                </label>
                <PipelineTrace steps={triage.steps} />
              </Panel>

              <div className="grid gap-4 xl:grid-cols-2">
                <Panel title="Similar resolved tickets" subtitle="The neighbours that voted on this triage">
                  <ul className="space-y-1.5">
                    {triage.neighbours.map((n) => (
                      <li
                        key={n.id}
                        className={`rounded-md border px-3 py-2 ${
                          n.learned ? "border-ok/60 bg-ok/5" : "border-gray-200 dark:border-edge"
                        }`}
                      >
                        <div className="flex items-center gap-2">
                          <span className="font-mono text-[11px] text-gray-400 dark:text-faint">{n.id}</span>
                          <span className="text-sm truncate flex-1 text-gray-900 dark:text-ink">{n.subject}</span>
                          <Spectrum value={n.sim} label={n.sim.toFixed(3)} />
                        </div>
                        <div className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-[11px] font-mono text-gray-500 dark:text-muted">
                          <span>{n.category}</span>
                          <span>P{n.priority}</span>
                          <span>{n.team}</span>
                          <span>resolved in {fmtMinutes(n.minutes)}</span>
                          <span>CSAT {n.csat ? `${n.csat}/5` : "—"}</span>
                          {n.articleId && <span>{n.articleId}</span>}
                          {n.learned && <span className="text-ok">learned just now</span>}
                        </div>
                      </li>
                    ))}
                  </ul>
                </Panel>

                <Panel
                  title="Help-centre articles"
                  subtitle={`Deflection confidence ${pct(triage.deflection)} — could the customer have self-served?`}
                >
                  {triage.kb.length === 0 ? (
                    <p className="text-sm text-gray-500 dark:text-muted">No articles matched.</p>
                  ) : (
                    <ul className="space-y-2">
                      {triage.kb.map((a) => (
                        <li key={a.id} className="rounded-md border border-gray-200 dark:border-edge px-3 py-2">
                          <div className="flex items-center gap-2">
                            <span className="font-mono text-[11px] text-accent">{a.id}</span>
                            <span className="text-sm font-medium truncate flex-1 text-gray-900 dark:text-ink">{a.title}</span>
                            <Spectrum value={a.sim} label={a.sim.toFixed(3)} />
                          </div>
                          <p className="mt-1 text-xs text-gray-500 dark:text-muted line-clamp-3">
                            {a.text.split("\n").slice(1).join(" ").slice(0, 260)}…
                          </p>
                        </li>
                      ))}
                    </ul>
                  )}
                </Panel>
              </div>
            </>
          )}

          <GroundedChat
            title="Draft reply"
            subtitle="Grounded on the ticket [0], the top help-centre articles and the closest past resolutions"
            system={system}
            getContext={getContext}
            resetKey={ticket?.id}
            disabledReason={
              seed.phase !== "ready"
                ? "Loading the support corpus…"
                : !triage || triage.ticketId !== ticket?.id
                  ? "Waiting for triage…"
                  : undefined
            }
            suggestions={[
              "Draft a reply to the customer",
              "Make it shorter and friendlier",
              "What info do we still need?",
              "Write the internal escalation note",
            ]}
            placeholder="Ask for a draft, a rewrite, or what to check next…"
          />

          {ticket && triage && triage.ticketId === ticket.id && (
            <ResolvePanel
              key={ticket.id}
              ticket={ticket}
              triage={triage}
              onLearned={async (docId) => {
                const next = [...learnedIds.filter((x) => x !== docId), docId];
                setLearnedIds(next);
                await runTriage(ticket, kbByCategory, next);
              }}
            />
          )}

          <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
        </div>
      </div>
    </div>
  );
}

// ---- pipeline ----------------------------------------------------------

async function triageTicket(t: InboxTicket, filterKb: boolean, learned: string[]): Promise<Triage> {
  const steps: PipelineStep[] = [];
  const q = semanticText(`${t.subject}. ${t.body}`);

  // 1. Nearest resolved tickets.
  const nSql = `SELECT * FROM ${TICKETS_BUCKET} WHERE semantic_match(text, ${lit(q)}) LIMIT ${NEIGHBOURS}`;
  const nRun = await runSql(nSql);
  const neighbours: Neighbour[] = nRun.rows.map((r) => toNeighbour(r, learned));
  steps.push({ label: "Similar resolved tickets (semantic SQL)", detail: nSql, rowsOut: neighbours.length, ms: nRun.tookMs });

  const t0 = performance.now();
  const category = vote(neighbours, (n) => n.category) ?? { value: "account", confidence: 0 };
  const priority = vote(neighbours, (n) => n.priority) ?? { value: 3, confidence: 0 };
  const team = vote(neighbours, (n) => n.team) ?? { value: "L1", confidence: 0 };
  steps.push({
    label: "Similarity-weighted vote",
    detail:
      `weight = max(0, 1 − distance)²  ·  category ${category.value} (${pct(category.confidence)})` +
      `  ·  P${priority.value} (${pct(priority.confidence)})  ·  team ${team.value} (${pct(team.confidence)})`,
    rowsIn: neighbours.length,
    rowsOut: 3,
    ms: Math.round(performance.now() - t0),
  });

  // 4 (computed early: rules can raise the priority the SLA uses).
  const t1 = performance.now();
  const flags = detectFlags(`${t.subject}\n${t.body}`);
  const rulePriority = flags.reduce<number>((p, f) => (f.priority && f.priority < p ? f.priority : p), 4);
  const finalPriority = Math.min(priority.value, rulePriority) as 1 | 2 | 3 | 4;

  // 2. SLA.
  const sla = SLA[t.plan][finalPriority];
  const now = new Date();
  const due = "minutes" in sla ? new Date(now.getTime() + sla.minutes * 60_000) : addBusinessDays(now, sla.businessDays);
  const slaLabel = "minutes" in sla ? fmtMinutes(sla.minutes) : `${sla.businessDays} business day${sla.businessDays > 1 ? "s" : ""}`;
  steps.push({
    label: "Rules: urgency, sentiment, SLA",
    detail:
      `${flags.length ? flags.map((f) => f.label).join(", ") : "no flags"}` +
      `  ·  priority = min(vote P${priority.value}, rules P${rulePriority}) = P${finalPriority}` +
      `  ·  SLA ${t.plan} × P${finalPriority} = ${slaLabel}`,
    rowsOut: flags.length,
    ms: Math.round(performance.now() - t1),
  });

  // 3. Help-centre retrieval.
  const kbQ = `SELECT * FROM ${KB_BUCKET} WHERE semantic_match(text, ${lit(q)})`;
  let kbSql = filterKb ? `${kbQ} AND category = ${lit(category.value)} LIMIT 3` : `${kbQ} LIMIT 3`;
  let kbRun = await runSql(kbSql);
  let kbFiltered = filterKb;
  steps.push({ label: filterKb ? "Help-centre search (category-filtered)" : "Help-centre search", detail: kbSql, rowsOut: kbRun.rows.length, ms: kbRun.tookMs });
  if (filterKb && kbRun.rows.length === 0) {
    kbSql = `${kbQ} LIMIT 3`;
    kbRun = await runSql(kbSql);
    kbFiltered = false;
    steps.push({ label: "No article in that category — retried unfiltered", detail: kbSql, rowsOut: kbRun.rows.length, ms: kbRun.tookMs });
  }
  const kb: Kb[] = kbRun.rows.map((r) => ({
    id: String(field(r, "article_id") ?? r.id),
    title: String(field(r, "title") ?? r.id),
    category: String(field(r, "category") ?? ""),
    sim: similarity(r.score),
    text: String(field(r, "text") ?? ""),
  }));
  const deflection = kb[0]?.sim ?? 0;

  return {
    ticketId: t.id,
    neighbours,
    category,
    priority,
    team,
    finalPriority,
    flags,
    due,
    slaLabel,
    kb,
    kbFiltered,
    deflection,
    steps,
  };
}

function toNeighbour(r: SqlRow, learned: string[]): Neighbour {
  const text = String(field(r, "text") ?? "");
  return {
    id: r.id,
    sim: similarity(r.score),
    subject: String(field(r, "subject") ?? text.split("\n")[0]),
    category: String(field(r, "category") ?? "account"),
    priority: Number(field(r, "priority") ?? 3),
    team: String(field(r, "team") ?? "L1"),
    csat: Number(field(r, "csat") ?? 0),
    minutes: Number(field(r, "resolution_minutes") ?? 0),
    articleId: String(field(r, "article_id") ?? ""),
    text,
    learned: learned.includes(r.id),
  };
}

/** Similarity-weighted plurality vote. Squaring the similarity lets the
 *  closest neighbours dominate a long tail of weak matches. */
function vote<T>(ns: Neighbour[], key: (n: Neighbour) => T): Vote<T> | null {
  const tally = new Map<T, number>();
  let total = 0;
  for (const n of ns) {
    const w = Math.max(0, n.sim) ** 2;
    total += w;
    tally.set(key(n), (tally.get(key(n)) ?? 0) + w);
  }
  let best: [T, number] | null = null;
  for (const e of tally) if (!best || e[1] > best[1]) best = e;
  if (!best || total === 0) return null;
  return { value: best[0], confidence: best[1] / total };
}

const RULES: Array<{ re: RegExp; flag: Flag }> = [
  {
    re: /\b(outage|is it down|site down|(is|are) (there an )?outage|not loading|blank page|500 errors?|none of our|nobody can|all (of )?our (invoices|users)|blocked)\b/i,
    flag: { id: "outage", label: "possible outage", severity: "high", priority: 1 },
  },
  {
    re: /\b(solicitor|lawyer|legal action|take this further|sue|court|ICO|ombudsman)\b/i,
    flag: { id: "legal", label: "legal threat", severity: "high", priority: 2 },
  },
  { re: /\bcancel/i, flag: { id: "churn", label: "cancellation / churn risk", severity: "high", priority: 2 } },
  { re: /\b(refund|money back|chargeback)\b/i, flag: { id: "refund", label: "refund request (policy KB-106)", severity: "medium" } },
  { re: /\b(today|asap|urgent|deadline|due in \d+|month end)\b/i, flag: { id: "time", label: "time pressure", severity: "medium" } },
  { re: /!{2,}|\?{2,}/, flag: { id: "punct", label: "frustrated tone", severity: "medium" } },
];

function detectFlags(text: string): Flag[] {
  const flags = RULES.filter((r) => r.re.test(text)).map((r) => r.flag);
  // Shouting: two or more ALL-CAPS words of 3+ letters (ignoring known acronyms).
  const acronyms = new Set(["SSO", "SAML", "VAT", "HMRC", "API", "MTD", "CSV", "ACS", "URL", "ASAP", "GDPR", "ICO", "HTTP", "UK", "USD", "GBP", "CLIENT_OR_AGENT_NOT_AUTHORISED"]);
  const caps = (text.match(/\b[A-Z]{3,}\b/g) ?? []).filter((w) => !acronyms.has(w));
  if (caps.length >= 2) flags.push({ id: "caps", label: "shouting (ALL CAPS)", severity: "medium" });
  return flags;
}

function addBusinessDays(from: Date, days: number): Date {
  const d = new Date(from);
  let left = days;
  while (left > 0) {
    d.setDate(d.getDate() + 1);
    const wd = d.getDay();
    if (wd !== 0 && wd !== 6) left--;
  }
  return d;
}

// ---- grounding ----------------------------------------------------------

function buildContext(t: InboxTicket, tr: Triage): ContextDoc[] {
  const ticketDoc: ContextDoc = {
    id: t.id,
    label: `${t.id} · customer ticket`,
    text:
      `${t.id} · ${t.subject}\nCustomer: ${t.customer} (${t.plan} plan)\nMessage: ${t.body}\n` +
      `Triage: category ${tr.category.value}, priority P${tr.finalPriority}, team ${tr.team.value}, ` +
      `first response due ${tr.due.toLocaleString("en-GB")}` +
      (tr.flags.length ? `, flags: ${tr.flags.map((f) => f.label).join("; ")}` : ""),
  };
  const kb = tr.kb.map<ContextDoc>((a) => ({
    id: a.id,
    label: `${a.id} · ${a.title}`,
    text: compact(a.text),
    bucket: KB_BUCKET,
  }));
  const past = tr.neighbours.slice(0, 3).map<ContextDoc>((n) => ({
    id: n.id,
    label: `${n.id} · ${n.subject}`,
    text: compact(`${n.text}\n(past ticket, CSAT ${n.csat || "n/a"}/5)`),
    bucket: TICKETS_BUCKET,
  }));
  return [ticketDoc, ...kb, ...past];
}

function systemPrompt(tr: Triage | null): string {
  const escalate = tr?.flags.some((f) => f.severity === "high");
  return [
    "You are a Ledgerly customer support agent. Ledgerly is a UK invoicing and accounting app.",
    "Write in warm, plain UK English. Acknowledge the customer's problem first.",
    "Document [0] is the customer's ticket. Other documents are help-centre articles (KB-…) and past resolved tickets (TCK-…).",
    "Only give fix steps that appear in the KB articles. Cite every step with [n].",
    "If you need information the customer has not given, ask for it.",
    "Never promise a refund, credit or discount unless the refund policy article explicitly allows it for this case.",
    "Never invent features, timelines or phone numbers.",
    escalate
      ? "This ticket has escalation flags: say a senior colleague is looking at it now, and do not argue with the customer."
      : "",
    "Sign off as 'The Ledgerly Support Team'.",
  ]
    .filter(Boolean)
    .join("\n");
}

function compact(s: string, max = 1200): string {
  return s.length > max ? s.slice(0, max - 1) + "…" : s;
}

function learnedDocId(t: InboxTicket): string {
  return `TCK-${t.id.replace(/^NEW-/, "")}`;
}

// ---- UI pieces ----------------------------------------------------------

function SeedBanner({ seed }: { seed: SeedState }) {
  if (seed.phase === "error") return <ErrorBanner err={`Could not load the support corpus: ${seed.message}`} />;
  return (
    <div className="flex flex-wrap items-center gap-4 rounded-md border border-gray-200 dark:border-edge px-3 py-2">
      <span className="metachip">
        <span className={`dot ${seed.phase === "ready" ? "dot-ok" : "dot-warn animate-pulseok"}`} />
        {seed.phase === "ready" ? "corpus ready" : `seeding ${seed.done}/${seed.total}`}
      </span>
      <Stat label="help centre" value={`${KB.length} articles`} />
      <Stat label="history" value={`${TICKETS.length} resolved tickets`} />
      <Stat label="buckets" value={`${KB_BUCKET}, ${TICKETS_BUCKET}`} />
      {seed.phase === "ready" && seed.written > 0 && <Stat label="written" value={seed.written} />}
    </div>
  );
}

function TriageCard({ triage, plan }: { triage: Triage; plan: Plan }) {
  const escalate = triage.flags.some((f) => f.severity === "high");
  return (
    <div className="rounded-md border border-gray-200 dark:border-edge p-3 space-y-2">
      <div className="flex items-center gap-2">
        <span className="eyebrow">Triage</span>
        {escalate && (
          <span className="rounded-full border border-bad/40 bg-bad/10 px-2 py-0.5 text-[11px] font-medium text-bad">
            escalate
          </span>
        )}
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <Cell label="category" value={triage.category.value} sub={`${pct(triage.category.confidence)} of vote`} />
        <Cell
          label="priority"
          value={`P${triage.finalPriority}`}
          sub={
            triage.finalPriority < triage.priority.value
              ? `raised by rules (vote P${triage.priority.value})`
              : `${pct(triage.priority.confidence)} of vote`
          }
        />
        <Cell label="team" value={triage.team.value} sub={`${pct(triage.team.confidence)} of vote`} />
        <Cell
          label="first response due"
          value={triage.due.toLocaleString("en-GB", { weekday: "short", hour: "2-digit", minute: "2-digit" })}
          sub={`${plan} · ${triage.slaLabel}`}
        />
      </div>
      {triage.flags.length > 0 && (
        <div className="flex flex-wrap gap-1.5">
          {triage.flags.map((f) => (
            <span
              key={f.id}
              className={`rounded-full border px-2 py-0.5 text-[11px] ${
                f.severity === "high"
                  ? "border-bad/40 text-bad bg-bad/5"
                  : "border-warn/40 text-warn bg-warn/5"
              }`}
            >
              {f.label}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

function Cell({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div>
      <div className="font-mono uppercase tracking-wider text-[10px] text-gray-500 dark:text-muted">{label}</div>
      <div className="text-sm font-semibold text-gray-900 dark:text-ink">{value}</div>
      {sub && <div className="text-[11px] text-gray-500 dark:text-faint">{sub}</div>}
    </div>
  );
}

function PlanBadge({ plan }: { plan: Plan }) {
  const cls =
    plan === "enterprise"
      ? "border-accent/40 text-accent"
      : plan === "growth"
        ? "border-ok/40 text-ok"
        : "border-gray-300 dark:border-edge text-gray-500 dark:text-muted";
  return <span className={`rounded border px-1 text-[10px] uppercase tracking-wide ${cls}`}>{plan}</span>;
}

function ComposeForm({ onSubmit }: { onSubmit: (t: InboxTicket) => void }) {
  const [customer, setCustomer] = useState("");
  const [plan, setPlan] = useState<Plan>("growth");
  const [subject, setSubject] = useState("");
  const [body, setBody] = useState("");
  const ok = customer.trim() && subject.trim() && body.trim();
  return (
    <form
      className="space-y-2 mb-3 rounded-md border border-gray-200 dark:border-edge p-2"
      onSubmit={(e) => {
        e.preventDefault();
        if (!ok) return;
        onSubmit({
          id: `NEW-${Date.now().toString(36).toUpperCase()}`,
          customer: customer.trim(),
          plan,
          subject: subject.trim(),
          body: body.trim(),
        });
      }}
    >
      <input className="input" placeholder="Customer company" value={customer} onChange={(e) => setCustomer(e.target.value)} />
      <select className="input" value={plan} onChange={(e) => setPlan(e.target.value as Plan)}>
        <option value="starter">starter</option>
        <option value="growth">growth</option>
        <option value="enterprise">enterprise</option>
      </select>
      <input className="input" placeholder="Subject" value={subject} onChange={(e) => setSubject(e.target.value)} />
      <textarea
        className="input min-h-[6rem]"
        placeholder="What did the customer write?"
        value={body}
        onChange={(e) => setBody(e.target.value)}
      />
      <button className="btn w-full" disabled={!ok}>
        Add to inbox & triage
      </button>
    </form>
  );
}

function ResolvePanel({
  ticket,
  triage,
  onLearned,
}: {
  ticket: InboxTicket;
  triage: Triage;
  onLearned: (docId: string) => Promise<void>;
}) {
  const [category, setCategory] = useState(triage.category.value);
  const [priority, setPriority] = useState<number>(triage.finalPriority);
  const [team, setTeam] = useState(triage.team.value);
  const [article, setArticle] = useState(triage.kb[0]?.id ?? "");
  const [summary, setSummary] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [done, setDone] = useState<string | null>(null);

  const submit = async () => {
    if (!summary.trim()) return;
    setBusy(true);
    setErr(null);
    const id = learnedDocId(ticket);
    try {
      const text =
        `${id} · ${ticket.subject}\nCustomer (${ticket.customer}, ${ticket.plan}): ${ticket.body}\n` +
        `Resolution: ${summary.trim()}`;
      await api.upsertDoc(TICKETS_BUCKET, id, text, {
        ticket_id: id,
        subject: ticket.subject,
        customer: ticket.customer,
        category,
        priority,
        plan: ticket.plan,
        resolution_minutes: 0,
        csat: 0,
        article_id: article,
        team,
        status: "resolved",
        source: "resolve-and-learn",
      });
      setDone(id);
      await onLearned(id);
    } catch (e) {
      setErr(errText(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Panel
      title="Resolve & learn"
      subtitle={`Writes the outcome to "${TICKETS_BUCKET}" — the next similar ticket's triage votes with it`}
    >
      <div className="grid gap-2 sm:grid-cols-4">
        <LabeledSelect label="category" value={category} onChange={setCategory} options={[...CATEGORIES]} />
        <LabeledSelect
          label="priority"
          value={String(priority)}
          onChange={(v) => setPriority(Number(v))}
          options={["1", "2", "3", "4"]}
          render={(v) => `P${v}`}
        />
        <LabeledSelect label="team" value={team} onChange={setTeam} options={[...TEAMS]} />
        <LabeledSelect
          label="article used"
          value={article}
          onChange={setArticle}
          options={["", ...KB.map((k) => String(k.metadata.article_id))]}
          render={(v) => v || "none"}
        />
      </div>
      <textarea
        className="input min-h-[4.5rem]"
        placeholder="Resolution summary — what actually fixed it?"
        value={summary}
        onChange={(e) => setSummary(e.target.value)}
      />
      <div className="flex items-center gap-3">
        <button className="btn" disabled={busy || !summary.trim()} onClick={() => void submit()}>
          {busy ? <Spinner label="learning" /> : "Resolve & learn"}
        </button>
        {done && !busy && (
          <span className="text-xs text-ok">
            Saved {done}. Triage re-ran — it now appears among the neighbours (highlighted green).
          </span>
        )}
      </div>
      <ErrorBanner err={err} />
    </Panel>
  );
}

function LabeledSelect({
  label,
  value,
  onChange,
  options,
  render = (v) => v,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  options: string[];
  render?: (v: string) => string;
}) {
  return (
    <label className="block">
      <span className="block text-xs font-medium mb-1">{label}</span>
      <select className="input" value={value} onChange={(e) => onChange(e.target.value)}>
        {options.map((o) => (
          <option key={o} value={o}>
            {render(o)}
          </option>
        ))}
      </select>
    </label>
  );
}

// ---- formatting ----------------------------------------------------------

function pct(x: number): string {
  return `${Math.round(x * 100)}%`;
}

function fmtMinutes(m: number): string {
  if (!m) return "—";
  if (m < 60) return `${m}m`;
  if (m < 1440) return `${Math.round(m / 6) / 10}h`;
  return `${Math.round(m / 144) / 10}d`;
}

function errText(e: unknown): string {
  if (e instanceof ApiError) return `${e.code}: ${e.body.slice(0, 300)}`;
  return (e as Error).message;
}
