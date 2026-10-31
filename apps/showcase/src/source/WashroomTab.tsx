/**
 * App E — Source Washroom Service Desk.
 *
 * Triage (the Support Desk idea): a similarity-weighted vote over past
 * ss_washroom_jobs tickets for service and priority, backed by keyword
 * rules; the site's visit schedule (missed visit = last completed visit
 * older than the contract cycle); the closest guide; a cited reply
 * draft; and "Resolve & learn", which writes the outcome back so the
 * next vote includes it.
 *
 * Quote helper: a site description → units and visit frequency from the
 * demo rules in ss_tender_kb and past jobs, marked as a draft quote.
 */
import { useCallback, useState } from "react";
import { ApiError, api } from "../api";
import { ErrorBanner, Panel, Spinner, Stat } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { GroundedChat, type ContextDoc } from "../rag/GroundedChat";
import { lit, runSql, semanticText, similarity } from "../rag/sql";
import { B, DEMO_TODAY, exhaustiveLimit } from "./data/common";
import { ACCOUNT_DOCS, SITES, siteKey } from "./data/accounts";
import { JOB_DOCS, PRIORITIES } from "./data/washroom";
import { GUIDE_DOCS } from "./data/guides";
import { TENDER_DOCS } from "./data/tender";
import { PRODUCTS_LIMIT } from "./finder";
import { draftQuote, parseSite, priorityByRule, serviceByRule, visitStatus, type Priority, type QuoteDraft, type Service } from "./washroomRules";
import { Chips, DemoNote, SeedChip, gbp, metaOf, num, overlap, productOf, str, ukDate, useSeeds } from "./common";

const JOBS_LIMIT = exhaustiveLimit(JOB_DOCS.length + 40);
const ACCOUNTS_LIMIT = exhaustiveLimit(ACCOUNT_DOCS.length);
const GUIDES_LIMIT = exhaustiveLimit(GUIDE_DOCS.length);
const KB_LIMIT = exhaustiveLimit(TENDER_DOCS.length);
const SERVICES: Service[] = ["sanitary", "nappy", "air-care", "dryer", "refills"];

const SCENARIOS = [
  { label: "Sanitary bin not changed", site: siteKey("KINGSMEAD", "South site"), issue: "Sanitary bin in Block C not changed for 3 weeks, smells." },
  { label: "Dryer tripping the power", site: siteKey("BRIGHTWAY", "Reading Gateway"), issue: "The hand dryer in the ground floor gents keeps tripping the electrics." },
  { label: "Nappy bin overflowing", site: siteKey("CASTLEGATE", "Westbrook Pool"), issue: "Nappy bin in the baby change is overflowing before the visit is due." },
  { label: "Smell despite air care", site: siteKey("HARBOUR", "Seaview House"), issue: "The men's toilets still smell even though the air freshener is working." },
];

const QUOTES = [
  { label: "Office, 3 floors", text: "3 floors, 40 staff, 6 female toilets, 2 baby change rooms" },
  { label: "Secondary school", text: "2 floors, 600 pupils, 14 girls toilets, 2 accessible toilets, 10 basins" },
  { label: "Care home", text: "2 floors, 60 residents, 10 female toilets, 4 accessible toilets, 1 baby change room" },
  { label: "Leisure centre", text: "1 floor, 300 users, 8 female cubicles, 2 accessible toilets, 3 baby change rooms, 12 basins" },
];

interface Neighbour {
  id: string;
  sim: number;
  weight: number;
  service: Service;
  priority: Priority;
  issue: string;
  fix: string;
  site: string;
  learned: boolean;
}

interface Triage {
  service: { value: Service; confidence: number; by: string };
  priority: { value: Priority; vote: Priority; rules: string[] };
  neighbours: Neighbour[];
  schedule: { last: string | null; nextDue: string | null; overdue: boolean; daysSince: number | null; cycle: number; missed: string[] };
  guide: { id: string; title: string; text: string } | null;
  siteName: string;
  accountName: string;
}

function vote<T>(ns: Neighbour[], key: (n: Neighbour) => T): { value: T; confidence: number } | null {
  const tally = new Map<T, number>();
  let total = 0;
  for (const n of ns) {
    total += n.weight;
    tally.set(key(n), (tally.get(key(n)) ?? 0) + n.weight);
  }
  let best: [T, number] | null = null;
  for (const e of tally) if (!best || e[1] > best[1]) best = e;
  return best && total > 0 ? { value: best[0], confidence: best[1] / total } : null;
}

const TRIAGE_SYSTEM =
  "You draft replies for the Source Washroom service team. Document [0] is the customer's new issue. Other documents are the site's visit schedule, past resolved tickets and a guide. " +
  "Using ONLY these documents, write a short, polite reply to the customer in UK English: say sorry where we let them down, say what we will do, and cite facts as [n]. " +
  "If the schedule shows a missed visit, say so honestly. Never promise a date or time that is not in the documents: say a coordinator will confirm the visit. " +
  "You do not book visits. If the documents do not support an answer, say a person will follow up.";

const QUOTE_SYSTEM =
  "You help the Source Washroom team explain a DRAFT washroom service quote. Use ONLY the documents: the computed draft quote, the specifying rules from the tender knowledge base and past jobs. Cite each fact as [n]. " +
  "Say clearly that this is a draft quote and a person confirms numbers and price. Prices are GBP excluding VAT. Use UK English and plain words.";

export function WashroomTab() {
  const seed = useSeeds(["accounts", "washroom", "guides", "tender", "products"]);
  const [mode, setMode] = useState<"triage" | "quote">("triage");

  return (
    <div className="space-y-4">
      <ArchitecturePanel id="ss_washroom" />
      <DemoNote extra={`Demo calendar: today is ${ukDate(DEMO_TODAY)}.`} />
      <div className="flex gap-2">
        <button className={mode === "triage" ? "btn !text-xs" : "btn-secondary !text-xs"} onClick={() => setMode("triage")}>
          Service desk
        </button>
        <button className={mode === "quote" ? "btn !text-xs" : "btn-secondary !text-xs"} onClick={() => setMode("quote")}>
          Quote helper
        </button>
        <span className="ml-auto">
          <SeedChip seed={seed} />
        </span>
      </div>
      {mode === "triage" ? <TriageMode ready={seed.ready} /> : <QuoteMode ready={seed.ready} />}
    </div>
  );
}

function TriageMode({ ready }: { ready: boolean }) {
  const [site, setSite] = useState(SCENARIOS[0].site);
  const [issue, setIssue] = useState(SCENARIOS[0].issue);
  const [scenario, setScenario] = useState(SCENARIOS[0].label);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [triage, setTriage] = useState<Triage | null>(null);
  const [runId, setRunId] = useState(0);
  const [learned, setLearned] = useState<string[]>([]);
  const [fix, setFix] = useState("");
  const [outService, setOutService] = useState<Service>("sanitary");
  const [outPriority, setOutPriority] = useState<Priority>("P2");

  const run = async (learnedIds: string[] = learned) => {
    const text = issue.trim();
    if (!text) return;
    setErr(null);
    setBusy(true);
    const [account, siteName] = site.split("/") as [string, string];
    const trace: PipelineStep[] = [];
    const push = (s: PipelineStep) => {
      trace.push(s);
      setSteps([...trace]);
      return trace.length - 1;
    };
    const done = (i: number, patch: Partial<PipelineStep>) => {
      trace[i] = { ...trace[i], ...patch, status: "ok" };
      setSteps([...trace]);
    };
    try {
      // LIMIT 24 → 96 candidates covers every seeded ticket; it grows by one per learned ticket,
      // which lets the new neighbour in and skips the 30 s SQL result cache.
      const nSql =
        `SELECT * FROM ss_washroom_jobs\n WHERE semantic_match(text, ${lit(semanticText(text))})\n   AND kind = ${lit("ticket")}\n LIMIT ${24 + learnedIds.length}`;
      let i = push({ label: "Similar past tickets (nearest neighbours)", detail: nSql, status: "running" });
      const n = await runSql(nSql);
      const neighbours: Neighbour[] = n.rows.map((r) => {
        const m = metaOf(r);
        const sim = similarity(r.score);
        // Words in common keep the vote sensible even with the mock embedder.
        const words = overlap(text, str(m.issue));
        return {
          id: r.id,
          sim,
          weight: sim * sim * (1 + 3 * words),
          service: str(m.service) as Service,
          priority: (str(m.priority) || "P3") as Priority,
          issue: str(m.issue),
          fix: str(m.fix),
          site: `${str(m.account_name)}, ${str(m.site)}`,
          learned: learnedIds.includes(r.id) || m.learned === true,
        };
      });
      neighbours.sort((a, b) => b.weight - a.weight);
      done(i, { rowsOut: neighbours.length, ms: n.tookMs });

      const aSql =
        `SELECT * FROM ss_accounts\n WHERE semantic_match(text, ${lit("washroom contract")})\n   AND account_id = ${lit(account)}\n   AND site = ${lit(siteName)}\n LIMIT ${ACCOUNTS_LIMIT}`;
      i = push({ label: "The site's washroom contract", detail: aSql, status: "running" });
      const a = await runSql(aSql);
      const am = a.rows[0] ? metaOf(a.rows[0]) : {};
      const cycle = num(am.washroom_cycle_days, 28);
      done(i, { rowsOut: a.rows.length, ms: a.tookMs });

      const vSql =
        `SELECT * FROM ss_washroom_jobs\n WHERE semantic_match(text, ${lit("planned sanitary visit")})\n   AND kind = ${lit("visit")}\n   AND account_id = ${lit(account)}\n   AND site = ${lit(siteName)}\n LIMIT ${JOBS_LIMIT}`;
      i = push({ label: "Visit schedule for this site", detail: vSql, status: "running" });
      const v = await runSql(vSql);
      const visits = v.rows.map((r) => metaOf(r));
      const completed = visits.filter((x) => x.status === "completed").map((x) => str(x.visit_date)).sort();
      const missed = visits.filter((x) => x.status === "missed").map((x) => str(x.visit_date)).sort();
      const last = completed[completed.length - 1] ?? null;
      const vs = visitStatus(last, cycle, DEMO_TODAY);
      done(i, { rowsOut: visits.length, ms: v.tookMs, detail: vSql + `\n→ last completed ${last ?? "none"}, cycle ${cycle} days → ${vs.overdue ? "OVERDUE" : `next due ${vs.nextDue}`}` });

      const gSql =
        `SELECT * FROM ss_guides\n WHERE semantic_match(text, ${lit(semanticText(text))})\n   AND problem IN (${["odour", "washroom"].map(lit).join(", ")})\n LIMIT ${GUIDES_LIMIT}`;
      i = push({ label: "Closest guide", detail: gSql, status: "running" });
      const g = await runSql(gSql);
      const guides = g.rows
        .map((r) => ({ r, m: metaOf(r) }))
        .map(({ r, m }) => ({ id: r.id, title: `${str(m.title)} · ${str(m.heading)}`, text: str(m.text), score: overlap(text, str(m.text)) * 2 + similarity(r.score) }))
        .sort((x, y) => y.score - x.score);
      done(i, { rowsOut: g.rows.length, ms: g.tookMs });

      const top = neighbours.slice(0, 8);
      const sVote = vote(top, (x) => x.service) ?? { value: "sanitary" as Service, confidence: 0 };
      const ruleService = serviceByRule(text);
      const pVote = vote(top, (x) => x.priority) ?? { value: "P3" as Priority, confidence: 0 };
      const rules = priorityByRule(text, vs.overdue);
      const finalP = rules.priority && rules.priority < pVote.value ? rules.priority : pVote.value;
      push({
        label: "Vote + rules (in the browser)",
        detail:
          `weight = similarity² × (1 + 3 × words in common)\n` +
          `service: vote ${sVote.value} (${Math.round(sVote.confidence * 100)}%)${ruleService ? `, keywords say ${ruleService}` : ""}\n` +
          `priority: vote ${pVote.value}${rules.priority ? `, rules ${rules.priority} (${rules.why.join(", ")})` : ""} → ${finalP}`,
        status: "ok",
      });
      const t: Triage = {
        service: ruleService ? { value: ruleService, confidence: 1, by: "keywords" } : { value: sVote.value, confidence: sVote.confidence, by: "vote" },
        priority: { value: finalP, vote: pVote.value, rules: rules.why },
        neighbours: top,
        schedule: { last, nextDue: vs.nextDue, overdue: vs.overdue, daysSince: vs.daysSince, cycle, missed },
        guide: guides[0] ?? null,
        siteName,
        accountName: str(am.account_name) || account,
      };
      setTriage(t);
      setFix(top[0]?.fix ?? "");
      setOutService(t.service.value);
      setOutPriority(t.priority.value);
      setRunId((x) => x + 1);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const learn = async () => {
    if (!triage) return;
    const [account, siteName] = site.split("/") as [string, string];
    const jobId = `L-${Date.now().toString(36)}`;
    const id = `ss-job-${jobId}`;
    try {
      await api.upsertDoc(
        B.washroom,
        id,
        `Ticket ${jobId} (${outPriority}, ${outService}) from ${triage.accountName}, ${siteName} on ${DEMO_TODAY}: ${issue.trim()} Fix: ${fix.trim()} Learned from the service desk.`,
        {
          job_id: jobId,
          kind: "ticket",
          account_id: account,
          account_name: triage.accountName,
          site: siteName,
          service: outService,
          status: "resolved",
          visit_date: DEMO_TODAY,
          issue: issue.trim(),
          priority: outPriority,
          fix: fix.trim(),
          learned: true,
        }
      );
      const next = [...learned, id];
      setLearned(next);
      await run(next);
    } catch (e) {
      setErr((e as Error).message);
    }
  };

  const getContext = useCallback(async (): Promise<ContextDoc[]> => {
    if (!triage) return [];
    const s = triage.schedule;
    return [
      { id: "issue", label: "new issue", text: `NEW ISSUE from ${triage.accountName}, ${triage.siteName}: ${issue.trim()}` },
      {
        id: "schedule",
        label: "visit schedule",
        bucket: B.washroom,
        text:
          `VISIT SCHEDULE for ${triage.siteName}: contract cycle every ${s.cycle} days. Last completed sanitary visit: ${s.last ? ukDate(s.last) : "none on record"}. ` +
          `${s.missed.length ? `Missed visits: ${s.missed.map(ukDate).join(", ")}. ` : ""}` +
          `${s.overdue ? `The site is OVERDUE (${s.daysSince} days since the last visit).` : `Next visit due ${s.nextDue ? ukDate(s.nextDue) : "unknown"}.`} ` +
          `Suggested priority ${triage.priority.value}, service ${triage.service.value}.`,
      },
      ...triage.neighbours.slice(0, 5).map((n) => ({ id: n.id, label: n.id.replace("ss-job-", ""), bucket: B.washroom, text: `PAST TICKET (${n.priority}, ${n.service}) at ${n.site}: ${n.issue} Fix: ${n.fix}` })),
      ...(triage.guide ? [{ id: triage.guide.id, label: "guide", bucket: B.guides, text: triage.guide.text }] : []),
    ];
  }, [triage, issue]);

  return (
    <>
      <Panel title="Source Washroom Service Desk" subtitle="A new issue → suggested service and priority, the site's visit status, and a cited reply draft.">
        <Chips
          items={SCENARIOS}
          active={scenario}
          onPick={(s) => {
            setScenario(s.label);
            setSite(s.site);
            setIssue(s.issue);
          }}
        />
        <div className="flex flex-wrap gap-3 items-end">
          <label className="text-xs">
            <span className="block font-medium mb-1">Site</span>
            <select className="code-input !py-1.5 !text-sm" value={site} onChange={(e) => setSite(e.target.value)}>
              {SITES.map((s) => (
                <option key={siteKey(s.account_id, s.site)} value={siteKey(s.account_id, s.site)}>
                  {s.account_name} · {s.site}
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs flex-1 min-w-[18rem]">
            <span className="block font-medium mb-1">New issue</span>
            <input className="input w-full" value={issue} onChange={(e) => setIssue(e.target.value)} />
          </label>
          <button className="btn" disabled={busy || !ready || !issue.trim()} onClick={() => void run()}>
            {busy ? <Spinner label="triaging…" /> : "Triage"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {triage && (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)]">
          <div className="space-y-4">
            <Panel title="Suggested triage" subtitle={`${triage.accountName}, ${triage.siteName}`}>
              <div className="flex flex-wrap gap-4">
                <Stat label="service" value={`${triage.service.value} (${triage.service.by})`} />
                <Stat label="priority" value={triage.priority.value} />
                <Stat label="vote" value={triage.priority.vote} />
              </div>
              {triage.priority.rules.length > 0 && <p className="text-xs text-warn">Raised by rules: {triage.priority.rules.join(", ")}</p>}
              <div className={`rounded-md border px-3 py-2 text-sm ${triage.schedule.overdue ? "border-bad/50 bg-bad/10" : "border-gray-200 dark:border-edge"}`}>
                {triage.schedule.overdue ? (
                  <b className="text-bad">Missed visit: {triage.schedule.daysSince ?? "?"} days since the last completed visit (cycle {triage.schedule.cycle} days).</b>
                ) : (
                  <>
                    Next visit due <b>{triage.schedule.nextDue ? ukDate(triage.schedule.nextDue) : "unknown"}</b> (every {triage.schedule.cycle} days).
                  </>
                )}
                <div className="text-xs text-gray-500 dark:text-muted mt-0.5">
                  Last completed: {triage.schedule.last ? ukDate(triage.schedule.last) : "none"}
                  {triage.schedule.missed.length > 0 && ` · missed: ${triage.schedule.missed.map(ukDate).join(", ")}`}
                </div>
              </div>
            </Panel>
            <Panel title="Similar past tickets" subtitle="The neighbours that voted">
              <ul className="space-y-1.5">
                {triage.neighbours.map((nb) => (
                  <li key={nb.id} className={`rounded-md border px-3 py-1.5 text-xs ${nb.learned ? "border-ok/60 bg-ok/5" : "border-gray-200 dark:border-edge"}`}>
                    <div className="flex justify-between gap-2">
                      <span className="font-medium">
                        {nb.priority} · {nb.service} · {nb.site}
                        {nb.learned && <span className="text-ok ml-1">learned</span>}
                      </span>
                      <span className="font-mono text-gray-500">{Math.round(nb.sim * 100)}%</span>
                    </div>
                    <div className="text-gray-600 dark:text-muted">{nb.issue}</div>
                    <div className="text-gray-500 dark:text-muted">Fix: {nb.fix}</div>
                  </li>
                ))}
              </ul>
            </Panel>
            <Panel title="Resolve & learn" subtitle="Write the outcome back so the next similar issue votes with it">
              <div className="flex flex-wrap gap-3 items-end">
                <label className="text-xs">
                  <span className="block font-medium mb-1">Service</span>
                  <select className="code-input !py-1.5 !text-sm" value={outService} onChange={(e) => setOutService(e.target.value as Service)}>
                    {SERVICES.map((s) => (
                      <option key={s}>{s}</option>
                    ))}
                  </select>
                </label>
                <label className="text-xs">
                  <span className="block font-medium mb-1">Priority</span>
                  <select className="code-input !py-1.5 !text-sm" value={outPriority} onChange={(e) => setOutPriority(e.target.value as Priority)}>
                    {PRIORITIES.map((p) => (
                      <option key={p}>{p}</option>
                    ))}
                  </select>
                </label>
                <label className="text-xs flex-1 min-w-[14rem]">
                  <span className="block font-medium mb-1">What fixed it</span>
                  <input className="input w-full" value={fix} onChange={(e) => setFix(e.target.value)} />
                </label>
                <button className="btn !text-xs" disabled={busy || !fix.trim()} onClick={() => void learn()}>
                  Resolve & learn
                </button>
              </div>
              {learned.length > 0 && <p className="text-xs text-ok">{learned.length} ticket{learned.length === 1 ? "" : "s"} learned this session.</p>}
            </Panel>
            <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
          </div>
          <GroundedChat
            title="Reply draft"
            subtitle="Cited to the schedule, past tickets and guide. A person sends it and books any visit."
            system={TRIAGE_SYSTEM}
            getContext={getContext}
            resetKey={String(runId)}
            suggestions={["Draft the reply to the customer.", "Why this priority?", "Has this site missed visits?"]}
            placeholder="Ask to draft or adjust the reply…"
          />
        </div>
      )}
    </>
  );
}

function QuoteMode({ ready }: { ready: boolean }) {
  const [text, setText] = useState(QUOTES[0].text);
  const [scenario, setScenario] = useState(QUOTES[0].label);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [quote, setQuote] = useState<(QuoteDraft & { prices: Record<string, number> }) | null>(null);
  const [ctx, setCtx] = useState<ContextDoc[]>([]);
  const [runId, setRunId] = useState(0);

  const run = async () => {
    setErr(null);
    setBusy(true);
    const trace: PipelineStep[] = [];
    const push = (s: PipelineStep) => {
      trace.push(s);
      setSteps([...trace]);
      return trace.length - 1;
    };
    const done = (i: number, patch: Partial<PipelineStep>) => {
      trace[i] = { ...trace[i], ...patch, status: "ok" };
      setSteps([...trace]);
    };
    try {
      const d = parseSite(text);
      const q = draftQuote(d);
      push({ label: "Read the site description (code)", detail: JSON.stringify(d), status: "ok" });
      const kSql =
        `SELECT * FROM ss_tender_kb\n WHERE semantic_match(text, ${lit(semanticText(`washroom units visit frequency ${text}`))})\n   AND topic = ${lit("washroom")}\n LIMIT ${KB_LIMIT}`;
      let i = push({ label: "Specifying rules from the tender KB", detail: kSql, status: "running" });
      const k = await runSql(kSql);
      done(i, { rowsOut: k.rows.length, ms: k.tookMs });
      const jSql =
        `SELECT * FROM ss_washroom_jobs\n WHERE semantic_match(text, ${lit(semanticText(`extra unit cycle ${text}`))})\n   AND kind = ${lit("ticket")}\n   AND service IN (${["sanitary", "nappy"].map(lit).join(", ")})\n LIMIT 6`;
      i = push({ label: "Past jobs that changed units or cycles", detail: jSql, status: "running" });
      const j = await runSql(jSql);
      done(i, { rowsOut: j.rows.length, ms: j.tookMs });
      const pSql = `SELECT * FROM ss_products\n WHERE semantic_match(text, ${lit("washroom service units")})\n   AND sku IN (${q.lines.map((l) => lit(l.sku)).join(", ")})\n LIMIT ${PRODUCTS_LIMIT}`;
      i = push({ label: "Unit prices", detail: pSql, status: "running" });
      const p = await runSql(pSql);
      const prices = Object.fromEntries(p.rows.map((r) => [productOf(r).sku, productOf(r).price_gbp]));
      done(i, { rowsOut: p.rows.length, ms: p.tookMs });

      const quoteDoc: ContextDoc = {
        id: "draft-quote",
        label: "draft quote",
        text:
          `DRAFT QUOTE for: ${text}. Visit every ${q.cycleDays} days (${q.cycleWhy}). ` +
          q.lines.map((l) => `${l.units} × ${l.label} (${l.sku}, ${prices[l.sku] !== undefined ? gbp(prices[l.sku]) : "price on request"} ex VAT each): ${l.why}`).join("; ") +
          ". A person confirms numbers and price.",
      };
      setCtx([
        quoteDoc,
        ...k.rows.map((r) => ({ id: r.id, label: str(metaOf(r).kb_id), bucket: B.tender, text: str(metaOf(r).text) })),
        ...j.rows.map((r) => ({ id: r.id, label: str(metaOf(r).job_id), bucket: B.washroom, text: str(metaOf(r).text) })),
      ]);
      setQuote({ ...q, prices });
      setRunId((x) => x + 1);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const getContext = useCallback(async () => ctx, [ctx]);
  const perVisit = quote ? quote.lines.filter((l) => l.sku !== "HD-570").reduce((n, l) => n + l.units * (quote.prices[l.sku] ?? 0), 0) : 0;

  return (
    <>
      <Panel title="Washroom quote helper" subtitle="Describe the site. Get a draft list of units and a visit frequency.">
        <Chips items={QUOTES} active={scenario} onPick={(s) => { setScenario(s.label); setText(s.text); }} />
        <div className="flex flex-wrap gap-3 items-end">
          <label className="text-xs flex-1 min-w-[18rem]">
            <span className="block font-medium mb-1">Site description</span>
            <input className="input w-full" value={text} onChange={(e) => setText(e.target.value)} />
          </label>
          <button className="btn" disabled={busy || !ready || !text.trim()} onClick={() => void run()}>
            {busy ? <Spinner label="working out…" /> : "Draft quote"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>
      {quote && (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)]">
          <Panel title="Draft quote" subtitle={`Visit every ${quote.cycleDays} days · ${quote.cycleWhy}`}>
            <div className="rounded-md border border-warn/50 bg-warn/10 px-3 py-1.5 text-xs font-semibold text-warn">Draft quote: a person confirms numbers and price.</div>
            <table className="w-full text-sm mt-2">
              <tbody>
                {quote.lines.map((l) => (
                  <tr key={l.sku} className="border-t border-gray-100 dark:border-edge align-top">
                    <td className="py-1.5 pr-2 font-semibold tabular-nums">{l.units}</td>
                    <td className="py-1.5 pr-2">
                      {l.label} <span className="font-mono text-xs text-gray-500">{l.sku}</span>
                      <div className="text-xs text-gray-500 dark:text-muted">{l.why}</div>
                    </td>
                    <td className="py-1.5 text-right text-xs tabular-nums">{quote.prices[l.sku] !== undefined ? `${gbp(quote.prices[l.sku])} each` : "on request"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="text-xs text-gray-600 dark:text-muted">
              Service units per visit: about <b>{gbp(perVisit)}</b> ex VAT, every {quote.cycleDays} days. Dryers are a one-off purchase.
            </p>
            <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag"]} />
          </Panel>
          <GroundedChat
            title="Explain the quote"
            subtitle="Grounded on the draft, the specifying rules and past jobs"
            system={QUOTE_SYSTEM}
            getContext={getContext}
            resetKey={String(runId)}
            suggestions={["Explain this quote for the customer.", "Why this visit frequency?", "What would change with 2 more floors?"]}
            placeholder="Ask about units, frequency or price…"
          />
        </div>
      )}
    </>
  );
}
