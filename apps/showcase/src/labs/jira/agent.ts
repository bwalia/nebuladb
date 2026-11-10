/**
 * Jira Triage Agent — Priority Score monitoring, pure logic (no React, no fetch).
 *
 * The split is the same as the Visa agent: code decides what to look up and
 * computes every change (new, up, down, resolved, rank moves); NebulaDB stores
 * the tickets and the snapshot history and answers the lookups; the LLM only
 * writes the short summary at the top of the email, from numbered facts, and
 * every number it writes is checked against those facts.
 */
import { claims, lit, numbersIn, unsupportedNumbers } from "../engine/core.ts";
import { DAILY_RUNS, PROJECTS, RUN_TIME, TICKETS, WEEKLY_RUNS, type Project, type ScoreChange, type Ticket } from "./data.ts";

export const BUCKET = "lab_jira";
export const CORPUS_VERSION = "jira-v3";
/**
 * Vector distance (lower = closer) below which a new ticket is flagged against an existing one.
 * Calibrated on the prod embedder: the planted duplicate pair is 0.16, related tickets 0.20–0.25, the rest 0.25+.
 */
export const DUPLICATE_DISTANCE = 0.18;
export const RELATED_DISTANCE = 0.26;
export const TOP_N = 10;
export const NOT_COVERED = "The Jira data I checked doesn't cover that";

export type Cadence = "weekly" | "daily";

// ---------------------------------------------------------------- snapshots

/** Priority Scores of open tickets at one run. */
export interface Snapshot {
  project: string;
  date: string;
  cadence: Cadence[];
  scores: Record<string, number>;
}

/** The score a ticket had at `at` (ISO), or undefined if it had none yet / null if resolved. */
export function scoreAt(history: ScoreChange[], at: string): number | null | undefined {
  let s: number | null | undefined;
  for (const h of history) if (h.at < at) s = h.score;
  return s;
}

export function takeSnapshot(project: string, date: string, tickets: Ticket[] = TICKETS): Snapshot {
  const at = `${date}T${RUN_TIME}`;
  const scores: Record<string, number> = {};
  for (const t of tickets) {
    if (t.project !== project) continue;
    const s = scoreAt(t.history, at);
    if (typeof s === "number") scores[t.key] = s;
  }
  const cadence: Cadence[] = [];
  if (WEEKLY_RUNS.includes(date)) cadence.push("weekly");
  if (DAILY_RUNS.includes(date)) cadence.push("daily");
  return { project, date, cadence, scores };
}

export const encodeScores = (s: Record<string, number>) =>
  Object.entries(s)
    .map(([k, v]) => `${k}=${v}`)
    .join(";");

export function decodeScores(s: string): Record<string, number> {
  const out: Record<string, number> = {};
  for (const part of s.split(";")) {
    const [k, v] = part.split("=");
    if (k && v !== undefined && Number.isFinite(Number(v))) out[k] = Number(v);
  }
  return out;
}

/** Keys ordered by score (highest first), ties by key; rank is 1-based. */
export function ranking(scores: Record<string, number>): string[] {
  return Object.keys(scores).sort((a, b) => scores[b] - scores[a] || a.localeCompare(b));
}

// ---------------------------------------------------------------- corpus (what the agent keeps in NebulaDB)

export interface CorpusDoc {
  id: string;
  text: string;
  metadata: Record<string, unknown>;
}

export const encodeHistory = (h: ScoreChange[]) => h.map((c) => [c.at, c.score ?? "resolved", c.by, c.note.replace(/[|¦]/g, "/")].join("|")).join("¦");

export function decodeHistory(s: string): ScoreChange[] {
  if (!s) return [];
  return s.split("¦").map((row) => {
    const [at, score, by, note] = row.split("|");
    return { at, score: score === "resolved" ? null : Number(score), by, note };
  });
}

export function ticketDoc(t: Ticket): CorpusDoc {
  const last = t.history[t.history.length - 1];
  const notes = t.history.map((h) => `${h.at.slice(0, 10)} ${h.by}: ${h.note}`).join(" ");
  return {
    id: `jira-${t.key}`,
    text: `${t.key} ${t.type}: ${t.summary}. ${t.description} Component: ${t.component}. Team: ${t.team}. Comments: ${notes}`,
    metadata: {
      kind: "ticket",
      project: t.project,
      key: t.key,
      type: t.type,
      summary: t.summary,
      description: t.description,
      component: t.component,
      team: t.team,
      assignee: t.assignee,
      jira_priority: t.jiraPriority,
      status: t.status,
      score: last.score ?? 0,
      created: t.history[0].at,
      updated: last.at,
      history: encodeHistory(t.history),
    },
  };
}

export function snapshotDoc(s: Snapshot, tickets: Ticket[] = TICKETS): CorpusDoc {
  const order = ranking(s.scores);
  const title = (k: string) => tickets.find((t) => t.key === k)?.summary ?? "";
  return {
    id: `jira-snap-${s.project}-${s.date}`,
    text: `Priority Score snapshot for project ${s.project} taken ${s.date} ${RUN_TIME}, ${order.length} open tickets with a score. ${order
      .slice(0, 12)
      .map((k, i) => `#${i + 1} ${k} score ${s.scores[k]} ${title(k)}`)
      .join("; ")}.`,
    metadata: { kind: "snapshot", project: s.project, date: s.date, cadence: s.cadence, scores: encodeScores(s.scores), open: order.length },
  };
}

/** Everything the agent keeps in NebulaDB: one doc per ticket and one per project per run. */
export function buildCorpus(): CorpusDoc[] {
  const dates = [...new Set([...WEEKLY_RUNS, ...DAILY_RUNS])].sort();
  return [...TICKETS.map(ticketDoc), ...PROJECTS.flatMap((p) => dates.map((d) => snapshotDoc(takeSnapshot(p.key, d))))];
}

// ---------------------------------------------------------------- SQL (one semantic_match each, filters after)

export interface ToolCall {
  tool: "load_tickets" | "load_snapshots" | "find_similar" | "search_tickets";
  why: string;
  sql: string;
}

export function ticketsSql(project: string): string {
  return `SELECT * FROM ${BUCKET}\n WHERE semantic_match(text, 'jira ticket priority score')\n   AND kind = 'ticket'\n   AND project = ${lit(project)}\n LIMIT 100`;
}

export function snapshotsSql(project: string, cadence: Cadence): string {
  return `SELECT * FROM ${BUCKET}\n WHERE semantic_match(text, 'priority score snapshot')\n   AND kind = 'snapshot'\n   AND project = ${lit(project)}\n   AND array_contains(cadence, ${lit(cadence)})\n LIMIT 40`;
}

export function similarSql(project: string, t: { summary: string; description: string }, limit = 5): string {
  return `SELECT * FROM ${BUCKET}\n WHERE semantic_match(text, ${lit(`${t.summary}. ${t.description}`)})\n   AND kind = 'ticket'\n   AND project = ${lit(project)}\n LIMIT ${limit}`;
}

/** LIMIT 25: filters run over LIMIT × 4 nearest docs in the whole bucket, so a small LIMIT can starve one project. */
export function searchSql(project: string, q: string, limit = 25): string {
  return `SELECT * FROM ${BUCKET}\n WHERE semantic_match(text, ${lit(q)})\n   AND kind = 'ticket'\n   AND project = ${lit(project)}\n LIMIT ${limit}`;
}

/** A ticket back from SQL (metadata fields flattened). */
export function ticketFromRow(f: Record<string, unknown>): Ticket {
  return {
    key: String(f.key),
    project: String(f.project),
    type: f.type as Ticket["type"],
    summary: String(f.summary ?? ""),
    description: String(f.description ?? ""),
    component: String(f.component ?? ""),
    team: String(f.team ?? ""),
    assignee: String(f.assignee ?? ""),
    jiraPriority: f.jira_priority as Ticket["jiraPriority"],
    status: f.status as Ticket["status"],
    history: decodeHistory(String(f.history ?? "")),
  };
}

export function snapshotFromRow(f: Record<string, unknown>): Snapshot {
  const c = f.cadence;
  return {
    project: String(f.project),
    date: String(f.date),
    cadence: (Array.isArray(c) ? c : String(c ?? "").split(",")).map(String) as Cadence[],
    scores: decodeScores(String(f.scores ?? "")),
  };
}

// ---------------------------------------------------------------- the digest (pure diff of two snapshots)

export interface Move {
  key: string;
  from?: number;
  to?: number;
  delta: number;
  rankFrom?: number;
  rankTo?: number;
  /** Changelog comments between the two runs — the "why". */
  notes: ScoreChange[];
}

export interface Digest {
  project: string;
  from: string;
  to: string;
  openFrom: number;
  openTo: number;
  added: Move[];
  up: Move[];
  down: Move[];
  resolved: Move[];
  unchanged: number;
  top: { key: string; score: number; rank: number; rankFrom?: number; from?: number }[];
  newLeader?: { key: string; score: number; prevKey?: string; prevScore?: number };
  maxFrom?: number;
  maxTo?: number;
  enteredTop: string[];
  leftTop: string[];
}

const between = (t: Ticket | undefined, from: string, to: string) => (t ? t.history.filter((h) => h.at >= `${from}T${RUN_TIME}` && h.at < `${to}T${RUN_TIME}`) : []);

export function diff(prev: Snapshot, cur: Snapshot, tickets: Ticket[], topN = TOP_N): Digest {
  const byKey = new Map(tickets.map((t) => [t.key, t]));
  const rp = ranking(prev.scores);
  const rc = ranking(cur.scores);
  const rankP = new Map(rp.map((k, i) => [k, i + 1]));
  const rankC = new Map(rc.map((k, i) => [k, i + 1]));
  const d: Digest = {
    project: cur.project,
    from: prev.date,
    to: cur.date,
    openFrom: rp.length,
    openTo: rc.length,
    added: [],
    up: [],
    down: [],
    resolved: [],
    unchanged: 0,
    top: rc.slice(0, topN).map((k) => ({ key: k, score: cur.scores[k], rank: rankC.get(k)!, rankFrom: rankP.get(k), from: prev.scores[k] })),
    maxFrom: rp.length ? prev.scores[rp[0]] : undefined,
    maxTo: rc.length ? cur.scores[rc[0]] : undefined,
    enteredTop: rc.slice(0, topN).filter((k) => !rp.slice(0, topN).includes(k)),
    leftTop: rp.slice(0, topN).filter((k) => !rc.slice(0, topN).includes(k)),
  };
  for (const k of rc) {
    const notes = between(byKey.get(k), prev.date, cur.date);
    const m: Move = { key: k, from: prev.scores[k], to: cur.scores[k], delta: cur.scores[k] - (prev.scores[k] ?? 0), rankFrom: rankP.get(k), rankTo: rankC.get(k), notes };
    if (prev.scores[k] === undefined) d.added.push(m);
    else if (m.delta > 0) d.up.push(m);
    else if (m.delta < 0) d.down.push(m);
    else d.unchanged++;
  }
  for (const k of rp)
    if (cur.scores[k] === undefined)
      d.resolved.push({ key: k, from: prev.scores[k], delta: -prev.scores[k], rankFrom: rankP.get(k), notes: between(byKey.get(k), prev.date, cur.date) });
  d.added.sort((a, b) => b.to! - a.to!);
  d.up.sort((a, b) => b.delta - a.delta);
  d.down.sort((a, b) => a.delta - b.delta);
  if (rc.length && rc[0] !== rp[0]) d.newLeader = { key: rc[0], score: cur.scores[rc[0]], prevKey: rp[0], prevScore: rp.length ? prev.scores[rp[0]] : undefined };
  return d;
}

/** Plain-English position: "#1 of 17", plus a relative band. */
export function position(rank: number, open: number): string {
  const pct = rank / Math.max(open, 1);
  const band = rank === 1 ? "the highest" : pct <= 0.1 ? "top 10%" : pct <= 0.25 ? "top quarter" : pct <= 0.5 ? "top half" : "bottom half";
  return `#${rank} of ${open} (${band})`;
}

// ---------------------------------------------------------------- facts → LLM summary

export interface Fact {
  kind: "new" | "up" | "down" | "resolved" | "leader" | "scale" | "summary";
  text: string;
  keys: string[];
}

const dmy = (iso: string) => new Date(`${iso.slice(0, 10)}T12:00:00Z`).toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
const why = (m: Move) => (m.notes.length ? ` Why (${m.notes[m.notes.length - 1].by}): "${m.notes[m.notes.length - 1].note}"` : "");

/** Numbered facts for the summary — every number the LLM may use is in here. */
export function facts(d: Digest, tickets: Ticket[]): Fact[] {
  const name = (k: string) => tickets.find((t) => t.key === k)?.summary ?? k;
  const out: Fact[] = [];
  out.push({
    kind: "summary",
    text: `Between ${dmy(d.from)} and ${dmy(d.to)}: ${d.added.length} new scored tickets, ${d.up.length} moved up, ${d.down.length} moved down, ${d.resolved.length} resolved, ${d.unchanged} unchanged. ${d.openTo} open tickets have a score now (${d.openFrom} before).`,
    keys: [],
  });
  if (d.newLeader)
    out.push({
      kind: "leader",
      text: `${d.newLeader.key} "${name(d.newLeader.key)}" is now the highest-priority ticket at ${d.newLeader.score}.${d.newLeader.prevKey ? ` ${d.newLeader.prevKey} was the highest before this run${d.top.find((r) => r.key === d.newLeader!.prevKey) ? ` and is now #${d.top.find((r) => r.key === d.newLeader!.prevKey)!.rank}` : ""}.` : ""}`,
      keys: [d.newLeader.key, ...(d.newLeader.prevKey ? [d.newLeader.prevKey] : [])],
    });
  if (d.maxFrom !== undefined && d.maxTo !== undefined && d.maxTo - d.maxFrom >= 20)
    out.push({ kind: "scale", text: `The top score jumped from ${d.maxFrom} to ${d.maxTo}, so the rest of the backlog is relatively lower priority than last time.`, keys: [] });
  for (const m of d.added)
    out.push({ kind: "new", text: `NEW ${m.key} "${name(m.key)}" scored ${m.to}, now ${position(m.rankTo!, d.openTo)}.${why(m)}`, keys: [m.key] });
  for (const m of d.up)
    out.push({ kind: "up", text: `UP ${m.key} "${name(m.key)}" ${m.from} → ${m.to} (+${m.delta}), rank #${m.rankFrom} → #${m.rankTo}.${m.rankTo! > m.rankFrom! ? " Its score rose but it slipped in the ranking because other tickets rose further." : ""}${why(m)}`, keys: [m.key] });
  for (const m of d.down)
    out.push({ kind: "down", text: `DOWN ${m.key} "${name(m.key)}" ${m.from} → ${m.to} (${m.delta}), rank #${m.rankFrom} → #${m.rankTo}.${why(m)}`, keys: [m.key] });
  for (const m of d.resolved) out.push({ kind: "resolved", text: `RESOLVED ${m.key} "${name(m.key)}" (was ${m.from}).${why(m)}`, keys: [m.key] });
  return out;
}

/**
 * The facts the LLM sees: the headline counts, a new leader, and the biggest few moves.
 * The full lists are in the email's generated tables; a small model writes a better
 * paragraph (and misstates less) from a short list.
 */
export function headlineFacts(fs: Fact[]): Fact[] {
  const take = (k: Fact["kind"], n: number) => fs.filter((f) => f.kind === k).slice(0, n);
  return [...take("summary", 1), ...take("leader", 1), ...take("scale", 1), ...take("new", 3), ...take("up", 3), ...take("down", 2), ...take("resolved", 2)];
}

export function summaryPrompt(p: Project, d: Digest, fs: Fact[], cadence: Cadence): { system: string; user: string } {
  const system = [
    `You write the opening of a ${cadence} Priority Score digest email for ${p.manager} at ${p.company}.`,
    "Priority Score is a custom 1-100 field (100 = most urgent); it is NOT Jira's built-in Priority, so never mention Critical/High/Medium/Low.",
    "A higher score means MORE urgent: a rise means the ticket needs more attention, a fall means it needs less.",
    "Use ONLY the numbered facts. End EVERY sentence with the number of the fact it uses in square brackets, like: <KEY> is now the top priority at <score>, replacing <KEY> [2].",
    "Copy ticket keys and numbers exactly; do not calculate new numbers.",
    "Write 3 to 5 short sentences in plain English for a busy manager: lead with what needs attention first, explain relative ranking (what is now on top and why), then what eased off.",
    "No greeting, no sign-off, no bullet points, no markdown.",
  ].join(" ");
  const user = `Project ${p.key} — ${p.name} (${p.company}). Comparing the run on ${d.from} with the run on ${d.to}.\n\nFacts:\n${fs
    .map((f, i) => `[${i + 1}] ${f.text}`)
    .join("\n")}\n\nWrite the summary.`;
  return { system, user };
}

export function rewritePrompt(answer: string, unsupported: string[]): string {
  return `Your draft used ${unsupported.join(", ")}, which are not in any numbered fact. Rewrite it using only numbers copied exactly from the facts.\n\nDraft:\n${answer}`;
}

// ---------------------------------------------------------------- Ask the agent

export interface Evidence {
  id: string;
  key: string;
  text: string;
}

/** One evidence line per ticket: current state + score history + comments. */
export function ticketEvidence(t: Ticket, snaps: Snapshot[]): Evidence {
  const series = snaps
    .filter((s) => s.scores[t.key] !== undefined)
    .map((s) => `${dmy(s.date)} ${s.scores[t.key]}`)
    .join(", ");
  const last = snaps[snaps.length - 1];
  const r = last ? ranking(last.scores) : [];
  const pos = last && last.scores[t.key] !== undefined ? ` Current rank ${position(r.indexOf(t.key) + 1, r.length)}.` : "";
  const cur = t.history[t.history.length - 1];
  const first = t.history[0];
  const origin = t.history.length === 1 ? ` New ticket: first scored ${first.score} on ${first.at.slice(0, 10)}.` : "";
  return {
    id: `jira-${t.key}`,
    key: t.key,
    text: `${t.key} (${t.type}, ${t.status}, ${t.component}, ${t.team}, assignee ${t.assignee}) "${t.summary}". Priority Score now ${cur.score ?? "resolved"}.${origin}${pos} Score at each run: ${series || "none"}. Changes: ${t.history
      .map((h) => `${h.at.slice(0, 10)} → ${h.score ?? "resolved"} by ${h.by}: ${h.note}`)
      .join(" ")}`,
  };
}

/** The latest run's changes as one source, so "what moved this week?" has an answer. */
export function digestEvidence(d: Digest, fs: Fact[], cadence: Cadence): Evidence {
  return { id: "digest", key: "this run", text: `Changes in the ${cadence} run of ${dmy(d.to)} compared with ${dmy(d.from)}: ${headlineFacts(fs).map((f) => f.text).join(" ")}` };
}

export const KEY_RE = /\b([A-Z]{2,6}-\d{1,6})\b/g;

export function askPrompt(p: Project, question: string, ev: Evidence[]): { system: string; user: string } {
  const system = [
    `You are the Jira triage assistant for the ${p.key} project at ${p.company}.`,
    "Priority Score is a custom 1-100 field (100 = most urgent), not Jira's built-in Priority.",
    "A higher score means more urgent.",
    "Answer ONLY from the numbered tickets. End EVERY sentence with the number of the ticket it uses, like [1]. Copy keys, scores and dates exactly.",
    `Only if none of the tickets is relevant to the question, reply exactly: "${NOT_COVERED}." Never use outside knowledge.`,
    "Keep it under 120 words.",
  ].join(" ");
  return { system, user: `Tickets:\n${ev.map((e, i) => `[${i + 1}] ${e.text}`).join("\n")}\n\nManager's question: ${question}` };
}

// ---------------------------------------------------------------- history table (Confluence) + email

export interface HistoryRow {
  key: string;
  summary: string;
  status: string;
  series: (number | null)[];
  latest?: number;
  delta?: number;
}

/** Ticket × run matrix, ordered by the latest score. A ticket appears once it has a score in any run shown. */
export function historyMatrix(snaps: Snapshot[], tickets: Ticket[]): HistoryRow[] {
  const keys = new Set(snaps.flatMap((s) => Object.keys(s.scores)));
  const last = snaps[snaps.length - 1];
  const prev = snaps[snaps.length - 2];
  return [...keys]
    .map((k) => {
      const t = tickets.find((x) => x.key === k);
      const latest = last?.scores[k];
      return {
        key: k,
        summary: t?.summary ?? "",
        status: t?.status ?? "",
        series: snaps.map((s) => s.scores[k] ?? null),
        latest,
        delta: latest !== undefined && prev?.scores[k] !== undefined ? latest - prev.scores[k] : undefined,
      };
    })
    .sort((a, b) => (b.latest ?? -1) - (a.latest ?? -1) || a.key.localeCompare(b.key));
}

const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

/** Background for a score cell: pale to strong red as the score rises. */
export function heat(score: number | null | undefined): string {
  if (score === null || score === undefined) return "#f4f5f7";
  const a = Math.max(0.06, Math.min(1, (score - 15) / 85));
  return `rgba(222, 53, 11, ${a.toFixed(2)})`;
}

/** Confluence storage format (XHTML) for the history page — paste into the page editor's source view or PUT via the REST API. */
export function confluenceStorage(p: Project, cadence: Cadence, snaps: Snapshot[], rows: HistoryRow[]): string {
  const head = snaps.map((s) => `<th>${esc(dmy(s.date))}</th>`).join("");
  const body = rows
    .map(
      (r) =>
        `<tr><td><a href="https://jira.example/browse/${r.key}">${r.key}</a></td><td>${esc(r.summary)}</td>${r.series
          .map((v) => `<td style="background-color:${heat(v)};text-align:center">${v ?? ""}</td>`)
          .join("")}<td>${r.delta === undefined ? (r.latest === undefined ? "resolved" : "new") : r.delta > 0 ? `▲ ${r.delta}` : r.delta < 0 ? `▼ ${-r.delta}` : "="}</td></tr>`
    )
    .join("\n");
  return `<h1>${esc(p.key)} Priority Score history (${cadence})</h1>
<p>Maintained by the Jira Triage Agent. Priority Score (${p.scoreField}) is our 1–100 custom field, not Jira Priority. Updated ${esc(dmy(snaps[snaps.length - 1]?.date ?? ""))} ${RUN_TIME}.</p>
<table><tbody>
<tr><th>Ticket</th><th>Summary</th>${head}<th>Change</th></tr>
${body}
</tbody></table>`;
}

export function csv(snaps: Snapshot[], rows: HistoryRow[]): string {
  const q = (s: string) => `"${s.replace(/"/g, '""')}"`;
  return [["key", "summary", ...snaps.map((s) => s.date)].map(q).join(","), ...rows.map((r) => [q(r.key), q(r.summary), ...r.series.map((v) => (v ?? "").toString())].join(","))].join("\n");
}

export function subject(p: Project, d: Digest, cadence: Cadence): string {
  const lead = d.newLeader ? ` · new #1 ${d.newLeader.key} (${d.newLeader.score})` : "";
  return `[${p.key}] ${cadence === "weekly" ? "Weekly" : "Daily"} Priority Score digest ${dmy(d.to)}: ${d.added.length} new, ${d.up.length} up, ${d.down.length} down, ${d.resolved.length} resolved${lead}`;
}

/** The digest email body as HTML (inline styles so it survives mail clients). */
export function emailHtml(p: Project, d: Digest, tickets: Ticket[], cadence: Cadence, summary: string): string {
  const name = (k: string) => esc(tickets.find((t) => t.key === k)?.summary ?? "");
  const link = (k: string) => `<a href="https://jira.example/browse/${k}" style="color:#0052cc;font-weight:600">${k}</a>`;
  const td = 'style="padding:4px 8px;border-bottom:1px solid #eee"';
  const section = (title: string, ms: Move[], fmt: (m: Move) => string) =>
    ms.length ? `<h3 style="margin:18px 0 6px;font-size:15px">${title} (${ms.length})</h3><table style="border-collapse:collapse;font-size:13px">${ms.map((m) => `<tr><td ${td}>${link(m.key)}</td><td ${td}>${name(m.key)}</td><td ${td}>${fmt(m)}</td></tr>`).join("")}</table>` : "";
  const top = d.top
    .map(
      (r) =>
        `<tr><td ${td}>#${r.rank}</td><td ${td}>${link(r.key)}</td><td ${td}>${name(r.key)}</td><td ${td} align="center"><b>${r.score}</b></td><td ${td}>${r.rankFrom === undefined ? "new" : r.rankFrom > r.rank ? `▲ from #${r.rankFrom}` : r.rankFrom < r.rank ? `▼ from #${r.rankFrom}` : "="}</td></tr>`
    )
    .join("");
  return `<div style="font-family:-apple-system,Segoe UI,Arial,sans-serif;color:#172b4d;max-width:720px">
<p style="font-size:14px;line-height:1.5">${esc(summary.replace(/\s*\[\d+(?:\s*,\s*\d+)*\]/g, ""))}</p>
${section("New tickets with a score", d.added, (m) => `<b>${m.to}</b> · ${position(m.rankTo!, d.openTo)}`)}
${section("Moved up", d.up, (m) => `${m.from} → <b>${m.to}</b> (+${m.delta}) · #${m.rankFrom} → #${m.rankTo}`)}
${section("Moved down", d.down, (m) => `${m.from} → <b>${m.to}</b> (${m.delta}) · #${m.rankFrom} → #${m.rankTo}`)}
${section("Resolved / left the ranking", d.resolved, (m) => `was ${m.from}`)}
<h3 style="margin:18px 0 6px;font-size:15px">Top ${d.top.length} now</h3>
<table style="border-collapse:collapse;font-size:13px">${top}</table>
<p style="font-size:12px;color:#6b778c;margin-top:16px">Priority Score is the custom 1–100 field (${p.scoreField}), not Jira Priority. ${cadence === "weekly" ? "Weekly" : "Daily"} digest, compared with the run on ${dmy(d.from)} ${RUN_TIME}. History: Confluence space ${p.confluenceSpace}.</p>
</div>`;
}

export function eml(p: Project, subj: string, html: string, date: string): string {
  return [
    `From: Jira Triage Agent <triage-agent@${p.managerEmail.split("@")[1]}>`,
    `To: ${p.manager.split(" (")[0]} <${p.managerEmail}>`,
    `Subject: ${subj}`,
    `Date: ${new Date(`${date}T${RUN_TIME}:00Z`).toUTCString()}`,
    "MIME-Version: 1.0",
    "Content-Type: text/html; charset=utf-8",
    "",
    html,
  ].join("\r\n");
}

/** The JQL a real deployment would run. */
export function jql(p: Project): string {
  return `project = ${p.key} AND "Priority Score[Number]" is not EMPTY AND (statusCategory != Done OR resolved >= -14d) ORDER BY "Priority Score[Number]" DESC`;
}

// ---------------------------------------------------------------- citation check

export interface CheckedClaim {
  text: string;
  cites: number[];
  /** The model's citation was missing or pointed at a source without this sentence's keys/numbers; code chose the source. */
  fixed: boolean;
}

/** Ticket keys and multi-digit numbers in a sentence — the things a citation must back up. */
function anchors(text: string): string[] {
  return [...new Set([...[...text.matchAll(KEY_RE)].map((m) => m[1]), ...numbersIn(text).filter((n) => !/^-?\d$/.test(n))])];
}

const has = (source: string, a: string) => (/^[A-Z]/.test(a) ? source.includes(a) : numbersIn(source).includes(a));

/**
 * Check each sentence's citations against the sources: together the cited sources must contain every
 * ticket key and number in the sentence. If they don't, pick the sources that do (fewest first).
 */
export function checkCites(answer: string, sources: string[]): CheckedClaim[] {
  return claims(answer).map((c) => {
    const text = c.text
      .replace(/\[(\d+(?:\s*,\s*\d+)*)\]/g, "")
      .replace(/\s+([.,;:!?])/g, "$1")
      .trim();
    const need = anchors(text);
    const given = c.cites.filter((n) => sources[n - 1] !== undefined);
    if (!need.length) return { text, cites: given, fixed: given.length !== c.cites.length };
    if (given.length && need.every((a) => given.some((n) => has(sources[n - 1], a)))) return { text, cites: given, fixed: false };
    const left = new Set(need);
    const pick: number[] = [];
    while (left.size) {
      let best = -1;
      let bestN = 0;
      sources.forEach((src, i) => {
        const n = [...left].filter((a) => has(src, a)).length;
        if (n > bestN) [best, bestN] = [i, n];
      });
      if (best < 0) break;
      pick.push(best + 1);
      for (const a of [...left]) if (has(sources[best], a)) left.delete(a);
    }
    return { text, cites: pick.sort((a, b) => a - b), fixed: true };
  });
}

// ---------------------------------------------------------------- direction check

export interface DirectionError {
  sentence: string;
  key: string;
  problem: string;
}

const UP = /\b(rose|risen|rises|rise|rising|increased?|increasing|up|jumped|jumps|climbed|climbing|higher|gained)\b/i;
const DOWN = /\b(dropped|drops|drop|decreased?|decreasing|fell|fallen|falls|eased|easing|down|lower|slipped|declined)\b/i;
const STABLE = /\b(stable|unchanged|remained|remains|steady|no change)\b/i;
const TOP = /\b(top priority|top spot|top-priority|the top|highest|number one|#1\b)/i;
const REPLACED = /\b(replac\w*|overtak\w*|displac\w*)\b/i;
/** "its score dropped", "score has increased", "decreased in score" — a claim about the score, not the rank. */
const SCORE_MOVE = /\bscore\s+(?:has\s+|had\s+|was\s+|is\s+)?(?:\w+\s+)?(?:drop|fell|fall|decreas|eas|declin|rose|rise|increas|jump|climb)|(?:drop|fell|decreas|declin|rose|increas)\w*\s+(?:in\s+)?(?:its\s+|their\s+)?score/i;
const RESOLVED = /\b(resolved|closed|fixed|done)\b/i;

/**
 * Check what each clause says about a ticket's direction against the computed diff:
 * up/down/stable/top/replaced/resolved. Numbers can all be right and the direction
 * still wrong ("eased off from 71 to 88"); this catches that.
 */
export function directionErrors(answer: string, d: Digest): DirectionError[] {
  const all = [...d.added, ...d.up, ...d.down, ...d.resolved];
  const mv = (k: string) => all.find((m) => m.key === k);
  const rankOf = (k: string) => {
    const m = mv(k);
    const t = d.top.find((r) => r.key === k);
    return { from: m?.rankFrom ?? t?.rankFrom, to: m?.rankTo ?? t?.rank };
  };
  const errs = new Map<string, DirectionError>();
  for (const c of claims(answer)) {
    const sentence = c.text.replace(/\[(\d+(?:\s*,\s*\d+)*)\]/g, "").trim();
    // Ticket titles ("Waiting room drops fans…") must not count as direction words.
    const bare = sentence.replace(/"[^"]*"|“[^”]*”|'[^']{12,}'/g, "");
    let key = "";
    for (const clause of bare.split(/,\s*(?:and|but|while|which|replacing)?\s*|;\s*|\s+(?:while|but|and|whereas)\s+/i)) {
      const keys = [...clause.matchAll(KEY_RE)].map((m) => m[1]);
      if (keys.length > 1) {
        key = "";
        continue;
      }
      if (keys.length === 1) key = keys[0];
      if (!key) continue;
      const m = mv(key);
      const r = rankOf(key);
      const fact = m
        ? m.to === undefined
          ? `resolved (was ${m.from})`
          : m.from === undefined
            ? `new at ${m.to}, #${m.rankTo}`
            : `${m.from} → ${m.to}, rank #${m.rankFrom} → #${m.rankTo}`
        : `score unchanged${r.to ? `, rank #${r.from} → #${r.to}` : ""}`;
      const bad = (problem: string) => {
        const e = { sentence, key, problem: `${key}: ${problem} — the data says ${fact}` };
        errs.set(e.problem + sentence, e);
      };
      const rankUp = r.from !== undefined && r.to !== undefined && r.to < r.from;
      const rankDown = r.from !== undefined && r.to !== undefined && r.to > r.from;
      const text = clause.trim();
      if (/^\W*(replac|overtak|displac)/i.test(text) || /\bwas (the )?(highest|top)\b/i.test(text)) {
        if (d.newLeader?.prevKey !== key) bad("described as the previous top ticket");
        continue;
      }
      if (STABLE.test(text) && TOP.test(text)) {
        if (!(r.from === 1 && r.to === 1)) bad("described as staying at the top");
        continue;
      }
      if (TOP.test(text) && !/\bnot\b|second|#2|third|#3/i.test(text) && d.top[0]?.key !== key) bad("described as the top ticket");
      else if (REPLACED.test(text) && TOP.test(text) && !d.newLeader) bad("described as replacing the previous top ticket, but the top ticket did not change");
      if (STABLE.test(text)) {
        if (m) bad("described as unchanged");
      } else if (RESOLVED.test(text) && !/\bfix\b/i.test(text)) {
        if (!d.resolved.some((x) => x.key === key)) bad("described as resolved");
      } else if (DOWN.test(text) && !UP.test(text)) {
        // "its score dropped" is about the score; "dropped to #2" may be about rank.
        const ok = (m && (m.delta < 0 || m.to === undefined)) || (!SCORE_MOVE.test(text) && rankDown);
        if (!ok) bad(SCORE_MOVE.test(text) ? "described as its score going down" : "described as going down");
      } else if (UP.test(text) && !DOWN.test(text)) {
        const ok = (m && (m.delta > 0 || m.from === undefined)) || (!SCORE_MOVE.test(text) && rankUp);
        if (!ok) bad(SCORE_MOVE.test(text) ? "described as its score going up" : "described as going up");
      }
    }
  }
  return [...errs.values()];
}

/** Sentences that still use a number found in no fact (after the rewrite) — left out like a wrong direction. */
export function numberErrors(answer: string, factTexts: string[]): DirectionError[] {
  return claims(answer).flatMap((c) => {
    const un = unsupportedNumbers(c.text, factTexts);
    const sentence = c.text.replace(/\[(\d+(?:\s*,\s*\d+)*)\]/g, "").trim();
    return un.length ? [{ sentence, key: "", problem: `${un.join(", ")} is not in any fact` }] : [];
  });
}

export function directionRewritePrompt(answer: string, errs: DirectionError[]): string {
  return `Your draft got these wrong:\n${errs.map((e) => `- ${e.problem}`).join("\n")}\nRewrite it using only the numbered facts, with each ticket's direction exactly as the facts say.\n\nDraft:\n${answer}`;
}

/**
 * The summary minus sentences that still fail the direction check, and minus a sentence
 * right after a removed one that starts with "This"/"It"/"The ticket" (it referred to it).
 */
export function dropSentences(answer: string, errs: DirectionError[]): string {
  const bad = new Set(errs.map((e) => e.sentence));
  const strip = (t: string) => t.replace(/\[(\d+(?:\s*,\s*\d+)*)\]/g, "").trim();
  const out: string[] = [];
  let dropped = false;
  for (const c of claims(answer)) {
    const gone: boolean = bad.has(strip(c.text)) || (dropped && /^(this|it|its|the ticket|that)\b/i.test(c.text));
    if (!gone) out.push(c.text);
    dropped = gone;
  }
  return out.join(" ");
}

/** A plain paragraph from the facts alone — used when too little of the LLM's paragraph survives the checks. */
export function factSummary(fs: Fact[]): string {
  const pick = [fs.find((f) => f.kind === "leader"), fs.find((f) => f.kind === "new" && !fs.some((l) => l.kind === "leader" && l.keys[0] === f.keys[0])), fs.find((f) => f.kind === "up"), fs.find((f) => f.kind === "down"), fs.find((f) => f.kind === "summary")];
  return pick
    .filter((f): f is Fact => !!f)
    .map((f) => `${f.text.replace(/ Why \(.*$/, "")} [${fs.indexOf(f) + 1}]`)
    .join(" ");
}

/** Keep the checked LLM paragraph if at least half of it (and at least one ticket) survived; otherwise use the facts. */
export function finalSummary(answer: string, errs: DirectionError[], fs: Fact[]): { text: string; fallback: boolean } {
  if (!errs.length) return { text: answer.trim(), fallback: false };
  const kept = dropSentences(answer, errs);
  const enough = claims(kept).length * 2 >= claims(answer).length && KEY_RE.test(kept);
  KEY_RE.lastIndex = 0;
  return enough ? { text: kept, fallback: false } : { text: factSummary(fs), fallback: true };
}
