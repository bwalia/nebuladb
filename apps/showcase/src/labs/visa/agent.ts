/**
 * Visa AI Agent — the deterministic half.
 *
 * Small local models (prod runs llama3.2:3b) don't follow JSON or
 * tool-calling instructions reliably, so the agent never lets the model
 * decide what to query. Code extracts the applicant's profile, picks the
 * tools and builds the SQL; the LLM only writes the final answer from
 * the tool outputs, and every number it writes is checked against them.
 *
 * Grounding: the model only ever sees passages from OFFICIAL Thai
 * government websites (bucket `lab_visa_gov`, built by
 * scripts/visa-gov-fetch.mjs) plus, for a signed-in client, their own case
 * notes. The agency's own notes (the Labs pack) drive the Visa Finder
 * shortlist but never reach the model, so when a note is vague or out of
 * date the official text wins.
 *
 * Pure functions only (no React, no network) so `node --test` covers them.
 */
import { lit, matchesSafetyRule } from "./util.ts";
import type { Pack, Persona, SafetyRule } from "../types.ts";

export const PURPOSES = [
  "tourism",
  "transit",
  "remote-work",
  "study",
  "family",
  "work",
  "invest",
  "retire",
  "volunteer",
  "media",
  "medical",
] as const;
export type Purpose = (typeof PURPOSES)[number];

export interface Profile {
  age?: number;
  /** Savings / funds the applicant can show, in THB. */
  fundsThb?: number;
  /** Monthly income in THB, when stated as "per month". */
  incomeThb?: number;
  purposes: Purpose[];
  /** A visa named in the conversation, e.g. "DTV". */
  visa?: string;
}

const PURPOSE_WORDS: [Purpose, RegExp][] = [
  ["retire", /\b(retire|retired|retirement|pension|pensioner|retiree)\b/i],
  ["remote-work", /\b(remote(ly)?|digital nomad|nomad|freelanc\w*|work(ing)? online|work from (thailand|home))\b/i],
  ["work", /\b(job|employ\w*|work permit|hired|employer|teach(er|ing)?|company in thailand|work in thailand)\b/i],
  ["study", /\b(study|student|school|university|course|language school|learn thai|muay thai|cooking class)\b/i],
  ["family", /\b(married|marry|wife|husband|spouse|partner|child(ren)?|kids?|son|daughter|family)\b/i],
  ["invest", /\b(invest\w*|business owner|start a (company|business)|buy (a )?condo)\b/i],
  ["tourism", /\b(holiday|vacation|tourist|tourism|travel(l?ing)?|visit|backpack\w*)\b/i],
  ["medical", /\b(medical|treatment|hospital|surgery)\b/i],
  ["volunteer", /\b(volunteer\w*|ngo|charity)\b/i],
  ["media", /\b(journalist|media|reporter|film(ing)?)\b/i],
  ["transit", /\b(transit|layover|stopover)\b/i],
];

const VISA_NAMES: [string, RegExp][] = [
  ["DTV", /\bdtv\b|destination thailand/i],
  ["LTR", /\bltr\b|long[- ]term resident/i],
  ["Privilege", /\b(privilege|elite)\b/i],
  ["SMART", /\bsmart visa\b/i],
  ["Non-O-A", /\bo-?a\b/i],
  ["Non-O-X", /\bo-?x\b/i],
  ["Non-B", /\bnon[- ]?b\b|business visa/i],
  ["Non-ED", /\bnon[- ]?ed\b|education visa|ed visa/i],
  ["TR", /\btourist visa\b|\btr visa\b/i],
  ["VOA", /\bvoa\b|visa on arrival/i],
  ["exemption", /\bvisa[- ]exempt(ion)?\b|without a visa|no visa/i],
];

/** "900k", "1.2 million", "800,000", "2m" → number. */
function amount(num: string, unit?: string): number {
  const n = Number(num.replace(/,/g, ""));
  const u = (unit ?? "").toLowerCase();
  if (/^(m|mn|million)$/.test(u)) return Math.round(n * 1_000_000);
  if (/^(k|thousand)$/.test(u)) return Math.round(n * 1_000);
  return Math.round(n);
}

/** Pull what we can from free text, merging into the previous profile. */
export function parseProfile(text: string, prev: Profile = { purposes: [] }): Profile {
  const p: Profile = { ...prev, purposes: [...prev.purposes] };
  const t = text.replace(/\s+/g, " ");

  const age =
    t.match(/\b(\d{2})\s*(?:-\s*)?(?:years?[ -]old|yrs?(?: old)?|y\/o|yo)\b/i) ??
    t.match(/\b(?:i'?m|i am|aged|age)\s*(\d{2})\b/i) ??
    t.match(/\bturn(?:ing)?\s*(\d{2})\b/i);
  if (age) {
    const a = Number(age[1]);
    if (a >= 16 && a <= 100) p.age = a;
  }

  // Money: a number with a THB-ish unit, or k/m shorthand next to money words.
  const money = [...t.matchAll(/(?:฿\s*)?(\d[\d,]*(?:\.\d+)?)\s*(k|m|mn|million|thousand)?\s*(thb|baht|฿)?(\s*(?:a|per|\/)\s*month|\s*monthly)?/gi)];
  for (const m of money) {
    const hasUnit = !!(m[2] || m[3]) || /฿/.test(m[0]);
    if (!hasUnit) continue;
    const v = amount(m[1], m[2]);
    if (v < 1000) continue;
    if (m[4]) p.incomeThb = v;
    else p.fundsThb = v;
  }

  // Purposes named in THIS message lead (the latest question wins); earlier ones follow.
  const now = PURPOSE_WORDS.filter(([, re]) => re.test(t)).map(([purpose]) => purpose);
  p.purposes = [...now, ...p.purposes.filter((x) => !now.includes(x))];
  // "work remotely" is remote work, not a local job.
  if (p.purposes.includes("remote-work")) p.purposes = p.purposes.filter((x) => x !== "work" || /work permit|employer|hired/i.test(t));

  for (const [name, re] of VISA_NAMES)
    if (re.test(t)) {
      p.visa = name;
      break;
    }
  return p;
}

export type Intent = "finder" | "rules" | "case";

export function intents(text: string, _profile: Profile, persona?: Persona): Intent[] {
  const t = text.toLowerCase();
  const out: Intent[] = [];
  const caseWords = /\b(my (case|application|file|extension|visa status)|status of my|what'?s next|next step|where are we|lg-\d{4})\b/;
  if (persona?.filter.client_id && caseWords.test(t)) out.push("case");
  const finderWords = /\b(which visa|what visa|best visa|right visa|options?|eligible|qualify|can i (stay|live|retire|work|move|study)|move to thailand|live in thailand|long[- ]term|fits? me)\b/;
  // Run the finder when THIS message asks for options or adds applicant facts —
  // not just because facts from earlier turns are still in the profile.
  const fresh = parseProfile(text);
  if (finderWords.test(t) || fresh.age !== undefined || fresh.fundsThb !== undefined || (fresh.purposes.length > 0 && !fresh.visa))
    out.push("finder");
  out.push("rules");
  return [...new Set(out)];
}

export interface ToolCall {
  tool: "visa_finder" | "search_official" | "case_status";
  why: string;
  sql?: string;
  query?: string;
}

/** NebulaDB bucket holding passages from official Thai government websites. */
export const GOV_BUCKET = "lab_visa_gov";

/** Official-sources search: one semantic_match, optionally narrowed to one visa route. */
export function officialSql(q: string, visa?: string, limit = 6): string {
  return [`SELECT * FROM ${GOV_BUCKET}`, ` WHERE semantic_match(text, ${lit(q)})`, ...(visa ? [`   AND visa = ${lit(visa)}`] : []), ` LIMIT ${limit}`].join("\n");
}

/**
 * The agency's visa names → the `visa` tag on official passages. Routes with
 * no official passage (PR, citizenship) are left out, so they only get the
 * general search.
 */
export const OFFICIAL_TAG: Record<string, string> = {
  exemption: "exemption",
  VOA: "exemption",
  TR: "TR",
  transit: "transit",
  DTV: "DTV",
  "Non-B": "Non-B",
  BOI: "Non-B",
  SMART: "SMART",
  LTR: "LTR",
  Privilege: "Privilege",
  "Non-O retirement": "Non-O",
  "Non-O-A": "Non-O-A",
  "Non-O-X": "Non-O-X",
  "Non-O marriage": "Non-O",
  "Non-O dependant": "Non-O",
  "Non-O volunteer": "Non-O",
  "Non-ED": "Non-ED",
  "Non-M": "Non-M",
  medical: "medical",
};

/**
 * After the Visa Finder runs: fetch the official requirements for the top
 * shortlisted routes (one search per route, 2 passages each), so the answer
 * quotes the government's figures rather than the agency's summary.
 */
export function officialFollowUps(q: string, finderVisas: string[], max = 3): ToolCall[] {
  const tags = [...new Set(finderVisas.map((v) => OFFICIAL_TAG[v]).filter(Boolean))].slice(0, max);
  return tags.map((tag) => ({
    tool: "search_official" as const,
    why: `official requirements for ${tag}`,
    query: q,
    sql: officialSql(`${tag} visa requirements: ${q}`, tag, 2),
  }));
}

/** Visa Finder SQL: visa types whose minimums the applicant meets, for their purpose. */
export function finderSql(pack: Pack, q: string, p: Profile): string {
  const preds = ["kind = 'visa_type'"];
  if (p.age !== undefined) preds.push(`min_age <= ${p.age}`);
  if (p.fundsThb !== undefined) preds.push(`min_funds_thb <= ${p.fundsThb}`);
  const purpose = p.purposes[0];
  if (purpose) preds.push(`array_contains(purpose, ${lit(purpose)})`);
  return [`SELECT * FROM ${pack.bucket}`, ` WHERE semantic_match(text, ${lit(q)})`, ...preds.map((x) => `   AND ${x}`), " LIMIT 200"].join("\n");
}

export function caseSql(pack: Pack, clientId: string): string {
  return [
    `SELECT * FROM ${pack.bucket}`,
    ` WHERE semantic_match(text, 'case status next step')`,
    `   AND kind = 'case_note'`,
    `   AND client_id = ${lit(clientId)}`,
    " LIMIT 200",
  ].join("\n");
}

export interface Plan {
  safety?: SafetyRule;
  calls: ToolCall[];
}

export function plan(pack: Pack, text: string, profile: Profile, persona?: Persona): Plan {
  const rule = pack.safety.find((r) => matchesSafetyRule(text, r.triggers));
  if (rule) return { safety: rule, calls: [] };
  const calls: ToolCall[] = [];
  for (const i of intents(text, profile, persona)) {
    if (i === "case" && persona?.filter.client_id)
      calls.push({ tool: "case_status", why: `open steps for ${persona.filter.client_id}`, sql: caseSql(pack, persona.filter.client_id) });
    if (i === "finder") {
      const facts = [
        profile.age !== undefined && `age ${profile.age}`,
        profile.fundsThb !== undefined && `funds ${profile.fundsThb.toLocaleString("en-GB")} THB`,
        profile.purposes[0] && `purpose ${profile.purposes[0]}`,
      ].filter(Boolean);
      calls.push({
        tool: "visa_finder",
        why: facts.length ? `visa types that fit: ${facts.join(", ")}` : "visa types matching the question",
        sql: finderSql(pack, text, profile),
      });
    }
    if (i === "rules") {
      const q = profile.visa && !new RegExp(profile.visa, "i").test(text) ? `${profile.visa} ${text}` : text;
      const tag = profile.visa ? OFFICIAL_TAG[profile.visa] : undefined;
      calls.push({
        tool: "search_official",
        why: tag ? `official ${tag} passages that answer the question` : "official government passages that answer the question",
        query: q,
        sql: officialSql(q, tag),
      });
    }
  }
  return { calls };
}

export interface Evidence {
  id: string;
  text: string;
  from: ToolCall["tool"];
  /** Official passages: where it came from. */
  url?: string;
  authority?: string;
}

export interface Turn {
  role: "user" | "agent";
  text: string;
}

/** Said when the official passages don't cover the question. Fixed text, so tests and the UI can spot it. */
export const NOT_COVERED = "The official sources I checked don't cover that";

export function answerPrompt(pack: Pack, question: string, profile: Profile, evidence: Evidence[], history: Turn[], fetched = ""): { system: string; user: string } {
  const system = [
    `You are the visa assistant for ${pack.name}, a Thai visa agency (demo).`,
    "Answer ONLY from the numbered passages. They come from official Thai government websites (Ministry of Foreign Affairs e-Visa, Immigration Bureau, Board of Investment, Tourism Authority of Thailand); passages marked CASE are the client's own file.",
    "Never use your own knowledge of Thai visa rules, even if you believe a passage is out of date. Cite each sentence like [1] or [2,3].",
    "Quote amounts, ages, days and fees exactly as written in the passages.",
    `If the passages don't answer part of the question, say "${NOT_COVERED}" and name the official website from the passages that is closest, then answer the parts they do cover.`,
    "When several visas fit, list each with its key official requirement in one line.",
    `Always end with: Source: official Thai government websites${fetched ? ` (checked ${fetched})` : ""}. Rules change — confirm with the Thai embassy or Immigration before you apply. This is not legal advice.`,
  ].join(" ");
  const facts = [
    profile.age !== undefined && `age ${profile.age}`,
    profile.fundsThb !== undefined && `savings ${profile.fundsThb} THB`,
    profile.incomeThb !== undefined && `income ${profile.incomeThb} THB/month`,
    profile.purposes.length && `purpose: ${profile.purposes.join(", ")}`,
  ].filter(Boolean);
  const recent = history.slice(-4).map((h) => `${h.role === "user" ? "Client" : "Assistant"}: ${h.text.slice(0, 400)}`);
  const label = (e: Evidence) => (e.from === "case_status" ? "CASE" : `${e.authority ?? "official"}, ${e.url ?? ""}`);
  const user = [
    recent.length ? `Conversation so far:\n${recent.join("\n")}\n` : "",
    facts.length ? `What we know about the client: ${facts.join("; ")}.\n` : "",
    `Passages:\n${evidence.map((e, i) => `[${i + 1}] (${label(e)}) ${e.text}`).join("\n")}`,
    `\nClient's question: ${question}`,
  ].join("\n");
  return { system, user };
}

/** Follow-up when a reply quotes numbers that are in no passage: ask once for a rewrite. */
export function rewritePrompt(answer: string, unsupported: string[]): string {
  return [
    `Your answer below contains numbers that are not in any passage: ${unsupported.join(", ")}.`,
    "Rewrite it using only numbers written in the passages, citing each sentence. If the passages don't give a figure, say so.",
    `\nYour answer:\n${answer}`,
  ].join("\n");
}

/**
 * Evidence for the model: official passages and the client's own case
 * notes, never the agency's Visa Finder rows. Deduplicated, capped.
 */
export function gatherEvidence(steps: { call: ToolCall; rows: { id: string; text: string; meta?: Record<string, unknown> }[] }[], cap = 10): Evidence[] {
  const out: Evidence[] = [];
  const seen = new Set<string>();
  // Route-specific official lookups first (they carry the requirements), then the general search, then case notes.
  const order = (c: ToolCall) => (c.tool === "search_official" && /^official requirements/.test(c.why) ? 0 : c.tool === "search_official" ? 1 : 2);
  for (const s of [...steps].filter((s) => s.call.tool !== "visa_finder").sort((a, b) => order(a.call) - order(b.call)))
    for (const row of s.rows.slice(0, 6)) {
      if (seen.has(row.id) || !row.text) continue;
      seen.add(row.id);
      out.push({
        id: row.id,
        text: row.text,
        from: s.call.tool,
        ...(s.call.tool === "search_official" ? { url: String(row.meta?.url ?? ""), authority: String(row.meta?.authority ?? "") } : {}),
      });
    }
  return out.slice(0, cap);
}
