/**
 * Visa AI Agent — the deterministic half.
 *
 * Small local models (prod runs llama3.2:3b) don't follow JSON or
 * tool-calling instructions reliably, so the agent never lets the model
 * decide what to query. Code extracts the applicant's profile, picks the
 * tools and builds the SQL; the LLM only writes the final answer from
 * the tool outputs, and every number it writes is checked against them.
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
  tool: "visa_finder" | "search_rules" | "case_status";
  why: string;
  sql?: string;
  query?: string;
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

/**
 * Rule search: semantic SQL limited to public documents. Plain hybrid search
 * ranks by relevance only, so a guest asking about retiring in Chiang Mai got
 * another client's case note back; the client filter keeps those rows inside
 * the database. Signed-in clients see their own notes through case_status.
 */
export function rulesSql(pack: Pack, q: string): string {
  return [`SELECT * FROM ${pack.bucket}`, ` WHERE semantic_match(text, ${lit(q)})`, `   AND client_id = 'public'`, " LIMIT 5"].join("\n");
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
      calls.push({ tool: "search_rules", why: "public rules, documents and fees that answer the question", query: q, sql: rulesSql(pack, q) });
    }
  }
  return { calls };
}

export interface Evidence {
  id: string;
  text: string;
  from: ToolCall["tool"];
}

export interface Turn {
  role: "user" | "agent";
  text: string;
}

export function answerPrompt(pack: Pack, question: string, profile: Profile, evidence: Evidence[], history: Turn[]): { system: string; user: string } {
  const system = [
    `You are the visa assistant for ${pack.name}, a Thai visa agency (demo).`,
    "Answer ONLY from the numbered sources. Cite each sentence like [1] or [2,3].",
    'If the sources don\'t answer it, say "I can\'t confirm that from our documents" and suggest booking a consultation.',
    "Quote amounts, ages and days exactly as written in the sources.",
    "When several visas fit, list each with its key requirement in one line, then say which you'd look at first and why.",
    "Always end with: Rules change often — verify with Thai Immigration or a Thai embassy. This is not legal advice.",
    pack.system,
  ].join(" ");
  const facts = [
    profile.age !== undefined && `age ${profile.age}`,
    profile.fundsThb !== undefined && `savings ${profile.fundsThb} THB`,
    profile.incomeThb !== undefined && `income ${profile.incomeThb} THB/month`,
    profile.purposes.length && `purpose: ${profile.purposes.join(", ")}`,
  ].filter(Boolean);
  const recent = history.slice(-4).map((h) => `${h.role === "user" ? "Client" : "Assistant"}: ${h.text.slice(0, 400)}`);
  const user = [
    recent.length ? `Conversation so far:\n${recent.join("\n")}\n` : "",
    facts.length ? `What we know about the client: ${facts.join("; ")}.\n` : "",
    `Sources:\n${evidence.map((e, i) => `[${i + 1}] (${e.id}) ${e.text}`).join("\n")}`,
    `\nClient's question: ${question}`,
  ].join("\n");
  return { system, user };
}
