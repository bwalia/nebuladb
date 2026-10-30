import { useCallback, useEffect, useMemo, useState } from "react";
import { ApiError } from "../api";
import { ErrorBanner, Panel, Spectrum, Spinner, Stat } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { GroundedChat, type ContextDoc } from "../rag/GroundedChat";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { ensureSeeded, type SeedDoc } from "../rag/seed";
import { field, lit, runSql, semanticText, similarity } from "../rag/sql";
import {
  PLAYBOOK,
  PLAYBOOK_BUCKET,
  PLAYBOOK_BY_TYPE,
  SAMPLE_CONTRACTS,
  SEED_VERSION,
  type Approver,
  type PlaybookMeta,
  type Risk,
} from "../rag/data/contracts";
import { ArchitecturePanel } from "../components/RagArchitecture";

/**
 * Contract Review: split a vendor contract into clauses, classify each
 * against the buyer's legal playbook with semantic SQL, run deterministic
 * red-flag rules, detect missing protections, then chat over the
 * contract + playbook with redline suggestions.
 */

interface Clause {
  n: number;
  no: string;
  heading: string;
  body: string;
}

interface Match {
  type: string;
  title: string;
  sim: number;
}

interface Finding {
  rule: string;
  reason: string;
  risk: Risk;
  approver: Approver;
}

interface ReviewedClause extends Clause {
  matches: Match[];
  /** Primary playbook type, or "unclassified". */
  type: string;
  classifiedBy: "heading" | "semantic" | "none";
  /** Every playbook type this clause addresses (primary + body keywords). */
  covers: string[];
  findings: Finding[];
  risk: Risk | "ok";
}

interface Review {
  hash: string;
  bucket: string;
  clauses: ReviewedClause[];
  missing: PlaybookMeta[];
  steps: PipelineStep[];
}

/** Below this cosine similarity a semantic match isn't trusted on its own. */
const MIN_SIM = 0.25;
const RISK_ORDER: Record<Risk | "ok", number> = { high: 3, medium: 2, low: 1, ok: 0 };

const SYSTEM = `You are a commercial contracts assistant for a UK customer buying SaaS, under the law of England and Wales. You are not giving legal advice.
Compare the vendor's contract clauses against the customer's playbook in the context.
Rules:
- Cite every claim with [n] using the numbered context.
- When proposing a redline, quote the replacement wording in full inside double quotes, based on the playbook preferred or fallback position.
- Say which approver (legal, finance or security) must sign off any red-line issue.
- If the context does not contain the answer, say so. Be concise; use short bullet points.`;

const SUGGESTIONS = [
  "Summarise the top 5 risks",
  "Redline the liability clause to our fallback",
  "What's missing vs our playbook?",
  "Draft an email to the vendor's counsel",
];

/** Heading keywords → playbook type. Headings are the most reliable signal. */
const HEADING_KEYWORDS: Array<[RegExp, string]> = [
  [/sub-?processor/i, "sub_processors"],
  [/breach/i, "breach_notification"],
  [/liabilit/i, "limitation_of_liability"],
  [/indemn/i, "ip_indemnity"],
  [/machine learning|\bmodel|artificial intelligence|\bai\b/i, "ai_model_training"],
  [/data protection|gdpr|processing|transfers?/i, "data_protection"],
  [/security/i, "security_standards"],
  [/service level|availability|\bsla\b/i, "sla_service_credits"],
  [/renew/i, "auto_renewal"],
  [/fee|pric|payment|charges/i, "price_increases"],
  [/governing law|jurisdiction/i, "governing_law"],
  [/assign|change of control/i, "assignment_change_of_control"],
  [/confidential/i, "confidentiality"],
  [/customer data|ownership/i, "customer_data_ownership"],
  [/audit/i, "audit_rights"],
  [/insurance/i, "insurance"],
  [/warrant/i, "warranties"],
  [/force majeure/i, "force_majeure"],
  [/publicity|marketing/i, "publicity"],
  [/return|deletion|exit|transition/i, "exit_data_return"],
  [/solicit/i, "non_solicitation"],
  [/terminat/i, "termination_for_convenience"],
];

/** Body keywords that mean a clause also *covers* a topic (for missing-clause detection). */
const BODY_KEYWORDS: Array<[RegExp, string]> = [
  [/sub-?processor/i, "sub_processors"],
  [/personal data breach|security incident/i, "breach_notification"],
  [/machine learning|train(?:ing)?\b[^.]{0,40}model/i, "ai_model_training"],
  [/\b(?:delete|return)\b[^.]{0,60}\bdata\b/i, "exit_data_return"],
  [/automatic(?:ally)? renew/i, "auto_renewal"],
  [/\bfees?\b/i, "price_increases"],
  [/governed by the laws? of/i, "governing_law"],
  [/data protection|uk gdpr|article 28/i, "data_protection"],
  [/confidential/i, "confidentiality"],
  [/\bowns?\b[^.]{0,40}customer data|customer data[^.]{0,40}\bowns?\b/i, "customer_data_ownership"],
  [/encrypt|iso 27001|soc 2|penetration test/i, "security_standards"],
  [/\baudit\b/i, "audit_rights"],
  [/\bliabilit/i, "limitation_of_liability"],
];

const NUMBER_WORDS: Record<string, number> = {
  one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9,
  ten: 10, eleven: 11, twelve: 12, eighteen: 18, twenty: 20, thirty: 30, "twenty-four": 24,
  "thirty-six": 36, "forty-eight": 48, sixty: 60, ninety: 90,
};

function num(word?: string, digits?: string): number | null {
  if (digits) return Number(digits);
  if (!word) return null;
  if (/^\d+$/.test(word)) return Number(word);
  return NUMBER_WORDS[word.toLowerCase()] ?? null;
}

const sentences = (s: string) => s.split(/(?<=[.;])\s+/);
const NEGATED = /\b(shall not|must not|will not|may not|not use|never|prohibited)\b/i;

/**
 * Deterministic red-flag rules. Each returns a reason when it fires. They
 * catch the patterns a buyer's lawyer looks for first; the LLM handles nuance.
 */
const RULES: Array<{
  id: string;
  risk: Risk;
  approver: Approver;
  test: (text: string) => string | null;
}> = [
  {
    id: "customer_uncapped",
    risk: "high",
    approver: "legal",
    test: (t) =>
      /customer'?s?\s+(?:total\s+)?liability[^.]*\b(unlimited|uncapped)\b/i.test(t)
        ? "Customer liability is unlimited while the vendor's is capped: reverse asymmetry."
        : null,
  },
  {
    id: "uncapped_liability",
    risk: "high",
    approver: "legal",
    test: (t) =>
      !/customer'?s?\s+(?:total\s+)?liability/i.test(t) && /\b(unlimited|uncapped)\s+liabilit/i.test(t)
        ? "Unlimited liability. Confirm which party carries it and for what."
        : null,
  },
  {
    id: "short_cap",
    risk: "high",
    approver: "legal",
    test: (t) => {
      if (!/liabilit/i.test(t)) return null;
      const m = /(\b[a-z-]+|\d+)\s*(?:\((\d+)\)\s*)?months?\s+(?:preceding|prior|before|immediately preceding)/i.exec(t);
      const n = m ? num(m[1], m[2]) : null;
      return n !== null && n < 12
        ? `Liability cap is ${n} months' fees, below the 12-month playbook minimum (preferred 150% of 12 months).`
        : null;
    },
  },
  {
    id: "data_loss_excluded",
    risk: "high",
    approver: "security",
    test: (t) =>
      /not\s+be\s+liable\s+for[^.]*\b(loss|corruption)\s+of\s+(customer\s+)?data/i.test(t)
        ? "Vendor excludes liability for loss or corruption of data, which is a playbook red line."
        : null,
  },
  {
    id: "auto_renew",
    risk: "medium",
    approver: "finance",
    test: (t) => {
      if (!/automatic(?:ally)?\s+renew/i.test(t)) return null;
      const reasons: string[] = [];
      const p = /periods?\s+of\s+(\b[a-z-]+|\d+)\s*(?:\((\d+)\)\s*)?months/i.exec(t);
      const months = p ? num(p[1], p[2]) : null;
      if (months !== null && months > 12) reasons.push(`renews for ${months}-month periods (max 12)`);
      const d = /(\d+)\s*days?[^.]{0,60}(?:before|prior to)\s+the\s+end/i.exec(t);
      if (d && Number(d[1]) > 60) reasons.push(`${d[1]}-day non-renewal notice (max 60)`);
      return reasons.length ? `Auto-renewal: ${reasons.join("; ")}.` : null;
    },
  },
  {
    id: "unilateral_price",
    risk: "high",
    approver: "finance",
    test: (t) =>
      sentences(t).some(
        (s) =>
          /\b(fees?|prices?|charges?|pricing)\b/i.test(s) &&
          /(sole discretion|unilateral(?:ly)?|at any time)/i.test(s) &&
          !NEGATED.test(s)
      )
        ? "Vendor can change fees unilaterally. Playbook requires a fixed initial term, then increases capped at the lower of CPI and 3%."
        : null,
  },
  {
    id: "ai_training",
    risk: "high",
    approver: "security",
    test: (t) =>
      sentences(t).some(
        (s) =>
          /\b(train|fine-?tun|improv|develop)\w*\b[^.]{0,80}\b(models?|machine learning|artificial intelligence|\bai\b)/i.test(s) &&
          !NEGATED.test(s)
      )
        ? "Vendor may use customer data to train or improve its models. Playbook requires prior written opt-in."
        : null,
  },
  {
    id: "perpetual_licence",
    risk: "high",
    approver: "legal",
    test: (t) =>
      /perpetual[^.]{0,80}licen[cs]e[^.]{0,60}customer data/i.test(t) || /customer data[^.]{0,80}perpetual/i.test(t)
        ? "Perpetual licence over customer data that survives termination."
        : null,
  },
  {
    id: "foreign_law",
    risk: "medium",
    approver: "legal",
    test: (t) => {
      const m = /governed by the laws? of\s+(?:the\s+)?([^,.;]+)/i.exec(t);
      if (!m || /england/i.test(m[1])) return null;
      return `Governed by the law of ${m[1].trim()}, not England and Wales.`;
    },
  },
  {
    id: "breach_notice",
    risk: "high",
    approver: "security",
    test: (t) => {
      if (!/breach/i.test(t)) return null;
      const m = /within\s+(\b[a-z-]+|\d+)\s*(?:\((\d+)\)\s*)?(business\s+days?|days?|hours?)/i.exec(t);
      if (!m) return null;
      const n = num(m[1], m[2]);
      if (n === null) return null;
      const hours = /hour/i.test(m[3]) ? n : n * 24;
      if (hours > 72)
        return `Breach notification within ${n} ${m[3]} (${hours}h) exceeds the ICO's 72-hour window. Playbook: 24h.`;
      if (hours > 48) return `Breach notification ${hours}h. Playbook fallback is 48h.`;
      return null;
    },
  },
  {
    id: "subprocessor_no_notice",
    risk: "medium",
    approver: "security",
    test: (t) =>
      /sub-?processors?/i.test(t) &&
      /(at its (?:sole )?discretion|without (?:prior )?notice|from time to time)/i.test(t) &&
      !/\bobject/i.test(t)
        ? "Sub-processors can be added without prior notice or a right to object."
        : null,
  },
  {
    id: "as_is",
    risk: "medium",
    approver: "legal",
    test: (t) =>
      /["“]?as is["”]?/i.test(t) && /disclaim/i.test(t)
        ? "Service is provided 'as is' with warranties disclaimed."
        : null,
  },
  {
    id: "sole_remedy",
    risk: "medium",
    approver: "finance",
    test: (t) =>
      /sole and exclusive remedy/i.test(t)
        ? "Service credits are the sole remedy, with no termination right for chronic failure."
        : null,
  },
  {
    id: "abrupt_deletion",
    risk: "high",
    approver: "security",
    test: (t) => {
      const m = /delete\s+customer\s+data[^.]*?(?:after\s+(\d+)\s*days?)?/i.exec(t);
      if (!m) return null;
      if (/without (?:further )?notice/i.test(t) || (m[1] && Number(m[1]) < 30))
        return "Customer data can be deleted shortly after termination with no export window. Playbook: 30-day export, then certified deletion.";
      return null;
    },
  },
  {
    id: "term_only_confidentiality",
    risk: "medium",
    approver: "legal",
    test: (t) =>
      /confidential/i.test(t) && /during the term/i.test(t) && !/surviv/i.test(t)
        ? "Confidentiality ends with the contract. Playbook: survives 5 years."
        : null,
  },
  {
    id: "one_way_indemnity",
    risk: "high",
    approver: "legal",
    test: (t) => {
      const ss = sentences(t).filter((s) => /indemnif/i.test(s));
      return ss.length > 0 && ss.every((s) => /^\s*(?:\d+(?:\.\d+)*\s+)?customer\b/i.test(s.trim()))
        ? "One-way indemnity: only the customer indemnifies, and there is no vendor IP indemnity."
        : null;
    },
  },
  {
    id: "free_assignment",
    risk: "low",
    approver: "legal",
    test: (t) =>
      /assign[^.]*without[^.]*consent/i.test(t) && !/neither party/i.test(t)
        ? "Vendor may assign without consent and the customer has no termination right."
        : null,
  },
  {
    id: "low_sla",
    risk: "medium",
    approver: "finance",
    test: (t) => {
      const m = /(\d{2}(?:\.\d+)?)\s*%\s*(?:monthly\s+)?availability|availability[^.]{0,40}?(\d{2}(?:\.\d+)?)\s*%/i.exec(t);
      const pct = m ? Number(m[1] ?? m[2]) : null;
      return pct !== null && pct < 99.5 ? `Availability target ${pct}% is below the 99.5% fallback.` : null;
    },
  },
];

/** Split on numbered headings: `1. Title`, `12.3 Text`, `Clause 4 Title`. */
const HEADING = /^\s*(?:clause\s+(\d+(?:\.\d+)*)[.:)]?|(\d+(?:\.\d+)+)|(\d+)[.)])\s+(.*)$/i;

function splitClauses(text: string): Clause[] {
  const out: Clause[] = [];
  let cur: Clause | null = null;
  for (const line of text.split("\n")) {
    const m = HEADING.exec(line);
    if (m) {
      if (cur) out.push(cur);
      const no = m[1] ?? m[2] ?? m[3];
      const rest = m[4].trim();
      // "1. Definitions" is a heading; "12.3 The vendor shall…" is heading-less body.
      const isTitle = rest.length <= 80 && !/[.;:]$/.test(rest) && rest.split(/\s+/).length <= 8;
      cur = {
        n: out.length,
        no,
        heading: isTitle ? rest : rest.split(/\s+/).slice(0, 6).join(" ") + "…",
        body: isTitle ? "" : rest,
      };
    } else if (cur) {
      cur.body += (cur.body ? "\n" : "") + line;
    }
  }
  if (cur) out.push(cur);
  const clauses = out
    .map((c) => ({ ...c, body: c.body.trim() }))
    .filter((c) => c.body || c.heading);
  if (clauses.length > 0) return clauses.map((c, i) => ({ ...c, n: i }));
  // No numbering: treat blank-line paragraphs as clauses.
  return text
    .split(/\n\s*\n/)
    .map((p) => p.trim())
    .filter((p) => p.length > 40)
    .map((p, i) => ({ n: i, no: String(i + 1), heading: p.split(/\s+/).slice(0, 6).join(" ") + "…", body: p }));
}

/** FNV-1a, 8 hex chars — a stable per-contract bucket suffix. */
function shortHash(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, "0");
}

async function mapLimit<T, R>(items: T[], limit: number, fn: (t: T) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      const i = next++;
      out[i] = await fn(items[i]);
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return out;
}

const clauseText = (c: Clause) => `Clause ${c.no} · ${c.heading}\n${c.body}`;
const typeTitle = (t: string) => PLAYBOOK_BY_TYPE[t]?.title ?? "Unclassified";
const errMsg = (e: unknown) => (e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);

export function ContractReviewTab() {
  const [text, setText] = useState(SAMPLE_CONTRACTS[0].text);
  const [seedState, setSeedState] = useState<"seeding" | "ready" | "error">("seeding");
  const [seedMsg, setSeedMsg] = useState<string>("Loading playbook…");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [review, setReview] = useState<Review | null>(null);
  const [liveSteps, setLiveSteps] = useState<PipelineStep[]>([]);
  const [selected, setSelected] = useState<number | null>(null);
  const [riskFilter, setRiskFilter] = useState<"all" | "flagged">("all");

  useEffect(() => {
    let cancelled = false;
    ensureSeeded(PLAYBOOK_BUCKET, PLAYBOOK, SEED_VERSION, (d, t) => {
      if (!cancelled) setSeedMsg(`Seeding playbook ${d}/${t}…`);
    })
      .then((r) => {
        if (cancelled) return;
        setSeedState("ready");
        setSeedMsg(
          r.written
            ? `Playbook seeded: ${r.written} positions in "${PLAYBOOK_BUCKET}".`
            : `Playbook ready: ${r.total} positions in "${PLAYBOOK_BUCKET}".`
        );
      })
      .catch((e) => {
        if (cancelled) return;
        setSeedState("error");
        setSeedMsg(`Playbook seeding failed: ${errMsg(e)}`);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const preview = useMemo(() => splitClauses(text), [text]);
  const hash = useMemo(() => shortHash(text), [text]);
  const stale = review !== null && review.hash !== hash;

  const runReview = async () => {
    setErr(null);
    setBusy(true);
    setSelected(null);
    const steps: PipelineStep[] = [];
    const push = (s: PipelineStep) => {
      steps.push(s);
      setLiveSteps([...steps]);
    };
    const finish = (patch: Partial<PipelineStep>) => {
      Object.assign(steps[steps.length - 1], patch);
      setLiveSteps([...steps]);
    };
    try {
      // 1. Split
      let t0 = performance.now();
      const clauses = splitClauses(text);
      if (clauses.length === 0) throw new Error("No clauses found. Paste a contract with numbered clauses.");
      push({
        label: "Split into clauses",
        detail: "regex on numbered headings: 1. / 12.3 / Clause 4 (blank-line paragraphs as a fallback)",
        rowsIn: text.length,
        rowsOut: clauses.length,
        ms: Math.round(performance.now() - t0),
      });

      // 2. Index into a per-contract bucket for chat retrieval
      const bucket = `legal_contract_${hash}`;
      t0 = performance.now();
      push({ label: `Index clauses → ${bucket}`, status: "running" });
      const docs: SeedDoc[] = clauses.map((c) => ({
        id: `c${c.n}`,
        text: clauseText(c),
        metadata: { clause_no: c.no, heading: c.heading, contract_hash: hash },
      }));
      const idx = await ensureSeeded(bucket, docs, hash);
      finish({
        status: "ok",
        detail: `POST /api/v1/bucket/${bucket}/docs/bulk (${idx.written ? `${idx.written} embedded` : "already indexed, skipped"})`,
        rowsOut: clauses.length,
        ms: Math.round(performance.now() - t0),
      });

      // 3. Classify every clause against the playbook
      t0 = performance.now();
      const exampleSql = (c: Clause) =>
        `SELECT * FROM ${PLAYBOOK_BUCKET} WHERE semantic_match(text, ${lit(semanticText(clauseText(c), 300))}) LIMIT 3`;
      push({ label: `Classify ${clauses.length} clauses vs playbook (semantic SQL)`, status: "running", detail: exampleSql(clauses[0]) });
      const matches = await mapLimit(clauses, 4, async (c) => {
        const r = await runSql(exampleSql(c));
        return r.rows.map<Match>((row) => ({
          type: String(field(row, "clause_type") ?? "unknown"),
          title: String(field(row, "title") ?? row.id),
          sim: similarity(row.score),
        }));
      });
      finish({
        status: "ok",
        rowsIn: clauses.length,
        rowsOut: matches.reduce((n, m) => n + m.length, 0),
        ms: Math.round(performance.now() - t0),
      });

      // 4. Rules
      t0 = performance.now();
      const reviewed: ReviewedClause[] = clauses.map((c, i) => {
        const ms = matches[i];
        const top = ms[0];
        const byHeading = HEADING_KEYWORDS.find(([re]) => re.test(c.heading))?.[1];
        let type = "unclassified";
        let classifiedBy: ReviewedClause["classifiedBy"] = "none";
        if (byHeading) {
          type = byHeading;
          classifiedBy = "heading";
        } else if (top && top.sim >= MIN_SIM) {
          type = top.type;
          classifiedBy = "semantic";
        }
        const covers = new Set<string>();
        if (type !== "unclassified") covers.add(type);
        if (top && top.sim >= MIN_SIM) {
          for (const m of ms) if (m.sim >= top.sim - 0.05) covers.add(m.type);
        }
        for (const [re, t] of BODY_KEYWORDS) if (re.test(c.body)) covers.add(t);
        // A "Termination" clause that only covers breach isn't an exit right.
        if (!/convenience/i.test(c.body)) covers.delete("termination_for_convenience");
        const full = `${c.heading}. ${c.body}`;
        const findings: Finding[] = [];
        for (const r of RULES) {
          const reason = r.test(full);
          if (reason) findings.push({ rule: r.id, reason, risk: r.risk, approver: r.approver });
        }
        const risk = findings.reduce<Risk | "ok">(
          (acc, f) => (RISK_ORDER[f.risk] > RISK_ORDER[acc] ? f.risk : acc),
          "ok"
        );
        return { ...c, matches: ms, type, classifiedBy, covers: [...covers], findings, risk };
      });
      const nFindings = reviewed.reduce((n, c) => n + c.findings.length, 0);
      push({
        label: "Red-flag rules",
        detail: `${RULES.length} deterministic rules (caps, renewal, pricing, AI training, governing law, breach notice, sub-processors…)`,
        rowsIn: reviewed.length,
        rowsOut: nFindings,
        ms: Math.round(performance.now() - t0),
      });

      // 5. Missing protections
      t0 = performance.now();
      const covered = new Set(reviewed.flatMap((c) => c.covers));
      const missing = Object.values(PLAYBOOK_BY_TYPE).filter(
        (p) => p.risk_if_missing !== "low" && !covered.has(p.clause_type)
      );
      push({
        label: "Missing-clause check",
        detail: "playbook positions with risk_if_missing ∈ {high, medium} that no clause addresses",
        rowsIn: Object.keys(PLAYBOOK_BY_TYPE).length,
        rowsOut: missing.length,
        ms: Math.round(performance.now() - t0),
      });

      setReview({ hash, bucket, clauses: reviewed, missing, steps: [...steps] });
      const firstFlag = reviewed.findIndex((c) => c.risk === "high");
      setSelected(firstFlag >= 0 ? firstFlag : 0);
    } catch (e) {
      if (steps.length && steps[steps.length - 1].status === "running") finish({ status: "error" });
      setErr(errMsg(e));
    } finally {
      setBusy(false);
    }
  };

  const summary = useMemo(() => {
    if (!review) return null;
    const counts = { high: 0, medium: 0, low: 0, ok: 0 };
    const approvers: Record<Approver, number> = { legal: 0, finance: 0, security: 0 };
    for (const c of review.clauses) {
      counts[c.risk]++;
      for (const a of new Set(c.findings.filter((f) => f.risk !== "low").map((f) => f.approver))) approvers[a]++;
    }
    for (const m of review.missing) approvers[m.approver]++;
    return { counts, approvers };
  }, [review]);

  const findingsDoc = useCallback((): ContextDoc | null => {
    if (!review) return null;
    const lines: string[] = ["Automated review findings for this contract (rules engine output):"];
    for (const c of review.clauses) {
      for (const f of c.findings) {
        lines.push(`- Clause ${c.no} (${c.heading}): ${f.risk.toUpperCase()} risk, approver ${f.approver}: ${f.reason}`);
      }
    }
    for (const m of review.missing) {
      lines.push(`- MISSING: no clause covers "${m.title}" (risk if missing: ${m.risk_if_missing}, approver ${m.approver}). Preferred: ${m.preferred}`);
    }
    if (lines.length === 1) lines.push("- No red flags found.");
    return { id: "findings", label: "Review findings", text: lines.join("\n") };
  }, [review]);

  const getContext = useCallback(
    async (question: string): Promise<ContextDoc[]> => {
      if (!review) return [];
      const q = lit(semanticText(question));
      const [c, p] = await Promise.all([
        runSql(`SELECT * FROM ${review.bucket} WHERE semantic_match(text, ${q}) LIMIT 4`),
        runSql(`SELECT * FROM ${PLAYBOOK_BUCKET} WHERE semantic_match(text, ${q}) LIMIT 3`),
      ]);
      const docs: ContextDoc[] = [];
      const seen = new Set<string>();
      const add = (d: ContextDoc) => {
        if (seen.has(d.id)) return;
        seen.add(d.id);
        docs.push(d);
      };
      if (selected !== null && review.clauses[selected]) {
        const sc = review.clauses[selected];
        add({ id: `c${sc.n}`, label: `Clause ${sc.no} · ${sc.heading}`, text: clauseText(sc), bucket: review.bucket });
        const pb = PLAYBOOK.find((d) => d.metadata.clause_type === sc.type);
        if (pb) add({ id: pb.id, label: `Playbook · ${pb.metadata.title}`, text: pb.text, bucket: PLAYBOOK_BUCKET });
      }
      const f = findingsDoc();
      if (f) add(f);
      for (const row of c.rows) {
        add({
          id: row.id,
          label: `Clause ${field(row, "clause_no") ?? "?"} · ${field(row, "heading") ?? ""}`,
          text: String(field(row, "text") ?? ""),
          bucket: review.bucket,
        });
      }
      for (const row of p.rows) {
        add({
          id: row.id,
          label: `Playbook · ${field(row, "title") ?? row.id}`,
          text: String(field(row, "text") ?? ""),
          bucket: PLAYBOOK_BUCKET,
        });
      }
      return docs;
    },
    [review, selected, findingsDoc]
  );

  const visible = review
    ? review.clauses.filter((c) => riskFilter === "all" || c.risk === "high" || c.risk === "medium")
    : [];
  const sel = review && selected !== null ? review.clauses[selected] : null;

  return (
    <div className="space-y-4">
      <ArchitecturePanel id="legal" />
      <Panel
        title="Contract"
        subtitle="Paste a vendor contract or pick a sample. Each clause is reviewed against the buyer's playbook (England & Wales)."
        action={
          <span className="metachip">
            <span className={`dot ${seedState === "ready" ? "dot-ok" : seedState === "error" ? "dot-bad" : "dot-warn animate-pulseok"}`} />
            {seedMsg}
          </span>
        }
      >
        <div className="flex flex-wrap gap-2">
          {SAMPLE_CONTRACTS.map((s) => (
            <button
              key={s.id}
              type="button"
              className={`btn-secondary !text-xs ${text === s.text ? "!border-accent !text-accent" : ""}`}
              onClick={() => setText(s.text)}
            >
              {s.name}
            </button>
          ))}
          <button type="button" className="btn-secondary !text-xs" onClick={() => setText("")}>
            Blank
          </button>
        </div>
        <textarea
          className="input font-mono !text-xs h-64 w-full"
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="1. Definitions&#10;…&#10;10. Limitation of Liability&#10;…"
          aria-label="Contract text"
        />
        <div className="flex flex-wrap items-center gap-3">
          <button
            className="btn"
            disabled={busy || seedState !== "ready" || preview.length === 0}
            onClick={() => void runReview()}
          >
            {busy ? <Spinner label="reviewing…" /> : review && !stale ? "Re-run review" : "Review contract"}
          </button>
          <Stat label="chars" value={text.length.toLocaleString()} />
          <Stat label="clauses detected" value={preview.length} />
          <Stat label="contract id" value={hash} />
          {stale && <span className="text-xs text-warn">Text changed since the last review. Re-run it.</span>}
        </div>
        <ErrorBanner err={err} />
      </Panel>

      {(busy || review) && (
        <Panel title="Pipeline" subtitle="Each step is a real call or computation, with measured timings">
          <PipelineTrace steps={busy ? liveSteps : review?.steps ?? liveSteps} />
        </Panel>
      )}

      {review && summary && (
        <Panel title="Risk summary" subtitle={`${review.clauses.length} clauses reviewed · ${review.missing.length} playbook protections missing`}>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            <RiskTile label="High" n={summary.counts.high} cls="text-bad" />
            <RiskTile label="Medium" n={summary.counts.medium} cls="text-warn" />
            <RiskTile label="Low" n={summary.counts.low} cls="text-gray-700 dark:text-ink" />
            <RiskTile label="No flags" n={summary.counts.ok} cls="text-ok" />
          </div>
          <div className="flex flex-wrap items-center gap-2 text-xs">
            <span className="eyebrow">sign-off needed</span>
            {(Object.keys(summary.approvers) as Approver[]).map((a) => (
              <span key={a} className="metachip">
                <span className={`dot ${summary.approvers[a] ? "dot-warn" : "dot-ok"}`} />
                {a} · {summary.approvers[a]}
              </span>
            ))}
          </div>
          {review.missing.length > 0 && (
            <div>
              <div className="eyebrow mb-1.5">Missing protections</div>
              <ul className="space-y-1">
                {review.missing.map((m) => (
                  <li key={m.clause_type} className="text-sm flex flex-wrap items-baseline gap-2">
                    <RiskBadge risk={m.risk_if_missing} />
                    <span className="font-medium">{m.title}</span>
                    <span className="text-xs text-gray-500 dark:text-muted">
                      approver {m.approver}. Preferred: {m.preferred}
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </Panel>
      )}

      {review && (
        <Panel
          title="Clauses"
          subtitle="Matched playbook position (semantic similarity), risk, and the rule that fired"
          action={
            <div className="flex gap-1">
              {(["all", "flagged"] as const).map((f) => (
                <button
                  key={f}
                  className={`btn-secondary !py-1 !px-2 !text-xs ${riskFilter === f ? "!border-accent !text-accent" : ""}`}
                  onClick={() => setRiskFilter(f)}
                >
                  {f === "all" ? "All" : "High + medium"}
                </button>
              ))}
            </div>
          }
        >
          <div className="grid lg:grid-cols-2 gap-4">
            <ul className="divide-y divide-gray-200 dark:divide-edge max-h-[36rem] overflow-y-auto rounded-md border border-gray-200 dark:border-edge">
              {visible.length === 0 && (
                <li className="p-3 text-sm text-gray-500 dark:text-muted">No flagged clauses.</li>
              )}
              {visible.map((c) => (
                <li key={c.n}>
                  <button
                    type="button"
                    onClick={() => setSelected(c.n)}
                    className={`w-full text-left px-3 py-2.5 hover:bg-gray-50 dark:hover:bg-carbon-900 ${
                      selected === c.n ? "bg-gray-100 dark:bg-carbon-800" : ""
                    }`}
                  >
                    <div className="flex items-center gap-2">
                      <span className="font-mono text-xs text-gray-400 dark:text-faint w-8">{c.no}</span>
                      <span className="text-sm font-medium truncate flex-1">{c.heading}</span>
                      <RiskBadge risk={c.risk} />
                    </div>
                    <div className="mt-1 flex items-center gap-2 pl-10">
                      <span className="text-xs text-gray-500 dark:text-muted truncate flex-1">
                        {typeTitle(c.type)}
                        <span className="text-gray-400 dark:text-faint"> · by {c.classifiedBy}</span>
                      </span>
                      {c.matches[0] && (
                        <Spectrum value={c.matches[0].sim} label={c.matches[0].sim.toFixed(3)} />
                      )}
                    </div>
                    {c.findings.length > 0 && (
                      <p className="mt-1 pl-10 text-xs text-gray-600 dark:text-muted line-clamp-2">
                        {c.findings[0].reason}
                        {c.findings.length > 1 && ` (+${c.findings.length - 1} more)`}
                      </p>
                    )}
                  </button>
                </li>
              ))}
            </ul>
            {sel ? <ClauseDetail clause={sel} /> : <p className="text-sm text-gray-500 dark:text-muted">Select a clause.</p>}
          </div>
        </Panel>
      )}

      <GroundedChat
        title="Ask about this contract"
        subtitle={
          review
            ? `Grounded on "${review.bucket}" + "${PLAYBOOK_BUCKET}" + the review findings${sel ? ` · focus: clause ${sel.no}` : ""}`
            : "Run a review first"
        }
        system={SYSTEM}
        getContext={getContext}
        suggestions={SUGGESTIONS}
        placeholder="e.g. Is the liability cap acceptable? Propose replacement wording."
        disabledReason={review ? undefined : "Review a contract to start chatting"}
        resetKey={review?.hash}
        onCite={(d) => {
          if (!review || !d.id.startsWith("c")) return;
          const n = Number(d.id.slice(1));
          if (Number.isInteger(n) && review.clauses[n]) setSelected(n);
        }}
      />

      <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
    </div>
  );
}

function ClauseDetail({ clause }: { clause: ReviewedClause }) {
  const pb = PLAYBOOK_BY_TYPE[clause.type];
  return (
    <div className="space-y-3 min-w-0">
      <div>
        <div className="eyebrow">Contract · clause {clause.no}</div>
        <h4 className="mt-1 text-sm font-semibold text-gray-900 dark:text-ink">{clause.heading}</h4>
        <p className="mt-1.5 text-sm whitespace-pre-wrap text-gray-700 dark:text-muted">{clause.body || "(heading only)"}</p>
      </div>
      {clause.findings.length > 0 && (
        <div className="space-y-1.5">
          <div className="eyebrow">Findings</div>
          {clause.findings.map((f) => (
            <div key={f.rule} className="flex gap-2 items-start text-sm">
              <RiskBadge risk={f.risk} />
              <span className="flex-1">{f.reason}</span>
              <span className="metachip shrink-0">{f.approver}</span>
            </div>
          ))}
        </div>
      )}
      {pb ? (
        <div className="rounded-md border border-gray-200 dark:border-edge p-3 space-y-2 text-sm">
          <div className="eyebrow">Playbook · {pb.title}</div>
          <div>
            <span className="font-medium text-ok">Preferred: </span>
            {pb.preferred}
          </div>
          <div>
            <span className="font-medium text-warn">Fallback: </span>
            {pb.fallback}
          </div>
          <div>
            <span className="font-medium text-bad">Red lines: </span>
            {pb.red_lines.join("; ")}
          </div>
          <div className="text-xs text-gray-500 dark:text-muted">Approver: {pb.approver}</div>
        </div>
      ) : (
        <p className="text-xs text-gray-500 dark:text-muted">No playbook position matched with confidence.</p>
      )}
      {clause.matches.length > 0 && (
        <div>
          <div className="eyebrow mb-1">Top semantic matches</div>
          <ul className="space-y-1">
            {clause.matches.map((m) => (
              <li key={m.type} className="flex items-center gap-2 text-xs">
                <span className="flex-1 truncate">{m.title}</span>
                <Spectrum value={m.sim} label={m.sim.toFixed(3)} />
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function RiskBadge({ risk }: { risk: Risk | "ok" }) {
  const cls =
    risk === "high"
      ? "border-bad/40 bg-bad/10 text-bad"
      : risk === "medium"
        ? "border-warn/40 bg-warn/10 text-warn"
        : risk === "low"
          ? "border-gray-300 bg-gray-100 text-gray-600 dark:border-edge dark:bg-carbon-800 dark:text-muted"
          : "border-ok/40 bg-ok/10 text-ok";
  return (
    <span className={`inline-flex shrink-0 items-center rounded border px-1.5 font-mono text-[10px] uppercase leading-5 ${cls}`}>
      {risk === "ok" ? "ok" : risk}
    </span>
  );
}

function RiskTile({ label, n, cls }: { label: string; n: number; cls: string }) {
  return (
    <div className="rounded-md border border-gray-200 dark:border-edge px-3 py-2">
      <div className="eyebrow">{label}</div>
      <div className={`mt-0.5 text-2xl font-semibold tabular-nums ${cls}`}>{n}</div>
    </div>
  );
}
