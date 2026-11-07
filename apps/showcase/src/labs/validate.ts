/**
 * Pack consistency rules. Returns a list of problems (empty = valid).
 * Used by labs.test.ts for every pack, and handy while authoring one:
 *
 *   node -e 'import("./src/labs/validate.ts").then(async v => {
 *     const { PACK } = await import("./src/labs/packs/campus.ts");
 *     console.log(v.validatePack(PACK)) })'
 */
import type { LabId, Pack, PackDoc } from "./types.ts";
import { checkSql } from "./engine/core.ts";

export const LAB_IDS: LabId[] = ["semantic", "models", "filters", "rag", "reason", "refuse", "scale", "map"];

export function matchesSafety(text: string, triggers: string[]): boolean {
  const t = text.toLowerCase();
  return triggers.some((p) => t.includes(p.toLowerCase()));
}

function docMatches(doc: PackDoc, filters: Record<string, string>): boolean {
  return Object.entries(filters).every(([k, v]) => {
    const m = doc.metadata[k];
    if (Array.isArray(m)) return m.includes(v);
    return String(m) === v;
  });
}

export function validatePack(p: Pack): string[] {
  const errs: string[] = [];
  const err = (m: string) => errs.push(`${p.id}: ${m}`);

  if (p.bucket !== `lab_${p.id}`) err(`bucket must be lab_${p.id}`);
  if (p.docs.length < 60) err(`needs >= 60 docs, has ${p.docs.length}`);
  const ids = new Set<string>();
  for (const d of p.docs) {
    if (ids.has(d.id)) err(`duplicate doc id ${d.id}`);
    ids.add(d.id);
    if (!d.text.trim()) err(`empty text ${d.id}`);
    if (d.text.length > 700) err(`text too long (${d.text.length}) ${d.id}`);
    if (d.id.includes("#")) err(`'#' not allowed in id ${d.id}`);
  }

  // Filters: every field on every doc, right shape.
  for (const f of p.filters) {
    for (const d of p.docs) {
      const v = d.metadata[f.field];
      if (v === undefined) { err(`doc ${d.id} lacks filter field ${f.field}`); break; }
      if (f.op === "contains" && !Array.isArray(v)) { err(`${f.field} must be an array on ${d.id}`); break; }
      if (f.op === "max" && typeof v !== "number") { err(`${f.field} must be a number on ${d.id}`); break; }
      if (f.op === "bool" && typeof v !== "boolean") { err(`${f.field} must be boolean on ${d.id}`); break; }
      if (f.op === "eq" && typeof v !== "string") { err(`${f.field} must be a string on ${d.id}`); break; }
    }
    if ((f.op === "eq" || f.op === "contains") && !(f.values && f.values.length >= 2)) err(`filter ${f.field} needs >= 2 values`);
  }
  if (p.filters.length < 3) err("needs >= 3 filters");
  const apostrophe = p.docs.some((d) =>
    p.filters.some((f) => f.op === "eq" && typeof d.metadata[f.field] === "string" && String(d.metadata[f.field]).includes("'"))
  );
  if (!apostrophe) err("at least one eq-filter value must contain an apostrophe (SQL escaping test)");

  // Colour field for the map.
  const colours = new Set(p.docs.map((d) => String(d.metadata[p.colorField])));
  if (p.docs.some((d) => d.metadata[p.colorField] === undefined)) err(`colorField ${p.colorField} missing on some docs`);
  if (colours.size < 3 || colours.size > 10) err(`colorField needs 3–10 distinct values, has ${colours.size}`);

  // Labelled questions.
  if (p.questions.length < 20) err(`needs >= 20 labelled questions, has ${p.questions.length}`);
  const qids = new Set<string>();
  for (const q of p.questions) {
    if (qids.has(q.id)) err(`duplicate question id ${q.id}`);
    qids.add(q.id);
    if (!q.expect.length) err(`question ${q.id} has no expected docs`);
    for (const e of q.expect) if (!ids.has(e)) err(`question ${q.id} expects unknown doc ${e}`);
    if (q.kind === "filtered") {
      if (!q.filters || !Object.keys(q.filters).length) err(`filtered question ${q.id} needs filters`);
      else {
        for (const k of Object.keys(q.filters)) if (!p.filters.some((f) => f.field === k)) err(`question ${q.id} filters on unknown field ${k}`);
        for (const e of q.expect) {
          const d = p.docs.find((x) => x.id === e);
          if (d && !docMatches(d, q.filters)) err(`question ${q.id}: expected doc ${e} fails its own filter`);
        }
      }
    }
    for (const r of p.safety) if (matchesSafety(q.q, r.triggers)) err(`safety rule ${r.id} falsely fires on question ${q.id}`);
  }
  const count = (k: string) => p.questions.filter((q) => q.kind === k).length;
  if (count("paraphrase") < 4) err("needs >= 4 paraphrase questions");
  if (count("keyword") < 2) err("needs >= 2 keyword questions");
  if (count("filtered") < 2) err("needs >= 2 filtered questions");

  // Refusals.
  for (const k of ["safety", "out_of_scope", "acl"] as const) {
    if (p.refuse.filter((r) => r.expect === k).length < 2) err(`needs >= 2 refuse questions of kind ${k}`);
  }
  for (const r of p.refuse) {
    if (r.expect === "safety") {
      const rule = p.safety.find((s) => s.id === r.rule);
      if (!rule) err(`refuse ${r.id} names unknown rule ${r.rule}`);
      else if (!matchesSafety(r.q, rule.triggers)) err(`refuse ${r.id}: rule ${rule.id} does not fire on its text`);
    } else if (r.expect === "acl") {
      const persona = p.personas.find((x) => x.id === r.persona);
      if (!persona) err(`refuse ${r.id} names unknown persona ${r.persona}`);
    } else {
      for (const s of p.safety) if (matchesSafety(r.q, s.triggers)) err(`out_of_scope ${r.id} also fires safety rule ${s.id}`);
    }
  }
  if (p.safety.length < 2) err("needs >= 2 safety rules");
  for (const s of p.safety) if (!s.card.trim() || !s.triggers.length) err(`safety rule ${s.id} incomplete`);

  // Personas: each restricts to a real, non-empty, non-total slice.
  if (p.personas.length < 2) err("needs >= 2 personas");
  for (const per of p.personas) {
    for (const k of Object.keys(per.filter)) if (!p.filters.some((f) => f.field === k)) err(`persona ${per.id} filters on non-filter field ${k}`);
    const n = p.docs.filter((d) => docMatches(d, per.filter)).length;
    if (n === 0 || n === p.docs.length) err(`persona ${per.id} must see some but not all docs (sees ${n})`);
  }

  // Reasoning plans.
  if (p.reason.length < 2) err("needs >= 2 reason tasks");
  for (const t of p.reason) {
    if (!t.plan.length) err(`reason ${t.id} has an empty plan`);
    for (const s of t.plan) {
      if (s.kind === "sql") for (const e of checkSql(s.sql, p.bucket)) err(`reason ${t.id}: ${e}`);
    }
  }

  for (const l of LAB_IDS) if (!p.challenges[l]?.trim()) err(`missing challenge for lab ${l}`);
  return errs;
}
