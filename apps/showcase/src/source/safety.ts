/**
 * Hard safety rules for the COSHH & Safety Data Assistant (app B).
 *
 * These run in code, before and instead of the model:
 *
 *   1. Emergency words (swallowed, in eyes, burn, can't breathe…) → a
 *      fixed first-aid card from Section 4 of that SDS plus "Call 999 in
 *      an emergency, or NHS 111 for advice". No model text.
 *   2. Mixing two products → always "Do not mix products unless the SDS
 *      for both says it is safe", plus the cited Section 10 text. No
 *      model text.
 *   3. If the SDS sections retrieved do not cover the question → "This
 *      is not in the safety data sheet." and stop. No model text.
 *
 * Only questions that pass all three reach the model, grounded on the
 * cited sections. No imports, so `node --test` can load this file.
 */

export const EMERGENCY_LINE = "Call 999 in an emergency, or NHS 111 for advice.";
export const MIX_RULE = "Do not mix products unless the SDS for both says it is safe.";
export const NOT_IN_SDS = "This is not in the safety data sheet.";
export const COSHH_DRAFT_NOTE = "Draft: a competent person must check this.";

export interface SdsSection {
  id: string;
  sku: string;
  product_name: string;
  section: number;
  section_title: string;
  text: string;
}

export type ExposureRoute = "eyes" | "skin" | "swallowed" | "inhaled";

const EMERGENCY: Array<[ExposureRoute, RegExp]> = [
  ["swallowed", /\b(swallow(ed|s|ing)?|drank|drunk|drink(ing)? it|ingest(ed|ing)?|in (my|his|her|their|the|a) mouth)\b/i],
  ["eyes", /\b(in(to)? (my|his|her|their|your|the|both|one) eyes?|in (the )?eyes?|eye contact|splash(ed)? (in|into) .{0,20}eyes?|eyes? (are )?(burning|stinging|sore))\b/i],
  ["inhaled", /\b(can'?t breathe|cannot breathe|can not breathe|could ?n[o']t breathe|struggling to breathe|trouble breathing|difficulty breathing|short(ness)? of breath|breathed (it )?in|inhal(ed|ing)|choking|coughing)\b/i],
  ["skin", /\b(burn(s|t|ed|ing)?(?! easily)|blister(s|ed|ing)?|on (my|his|her|their) (skin|hands?|arms?|face)|skin (is )?(red|sore|stinging))\b/i],
];

/** The exposure route an emergency question describes, or null. */
export function detectEmergency(question: string): ExposureRoute | null {
  for (const [route, re] of EMERGENCY) if (re.test(question)) return route;
  return null;
}

const SKU_RE = /\b([A-Z]{2})-(\d{3})\b/gi;
const MIX_RE =
  /\b(mix(es|ed|ing)?|combin(e|es|ed|ing)|blend(s|ed|ing)?|together|same (bucket|bottle|tank|mop|cloth|time)|at the same time|straight after|followed by|add(ed|ing)? (it )?(to|into))\b/i;
const OTHER_PRODUCT_RE =
  /\b(bleach|chlorine|ammonia|descaler|degreaser|disinfectant|sanitiser|remover|polish|stripper|detergent|toilet cleaner|another product|other products?|same (bucket|bottle|tank))\b/i;

/** SKU codes mentioned in a question, upper-cased, de-duplicated. */
export function mentionedSkus(question: string): string[] {
  return [...new Set([...question.matchAll(SKU_RE)].map((m) => `${m[1]}-${m[2]}`.toUpperCase()))];
}

/**
 * Is this a "can I mix…" question? True when it uses mixing words, or
 * names two products (two SKUs, or the selected one plus bleach etc.).
 */
export function detectMix(question: string, selectedSku: string): { mix: boolean; skus: string[] } {
  const named = mentionedSkus(question);
  const skus = [...new Set([selectedSku.toUpperCase(), ...named])];
  const twoProducts = skus.length >= 2 || OTHER_PRODUCT_RE.test(question);
  const mixing = MIX_RE.test(question);
  // "Do I mix it with water?" is dilution, not mixing two products.
  const waterOnly = !twoProducts && /\b(with|in|into) (cold |warm |hot )?water\b|\bdilut/i.test(question);
  return { mix: mixing && !waterOnly && (twoProducts || /\bmix/i.test(question)), skus };
}

/** Plain-language topics → the SDS sections that answer them. */
const TOPICS: Array<[RegExp, number[]]> = [
  [/\b(ppe|gloves?|gauntlets?|goggles|glasses|mask|apron|face shield|protect(ion|ive)?|wear)\b/i, [8]],
  [/\b(stor(e|ed|age|ing)|keep|shelf|cupboard|temperature|freez(e|ing)|sunlight|lock(ed)?)\b/i, [7]],
  [/\b(first[- ]aid|eyes?|skin|swallow(ed)?|breath(e|ing)?|inhal(ed|e))\b/i, [4]],
  [/\b(spill(s|ed|age)?|spilt|leak(s|ed|ing)?|split (bottle|container))\b/i, [6]],
  [/\b(dispos(e|al)|empty (containers?|bottles?)|recycl(e|ing)|throw (it )?away|waste)\b/i, [13]],
  [/\b(fire|flammable|burn(s)? easily|extinguish(er)?)\b/i, [5]],
  [/\b(transport|carry(ing)? (it )?in (a|the) van|ship(ping)?)\b/i, [14]],
  [/\b(hazard(s|ous)?|dangerous|harmful|corrosive|irritant|signal word|classif(y|ied|ication)|warning)\b/i, [2]],
  [/\b(ingredients?|contain(s)?|made of|composition|what'?s in it)\b/i, [3]],
  [/\b(ph|appearance|colou?r|smell|odou?r)\b/i, [9]],
  [/\b(react(s|ion)?|incompatible|stable|stability|marble|limestone|alumin(i)?um|metals?|surfaces?)\b/i, [10]],
  [/\b(environment(al)?|aquatic|eco(logy|logical)?|biodegrad\w*|watercourse|drains?)\b/i, [12, 13]],
  [/\b(toxic(ity|ological)?|health effects?|long[- ]term)\b/i, [11]],
  [/\b(what is it (for|used for)|used for|supplier|product code|identification)\b/i, [1]],
  [/\b(safe|safety)\b/i, [2, 7, 8]],
];

/** SDS sections a question is about, by plain-language topic. Empty = no topic matched. */
export function sectionsFor(question: string): number[] {
  const out = new Set<number>();
  for (const [re, secs] of TOPICS) if (re.test(question)) secs.forEach((s) => out.add(s));
  return [...out].sort((a, b) => a - b);
}

const STOP = new Set(
  "a an and are as at be by can could do does for from has have how i if in into is it its me my of on or our should so that the their them then there these this to us was we what when where which who why will with you your use using used product".split(" ")
);

function contentWords(s: string): string[] {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, " ")
    .split(/\s+/)
    .filter((w) => w.length > 2 && !STOP.has(w));
}

/**
 * Do the retrieved sections cover this question? A topic match means
 * the SDS has a section for it; otherwise most of the question's content
 * words must appear in one section. Deterministic, so it still works
 * with the mock embedder.
 */
export function sdsCovers(question: string, sections: SdsSection[]): { covered: boolean; sections: SdsSection[] } {
  if (sections.length === 0) return { covered: false, sections: [] };
  const topical = sectionsFor(question);
  if (topical.length) {
    const hit = sections.filter((s) => topical.includes(s.section));
    if (hit.length) return { covered: true, sections: hit };
  }
  const q = [...new Set(contentWords(question))];
  if (q.length < 2) return { covered: false, sections: [] };
  const scored = sections
    .map((s) => {
      const t = new Set(contentWords(s.text));
      return { s, share: q.filter((w) => t.has(w)).length / q.length };
    })
    .filter((x) => x.share >= 0.6)
    .sort((a, b) => b.share - a.share);
  return scored.length ? { covered: true, sections: scored.map((x) => x.s) } : { covered: false, sections: [] };
}

export function citeLabel(s: Pick<SdsSection, "sku" | "section" | "section_title">): string {
  return `${s.sku} · Section ${s.section}: ${s.section_title}`;
}

/** Strip the demo banner and heading line, leaving the section body. */
export function sectionBody(s: SdsSection): string {
  const lines = s.text.split("\n");
  return (lines.length >= 3 ? lines.slice(2).join(" ") : s.text).trim();
}

const ROUTE_LABEL: Record<ExposureRoute, string> = {
  eyes: "If in eyes",
  skin: "If on skin",
  swallowed: "If swallowed",
  inhaled: "If breathed in",
};

export interface FirstAidCard {
  route: ExposureRoute;
  heading: string;
  steps: string;
  emergency: string;
  cite: string;
}

/** The fixed first-aid card for one exposure route, from Section 4. */
export function firstAidCard(route: ExposureRoute, section4: SdsSection | undefined): FirstAidCard {
  const label = ROUTE_LABEL[route];
  let steps = "Follow the first-aid steps on the product label and get medical advice.";
  if (section4) {
    const body = sectionBody(section4);
    const start = body.indexOf(`${label}:`);
    if (start >= 0) {
      const rest = body.slice(start + label.length + 1);
      const next = rest.search(/\bIf (in eyes|on skin|swallowed|breathed in):/);
      steps = (next >= 0 ? rest.slice(0, next) : rest).trim();
    }
  }
  return {
    route,
    heading: `${label}`,
    steps,
    emergency: EMERGENCY_LINE,
    cite: section4 ? citeLabel(section4) : "Section 4 not found",
  };
}

export interface MixAnswer {
  rule: string;
  cites: Array<{ label: string; text: string; id: string }>;
}

/** The fixed answer to any "can I mix…" question, citing each product's Section 10. */
export function mixAnswer(section10s: SdsSection[]): MixAnswer {
  return {
    rule: MIX_RULE,
    cites: section10s
      .filter((s) => s.section === 10)
      .map((s) => ({ id: s.id, label: citeLabel(s), text: sectionBody(s) })),
  };
}

export type Plan =
  | { kind: "emergency"; route: ExposureRoute }
  | { kind: "mix"; skus: string[] }
  | { kind: "lookup"; sections: number[] };

/** Which rule handles a question. Emergency beats mixing beats a normal lookup. */
export function planQuestion(question: string, selectedSku: string): Plan {
  const route = detectEmergency(question);
  if (route) return { kind: "emergency", route };
  const mix = detectMix(question, selectedSku);
  if (mix.mix) return { kind: "mix", skus: mix.skus };
  return { kind: "lookup", sections: sectionsFor(question) };
}

export interface CoshhField {
  label: string;
  value: string;
  cite: string;
}

export interface CoshhDraft {
  note: string;
  fields: CoshhField[];
}

/** A simple COSHH assessment form filled only from cited SDS sections. */
export function draftCoshh(sections: SdsSection[]): CoshhDraft {
  const by = new Map(sections.map((s) => [s.section, s]));
  const f = (label: string, n: number, pick: (body: string) => string = (b) => b): CoshhField => {
    const s = by.get(n);
    return s
      ? { label, value: pick(sectionBody(s)), cite: citeLabel(s) }
      : { label, value: "Not found in the SDS — a competent person must fill this in.", cite: `Section ${n} missing` };
  };
  return {
    note: COSHH_DRAFT_NOTE,
    fields: [
      f("Substance and use", 1, (b) => b.replace(/ Supplier:.*$/, "")),
      f("Hazards", 2),
      f("PPE", 8),
      f("Storage and handling", 7),
      f("First aid", 4),
      f("Spills", 6),
      f("Disposal", 13),
    ],
  };
}
