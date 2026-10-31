/**
 * Deterministic parts of the Source Washroom Service Desk (app E):
 * keyword rules that back up the nearest-neighbour vote, the
 * missed-visit check, and the quote helper's unit maths. No runtime
 * imports, so `node --test` can load it.
 */

export type Service = "sanitary" | "nappy" | "air-care" | "dryer" | "refills";
export type Priority = "P1" | "P2" | "P3" | "P4";

const SERVICE_RULES: Array<[Service, RegExp]> = [
  ["nappy", /\b(nappy|nappies|baby chang\w*)\b/i],
  ["sanitary", /\b(sanitary|feminine|period|sani ?bins?|ladies'? (bin|toilet)|girls'? toilets?)\b/i],
  ["dryer", /\b(hand ?dryers?|dryers?)\b/i],
  ["refills", /\b(towels?|soap|toilet rolls?|tissue|refills?|ran out|run out|empty dispensers?)\b/i],
  ["air-care", /\b(air ?(freshener|care)|fragrance|smells?|odou?r|stinks?)\b/i],
];

/** Service named by keywords, or null when the text does not say. */
export function serviceByRule(issue: string): Service | null {
  for (const [s, re] of SERVICE_RULES) if (re.test(issue)) return s;
  return null;
}

/** Priority floor from keywords (lower number = more urgent). */
export function priorityByRule(issue: string, missedVisit: boolean): { priority: Priority | null; why: string[] } {
  const why: string[] = [];
  let p: number | null = null;
  const raise = (n: number, reason: string) => {
    why.push(reason);
    p = p === null ? n : Math.min(p, n);
  };
  if (/\b(trip(s|ping)? the (electrics|power)|sparks?|shock|smoke|burning smell|exposed wires?)\b/i.test(issue)) raise(1, "electrical safety");
  if (/\b(overflow\w*|full|weeks?|not (been )?changed|not emptied)\b/i.test(issue)) raise(2, "unit full or not changed");
  if (/\b(smells?|stinks?|odou?r)\b/i.test(issue) && /\b(weeks?|days?|still|always|keeps?)\b/i.test(issue)) raise(2, "lasting smell");
  if (missedVisit) raise(2, "missed planned visit");
  return { priority: p === null ? null : (`P${p}` as Priority), why };
}

/** Is the site overdue? Last completed visit older than the contract cycle. */
export function visitStatus(lastCompleted: string | null, cycleDays: number, todayIso: string): { nextDue: string | null; overdue: boolean; daysSince: number | null } {
  if (!lastCompleted) return { nextDue: null, overdue: true, daysSince: null };
  const t = (iso: string) => Date.parse(`${iso}T00:00:00Z`);
  const daysSince = Math.round((t(todayIso) - t(lastCompleted)) / 86_400_000);
  const due = new Date(t(lastCompleted));
  due.setUTCDate(due.getUTCDate() + cycleDays);
  return { nextDue: due.toISOString().slice(0, 10), overdue: daysSince > cycleDays, daysSince };
}

export interface SiteDescription {
  floors: number;
  staff: number;
  femaleToilets: number;
  accessibleToilets: number;
  babyChange: number;
  basins: number;
}

function grab(text: string, re: RegExp): number {
  const m = re.exec(text);
  return m ? Number(m[1]) : 0;
}

/** "3 floors, 40 staff, 6 female toilets, 2 baby change rooms" → numbers. */
export function parseSite(text: string): SiteDescription {
  const t = text.toLowerCase();
  return {
    floors: grab(t, /(\d+)\s*(?:floors?|storeys?|levels?)/) || 1,
    staff: grab(t, /(\d+)\s*(?:staff|people|employees|pupils|students|residents|users)/),
    femaleToilets: grab(t, /(\d+)\s*(?:female|ladies'?|women'?s|girls'?)\s*(?:toilets?|cubicles?|wcs?)/),
    accessibleToilets: grab(t, /(\d+)\s*(?:accessible|disabled)\s*(?:toilets?|cubicles?|wcs?)/),
    babyChange: grab(t, /(\d+)\s*(?:baby[- ]?chang\w*|nappy)\s*(?:rooms?|units?|areas?)?/),
    basins: grab(t, /(\d+)\s*(?:basins?|sinks?)/),
  };
}

export interface QuoteLine {
  sku: string;
  label: string;
  units: number;
  why: string;
}

export interface QuoteDraft {
  lines: QuoteLine[];
  cycleDays: number;
  cycleWhy: string;
}

/**
 * Units and visit frequency from the demo rules in ss_tender_kb
 * (CAP-WR-02..04): one sanitary unit per female and accessible toilet,
 * one nappy unit per baby change room, one air care unit per washroom,
 * a 14-day cycle when more than 40 female users share a washroom.
 */
export function draftQuote(d: SiteDescription): QuoteDraft {
  const washrooms = Math.max(d.floors, 1) * 2;
  const femaleUsers = Math.round(d.staff / 2);
  const perWashroom = femaleUsers / Math.max(1, d.floors);
  const lines: QuoteLine[] = [
    { sku: "FH-560", label: "Sanitary disposal units", units: d.femaleToilets + d.accessibleToilets, why: `${d.femaleToilets} female + ${d.accessibleToilets} accessible toilets, one unit each` },
    { sku: "NB-565", label: "Nappy waste units", units: d.babyChange, why: `${d.babyChange} baby change room${d.babyChange === 1 ? "" : "s"}, one unit each` },
    { sku: "AF-540", label: "Passive air care refills", units: washrooms, why: `about ${washrooms} washrooms (2 per floor), one unit each` },
  ];
  if (d.basins > 0) lines.push({ sku: "HD-570", label: "High-speed hand dryers", units: Math.ceil(d.basins / 2), why: `${d.basins} basins, one dryer per two` });
  const busy = perWashroom > 40;
  return {
    lines: lines.filter((l) => l.units > 0),
    cycleDays: busy ? 14 : 28,
    cycleWhy: `about ${femaleUsers} female users over ${Math.max(d.floors, 1)} floor${d.floors === 1 ? "" : "s"} ≈ ${Math.round(perWashroom)} per washroom → ${busy ? "over 40, so every 14 days" : "40 or fewer, so every 28 days"}`,
  };
}
