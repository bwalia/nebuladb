/**
 * Deterministic reorder logic for app D: the forecast, the spike flag,
 * "switch to…" parsing and the delivery-day split. No model involved,
 * and no runtime imports, so `node --test` can load it.
 */

export interface HistLine {
  sku: string;
  month: string;
  qty: number;
}

export interface ForecastRow {
  sku: string;
  /** Packs per month, oldest first. */
  history: Array<{ month: string; qty: number }>;
  last: number;
  avg3: number;
  forecast: number;
  spike: boolean;
  /** Last month vs the average of the three months before it, as a share (0.7 = +70%). */
  spikeShare: number;
  reason: string;
}

/**
 * Forecast per SKU: the average of the last 3 months, rounded up to whole
 * packs. A spike is last month more than 50% above the 3 months before it.
 */
export function forecastLines(lines: HistLine[], months: readonly string[]): ForecastRow[] {
  const skus = [...new Set(lines.map((l) => l.sku))].sort();
  return skus.map((sku) => {
    const history = months.map((month) => ({
      month,
      qty: lines.filter((l) => l.sku === sku && l.month === month).reduce((n, l) => n + l.qty, 0),
    }));
    const qty = history.map((h) => h.qty);
    const last3 = qty.slice(-3);
    const avg3 = last3.reduce((a, b) => a + b, 0) / Math.max(1, last3.length);
    const last = qty[qty.length - 1] ?? 0;
    const before = qty.slice(-4, -1);
    const beforeAvg = before.reduce((a, b) => a + b, 0) / Math.max(1, before.length);
    const spikeShare = beforeAvg > 0 ? last / beforeAvg - 1 : 0;
    const spike = spikeShare > 0.5;
    const forecast = Math.ceil(avg3 - 1e-9);
    let reason =
      forecast > last ? `Up: the 3-month average (${avg3.toFixed(1)}) is above last month (${last})` :
      forecast < last ? `Down: the 3-month average (${avg3.toFixed(1)}) is below last month (${last})` :
      "Same as last month";
    if (spike) reason += `. Last month was ${Math.round(spikeShare * 100)}% above the 3 months before it: check for a one-off before ordering this much`;
    return { sku, history, last, avg3, forecast, spike, spikeShare, reason };
  });
}

/** "…but switch to recycled hand towels and swap the floor cleaner for a concentrate" → phrases. */
export function parseSwitches(text: string): string[] {
  const out: string[] = [];
  const re = /\b(?:switch(?:ing)?|swap(?:ping)?|change|move)\s+(?:(?:the|our)\s+[a-z\s-]+?\s+)?(?:to|for|with)\s+([^,.;]+?)(?=\s+and\s+(?:switch|swap|change|move)\b|[,.;]|$)/gi;
  for (const m of text.matchAll(re)) {
    const phrase = m[1].trim().replace(/^(an?|the|some)\s+/i, "");
    if (phrase) out.push(phrase);
  }
  return out;
}

/** Which product category a "switch to…" phrase is about. */
export function guessCategory(phrase: string): "paper" | "chemicals" | "waste" | "PPE" | "equipment" | "washroom" | null {
  const p = phrase.toLowerCase();
  if (/towel|roll|tissue|napkin|paper|wiper/.test(p)) return "paper";
  if (/liner|sack|bin bag|bags?\b/.test(p)) return "waste";
  if (/glove|apron|mask|goggle/.test(p)) return "PPE";
  if (/cloth|mop|pad|bucket/.test(p)) return "equipment";
  if (/dispenser|dryer|urinal screen|air freshener/.test(p)) return "washroom";
  if (/clean|concentrate|descal|degreas|sanitis|soap|spray|chemical|detergent|polish/.test(p)) return "chemicals";
  return null;
}

/** Match an account (and optionally a site) named in free text. */
export function matchSite<T extends { account_id: string; account_name: string; site: string }>(
  text: string,
  sites: T[]
): { account?: string; site?: string } {
  const t = text.toLowerCase();
  const acct = sites.find(
    (s) => t.includes(s.account_id.toLowerCase()) || t.includes(s.account_name.toLowerCase().split(" ")[0])
  );
  const site = [...sites]
    .sort((a, b) => b.site.length - a.site.length)
    .find((s) => t.includes(s.site.toLowerCase()) && (!acct || s.account_id === acct.account_id));
  if (site) return { account: site.account_id, site: site.site };
  return acct ? { account: acct.account_id } : {};
}

const DAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

/** Next date (strictly after today) that falls on the site's delivery day. */
export function nextDelivery(todayIso: string, day: string): string {
  const d = new Date(`${todayIso}T00:00:00Z`);
  const target = DAYS.indexOf(day);
  if (target < 0) return todayIso;
  do d.setUTCDate(d.getUTCDate() + 1);
  while (d.getUTCDay() !== target);
  return d.toISOString().slice(0, 10);
}

export function daysUntil(fromIso: string, toIso: string): number {
  return Math.round((Date.parse(`${toIso}T00:00:00Z`) - Date.parse(`${fromIso}T00:00:00Z`)) / 86_400_000);
}

/** Can this line make the next delivery? */
export function makesDelivery(p: { in_stock: boolean; stock_qty: number; lead_days: number }, qty: number, daysToDelivery: number): boolean {
  return p.in_stock && p.stock_qty >= qty && p.lead_days <= daysToDelivery;
}
