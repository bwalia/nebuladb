/**
 * Voice of Customer — type a theme ("battery dies fast"); NebulaDB pulls
 * the reviews closest to it across products and channels; the panel
 * aggregates sentiment, rating and product mix of that neighbourhood.
 * Deterministic analytics on vector results; reviews are invented.
 */
import { Stat } from "../../components";
import type { SeedDoc } from "../../rag/seed";
import type { UseCaseHit, UseCaseSpec } from "../UseCaseDemo";

const PRODUCTS = ["Pulse Earbuds", "Pulse Watch", "Pulse App"] as const;
const CHANNELS = ["app-store", "support-ticket", "survey", "social"] as const;

type Sentiment = "negative" | "neutral" | "positive";

function r(id: string, product: string, channel: string, rating: number, sentiment: Sentiment, body: string): SeedDoc {
  return {
    id,
    text: `${product} · ${rating}★ · ${channel}\n${body}`,
    metadata: { review_id: id, product, channel, rating, sentiment, snippet: body.slice(0, 60) },
  };
}

const DOCS: SeedDoc[] = [
  r("RV-001", "Pulse Earbuds", "app-store", 2, "negative", "Battery barely lasts three hours now, used to be six. Very disappointed after the update."),
  r("RV-002", "Pulse Earbuds", "support-ticket", 1, "negative", "Left earbud drains to zero overnight in the case. Replacement had the same problem."),
  r("RV-003", "Pulse Earbuds", "survey", 4, "positive", "Sound is great and noise cancelling on the tube is excellent, battery fine for my commute."),
  r("RV-004", "Pulse Earbuds", "social", 2, "negative", "Connection keeps dropping when my phone is in my back pocket. Annoying on runs."),
  r("RV-005", "Pulse Earbuds", "app-store", 5, "positive", "Comfortable for hours, the fit tips in the box are a nice touch."),
  r("RV-006", "Pulse Watch", "app-store", 2, "negative", "Watch battery went from 4 days to under 2 after firmware 5.1. Sleep tracking seems to be the drain."),
  r("RV-007", "Pulse Watch", "support-ticket", 1, "negative", "Heart rate readings jump to 180 while I'm sitting at my desk. Can't trust the data."),
  r("RV-008", "Pulse Watch", "survey", 4, "positive", "Love the sleep score, it actually changed my bedtime habits."),
  r("RV-009", "Pulse Watch", "social", 3, "neutral", "Strap irritates my wrist after a long day, otherwise does the job."),
  r("RV-010", "Pulse Watch", "app-store", 5, "positive", "GPS lock is quick and run tracking matches my old Garmin."),
  r("RV-011", "Pulse App", "app-store", 1, "negative", "Sync takes forever and sometimes loses a whole day of steps."),
  r("RV-012", "Pulse App", "support-ticket", 2, "negative", "After the redesign I can't find where to export my workouts. Please bring it back."),
  r("RV-013", "Pulse App", "survey", 3, "neutral", "The new dashboard looks nice but it's slower to load than before."),
  r("RV-014", "Pulse App", "social", 2, "negative", "App kills my phone battery running in the background all day."),
  r("RV-015", "Pulse App", "app-store", 5, "positive", "Weekly insights email is genuinely useful, well done."),
  r("RV-016", "Pulse Earbuds", "support-ticket", 2, "negative", "Charging case stops charging unless I wiggle the cable. USB-C port feels loose."),
];

function VocInsight({ hits, query }: { hits: UseCaseHit[]; query: string }) {
  const theme = hits.slice(0, 10);
  if (theme.length === 0) return <p className="text-sm text-gray-500 dark:text-muted">No reviews matched.</p>;
  const sentiment = { negative: 0, neutral: 0, positive: 0 };
  const products: Record<string, number> = {};
  let ratingSum = 0;
  for (const h of theme) {
    const s = String(h.meta.sentiment ?? "neutral") as Sentiment;
    sentiment[s] = (sentiment[s] ?? 0) + 1;
    const p = String(h.meta.product ?? "?");
    products[p] = (products[p] ?? 0) + 1;
    ratingSum += Number(h.meta.rating ?? 0);
  }
  const n = theme.length;
  const pct = (k: Sentiment) => Math.round((sentiment[k] / n) * 100);
  return (
    <div className="space-y-3">
      <p className="text-xs text-gray-500 dark:text-muted">
        The {n} reviews closest to “{query}” form this theme's neighbourhood.
      </p>
      <div className="flex h-3 w-full overflow-hidden rounded-full border border-gray-200 dark:border-edge">
        <div className="bg-bad" style={{ width: `${pct("negative")}%` }} title="negative" />
        <div className="bg-warn" style={{ width: `${pct("neutral")}%` }} title="neutral" />
        <div className="bg-ok" style={{ width: `${pct("positive")}%` }} title="positive" />
      </div>
      <div className="flex flex-wrap gap-3">
        <Stat label="negative" value={`${pct("negative")}%`} />
        <Stat label="neutral" value={`${pct("neutral")}%`} />
        <Stat label="positive" value={`${pct("positive")}%`} />
        <Stat label="avg rating" value={(ratingSum / n).toFixed(1)} />
      </div>
      <div>
        <div className="eyebrow mb-1">Products in this theme</div>
        <div className="flex flex-wrap gap-2">
          {Object.entries(products)
            .sort((a, b) => b[1] - a[1])
            .map(([p, c]) => (
              <Stat key={p} label={p} value={c} />
            ))}
        </div>
      </div>
      <div>
        <div className="eyebrow mb-1">Most representative quotes</div>
        <ul className="space-y-1.5 text-sm">
          {theme.slice(0, 3).map((h) => (
            <li key={h.id} className="border-l-2 border-accent pl-2">
              {h.text.split("\n").slice(1).join(" ")}{" "}
              <span className="text-xs text-gray-500 dark:text-muted">— {h.sub}</span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export const voiceOfCustomerSpec: UseCaseSpec = {
  bucket: "customer_reviews",
  seedVersion: "voc-v1",
  docs: DOCS,
  noun: "reviews",
  title: "Voice of Customer",
  subtitle: "Theme in plain English → nearest reviews across app stores, tickets, surveys and social → sentiment, rating and product mix. No LLM.",
  queryLabel: "Theme to investigate",
  placeholder: "e.g. battery drains fast after update",
  scenarios: [
    { label: "Battery complaints", q: "battery drains much faster than it used to" },
    { label: "Connectivity", q: "bluetooth keeps disconnecting", filters: { product: "Pulse Earbuds" } },
    { label: "Data accuracy", q: "readings are wrong and I don't trust the data" },
    { label: "Redesign backlash", q: "new update moved features and made it slower", filters: { product: "Pulse App" } },
    { label: "Tickets only", q: "hardware fault needing replacement", filters: { channel: "support-ticket" } },
  ],
  filters: [
    { key: "product", label: "Product", kind: "select", options: PRODUCTS },
    { key: "channel", label: "Channel", kind: "select", options: CHANNELS },
    { key: "rating", label: "Max rating", kind: "max" },
  ],
  limit: 12,
  idKey: "review_id",
  titleKey: "snippet",
  subKeys: ["product", "channel", "sentiment"],
  resultsTitle: "Reviews in this theme",
  Insight: VocInsight,
  insightTitle: "Theme breakdown",
  insightSubtitle: "Aggregated over the 10 nearest reviews",
};
