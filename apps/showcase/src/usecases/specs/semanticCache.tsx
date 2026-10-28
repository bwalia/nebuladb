/**
 * Semantic LLM Cache — before calling the model, look up the nearest
 * previously answered prompt. Above the similarity threshold the cached
 * answer is served (no tokens spent); below it the request falls through
 * to the LLM. Deterministic — the threshold is the whole policy.
 */
import { useState } from "react";
import { Stat } from "../../components";
import type { SeedDoc } from "../../rag/seed";
import type { UseCaseHit, UseCaseSpec } from "../UseCaseDemo";

const TENANTS = ["support-bot", "hr-bot"] as const;

function entry(id: string, tenant: string, prompt: string, answer: string, tokens: number, latency_ms: number): SeedDoc {
  return {
    id,
    // Only the prompt is embedded meaningfully; the answer rides along as metadata.
    text: prompt,
    metadata: { cache_id: id, tenant, prompt, answer, tokens, latency_ms },
  };
}

const DOCS: SeedDoc[] = [
  entry("KV-001", "support-bot", "How do I reset my password?",
    "Go to Settings → Security → Reset password. A link is emailed and valid for 30 minutes.", 410, 1850),
  entry("KV-002", "support-bot", "Can I change the email address on my account?",
    "Yes — Settings → Profile → Email. We send a confirmation to both the old and the new address.", 380, 1700),
  entry("KV-003", "support-bot", "How do I cancel my subscription?",
    "Billing → Plan → Cancel. You keep access until the end of the current billing period; no partial refunds.", 520, 2100),
  entry("KV-004", "support-bot", "Do you offer refunds on annual plans?",
    "Annual plans are refundable pro rata within 30 days of purchase. Contact billing with your invoice number.", 560, 2300),
  entry("KV-005", "support-bot", "Why was my card declined?",
    "Common causes are 3-D Secure failing, an expired card or a bank fraud block. Try again or use a different card.", 600, 2400),
  entry("KV-006", "support-bot", "How do I export my data?",
    "Settings → Data → Export creates a ZIP of JSON and CSV files; large accounts receive an email link within an hour.", 470, 1950),
  entry("KV-007", "support-bot", "Is there a dark mode?",
    "Yes. Toggle it under Settings → Appearance, or it follows your system setting by default.", 260, 1300),
  entry("KV-101", "hr-bot", "How many days of annual leave do I get?",
    "25 days plus bank holidays, rising by one day per year of service to a maximum of 30.", 300, 1500),
  entry("KV-102", "hr-bot", "How do I claim back a train ticket for a client visit?",
    "Submit it in the expenses tool with the receipt within 60 days; standard class only unless pre-approved.", 450, 1900),
  entry("KV-103", "hr-bot", "What is the parental leave policy?",
    "Birth parents get 26 weeks at full pay; partners 6 weeks at full pay. Notify People Ops 15 weeks before the due date.", 640, 2500),
];

function CacheInsight({ hits }: { hits: UseCaseHit[] }) {
  const [threshold, setThreshold] = useState(0.85);
  const top = hits[0];
  if (!top) return <p className="text-sm text-gray-500 dark:text-muted">Cache is empty for this tenant.</p>;
  const hit = top.sim >= threshold;
  const runnerUp = hits[1];
  const tokens = Number(top.meta.tokens ?? 0);
  const latency = Number(top.meta.latency_ms ?? 0);
  return (
    <div className="space-y-3">
      <label className="block text-xs">
        <span className="font-medium">Similarity threshold: </span>
        <span className="font-mono">{threshold.toFixed(2)}</span>
        <input
          type="range"
          min={0.5}
          max={0.99}
          step={0.01}
          value={threshold}
          onChange={(e) => setThreshold(Number(e.target.value))}
          className="w-full mt-1"
        />
        <span className="text-gray-500 dark:text-muted">
          Lower = more hits but more risk of serving the wrong answer; higher = safer, more LLM calls.
        </span>
      </label>
      <div
        className={`rounded-md px-3 py-2 text-center text-sm font-semibold uppercase ${
          hit ? "bg-ok/15 text-ok" : "bg-warn/15 text-warn"
        }`}
      >
        {hit ? "Cache hit — served from NebulaDB" : "Cache miss — forward to LLM"}
      </div>
      <div className="flex flex-wrap gap-3">
        <Stat label="top sim" value={top.sim.toFixed(3)} />
        <Stat label="matched" value={top.id} />
        {runnerUp && <Stat label="margin" value={(top.sim - runnerUp.sim).toFixed(3)} />}
      </div>
      <div className="rounded-md border border-gray-200 dark:border-edge px-3 py-2 text-sm">
        <div className="eyebrow mb-1">Nearest cached prompt</div>
        <div className="font-medium">{String(top.meta.prompt ?? top.title)}</div>
        <div className="eyebrow mt-2 mb-1">{hit ? "Answer served" : "Would have served"}</div>
        <div className={hit ? "" : "text-gray-500 dark:text-muted line-through"}>{String(top.meta.answer ?? "")}</div>
      </div>
      {hit ? (
        <div className="flex flex-wrap gap-3">
          <Stat label="tokens saved" value={tokens} />
          <Stat label="latency saved" value={`~${latency} ms`} />
        </div>
      ) : (
        <p className="text-xs text-gray-500 dark:text-muted">
          On a miss the app calls the model, then writes the new prompt/answer pair back to this bucket so
          the next paraphrase hits.
        </p>
      )}
      {runnerUp && top.sim - runnerUp.sim < 0.02 && (
        <p className="text-xs text-warn">
          Two cached prompts are almost equally close — an ambiguous question. Consider a higher threshold.
        </p>
      )}
    </div>
  );
}

export const semanticCacheSpec: UseCaseSpec = {
  bucket: "llm_semantic_cache",
  seedVersion: "cache-v1",
  docs: DOCS,
  noun: "cached answers",
  title: "Semantic LLM Cache",
  subtitle: "Incoming prompt → nearest previously answered prompt → serve cached answer above a similarity threshold, else call the LLM. No model in the hit path.",
  queryLabel: "Incoming user prompt",
  placeholder: "Type a paraphrase of a common question…",
  scenarios: [
    { label: "Paraphrase → hit", q: "i forgot my password, how can i set a new one", filters: { tenant: "support-bot" } },
    { label: "Close but different", q: "Can I get a refund on my monthly plan?", filters: { tenant: "support-bot" } },
    { label: "Unseen → miss", q: "Does the API support webhooks for invoice events?", filters: { tenant: "support-bot" } },
    { label: "HR tenant", q: "how much holiday allowance do employees get", filters: { tenant: "hr-bot" } },
  ],
  filters: [{ key: "tenant", label: "Tenant / app", kind: "select", options: TENANTS }],
  limit: 5,
  idKey: "cache_id",
  titleKey: "prompt",
  subKeys: ["tenant", "tokens"],
  resultsTitle: "Nearest cached prompts",
  Insight: CacheInsight,
  insightTitle: "Cache decision",
  insightSubtitle: "Raw cosine similarity vs threshold",
};
