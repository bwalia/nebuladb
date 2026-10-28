/**
 * Prior-Art Search — invention disclosure → nearest patent abstracts,
 * optionally within a CPC class → a novelty indicator and the closest
 * references to review. Deterministic; patents are invented.
 */
import { Stat } from "../../components";
import type { SeedDoc } from "../../rag/seed";
import type { UseCaseHit, UseCaseSpec } from "../UseCaseDemo";

const CPC = ["G06F", "G06N", "H04L", "A61B", "B60L"] as const;

function pat(id: string, cpc: string, year: number, assignee: string, title: string, abstract: string): SeedDoc {
  return {
    id,
    text: `${id} · ${title}\nCPC ${cpc} · ${assignee} · ${year}\n${abstract}`,
    metadata: { patent_id: id, cpc, year, assignee, title },
  };
}

const DOCS: SeedDoc[] = [
  pat("XP-1182044", "G06F", 2021, "Orbis Data", "Approximate nearest-neighbour index with tiered storage",
    "A graph-based vector index keeps upper layers in memory and lower layers on SSD, prefetching neighbour lists to reduce query latency for billion-scale embeddings."),
  pat("XP-1203311", "G06F", 2022, "Lattice Systems", "Hybrid lexical and vector retrieval with score fusion",
    "Search results from an inverted index and a vector index are merged using reciprocal rank fusion, with per-query weights learned from click feedback."),
  pat("XP-1219870", "G06N", 2023, "Kestrel AI", "Retrieval-augmented generation with citation verification",
    "A language model generates an answer from retrieved passages; a verifier checks each sentence is entailed by a cited passage and removes unsupported claims."),
  pat("XP-1224105", "G06N", 2023, "Corvid Labs", "Semantic caching of language model responses",
    "Prompts are embedded and stored with responses; a new prompt within a distance threshold of a stored prompt returns the stored response without invoking the model."),
  pat("XP-1230456", "H04L", 2022, "Meridian Networks", "Replicated write-ahead log across regions",
    "A database ships write-ahead log segments to remote regions asynchronously and promotes a replica on failure using a fencing token to prevent split brain."),
  pat("XP-1241789", "H04L", 2024, "Meridian Networks", "Health-checked DNS failover with low TTL",
    "An authoritative DNS server returns different addresses based on active health checks of points of presence, with TTLs under 60 seconds."),
  pat("XP-1150223", "A61B", 2020, "Halden Medical", "Wearable ECG patch with on-device arrhythmia detection",
    "A flexible adhesive patch records single-lead ECG and runs a neural network on a microcontroller to flag atrial fibrillation episodes."),
  pat("XP-1188760", "A61B", 2021, "Halden Medical", "Retrieval of similar patient cases by imaging embedding",
    "Medical images are encoded into vectors; clinicians query a database for prior cases with similar appearance and their confirmed diagnoses."),
  pat("XP-1199034", "B60L", 2022, "Voltaic Motors", "Battery pack thermal runaway early warning",
    "Cell gas sensors and impedance spectroscopy feed a model that predicts thermal runaway minutes before onset and isolates affected modules."),
  pat("XP-1236612", "B60L", 2024, "Gridline Energy", "Bidirectional EV charging scheduler",
    "A scheduler decides when a parked EV charges or exports to the grid based on tariff forecasts and the driver's next departure time."),
  pat("XP-1247001", "G06F", 2024, "Orbis Data", "SQL predicate pushdown into vector search",
    "A query planner rewrites SQL with a similarity predicate so metadata filters are evaluated during graph traversal rather than after retrieval."),
];

function PriorArtInsight({ hits }: { hits: UseCaseHit[] }) {
  if (hits.length === 0) return <p className="text-sm text-gray-500 dark:text-muted">No references in range.</p>;
  const top = hits.slice(0, 3);
  const novelty = 1 - top[0].sim;
  const band = top[0].sim >= 0.8 ? "low" : top[0].sim >= 0.65 ? "medium" : "high";
  const byCpc = hits.reduce<Record<string, number>>((acc, h) => {
    const k = String(h.meta.cpc ?? "?");
    acc[k] = (acc[k] ?? 0) + 1;
    return acc;
  }, {});
  return (
    <div className="space-y-3">
      <div
        className={`rounded-md px-3 py-2 text-center text-sm font-semibold uppercase ${
          band === "low" ? "bg-bad/15 text-bad" : band === "medium" ? "bg-warn/15 text-warn" : "bg-ok/15 text-ok"
        }`}
      >
        {band} novelty signal
      </div>
      <div className="flex flex-wrap gap-3">
        <Stat label="1 − max sim" value={novelty.toFixed(3)} />
        <Stat label="refs scanned" value={hits.length} />
      </div>
      <div>
        <div className="eyebrow mb-1">Closest references to review first</div>
        <ol className="space-y-1.5 text-sm list-decimal pl-5">
          {top.map((h) => (
            <li key={h.id}>
              <span className="font-medium">{h.id}</span> — {h.title}{" "}
              <span className="text-xs text-gray-500 dark:text-muted">
                ({String(h.meta.assignee ?? "")}, {String(h.meta.year ?? "")}) · sim {h.sim.toFixed(3)}
              </span>
            </li>
          ))}
        </ol>
      </div>
      <div>
        <div className="eyebrow mb-1">CPC spread of results</div>
        <div className="flex flex-wrap gap-2">
          {Object.entries(byCpc).map(([k, n]) => (
            <Stat key={k} label={k} value={n} />
          ))}
        </div>
      </div>
      <p className="text-xs text-gray-500 dark:text-muted">
        A screening aid to prioritise reading, not a patentability opinion. Bands use raw cosine similarity
        (≥0.80 low, ≥0.65 medium) and depend on the embedding model.
      </p>
    </div>
  );
}

export const priorArtSpec: UseCaseSpec = {
  bucket: "patent_abstracts",
  seedVersion: "priorart-v1",
  docs: DOCS,
  noun: "patents",
  title: "Prior-Art Search",
  subtitle: "Invention disclosure → nearest patent abstracts (optionally within a CPC class / after a year) → novelty signal and reading list. No LLM.",
  queryLabel: "Invention disclosure",
  placeholder: "Describe the invention in a few sentences…",
  scenarios: [
    { label: "LLM response cache", q: "Store embeddings of user prompts with model answers and reuse an answer when a new prompt is close enough, skipping the model call." },
    { label: "Filtered vector search", q: "Apply SQL WHERE filters on metadata while traversing an HNSW graph instead of filtering afterwards.", filters: { cpc: "G06F" } },
    { label: "EV battery safety", q: "Detect lithium battery cell venting early using gas sensors and cut off the module.", filters: { cpc: "B60L" } },
    { label: "Novel: soil sensor", q: "Low-cost soil moisture probe that uses LoRa to schedule irrigation for vineyards." },
  ],
  filters: [
    { key: "cpc", label: "CPC class", kind: "select", options: CPC },
    { key: "year", label: "Filed from year", kind: "min" },
  ],
  limit: 8,
  idKey: "patent_id",
  titleKey: "title",
  subKeys: ["cpc", "assignee", "year"],
  resultsTitle: "Nearest prior art",
  Insight: PriorArtInsight,
  insightTitle: "Novelty screen",
  insightSubtitle: "Deterministic — based on nearest-neighbour distance",
};
