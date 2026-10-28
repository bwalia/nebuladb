/**
 * Earnings Analyst — research over earnings-call transcript excerpts,
 * filtered by ticker and quarter, with a cited analyst chat.
 * Companies, tickers and figures are invented.
 */
import type { SeedDoc } from "../../rag/seed";
import type { UseCaseSpec } from "../UseCaseDemo";

const TICKERS = ["HLX", "BRCK", "VNTA"] as const;
const QUARTERS = ["2026-Q1", "2026-Q2", "2026-Q3"] as const;

function ex(id: string, ticker: string, company: string, quarter: string, speaker: string, section: string, body: string): SeedDoc {
  return {
    id,
    text: `${id} · ${company} (${ticker}) ${quarter} call · ${speaker} · ${section}\n${body}`,
    metadata: { excerpt_id: id, ticker, company, quarter, speaker, section },
  };
}

const DOCS: SeedDoc[] = [
  ex("HLX-Q1-1", "HLX", "Helix Cloud", "2026-Q1", "CFO", "prepared remarks",
    "Revenue grew 21% year over year to $412M. Net revenue retention was 118%. Gross margin dipped 150 basis points as we pre-bought GPU capacity for inference workloads."),
  ex("HLX-Q2-1", "HLX", "Helix Cloud", "2026-Q2", "CEO", "prepared remarks",
    "AI inference is now 19% of new bookings. We launched a managed vector search tier and signed three seven-figure contracts with European banks needing EU data residency."),
  ex("HLX-Q2-2", "HLX", "Helix Cloud", "2026-Q2", "CFO", "guidance",
    "We are raising full-year revenue guidance to $1.74–1.76B. Gross margin should recover to 72% by Q4 as GPU utilisation improves from 54% to the high 60s."),
  ex("HLX-Q3-1", "HLX", "Helix Cloud", "2026-Q3", "CFO", "prepared remarks",
    "Revenue of $455M, up 23%. GPU utilisation reached 66% and gross margin recovered to 71.2%. Free cash flow margin was 24%."),
  ex("HLX-Q3-2", "HLX", "Helix Cloud", "2026-Q3", "CEO", "Q&A",
    "Asked about pricing pressure, the CEO said committed-use discounts deepened in large deals, but usage expansion more than offset it. Churn in the SMB segment ticked up slightly."),
  ex("BRCK-Q1-1", "BRCK", "Bricklane Retail", "2026-Q1", "CEO", "prepared remarks",
    "Like-for-like sales fell 2.1% due to warm weather hurting outerwear. Online grew 9% and now represents 34% of sales. We closed 11 underperforming stores."),
  ex("BRCK-Q2-1", "BRCK", "Bricklane Retail", "2026-Q2", "CFO", "prepared remarks",
    "Gross margin improved 80 basis points from lower freight costs. Inventory is down 12% year over year. We expect a £14M one-off charge from the warehouse consolidation."),
  ex("BRCK-Q3-1", "BRCK", "Bricklane Retail", "2026-Q3", "CEO", "Q&A",
    "On AI, management said the new semantic product search lifted online conversion by 6% and reduced 'no results' searches by 40%. Returns rate fell from 27% to 24% after adding size recommendations."),
  ex("BRCK-Q3-2", "BRCK", "Bricklane Retail", "2026-Q3", "CFO", "guidance",
    "We maintain full-year profit guidance of £180–190M. Christmas trading is the swing factor; we are cautious on consumer confidence."),
  ex("VNTA-Q1-1", "VNTA", "Vanta Therapeutics", "2026-Q1", "CEO", "prepared remarks",
    "Phase II results for VT-204 in moderate asthma met the primary endpoint. We plan to start Phase III in early 2027 subject to regulatory feedback."),
  ex("VNTA-Q2-1", "VNTA", "Vanta Therapeutics", "2026-Q2", "CFO", "prepared remarks",
    "Cash and equivalents of $610M give us runway into 2029. R&D spend rose 31% as we expanded manufacturing for the Phase III supply."),
  ex("VNTA-Q3-1", "VNTA", "Vanta Therapeutics", "2026-Q3", "CEO", "Q&A",
    "An analyst asked about the FDA end-of-phase-II meeting; management said the agency agreed on the primary endpoint but asked for a longer safety follow-up, which adds roughly six months to the timeline."),
];

export const earningsSpec: UseCaseSpec = {
  bucket: "earnings_calls",
  seedVersion: "earnings-v1",
  docs: DOCS,
  noun: "excerpts",
  title: "Earnings Analyst",
  subtitle: "Analyst question → transcript excerpts filtered by ticker / quarter → cited research notes. Fictional companies.",
  queryLabel: "Research question",
  placeholder: "Ask about margins, guidance, AI revenue, risks…",
  scenarios: [
    { label: "Helix margin story", q: "How did gross margin and GPU utilisation trend across the year?", filters: { ticker: "HLX" } },
    { label: "AI impact on retail", q: "What measurable impact did AI search have on sales?", filters: { ticker: "BRCK" } },
    { label: "Biotech timeline risk", q: "What is the risk to the Phase III start date?", filters: { ticker: "VNTA" } },
    { label: "Q3 guidance across names", q: "What did management say about full-year guidance?", filters: { quarter: "2026-Q3" } },
  ],
  filters: [
    { key: "ticker", label: "Ticker", kind: "select", options: TICKERS },
    { key: "quarter", label: "Quarter", kind: "select", options: QUARTERS },
  ],
  idKey: "excerpt_id",
  titleKey: "company",
  subKeys: ["quarter", "speaker", "section"],
  resultsTitle: "Transcript excerpts",
  chat: {
    title: "Research assistant",
    subtitle: "Every figure is cited to a transcript excerpt",
    system:
      "You are an equity research assistant. Answer ONLY from the cited transcript excerpts [n]; quote figures exactly and attribute them to the speaker and quarter. " +
      "Separate facts from management commentary. Never give investment advice or price targets.",
    suggestions: ["Write a 5-bullet research note.", "What are the bear-case risks?", "Build a quarter-by-quarter table."],
    placeholder: "Ask a follow-up on these calls…",
    topK: 8,
  },
};
