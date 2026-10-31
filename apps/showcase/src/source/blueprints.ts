/**
 * Production-architecture blueprints for the Source Supplies apps,
 * merged into BLUEPRINTS in components/RagArchitecture.tsx.
 */
import type { RagBlueprint } from "../components/RagArchitecture";

const TRADE_ACCOUNT = { title: "Trade account", sub: "contract prices,\nsites, orders" };

export const SOURCE_BLUEPRINTS: Record<string, RagBlueprint> = {
  ss_finder: {
    id: "ss_finder",
    label: "Source Supplies · Green Product Finder",
    pitch: "A buyer says what they need; one SQL query mixes meaning with hard filters, then greener swaps are priced at their real order volume.",
    client: { title: "Buyer / supervisor", sub: "trade account, phone" },
    inputGuard: "off-topic\nprompt injection",
    cache: "common product\nquestions",
    router: "find product · swap\nprice · stock",
    outputGuard: "no invented badges,\nprices ex VAT",
    answer: "shortlist + dilution\n+ pack, cited",
    rag: "semantic_match +\nbadge/sector filters",
    buckets: "ss_products · ss_orders",
    ingest: { title: "Catalogue feed", sub: "products, badges, SDS" },
    cdc: "price / stock\nchanges",
    domainDb: { title: "ERP / stock", sub: "stock, lead times" },
    domainApi: TRADE_ACCOUNT,
    hitl: { title: "Buyer", sub: "adds to basket" },
    intents: [
      { intent: "What should I use for…?", route: "execute_sql: semantic_match + array_contains(badges, …) + in_stock + price_gbp <= …" },
      { intent: "Is there a greener option?", route: "execute_sql: sku IN (replaces_sku …) and the account's ss_orders volume" },
      { intent: "Is it in stock?", route: "stock comes from metadata synced from the ERP, never from the model" },
      { intent: "Order it", route: "the buyer adds it to the basket; the assistant never orders" },
    ],
    notes: [
      "Filters run after retrieval over LIMIT × 4 candidates, so the finder uses a LIMIT that covers the whole products bucket.",
      "Near misses come from the same meaning with no filters, each labelled with the filter it failed.",
    ],
  },
  ss_safety: {
    id: "ss_safety",
    label: "Source Supplies · COSHH & Safety Data Assistant",
    pitch: "A cleaner asks about a product's safety data sheet; hard rules in code handle emergencies, mixing and gaps before any model runs.",
    client: { title: "Cleaner / site manager", sub: "phone on site" },
    inputGuard: "emergency words →\nfixed first-aid card",
    cache: "none: always\nthe live SDS",
    router: "PPE · storage · spill\nmixing · first aid",
    outputGuard: "cite the SDS section;\n'not in the SDS'",
    answer: "answer + Section n\ncitations",
    rag: "SDS sections by\nsku + section IN (…)",
    buckets: "ss_sds · ss_guides",
    ingest: { title: "SDS ingestion", sub: "one doc per section" },
    cdc: "new SDS\nrevisions",
    domainDb: { title: "Product master", sub: "SKU, SDS revision" },
    domainApi: { title: "COSHH register", sub: "site assessments" },
    hitl: { title: "Competent person", sub: "signs off COSHH" },
    intents: [
      { intent: "Swallowed / in eyes / can't breathe", route: "code: fixed Section 4 card + 'Call 999 … NHS 111' before any model text" },
      { intent: "Can I mix…?", route: "code: 'Do not mix products unless the SDS for both says it is safe' + Section 10" },
      { intent: "Which PPE / how to store…?", route: "execute_sql on ss_sds: sku = … AND section IN (8) → grounded answer" },
      { intent: "Not covered by the SDS", route: "code: 'This is not in the safety data sheet.' and stop" },
      { intent: "COSHH assessment", route: "draft from cited sections, marked for a competent person to check" },
    ],
    notes: ["The safety rules are unit-tested code (source/safety.test.ts), not prompt text."],
  },
};
