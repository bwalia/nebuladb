/**
 * Everything the Source Supplies customer site lists: the demos, what
 * each one fixes (in Dave's words where we have them), the measurable win,
 * the video tour, and search keywords. One list drives the nav, the home
 * page teasers, the video gallery and search.
 */

export type DemoId =
  | "ssretail"
  | "ssstock"
  | "ssreports"
  | "ssemail"
  | "ssreorder"
  | "ssfinder"
  | "sssafety"
  | "sssolver"
  | "sswashroom"
  | "sstender"
  | "labs";

export interface Video {
  src: string;
  poster: string;
  /** mm:ss */
  length: string;
  title: string;
  narrated: boolean;
}

export interface Demo {
  id: DemoId;
  name: string;
  icon: string;
  group: "top3" | "ops" | "sales" | "explore";
  /** The pain, in one line. */
  problem: string;
  /** What Jordan does, in one line. */
  does: string;
  /** The headline win (shown big). */
  win: string;
  winLabel: string;
  who: string;
  quote?: string;
  video?: Video;
  keywords: string;
}

const V = "/tours/source-supplies";
const video = (file: string, length: string, title: string, narrated = true): Video => ({
  src: `${V}/${file}.mp4`,
  poster: `${V}/posters/${file}.jpg`,
  length,
  title,
  narrated,
});

export const DEMOS: Demo[] = [
  {
    id: "ssretail",
    name: "Retail POD Autopilot",
    icon: "🚚",
    group: "top3",
    problem: "~600 retail portal orders a month keyed into Merlin by hand, then two proofs of delivery found and uploaded for every one.",
    does: "Drafts the Merlin orders, matches the Merlin scan and the courier photo for each delivery, attaches both, and only asks a person about real exceptions.",
    win: "≈ £22k",
    winLabel: "a year back · 87% need no person",
    who: "Order desk",
    quote: "That whole process… it's probably costing 20 grand a year.",
    video: video("retail-pod-autopilot", "2:10", "Retail POD Autopilot — the full tour"),
    keywords: "tk maxx retailer portal service channel pod proof of delivery courier dx fedex merlin sales order keying exceptions learning",
  },
  {
    id: "ssstock",
    name: "Stock Autopilot",
    icon: "📦",
    group: "top3",
    problem: "No live stock in Merlin, popular lines still run out, and POs are drafted from averages pasted into ChatGPT.",
    does: "Forecasts every core line each night with seasons, growth, new contracts and one-offs, then drafts one PO per supplier to approve.",
    win: "−60%",
    winLabel: "stock-outs, with less cash in stock",
    who: "Warehouse and buying",
    quote: "It's cleverer than just looking at an average of 3 months.",
    video: video("stock-autopilot", "1:53", "Stock Autopilot — the full tour"),
    keywords: "stock control inventory forecast reorder purchase order po supplier seasonality christmas run out stockout safety stock",
  },
  {
    id: "ssreports",
    name: "Jordan Report Studio",
    icon: "📊",
    group: "top3",
    problem: "Customer reports wait weeks for Merlin custom-report tickets, or someone who writes SQL.",
    does: "Ask in plain English; Jordan writes the query, builds the report with insights, exports to Excel and schedules it monthly.",
    win: "Weeks → secs",
    winLabel: "per customer report, no SQL",
    who: "Account managers and ops",
    quote: "Third attempt… three or four days apart.",
    video: video("report-studio", "1:34", "Jordan Report Studio — the full tour"),
    keywords: "reports reporting customer spend sql merlin excel csv email schedule quarterly by site insights",
  },
  {
    id: "ssemail",
    name: "Email Order Intake",
    icon: "✉️",
    group: "ops",
    problem: "The orders that still arrive by email are re-keyed into Merlin by hand.",
    does: "Reads the email, matches each line to the catalogue and drafts the Merlin sales order; weak matches stay with a person.",
    win: "~10%",
    winLabel: "of orders no longer re-keyed",
    who: "Order desk",
    video: video("email-order", "0:34", "Email Order Intake — teaser"),
    keywords: "email order inbox re-key merlin sales order parse catalogue match",
  },
  {
    id: "ssreorder",
    name: "Smart Reorder",
    icon: "↻",
    group: "ops",
    problem: "Multi-site customers re-key the same order every month, site by site.",
    does: "“Same as last month for North site, but recycled towels” → a forecast basket, greener swaps and a delivery-day check.",
    win: "10 min",
    winLabel: "saved per site per month",
    who: "FM and contract-cleaning buyers",
    video: video("smart-reorder", "0:35", "Smart Reorder — teaser"),
    keywords: "reorder repeat order same as last month basket multi-site delivery day swap",
  },
  {
    id: "sswashroom",
    name: "Washroom Service Desk",
    icon: "🧻",
    group: "ops",
    problem: "Washroom issues need the right priority, a missed-visit check and a quick reply.",
    does: "Finds similar past jobs, flags missed visits against the contract cycle, drafts the reply and helps quote new sites.",
    win: "10 min",
    winLabel: "saved per ticket",
    who: "Source Washroom team",
    video: video("washroom-desk", "0:40", "Washroom Service Desk — teaser"),
    keywords: "washroom sanitary bins air freshener big change visit ticket missed visit quote",
  },
  {
    id: "ssfinder",
    name: "Green Product Finder",
    icon: "🌿",
    group: "sales",
    problem: "Buyers ask “what should I use?” and want the greener choice that's in stock and on budget.",
    does: "One search mixes meaning with eco badges, sector, stock and price, then prices greener swaps at the customer's real volume.",
    win: "15 min",
    winLabel: "saved per product question",
    who: "Buyers and supervisors",
    video: video("green-finder", "0:38", "Green Product Finder — teaser"),
    keywords: "product finder eco green sustainable badges nordic swan ecolabel swap price stock",
  },
  {
    id: "sssafety",
    name: "COSHH & Safety Data",
    icon: "⚠️",
    group: "sales",
    problem: "Cleaners need safety answers from the data sheet on site, fast and correct.",
    does: "Answers from the SDS with section citations; emergencies and product-mixing questions get fixed safe answers, never model text.",
    win: "20 min",
    winLabel: "saved per COSHH draft",
    who: "Cleaners and site managers",
    video: video("coshh-safety", "0:39", "COSHH & Safety Data — teaser"),
    keywords: "coshh safety data sheet sds ppe first aid mixing chemicals compliance",
  },
  {
    id: "sssolver",
    name: "Cleaning Problem Solver",
    icon: "✨",
    group: "sales",
    problem: "Tough jobs — mould, gum, smells, worn floors — need a method and the right kit.",
    does: "Finds the right guide for the surface and sector, lists the kit with stock, and drafts a quick order.",
    win: "30 min",
    winLabel: "saved per problem call",
    who: "Account managers",
    video: video("problem-solver", "0:32", "Cleaning Problem Solver — teaser"),
    keywords: "cleaning problem mould gum odour floor method kit guide",
  },
  {
    id: "sstender",
    name: "Tender Responder",
    icon: "📝",
    group: "sales",
    problem: "School, NHS and council tenders and big price lists ask the same questions in new words.",
    does: "Finds evidence for every question, scores coverage, drafts cited answers and exports them.",
    win: "½ day",
    winLabel: "saved per tender",
    who: "Sales and bid writers",
    video: video("tender-responder", "0:30", "Tender Responder — teaser"),
    keywords: "tender questionnaire bid nhs council school sustainability carbon iso social value price list quote",
  },
  {
    id: "labs",
    name: "AI Labs on your data",
    icon: "🧪",
    group: "explore",
    problem: "How does the AI actually find things, and when should it refuse?",
    does: "Eight hands-on experiments on the Source Supplies demo data: search by meaning, filters, step-by-step answers, when to say no.",
    win: "8 labs",
    winLabel: "2–10 minutes each",
    who: "Anyone curious",
    video: video("labs-source", "0:28", "AI Labs on your data — teaser"),
    keywords: "labs experiments semantic search vector rag filters how it works learn",
  },
];

export const DEMO_BY_ID = Object.fromEntries(DEMOS.map((d) => [d.id, d])) as Record<DemoId, Demo>;

export const GROUPS: { id: Demo["group"]; label: string }[] = [
  { id: "top3", label: "Your top 3" },
  { id: "ops", label: "Orders & operations" },
  { id: "sales", label: "Sales & compliance" },
  { id: "explore", label: "Explore" },
];

export const COMBINED = video("source-supplies-top3", "5:38", "All three top-3 tours in one");

/** Plain search over name, problem, outcome and keywords; every word must match. */
export function searchDemos(q: string): Demo[] {
  const words = q.toLowerCase().split(/\s+/).filter(Boolean);
  if (!words.length) return DEMOS;
  return DEMOS.filter((d) => {
    const hay = `${d.name} ${d.problem} ${d.does} ${d.who} ${d.winLabel} ${d.keywords}`.toLowerCase();
    return words.every((w) => hay.includes(w));
  });
}
