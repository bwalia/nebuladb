/**
 * Reference architecture for a production RAG application on NebulaDB.
 *
 * One diagram, re-labelled per use case: client → gateway → input
 * guardrails → semantic cache → router → output guardrails → answer,
 * with RAG retrieval, ingestion + CDC, the domain system of record, a
 * domain API, the LLM and a human-in-the-loop hanging off the router.
 *
 * Boxes are tinted by who provides them: NebulaDB (accent), your
 * application (neutral) or a human (amber). Guardrails, routing and the
 * domain systems are yours; NebulaDB does not pretend to ship them.
 */
import { useState } from "react";

export type Owner = "nebula" | "app" | "human";

interface Node {
  title: string;
  /** Up to two short lines, separated by "\n". */
  sub: string;
}

export interface RagBlueprint {
  id: string;
  label: string;
  /** One-line summary of what the app does. */
  pitch: string;
  client: Node;
  inputGuard: string;
  cache: string;
  router: string;
  outputGuard: string;
  answer: string;
  rag: string;
  /** Buckets this app reads, shown on the NebulaDB node. */
  buckets: string;
  ingest: Node;
  cdc: string;
  domainDb: Node;
  domainApi: Node;
  hitl: Node;
  /** Intents the router recognises, and what each one calls. */
  intents: { intent: string; route: string }[];
  /** Use-case specific notes worth calling out. */
  notes?: string[];
}

const GATEWAY: Node = { title: "API gateway", sub: "bearer keys\nrate limit" };
const LLM: Node = { title: "LLM", sub: "grounded on\ncited context" };

export const BLUEPRINTS: Record<string, RagBlueprint> = {
  shop: {
    id: "shop",
    label: "Online shopping",
    pitch: "A shopping assistant that answers from the live catalogue and stock levels, never from memory.",
    client: { title: "Shopper", sub: "mobile, web chat" },
    inputGuard: "prompt injection\noff-topic",
    cache: "repeat questions\nanswered instantly",
    router: "product Q · stock\norder status",
    outputGuard: "no PII, no\ninvented prices",
    answer: "answer + product\nlinks",
    rag: "catalogue\nretrieval",
    buckets: "shop_catalog · shop_stock_levels",
    ingest: { title: "Catalogue ingestion", sub: "product feed, spec PDFs" },
    cdc: "price / stock\nchanges",
    domainDb: { title: "Commerce DB", sub: "orders, stock" },
    domainApi: { title: "Orders API", sub: "status, returns" },
    hitl: { title: "Store associate", sub: "refunds, edge cases" },
    intents: [
      { intent: "Product question", route: "semantic_search on shop_catalog, then answer_question" },
      { intent: "Is it in stock?", route: "execute_sql on shop_stock_levels (exact, never generated)" },
      { intent: "Where is my order?", route: "Orders API after the shopper is authenticated" },
      { intent: "Refund or complaint", route: "hand off to a store associate" },
    ],
    notes: [
      "Prices and stock come from SQL on the system of record, so the LLM never states a number it was not given.",
      "CDC on price and stock writes evicts stale cached answers the moment the catalogue changes.",
    ],
  },
  insurance: {
    id: "insurance",
    label: "Insurance claims",
    pitch: "Claims triage that grounds every answer in the policy wording and the claim file.",
    client: { title: "Policyholder", sub: "app, claims handler" },
    inputGuard: "fraud signals\nprompt injection",
    cache: "common coverage\nquestions",
    router: "coverage Q · new claim\nclaim status",
    outputGuard: "no PII, no\ncoverage promises",
    answer: "answer + clause\ncitations",
    rag: "policy + claim\nretrieval",
    buckets: "policy_docs · insurance_claims",
    ingest: { title: "Document ingestion", sub: "wordings, claim notes" },
    cdc: "claim status\nupdates",
    domainDb: { title: "Policy admin", sub: "policies, cover limits" },
    domainApi: { title: "Claims API", sub: "FNOL, fraud score" },
    hitl: { title: "Claims handler", sub: "approves payouts" },
    intents: [
      { intent: "Am I covered for…?", route: "semantic_search on policy_docs filtered to the customer's product" },
      { intent: "Report a claim", route: "Claims API (FNOL), then insert_document for the claim notes" },
      { intent: "Similar past claims", route: "execute_sql with semantic_match on insurance_claims + filters" },
      { intent: "Payout decision", route: "always a claims handler" },
    ],
    notes: [
      "The assistant explains cover with clause citations; a person makes every settlement decision.",
    ],
  },
  legal: {
    id: "legal",
    label: "Legal search",
    pitch: "Clause and precedent search across contracts, the firm playbook and case law.",
    client: { title: "Lawyer", sub: "paralegal, in-house" },
    inputGuard: "matter access\nchecks",
    cache: "playbook\nanswers",
    router: "clause search · risk\nreview · precedent",
    outputGuard: "privilege, no\nadvice to public",
    answer: "answer + clause\ncitations",
    rag: "contract + playbook\nretrieval",
    buckets: "legal_playbook · legal_contract_*",
    ingest: { title: "Document ingestion", sub: "contracts, case law" },
    cdc: "new versions,\nredlines",
    domainDb: { title: "Matter management", sub: "DMS, clients" },
    domainApi: { title: "Courts API", sub: "statute, judgments" },
    hitl: { title: "Solicitor", sub: "signs off advice" },
    intents: [
      { intent: "Find a clause", route: "semantic_search with hybrid: true (exact terms matter in law)" },
      { intent: "Review against playbook", route: "answer_question over legal_playbook + the contract bucket" },
      { intent: "Precedent", route: "Courts API, then semantic_search on ingested judgments" },
      { intent: "Advice to a client", route: "drafted for a solicitor to approve" },
    ],
    notes: ["One bucket per contract keeps matters isolated and makes per-matter access control simple."],
  },
  property: {
    id: "property",
    label: "Estate agents",
    pitch: "Match buyers to homes for sale from a plain-English brief plus hard filters.",
    client: { title: "Home buyer", sub: "portal, negotiator" },
    inputGuard: "no protected-\ncharacteristic filters",
    cache: "popular\nsearches",
    router: "match homes · compare\nbook viewing",
    outputGuard: "no PII, accurate\nprice and EPC",
    answer: "shortlist +\ncomparison",
    rag: "listing\nmatching",
    buckets: "property_listings · applicants",
    ingest: { title: "Listing ingestion", sub: "descriptions, EPCs" },
    cdc: "price changes,\nsold STC",
    domainDb: { title: "Agency CRM", sub: "applicants, viewings" },
    domainApi: { title: "Portals API", sub: "Land Registry prices" },
    hitl: { title: "Negotiator", sub: "books viewings" },
    intents: [
      { intent: "Find me a home like…", route: "semantic_match + price_gbp <= and bedrooms >= in one SQL" },
      { intent: "New listing → who wants it?", route: "reverse match: the listing text against saved applicant briefs" },
      { intent: "Compare these three", route: "answer_question over the shortlist" },
      { intent: "Book a viewing", route: "Agency CRM, confirmed by a negotiator" },
    ],
    notes: [
      "Matching runs both ways: buyers find homes, and each new instruction finds its likely buyers.",
      "CDC on 'sold STC' removes a home from every cached shortlist straight away.",
    ],
  },
  tax: {
    id: "tax",
    label: "HMRC Self Assessment",
    pitch: "Categorise every bank transaction into Self Assessment expense categories and prepare the return.",
    client: { title: "Sole trader", sub: "landlord, accountant" },
    inputGuard: "account number\nredaction",
    cache: "merchant →\ncategory",
    router: "categorise · allowable?\nprepare return",
    outputGuard: "no account numbers,\ntotals reconcile",
    answer: "draft return +\nevidence",
    rag: "HMRC rules +\npast txns",
    buckets: "bank_transactions · hmrc_guidance",
    ingest: { title: "Feed ingestion", sub: "bank feeds, receipts" },
    cdc: "new transactions\nre-categorised",
    domainDb: { title: "Ledger", sub: "categorised txns" },
    domainApi: { title: "Open Banking", sub: "HMRC MTD API" },
    hitl: { title: "Taxpayer", sub: "or accountant,\napproves submit" },
    intents: [
      { intent: "Categorise a transaction", route: "nearest already-categorised transactions (semantic_match on the description) → category" },
      { intent: "Is this allowable?", route: "answer_question over ingested HMRC guidance, with citations" },
      { intent: "Prepare the return", route: "totals per category summed in the ledger (the system of record), never by the LLM" },
      { intent: "Submit", route: "only after the taxpayer or accountant approves" },
    ],
    notes: [
      "Totals are sums over the ledger's categorised rows, so the figures on the return always reconcile to the bank feed.",
      "Low-confidence matches (a new merchant, a mixed personal/business payment) go to the human queue instead of being guessed.",
      "Automation does the categorising and the arithmetic; the taxpayer still declares the return, so the submit step stays human.",
    ],
  },
  support: {
    id: "support",
    label: "Support desk",
    pitch: "Answer tickets from the knowledge base and past resolutions.",
    client: { title: "Customer", sub: "email, chat" },
    inputGuard: "abuse, prompt\ninjection",
    cache: "FAQ\nanswers",
    router: "how-to · bug\naccount",
    outputGuard: "no PII, no\nunsupported claims",
    answer: "reply + KB\nlinks",
    rag: "KB + ticket\nretrieval",
    buckets: "support_kb · support_tickets",
    ingest: { title: "KB ingestion", sub: "articles, resolved tickets" },
    cdc: "KB edits",
    domainDb: { title: "Helpdesk", sub: "tickets, SLAs" },
    domainApi: { title: "Account API", sub: "plan, usage" },
    hitl: { title: "Support agent", sub: "escalations" },
    intents: [
      { intent: "How do I…?", route: "answer_question over support_kb" },
      { intent: "Seen this before?", route: "semantic_search on support_tickets" },
      { intent: "Escalate", route: "support agent" },
    ],
  },
  talent: {
    id: "talent",
    label: "Talent matching",
    pitch: "Match candidates to roles on skills and experience, not keywords.",
    client: { title: "Recruiter", sub: "hiring manager" },
    inputGuard: "no protected-\ncharacteristic filters",
    cache: "role\nsearches",
    router: "match · compare\nshortlist",
    outputGuard: "no PII leakage,\nexplain matches",
    answer: "shortlist +\nreasons",
    rag: "candidate\nmatching",
    buckets: "talent_candidates · talent_jobs",
    ingest: { title: "CV ingestion", sub: "CVs, job specs" },
    cdc: "candidate\nupdates",
    domainDb: { title: "ATS", sub: "pipelines" },
    domainApi: { title: "Job boards", sub: "postings" },
    hitl: { title: "Recruiter", sub: "decides" },
    intents: [
      { intent: "Who fits this role?", route: "semantic_match of the job spec on talent_candidates" },
      { intent: "Compare finalists", route: "answer_question over the shortlist" },
    ],
  },
  incidents: {
    id: "incidents",
    label: "Incident copilot",
    pitch: "Pull the right runbook and similar past incidents during an outage.",
    client: { title: "On-call engineer", sub: "Slack, console" },
    inputGuard: "secrets\nredaction",
    cache: "known\nalerts",
    router: "triage · runbook\nsimilar incidents",
    outputGuard: "no secrets,\ncite runbooks",
    answer: "next steps +\nrunbook",
    rag: "runbook\nretrieval",
    buckets: "sre_runbooks · sre_incidents",
    ingest: { title: "Runbook ingestion", sub: "runbooks, postmortems" },
    cdc: "runbook edits",
    domainDb: { title: "Incident tracker", sub: "timeline" },
    domainApi: { title: "Monitoring API", sub: "alerts, metrics" },
    hitl: { title: "Incident lead", sub: "approves actions" },
    intents: [
      { intent: "What is this alert?", route: "semantic_search on sre_runbooks" },
      { intent: "Happened before?", route: "semantic_search on sre_incidents" },
    ],
  },
  policy: {
    id: "policy",
    label: "Policy desk",
    pitch: "Answer staff questions from internal policies, with the clause cited.",
    client: { title: "Employee", sub: "intranet chat" },
    inputGuard: "prompt\ninjection",
    cache: "common\nquestions",
    router: "policy Q\nwho to ask",
    outputGuard: "no personal\ndata",
    answer: "answer +\ncitation",
    rag: "policy\nretrieval",
    buckets: "policy_docs",
    ingest: { title: "Policy ingestion", sub: "handbooks, PDFs" },
    cdc: "policy\nrevisions",
    domainDb: { title: "HR system", sub: "entitlements" },
    domainApi: { title: "HR API", sub: "leave balances" },
    hitl: { title: "HR partner", sub: "exceptions" },
    intents: [{ intent: "What does the policy say?", route: "answer_question over policy_docs" }],
  },
  compliance: {
    id: "compliance",
    label: "Compliance",
    pitch: "Map regulatory obligations to internal controls and evidence.",
    client: { title: "Compliance team", sub: "risk, audit" },
    inputGuard: "access\nchecks",
    cache: "obligation\nlookups",
    router: "obligation · control\ngap",
    outputGuard: "cite the\nregulation",
    answer: "mapping +\ngaps",
    rag: "obligation\nretrieval",
    buckets: "compliance_obligations",
    ingest: { title: "Rulebook ingestion", sub: "regulations, controls" },
    cdc: "rule changes",
    domainDb: { title: "GRC system", sub: "controls, evidence" },
    domainApi: { title: "Regulator feeds", sub: "updates" },
    hitl: { title: "Compliance officer", sub: "attests" },
    intents: [{ intent: "Which obligations apply?", route: "semantic_match on compliance_obligations + filters" }],
  },
  clinical: {
    id: "clinical",
    label: "Clinical guidelines",
    pitch: "Surface the relevant guideline for a clinician, with the source cited.",
    client: { title: "Clinician", sub: "ward, GP" },
    inputGuard: "patient-data\nredaction",
    cache: "common\nlookups",
    router: "guideline Q\ndosing ref",
    outputGuard: "cite source,\nno diagnosis",
    answer: "guideline +\ncitation",
    rag: "guideline\nretrieval",
    buckets: "clinical_guidelines",
    ingest: { title: "Guideline ingestion", sub: "approved guidance" },
    cdc: "guideline\nupdates",
    domainDb: { title: "EHR", sub: "read-only" },
    domainApi: { title: "Formulary API", sub: "medicines" },
    hitl: { title: "Clinician", sub: "decides care" },
    intents: [{ intent: "What does the guideline say?", route: "answer_question over clinical_guidelines" }],
    notes: ["Decision support only: the clinician makes every care decision."],
  },
  fieldservice: {
    id: "fieldservice",
    label: "Field service",
    pitch: "Give engineers the fix for a fault from manuals and past jobs.",
    client: { title: "Field engineer", sub: "tablet, van" },
    inputGuard: "prompt\ninjection",
    cache: "common\nfaults",
    router: "fault · part\nprocedure",
    outputGuard: "safety\nwarnings kept",
    answer: "fix steps +\nparts",
    rag: "manual + job\nretrieval",
    buckets: "field_service",
    ingest: { title: "Manual ingestion", sub: "manuals, job notes" },
    cdc: "new job notes",
    domainDb: { title: "Job system", sub: "assets, history" },
    domainApi: { title: "Parts API", sub: "stock, ordering" },
    hitl: { title: "Supervisor", sub: "safety sign-off" },
    intents: [{ intent: "How do I fix…?", route: "semantic_match on field_service + asset filters" }],
  },
};

export const GUIDE_HREF = "#/blog/rag-apps-quickly-with-nebula-mcp";

/** The use cases featured in the setup guide's switcher. */
export const GUIDE_BLUEPRINTS = ["shop", "insurance", "legal", "property", "tax"] as const;

const OWNER_CLS: Record<Owner, string> = {
  nebula: "fill-accent/10 stroke-accent",
  app: "fill-white stroke-gray-300 dark:fill-carbon-900 dark:stroke-edge",
  human: "fill-warn/10 stroke-warn",
};

function Box({
  x, y, w, h = 58, title, sub, owner = "app",
}: { x: number; y: number; w: number; h?: number; title: string; sub?: string; owner?: Owner }) {
  const lines = (sub ?? "").split("\n").filter(Boolean);
  const top = y + h / 2 - (lines.length * 13) / 2 + (lines.length ? -1 : 4);
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx="9" className={OWNER_CLS[owner]} strokeWidth="1.3" />
      <text x={x + w / 2} y={top} textAnchor="middle" fontSize="12" fontWeight="600" className="fill-gray-900 dark:fill-ink">
        {title}
      </text>
      {lines.map((l, i) => (
        <text key={i} x={x + w / 2} y={top + 14 + i * 12.5} textAnchor="middle" fontSize="10" className="fill-gray-500 dark:fill-muted">
          {l}
        </text>
      ))}
    </g>
  );
}

export function RagArchitectureDiagram({ bp }: { bp: RagBlueprint }) {
  const line = "stroke-gray-400 dark:stroke-faint";
  const mk = `rag-arr-${bp.id}`;
  const mkA = `rag-arr-a-${bp.id}`;
  const mkW = `rag-arr-w-${bp.id}`;
  const arrow = `url(#${mk})`;
  // Top row: y 20..78, centre line y 49.
  const Y = 20, H = 58, CY = Y + H / 2;
  return (
    <svg viewBox="0 0 1000 440" className="h-auto w-full" role="img" aria-label={`RAG architecture: ${bp.label}`}>
      <defs>
        {[
          [mk, "fill-gray-400 dark:fill-faint"],
          [mkA, "fill-accent"],
          [mkW, "fill-warn"],
        ].map(([id, cls]) => (
          <marker key={id} id={id} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M0,0 L10,5 L0,10 z" className={cls} />
          </marker>
        ))}
      </defs>

      {/* request path */}
      <Box x={8} y={Y} w={118} h={H} title={bp.client.title} sub={bp.client.sub} />
      <Box x={150} y={Y} w={112} h={H} title={GATEWAY.title} sub={GATEWAY.sub} owner="nebula" />
      <Box x={286} y={Y} w={118} h={H} title="Input guardrails" sub={bp.inputGuard} />
      <Box x={428} y={Y} w={122} h={H} title="Semantic cache" sub={bp.cache} owner="nebula" />
      <Box x={574} y={Y} w={140} h={H} title="Router (intent)" sub={bp.router} />
      <Box x={738} y={Y} w={130} h={H} title="Output guardrails" sub={bp.outputGuard} />
      <Box x={892} y={Y} w={100} h={H} title="Answer" sub={bp.answer} />
      {[[126, 150], [262, 286], [404, 428], [550, 574], [714, 738], [868, 892]].map(([a, b]) => (
        <path key={a} d={`M${a} ${CY} L${b - 2} ${CY}`} className={line} fill="none" markerEnd={arrow} />
      ))}

      {/* orchestration bus under the router */}
      <path d="M644 78 L644 140" className={line} fill="none" markerStart={arrow} />
      <path d="M82 140 L790 140" className={line} fill="none" />
      <path d="M82 140 L82 176" className={line} fill="none" markerEnd={arrow} />
      <path d="M560 140 L560 262" className={line} fill="none" markerEnd={arrow} />
      <path d="M680 140 L680 262" className={line} fill="none" markerEnd={arrow} />
      <path d="M790 140 L790 262" className={line} fill="none" markerEnd={arrow} />
      {/* HITL */}
      <path d="M790 140 L930 140 L930 182" className="stroke-warn" strokeDasharray="5 4" fill="none" markerEnd={`url(#${mkW})`} />
      <text x="862" y="133" textAnchor="middle" fontSize="10.5" fontWeight="600" className="fill-warn">HITL</text>

      {/* retrieval column */}
      <Box x={22} y={178} w={120} h={50} title="RAG" sub={bp.rag} owner="nebula" />
      <path d="M82 230 L82 262" className="stroke-accent" fill="none" markerStart={`url(#${mkA})`} markerEnd={`url(#${mkA})`} />
      <g>
        <rect x={8} y={264} width={260} height={62} rx="11" className="fill-accent/15 stroke-accent" strokeWidth="1.6" />
        <text x={138} y={287} textAnchor="middle" fontSize="13" fontWeight="700" className="fill-gray-900 dark:fill-ink">NebulaDB</text>
        <text x={138} y={302} textAnchor="middle" fontSize="10" className="fill-gray-500 dark:fill-muted">vector + BM25 + SQL · chunk + embed</text>
        <text x={138} y={316} textAnchor="middle" fontSize="9.5" className="fill-accent font-mono">{bp.buckets}</text>
      </g>
      <path d="M82 360 L82 328" className="stroke-accent" fill="none" markerEnd={`url(#${mkA})`} />
      <Box x={8} y={362} w={200} h={56} title={bp.ingest.title} sub={bp.ingest.sub} />

      {/* CDC: ingestion ↔ CDC → cache invalidation */}
      <path d="M210 390 L318 390" className="stroke-accent" strokeDasharray="5 4" fill="none" markerStart={`url(#${mkA})`} markerEnd={`url(#${mkA})`} />
      <Box x={320} y={362} w={120} h={56} title="CDC" sub={bp.cdc} owner="nebula" />
      <path d="M380 360 L380 200 L489 200 L489 80" className="stroke-accent" strokeDasharray="5 4" fill="none" markerEnd={`url(#${mkA})`} />
      <text x="484" y="194" textAnchor="end" fontSize="9.5" className="fill-accent">evicts stale answers</text>

      {/* tools under the router */}
      <Box x={500} y={264} w={120} h={56} title={bp.domainDb.title} sub={bp.domainDb.sub} />
      <Box x={625} y={264} w={110} h={56} title={bp.domainApi.title} sub={bp.domainApi.sub} />
      <Box x={740} y={264} w={100} h={56} title={LLM.title} sub={LLM.sub} />
      <Box x={866} y={184} w={128} h={58} title={bp.hitl.title} sub={bp.hitl.sub} owner="human" />

      {/* legend */}
      <g transform="translate(500 360)">
        {(
          [
            ["nebula", "NebulaDB provides"],
            ["app", "your application"],
            ["human", "human in the loop"],
          ] as [Owner, string][]
        ).map(([o, l], i) => (
          <g key={o} transform={`translate(0 ${i * 20})`}>
            <rect width="14" height="12" rx="3" className={OWNER_CLS[o]} />
            <text x="22" y="10" fontSize="10.5" className="fill-gray-600 dark:fill-muted">{l}</text>
          </g>
        ))}
        <g transform="translate(160 0)">
          <path d="M0 6 L28 6" className="stroke-accent" strokeDasharray="5 4" />
          <text x="36" y="10" fontSize="10.5" className="fill-gray-600 dark:fill-muted">change data capture</text>
          <path d="M0 26 L28 26" className="stroke-warn" strokeDasharray="5 4" />
          <text x="36" y="30" fontSize="10.5" className="fill-gray-600 dark:fill-muted">hand-off to a person</text>
        </g>
      </g>
    </svg>
  );
}

/** Intent → route table for a blueprint. */
export function IntentTable({ bp }: { bp: RagBlueprint }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-xs">
        <thead>
          <tr className="border-b border-gray-200 dark:border-edge text-gray-500 dark:text-muted">
            <th className="py-1.5 pr-3 font-medium">Intent</th>
            <th className="py-1.5 font-medium">What the router calls</th>
          </tr>
        </thead>
        <tbody>
          {bp.intents.map((r) => (
            <tr key={r.intent} className="border-b border-gray-100 dark:border-edge/60 align-top">
              <td className="py-1.5 pr-3 text-gray-900 dark:text-ink whitespace-nowrap">{r.intent}</td>
              <td className="py-1.5 text-gray-600 dark:text-muted">{r.route}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/**
 * Collapsible "Architecture" panel for a demo page: the diagram, the
 * router's intents and any use-case notes.
 */
export function ArchitecturePanel({ id, defaultOpen = false }: { id: keyof typeof BLUEPRINTS | string; defaultOpen?: boolean }) {
  const bp = BLUEPRINTS[id];
  const [open, setOpen] = useState(defaultOpen);
  if (!bp) return null;
  return (
    <div className="rounded-md border border-gray-200 dark:border-edge overflow-hidden">
      <div className="flex items-center hover:bg-gray-50 dark:hover:bg-carbon-900 transition-colors">
        <button
          onClick={() => setOpen((v) => !v)}
          className="flex-1 flex items-center gap-2 px-3 py-2 text-left text-xs font-medium text-gray-600 dark:text-muted"
          aria-expanded={open}
        >
          <span className={`transition-transform ${open ? "rotate-90" : ""}`}>▸</span>
          <span>Production architecture · {bp.label}</span>
        </button>
        <a href={GUIDE_HREF} className="px-3 py-2 text-xs font-medium text-accent hover:underline">
          Setup guide →
        </a>
      </div>
      {open && (
        <div className="border-t border-gray-200 dark:border-edge p-3 space-y-3 bg-gray-50/60 dark:bg-carbon-950">
          <p className="text-xs text-gray-600 dark:text-muted">{bp.pitch}</p>
          <RagArchitectureDiagram bp={bp} />
          <IntentTable bp={bp} />
          {bp.notes && (
            <ul className="list-disc space-y-1 pl-5 text-xs text-gray-600 dark:text-muted">
              {bp.notes.map((n) => <li key={n}>{n}</li>)}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
