/**
 * Landing page for the Source Supplies section: the apps as cards,
 * and the agent team. Opening it seeds every ss_* bucket (idempotent).
 */
import { Panel } from "../components";
import { DemoNote, SeedChip, useSeeds } from "./common";

const APPS = [
  {
    tab: "ssfinder",
    name: "Green Product Finder",
    problem: "Buyers ask “what should I use?” and want the greener choice that is in stock and on budget.",
    who: "Cleaning supervisors and buyers at trade accounts",
    saves: "About 15 minutes per product question",
  },
  {
    tab: "sssafety",
    name: "COSHH & Safety Data Assistant",
    problem: "Cleaners need safety answers from the SDS on site, fast, without guessing.",
    who: "Cleaners on site and site managers doing COSHH",
    saves: "About 20 minutes per COSHH assessment draft",
  },
  {
    tab: "sssolver",
    name: "Cleaning Problem Solver",
    problem: "Tough jobs (mould, gum, smells, worn floors) need a method and the right kit.",
    who: "Account managers and supervisors",
    saves: "About 30 minutes per problem call",
  },
  {
    tab: "ssreorder",
    name: "Smart Reorder",
    problem: "Multi-site cleaners re-key the same orders every month, site by site.",
    who: "FM and contract-cleaning buyers",
    saves: "About 10 minutes per site per month",
  },
  {
    tab: "ssstock",
    name: "Stock Autopilot",
    problem: "No live Merlin stock and popular lines still run out; POs drafted from pasted 3-month averages.",
    who: "Warehouse and buying",
    saves: "~60% fewer stock-outs with less cash in stock (12-month backtest)",
  },
  {
    tab: "ssreports",
    name: "Jordan Report Studio",
    problem: "Customer reports wait weeks for Merlin custom-report tickets or someone who writes SQL.",
    who: "Account managers and ops",
    saves: "Weeks → seconds per report; monthly reports run themselves",
  },
  {
    tab: "ssretail",
    name: "Retail POD Autopilot",
    problem: "~600 retail portal orders a month keyed into Merlin, then two PODs found and uploaded for each.",
    who: "Order desk",
    saves: "~£20k/year; ~87% of deliveries need no person",
  },
  {
    tab: "ssemail",
    name: "Email Order Intake",
    problem: "The ~10% of orders still emailed must be re-keyed into Merlin by hand.",
    who: "Order desk",
    saves: "Re-keying time on every emailed PO",
  },
  {
    tab: "sswashroom",
    name: "Washroom Service Desk",
    problem: "Washroom issues need the right priority, a missed-visit check and a quick reply.",
    who: "The Source Washroom service team",
    saves: "About 10 minutes per ticket",
  },
  {
    tab: "sstender",
    name: "Tender Responder",
    problem: "School, NHS and council tenders ask the same questions in new words.",
    who: "Sales and bid writers",
    saves: "About half a day per tender",
  },
];

const TOP3 = [
  {
    tab: "ssretail",
    rank: 1,
    name: "Retail POD Autopilot",
    pain: "“This is the biggest pain” — portal orders re-keyed into Merlin, two PODs per delivery uploaded by hand.",
    win: "≈ £20k a year back · exceptions only",
    video: "/tours/source-supplies/retail-pod-autopilot.mp4",
  },
  {
    tab: "ssstock",
    rank: 2,
    name: "Stock Autopilot",
    pain: "“Our stock control is always an issue” — popular lines run out; POs from pasted averages.",
    win: "~60% fewer stock-outs, less cash tied up",
    video: "/tours/source-supplies/stock-autopilot.mp4",
  },
  {
    tab: "ssreports",
    rank: 3,
    name: "Jordan Report Studio",
    pain: "“Third attempt… three or four days apart” — customer reports stuck behind Merlin and SQL.",
    win: "Weeks → seconds, scheduled monthly",
    video: "/tours/source-supplies/report-studio.mp4",
  },
];

export function SourceHomeTab({ onNavigate }: { onNavigate: (tab: string) => void }) {
  const seed = useSeeds(["products", "sds", "guides", "accounts", "orders", "washroom", "tender", "retail", "demand"]);
  return (
    <div className="space-y-4">
      <DemoNote extra="Time savings are demo estimates." />
      <Panel title="The top 3 from the discovery call" subtitle="Dave’s own order: the retail portal + PODs, stock, then reports. Each has a narrated video tour.">
        <div className="grid gap-3 lg:grid-cols-3">
          {TOP3.map((t) => (
            <div key={t.tab} className="rounded-lg border border-gray-200 dark:border-edge p-3 space-y-2">
              <video src={t.video} controls preload="metadata" className="w-full rounded-md border border-gray-200 dark:border-edge bg-black aspect-video" />
              <div className="flex items-baseline gap-2">
                <span className="font-display text-accent">#{t.rank}</span>
                <button type="button" onClick={() => onNavigate(t.tab)} className="font-semibold text-gray-900 dark:text-ink hover:text-accent text-left">
                  {t.name} →
                </button>
              </div>
              <p className="text-xs text-gray-600 dark:text-muted">{t.pain}</p>
              <p className="text-xs font-medium text-ok">{t.win}</p>
            </div>
          ))}
        </div>
      </Panel>
      <Panel
        title="AI for Source Supplies"
        subtitle="Everyday jobs at a UK B2B cleaning and hygiene supplier — including stock, Merlin reports, retail PODs and email orders from the discovery call — on NebulaDB SQL, grounded chat and MCP agents."
        action={<SeedChip seed={seed} />}
      >
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {APPS.map((a) => (
            <button
              key={a.tab}
              type="button"
              onClick={() => onNavigate(a.tab)}
              className="text-left rounded-lg border border-gray-200 dark:border-edge p-3 hover:border-accent/60 hover:bg-accent/5 transition-colors"
            >
              <div className="font-semibold text-gray-900 dark:text-ink">{a.name}</div>
              <p className="mt-1 text-sm text-gray-700 dark:text-muted">{a.problem}</p>
              <div className="mt-2 text-xs text-gray-500 dark:text-muted">
                <div>
                  <span className="font-medium">Who:</span> {a.who}
                </div>
                <div>
                  <span className="font-medium">Saves:</span> {a.saves}
                </div>
              </div>
            </button>
          ))}
        </div>
      </Panel>
      <Panel title="Agent team" subtitle="The same story with Claude choosing the tool calls over MCP — Jordan AI middleware to Merlin">
        <p className="text-sm text-gray-700 dark:text-muted">
          Seven agents (product, safety, order, washroom, sustainability, account assistant and a supervisor) each get only the NebulaDB
          tools they need. The order agent can only write drafts to <code>ss_order_drafts</code>; nothing places an order. See{" "}
          <code>examples/nebula-agents</code> (<code>--team source</code>) or the{" "}
          <button type="button" className="text-accent hover:underline" onClick={() => onNavigate("agents")}>
            AI agents
          </button>{" "}
          tab.
        </p>
      </Panel>
    </div>
  );
}
