/**
 * Landing page for the Source Supplies section: the six apps as cards,
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

export function SourceHomeTab({ onNavigate }: { onNavigate: (tab: string) => void }) {
  const seed = useSeeds(["products", "sds", "guides", "accounts", "orders", "washroom", "tender"]);
  return (
    <div className="space-y-4">
      <DemoNote extra="Time savings are demo estimates." />
      <Panel
        title="AI for Source Supplies"
        subtitle="Six everyday jobs at a UK B2B cleaning and hygiene supplier, each built on NebulaDB SQL, grounded chat and MCP agents."
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
      <Panel title="Agent team" subtitle="The same story with Claude choosing the tool calls over MCP">
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
