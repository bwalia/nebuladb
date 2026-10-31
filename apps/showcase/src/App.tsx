import { useEffect, useState } from "react";
import { api, type Health } from "./api";
import {
  fetchShowcaseUser,
  logShowcaseVisit,
  showcaseSignOutUrl,
  type ShowcaseUser,
} from "./auth";
import { getTheme, toggleTheme } from "./theme";
import { OverviewTab } from "./tabs/OverviewTab";
import { DocumentsTab } from "./tabs/DocumentsTab";
import { SqlTab } from "./tabs/SqlTab";
import { SearchTab } from "./tabs/SearchTab";
import { RagTab } from "./tabs/RagTab";
import { HybridTab } from "./tabs/HybridTab";
import { AdminTab } from "./tabs/AdminTab";
import { MetricsTab } from "./tabs/MetricsTab";
import { ExecutiveTab } from "./tabs/ExecutiveTab";
import { McpTab } from "./tabs/McpTab";
import { AgentsTab } from "./tabs/AgentsTab";
import { ClusterTab } from "./tabs/ClusterTab";
import { RebalanceTab } from "./tabs/RebalanceTab";
import { EnterpriseSearchTab } from "./tabs/EnterpriseSearchTab";
import { CompaniesHouseTab } from "./tabs/CompaniesHouseTab";
import { SecureAgentTab } from "./tabs/SecureAgentTab";
import { ShopCatalogTab } from "./tabs/ShopCatalogTab";
import { TalentMatchTab } from "./tabs/TalentMatchTab";
import { IncidentCopilotTab } from "./tabs/IncidentCopilotTab";
import { ContractReviewTab } from "./tabs/ContractReviewTab";
import { SupportDeskTab } from "./tabs/SupportDeskTab";
import { PolicyDeskTab } from "./tabs/PolicyDeskTab";
import { RfpResponderTab } from "./tabs/RfpResponderTab";
import { OnboardingTab } from "./tabs/OnboardingTab";
import { EntityResolveTab } from "./tabs/EntityResolveTab";
import { LookalikeTab } from "./tabs/LookalikeTab";
import { FraudTab } from "./tabs/FraudTab";
import { RecommendTab } from "./tabs/RecommendTab";
import { BlogTab } from "./tabs/BlogTab";
import { FinderTab } from "./source/FinderTab";
import { SafetyTab } from "./source/SafetyTab";
import { ProblemSolverTab } from "./source/ProblemSolverTab";
import { UseCaseDemo, type UseCaseSpec } from "./usecases/UseCaseDemo";
import { clinicalSpec } from "./usecases/specs/clinical";
import { complianceSpec } from "./usecases/specs/compliance";
import { earningsSpec } from "./usecases/specs/earnings";
import { claimsSpec } from "./usecases/specs/claims";
import { fieldServiceSpec } from "./usecases/specs/fieldService";
import { codeSearchSpec } from "./usecases/specs/codeSearch";
import { propertySpec } from "./usecases/specs/property";
import { semanticCacheSpec } from "./usecases/specs/semanticCache";
import { priorArtSpec } from "./usecases/specs/priorArt";
import { voiceOfCustomerSpec } from "./usecases/specs/voiceOfCustomer";

type TabId =
  | "executive"
  | "blog"
  | "overview"
  | "documents"
  | "sql"
  | "search"
  | "enterprise"
  | "companies"
  | "secureagent"
  | "shop"
  | "talent"
  | "incidents"
  | "contracts"
  | "support"
  | "policy"
  | "rfp"
  | "onboarding"
  | "entity"
  | "lookalike"
  | "fraud"
  | "recommend"
  | "ssfinder"
  | "sssafety"
  | "sssolver"
  | IndustryTabId
  | "rag"
  | "hybrid"
  | "agents"
  | "mcp"
  | "cluster"
  | "rebalance"
  | "metrics"
  | "admin";

/** Config-driven industry demos — one spec each, rendered by UseCaseDemo. */
const INDUSTRY_SPECS = {
  clinical: clinicalSpec,
  compliance: complianceSpec,
  earnings: earningsSpec,
  claims: claimsSpec,
  fieldservice: fieldServiceSpec,
  codesearch: codeSearchSpec,
  property: propertySpec,
  semcache: semanticCacheSpec,
  priorart: priorArtSpec,
  voc: voiceOfCustomerSpec,
} satisfies Record<string, UseCaseSpec>;

type IndustryTabId = keyof typeof INDUSTRY_SPECS;

interface NavItem {
  id: TabId;
  label: string;
  hint: string;
  icon: string;
  group: "home" | "data" | "ai" | "apps" | "source" | "use" | "industry" | "cluster" | "ops";
}

const NAV: NavItem[] = [
  { id: "executive", label: "Executive demo", hint: "The 5-minute story", icon: "★", group: "home" },
  { id: "blog", label: "Engineering blog", hint: "Notes from running NebulaDB in production", icon: "✎", group: "home" },
  { id: "overview", label: "Overview", hint: "Cluster summary", icon: "◎", group: "home" },
  { id: "documents", label: "Documents", hint: "Ingest + chunk + embed", icon: "❏", group: "data" },
  { id: "sql", label: "SQL", hint: "Query workbench", icon: "⌘", group: "data" },
  { id: "search", label: "Semantic search", hint: "Vector retrieval", icon: "✷", group: "ai" },
  { id: "enterprise", label: "Enterprise search", hint: "Azure-compatible hybrid search", icon: "⧉", group: "ai" },
  { id: "rag", label: "RAG chat", hint: "Streaming answers", icon: "❯", group: "ai" },
  { id: "hybrid", label: "Hybrid", hint: "SQL + retrieval in one query", icon: "⟡", group: "ai" },
  { id: "agents", label: "AI agents", hint: "Tool-calling agents over MCP", icon: "⬡", group: "ai" },
  { id: "mcp", label: "MCP", hint: "Live MCP server + playground", icon: "⚯", group: "ai" },
  { id: "secureagent", label: "Secure agent", hint: "Identity-bound RAG agent: injection, PII and session-leak guard rails", icon: "⛨", group: "ai" },
  { id: "companies", label: "Companies House", hint: "UK CH search + RAG chat", icon: "⌂", group: "apps" },
  { id: "shop", label: "Shop catalogue", hint: "Stock SOT + product RAG", icon: "▣", group: "apps" },
  { id: "talent", label: "Talent Match", hint: "Job board: SQL hard filters + multi-facet semantic matching + recruiter chat", icon: "♟", group: "apps" },
  { id: "incidents", label: "Incident Copilot", hint: "Alert → similar past incidents + runbooks → grounded triage chat", icon: "⚠", group: "apps" },
  { id: "contracts", label: "Contract Review", hint: "Clause-by-clause review against a legal playbook, with redline chat", icon: "§", group: "apps" },
  { id: "support", label: "Support Desk", hint: "Ticket triage by nearest neighbours + cited reply drafts", icon: "✉", group: "apps" },
  { id: "policy", label: "Policy Desk", hint: "HR / security / finance handbook Q&A with citations", icon: "▤", group: "apps" },
  { id: "rfp", label: "RFP Responder", hint: "Buyer questions → capabilities + past proposals → cited drafts", icon: "❐", group: "apps" },
  { id: "onboarding", label: "Onboarding", hint: "Day-one checklist + role-filtered handbook concierge", icon: "⚑", group: "apps" },
  { id: "ssfinder", label: "Green Product Finder", hint: "What should I use? Meaning + eco filters in one SQL query, greener swaps priced at your volume", icon: "❀", group: "source" },
  { id: "sssafety", label: "COSHH & Safety Data", hint: "Safety data sheet answers with section citations; emergency, mixing and not-in-the-SDS rules in code", icon: "⚠", group: "source" },
  { id: "sssolver", label: "Cleaning Problem Solver", hint: "Tough job → the right guide by surface and sector → method, kit with stock, draft Quick Order", icon: "✦", group: "source" },
  { id: "entity", label: "Entity Resolve", hint: "CRM dedupe via semantic neighbours + merge/review scores", icon: "⊕", group: "use" },
  { id: "lookalike", label: "Lookalike Accounts", hint: "Won ICP → similar prospects with industry filters", icon: "◈", group: "use" },
  { id: "fraud", label: "Fraud Neighbours", hint: "Alert → labelled tx neighbourhood → risk vote (no LLM)", icon: "⚡", group: "use" },
  { id: "recommend", label: "Recommendations", hint: "Basket → in-stock SKUs under price/category filters", icon: "↝", group: "use" },
  { id: "clinical", label: "Clinical Guidelines", hint: "Bedside question → trust pathways by specialty → cited answer", icon: "✚", group: "industry" },
  { id: "compliance", label: "Compliance Mapper", hint: "Internal control → GDPR / DORA / EU AI Act / NIS2 obligations → gap analysis", icon: "⚖", group: "industry" },
  { id: "earnings", label: "Earnings Analyst", hint: "Earnings-call excerpts by ticker and quarter → cited research notes", icon: "↗", group: "industry" },
  { id: "claims", label: "Claims Assist", hint: "First notice of loss → policy wording + precedent claims → coverage view", icon: "☂", group: "industry" },
  { id: "fieldservice", label: "Field Service", hint: "Fault on site → manuals, fault codes, past jobs → step-by-step fix", icon: "⚒", group: "industry" },
  { id: "codesearch", label: "Code Search", hint: "Natural-language questions over code, migrations, config and ADRs", icon: "{}", group: "industry" },
  { id: "property", label: "Property Finder", hint: "Plain-English brief + price / bedroom filters in one SQL query", icon: "⌂", group: "industry" },
  { id: "semcache", label: "Semantic LLM Cache", hint: "Serve cached answers for paraphrased prompts above a similarity threshold", icon: "⟳", group: "industry" },
  { id: "priorart", label: "Prior-Art Search", hint: "Invention disclosure → nearest patents → novelty screen (no LLM)", icon: "⌕", group: "industry" },
  { id: "voc", label: "Voice of Customer", hint: "Theme → nearest reviews → sentiment, rating and product mix (no LLM)", icon: "☊", group: "industry" },
  { id: "cluster", label: "Cluster", hint: "Topology, regions, failover", icon: "⬢", group: "cluster" },
  { id: "rebalance", label: "Rebalance", hint: "Zero-downtime node swap", icon: "⇄", group: "cluster" },
  { id: "metrics", label: "Metrics", hint: "Embedded Grafana", icon: "▟", group: "ops" },
  { id: "admin", label: "Admin", hint: "Buckets, EXPLAIN, audit, slow queries", icon: "⚙", group: "ops" },
];

const GROUP_LABELS: Record<NavItem["group"], string> = {
  home: "Home",
  data: "Data",
  ai: "AI",
  apps: "RAG apps",
  source: "Source Supplies",
  use: "Applied use cases",
  industry: "Industry use cases",
  cluster: "Cluster",
  ops: "Operations",
};

const SIDEBAR_KEY = "nebula-sidebar";

function tabFromHash(): TabId | null {
  const h = window.location.hash;
  if (h.startsWith("#/blog")) return "blog";
  const m = /^#\/tab\/([a-z]+)/.exec(h);
  return m && NAV.some((t) => t.id === m[1]) ? (m[1] as TabId) : null;
}

export function App() {
  // `#/blog/...` deep links (shared posts, landing-page cards) open the blog;
  // `#/tab/<id>` opens any demo directly (e.g. #/tab/secureagent).
  const [tab, setTab] = useState<TabId>(() => tabFromHash() ?? "executive");
  // In-page `#/blog/...` links (e.g. a demo's "Setup guide") switch to the blog.
  useEffect(() => {
    const onHash = () => {
      const t = tabFromHash();
      if (t) setTab(t);
    };
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  const [theme, setThemeState] = useState(getTheme());
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    try {
      return localStorage.getItem(SIDEBAR_KEY) === "collapsed";
    } catch {
      return false;
    }
  });
  const [health, setHealth] = useState<Health | null>(null);
  const [healthErr, setHealthErr] = useState<string | null>(null);
  const [viewer, setViewer] = useState<ShowcaseUser | null>(null);

  useEffect(() => {
    try {
      localStorage.setItem(SIDEBAR_KEY, collapsed ? "collapsed" : "expanded");
    } catch {
      /* private mode — collapse still works for the session */
    }
  }, [collapsed]);

  useEffect(() => {
    let cancelled = false;
    const refresh = async () => {
      try {
        const h = await api.health();
        if (!cancelled) {
          setHealth(h);
          setHealthErr(null);
        }
      } catch (e) {
        if (!cancelled) setHealthErr((e as Error).message);
      }
    };
    refresh();
    const id = setInterval(refresh, 5000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const user = await fetchShowcaseUser();
      if (cancelled || !user) return;
      setViewer(user);
      void logShowcaseVisit(user);
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const grouped = NAV.reduce<Record<string, NavItem[]>>((acc, n) => {
    (acc[n.group] ||= []).push(n);
    return acc;
  }, {});

  const active = NAV.find((n) => n.id === tab);

  return (
    <div className="h-full flex flex-col">
      <TopBar
        theme={theme}
        onToggleTheme={() => setThemeState(toggleTheme())}
        health={health}
        healthErr={healthErr}
        viewer={viewer}
        onToggleSidebar={() => setCollapsed((v) => !v)}
        sidebarCollapsed={collapsed}
      />

      <div className="flex-1 flex overflow-hidden">
        <Sidebar
          collapsed={collapsed}
          grouped={grouped}
          active={tab}
          onSelect={(id) => {
            if (id !== "blog" && window.location.hash.startsWith("#/blog")) {
              history.replaceState(null, "", window.location.pathname + window.location.search);
            }
            setTab(id);
          }}
        />

        <main className="flex-1 overflow-auto px-6 py-8">
          <div className="max-w-6xl mx-auto">
            {active && (
              <div key={tab} className="mb-6 animate-rise">
                <div className="eyebrow">{GROUP_LABELS[active.group]}</div>
                <h2 className="mt-1.5 text-2xl font-semibold text-black dark:text-ink">
                  {active.label}
                </h2>
                <p className="mt-1 text-sm text-gray-500 dark:text-muted">{active.hint}</p>
              </div>
            )}
            {(active?.group === "apps" || active?.group === "source" || active?.group === "industry") && health && /mock/i.test(health.model) && (
              <div className="mb-4 rounded-md border border-warn/40 bg-warn/10 px-3 py-2 text-xs text-gray-700 dark:text-muted">
                <span className="font-semibold text-warn">Mock embedder.</span> This server embeds with{" "}
                <code>{health.model}</code> (a deterministic hash, not a language model), so semantic
                similarity scores are noise here. Hard filters, skills, rates and dates still rank
                correctly; point <code>NEBULA_OPENAI_*</code> at a real embedding model for meaningful
                semantic matching.
              </div>
            )}
            <div key={`${tab}-body`} className="animate-rise">
              {tab === "executive" && (
                <ExecutiveTab
                  onNavigate={(t) => {
                    // "blog:<slug>" opens a specific post.
                    if (t.startsWith("blog:")) window.location.hash = `#/blog/${t.slice(5)}`;
                    setTab((t.startsWith("blog:") ? "blog" : t) as TabId);
                  }}
                />
              )}
              {tab === "blog" && <BlogTab />}
              {tab === "overview" && <OverviewTab onNavigate={(t) => setTab(t as TabId)} />}
              {tab === "documents" && <DocumentsTab />}
              {tab === "sql" && <SqlTab />}
              {tab === "search" && <SearchTab />}
              {tab === "enterprise" && <EnterpriseSearchTab />}
              {tab === "companies" && <CompaniesHouseTab />}
              {tab === "secureagent" && <SecureAgentTab />}
              {tab === "shop" && <ShopCatalogTab />}
              {tab === "talent" && <TalentMatchTab />}
              {tab === "incidents" && <IncidentCopilotTab />}
              {tab === "contracts" && <ContractReviewTab />}
              {tab === "support" && <SupportDeskTab />}
              {tab === "policy" && <PolicyDeskTab />}
              {tab === "rfp" && <RfpResponderTab />}
              {tab === "onboarding" && <OnboardingTab />}
              {tab === "entity" && <EntityResolveTab />}
              {tab === "lookalike" && <LookalikeTab />}
              {tab === "fraud" && <FraudTab />}
              {tab === "recommend" && <RecommendTab />}
              {tab === "ssfinder" && <FinderTab />}
              {tab === "sssafety" && <SafetyTab />}
              {tab === "sssolver" && <ProblemSolverTab />}
              {tab in INDUSTRY_SPECS && (
                <UseCaseDemo spec={INDUSTRY_SPECS[tab as IndustryTabId]} />
              )}
              {tab === "rag" && <RagTab />}
              {tab === "hybrid" && <HybridTab />}
              {tab === "agents" && <AgentsTab />}
              {tab === "mcp" && <McpTab />}
              {tab === "cluster" && <ClusterTab />}
              {tab === "rebalance" && <RebalanceTab />}
              {tab === "metrics" && <MetricsTab />}
              {tab === "admin" && <AdminTab />}
            </div>
          </div>
        </main>
      </div>

      <StatusBar health={health} err={healthErr} />
    </div>
  );
}

function TopBar({
  theme,
  onToggleTheme,
  health,
  healthErr,
  viewer,
  onToggleSidebar,
  sidebarCollapsed,
}: {
  theme: string;
  onToggleTheme: () => void;
  health: Health | null;
  healthErr: string | null;
  viewer: ShowcaseUser | null;
  onToggleSidebar: () => void;
  sidebarCollapsed: boolean;
}) {
  return (
    <header className="border-b border-gray-200 dark:border-edge bg-white/80 dark:bg-carbon-950/80 backdrop-blur">
      <div className="px-4 py-2.5 flex items-center gap-4">
        <button
          onClick={onToggleSidebar}
          className="btn-secondary !py-1 !px-2 !text-xs"
          title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
          aria-label="Toggle sidebar"
        >
          ☰
        </button>
        <div className="flex items-center gap-2.5">
          <Mark />
          <div className="flex items-baseline gap-2">
            <span className="font-display text-base font-semibold tracking-tight text-black dark:text-ink">
              NebulaDB
            </span>
            <span className="eyebrow hidden sm:inline">Knowledge Ops</span>
          </div>
        </div>

        <div className="ml-auto flex items-center gap-2.5 text-xs">
          {viewer && (
            <div className="hidden sm:flex items-center gap-2 text-gray-600 dark:text-muted">
              <span className="max-w-[14rem] truncate" title={viewer.email}>
                {viewer.email}
              </span>
              <a
                href={showcaseSignOutUrl()}
                className="btn-secondary !py-1 !px-2 no-underline"
                title="Sign out"
              >
                Sign out
              </a>
            </div>
          )}
          <HealthBadge health={health} err={healthErr} />
          <button
            onClick={onToggleTheme}
            className="btn-secondary !py-1 !px-2"
            aria-label="Toggle dark mode"
            title="Toggle dark mode"
          >
            {theme === "dark" ? "☾" : "☀"}
          </button>
        </div>
      </div>
    </header>
  );
}

/** Wordmark glyph: a constellation node — the "nebula" in NebulaDB. */
function Mark() {
  return (
    <span className="grid h-7 w-7 place-items-center rounded-lg border border-gray-300 bg-gray-100 dark:border-edge dark:bg-carbon-900">
      <svg width="15" height="15" viewBox="0 0 16 16" fill="none" aria-hidden>
        <circle cx="8" cy="8" r="2" className="fill-black dark:fill-ink" />
        <circle cx="3" cy="4" r="1" className="fill-ok" />
        <circle cx="13" cy="5" r="1" className="fill-accent" />
        <circle cx="12" cy="12" r="1" className="fill-black dark:fill-muted" />
        <path
          d="M8 8 L3 4 M8 8 L13 5 M8 8 L12 12"
          className="stroke-gray-400 dark:stroke-faint"
          strokeWidth="0.8"
        />
      </svg>
    </span>
  );
}

function Sidebar({
  collapsed,
  grouped,
  active,
  onSelect,
}: {
  collapsed: boolean;
  grouped: Record<string, NavItem[]>;
  active: TabId;
  onSelect: (id: TabId) => void;
}) {
  return (
    <aside
      className={`border-r border-gray-200 dark:border-edge bg-white/60 dark:bg-carbon-950
                  transition-all duration-200 overflow-y-auto shrink-0
                  ${collapsed ? "w-14" : "w-56"}`}
    >
      <nav className="py-3">
        {(Object.keys(GROUP_LABELS) as Array<keyof typeof GROUP_LABELS>).map((g) => {
          const items = grouped[g] ?? [];
          if (items.length === 0) return null;
          return (
            <div key={g} className="mb-3">
              {!collapsed && <div className="px-4 pt-2 pb-1.5 eyebrow">{GROUP_LABELS[g]}</div>}
              {items.map((n) => {
                const isActive = active === n.id;
                return (
                  <button
                    key={n.id}
                    onClick={() => onSelect(n.id)}
                    title={n.hint}
                    aria-current={isActive ? "page" : undefined}
                    className={`group w-full flex items-center gap-3 px-4 py-2 text-sm transition-colors
                                ${
                                  isActive
                                    ? "bg-gray-100 text-black dark:bg-carbon-800 dark:text-ink"
                                    : "text-gray-600 dark:text-muted hover:bg-gray-50 dark:hover:bg-carbon-900 hover:text-black dark:hover:text-ink"
                                }`}
                  >
                    <span
                      className={`w-5 text-center text-base leading-none
                        ${isActive ? "text-black dark:text-ink" : "text-gray-400 dark:text-faint group-hover:text-black dark:group-hover:text-ink"}`}
                    >
                      {n.icon}
                    </span>
                    {!collapsed && <span className="truncate">{n.label}</span>}
                  </button>
                );
              })}
            </div>
          );
        })}
      </nav>
    </aside>
  );
}

function HealthBadge({ health, err }: { health: Health | null; err: string | null }) {
  if (err) {
    return (
      <span className="inline-flex items-center gap-2 rounded-full border border-red-300/60 bg-red-50 px-2.5 py-1 font-mono text-bad dark:border-bad/30 dark:bg-bad/10">
        <span className="dot dot-bad" />
        offline
      </span>
    );
  }
  if (!health) {
    return <span className="font-mono text-gray-400 dark:text-faint">connecting…</span>;
  }
  return (
    <span className="inline-flex items-center gap-2 rounded-full border border-gray-200 bg-white px-2.5 py-1 font-mono text-gray-600 dark:border-edge dark:bg-carbon-900 dark:text-muted">
      <span className="dot dot-ok animate-pulseok" />
      <span className="text-gray-800 dark:text-ink">{health.model}</span>
      <span className="text-gray-300 dark:text-edge">·</span>
      dim {health.dim}
      <span className="text-gray-300 dark:text-edge">·</span>
      {health.docs} docs
    </span>
  );
}

/**
 * Bottom status bar — jobshout-style build stamp: live release tag,
 * deploy age, commit, embedder, corpus size.
 */
function StatusBar({ health, err }: { health: Health | null; err: string | null }) {
  const commit =
    health?.git_commit && health.git_commit !== "unknown"
      ? health.git_commit.slice(0, 7)
      : health?.image_tag && !/^v?\d+\.\d+/.test(health.image_tag)
        ? health.image_tag.slice(0, 7)
        : null;
  const ver = health?.version?.replace(/^v/, "");
  const deployedRel = formatDeployed(health?.deployed_at || health?.build_date);
  const deployedAbs = absoluteTime(health?.deployed_at || health?.build_date);
  const deployTitle = [health?.env, deployedAbs && `Deployed ${deployedAbs}`]
    .filter(Boolean)
    .join(" · ");

  return (
    <footer className="border-t border-gray-200 dark:border-edge bg-white/80 dark:bg-carbon-950/90 backdrop-blur">
      <div className="px-4 py-2 flex items-center gap-5 overflow-x-auto">
        <span className="font-display text-xs font-semibold text-black dark:text-ink shrink-0">NebulaDB</span>
        {err ? (
          <span className="metachip text-bad">
            <span className="dot dot-bad" /> server unreachable
          </span>
        ) : (
          <>
            <span className="metachip">
              <span className={`dot ${health ? "dot-ok" : "dot-idle"}`} />
              {health ? "live" : "connecting"}
            </span>
            {ver && (
              <span className="metachip" title={deployTitle || "server version"}>
                🏷 v{ver}
              </span>
            )}
            {deployedRel && (
              <span className="metachip" title={deployTitle || undefined}>
                {health?.env ? `${health.env} · ` : ""}
                Deployed {deployedRel}
              </span>
            )}
            {commit && <span className="metachip" title="git commit / image tag">⑂ {commit}</span>}
            {health && (
              <span className="metachip" title="embedder">✷ {health.model}</span>
            )}
            {health && (
              <span className="metachip" title="documents indexed">
                ❏ {health.docs.toLocaleString()} docs · dim {health.dim}
              </span>
            )}
          </>
        )}
      </div>
    </footer>
  );
}

function absoluteTime(iso?: string): string | null {
  if (!iso || iso === "unknown") return null;
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return null;
  return new Date(t).toLocaleString();
}

/** Relative deploy age — same wording as jobshout's sidebar stamp. */
function formatDeployed(iso?: string): string | null {
  if (!iso || iso === "unknown") return null;
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return null;
  const mins = Math.floor((Date.now() - t) / 60_000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  return new Date(t).toLocaleDateString();
}
