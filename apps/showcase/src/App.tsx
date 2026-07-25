import { useEffect, useState } from "react";
import { api, type Health } from "./api";
import { getTheme, toggleTheme } from "./theme";
import { OverviewTab } from "./tabs/OverviewTab";
import { DocumentsTab } from "./tabs/DocumentsTab";
import { SqlTab } from "./tabs/SqlTab";
import { SearchTab } from "./tabs/SearchTab";
import { RagTab } from "./tabs/RagTab";
import { HybridTab } from "./tabs/HybridTab";
import { AdminTab } from "./tabs/AdminTab";
import { MetricsTab } from "./tabs/MetricsTab";

type TabId =
  | "overview"
  | "documents"
  | "sql"
  | "search"
  | "rag"
  | "hybrid"
  | "metrics"
  | "admin";

interface NavItem {
  id: TabId;
  label: string;
  hint: string;
  icon: string;
  group: "home" | "data" | "ai" | "ops";
}

const NAV: NavItem[] = [
  { id: "overview", label: "Overview", hint: "Cluster summary", icon: "◎", group: "home" },
  { id: "documents", label: "Documents", hint: "Ingest + chunk + embed", icon: "❏", group: "data" },
  { id: "sql", label: "SQL", hint: "Query workbench", icon: "⌘", group: "data" },
  { id: "search", label: "Semantic search", hint: "Vector retrieval", icon: "✷", group: "ai" },
  { id: "rag", label: "RAG chat", hint: "Streaming answers", icon: "❯", group: "ai" },
  { id: "hybrid", label: "Hybrid", hint: "SQL + retrieval in one query", icon: "⟡", group: "ai" },
  { id: "metrics", label: "Metrics", hint: "Embedded Grafana", icon: "▟", group: "ops" },
  { id: "admin", label: "Admin", hint: "Buckets, EXPLAIN, audit, slow queries", icon: "⚙", group: "ops" },
];

const GROUP_LABELS: Record<NavItem["group"], string> = {
  home: "Home",
  data: "Data",
  ai: "AI",
  ops: "Operations",
};

const SIDEBAR_KEY = "nebula-sidebar";

export function App() {
  const [tab, setTab] = useState<TabId>("overview");
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
        onToggleSidebar={() => setCollapsed((v) => !v)}
        sidebarCollapsed={collapsed}
      />

      <div className="flex-1 flex overflow-hidden">
        <Sidebar
          collapsed={collapsed}
          grouped={grouped}
          active={tab}
          onSelect={(id) => setTab(id)}
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
            <div key={`${tab}-body`} className="animate-rise">
              {tab === "overview" && <OverviewTab onNavigate={(t) => setTab(t as TabId)} />}
              {tab === "documents" && <DocumentsTab />}
              {tab === "sql" && <SqlTab />}
              {tab === "search" && <SearchTab />}
              {tab === "rag" && <RagTab />}
              {tab === "hybrid" && <HybridTab />}
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
  onToggleSidebar,
  sidebarCollapsed,
}: {
  theme: string;
  onToggleTheme: () => void;
  health: Health | null;
  healthErr: string | null;
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
            <span className="text-base font-semibold tracking-tight text-black dark:text-ink">
              NebulaDB
            </span>
            <span className="eyebrow hidden sm:inline">Knowledge Ops</span>
          </div>
        </div>

        <div className="ml-auto flex items-center gap-2.5 text-xs">
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
 * Bottom status bar — mirrors the Ring Promoter deploy footer: the app
 * identity plus mono build/runtime metadata (version, commit, embedder).
 */
function StatusBar({ health, err }: { health: Health | null; err: string | null }) {
  const commit = health?.git_commit && health.git_commit !== "unknown" ? health.git_commit.slice(0, 7) : null;
  return (
    <footer className="border-t border-gray-200 dark:border-edge bg-white/80 dark:bg-carbon-950/90 backdrop-blur">
      <div className="px-4 py-2 flex items-center gap-5 overflow-x-auto">
        <span className="text-xs font-semibold text-black dark:text-ink shrink-0">NebulaDB</span>
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
            {health?.version && (
              <span className="metachip" title="server version">🏷 v{health.version}</span>
            )}
            {commit && <span className="metachip" title="git commit">⑂ {commit}</span>}
            {health && (
              <span className="metachip" title="embedder">✷ {health.model}</span>
            )}
            {health && (
              <span className="metachip" title="documents indexed">❏ {health.docs} docs · dim {health.dim}</span>
            )}
          </>
        )}
      </div>
    </footer>
  );
}
