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

type TabId =
  | "overview"
  | "documents"
  | "sql"
  | "search"
  | "rag"
  | "hybrid"
  | "admin";

interface NavItem {
  id: TabId;
  label: string;
  hint: string;
  // Inline SVG glyph — no icon-font dependency and dark-mode agnostic.
  icon: string;
  group: "home" | "data" | "ai" | "ops";
}

// Couchbase-style left nav: labelled groups rather than one flat list.
// Grouping communicates scope at a glance ("AI vs data vs ops") and
// gives us a natural place to add future admin-only sections.
const NAV: NavItem[] = [
  { id: "overview", label: "Overview", hint: "Cluster summary", icon: "🏠", group: "home" },
  { id: "documents", label: "Documents", hint: "Ingest + chunk + embed", icon: "📄", group: "data" },
  { id: "sql", label: "SQL", hint: "Query workbench", icon: "⌘", group: "data" },
  { id: "search", label: "Semantic search", hint: "Vector retrieval", icon: "🔎", group: "ai" },
  { id: "rag", label: "RAG chat", hint: "Streaming answers", icon: "💬", group: "ai" },
  { id: "hybrid", label: "Hybrid", hint: "SQL + retrieval in one query", icon: "⚡", group: "ai" },
  { id: "admin", label: "Admin", hint: "Buckets, EXPLAIN, audit", icon: "⚙", group: "ops" },
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
    // Persist the sidebar state across reloads. Defaults to expanded.
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

  // Poll /healthz; 5s gives the header a live feel without spamming.
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

        <main className="flex-1 overflow-auto px-6 py-6">
          <div className="max-w-6xl mx-auto">
            {tab === "overview" && <OverviewTab onNavigate={(t) => setTab(t as TabId)} />}
            {tab === "documents" && <DocumentsTab />}
            {tab === "sql" && <SqlTab />}
            {tab === "search" && <SearchTab />}
            {tab === "rag" && <RagTab />}
            {tab === "hybrid" && <HybridTab />}
            {tab === "admin" && <AdminTab />}
          </div>
        </main>
      </div>
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
    <header className="border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900">
      <div className="px-4 py-2 flex items-center gap-4">
        <button
          onClick={onToggleSidebar}
          className="btn-secondary !py-1 !px-2 !text-xs"
          title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
          aria-label="Toggle sidebar"
        >
          ☰
        </button>
        <div className="flex items-baseline gap-3">
          <h1 className="text-base font-bold tracking-tight">NebulaDB</h1>
          <span className="text-xs text-gray-500 dark:text-gray-400">
            Control Plane
          </span>
        </div>

        <div className="ml-auto flex items-center gap-3 text-xs">
          <HealthBadge health={health} err={healthErr} />
          <a
            className="btn-secondary !py-1 !px-2"
            href="http://localhost:3000"
            target="_blank"
            rel="noreferrer"
            title="Open Grafana"
          >
            Grafana
          </a>
          <a
            className="btn-secondary !py-1 !px-2"
            href="http://localhost:9090"
            target="_blank"
            rel="noreferrer"
            title="Open Prometheus"
          >
            Prom
          </a>
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
      className={`border-r border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900
                  transition-all duration-200 overflow-y-auto shrink-0
                  ${collapsed ? "w-14" : "w-56"}`}
    >
      <nav className="py-2">
        {(Object.keys(GROUP_LABELS) as Array<keyof typeof GROUP_LABELS>).map((g) => {
          const items = grouped[g] ?? [];
          if (items.length === 0) return null;
          return (
            <div key={g} className="mb-2">
              {!collapsed && (
                <div className="px-3 pt-2 pb-1 text-[10px] uppercase tracking-wider text-gray-400 dark:text-gray-500">
                  {GROUP_LABELS[g]}
                </div>
              )}
              {items.map((n) => {
                const isActive = active === n.id;
                return (
                  <button
                    key={n.id}
                    onClick={() => onSelect(n.id)}
                    title={n.hint}
                    className={`w-full flex items-center gap-2 px-3 py-1.5 text-sm
                                ${
                                  isActive
                                    ? "bg-brand-50 text-brand-700 dark:bg-brand-600/20 dark:text-brand-500 border-l-2 border-brand-500"
                                    : "text-gray-700 dark:text-gray-300 border-l-2 border-transparent hover:bg-gray-100 dark:hover:bg-gray-800"
                                }`}
                  >
                    <span className="text-base leading-none w-5 text-center">{n.icon}</span>
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
      <span className="rounded-full bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300 px-2 py-1">
        offline
      </span>
    );
  }
  if (!health) {
    return <span className="text-gray-400">loading…</span>;
  }
  return (
    <span className="rounded-full bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300 px-2 py-1 flex items-center gap-2">
      <span className="h-1.5 w-1.5 rounded-full bg-green-500 animate-pulse" />
      {health.model} · dim {health.dim} · {health.docs} docs
    </span>
  );
}
