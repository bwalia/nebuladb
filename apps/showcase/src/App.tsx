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
  // Inline SVG glyph — no icon-font dependency and dark-mode agnostic.
  icon: string;
  group: "home" | "data" | "ai" | "ops";
}

// Grouped left rail: labelled groups rather than one flat list.
// Grouping communicates scope at a glance ("AI vs data vs ops") and
// gives us a natural place to add future admin-only sections.
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

  const active = NAV.find((n) => n.id === tab);

  return (
    <div className="relative h-full flex flex-col">
      <Starfield />

      <TopBar
        theme={theme}
        onToggleTheme={() => setThemeState(toggleTheme())}
        health={health}
        healthErr={healthErr}
        onToggleSidebar={() => setCollapsed((v) => !v)}
        sidebarCollapsed={collapsed}
      />

      <div className="relative flex-1 flex overflow-hidden">
        <Sidebar
          collapsed={collapsed}
          grouped={grouped}
          active={tab}
          onSelect={(id) => setTab(id)}
        />

        <main className="flex-1 overflow-auto px-6 py-8">
          <div className="max-w-6xl mx-auto">
            {/* Screen eyebrow — orients the operator: which instrument is live. */}
            {active && (
              <div key={tab} className="mb-6 animate-rise">
                <div className="eyebrow">{GROUP_LABELS[active.group]}</div>
                <h2 className="mt-1 text-2xl font-semibold text-gray-900 dark:text-ink">
                  {active.label}
                </h2>
                <p className="mt-0.5 text-sm text-gray-500 dark:text-muted">{active.hint}</p>
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
    </div>
  );
}

/**
 * Ambient vector-field: a faint drifting starfield behind the whole
 * shell, evoking the "knowledge nebula" the engine indexes. Dark-mode
 * only, pointer-inert, and frozen under prefers-reduced-motion (the
 * drift animation is disabled in index.css).
 */
function Starfield() {
  return (
    <div
      aria-hidden
      className="pointer-events-none fixed inset-0 -z-10 hidden dark:block overflow-hidden"
    >
      <div
        className="absolute -inset-32 animate-drift opacity-[0.5]"
        style={{
          backgroundImage:
            "radial-gradient(1px 1px at 20px 30px, rgba(164,139,255,0.5), transparent)," +
            "radial-gradient(1px 1px at 120px 80px, rgba(124,240,220,0.4), transparent)," +
            "radial-gradient(1.5px 1.5px at 210px 150px, rgba(231,236,245,0.35), transparent)," +
            "radial-gradient(1px 1px at 320px 60px, rgba(164,139,255,0.35), transparent)," +
            "radial-gradient(1px 1px at 400px 200px, rgba(124,240,220,0.3), transparent)",
          backgroundSize: "460px 320px",
        }}
      />
      {/* Nebula wash — one soft plasma bloom, upper-left. */}
      <div className="absolute -top-40 -left-40 h-96 w-96 rounded-full bg-plasma/10 blur-3xl" />
      <div className="absolute top-1/3 -right-40 h-96 w-96 rounded-full bg-signal/[0.06] blur-3xl" />
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
    <header className="relative z-10 border-b border-gray-200 dark:border-hairline bg-white/80 dark:bg-panel/70 backdrop-blur">
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
            <span className="font-display text-base font-bold tracking-tight text-gray-900 dark:text-ink">
              NebulaDB
            </span>
            <span className="eyebrow hidden sm:inline">Knowledge Ops</span>
          </div>
        </div>

        <div className="ml-auto flex items-center gap-2.5 text-xs">
          <HealthBadge health={health} err={healthErr} />
          <a
            className="btn-secondary !py-1 !px-2 hidden md:inline-flex"
            href="http://localhost:3000"
            target="_blank"
            rel="noreferrer"
            title="Open Grafana"
          >
            Grafana
          </a>
          <a
            className="btn-secondary !py-1 !px-2 hidden md:inline-flex"
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

/** Wordmark glyph: a small constellation node — the "nebula" in NebulaDB. */
function Mark() {
  return (
    <span className="grid h-7 w-7 place-items-center rounded-md bg-spectrum shadow-[0_4px_16px_-6px_rgba(124,92,255,0.8)]">
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden>
        <circle cx="8" cy="8" r="2.2" fill="#0B0E14" />
        <circle cx="3" cy="4" r="1" fill="#0B0E14" />
        <circle cx="13" cy="5" r="1" fill="#0B0E14" />
        <circle cx="12" cy="12" r="1" fill="#0B0E14" />
        <path d="M8 8 L3 4 M8 8 L13 5 M8 8 L12 12" stroke="#0B0E14" strokeWidth="0.8" opacity="0.7" />
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
      className={`relative z-10 border-r border-gray-200 dark:border-hairline bg-white/70 dark:bg-panel/40 backdrop-blur
                  transition-all duration-200 overflow-y-auto shrink-0
                  ${collapsed ? "w-14" : "w-56"}`}
    >
      <nav className="py-3">
        {(Object.keys(GROUP_LABELS) as Array<keyof typeof GROUP_LABELS>).map((g) => {
          const items = grouped[g] ?? [];
          if (items.length === 0) return null;
          return (
            <div key={g} className="mb-3">
              {!collapsed && (
                <div className="px-4 pt-2 pb-1.5 eyebrow">{GROUP_LABELS[g]}</div>
              )}
              {items.map((n) => {
                const isActive = active === n.id;
                return (
                  <button
                    key={n.id}
                    onClick={() => onSelect(n.id)}
                    title={n.hint}
                    aria-current={isActive ? "page" : undefined}
                    className={`group w-full flex items-center gap-3 px-4 py-2 text-sm
                                border-l-2 transition-colors
                                ${
                                  isActive
                                    ? "border-plasma bg-plasma/5 text-gray-900 dark:bg-plasma/10 dark:text-ink"
                                    : "border-transparent text-gray-600 dark:text-muted hover:bg-gray-100 dark:hover:bg-hairline/40 hover:text-gray-900 dark:hover:text-ink"
                                }`}
                  >
                    <span
                      className={`w-5 text-center text-base leading-none transition-colors
                        ${isActive ? "text-plasma dark:text-plasma-soft" : "text-gray-400 dark:text-muted group-hover:text-plasma dark:group-hover:text-plasma-soft"}`}
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
      <span className="inline-flex items-center gap-2 rounded-full border border-red-300/60 bg-red-50 px-2.5 py-1 font-mono text-red-700 dark:border-red-500/30 dark:bg-red-950/40 dark:text-red-300">
        <span className="h-1.5 w-1.5 rounded-full bg-red-500" />
        offline
      </span>
    );
  }
  if (!health) {
    return <span className="font-mono text-gray-400 dark:text-muted">connecting…</span>;
  }
  return (
    <span className="inline-flex items-center gap-2 rounded-full border border-gray-200 bg-white px-2.5 py-1 font-mono text-gray-600 dark:border-hairline dark:bg-panel dark:text-muted">
      <span className="relative flex h-1.5 w-1.5">
        <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-signal opacity-70" />
        <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-signal" />
      </span>
      <span className="text-gray-800 dark:text-ink">{health.model}</span>
      <span className="text-gray-300 dark:text-hairline">·</span>
      dim {health.dim}
      <span className="text-gray-300 dark:text-hairline">·</span>
      {health.docs} docs
    </span>
  );
}
