import { useEffect, useState } from "react";
import { api, type Health } from "./api";
import { getTheme, toggleTheme } from "./theme";
import { DocumentsTab } from "./tabs/DocumentsTab";
import { SqlTab } from "./tabs/SqlTab";
import { SearchTab } from "./tabs/SearchTab";
import { RagTab } from "./tabs/RagTab";
import { HybridTab } from "./tabs/HybridTab";

type TabId = "documents" | "sql" | "search" | "rag" | "hybrid";

const TABS: Array<{ id: TabId; label: string; hint: string }> = [
  { id: "documents", label: "Documents", hint: "Ingest + chunk + embed" },
  { id: "sql", label: "SQL", hint: "Extended dialect with semantic_match" },
  { id: "search", label: "Semantic search", hint: "Vector retrieval" },
  { id: "rag", label: "RAG chat", hint: "Streaming answers" },
  { id: "hybrid", label: "Hybrid", hint: "SQL + retrieval in one query" },
];

export function App() {
  const [tab, setTab] = useState<TabId>("documents");
  const [theme, setThemeState] = useState(getTheme());
  const [health, setHealth] = useState<Health | null>(null);
  const [healthErr, setHealthErr] = useState<string | null>(null);

  // Poll /healthz so the badge in the header reflects live state
  // (dim, model, doc count). 5s is a compromise between "obviously
  // reflects my doc uploads" and "doesn't spam the server".
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

  return (
    <div className="min-h-full flex flex-col">
      <Header
        theme={theme}
        onToggleTheme={() => setThemeState(toggleTheme())}
        health={health}
        healthErr={healthErr}
      />

      <nav className="border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 sticky top-0 z-10">
        <div className="max-w-6xl mx-auto px-4 flex items-center gap-1 overflow-x-auto">
          {TABS.map((t) => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={`tab-btn ${tab === t.id ? "tab-btn-active" : ""}`}
              title={t.hint}
            >
              {t.label}
            </button>
          ))}
        </div>
      </nav>

      <main className="flex-1 max-w-6xl w-full mx-auto px-4 py-6">
        {tab === "documents" && <DocumentsTab />}
        {tab === "sql" && <SqlTab />}
        {tab === "search" && <SearchTab />}
        {tab === "rag" && <RagTab />}
        {tab === "hybrid" && <HybridTab />}
      </main>

      <footer className="border-t border-gray-200 dark:border-gray-800 py-3 text-center text-xs text-gray-500 dark:text-gray-400">
        NebulaDB · REST <a className="underline" href="/healthz">/healthz</a> ·
        Grafana at <a className="underline" href="http://localhost:3000">:3000</a> ·
        Prometheus at <a className="underline" href="http://localhost:9090">:9090</a>
      </footer>
    </div>
  );
}

function Header({
  theme,
  onToggleTheme,
  health,
  healthErr,
}: {
  theme: string;
  onToggleTheme: () => void;
  health: Health | null;
  healthErr: string | null;
}) {
  return (
    <header className="border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900">
      <div className="max-w-6xl mx-auto px-4 py-3 flex items-center justify-between gap-4">
        <div className="flex items-baseline gap-3">
          <h1 className="text-lg font-bold tracking-tight">NebulaDB</h1>
          <span className="text-xs text-gray-500 dark:text-gray-400">
            Knowledge Ops showcase
          </span>
        </div>
        <div className="flex items-center gap-3 text-xs">
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
