import { useEffect, useRef, useState } from "react";
import { Panel } from "../components";

/**
 * Metrics tab. Embeds Grafana in an iframe so operators don't need
 * to tab-switch to see the pre-provisioned dashboards alongside their
 * workflow.
 *
 * The Grafana URL is read from `localStorage` (`nebula:grafana-url`)
 * so devs with custom port mappings don't have to rebuild the app;
 * sensible default is `http://localhost:3000`. The selected dashboard
 * uid is stored under `nebula:grafana-dashboard`.
 */

const URL_KEY = "nebula:grafana-url";
const DASH_KEY = "nebula:grafana-dashboard";
const DEFAULT_URL = "http://localhost:3000";

const DASHBOARDS = [
  {
    uid: "nebula-overview",
    slug: "nebuladb-overview",
    label: "Overview",
    hint: "Traffic + HA strip",
  },
  {
    uid: "nebula-reliability",
    slug: "nebuladb-reliability",
    label: "Reliability",
    hint: "Mode, memory, replication",
  },
  {
    uid: "nebula-query-perf",
    slug: "nebuladb-query-performance",
    label: "Query Performance",
    hint: "Errors + endpoint mix",
  },
  {
    uid: "nebula-ai",
    slug: "nebuladb-ai-cache",
    label: "AI & Cache",
    hint: "Semantic / RAG / embed cache",
  },
  {
    uid: "nebula-durability",
    slug: "nebuladb-durability-health",
    label: "Durability",
    hint: "WAL + snapshots",
  },
  {
    uid: "nebula-backup",
    slug: "nebuladb-backup-restore",
    label: "Backup & Restore",
    hint: "Job age, size, failures",
  },
  {
    uid: "nebula-stack",
    slug: "nebuladb-stack-health",
    label: "Stack health",
    hint: "Gatus probes",
  },
] as const;

type DashUid = (typeof DASHBOARDS)[number]["uid"];

function dashPath(uid: DashUid): string {
  const d = DASHBOARDS.find((x) => x.uid === uid) ?? DASHBOARDS[0];
  return `/d/${d.uid}/${d.slug}?orgId=1&kiosk=tv&refresh=10s`;
}

function readStoredDash(): DashUid {
  try {
    const v = localStorage.getItem(DASH_KEY);
    if (v && DASHBOARDS.some((d) => d.uid === v)) return v as DashUid;
  } catch {
    /* private mode */
  }
  return "nebula-overview";
}

export function MetricsTab() {
  const [baseUrl, setBaseUrl] = useState<string>(() => {
    try {
      return localStorage.getItem(URL_KEY) || DEFAULT_URL;
    } catch {
      return DEFAULT_URL;
    }
  });
  const [dashUid, setDashUid] = useState<DashUid>(readStoredDash);
  const [reachable, setReachable] = useState<"checking" | "yes" | "no">(
    "checking"
  );
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    let cancelled = false;
    const probe = async () => {
      setReachable("checking");
      try {
        // Grafana's `/api/health` is lightweight and CORS-open by
        // default; a `no-cors` fetch lets us differentiate
        // "unreachable" from "reachable but blocked CORS" — both
        // come back with `ok: false` but the former *throws*.
        await fetch(`${baseUrl}/api/health`, { mode: "no-cors" });
        if (!cancelled) setReachable("yes");
      } catch {
        if (!cancelled) setReachable("no");
      }
    };
    void probe();
    return () => {
      cancelled = true;
    };
  }, [baseUrl]);

  const saveUrl = () => {
    const next = inputRef.current?.value?.trim() || DEFAULT_URL;
    try {
      localStorage.setItem(URL_KEY, next);
    } catch {
      /* private mode — apply for session only */
    }
    setBaseUrl(next);
  };

  const selectDash = (uid: DashUid) => {
    setDashUid(uid);
    try {
      localStorage.setItem(DASH_KEY, uid);
    } catch {
      /* private mode */
    }
  };

  const fullUrl = `${baseUrl}${dashPath(dashUid)}`;
  const current = DASHBOARDS.find((d) => d.uid === dashUid) ?? DASHBOARDS[0];

  return (
    <div className="space-y-4">
      <Panel
        title="Metrics"
        subtitle={`Embedded Grafana — ${current.label} (${current.hint})`}
        action={
          <a
            className="btn-secondary !py-1 !px-2 !text-xs"
            href={fullUrl.replace("&kiosk=tv", "")}
            target="_blank"
            rel="noreferrer"
            title="Open Grafana in a new tab with full chrome"
          >
            Open in Grafana ↗
          </a>
        }
      >
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="text-gray-500 dark:text-gray-400">Dashboard</span>
          <select
            className="input !py-1 !w-auto min-w-[12rem]"
            value={dashUid}
            onChange={(e) => selectDash(e.target.value as DashUid)}
            aria-label="Grafana dashboard"
          >
            {DASHBOARDS.map((d) => (
              <option key={d.uid} value={d.uid}>
                {d.label}
              </option>
            ))}
          </select>
          <span className="text-gray-500 dark:text-gray-400">Grafana URL</span>
          <input
            ref={inputRef}
            className="input !py-1 flex-1 min-w-[16rem]"
            defaultValue={baseUrl}
            onKeyDown={(e) => {
              if (e.key === "Enter") saveUrl();
            }}
          />
          <button className="btn-secondary !py-1 !px-2 !text-xs" onClick={saveUrl}>
            Save
          </button>
          <ReachabilityBadge status={reachable} />
        </div>
      </Panel>

      {reachable === "no" ? (
        <Panel
          title="Grafana unreachable"
          subtitle={`No response from ${baseUrl}/api/health`}
        >
          <p className="text-sm text-gray-700 dark:text-gray-300">
            Bring it up with the rest of the compose stack:
          </p>
          <pre className="text-xs bg-gray-100 dark:bg-gray-950 rounded-md p-2 mt-2 overflow-x-auto">
            {`./scripts/start.sh            # picks free ports, builds everything
# or, piece-meal:
docker compose up -d grafana prometheus
`}
          </pre>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-3">
            If Grafana is up on a non-default host/port, set the URL above.
            Common case: the ports that start.sh auto-picks are different
            when :3000 is already taken — check the URL table start.sh prints.
          </p>
        </Panel>
      ) : (
        <div className="card p-0 overflow-hidden">
          <iframe
            // `loading="lazy"` avoids burning a Grafana round-trip
            // when the user is still on another tab. `key` forces a
            // full reload when the URL changes so the saved URL
            // actually takes effect immediately.
            key={fullUrl}
            src={fullUrl}
            loading="lazy"
            title={`NebulaDB metrics — ${current.label}`}
            className="w-full h-[70vh] border-0"
          />
        </div>
      )}
    </div>
  );
}

function ReachabilityBadge({
  status,
}: {
  status: "checking" | "yes" | "no";
}) {
  const cls =
    status === "yes"
      ? "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300"
      : status === "no"
      ? "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300"
      : "bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-300";
  const label =
    status === "yes" ? "reachable" : status === "no" ? "offline" : "probing…";
  return (
    <span className={`rounded-full px-2 py-0.5 text-xs ${cls}`}>{label}</span>
  );
}
