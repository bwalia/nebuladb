import { useState } from "react";
import { api, ApiError, type SqlRow, type Hit } from "../api";
import { ErrorBanner, JsonView, Panel, Spinner, Stat } from "../components";

/**
 * Hybrid query demos. The point: **one** request expresses a
 * relational filter and a semantic ranking at the same time. Each
 * preset runs the equivalent pure-semantic search side-by-side so
 * the difference (filter dropping rows + re-ranking) is visible.
 */

interface Preset {
  label: string;
  blurb: string;
  query: string;
  sql: string;
  bucket?: string;
}

const PRESETS: Preset[] = [
  {
    label: "EU docs about DNS failover",
    blurb:
      "Semantic retrieval ranks by meaning; the SQL predicate drops rows where region ≠ 'eu'.",
    query: "dns failover",
    sql: "SELECT id, region, text FROM docs\n WHERE semantic_match(content, 'dns failover')\n   AND region = 'eu'\n LIMIT 10",
  },
  {
    label: "Top 3 incident reports by region",
    blurb: "GROUP BY on the residual filter column shows distribution across retrieved hits.",
    query: "incident",
    sql: "SELECT region, COUNT(*) AS n FROM docs\n WHERE semantic_match(content, 'incident')\n GROUP BY region\n ORDER BY n DESC\n LIMIT 3",
  },
  {
    label: "Zero-trust docs owned by platform",
    blurb: "Owner filter narrows retrieval to a specific team's corpus.",
    query: "zero trust",
    sql: "SELECT id, owner, text FROM docs\n WHERE semantic_match(content, 'zero trust architecture')\n   AND owner = 'platform'\n LIMIT 10",
  },
];

export function HybridTab() {
  const [preset, setPreset] = useState<Preset>(PRESETS[0]);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [hybrid, setHybrid] = useState<{ rows: SqlRow[]; took: number } | null>(null);
  const [plain, setPlain] = useState<{ hits: Hit[]; took: number } | null>(null);

  const run = async () => {
    setErr(null);
    setBusy(true);
    setHybrid(null);
    setPlain(null);
    try {
      // Fire both requests in parallel. The whole point of the
      // hybrid demo is the side-by-side comparison; issuing them
      // serially would double the latency the user sees.
      const [sqlResp, searchResp] = await Promise.all([
        api.sql(preset.sql),
        api.search(preset.query, 10, preset.bucket),
      ]);
      setHybrid({ rows: sqlResp.rows, took: sqlResp.took_ms });
      setPlain({ hits: searchResp.hits, took: searchResp.took_ms });
    } catch (e) {
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-4">
      <Panel
        title="Hybrid query"
        subtitle="One request: SQL filters + semantic retrieval. Compared side-by-side with plain semantic search."
      >
        <div className="flex flex-wrap gap-2">
          {PRESETS.map((p) => (
            <button
              key={p.label}
              onClick={() => setPreset(p)}
              className={`text-xs rounded-md px-3 py-1.5 border ${
                preset.label === p.label
                  ? "bg-brand-600 text-white border-brand-600"
                  : "border-gray-300 dark:border-gray-700 hover:bg-gray-100 dark:hover:bg-gray-800"
              }`}
            >
              {p.label}
            </button>
          ))}
        </div>

        <p className="text-sm text-gray-600 dark:text-gray-400">{preset.blurb}</p>

        <pre className="text-xs bg-gray-100 dark:bg-gray-950 rounded-md p-3 overflow-x-auto border border-gray-200 dark:border-gray-800">
          {preset.sql}
        </pre>

        <button className="btn" onClick={run} disabled={busy}>
          {busy ? <Spinner label="running…" /> : "Run hybrid + comparison"}
        </button>

        <ErrorBanner err={err} />
      </Panel>

      {(hybrid || plain) && (
        <div className="grid md:grid-cols-2 gap-4">
          <Panel
            title="Hybrid (SQL + semantic)"
            subtitle="SQL retrieves then filters"
            action={
              hybrid && (
                <div className="flex gap-3">
                  <Stat label="rows" value={hybrid.rows.length} />
                  <Stat label="took" value={`${hybrid.took}ms`} />
                </div>
              )
            }
          >
            {hybrid && <JsonView value={hybrid.rows} />}
          </Panel>
          <Panel
            title="Plain semantic (no filter)"
            subtitle={`Just semantic_match("${preset.query}")`}
            action={
              plain && (
                <div className="flex gap-3">
                  <Stat label="rows" value={plain.hits.length} />
                  <Stat label="took" value={`${plain.took}ms`} />
                </div>
              )
            }
          >
            {plain && (
              <JsonView
                value={plain.hits.map((h) => ({
                  id: h.id,
                  score: h.score,
                  metadata: h.metadata,
                }))}
              />
            )}
          </Panel>
        </div>
      )}
    </div>
  );
}
