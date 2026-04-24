import { useState } from "react";
import { api, ApiError, type SqlRow } from "../api";
import { ErrorBanner, JsonView, Panel, Spinner, Stat } from "../components";
import { downloadBlob, recordHistory, rowsToCsv } from "../utils";

const EXAMPLES: Array<{ label: string; sql: string }> = [
  {
    label: "Semantic search",
    sql: "SELECT id, text FROM docs WHERE semantic_match(content, 'zero trust networking') LIMIT 5",
  },
  {
    label: "Residual filter",
    sql: "SELECT id, region FROM docs\n WHERE semantic_match(content, 'dns failover') AND region = 'eu'\n LIMIT 10",
  },
  {
    label: "GROUP BY + COUNT",
    sql: "SELECT region, COUNT(*) AS n FROM docs\n WHERE semantic_match(content, 'incident')\n GROUP BY region\n ORDER BY n DESC",
  },
  {
    label: "INNER JOIN",
    sql: "SELECT u.id, o.id FROM users AS u\n JOIN orders AS o ON u.region = o.region\n WHERE semantic_match(u.content, 'developer')\n   AND semantic_match(o.content, 'subscription')",
  },
];

export function SqlTab() {
  const [sql, setSql] = useState(EXAMPLES[0].sql);
  const [rows, setRows] = useState<SqlRow[] | null>(null);
  const [took, setTook] = useState<number | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [selected, setSelected] = useState<SqlRow | null>(null);

  const run = async () => {
    setErr(null);
    setBusy(true);
    try {
      const r = await api.sql(sql);
      setRows(r.rows);
      setTook(r.took_ms);
      setSelected(null);
      // Persist the success so the Admin tab can show a queryable
      // history across page reloads.
      recordHistory({ ts: Date.now(), sql, ok: true, took_ms: r.took_ms, rows: r.rows.length });
    } catch (e) {
      const msg = e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message;
      setErr(msg);
      setRows(null);
      recordHistory({ ts: Date.now(), sql, ok: false, error: msg });
    } finally {
      setBusy(false);
    }
  };

  const exportCsv = () => {
    if (!rows || rows.length === 0) return;
    downloadBlob(
      `nebula-export-${Date.now()}.csv`,
      new Blob([rowsToCsv(rows)], { type: "text/csv;charset=utf-8" })
    );
  };
  const exportJson = () => {
    if (!rows || rows.length === 0) return;
    downloadBlob(
      `nebula-export-${Date.now()}.json`,
      new Blob([JSON.stringify(rows, null, 2)], { type: "application/json" })
    );
  };

  const columns = computeColumns(rows);

  return (
    <div className="space-y-4">
      <Panel
        title="SQL console"
        subtitle="NebulaDB's SELECT dialect — use semantic_match(col, 'text') for retrieval"
        action={
          <select
            className="input !w-auto !py-1 !text-xs"
            value=""
            onChange={(e) => {
              const ex = EXAMPLES.find((x) => x.label === e.target.value);
              if (ex) setSql(ex.sql);
            }}
            title="Load example"
          >
            <option value="">Load example…</option>
            {EXAMPLES.map((e) => (
              <option key={e.label}>{e.label}</option>
            ))}
          </select>
        }
      >
        <textarea
          className="input font-mono text-xs min-h-[9rem] resize-y"
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          spellCheck={false}
          onKeyDown={(e) => {
            // Ctrl/Cmd+Enter to run — the universal "just run this"
            // shortcut every SQL console respects.
            if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
              e.preventDefault();
              void run();
            }
          }}
        />
        <div className="flex items-center gap-3">
          <button className="btn" onClick={run} disabled={busy}>
            {busy ? <Spinner label="running…" /> : "Run"}
          </button>
          {took !== null && <Stat label="took" value={`${took}ms`} />}
          {rows !== null && <Stat label="rows" value={rows.length} />}
        </div>
        <ErrorBanner err={err} />
      </Panel>

      {rows && rows.length > 0 && (
        <Panel
          title="Results"
          subtitle="Click a row to inspect raw JSON"
          action={
            <div className="flex gap-2">
              <button className="btn-secondary !py-1 !px-2 !text-xs" onClick={exportCsv}>
                Export CSV
              </button>
              <button className="btn-secondary !py-1 !px-2 !text-xs" onClick={exportJson}>
                Export JSON
              </button>
            </div>
          }
        >
          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead className="bg-gray-100 dark:bg-gray-950 text-gray-600 dark:text-gray-400">
                <tr>
                  <th className="text-left px-2 py-1 font-medium">#</th>
                  <th className="text-left px-2 py-1 font-medium">id</th>
                  <th className="text-left px-2 py-1 font-medium">score</th>
                  {columns.map((c) => (
                    <th key={c} className="text-left px-2 py-1 font-medium">
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => (
                  <tr
                    key={i}
                    onClick={() => setSelected(r)}
                    className="border-t border-gray-200 dark:border-gray-800 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-800"
                  >
                    <td className="px-2 py-1 text-gray-500">{i + 1}</td>
                    <td className="px-2 py-1 font-mono">{r.id}</td>
                    <td className="px-2 py-1 font-mono">{r.score.toFixed(4)}</td>
                    {columns.map((c) => (
                      <td key={c} className="px-2 py-1 align-top max-w-sm truncate">
                        {renderCell(r.fields[c])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {selected && (
            <div className="pt-3">
              <div className="text-xs text-gray-500 mb-1">selected row</div>
              <JsonView value={selected} />
            </div>
          )}
        </Panel>
      )}

      {rows && rows.length === 0 && (
        <Panel title="Results" subtitle="Query ran, returned zero rows.">
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Try ingesting a document first, then re-run.
          </p>
        </Panel>
      )}
    </div>
  );
}

/**
 * A SQL query doesn't tell us its projection up-front, so we derive
 * column order from the union of keys across the returned rows. Keeps
 * the table flexible without inventing a schema layer.
 */
function computeColumns(rows: SqlRow[] | null): string[] {
  if (!rows || rows.length === 0) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const r of rows) {
    for (const k of Object.keys(r.fields)) {
      if (!seen.has(k)) {
        seen.add(k);
        out.push(k);
      }
    }
  }
  return out;
}

function renderCell(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return JSON.stringify(v);
}
