/**
 * Lookalike Accounts — pick a won customer, retrieve similar prospects
 * with SQL hard filters (industry, min ARR excluded, stage=prospect).
 */
import { useEffect, useMemo, useState } from "react";
import { ApiError, type SqlRow } from "../api";
import { ErrorBanner, Panel, Spinner, Stat } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ensureSeeded } from "../rag/seed";
import { field, lit, minMax, runSql, semanticText, similarity } from "../rag/sql";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { ACCOUNTS, CRM_BUCKET, INDUSTRIES, SEED_VERSION, type Industry } from "../usecases/data/crm";

interface Acc {
  id: string;
  name: string;
  industry: string;
  country: string;
  stage: string;
  arr: number;
  employees: number;
  sim: number;
  text: string;
}

function toAcc(r: SqlRow): Acc {
  return {
    id: String(field(r, "account_id") ?? r.id),
    name: String(field(r, "name") ?? ""),
    industry: String(field(r, "industry") ?? ""),
    country: String(field(r, "country") ?? ""),
    stage: String(field(r, "stage") ?? ""),
    arr: Number(field(r, "arr_gbp") ?? 0),
    employees: Number(field(r, "employees") ?? 0),
    sim: similarity(r.score),
    text: String(field(r, "text") ?? ""),
  };
}

const ICPS = ACCOUNTS.filter((a) => a.metadata.stage === "customer");

export function LookalikeTab() {
  const [seedMsg, setSeedMsg] = useState("Checking CRM corpus…");
  const [seeded, setSeeded] = useState(false);
  const [icpId, setIcpId] = useState(String(ICPS[0]?.id ?? "ACC-100"));
  const [industry, setIndustry] = useState<Industry | "">("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [hits, setHits] = useState<Acc[]>([]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await ensureSeeded(CRM_BUCKET, ACCOUNTS, SEED_VERSION, (d, t) => {
          if (!cancelled) setSeedMsg(`Seeding CRM ${d}/${t}…`);
        });
        if (!cancelled) {
          setSeeded(true);
          setSeedMsg(r.written ? `Seeded ${r.written} accounts.` : `${r.total} accounts ready.`);
        }
      } catch (e) {
        if (!cancelled) setSeedMsg(`Seeding failed: ${(e as Error).message}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const icp = useMemo(() => ACCOUNTS.find((a) => a.id === icpId), [icpId]);

  const run = async () => {
    if (!icp) return;
    setErr(null);
    setBusy(true);
    const q = lit(semanticText(icp.text));
    const ind = industry || String(icp.metadata.industry ?? "");
    let sql =
      `SELECT * FROM ${CRM_BUCKET}\nWHERE semantic_match(text, ${q})\n` +
      `  AND stage = 'prospect'\n` +
      `  AND account_id <> ${lit(icpId)}\n`;
    if (ind) sql += `  AND industry = ${lit(ind)}\n`;
    sql += `LIMIT 12`;
    const trace: PipelineStep[] = [{ label: "Lookalike prospects", detail: sql, status: "running" }];
    setSteps([...trace]);
    try {
      const r = await runSql(sql);
      const rows = r.rows.map(toAcc).sort((a, b) => b.sim - a.sim);
      trace[0] = { ...trace[0], rowsOut: rows.length, ms: r.tookMs, status: "ok" };
      setSteps([...trace]);
      setHits(rows);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const norm = useMemo(() => minMax(hits.map((h) => h.sim)), [hits]);

  return (
    <div className="space-y-4">
      <Panel
        title="Lookalike Accounts"
        subtitle="Won-customer ICP → semantically similar prospects with industry filters — classic GTM use of a vector+SQL store"
        action={
          <span className="metachip">
            <span className={`dot ${seeded ? "dot-ok" : "dot-warn animate-pulseok"}`} />
            {seedMsg}
          </span>
        }
      >
        <div className="flex flex-wrap gap-3 items-end mb-3">
          <label className="text-xs">
            <span className="block font-medium mb-1">ICP (customer)</span>
            <select className="code-input !py-1.5 !text-sm" value={icpId} onChange={(e) => setIcpId(e.target.value)}>
              {ICPS.map((a) => (
                <option key={a.id} value={a.id}>
                  {String(a.metadata.name)} · {String(a.metadata.industry)}
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs">
            <span className="block font-medium mb-1">Industry lock</span>
            <select
              className="code-input !py-1.5 !text-sm"
              value={industry}
              onChange={(e) => setIndustry(e.target.value as Industry | "")}
            >
              <option value="">match ICP industry</option>
              {INDUSTRIES.map((i) => (
                <option key={i} value={i}>
                  {i}
                </option>
              ))}
            </select>
          </label>
          <button className="btn" disabled={!seeded || busy} onClick={() => void run()}>
            {busy ? <Spinner label="scoring…" /> : "Find lookalikes"}
          </button>
        </div>
        {icp && (
          <p className="text-xs text-gray-600 dark:text-muted mb-2 line-clamp-2">{icp.text}</p>
        )}
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {hits.length > 0 && (
        <Panel title="Prospect shortlist" subtitle="stage = prospect · ranked by similarity to ICP">
          <ul className="space-y-2">
            {hits.map((h, i) => (
              <li key={h.id} className="rounded-md border border-gray-200 dark:border-edge px-3 py-2 flex justify-between gap-3">
                <div>
                  <div className="text-sm font-medium">
                    #{i + 1} · {h.id} · {h.name}
                  </div>
                  <div className="text-xs text-gray-500">
                    {h.industry} · {h.country} · {h.employees} emp · ARR £{h.arr.toLocaleString()}
                  </div>
                  <p className="text-xs mt-1 text-gray-600 dark:text-muted line-clamp-2">{h.text}</p>
                </div>
                <Stat label="fit" value={`${Math.round(norm(h.sim) * 100)}%`} />
              </li>
            ))}
          </ul>
          <UnderTheHood filter={["/api/v1/query", "/api/v1/bucket"]} />
        </Panel>
      )}
    </div>
  );
}
