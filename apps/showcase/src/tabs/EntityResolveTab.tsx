/**
 * Entity Resolve — CRM deduplication via semantic neighbours + hard filters.
 * No LLM: score and recommend merge / review / distinct in the browser.
 */
import { useEffect, useMemo, useState } from "react";
import { ApiError, type SqlRow } from "../api";
import { ErrorBanner, Panel, Spinner, Stat } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ensureSeeded } from "../rag/seed";
import { field, lit, runSql, semanticText, similarity } from "../rag/sql";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import {
  ACCOUNTS,
  CRM_BUCKET,
  SEED_VERSION,
  SUSPECT_SEEDS,
} from "../usecases/data/crm";

interface Acc {
  id: string;
  name: string;
  industry: string;
  country: string;
  domain: string;
  stage: string;
  arr: number;
  text: string;
  sim: number;
}

function toAcc(r: SqlRow): Acc {
  return {
    id: String(field(r, "account_id") ?? r.id),
    name: String(field(r, "name") ?? ""),
    industry: String(field(r, "industry") ?? ""),
    country: String(field(r, "country") ?? ""),
    domain: String(field(r, "domain") ?? ""),
    stage: String(field(r, "stage") ?? ""),
    arr: Number(field(r, "arr_gbp") ?? 0),
    text: String(field(r, "text") ?? ""),
    sim: similarity(r.score),
  };
}

function decision(seed: Acc, hit: Acc): { label: "merge" | "review" | "distinct"; why: string } {
  const sameDomain = seed.domain.split(".")[0] === hit.domain.split(".")[0];
  const sameIndustry = seed.industry === hit.industry;
  if (hit.sim > 0.72 && sameIndustry && (sameDomain || seed.country === hit.country)) {
    return { label: "merge", why: "High similarity + shared industry and brand/geo signal" };
  }
  if (hit.sim > 0.55 && sameIndustry) {
    return { label: "review", why: "Similar description — human check on company number / domain" };
  }
  return { label: "distinct", why: "Weak overlap after filters" };
}

export function EntityResolveTab() {
  const [seedMsg, setSeedMsg] = useState("Checking CRM corpus…");
  const [seeded, setSeeded] = useState(false);
  const [leftId, setLeftId] = useState(SUSPECT_SEEDS[0].left);
  const [sameCountry, setSameCountry] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [seed, setSeed] = useState<Acc | null>(null);
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

  const leftMeta = useMemo(() => ACCOUNTS.find((a) => a.id === leftId), [leftId]);

  const run = async () => {
    if (!leftMeta) return;
    setErr(null);
    setBusy(true);
    const q = lit(semanticText(leftMeta.text));
    const country = String(leftMeta.metadata.country ?? "");
    let sql =
      `SELECT * FROM ${CRM_BUCKET}\nWHERE semantic_match(text, ${q})\n` +
      `  AND account_id <> ${lit(leftId)}\n`;
    if (sameCountry && country) sql += `  AND country = ${lit(country)}\n`;
    sql += `LIMIT 15`;
    const trace: PipelineStep[] = [
      { label: "Near-duplicate search", detail: sql, status: "running" },
    ];
    setSteps([...trace]);
    try {
      const r = await runSql(sql);
      const rows = r.rows.map(toAcc).sort((a, b) => b.sim - a.sim);
      const self: Acc = {
        id: leftId,
        name: String(leftMeta.metadata.name ?? ""),
        industry: String(leftMeta.metadata.industry ?? ""),
        country,
        domain: String(leftMeta.metadata.domain ?? ""),
        stage: String(leftMeta.metadata.stage ?? ""),
        arr: Number(leftMeta.metadata.arr_gbp ?? 0),
        text: leftMeta.text,
        sim: 1,
      };
      trace[0] = { ...trace[0], rowsOut: rows.length, ms: r.tookMs, status: "ok" };
      setSteps([...trace]);
      setSeed(self);
      setHits(rows);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-4">
      <Panel
        title="Entity Resolve"
        subtitle="CRM deduplication — semantic neighbours + industry/geo filters, merge recommendations without an LLM"
        action={
          <span className="metachip">
            <span className={`dot ${seeded ? "dot-ok" : "dot-warn animate-pulseok"}`} />
            {seedMsg}
          </span>
        }
      >
        <div className="flex flex-wrap gap-1.5 mb-3">
          {SUSPECT_SEEDS.map((s) => (
            <button
              key={s.left + s.right}
              type="button"
              onClick={() => setLeftId(s.left)}
              className={`rounded-full border px-2.5 py-1 text-xs ${
                leftId === s.left
                  ? "border-accent text-accent bg-accent/10"
                  : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted"
              }`}
            >
              {s.left} · {s.reason}
            </button>
          ))}
        </div>
        <div className="flex flex-wrap gap-3 items-center">
          <label className="text-xs flex items-center gap-2">
            <input type="checkbox" checked={sameCountry} onChange={(e) => setSameCountry(e.target.checked)} />
            Same country only
          </label>
          <button className="btn" disabled={!seeded || busy} onClick={() => void run()}>
            {busy ? <Spinner label="matching…" /> : "Find duplicates"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {seed && (
        <div className="grid gap-4 lg:grid-cols-2">
          <Panel title="Seed account" subtitle={seed.id}>
            <div className="text-sm font-semibold">{seed.name}</div>
            <div className="text-xs text-gray-500 dark:text-muted mt-1">
              {seed.industry} · {seed.country} · {seed.stage} · {seed.domain}
            </div>
            <p className="mt-2 text-xs text-gray-600 dark:text-muted whitespace-pre-wrap">{seed.text}</p>
          </Panel>
          <Panel title="Candidates" subtitle={`${hits.length} neighbours`}>
            <ul className="space-y-2">
              {hits.map((h) => {
                const d = decision(seed, h);
                return (
                  <li key={h.id} className="rounded-md border border-gray-200 dark:border-edge px-3 py-2">
                    <div className="flex justify-between gap-2 items-start">
                      <div>
                        <div className="text-sm font-medium">
                          {h.id} · {h.name}
                        </div>
                        <div className="text-xs text-gray-500">
                          {h.industry} · {h.country} · {h.domain}
                        </div>
                        <div className="text-xs mt-1 text-gray-600 dark:text-muted">{d.why}</div>
                      </div>
                      <div className="text-right space-y-1">
                        <Stat label="sim" value={`${Math.round(h.sim * 100)}%`} />
                        <span
                          className={`inline-block rounded-full px-2 py-0.5 text-[10px] uppercase font-semibold ${
                            d.label === "merge"
                              ? "bg-ok/15 text-ok"
                              : d.label === "review"
                                ? "bg-warn/15 text-warn"
                                : "bg-gray-200 dark:bg-edge text-gray-600"
                          }`}
                        >
                          {d.label}
                        </span>
                      </div>
                    </div>
                  </li>
                );
              })}
            </ul>
            <UnderTheHood filter={["/api/v1/query", "/api/v1/bucket"]} />
          </Panel>
        </div>
      )}
    </div>
  );
}
