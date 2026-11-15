import { useEffect, useMemo, useState } from "react";
import { api, type SqlResponse } from "../../api";
import { ErrorBanner } from "../../components";
import { PipelineTrace } from "../../rag/PipelineTrace";
import { docPasses, filterSql, flatFields } from "../engine/core";
import { Bar, PersonaPicker, QuestionBox, fmtPct, personaOf } from "../shell";
import type { FilterField } from "../types";
import type { LabProps } from "./props";

const LIMITS = [5, 10, 20, 50, 200];

export function FiltersLab({ pack, state, update }: LabProps) {
  const q = state.q ?? pack.sample;
  const limit = state.k ?? 10;
  const values = state.f ?? {};
  const persona = personaOf(pack, state.persona);
  const extra = persona?.filter ?? {};
  const sql = filterSql(pack, q, values, limit, extra);
  const [res, setRes] = useState<SqlResponse | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    setBusy(true);
    setErr(null);
    api
      .sql(sql, true)
      .then((r) => live && setRes(r))
      .catch((e: Error) => live && setErr(e.message))
      .finally(() => live && setBusy(false));
    return () => {
      live = false;
    };
  }, [sql]);

  const passing = useMemo(() => pack.docs.filter((d) => docPasses(pack, d, values, extra)), [pack, values, extra]);
  const returned = res?.rows ?? [];
  const passIds = new Set(passing.map((d) => d.id));
  const found = returned.filter((r) => passIds.has(r.id)).length;
  const reachable = Math.min(limit, passing.length);
  const completeness = reachable ? found / reachable : 1;
  const candidates = Math.max(32, limit * 4);
  const filtered = Object.values(values).some(Boolean) || Object.keys(extra).length > 0;

  const setValue = (f: FilterField, v: string) => update({ f: { ...values, [f.field]: v } });
  const filteredQs = pack.questions.filter((x) => x.kind === "filtered");

  return (
    <div className="space-y-4">
      <div className="card space-y-4">
        <QuestionBox
          value={q}
          busy={busy}
          samples={[pack.sample, ...filteredQs.map((x) => x.q)]}
          onSubmit={(nq) => {
            const lq = pack.questions.find((x) => x.q === nq);
            update({ q: nq, f: lq?.filters ?? values });
          }}
        />
        <div className="flex flex-wrap items-end gap-3">
          {pack.filters.map((f) => (
            <FilterControl key={f.field} f={f} value={values[f.field] ?? ""} onChange={(v) => setValue(f, v)} />
          ))}
          <label className="text-xs text-gray-500 dark:text-muted">
            <span className="eyebrow block mb-1">LIMIT</span>
            <select className="input !w-auto !py-1.5" value={limit} onChange={(e) => update({ k: Number(e.target.value) })}>
              {LIMITS.map((n) => (
                <option key={n}>{n}</option>
              ))}
            </select>
          </label>
          <PersonaPicker pack={pack} value={state.persona} onChange={(id) => update({ persona: id })} />
          {filtered && (
            <button className="btn-secondary !py-1" onClick={() => update({ f: undefined, persona: undefined })}>
              Clear filters
            </button>
          )}
        </div>
        <pre className="rounded-md border border-gray-200 dark:border-edge bg-gray-50 dark:bg-carbon-950 p-3 text-[12px] font-mono whitespace-pre-wrap text-gray-700 dark:text-ink">
          {sql}
        </pre>
      </div>
      <ErrorBanner err={err} />

      {res && (
        <div className="grid gap-4 lg:grid-cols-[1fr_20rem]">
          <div className="card space-y-2">
            <div className="flex items-baseline justify-between">
              <div className="font-semibold text-sm text-black dark:text-ink">{returned.length} rows</div>
              <span className="font-mono text-[11px] text-gray-500 dark:text-muted">{res.took_ms} ms</span>
            </div>
            {returned.length === 0 && (
              <p className="text-sm text-gray-500 dark:text-muted">
                No rows. {passing.length === 0 ? "No document in the pack passes these filters." : "Matching documents exist but none were in the retrieved candidates — raise LIMIT."}
              </p>
            )}
            <ol className="space-y-1.5">
              {returned.map((r, i) => (
                <li key={r.id} className="rounded-md border border-gray-200 dark:border-edge px-2.5 py-2">
                  <div className="flex gap-2 text-[11px] font-mono text-gray-500 dark:text-muted">
                    <span>{i + 1}</span>
                    <span>{r.id}</span>
                    <span className="ml-auto" title="cosine distance — lower is closer">dist {r.score.toFixed(3)}</span>
                  </div>
                  <p className="mt-1 text-xs text-gray-700 dark:text-ink">{String(r.fields.text ?? pack.docs.find((d) => d.id === r.id)?.text ?? "")}</p>
                  <div className="mt-1 flex flex-wrap gap-1">
                    {pack.filters.map((f) => (
                      <span key={f.field} className="rounded bg-gray-100 dark:bg-carbon-800 px-1.5 py-0.5 text-[10px] font-mono text-gray-600 dark:text-muted">
                        {f.field}: {JSON.stringify(flatFields(r.fields)[f.field] ?? pack.docs.find((d) => d.id === r.id)?.metadata[f.field])}
                      </span>
                    ))}
                  </div>
                </li>
              ))}
            </ol>
          </div>

          <div className="space-y-4">
            <div className="card space-y-2">
              <div className="eyebrow">Did the filter cost recall?</div>
              <Bar value={completeness} label={fmtPct(completeness)} tone={completeness < 1 ? "warn" : "ok"} />
              <p className="text-xs text-gray-600 dark:text-muted">
                {passing.length} of {pack.docs.length} docs pass these filters; with LIMIT {limit} the best possible is {reachable}. NebulaDB
                returned {found}. It ranked {candidates} candidates by meaning first, then applied the filters.
              </p>
              {completeness < 1 && (
                <p className="text-xs text-warn">
                  Some matching docs weren't among the {candidates} nearest candidates. Raise LIMIT (more candidates) or loosen a filter.
                </p>
              )}
            </div>
            {res.explain && (
              <div className="card space-y-2">
                <div className="eyebrow">Plan (EXPLAIN ANALYZE)</div>
                <PipelineTrace
                  steps={res.explain.stages.map((s) => ({
                    label: s.label,
                    detail: s.detail,
                    rowsIn: s.rows_in ?? undefined,
                    rowsOut: s.rows_out ?? undefined,
                    ms: Math.round(s.took_us / 100) / 10,
                  }))}
                />
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function FilterControl({ f, value, onChange }: { f: FilterField; value: string; onChange: (v: string) => void }) {
  return (
    <label className="text-xs text-gray-500 dark:text-muted">
      <span className="eyebrow block mb-1">{f.label}</span>
      {f.op === "max" ? (
        <input className="input !w-28 !py-1.5" inputMode="decimal" placeholder="any" value={value} onChange={(e) => onChange(e.target.value.replace(/[^\d.]/g, ""))} />
      ) : f.op === "bool" ? (
        <select className="input !w-auto !py-1.5" value={value} onChange={(e) => onChange(e.target.value)}>
          <option value="">any</option>
          <option value="true">yes</option>
          <option value="false">no</option>
        </select>
      ) : (
        <select className="input !w-auto !py-1.5 max-w-[14rem]" value={value} onChange={(e) => onChange(e.target.value)}>
          <option value="">any</option>
          {f.values?.map((v) => (
            <option key={v} value={v}>
              {v}
            </option>
          ))}
        </select>
      )}
    </label>
  );
}
