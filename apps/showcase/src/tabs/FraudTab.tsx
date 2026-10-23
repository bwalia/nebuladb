/**
 * Fraud Neighbours — similarity-weighted vote over labelled history.
 * Deterministic risk score; no generative model.
 */
import { useEffect, useMemo, useState } from "react";
import { ApiError, type SqlRow } from "../api";
import { ErrorBanner, Panel, Spinner, Stat } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ensureSeeded } from "../rag/seed";
import { field, lit, runSql, semanticText, similarity } from "../rag/sql";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { ALERTS, SEED_VERSION, TRANSACTIONS, TX_BUCKET, type Label } from "../usecases/data/fraud";

interface Tx {
  id: string;
  label: Label;
  amount: number;
  mcc: string;
  channel: string;
  country: string;
  sim: number;
  text: string;
}

function toTx(r: SqlRow): Tx {
  return {
    id: String(field(r, "tx_id") ?? r.id),
    label: (field(r, "label") as Label) ?? "review",
    amount: Number(field(r, "amount_gbp") ?? 0),
    mcc: String(field(r, "mcc") ?? ""),
    channel: String(field(r, "channel") ?? ""),
    country: String(field(r, "country") ?? ""),
    sim: similarity(r.score),
    text: String(field(r, "text") ?? ""),
  };
}

function vote(neighbours: Tx[]) {
  const top = neighbours.slice(0, 8);
  let fraud = 0;
  let legit = 0;
  let review = 0;
  let wSum = 0;
  for (const t of top) {
    const w = t.sim * t.sim;
    wSum += w;
    if (t.label === "fraud") fraud += w;
    else if (t.label === "legit") legit += w;
    else review += w;
  }
  if (wSum <= 0) return { score: 0.5, label: "review" as Label, fraud: 0, legit: 0, review: 0 };
  const score = fraud / wSum;
  const label: Label = score >= 0.55 ? "fraud" : score <= 0.35 ? "legit" : "review";
  return {
    score,
    label,
    fraud: fraud / wSum,
    legit: legit / wSum,
    review: review / wSum,
  };
}

export function FraudTab() {
  const [seedMsg, setSeedMsg] = useState("Checking fraud corpus…");
  const [seeded, setSeeded] = useState(false);
  const [alertId, setAlertId] = useState(ALERTS[0].id);
  const [cnpOnly, setCnpOnly] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [hits, setHits] = useState<Tx[]>([]);

  const alert = ALERTS.find((a) => a.id === alertId) ?? ALERTS[0];

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await ensureSeeded(TX_BUCKET, TRANSACTIONS, SEED_VERSION, (d, t) => {
          if (!cancelled) setSeedMsg(`Seeding transactions ${d}/${t}…`);
        });
        if (!cancelled) {
          setSeeded(true);
          setSeedMsg(r.written ? `Seeded ${r.written} txs.` : `${r.total} labelled txs ready.`);
        }
      } catch (e) {
        if (!cancelled) setSeedMsg(`Seeding failed: ${(e as Error).message}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const run = async () => {
    setErr(null);
    setBusy(true);
    const q = lit(semanticText(alert.text));
    let sql = `SELECT * FROM ${TX_BUCKET}\nWHERE semantic_match(text, ${q})\n`;
    if (cnpOnly) sql += `  AND channel = 'card-not-present'\n`;
    sql += `LIMIT 20`;
    const trace: PipelineStep[] = [{ label: "Similar labelled txs", detail: sql, status: "running" }];
    setSteps([...trace]);
    try {
      const r = await runSql(sql);
      const rows = r.rows.map(toTx).sort((a, b) => b.sim - a.sim);
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

  const v = useMemo(() => vote(hits), [hits]);

  return (
    <div className="space-y-4">
      <Panel
        title="Fraud Neighbours"
        subtitle="Alert → nearest labelled transactions → similarity-weighted fraud/legit vote (no LLM)"
        action={
          <span className="metachip">
            <span className={`dot ${seeded ? "dot-ok" : "dot-warn animate-pulseok"}`} />
            {seedMsg}
          </span>
        }
      >
        <div className="flex flex-wrap gap-1.5 mb-3">
          {ALERTS.map((a) => (
            <button
              key={a.id}
              type="button"
              onClick={() => setAlertId(a.id)}
              className={`rounded-full border px-2.5 py-1 text-xs ${
                alertId === a.id
                  ? "border-accent text-accent bg-accent/10"
                  : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted"
              }`}
            >
              {a.title}
            </button>
          ))}
        </div>
        <p className="text-sm rounded-md border border-gray-200 dark:border-edge px-3 py-2 mb-3">{alert.text}</p>
        <div className="flex flex-wrap gap-3 items-center">
          <label className="text-xs flex items-center gap-2">
            <input type="checkbox" checked={cnpOnly} onChange={(e) => setCnpOnly(e.target.checked)} />
            CNP channel only
          </label>
          <button className="btn" disabled={!seeded || busy} onClick={() => void run()}>
            {busy ? <Spinner label="scoring…" /> : "Score alert"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {hits.length > 0 && (
        <div className="grid gap-4 lg:grid-cols-[16rem_1fr]">
          <Panel title="Risk vote" subtitle="Σ similarity² over top 8">
            <Stat label="fraud mass" value={`${Math.round(v.fraud * 100)}%`} />
            <Stat label="legit mass" value={`${Math.round(v.legit * 100)}%`} />
            <Stat label="review mass" value={`${Math.round(v.review * 100)}%`} />
            <div
              className={`mt-3 rounded-md px-3 py-2 text-center text-sm font-semibold uppercase ${
                v.label === "fraud"
                  ? "bg-bad/15 text-bad"
                  : v.label === "legit"
                    ? "bg-ok/15 text-ok"
                    : "bg-warn/15 text-warn"
              }`}
            >
              {v.label}
            </div>
          </Panel>
          <Panel title="Nearest labelled history" subtitle={`${hits.length} txs`}>
            <ul className="space-y-2">
              {hits.map((h) => (
                <li key={h.id} className="rounded-md border border-gray-200 dark:border-edge px-3 py-2 flex justify-between gap-2">
                  <div>
                    <div className="text-sm font-medium">
                      {h.id} · <span className="uppercase text-xs">{h.label}</span>
                    </div>
                    <div className="text-xs text-gray-500">
                      £{h.amount} · MCC {h.mcc} · {h.channel} · {h.country}
                    </div>
                  </div>
                  <Stat label="sim" value={`${Math.round(h.sim * 100)}%`} />
                </li>
              ))}
            </ul>
            <UnderTheHood filter={["/api/v1/query", "/api/v1/bucket"]} />
          </Panel>
        </div>
      )}
    </div>
  );
}
